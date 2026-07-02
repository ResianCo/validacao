#!/usr/bin/env python3
"""
Resian Consultoria — Passo 0: Validacao de Layout das Bases de Entrada
Versao: 2.0 — Out-of-Core (memoria restrita)

Valida LOANS.csv, INSTALLMENTS.csv e RENEGOCIACAO.csv antes de rodar
o pipeline_csv.py. Deve ser executado sempre como primeira etapa.

ARQUITETURA DE MEMORIA (VM 24GB RAM / 4 CPU / 100GB disco):
  - Regra 1 (streaming): nenhum arquivo do MinIO/S3 e lido com obj["Body"].read().
    O download e feito por streaming direto para o disco (s3.download_file) e o
    arquivo temporario e apagado no final.
  - LOANS.csv (~2M) e RENEGOCIACAO.csv (pequeno): validados em pandas (cabem na RAM).
  - INSTALLMENTS.csv (ate 72M de linhas): NUNCA e carregado em pandas. Todas as
    validacoes (contagem, nulos, tipos, valores) sao feitas via queries agregadas
    do DuckDB lendo o CSV direto do disco. A consistencia cruzada usa ANTI-JOIN
    relacional no DuckDB, jamais conjuntos Python (set) com dezenas de milhoes de IDs.

Uso:
    # Com MinIO (padrao):
    python3 passo0_validacao.py --data_base 2026-05

    # Com arquivos locais:
    python3 passo0_validacao.py --data_base 2026-05 --local_dir ./dados

    # Com bucket especifico:
    python3 passo0_validacao.py --data_base 2026-05 --bucket outro-bucket

Variaveis de ambiente (mesmas do pipeline_csv.py):
    MINIO_URL        = https://...
    MINIO_ACCESS_KEY = (set via env var or docker secret — never hardcoded)
    MINIO_SECRET_KEY = (set via env var or docker secret — never hardcoded)

Saida:
    Relatorio de validacao no terminal com status [OK] / [AVISO] / [ERRO]
    Exit 0 se nao ha erros bloqueantes
    Exit 1 se ha erros bloqueantes (pipeline NAO deve rodar)

Dependencias:
    pip install boto3 duckdb pandas --break-system-packages
"""

import argparse
import os
import sys
import tempfile
from datetime import datetime

import boto3
import duckdb
import pandas as pd

# ============================================================================
# SCHEMAS DE LAYOUT
# ============================================================================

LOANS_COLUNAS_OBRIGATORIAS_NULOS_PROIBIDOS = [
    "principal",
    "pmt",
    "num_parcelas",
    "data_aquisicao",
    "taxa_cliente",
    "rating_concessao",      # obrigatorio: usado na metodologia de projecao
]

LOANS_COLUNAS_OBRIGATORIAS_NULOS_PERMITIDOS = [
    "id_contrato",
    "dias_maior_atraso",
]

LOANS_COLUNAS_OPCIONAIS = [
    "vencimento_primeira_parcela",
    "vencimento_ultima_parcela",
    "bhv_m",
    "regiao",
    "uf",
    "idade",
    "especie_beneficio",
    "situacao_beneficio",
    "canal_originacao",
    "competencia_averbacao",
    "competencia_inicio_desconto",
    "margem_consignavel_utilizada",
    "prazo_remanescente",
    "tipo_produto",
    "taxa_funding",
    "profissao",
]

INSTALLMENTS_COLUNAS_OBRIGATORIAS_NULOS_PROIBIDOS = [
    "num_parcela",
    "data_vencimento",
]

INSTALLMENTS_COLUNAS_OBRIGATORIAS_NULOS_PERMITIDOS = [
    "id_contrato",
    "id_parcela",
    "data_pagamento",      # nulo = parcela nao paga (inadimplencia — dado valido)
    "valor_pago",          # nulo = sem pagamento registrado (valido)
    "valor_parcela",
    "dias_atraso",
]

INSTALLMENTS_COLUNAS_OPCIONAIS = [
    "motivo_falha_repasse",
]

RENEGOCIACAO_COLUNAS_OBRIGATORIAS_NULOS_PROIBIDOS = [
    "id_contrato_novo",
    "id_contrato_origem",
]

RENEGOCIACAO_COLUNAS_OBRIGATORIAS_NULOS_PERMITIDOS = [
    "id_parcela_reneg",
    "data_renegociacao",
    "valor_refinanciado",
    "valor_troco",
    "num_parcela",
    "data_vencimento",
    "valor_parcela",
    "data_pagamento",
    "valor_pago",
]

# Formatos de data aceitos pelo parse_date() do pipeline
DATE_FORMATS = ["%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]

# Mesma lista, no formato de lista do DuckDB (try_strptime aceita varios formatos)
DUCK_DATE_FMTS = "['%d/%m/%Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']"

# Colunas que o pipeline trata como 0 quando nulas (aviso, nao erro)
COLS_TRATADAS_COMO_ZERO = ["dias_maior_atraso", "valor_parcela", "dias_atraso"]

# ============================================================================
# ESTADO DE VALIDACAO
# ============================================================================

def _aplicar_limites_duck(conn, path_ref=None):
    """
    Limita a RAM do DuckDB e habilita SPILL para disco. Sem isto, em uma VM sem
    swap, o scan do INSTALLMENTS (72M linhas) estoura a memoria e CONGELA a maquina
    (precisando de reboot). O temp_directory aponta para o mesmo disco do CSV.
    """
    try:
        conn.execute("PRAGMA memory_limit='6GB'")
        conn.execute("PRAGMA threads=3")
        conn.execute("PRAGMA preserve_insertion_order=false")
        base = os.path.dirname(path_ref) if path_ref else tempfile.gettempdir()
        tmpdir = os.path.join(base or ".", "duck_tmp")
        os.makedirs(tmpdir, exist_ok=True)
        conn.execute(f"PRAGMA temp_directory='{tmpdir}'")
        conn.execute("PRAGMA max_temp_directory_size='120GB'")
    except Exception:
        pass


class Resultado:
    def __init__(self):
        self.linhas = []
        self.erros  = 0
        self.avisos = 0

    def ok(self, msg):
        self.linhas.append(f"  [OK]    {msg}")

    def aviso(self, msg):
        self.linhas.append(f"  [AVISO] {msg}")
        self.avisos += 1

    def erro(self, msg):
        self.linhas.append(f"  [ERRO]  {msg}")
        self.erros += 1

    def titulo(self, msg):
        self.linhas.append(f"\n{msg}")
        self.linhas.append("-" * 60)

    def imprimir(self):
        for linha in self.linhas:
            print(linha)


r = Resultado()

# ============================================================================
# HELPERS
# ============================================================================

def parse_date_safe(s):
    """Tenta parsear data nos formatos aceitos pelo pipeline."""
    if pd.isna(s) or str(s).strip() in ("", "nan", "None", "NaT"):
        return None
    s = str(s).strip()[:19]
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return False  # False = presente mas invalido (diferente de None = ausente)


def normalizar_colunas(df):
    """Lowercase e strip nos nomes de coluna, como o pipeline faz."""
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def ler_df_pequeno(path):
    """
    Le um CSV de tamanho pequeno/medio (LOANS ~2M, RENEGOCIACAO) para um
    DataFrame pandas usando o DuckDB como parser robusto (sample_size=-1).
    NUNCA usar para INSTALLMENTS (72M) — esse vai por DuckDB puro.
    """
    conn = duckdb.connect(database=":memory:")
    _aplicar_limites_duck(conn, path)
    try:
        df = conn.execute(
            f"SELECT * FROM read_csv_auto('{path}', sample_size=-1, ignore_errors=true)"
        ).df()
    finally:
        conn.close()
    return normalizar_colunas(df)


def _criar_view_normalizada(conn, path, view_name):
    """
    Cria uma VIEW no DuckDB sobre o CSV em disco com os nomes de coluna
    normalizados (strip + lowercase), espelhando normalizar_colunas() do pandas.
    Retorna a lista de colunas normalizadas presentes no arquivo.

    O CSV e lido direto do disco — nenhuma linha e materializada na RAM aqui.
    """
    # Configura para streaming s3
    print("  [LOG] Auto-detecion Bypass e amostra reduzida (sample_size=1000)... ", flush=True)    
    desc = conn.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto('{path}', sample_size=1000, ignore_errors=true)"
    ).fetchall()
    raw_cols  = [row[0] for row in desc]
    norm_cols = [c.strip().lower() for c in raw_cols]
    select_list = ", ".join(
        f'"{raw}" AS "{norm}"' for raw, norm in zip(raw_cols, norm_cols)
    )
    print(f"  [LOG] Disparando CREATE OR REPLACE VIEW em cima do duck... ", flush=True)    
    conn.execute(
        f"CREATE OR REPLACE VIEW {view_name} AS "
        f"SELECT {select_list} "
        f"FROM read_csv_auto('{path}', sample_size=1000, ignore_errors=true)"
    )
    return norm_cols


# ============================================================================
# CONEXAO MINIO / LOCAL
# ============================================================================

def get_s3():
    url = os.environ.get("MINIO_URL")
    if not url:
        sys.exit("ERRO: MINIO_URL env var is required (e.g. https://storage-minio.resian.com.br)")
    key = os.environ.get("MINIO_ACCESS_KEY")
    if not key:
        sys.exit("ERRO: MINIO_ACCESS_KEY env var is required — no hardcoded default")
    secret = os.environ.get("MINIO_SECRET_KEY")
    if not secret:
        sys.exit("ERRO: MINIO_SECRET_KEY env var is required — no hardcoded default")
    return boto3.client(
        "s3",
        endpoint_url=url,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=boto3.session.Config(
            signature_version="s3v4",
            connect_timeout=30,
            read_timeout=300
        )
    )


# Lista global de arquivos temporarios para limpeza no final
_TEMPS = []


def preparar_local(nome, local_dir=None, s3=None, bucket=None):
    """
    Garante que o arquivo esteja disponivel no disco local e retorna o caminho.

    Regra de ouro 1: para o MinIO/S3 o download e feito por STREAMING direto
    para um arquivo temporario (s3.download_file), nunca via obj["Body"].read().
    O arquivo temporario e registrado em _TEMPS e apagado ao final da execucao.

    Retorna (path, origem) ou (None, None) se o arquivo nao existir.
    """
    print(f"[LOG] Localizando '{nome}' (dir={local_dir}, bucket={bucket})...", flush=True)    
    if local_dir:
        path = os.path.join(local_dir, nome)
        if not os.path.exists(path):
            return None, None
        return path, f"local:{path}"

    # MinIO/S3 — streaming para o disco
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    path = tmp.name
    tmp.close()
    _TEMPS.append(path)
    s3.download_file(bucket, nome, path)
    return path, f"s3://{bucket}/{nome}"


def limpar_temps():
    for p in _TEMPS:
        try:
            os.unlink(p)
        except OSError:
            pass


# ============================================================================
# LAYER 1+2+3 (PANDAS) — para LOANS e RENEGOCIACAO (cabem na RAM)
# ============================================================================
def validar_existencia_leitura_pandas(nome, local_dir, s3, bucket):
    """
    Baixa (streaming) e le LOANS/RENEGOCIACAO em pandas. Retorna (df, origem)
    ou (None, None). Usar apenas em arquivos que cabem na memoria.
    """
    try:
        path, origem = preparar_local(nome, local_dir, s3, bucket)
    except Exception as e:
        r.erro(f"{nome} — falha ao acessar o arquivo: {e}")
        return None, None

    if path is None:
        r.erro(f"{nome} — arquivo nao encontrado")
        return None, None

    try:
        df = ler_df_pequeno(path)
    except Exception as e:
        r.erro(f"{nome} — nao foi possivel ler o CSV: {e}")
        return None, None

    n_linhas  = len(df)
    n_colunas = len(df.columns)

    if n_linhas == 0:
        if "RENEGOCIACAO" in nome.upper():
            r.aviso(f"{nome} — arquivo vazio (0 linhas de dados). Considerando 0 renegociacoes.")
        else:
            r.erro(f"{nome} — arquivo vazio (0 linhas de dados)")
        return None, None

    r.ok(f"{nome} — {n_linhas:,} linhas | {n_colunas} colunas | origem: {origem}")
    return df, origem


def validar_colunas(df, nome_arquivo, obrig_nulos_proibidos,
                    obrig_nulos_permitidos=None, opcionais=None):
    """Valida presenca de colunas e nulidade onde exigido (pandas)."""
    opcionais              = opcionais or []
    obrig_nulos_permitidos = obrig_nulos_permitidos or []
    cols_presentes = set(df.columns.tolist())
    passou = True
    n = len(df)

    for col in obrig_nulos_proibidos:
        if col not in cols_presentes:
            r.erro(f"{nome_arquivo} — coluna obrigatoria ausente: '{col}'")
            passou = False
        else:
            n_nulos = int(df[col].isna().sum())
            if n_nulos > 0:
                if col in COLS_TRATADAS_COMO_ZERO:
                    r.aviso(
                        f"{nome_arquivo} — '{col}' tem {n_nulos:,} valores nulos "
                        f"({n_nulos / n * 100:.1f}% das linhas). Sera tratado como 0."
                    )
                else:
                    r.erro(
                        f"{nome_arquivo} — '{col}' tem {n_nulos:,} valores nulos "
                        f"({n_nulos / n * 100:.1f}% das linhas)"
                    )
                    passou = False
            else:
                r.ok(f"{nome_arquivo} — '{col}' presente e sem nulos")

    for col in obrig_nulos_permitidos:
        if col not in cols_presentes:
            r.erro(f"{nome_arquivo} — coluna obrigatoria ausente: '{col}'")
            passou = False
        else:
            n_nulos = int(df[col].isna().sum())
            if n_nulos > 0:
                r.ok(
                    f"{nome_arquivo} — '{col}' presente | "
                    f"{n_nulos:,} nulos ({n_nulos / n * 100:.1f}%) — permitidos"
                )
            else:
                r.ok(f"{nome_arquivo} — '{col}' presente e sem nulos")

    for col in opcionais:
        if col not in cols_presentes:
            r.aviso(f"{nome_arquivo} — coluna opcional ausente: '{col}' (pipeline usa fallback N/I)")

    todas_conhecidas = (
        set(obrig_nulos_proibidos) |
        set(obrig_nulos_permitidos) |
        set(opcionais)
    )
    desconhecidas = cols_presentes - todas_conhecidas
    if desconhecidas:
        r.aviso(
            f"{nome_arquivo} — colunas nao mapeadas no layout: "
            f"{sorted(desconhecidas)} (ignoradas pelo pipeline)"
        )

    return passou


def validar_loans_tipos(df):
    """Valida tipos e valores logicos de LOANS.csv (pandas — base cabe na RAM)."""
    r.titulo("Tipagem e Valores Logicos (LOANS)")
    for col in ["principal", "pmt"]:
        if col in df.columns:
            v = pd.to_numeric(
                df[col].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False),
                errors="coerce",
            )
            n_inval = int(v.isna().sum())
            n_neg   = int((v < 0).sum())
            if n_inval > 0:
                r.aviso(f"LOANS — '{col}': {n_inval:,} valores nao numericos")
            if n_neg > 0:
                r.erro(f"LOANS — '{col}': {n_neg:,} valores negativos")
            if n_inval == 0 and n_neg == 0:
                r.ok(f"LOANS — '{col}' numerico e nao-negativo")

    if "num_parcelas" in df.columns:
        v = pd.to_numeric(
            df["num_parcelas"].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )
        n_bad = int((v.fillna(0) < 1).sum())
        if n_bad > 0:
            r.erro(f"LOANS — 'num_parcelas': {n_bad:,} contratos com num_parcelas < 1")
        else:
            r.ok("LOANS — 'num_parcelas' >= 1 em todos os contratos")

    if "data_aquisicao" in df.columns:
        presentes = df["data_aquisicao"].dropna()
        n_inval = sum(1 for x in presentes if parse_date_safe(x) is False)
        if n_inval > 0:
            r.erro(f"LOANS — 'data_aquisicao': {n_inval:,} datas presentes invalidas (necessaria p/ safra)")
        else:
            r.ok("LOANS — 'data_aquisicao' parseavel")


def validar_renegociacao_tipos(df):
    """Valida tipos e valores de RENEGOCIACAO.csv (pandas)."""
    if "data_renegociacao" in df.columns:
        presentes = df["data_renegociacao"].dropna()
        if len(presentes) > 0:
            n_invalidos = sum(1 for v in presentes if parse_date_safe(v) is False)
            if n_invalidos > 0:
                r.aviso(
                    f"RENEGOCIACAO — data_renegociacao: {n_invalidos} datas invalidas "
                    f"(nao bloqueante — pipeline usa apenas id_contrato_origem)"
                )
            else:
                r.ok(f"RENEGOCIACAO — data_renegociacao parseavel")

    if "valor_refinanciado" in df.columns:
        vr = pd.to_numeric(
            df["valor_refinanciado"].astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False),
            errors="coerce"
        ).dropna()
        n_neg = (vr < 0).sum()
        if n_neg > 0:
            r.aviso(
                f"RENEGOCIACAO — valor_refinanciado: {n_neg} valores negativos "
                f"(nao bloqueante — pipeline usa apenas id_contrato_origem)"
            )
        else:
            r.ok(f"RENEGOCIACAO — valor_refinanciado todos nao-negativos")


# ============================================================================
# INSTALLMENTS — VALIDACAO 100% DUCKDB (OUT-OF-CORE, 72M LINHAS)
# ============================================================================

def _num_br(coluna):
    """Expressao SQL que converte texto BR (1.234,56) em DOUBLE com TRY_CAST."""
    return (f"TRY_CAST(REPLACE(REPLACE(CAST({coluna} AS VARCHAR), '.', ''), ',', '.') "
            f"AS DOUBLE)")


def validar_colunas_duckdb(conn, view, n_total, nome,
                           obrig_nulos_proibidos,
                           obrig_nulos_permitidos, opcionais,
                           cols_presentes):
    """
    Equivalente a validar_colunas() porem as contagens de nulos vem de queries
    agregadas do DuckDB (COUNT(*) - COUNT(col)) lendo o CSV do disco. Nenhuma
    linha e trazida para a RAM.
    """
    presentes = set(cols_presentes)
    passou = True

    # Conta nulos de TODAS as colunas obrigatorias presentes em UM unico scan
    # (COUNT(*) - COUNT(col) = nulos). Out-of-core: nenhuma linha vai p/ RAM.
    cols_check = [c for c in (list(obrig_nulos_proibidos) + list(obrig_nulos_permitidos))
                  if c in presentes]
    _nulos_cache = {}
    if cols_check:
        exprs = ", ".join(f'COUNT(*) - COUNT("{c}") AS "{c}"' for c in cols_check)
        print("    -> consultando contagem de nulos (1 scan DuckDB)...", flush=True)
        row = conn.execute(f"SELECT {exprs} FROM {view}").fetchone()
        for i, c in enumerate(cols_check):
            _nulos_cache[c] = int(row[i] or 0)

    def n_nulos(col):
        return _nulos_cache.get(col, 0)

    for col in obrig_nulos_proibidos:
        if col not in presentes:
            r.erro(f"{nome} — coluna obrigatoria ausente: '{col}'")
            passou = False
        else:
            nn = n_nulos(col)
            if nn > 0:
                if col in COLS_TRATADAS_COMO_ZERO:
                    r.aviso(
                        f"{nome} — '{col}' tem {nn:,} valores nulos "
                        f"({nn / n_total * 100:.1f}% das linhas). Sera tratado como 0."
                    )
                else:
                    r.erro(
                        f"{nome} — '{col}' tem {nn:,} valores nulos "
                        f"({nn / n_total * 100:.1f}% das linhas)"
                    )
                    passou = False
            else:
                r.ok(f"{nome} — '{col}' presente e sem nulos")

    for col in obrig_nulos_permitidos:
        if col not in presentes:
            r.erro(f"{nome} — coluna obrigatoria ausente: '{col}'")
            passou = False
        else:
            nn = n_nulos(col)
            if nn > 0:
                r.ok(
                    f"{nome} — '{col}' presente | "
                    f"{nn:,} nulos ({nn / n_total * 100:.1f}%) — permitidos"
                )
            else:
                r.ok(f"{nome} — '{col}' presente e sem nulos")

    for col in opcionais:
        if col not in presentes:
            r.aviso(f"{nome} — coluna opcional ausente: '{col}' (pipeline usa fallback N/I)")

    todas_conhecidas = (
        set(obrig_nulos_proibidos) |
        set(obrig_nulos_permitidos) |
        set(opcionais)
    )
    desconhecidas = presentes - todas_conhecidas
    if desconhecidas:
        r.aviso(
            f"{nome} — colunas nao mapeadas no layout: "
            f"{sorted(desconhecidas)} (ignoradas pelo pipeline)"
        )

    return passou


def validar_installments_tipos_duckdb(conn, view, n_total, cols):
    """
    Valida tipos e valores de INSTALLMENTS.csv via agregacoes DuckDB.
    Diferente da versao pandas, valida a base INTEIRA (nao apenas uma amostra),
    pois o DuckDB faz isso num scan sem materializar as linhas.
    """

    # Tudo em UM unico scan do DuckDB (sem materializar linhas).
    sel  = []
    keys = []
    if "data_vencimento" in cols:
        sel.append(
            f"SUM(CASE WHEN data_vencimento IS NOT NULL "
            f"AND TRIM(CAST(data_vencimento AS VARCHAR)) NOT IN ('','nan','None','NaT') "
            f"AND try_strptime(CAST(data_vencimento AS VARCHAR), {DUCK_DATE_FMTS}) IS NULL "
            f"THEN 1 ELSE 0 END)"
        ); keys.append("dv_inval")
    if "data_pagamento" in cols:
        sel.append(
            f"SUM(CASE WHEN data_pagamento IS NOT NULL "
            f"AND TRIM(CAST(data_pagamento AS VARCHAR)) NOT IN ('','nan','None','NaT') "
            f"AND try_strptime(CAST(data_pagamento AS VARCHAR), {DUCK_DATE_FMTS}) IS NULL "
            f"THEN 1 ELSE 0 END)"
        ); keys.append("dp_inval")
    if "valor_parcela" in cols:
        sel.append(f"SUM(CASE WHEN {_num_br('valor_parcela')} < 0 THEN 1 ELSE 0 END)")
        keys.append("vp_neg")
    if "num_parcela" in cols:
        sel.append(f"SUM(CASE WHEN {_num_br('num_parcela')} = 1 THEN 1 ELSE 0 END)")
        keys.append("tem_p1")
    if "dias_atraso" in cols:
        sel.append(f"SUM(CASE WHEN {_num_br('dias_atraso')} < 0 THEN 1 ELSE 0 END)")
        keys.append("da_neg")

    if not sel:
        return

    print("  -> validando tipos/valores (1 scan DuckDB)...", flush=True)
    row = conn.execute(f"SELECT {', '.join(sel)} FROM {view}").fetchone()
    res = {k: int(row[i] or 0) for i, k in enumerate(keys)}

    if "dv_inval" in res:
        if res["dv_inval"] > 0:
            r.erro(f"INSTALLMENTS — data_vencimento: {res['dv_inval']:,} datas presentes invalidas (nao parseaveis)")
        else:
            r.ok("INSTALLMENTS — data_vencimento parseavel")
    if "dp_inval" in res:
        if res["dp_inval"] > 0:
            r.aviso(f"INSTALLMENTS — data_pagamento: {res['dp_inval']:,} datas presentes invalidas (nulos sao validos)")
        else:
            r.ok("INSTALLMENTS — data_pagamento parseavel (ou nula)")
    if "vp_neg" in res:
        if res["vp_neg"] > 0:
            r.aviso(f"INSTALLMENTS — valor_parcela: {res['vp_neg']:,} valores negativos")
        else:
            r.ok("INSTALLMENTS — valor_parcela sem negativos")
    if "tem_p1" in res:
        if res["tem_p1"] == 0:
            r.erro("INSTALLMENTS — nenhuma parcela num_parcela=1 (necessaria para o FPD)")
        else:
            r.ok(f"INSTALLMENTS — num_parcela=1 presente ({res['tem_p1']:,} parcelas)")
    if "da_neg" in res:
        if res["da_neg"] > 0:
            r.aviso(f"INSTALLMENTS — dias_atraso: {res['da_neg']:,} valores negativos")
        else:
            r.ok("INSTALLMENTS — dias_atraso sem negativos")


def validar_installments(path, local_dir_origem):
    """
    Pipeline completo de validacao do INSTALLMENTS via DuckDB (out-of-core).
    Retorna (conn, view, cols, n_total) com a conexao ABERTA para reuso na
    consistencia cruzada, ou (None, None, None, 0) em caso de erro/vazio.
    """
    try:
        conn = duckdb.connect(database=":memory:")
        # Limita o uso de RAM do proprio DuckDB; ele faz spill para disco se preciso.
        _aplicar_limites_duck(conn, path)
        cols = _criar_view_normalizada(conn, path, "inst")
    except Exception as e:
        r.erro(f"INSTALLMENTS.csv — nao foi possivel ler o CSV: {e}")
        return None, None, None, 0

    print("[LOG] Iniciando contagem de linhas INSTALLMENTS.csv via DuckDB...", flush=True)
    n_total = int(conn.execute("SELECT COUNT(*) FROM inst").fetchone()[0])
    print(f"[LOG] OK, {n_total} linhas contabilizadas.", flush=True)
    if n_total == 0:
        r.erro("INSTALLMENTS.csv — arquivo vazio (0 linhas de dados)")
        conn.close()
        return None, None, None, 0

    r.ok(f"INSTALLMENTS.csv — {n_total:,} linhas | {len(cols)} colunas | origem: {local_dir_origem}")

    print("[LOG] Validando Colunas...", flush=True)
    validar_colunas_duckdb(
        conn, "inst", n_total, "INSTALLMENTS",
        obrig_nulos_proibidos  = INSTALLMENTS_COLUNAS_OBRIGATORIAS_NULOS_PROIBIDOS,
        obrig_nulos_permitidos = INSTALLMENTS_COLUNAS_OBRIGATORIAS_NULOS_PERMITIDOS,
        opcionais              = INSTALLMENTS_COLUNAS_OPCIONAIS,
        cols_presentes         = cols,
    )
    print("[LOG] Validando Tipos de Datas...", flush=True)
    validar_installments_tipos_duckdb(conn, "inst", n_total, cols)
    print("[LOG] Fim de Validacoes DuckDB", flush=True)

    return conn, "inst", cols, n_total


# ============================================================================
# LAYER 4: CONSISTENCIA CRUZADA (ANTI-JOIN no DuckDB)
# ============================================================================

def validar_consistencia_cruzada(conn_inst, view_inst, cols_inst,
                                  df_loans, df_reneg):
    tem_loans = (df_loans is not None and "id_contrato" in getattr(df_loans, "columns", []))

    # ── INSTALLMENTS x LOANS (via DuckDB) ──────────────────────────────────
    if conn_inst is not None and cols_inst and "id_contrato" in cols_inst and tem_loans:
        # registra o df_loans (pequeno) como tabela no mesmo conn do installments
        conn_inst.register("loans_df", df_loans)
        conn_inst.execute("""
            CREATE OR REPLACE VIEW loans_ids AS
            SELECT DISTINCT TRIM(CAST(id_contrato AS VARCHAR)) AS id FROM loans_df
        """)
        conn_inst.execute(f"""
            CREATE OR REPLACE VIEW inst_ids AS
            SELECT DISTINCT TRIM(CAST(id_contrato AS VARCHAR)) AS id FROM {view_inst}
        """)

        n_loans = int(conn_inst.execute("SELECT COUNT(*) FROM loans_ids").fetchone()[0])

        # Contratos em INSTALLMENTS sem registro em LOANS (anti-join)
        orfaos_inst = int(conn_inst.execute("""
            SELECT COUNT(*) FROM inst_ids i
            LEFT JOIN loans_ids l USING (id)
            WHERE l.id IS NULL
        """).fetchone()[0])
        if orfaos_inst > 0:
            r.aviso(
                f"INSTALLMENTS x LOANS — {orfaos_inst:,} id_contrato em "
                f"INSTALLMENTS nao encontrados em LOANS. Parcelas serao "
                f"ignoradas pelo pipeline (INNER JOIN em load_installments)."
            )
        else:
            r.ok("INSTALLMENTS x LOANS — todos os id_contrato de INSTALLMENTS existem em LOANS")

        # Contratos em LOANS sem nenhuma parcela em INSTALLMENTS (anti-join)
        sem_parcela = int(conn_inst.execute("""
            SELECT COUNT(*) FROM loans_ids l
            LEFT JOIN inst_ids i USING (id)
            WHERE i.id IS NULL
        """).fetchone()[0])
        pct_sem = sem_parcela / n_loans * 100 if n_loans else 0
        if pct_sem > 20:
            r.erro(
                f"INSTALLMENTS x LOANS — {sem_parcela:,} contratos de LOANS "
                f"({pct_sem:.1f}%) sem nenhuma parcela em INSTALLMENTS. "
                f"Calculos de FPD, comportamento de pagamento, rolagens e EAD "
                f"serao severamente comprometidos."
            )
        elif pct_sem > 5:
            r.aviso(
                f"INSTALLMENTS x LOANS — {sem_parcela:,} contratos "
                f"({pct_sem:.1f}%) sem parcelas em INSTALLMENTS"
            )
        else:
            r.ok(
                f"INSTALLMENTS x LOANS — {100 - pct_sem:.1f}% dos contratos "
                f"tem ao menos 1 parcela em INSTALLMENTS"
            )

    # ── RENEGOCIACAO x LOANS (pandas, base pequena) ────────────────────────
    if df_reneg is not None and "id_contrato_origem" in df_reneg.columns and tem_loans:
        ids_loans  = set(df_loans["id_contrato"].astype(str).str.strip().tolist())
        ids_origem = set(df_reneg["id_contrato_origem"].astype(str).str.strip().tolist())
        orfaos_reneg = ids_origem - ids_loans
        if orfaos_reneg:
            r.aviso(
                f"RENEGOCIACAO x LOANS — {len(orfaos_reneg):,} id_contrato_origem em "
                f"RENEGOCIACAO nao encontrados em LOANS "
                f"(nao bloqueante — pipeline usa RENEGOCIACAO apenas para contagem)"
            )
        else:
            r.ok("RENEGOCIACAO x LOANS — todos os id_contrato_origem existem em LOANS")

    # ── motivo_falha_repasse vs Consignado em LOANS ────────────────────────
    if cols_inst is not None and df_loans is not None:
        if "motivo_falha_repasse" not in cols_inst:
            if "tipo_produto" in df_loans.columns:
                tem_consignado = df_loans["tipo_produto"].astype(str).str.contains(
                    "(?i)consignado", regex=True
                ).any()
                if tem_consignado:
                    r.erro(
                        "INSTALLMENTS x LOANS — coluna 'motivo_falha_repasse' ausente em "
                        "INSTALLMENTS, e existem contratos com tipo_produto Consignado em LOANS"
                    )


# ============================================================================
# VALIDACAO DE DATA_BASE vs DATAS DA CARTEIRA
# ============================================================================

def validar_data_base(df_loans, data_base_dt):
    """Valida coerencia entre data_base e datas de aquisicao (pandas, LOANS)."""
    if df_loans is None or "data_aquisicao" not in df_loans.columns:
        return

    datas_parsed = df_loans["data_aquisicao"].dropna().apply(parse_date_safe)
    datas_validas = [d for d in datas_parsed if d and d is not False]

    if not datas_validas:
        r.aviso("LOANS — nao foi possivel validar data_aquisicao vs data_base")
        return

    dt_min = min(datas_validas)
    dt_max = max(datas_validas)

    pos_data_base = [d for d in datas_validas if d > data_base_dt]
    if pos_data_base:
        r.aviso(
            f"LOANS x data_base — {len(pos_data_base):,} contratos com "
            f"data_aquisicao posterior a {data_base_dt.strftime('%Y-%m')}. "
            f"Serao incluidos no pipeline mas suas parcelas vencidas apos a "
            f"data_base serao ignoradas."
        )
    else:
        r.ok(
            f"LOANS x data_base — todas as data_aquisicao anteriores ou iguais "
            f"a data_base ({data_base_dt.strftime('%Y-%m')})"
        )

    r.ok(
        f"LOANS — safras: {dt_min.strftime('%Y-%m')} ate {dt_max.strftime('%Y-%m')} "
        f"| {len(set(d.strftime('%Y-%m') for d in datas_validas))} safras distintas"
    )


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Resian — Passo 0: Validacao de Layout das Bases de Entrada"
    )
    parser.add_argument(
        "--data_base",
        default="",
        help="Data-base no formato AAAA-MM (ex: 2026-05). "
             "Opcional mas recomendado para validacao de datas."
    )
    parser.add_argument(
        "--bucket",
        default="clientes-uploads",
        help="Nome do bucket MinIO (default: clientes-uploads)"
    )
    parser.add_argument(
        "--local_dir",
        default=None,
        help="Diretorio local com os arquivos CSV (alternativa ao MinIO)"
    )
    args = parser.parse_args()

    data_base_dt = None
    if args.data_base:
        try:
            data_base_dt = datetime.strptime(args.data_base[:7], "%Y-%m")
        except ValueError:
            print(f"[ERRO] data_base invalida: '{args.data_base}'. Use o formato AAAA-MM.")
            sys.exit(1)

    print("=" * 60)
    print("RESIAN — PASSO 0: VALIDACAO DE LAYOUT DAS BASES (out-of-core)")
    if data_base_dt:
        print(f"Data-base: {args.data_base}")
    if args.local_dir:
        print(f"Modo: arquivos locais em '{args.local_dir}'")
    else:
        print(f"Modo: MinIO bucket '{args.bucket}'")
    print("=" * 60)

    if not args.local_dir and not args.bucket:
        print("\n[ERRO] Voce deve fornecer no minimo a flag --local_dir ou a flag --bucket.\n")
        sys.exit(1)

    s3 = None
    if not args.local_dir and args.bucket:
        try:
            s3 = get_s3()
            s3.list_objects_v2(Bucket=args.bucket, MaxKeys=1)
            print(f"\n[OK] Conexao com MinIO estabelecida (bucket: {args.bucket})\n")
        except Exception as e:
            print(f"\n[ERRO] Nao foi possivel conectar ao MinIO: {e}\n")
            print("       Verifique as credenciais ou use --local_dir para rodar com arquivos locais.\n")
            sys.exit(1)

    conn_inst  = None
    cols_inst  = None
    try:
        # ── LOANS ──────────────────────────────────────────────────────────
        r.titulo("LOANS.csv")
        print("[LOG] Processando LOANS...", flush=True)        
        df_loans, origem_loans = validar_existencia_leitura_pandas(
            "LOANS.csv", args.local_dir, s3, args.bucket
        )
        if df_loans is not None:
            validar_colunas(
                df_loans, "LOANS",
                obrig_nulos_proibidos  = LOANS_COLUNAS_OBRIGATORIAS_NULOS_PROIBIDOS,
                obrig_nulos_permitidos = LOANS_COLUNAS_OBRIGATORIAS_NULOS_PERMITIDOS,
                opcionais              = LOANS_COLUNAS_OPCIONAIS,
            )
            validar_loans_tipos(df_loans)
            if data_base_dt:
                validar_data_base(df_loans, data_base_dt)

        # ── INSTALLMENTS (DuckDB out-of-core) ──────────────────────────────
        r.titulo("INSTALLMENTS.csv")
        print("[LOG] Processando INSTALLMENTS...", flush=True)        
        try:
            path_inst, origem_inst = preparar_local(
                "INSTALLMENTS.csv", args.local_dir, s3, args.bucket
            )
        except Exception as e:
            path_inst, origem_inst = None, None
            r.erro(f"INSTALLMENTS.csv — falha ao acessar o arquivo: {e}")

        if path_inst:
            conn_inst, _view_inst, cols_inst, _n_inst = validar_installments(
                path_inst, origem_inst
            )
        elif origem_inst is None:
            # local_dir definido e arquivo ausente (sem excecao)
            r.erro("INSTALLMENTS.csv — arquivo nao encontrado")

        # ── RENEGOCIACAO (pandas) ──────────────────────────────────────────
        r.titulo("RENEGOCIACAO.csv")
        print("[LOG] Processando RENEG...", flush=True)        
        df_reneg, _ = validar_existencia_leitura_pandas(
            "RENEGOCIACAO.csv", args.local_dir, s3, args.bucket
        )
        if df_reneg is not None:
            validar_colunas(
                df_reneg, "RENEGOCIACAO",
                obrig_nulos_proibidos  = RENEGOCIACAO_COLUNAS_OBRIGATORIAS_NULOS_PROIBIDOS,
                obrig_nulos_permitidos = RENEGOCIACAO_COLUNAS_OBRIGATORIAS_NULOS_PERMITIDOS,
            )
            validar_renegociacao_tipos(df_reneg)

        # ── CONSISTENCIA CRUZADA ───────────────────────────────────────────
        r.titulo("CONSISTENCIA CRUZADA")
        print("[LOG] Processando Cruzamentos...", flush=True)        
        validar_consistencia_cruzada(
            conn_inst, "inst" if conn_inst else None, cols_inst,
            df_loans, df_reneg
        )

    finally:
        if conn_inst is not None:
            conn_inst.close()
        limpar_temps()

    # ── RELATORIO FINAL ────────────────────────────────────────────────────
    print()
    r.imprimir()

    print()
    print("=" * 60)
    print(f"RESULTADO FINAL")
    print(f"  Erros bloqueantes : {r.erros}")
    print(f"  Avisos            : {r.avisos}")

    if r.erros == 0:
        print()
        print("  APROVADO — pipeline pode ser executado.")
        print("=" * 60)
        sys.exit(0)
    else:
        print()
        print("  REPROVADO — corrija os erros antes de rodar o pipeline.")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()

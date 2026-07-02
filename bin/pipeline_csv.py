#!/usr/bin/env python3
"""
Resian Consultoria — Pipeline de Extração de Dados para Valuation
Versão: 2.0 — Saída em CSV para geração de relatório no Claude.ai

Lê os arquivos do MinIO (clientes-uploads), executa todos os cálculos
de valuation e salva um conjunto de CSVs prontos para análise.

Uso:
    python3 pipeline_csv.py \
        --cliente "Nome_do_Cliente" \
        --data_base 2026-05 \
        --output_dir ./output

Variáveis de ambiente obrigatórias:
    MINIO_URL        = https://storage-minio.resian.com.br
    MINIO_ACCESS_KEY = client-valuation
    MINIO_SECRET_KEY = valuation-2024

Dependências:
    pip install boto3 pandas numpy python-dateutil --break-system-packages
"""

import argparse
import json
import logging
import os
import tempfile
import warnings
from collections import defaultdict, namedtuple
from datetime import datetime
from pathlib import Path

import boto3
import duckdb
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("resian.pipeline")

# ════════════════════════════════════════════════════════════════════════════
# CONSTANTES METODOLÓGICAS
# ════════════════════════════════════════════════════════════════════════════

CHUNK_SIZE   = 50_000   # linhas por chunk na leitura de CSVs grandes
CORTE_MOBS   = 2        # mobs finais descartados para evitar contaminação
DECAY_PERDA  = 0.85     # fator de decaimento do shape de perda
DECAY_RECEITA_BASE = 0.92  # decay de receita do grupo "neutro" (perda = média da carteira)
DECAY_RECEITA_K    = 1.2   # sensibilidade do decay de receita à perda média do grupo
                            # (ver _calc_shapes_from_accumulated: decay = BASE^((perda_grupo/perda_carteira)^K).
                            # k=1,2 é o menor valor que garante separação estável entre os três
                            # grupos na hierarquia de receita projetada baixo>médio>alto; calibrado
                            # contra LOANS/INSTALLMENTS reais em 2026-06, documentado e auditável —
                            # não há calibração 100% empírica possível pois k governa exclusivamente
                            # a região da curva além do max_mob_limpo, sem dado realizado.)
TAXAS_VPL    = [0.01, 0.015]  # 1,0% a.m. e 1,5% a.m.

# ════════════════════════════════════════════════════════════════════════════
# ESTRUTURA DE BAIXO CONSUMO DE RAM
# ════════════════════════════════════════════════════════════════════════════
# Cada parcela do INSTALLMENTS (até 72M de linhas) é guardada como namedtuple.
# Um namedtuple NÃO carrega um __dict__ por instância (usa __slots__ via tuple),
# então consome ~3x a 5x menos RAM que um dict Python equivalente. Em 72M de
# parcelas isso representa a diferença entre ~22GB (dicts) e ~6-7GB (namedtuples).
# Acesso sempre por notação de ponto: p.dt_pago, p.valor_pago, etc.
Parcela = namedtuple(
    "Parcela",
    ["num_parcela", "valor_parcela", "valor_pago", "dt_venc", "dt_pago", "dias_atraso"]
)

# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def parse_date(s):
    if pd.isna(s) or str(s).strip() in ("", "nan", "None", "NaT"):
        return None
    s = str(s).strip()[:19]
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def parse_float(s):
    try:
        s = str(s).strip()
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'):
                return float(s.replace('.', '').replace(',', '.'))
            else:
                return float(s.replace(',', ''))
        elif ',' in s:
             return float(s.replace(',', '.'))
        else:
             return float(s)
    except Exception:
        return 0.0


def fmt_brl(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "R$ 0,00"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(v, dec=2):
    return f"{v * 100:.{dec}f}%".replace(".", ",")


def fmt_int(v):
    return f"{int(v):,}".replace(",", ".")


def ym(dt):
    return dt.strftime("%Y-%m")


def months_diff(d_from, d_to):
    return (d_to.year - d_from.year) * 12 + (d_to.month - d_from.month)


# ════════════════════════════════════════════════════════════════════════════
# CONEXÃO MINIO
# ════════════════════════════════════════════════════════════════════════════

def get_s3():
    url    = os.environ.get("MINIO_URL", "https://storage-minio.resian.com.br")
    key    = os.environ.get("MINIO_ACCESS_KEY", "client-valuation")
    secret = os.environ.get("MINIO_SECRET_KEY", "valuation-2024")
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


def duck_connect(local_dir=None):
    """
    Conexão DuckDB com limite de memória e SPILL para disco.

    Sem isto, o DuckDB assume ~80% da RAM da VM como memory_limit; com a leitura
    do INSTALLMENTS (72M linhas) + JOIN ele estoura os 24GB e, como a VM Oracle
    NÃO tem swap, o kernel congela a máquina (foi o que exigiu o reboot).
    Aqui limitamos a RAM do motor e mandamos o overflow para o disco (138GB livres).
    Ajuste memory_limit conforme a RAM realmente livre na hora da execução.
    """
    conn = duckdb.connect(database=":memory:")
    # Registra parse_float() como UDF para que _sql_num/_sql_int usem exatamente
    # a mesma lógica de parsing que o resto do pipeline (ponto BR como separador
    # de milhar tratado condicionalmente). Elimina a duplicação que causava
    # inflação de ~100x em perda_acumulada/receita_acumulada quando valor_parcela
    # chegava no Parquet em formato padrão ("1234.56") — o REPLACE fixo anterior
    # removia o ponto decimal, transformando "1821.11" em 182111.
    conn.create_function("parse_float_br", parse_float, ["VARCHAR"], "DOUBLE")
    conn.execute("PRAGMA memory_limit='6GB'")
    conn.execute("PRAGMA threads=3")
    conn.execute("PRAGMA preserve_insertion_order=false")
    tmpdir = os.path.join(local_dir or tempfile.gettempdir(), "duck_tmp")
    try:
        os.makedirs(tmpdir, exist_ok=True)
        conn.execute(f"PRAGMA temp_directory='{tmpdir}'")
        conn.execute("PRAGMA max_temp_directory_size='120GB'")
    except Exception:
        pass
    return conn


def read_csv_minio(s3, bucket, key, sep=";", encoding="utf-8", chunksize=None):
    """
    Regra de ouro 1: NUNCA usar obj["Body"].read() (carrega o arquivo inteiro
    na RAM). O download é feito por streaming direto para um arquivo temporário
    no disco via s3.download_file e o arquivo é apagado ao final.

    Usado apenas por arquivos pequenos (META.csv). Para LOANS/INSTALLMENTS o
    motor é o DuckDB lendo o CSV direto do disco.
    """
    # Offline mode (--bucket ""): no S3 client, so don't attempt a network call
    # — raise immediately so the caller's try/except falls back to defaults.
    if s3 is None:
        raise RuntimeError("no S3 client configured (offline mode)")
    log.info(f"  Baixando s3://{bucket}/{key} via streaming para o disco …")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    path = tmp.name
    tmp.close()
    try:
        s3.download_file(bucket, key, path)
        df = duckdb.read_csv(path).df()
        return df
    except:
        raise
# INGESTÃO
# ════════════════════════════════════════════════════════════════════════════

def load_meta(s3, bucket, cliente_arg, data_base_arg):
    # Tenta separador ; primeiro, depois ,
    meta = None
    for sep in (";", ","):
        try:
            df = read_csv_minio(s3, bucket, "META.csv", sep=sep)
            df.columns = [c.strip().lower() for c in df.columns]
            if len(df) > 0:
                row = df.iloc[0]
                # Bug 3: data_base pode vir como "2026-05-31" — truncar para "YYYY-MM"
                db_raw = str(row.get("data_base", data_base_arg or "")).strip()[:7]
                meta = {
                    "nome_cliente": str(row.get("nome_cliente", cliente_arg or "Cliente")).strip(),
                    "data_base":    db_raw,
                    "analista":     str(row.get("analista",     "Resian")).strip(),
                    "produto":      str(row.get("produto",      "Crédito")).strip(),
                }
                break
        except Exception:
            continue
    if meta is None:
        meta = {
            "nome_cliente": cliente_arg or "Cliente",
            "data_base":    (data_base_arg or "")[:7],
            "analista":     "Resian",
            "produto":      "Crédito",
        }
    # Argumentos de linha de comando têm prioridade
    if cliente_arg:
        meta["nome_cliente"] = cliente_arg
    if data_base_arg:
        meta["data_base"] = data_base_arg[:7]
    return meta


def load_loans(s3, bucket, local_dir=None):
    log.info("Carregando LOANS.csv via DuckDB...")
    import os
    if local_dir and os.path.exists(f"{local_dir}/LOANS.csv"):
        path = f"{local_dir}/LOANS.csv"
    else:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        path = tmp.name
        tmp.close()
        log.info("  Baixando LOANS.csv via streaming para o disco temporário...")
        s3.download_file(bucket, "LOANS.csv", path)

    import duckdb
    conn = duck_connect(local_dir)
    try:
        df_loans = conn.execute(
            f"SELECT * FROM read_csv('{path}', auto_detect=true, sample_size=-1, ignore_errors=true, all_varchar=true)"
        ).df()
    finally:
        conn.close()

    if "unique id" in df_loans.columns and "id_contrato" not in df_loans.columns:
        df_loans.rename(columns={"unique id": "id_contrato"}, inplace=True)
    if "[srm] codigo operacao" in df_loans.columns and "id_contrato" not in df_loans.columns:
        df_loans.rename(columns={"[srm] codigo operacao": "id_contrato"}, inplace=True)
    
    df_loans.columns = [c.strip().lower() for c in df_loans.columns]
    log.info(f"  {len(df_loans):,} contratos carregados")

    # Stripping space from id_contrato right after Pandas loading
    if "id_contrato" in df_loans.columns:
        df_loans["id_contrato"] = df_loans["id_contrato"].astype(str).str.strip()
    
    # CRÍTICO: normalizar id_contrato para string trimada na origem.
    # As parcelas (inst_by_id) são chaveadas por TRIM(CAST(... AS VARCHAR)) na
    # query DuckDB, ou seja, STRINGS. Se aqui o id_contrato ficar numérico, o
    # loan_map.set_index(...).get(id_str) em calc_fpd e _build_buckets_mes
    # (rolagens) falha para TODOS os contratos (int 123 != str "123"), os
    # contratos são pulados, os buckets ficam zerados e o rolagens.csv sai
    # inteiro vazio. Normalizar aqui mantém todas as chaves consistentes com o
    # str(loan["id_contrato"]).strip() já usado nas demais funções.
    if "id_contrato" in df_loans.columns:
        df_loans["id_contrato"] = df_loans["id_contrato"].astype(str).str.strip()

    for col in ["principal", "pmt", "num_parcelas", "dias_maior_atraso"]:
        if col in df_loans.columns:
            df_loans[col] = df_loans[col].apply(parse_float)

    if "taxa_cliente" in df_loans.columns:
        df_loans["taxa_cliente"] = pd.to_numeric(df_loans["taxa_cliente"].astype(str).str.replace(",", ".", regex=False), errors="coerce").fillna(0.0)
        df_loans["taxa_cliente"] = df_loans["taxa_cliente"].apply(lambda x: x / 100 if x > 1.0 else x)

    df_loans["data_aquisicao"] = df_loans.get("data_aquisicao", pd.Series(dtype=str)).apply(parse_date)
    df_loans["safra"] = df_loans["data_aquisicao"].apply(lambda d: ym(d) if d else "N/I")

    for col in ["rating_concessao", "bhv_m", "especie_beneficio", "situacao_beneficio", "canal_originacao", "uf", "regiao", "profissao"]:
        if col not in df_loans.columns:
            df_loans[col] = "N/I"
        else:
            df_loans[col] = df_loans[col].fillna("N/I").astype(str).replace(["nan", "None", "", "NaN"], "N/I")

    if "regiao" in df_loans.columns and "uf" not in df_loans.columns:
        df_loans["uf"] = df_loans["regiao"]

    if "id_contrato" in df_loans.columns:
        df_loans = df_loans.drop_duplicates(subset=["id_contrato"])

    if not local_dir:
        try: os.unlink(path)
        except: pass
    return df_loans


def load_renegociacoes(s3, bucket, local_dir=None):
    """
    Retorna o conjunto de id_contrato_origem renegociados.

    Regra de ouro 1: download por streaming para o disco (sem .read()).
    Em vez de materializar um DataFrame pandas inteiro, o DuckDB extrai apenas
    a coluna necessária (DISTINCT) lendo o CSV direto do disco. Mesmo com
    milhões de renegociações, só os IDs únicos chegam à RAM.
    """
    path       = None
    tmp_criado = False
    try:
        if local_dir and os.path.exists(f"{local_dir}/RENEGOCIACAO.csv"):
            path = f"{local_dir}/RENEGOCIACAO.csv"
        else:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
            path = tmp.name
            tmp.close()
            tmp_criado = True
            log.info("  Baixando RENEGOCIACAO.csv via streaming para o disco temporário...")
            s3.download_file(bucket, "RENEGOCIACAO.csv", path)

        conn = duck_connect(local_dir)
        try:
            rows = conn.execute(f"""
                SELECT DISTINCT TRIM(CAST(id_contrato_origem AS VARCHAR)) AS id
                FROM read_csv_auto('{path}', sample_size=-1, ignore_errors=true)
            """).fetchall()
        finally:
            conn.close()
        return set(r[0] for r in rows if r[0] is not None)
    except Exception:
        log.warning("  RENEGOCIACAO.csv não encontrado — assumindo zero renegociações")
        return set()

def load_installments(s3, bucket, data_base, df_loans, local_dir=None):
    if local_dir:
        path = f"{local_dir}/INSTALLMENTS.csv"
    else:
        raise ValueError("O pipeline streaming deve garantir diretorio local para base!")

    # Conversão única CSV -> Parquet. O sniff/parse do CSV de 72M linhas (caro e
    # "silencioso") acontece UMA vez; cada lote depois lê o Parquet (colunar, com
    # column/predicate pushdown) em vez de re-escanear os 4.9GB do CSV a cada lote.
    parquet_path = f"{local_dir}/INSTALLMENTS.parquet"
    if not os.path.exists(parquet_path):
        log.info("  Convertendo INSTALLMENTS.csv -> Parquet (uma única vez, com spill p/ disco)...")
        conv = duck_connect(local_dir)
        try:
            conv.execute(f"""
                COPY (
                    SELECT * FROM read_csv_auto('{path}', sample_size=-1, ignore_errors=true)
                ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        finally:
            conv.close()
        log.info("  Parquet pronto (lotes seguintes leem deste arquivo).")
            
    print(f"      [CHUNKING INSTALLMENTS DO LOTE] carregando via scan do Parquet com filter em {len(df_loans)} loans... \n", flush=True)

    # Regra de ouro 2: o DuckDB mapeia o DataFrame 'df_loans' nativamente como
    # tabela SQL. O cruzamento é um INNER JOIN relacional direto no motor do
    # DuckDB, sem nenhuma cláusula WHERE id IN (...) com strings gigantes.
    q = f"""
    SELECT 
        i.id_contrato as id_contrato,
        data_vencimento,
        data_pagamento,
        CAST(REPLACE(REPLACE(CAST(i.num_parcela AS VARCHAR), '.', ''), ',', '.') AS INTEGER) as num_parcela,
        CAST(REPLACE(REPLACE(CAST(i.valor_parcela AS VARCHAR), '.', ''), ',', '.') AS DOUBLE) as valor_parcela,
        CAST(REPLACE(REPLACE(CAST(i.valor_pago AS VARCHAR), '.', ''), ',', '.') AS DOUBLE) as valor_pago,
        CAST(REPLACE(REPLACE(CAST(i.dias_atraso AS VARCHAR), '.', ''), ',', '.') AS DOUBLE) as dias_atraso
    FROM read_parquet('{parquet_path}') i
    INNER JOIN df_loans l
        ON i.id_contrato = l.id_contrato
    """
    
    log.info("  Executando query analítica com INNER JOIN relacional no DuckDB...")
    conn = duck_connect(local_dir)
    inst_by_id = defaultdict(list)
    linhas_uteis = 0
    try:
        cursor = conn.cursor()
        cursor.execute(q)

        # Regra de ouro 3: NUNCA usar .df() (transformaria 72M de linhas num
        # DataFrame pandas e estouraria a RAM) nem .iterrows(). Iteramos linha a
        # linha direto no cursor do DuckDB, que mantém apenas um buffer pequeno.
        log.info("  Populando parcelas via cursor (fetchmany, namedtuple, baixo consumo de RAM)...")
        while True:
            rows = cursor.fetchmany(50_000)
            if not rows:
                break
            for row in rows:
                dt_venc = parse_date(row[1])
                if dt_venc is None:
                    continue

                dt_pago_raw = str(row[2]).strip() if row[2] is not None else ""
                dt_pago = parse_date(dt_pago_raw) if dt_pago_raw not in ("", "nan", "None", "Nat", "NaT") else None

                # Regra de ouro 4: cada linha vira um namedtuple Parcela (leve),
                # nunca um dict Python.
                inst_by_id[row[0]].append(Parcela(
                    num_parcela   = row[3] if row[3] is not None else 0,
                    valor_parcela = row[4] if row[4] is not None else 0.0,
                    valor_pago    = row[5] if row[5] is not None else 0.0,
                    dt_venc       = dt_venc,
                    dt_pago       = dt_pago,
                    dias_atraso   = row[6] if row[6] is not None else 0.0,
                ))
                linhas_uteis += 1
    finally:
        conn.close()
        if tmp_criado:
            try: os.unlink(path)
            except: pass

    log.info(f"  {linhas_uteis:,} linhas úteis processadas | {len(inst_by_id):,} contratos mapeados")
    return inst_by_id


# ════════════════════════════════════════════════════════════════════════════
# CÁLCULOS
# ════════════════════════════════════════════════════════════════════════════

# ── 1. KPIs principais ───────────────────────────────────────────────────────

def calc_kpis(loans, renegociacoes):
    n   = len(loans)
    pt  = loans["principal"].sum()
    return {
        "contratos":       n,
        "principal_total": pt,
        "ticket_medio":    pt / n if n else 0,
        "taxa_media_pct":  (pd.to_numeric(loans["taxa_cliente"], errors='coerce').fillna(0).apply(lambda x: x / 100 if x > 1.0 else x) * loans["principal"]).sum() / pt
                           if pt > 0 else 0,
        "prazo_medio":     loans["num_parcelas"].mean(),
        "over30_n":        int((pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) > 30).sum()),
        "over60_n":        int((pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum()),
        "over90_n":        int((pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) > 90).sum()),
        "over30_pct":      (pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) > 30).mean(),
        "over60_pct":      (pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) > 60).mean(),
        "over90_pct":      (pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) > 90).mean(),
        "renegociacoes":   len(renegociacoes),
        "safra_min":       sorted(loans["safra"].unique())[0]
                           if len(loans) > 0 else "",
        "safra_max":       sorted(loans["safra"].unique())[-1]
                           if len(loans) > 0 else "",
    }


# ── 2. Faixas de atraso ──────────────────────────────────────────────────────

def calc_faixas_atraso(loans):
    n = len(loans)
    faixas = [
        ("Corrente (0 dias)",  0,   0),
        ("1 a 30 dias",        1,   30),
        ("31 a 60 dias",       31,  60),
        ("61 a 90 dias",       61,  90),
        ("Acima de 90 dias",   91,  999999),
    ]
    rows  = []
    acum  = 0.0
    for label, lo, hi in faixas:
        if lo == 0 and hi == 0:
            mask = loans["dias_maior_atraso"].isna() | (pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) <= 0)
        else:
            mask = (pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) >= lo) & \
                   (pd.to_numeric(loans["dias_maior_atraso"], errors='coerce').fillna(0) <= hi)
        cnt  = int(mask.sum())
        pct  = cnt / n if n > 0 else 0
        acum += pct
        rows.append({
            "faixa":       label,
            "contratos_n": cnt,
            "pct_carteira": round(pct * 100, 2),
            "acumulado":   round(acum * 100, 2),
        })
    return rows


# ── 3. Faixas de prazo ───────────────────────────────────────────────────────

def calc_faixas_prazo(loans):
    faixas = [
        ("1-12 meses",  1,   12),
        ("13-24 meses", 13,  24),
        ("25-36 meses", 25,  36),
        ("37-48 meses", 37,  48),
        ("49-60 meses", 49,  60),
        ("61-72 meses", 61,  72),
        (">72 meses",   73,  9999),
    ]
    rows = []
    for label, lo, hi in faixas:
        sub  = loans[(loans["num_parcelas"] >= lo) & (loans["num_parcelas"] <= hi)]
        n    = len(sub)
        princ= sub["principal"].sum()
        o60  = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum())
        rows.append({
            "faixa":        label,
            "contratos_n":  n,
            "principal":    round(princ, 2),
            "over60_n":     o60,
            "over60_pct":   round(o60 / n * 100, 2) if n > 0 else 0,
        })
    return rows


# ── 4. Rating ────────────────────────────────────────────────────────────────

def calc_rating(loans):
    pt    = loans["principal"].sum()
    todos = not (loans["rating_concessao"] == "N/I").any()
    rows  = []
    for r in sorted(loans["rating_concessao"].unique()):
        sub  = loans[loans["rating_concessao"] == r]
        n    = len(sub)
        p    = sub["principal"].sum()
        o60  = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum())
        # Taxa ponderada pelo principal (nunca média simples)
        taxas = pd.to_numeric(sub["taxa_cliente"], errors='coerce').fillna(0).apply(lambda x: x / 100 if x > 1.0 else x)
        taxa = (taxas * sub["principal"]).sum() / p if p > 0 else 0
        rows.append({
            "rating":       r,
            "contratos_n":  n,
            "principal":    round(p, 2),
            "over60_n":     o60,
            "over60_pct":   round(o60 / n * 100, 2) if n > 0 else 0,
            "taxa_pond_pct":round(taxa * 100, 4),
            "part_pct":     round(p / pt * 100, 2) if pt > 0 else 0,
        })
    return rows, todos


# ── 5. UF ────────────────────────────────────────────────────────────────────

def calc_uf(loans):
    pt   = loans["principal"].sum()
    rows = []
    for uf, sub in loans.groupby("uf"):
        n   = len(sub)
        p   = sub["principal"].sum()
        o60 = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum())
        rows.append({
            "uf":          uf,
            "contratos_n": n,
            "principal":   round(p, 2),
            "part_pct":    round(p / pt * 100, 2) if pt > 0 else 0,
            "over60_n":    o60,
            "over60_pct":  round(o60 / n * 100, 2) if n > 0 else 0,
        })
    return sorted(rows, key=lambda r: -r["principal"])


# ── 6. Perfil / Espécie ──────────────────────────────────────────────────────

def calc_perfil(loans, col, label):
    pt   = loans["principal"].sum()
    rows = []
    for val, sub in loans.groupby(col):
        n   = len(sub)
        p   = sub["principal"].sum()
        o60 = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum())
        rows.append({
            label:         val,
            "contratos_n": n,
            "principal":   round(p, 2),
            "part_pct":    round(p / pt * 100, 2) if pt > 0 else 0,
            "over60_n":    o60,
            "over60_pct":  round(o60 / n * 100, 2) if n > 0 else 0,
        })
    return sorted(rows, key=lambda r: -r["principal"])


# ── 7. Cruzamentos (Espécie × Situação, Canal × Situação) ───────────────────

def calc_cruzamento(loans, col_grupo, col_sit, label_grupo):
    pt   = loans["principal"].sum()
    rows = []
    for (grp, sit), sub in loans.groupby([col_grupo, col_sit]):
        n   = len(sub)
        p   = sub["principal"].sum()
        o60 = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum())
        o90 = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 90).sum())
        rows.append({
            label_grupo:   str(grp),
            "situacao":    str(sit),
            "contratos_n": n,
            "principal":   round(p, 2),
            "part_pct":    round(p / pt * 100, 2) if pt > 0 else 0,
            "over60_n":    o60,
            "over60_pct":  round(o60 / n * 100, 2) if n > 0 else 0,
            "over90_pct":  round(o90 / n * 100, 2) if n > 0 else 0,
        })
    return sorted(rows, key=lambda r: -r["principal"])


# ── 8. FPD por safra ─────────────────────────────────────────────────────────

def calc_fpd(loans, inst_by_id):
    print("      -> calc_fpd", flush=True)
    """
    FPD calculado a partir do INSTALLMENTS (parcela num_parcela == 1).
    FPD = não paga OU dias_atraso > 30.
    """
    loan_map = loans.set_index("id_contrato").to_dict(orient="index")
    fpd_safra = defaultdict(lambda: {
        "total_n": 0, "total_val": 0.0,
        "fpd_n":   0, "fpd_val":   0.0,
        "sem_p1":  0,
    })

    for id_contrato, insts in inst_by_id.items():
        loan = loan_map.get(id_contrato)
        if loan is None:
            continue
        safra = loan["safra"]
        princ = loan["principal"]
        fpd_safra[safra]["total_n"]   += 1
        fpd_safra[safra]["total_val"] += princ

        primeiras = [i for i in insts if i.num_parcela == 1]
        if not primeiras:
            fpd_safra[safra]["sem_p1"] += 1
            continue

        p1     = primeiras[0]
        is_fpd = (p1.dt_pago is None) or (p1.dias_atraso > 30)
        if is_fpd:
            fpd_safra[safra]["fpd_n"]   += 1
            fpd_safra[safra]["fpd_val"] += princ

    rows = []
    for safra in sorted(fpd_safra):
        d    = fpd_safra[safra]
        tn   = d["total_n"]
        tv   = d["total_val"]
        fn   = d["fpd_n"]
        fv   = d["fpd_val"]
        qpct = fn / tn * 100   if tn > 0 else 0
        vpct = fv / tv * 100   if tv > 0 else 0

        if qpct > 80:
            sig = "Dado incompleto"
        elif qpct > 20:
            sig = "Alto"
        elif qpct > 10:
            sig = "Elevado"
        elif qpct > 5:
            sig = "Moderado"
        else:
            sig = "Adequado"

        rows.append({
            "safra":        safra,
            "contratos_n":  tn,
            "fpd_n":        fn,
            "fpd_qtd_pct":  round(qpct, 2),
            "fpd_val_pct":  round(vpct, 2),
            "sem_parcela1": d["sem_p1"],
            "sinalizacao":  sig,
        })
    return rows


# ── 9. Vencimentário ─────────────────────────────────────────────────────────

def calc_vencimentario(loans, data_base):
    venc = defaultdict(float)
    for l in loans.to_dict(orient="records"):
        if l["data_aquisicao"] is None:
            continue
        for p in range(1, int(l["num_parcelas"]) + 1):
            dt = l["data_aquisicao"] + relativedelta(months=p)
            if dt > data_base:
                venc[ym(dt)] += l["pmt"]
    return [
        {"mes": mes, "pmt_total": round(v, 2)}
        for mes, v in sorted(venc.items())
    ]


# ── 10. Comportamento de pagamento por safra ──────────────────────────────────

def calc_comportamento_pagamento(loans, inst_by_id, data_base):
    # DESATIVADA (era um BYPASS que retornava []). Substituída por
    # calc_comportamento_pagamento_duck (agregação out-of-core no DuckDB).
    raise NotImplementedError("Use calc_comportamento_pagamento_duck (DuckDB).")


# ── 11. Rolagens ─────────────────────────────────────────────────────────────

def bucket_dpd(dpd):
    # Removendo tolerância de 5 dias do 'corrente' conforme padrão de mercado de rolagens contábeis estritas?
    # Não, o cliente pediu para voltar para a regra de `dpd <= 5` como corrente! Mas as faixas subsequentes
    # devem iniciar em 6 e ir até 30, porém ele chamou de '5-30' por simplicidade (ou seja, de 5 até 30 na nomenclatura dele)
    # Importante: Como ele pede pra garantir as faixas "5-30", o nome fica f5_30, mas o corte é 5.
    if dpd <= 5:   return "corrente"
    if dpd <= 30:  return "f5_30"
    if dpd <= 60:  return "f31_60"
    if dpd <= 90:  return "f61_90"
    if dpd <= 120: return "f91_120"
    if dpd <= 150: return "f121_150"
    if dpd <= 180: return "f151_180"
    return "f180p"


BUCKETS = ["corrente", "f5_30", "f31_60", "f61_90",
           "f91_120", "f121_150", "f151_180", "f180p"]


def _dt_to_date(d):
    """Converte datetime/date/str para date. Definida fora dos loops para eficiência."""
    import datetime as _dt_mod
    if d is None:
        return None
    if isinstance(d, _dt_mod.datetime):
        return d.date()
    if hasattr(d, 'date') and callable(d.date):
        return d.date()
    if isinstance(d, _dt_mod.date):
        return d
    if isinstance(d, str):
        parsed = parse_date(d)
        return parsed.date() if parsed else None
    return None


def _bucket_dpd(dpd):
    if dpd <= 5:   return "corrente"
    if dpd <= 30:  return "f5_30"
    if dpd <= 60:  return "f31_60"
    if dpd <= 90:  return "f61_90"
    if dpd <= 120: return "f91_120"
    if dpd <= 150: return "f121_150"
    if dpd <= 180: return "f151_180"
    return "f180p"


def _build_buckets_mes(loans_batch, inst_by_id_batch, meses_dt):
    """
    Computa saldos por bucket DPD em cada mês para um lote de contratos.
    Retorna dict {ym: {bucket: saldo}} que pode ser somado entre lotes.
    """
    import datetime as _dt_mod
    loan_map = loans_batch.set_index("id_contrato").to_dict(orient="index")
    buckets_mes = {
        ym(cal_dt): {"corrente": 0.0, "f5_30": 0.0, "f31_60": 0.0,
                     "f61_90": 0.0, "f91_120": 0.0, "f121_150": 0.0,
                     "f151_180": 0.0, "f180p": 0.0}
        for cal_dt in meses_dt
    }

    # DESATIVADA (este corpo era um BYPASS que retornava buckets zerados).
    # As rolagens agora são calculadas em calc_rolagens_duck (DuckDB out-of-core).
    raise NotImplementedError("Use calc_rolagens_duck (DuckDB).")
        

    # CODIGO NUNCA EXECUTADO no bypass
    for id_contrato, insts in inst_by_id_batch.items():
        loan = loan_map.get(id_contrato)       # CORRIGIDO: era loan = None
        if loan is None or not isinstance(loan, dict):
            continue

        pmt_val    = float(loan.get("pmt", 0) or 0)
        num_parcelas = int(loan.get("num_parcelas", 0) or 0)

        for cal_dt in meses_dt:
            cal_dt_date = cal_dt.date() if isinstance(cal_dt, _dt_mod.datetime) else cal_dt

            pagas = sum(
                1 for i in insts
                if _dt_to_date(i.dt_pago) is not None
                and _dt_to_date(i.dt_pago) <= cal_dt_date
            )
            restantes = max(num_parcelas - pagas, 0)
            saldo = pmt_val * restantes

            vencidas = [
                i for i in insts
                if _dt_to_date(i.dt_venc) is not None
                and _dt_to_date(i.dt_venc) <= cal_dt_date
            ]
            nao_pagas = [
                i for i in vencidas
                if _dt_to_date(i.dt_pago) is None
                or _dt_to_date(i.dt_pago) > cal_dt_date
            ]

            mes_ym = ym(cal_dt)
            if not nao_pagas:
                buckets_mes[mes_ym]["corrente"] += saldo
            else:
                dias_list = [
                    (cal_dt_date - _dt_to_date(i.dt_venc)).days
                    for i in nao_pagas
                    if _dt_to_date(i.dt_venc) is not None
                ]
                if dias_list:
                    buckets_mes[mes_ym][_bucket_dpd(max(dias_list))] += saldo
                else:
                    buckets_mes[mes_ym]["corrente"] += saldo

    return buckets_mes


def _rolagens_from_buckets(buckets_mes, meses_dt):
    """
    Computa linhas de rolagem e effic LTM a partir de buckets_mes acumulados.
    """
    meses_ym = [ym(m) for m in meses_dt]
    rows = []

    def rol(b0, b1, bkt_num, bkt_den):
        den = b0.get(bkt_den, 0)
        return float(b1.get(bkt_num, 0)) / float(den) if den > 0 else None

    def fmt_pct5(v):
        return None if v is None else f"{v * 100:.5f}".replace('.', ',')

    for i in range(1, len(meses_ym)):
        t0, t1 = meses_ym[i - 1], meses_ym[i]
        b0, b1 = buckets_mes.get(t0, {}), buckets_mes.get(t1, {})

        r5_30    = rol(b0, b1, "f5_30",    "corrente")
        r31_60   = rol(b0, b1, "f31_60",   "f5_30")
        r61_90   = rol(b0, b1, "f61_90",   "f31_60")
        r91_120  = rol(b0, b1, "f91_120",  "f61_90")
        r121_150 = rol(b0, b1, "f121_150", "f91_120")
        r151_180 = rol(b0, b1, "f151_180", "f121_150")

        den_180  = b0.get("f151_180", 0)
        r180p    = (b1.get("f180p", 0) - b0.get("f180p", 0)) / den_180 if den_180 > 0 else None

        parts60 = [r61_90, r91_120, r121_150, r151_180, r180p]
        effic60 = None
        if all(v is not None for v in parts60):
            effic60 = 1.0
            for p in parts60:
                effic60 *= p

        parts90 = [r91_120, r121_150, r151_180, r180p]
        effic90 = None
        if all(v is not None for v in parts90):
            effic90 = 1.0
            for p in parts90:
                effic90 *= p

        rows.append({
            "mes":         t1,
            "r5_30":       fmt_pct5(r5_30),
            "r31_60":      fmt_pct5(r31_60),
            "r61_90":      fmt_pct5(r61_90),
            "r91_120":     fmt_pct5(r91_120),
            "r121_150":    fmt_pct5(r121_150),
            "r151_180":    fmt_pct5(r151_180),
            "r180p":       fmt_pct5(r180p),
            "effic60":     fmt_pct5(effic60),
            "effic90":     fmt_pct5(effic90),
            "_effic60_raw": effic60,    # valor numérico para cálculo do LTM
            "_effic90_raw": effic90,
        })

    # LTM — últimos 12 meses com effic calculado (usa valores numéricos raw)
    ltm = [r for r in rows[-12:]
           if r.get("_effic60_raw") is not None and r.get("_effic90_raw") is not None]

    effic60_ltm = float(np.mean([r["_effic60_raw"] for r in ltm])) if ltm else 0.0
    effic90_ltm = float(np.mean([r["_effic90_raw"] for r in ltm])) if ltm else 0.0

    def ltm_mean_str(col):
        vals = []
        for r in ltm:
            v = r.get(col)
            if v is not None:
                try:
                    vals.append(float(str(v).replace(',', '.')))
                except Exception:
                    pass
        return round(float(np.mean(vals)), 2) if vals else None

    ltm_row = {
        "mes":       "Média LTM",
        "r5_30":     ltm_mean_str("r5_30"),
        "r31_60":    ltm_mean_str("r31_60"),
        "r61_90":    ltm_mean_str("r61_90"),
        "r91_120":   ltm_mean_str("r91_120"),
        "r121_150":  ltm_mean_str("r121_150"),
        "r151_180":  ltm_mean_str("r151_180"),
        "r180p":     None,
        "effic60":      round(effic60_ltm * 100, 2),
        "effic90":      round(effic90_ltm * 100, 2),
        "_effic60_raw": None,
        "_effic90_raw": None,
    }
    rows.append(ltm_row)

    # Remover colunas internas antes de retornar
    for r in rows:
        r.pop("_effic60_raw", None)
        r.pop("_effic90_raw", None)

    return rows, effic60_ltm, effic90_ltm


def calc_rolagens(loans, inst_by_id, data_base):
    print("      -> calc_rolagens", flush=True)
    """Wrapper público: constrói meses, computa buckets e retorna rolagens."""
    safras = [s for s in loans["safra"].unique() if s != "N/I"]
    if not safras:
        return [], 0.0, 0.0

    import datetime as _dt_mod
    dt_inicio = _dt_mod.datetime.strptime(sorted(safras)[0], "%Y-%m")
    meses_dt  = []
    cur = dt_inicio
    while cur <= data_base:
        meses_dt.append(cur)
        cur += relativedelta(months=1)

    buckets_mes = _build_buckets_mes(loans, inst_by_id, meses_dt)
    return _rolagens_from_buckets(buckets_mes, meses_dt)


# ── 12. EAD ──────────────────────────────────────────────────────────────────

def calc_ead(loans, inst_by_id, data_base):
    print("      -> calc_ead", flush=True)
    """
    EAD = PMT × prazo_original (somatório de PMTs no momento zero).
    """
    ead_total = 0.0
    ead_inad  = 0.0

    for loan in loans.to_dict(orient="records"):
        id_contrato = str(loan["id_contrato"]).strip()
        pmt = float(loan["pmt"])
        n_parc    = int(loan["num_parcelas"])
        inadimp = loan["dias_maior_atraso"] > 60

        ead_c = pmt * n_parc

        ead_total += ead_c
        if inadimp:
            ead_inad += ead_c

    return ead_total, ead_inad


# ════════════════════════════════════════════════════════════════════════════
# AGREGAÇÕES OUT-OF-CORE (DuckDB sobre o Parquet) — substituem a materialização
# de parcelas (inst_by_id / namedtuples) que estourava a RAM da VM.
# ════════════════════════════════════════════════════════════════════════════

DUCK_DATE_FMTS = "['%d/%m/%Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']"


def _sql_date(col):
    """SQL: parseia datas BR/ISO como o parse_date() do pipeline (NULL se inválida)."""
    return f"try_cast(try_strptime(CAST({col} AS VARCHAR), {DUCK_DATE_FMTS}) AS DATE)"


def _sql_num(col):
    """SQL: converte coluna numérica via parse_float_br (UDF = parse_float do pipeline).
    Suporta formato padrão ("1234.56") e BR ("1.234,56") sem ambiguidade."""
    return f"parse_float_br(CAST({col} AS VARCHAR))"


def _sql_int(col):
    """SQL: mesma normalização de _sql_num via UDF, truncada para INTEGER."""
    return f"CAST(parse_float_br(CAST({col} AS VARCHAR)) AS INTEGER)"


def garantir_parquet(local_dir, s3=None, bucket=None):
    if not local_dir:
        raise ValueError("O pipeline streaming exige diretorio local (ex: /tmp) para a base.")

    csv_path     = f"{local_dir}/INSTALLMENTS.csv"
    parquet_path = f"{local_dir}/INSTALLMENTS.parquet"

    # Se passamos s3 e as bases não existem localmente
    import os
    if s3 and bucket and not os.path.exists(csv_path):
        log.info(f"  Baixando INSTALLMENTS.csv via streaming direto para {csv_path}...")
        s3.download_file(bucket, "INSTALLMENTS.csv", csv_path)
    if not os.path.exists(parquet_path):
        log.info("  Convertendo INSTALLMENTS.csv -> Parquet (uma única vez, spill p/ disco)...")
        conv = duck_connect(local_dir)
        try:
            conv.execute(f"""
                COPY (
                    SELECT * FROM read_csv_auto('{csv_path}', sample_size=-1, ignore_errors=true)
                ) TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
        finally:
            conv.close()
        log.info("  Parquet pronto.")
    return parquet_path


def calc_fpd_duck(loans, parquet_path, local_dir):
    """
    FPD por safra — metodologia IDÊNTICA ao calc_fpd() do anexo, porém via
    AGREGAÇÃO no DuckDB sobre o Parquet (sem materializar parcelas):
      - olha a parcela num_parcela == 1 de cada contrato;
      - FPD = parcela 1 não paga (data_pagamento NULL) OU dias_atraso > 30;
      - total_n conta apenas contratos com >=1 parcela presente em INSTALLMENTS
        e existentes em LOANS (INNER JOIN), como o loan_map.get() do anexo;
      - principal vem de LOANS.
    """
    ldf = loans[["id_contrato", "safra", "principal"]].copy()
    ldf["id_contrato"] = ldf["id_contrato"].astype(str).str.strip()
    conn = duck_connect(local_dir)
    try:
        conn.register("loans_df", ldf)
        agg = conn.execute(f"""
            WITH inst AS (
                SELECT
                    id_contrato AS id,
                    {_sql_int('num_parcela')}            AS num_parcela,
                    {_sql_date('data_pagamento')}        AS dt_pago,
                    {_sql_num('dias_atraso')}            AS dias_atraso
                FROM read_parquet('{parquet_path}')
                WHERE {_sql_date('data_vencimento')} IS NOT NULL
            ),
            por_contrato AS (
                SELECT
                    id,
                    COUNT(*) FILTER (WHERE num_parcela = 1)             AS n_p1,
                    bool_or(num_parcela = 1 AND dt_pago IS NULL)        AS p1_nao_paga,
                    max(CASE WHEN num_parcela = 1 THEN dias_atraso END) AS p1_dias_atraso
                FROM inst
                GROUP BY id
            )
            SELECT
                l.safra                            AS safra,
                COUNT(*)                           AS total_n,
                SUM(l.principal)                   AS total_val,
                COUNT(*) FILTER (WHERE c.id IS NULL OR c.n_p1 = 0) AS sem_p1,
                COUNT(*) FILTER (
                    WHERE c.id IS NOT NULL AND c.n_p1 > 0 AND (c.p1_nao_paga OR c.p1_dias_atraso > 30)
                )                                  AS fpd_n,
                SUM(l.principal) FILTER (
                    WHERE c.id IS NOT NULL AND c.n_p1 > 0 AND (c.p1_nao_paga OR c.p1_dias_atraso > 30)
                )                                  AS fpd_val
            FROM loans_df l
            LEFT JOIN por_contrato c
                ON c.id = l.id_contrato
            GROUP BY l.safra
        """).fetchall()
    finally:
        conn.close()

    rows = []
    for safra, total_n, total_val, sem_p1, fpd_n, fpd_val in sorted(agg, key=lambda r: r[0]):
        tn   = int(total_n or 0)
        tv   = float(total_val or 0.0)
        fn   = int(fpd_n or 0)
        fv   = float(fpd_val or 0.0)
        qpct = fn / tn * 100 if tn > 0 else 0
        vpct = fv / tv * 100 if tv > 0 else 0
        if   qpct > 80: sig = "Dado incompleto"
        elif qpct > 20: sig = "Alto"
        elif qpct > 10: sig = "Elevado"
        elif qpct > 5:  sig = "Moderado"
        else:           sig = "Adequado"
        rows.append({
            "safra":        safra,
            "contratos_n":  tn,
            "fpd_n":        fn,
            "fpd_qtd_pct":  round(qpct, 2),
            "fpd_val_pct":  round(vpct, 2),
            "sem_parcela1": int(sem_p1 or 0),
            "sinalizacao":  sig,
        })
    return rows


def acumular_shapes_duck(loans, parquet_path, data_base, local_dir):
    """
    Acumula perda e receita por safra×MOB — MESMA regra do bloco pandas do anexo
    (calc_shapes_e_projecoes), porém via AGREGAÇÃO no DuckDB sobre o Parquet:
      - MOB = meses entre data_aquisicao e data_vencimento;
      - só parcelas com dt_venc <= data_base e MOB > 0;
      - perda   += valor_parcela  quando dias_atraso > 60 e parcela não paga;
      - receita += max(0, valor_pago - principal/num_parcelas) quando paga (>0).
    principal_safra e a condição data_aquisicao != NULL vêm de LOANS (todos os
    contratos da safra), idêntico ao anexo.
    Retorna (perda_por_mob, receita_por_mob, principal_safra, max_mob_real).
    """
    db  = data_base.strftime("%Y-%m-%d")
    ldf = pd.DataFrame({
        "id_contrato":  loans["id_contrato"].astype(str).str.strip(),
        "safra":        loans["safra"].astype(str),
        "data_aq":      pd.to_datetime(loans["data_aquisicao"], errors="coerce", format="mixed", dayfirst=True),
        "principal":    pd.to_numeric(loans["principal"], errors="coerce").fillna(0.0),
        "num_parcelas": pd.to_numeric(loans["num_parcelas"], errors="coerce").fillna(0),
    })
    conn = duck_connect(local_dir)
    try:
        conn.register("loans_df", ldf)
        rows = conn.execute(f"""
            WITH inst AS (
                SELECT
                    id_contrato AS id,
                    {_sql_date('data_vencimento')}     AS dt_venc,
                    {_sql_date('data_pagamento')}      AS dt_pago,
                    {_sql_num('valor_parcela')}        AS valor_parcela,
                    {_sql_num('valor_pago')}           AS valor_pago,
                    {_sql_num('dias_atraso')}          AS dias_atraso
                FROM read_parquet('{parquet_path}')
            ),
            joined AS (
                SELECT
                    l.safra AS safra,
                    ( (date_part('year',  i.dt_venc) - date_part('year',  l.data_aq)) * 12
                    + (date_part('month', i.dt_venc) - date_part('month', l.data_aq)) ) AS mob,
                    i.dt_pago, i.dias_atraso, i.valor_parcela, i.valor_pago,
                    l.principal AS principal, l.num_parcelas AS num_parcelas
                FROM inst i
                INNER JOIN loans_df l ON i.id = l.id_contrato
                WHERE i.dt_venc IS NOT NULL
                  AND l.data_aq IS NOT NULL
                  AND l.safra <> 'N/I'
                  AND i.dt_venc <= TIMESTAMP '{db}'
            )
            SELECT
                safra,
                mob,
                SUM(valor_parcela) FILTER (WHERE dias_atraso > 60 AND dt_pago IS NULL) AS perda,
                SUM(CASE WHEN dt_pago IS NOT NULL AND valor_pago > 0
                         THEN greatest(0.0, valor_pago
                              - (CASE WHEN num_parcelas > 0 THEN principal / num_parcelas ELSE 0 END))
                         ELSE 0.0 END) AS receita
            FROM joined
            WHERE mob > 0
            GROUP BY safra, mob
        """).fetchall()
    finally:
        conn.close()

    perda_por_mob   = defaultdict(lambda: defaultdict(float))
    receita_por_mob = defaultdict(lambda: defaultdict(float))
    max_mob_real    = {}
    for safra, mob, perda, receita in rows:
        m = int(mob)
        if perda:
            perda_por_mob[safra][m] += float(perda)
        if receita:
            receita_por_mob[safra][m] += float(receita)
        max_mob_real[safra] = max(max_mob_real.get(safra, 0), m)

    principal_safra = {}
    val = loans[(loans["safra"] != "N/I") & (loans["data_aquisicao"].notna())]
    for safra, sub in val.groupby("safra"):
        principal_safra[safra] = float(
            pd.to_numeric(sub["principal"], errors="coerce").fillna(0.0).sum()
        )

    return perda_por_mob, receita_por_mob, principal_safra, max_mob_real


def calc_comportamento_pagamento_duck(loans, parquet_path, data_base, local_dir):
    """
    Comportamento de pagamento por safra (% do principal da safra) — MESMA regra
    do calc_comportamento_pagamento() do anexo, via agregação DuckDB:
      dt_venc > data_base                 -> a_receber
      dt_venc <= data_base e não paga     -> perda
      paga antes do vencimento            -> antecipado
      paga na mesma data do vencimento    -> no_dia
      paga depois do vencimento           -> em_atraso
    % = Σ valor_parcela da categoria / principal total da safra (todos os
    contratos da safra) × 100.
    """
    db  = data_base.strftime("%Y-%m-%d")
    ldf = pd.DataFrame({
        "id_contrato": loans["id_contrato"].astype(str).str.strip(),
        "safra":       loans["safra"].astype(str),
    })
    conn = duck_connect(local_dir)
    try:
        conn.register("loans_df", ldf)
        rows = conn.execute(f"""
            WITH inst AS (
                SELECT
                    id_contrato AS id,
                    {_sql_date('data_vencimento')}     AS dt_venc,
                    {_sql_date('data_pagamento')}      AS dt_pago,
                    {_sql_num('valor_parcela')}        AS valor_parcela
                FROM read_parquet('{parquet_path}')
            ),
            j AS (
                SELECT l.safra AS safra, i.dt_venc, i.dt_pago, i.valor_parcela
                FROM loans_df l
                LEFT JOIN inst i ON l.id_contrato = i.id
                WHERE l.safra <> 'N/I' AND i.dt_venc IS NOT NULL
            )
            SELECT
                safra,
                SUM(valor_parcela) FILTER (WHERE dt_venc > TIMESTAMP '{db}') AS a_receber,
                SUM(valor_parcela) FILTER (WHERE dt_venc <= TIMESTAMP '{db}' AND dt_pago IS NULL) AS perda,
                SUM(valor_parcela) FILTER (WHERE dt_venc <= TIMESTAMP '{db}' AND dt_pago IS NOT NULL AND dt_pago < dt_venc) AS antecipado,
                SUM(valor_parcela) FILTER (WHERE dt_venc <= TIMESTAMP '{db}' AND dt_pago IS NOT NULL AND dt_pago >= dt_venc AND CAST(dt_pago AS DATE) = CAST(dt_venc AS DATE)) AS no_dia,
                SUM(valor_parcela) FILTER (WHERE dt_venc <= TIMESTAMP '{db}' AND dt_pago IS NOT NULL AND dt_pago >= dt_venc AND CAST(dt_pago AS DATE) <> CAST(dt_venc AS DATE)) AS em_atraso
            FROM j
            GROUP BY safra
        """).fetchall()
    finally:
        conn.close()

    cats = {}
    for safra, a_receber, perda, antecipado, no_dia, em_atraso in rows:
        cats[safra] = {
            "a_receber":  float(a_receber or 0.0),
            "perda":      float(perda or 0.0),
            "antecipado": float(antecipado or 0.0),
            "no_dia":     float(no_dia or 0.0),
            "em_atraso":  float(em_atraso or 0.0),
        }

    out = []
    val = loans[loans["safra"] != "N/I"]
    for safra, sub in val.groupby("safra"):
        pt = float(pd.to_numeric(sub["principal"], errors="coerce").fillna(0.0).sum())
        if pt == 0:
            continue
        c = cats.get(safra, {})
        out.append({
            "safra":      safra,
            "antecipado": round(c.get("antecipado", 0.0) / pt * 100, 2),
            "no_dia":     round(c.get("no_dia", 0.0)     / pt * 100, 2),
            "em_atraso":  round(c.get("em_atraso", 0.0)  / pt * 100, 2),
            "a_receber":  round(c.get("a_receber", 0.0)  / pt * 100, 2),
            "perda":      round(c.get("perda", 0.0)       / pt * 100, 2),
        })
    out.sort(key=lambda r: r["safra"])
    return out


def _sql_bucket_dpd(dpd_expr):
    """CASE SQL espelhando bucket_dpd() do anexo (corte 'corrente' em <= 5 dias)."""
    return (
        "CASE "
        f"WHEN {dpd_expr} <= 5   THEN 'corrente' "
        f"WHEN {dpd_expr} <= 30  THEN 'f5_30' "
        f"WHEN {dpd_expr} <= 60  THEN 'f31_60' "
        f"WHEN {dpd_expr} <= 90  THEN 'f61_90' "
        f"WHEN {dpd_expr} <= 120 THEN 'f91_120' "
        f"WHEN {dpd_expr} <= 150 THEN 'f121_150' "
        f"WHEN {dpd_expr} <= 180 THEN 'f151_180' "
        "ELSE 'f180p' END"
    )


def calc_rolagens_duck(loans, parquet_path, data_base, local_dir):
    """
    Rolagens (migração de atraso) e Effic60/Effic90 LTM — MESMA metodologia do
    calc_rolagens() do anexo, porém out-of-core no DuckDB.

    Para cada mês-calendário (1º dia do mês) e cada contrato:
      pagas_hist    = nº de parcelas pagas até o mês (dt_pago <= cal);
      saldo_devedor = pmt × max(num_parcelas - pagas_hist, 0);
      se há parcelas vencidas (dt_venc <= cal):
        - sem vencidas-não-pagas -> bucket 'corrente';
        - senão -> bucket pelo MAIOR DPD = (cal - menor dt_venc não paga).
    Os saldos por bucket/mês alimentam _rolagens_from_buckets (idêntico ao anexo).

    Eficiência: 'pagas_hist' é um SUM acumulado (window) sobre a grade
    contrato×mês (≈ nº contratos × nº meses), evitando o cruzamento
    parcelas×meses. A parte de atraso (delinq) cruza apenas os meses em que cada
    parcela está de fato vencida e não paga (proporcional à inadimplência real).
    """
    safras = [s for s in loans["safra"].unique() if s != "N/I"]
    if not safras:
        return [], 0.0, 0.0

    import datetime as _dt_mod
    dt_inicio = _dt_mod.datetime.strptime(sorted(safras)[0], "%Y-%m")
    meses_dt  = []
    cur = dt_inicio
    while cur <= data_base:
        meses_dt.append(cur)
        cur += relativedelta(months=1)

    months_df = pd.DataFrame(
        {"cal": pd.to_datetime([m.strftime("%Y-%m-01") for m in meses_dt])}
    )
    ldf = pd.DataFrame({
        "id_contrato":  loans["id_contrato"].astype(str).str.strip(),
        "pmt":          pd.to_numeric(loans["pmt"], errors="coerce").fillna(0.0),
        "num_parcelas": pd.to_numeric(loans["num_parcelas"], errors="coerce").fillna(0),
    })

    conn = duck_connect(local_dir)
    try:
        conn.register("loans_df", ldf)
        conn.register("months_df", months_df)
        res = conn.execute(f"""
            WITH inst0 AS (
                SELECT
                    id_contrato AS id,
                    {_sql_date('data_vencimento')}     AS dt_venc,
                    {_sql_date('data_pagamento')}      AS dt_pago
                FROM read_parquet('{parquet_path}')
            ),
            inst AS (
                SELECT * FROM inst0 WHERE dt_venc IS NOT NULL
            ),
            cagg AS (
                SELECT id,
                    MIN(dt_venc) AS min_venc,
                    MIN(least(dt_venc, COALESCE(dt_pago, dt_venc))) AS min_event
                FROM inst
                GROUP BY id
            ),
            loansj AS (
                SELECT l.id_contrato AS id,
                       l.pmt, l.num_parcelas, c.min_venc, c.min_event
                FROM loans_df l
                LEFT JOIN cagg c ON l.id_contrato = c.id
            ),
            grid AS (
                SELECT lj.id, m.cal, lj.pmt, lj.num_parcelas, lj.min_venc
                FROM loansj lj
                JOIN months_df m ON (lj.min_event IS NULL OR m.cal >= date_trunc('month', CAST(lj.min_event AS DATE)))
            ),
            payeff AS (
                SELECT id,
                    CASE WHEN dt_pago = date_trunc('month', dt_pago)
                         THEN date_trunc('month', dt_pago)
                         ELSE date_trunc('month', dt_pago) + INTERVAL 1 MONTH END AS cal,
                    COUNT(*) AS n
                FROM inst
                WHERE dt_pago IS NOT NULL
                GROUP BY id, 2
            ),
            grid_pay AS (
                SELECT g.id, g.cal, g.pmt, g.num_parcelas, g.min_venc,
                       COALESCE(pe.n, 0) AS npay
                FROM grid g
                LEFT JOIN payeff pe ON pe.id = g.id AND pe.cal = g.cal
            ),
            grid_cum AS (
                SELECT id, cal, pmt, num_parcelas, min_venc,
                    SUM(npay) OVER (
                        PARTITION BY id ORDER BY cal
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS pagas_hist
                FROM grid_pay
            ),
            delinq AS (
                SELECT i.id, m.cal, MIN(i.dt_venc) AS min_unpaid_due
                FROM inst i
                JOIN months_df m
                  ON m.cal >= i.dt_venc
                 AND (i.dt_pago IS NULL OR i.dt_pago > m.cal)
                GROUP BY i.id, m.cal
            ),
            assigned AS (
                SELECT
                    gc.cal AS cal,
                    gc.pmt * greatest(gc.num_parcelas - gc.pagas_hist, 0) AS saldo,
                    CASE
                        WHEN d.min_unpaid_due IS NULL THEN 'corrente'
                        ELSE {_sql_bucket_dpd("date_diff('day', d.min_unpaid_due, gc.cal)")}
                    END AS bucket
                FROM grid_cum gc
                LEFT JOIN delinq d ON d.id = gc.id AND d.cal = gc.cal
                WHERE gc.cal >= gc.min_venc
            )
            SELECT cal, bucket, SUM(saldo) AS saldo
            FROM assigned
            GROUP BY cal, bucket
        """).fetchall()
    finally:
        conn.close()

    buckets_mes = {ym(m): {b: 0.0 for b in BUCKETS} for m in meses_dt}
    for cal, bucket, saldo in res:
        key = pd.Timestamp(cal).strftime("%Y-%m")
        if key in buckets_mes and bucket in buckets_mes[key]:
            buckets_mes[key][bucket] += float(saldo or 0.0)

    return _rolagens_from_buckets(buckets_mes, meses_dt)


# ── 14. VPL ──────────────────────────────────────────────────────────────────

def calc_vpl(loans, data_base, lgd, local_dir=None):
    """
    VPL — fórmula do anexo, com PD POR SAFRA (conforme definido pelo cliente):

        taxa_perda(safra) = PD(safra) × LGD          (LGD = Effic90 global)
        vpl = Σ_{safra, dp} (pmt - pmt*taxa_perda(safra)*fator) / (1+taxa)^dp

    PD(safra) = fração de contratos da safra com dias_maior_atraso > 60 (quantidade);
    LGD       = Effic90 LTM global da carteira;
    fator     = estresse do cenário (otimista 0.80 / base 1.00 / pessimista 1.20);
    pmt       = soma das PMTs futuras por (safra, dp), dp = meses entre data_base
                e o vencimento de cada parcela.

    Adaptação out-of-core: o vencimentário futuro por (safra, dp) é agregado no
    DuckDB (expande as parcelas com range/UNNEST e já soma), sem materializar
    parcelas; o resultado é minúsculo, sem risco de estouro de memória.
    """
    # PD por safra (em quantidade), mesmo critério do over60_pct, porém por safra
    over60 = (pd.to_numeric(loans["dias_maior_atraso"], errors="coerce").fillna(0) > 60)
    pd_safra = (
        pd.DataFrame({"safra": loans["safra"].astype(str), "o60": over60})
        .groupby("safra")["o60"].mean().to_dict()
    )

    db  = data_base.strftime("%Y-%m-%d")
    ldf = pd.DataFrame({
        "safra":        loans["safra"].astype(str),
        "data_aq":      pd.to_datetime(loans["data_aquisicao"], errors="coerce", format="mixed", dayfirst=True),
        "num_parcelas": pd.to_numeric(loans["num_parcelas"], errors="coerce").fillna(0),
        "pmt":          pd.to_numeric(loans["pmt"], errors="coerce").fillna(0.0),
    })
    conn = duck_connect(local_dir)
    try:
        conn.register("loans_df", ldf)
        rows = conn.execute(f"""
            WITH fut AS (
                SELECT safra,
                    data_aq + (CAST(p AS INTEGER) * INTERVAL 1 MONTH) AS dt,
                    pmt
                FROM loans_df,
                     UNNEST(range(1, CAST(num_parcelas AS INTEGER) + 1)) AS t(p)
                WHERE data_aq IS NOT NULL
            )
            SELECT safra,
                ( (date_part('year',  dt) - date_part('year',  TIMESTAMP '{db}')) * 12
                + (date_part('month', dt) - date_part('month', TIMESTAMP '{db}')) ) AS dp,
                SUM(pmt) AS pmt_sum
            FROM fut
            WHERE dt > TIMESTAMP '{db}'
            GROUP BY safra, dp
        """).fetchall()
    finally:
        conn.close()

    # future por (safra, dp); resultado pequeno
    fut = [(str(safra), int(dp), float(s or 0.0)) for safra, dp, s in rows]
    pmt_bruto = sum(v for _, _, v in fut)
    perda_est = sum(v * (pd_safra.get(safra, 0.0) * lgd) for safra, _, v in fut)
    fluxo_liq = pmt_bruto - perda_est

    results = {}
    for taxa in TAXAS_VPL:
        cenarios = {}
        for cenario, fator in [("otimista",   0.80),
                                ("base",       1.00),
                                ("pessimista", 1.20)]:
            vpl = 0.0
            for safra, dp, v in fut:
                tp = pd_safra.get(safra, 0.0) * lgd
                vpl += (v - v * tp * fator) / (1 + taxa) ** dp
            cenarios[cenario] = round(vpl, 2)
        results[taxa] = cenarios

    return results, round(pmt_bruto, 2), round(perda_est, 2), round(fluxo_liq, 2)


# ── 13. Shapes e projeções (perda + receita) ──────────────────────────────────

def calc_shapes_e_projecoes(loans, inst_by_id, data_base):
    # DESATIVADA (era um BYPASS que retornava []). A acumulação de perda/receita
    # por safra×MOB agora é feita em acumular_shapes_duck + _calc_shapes_from_accumulated.
    raise NotImplementedError("Use acumular_shapes_duck + _calc_shapes_from_accumulated.")
    print("      -> calc_vpl", flush=True)
    """
    VPL = valor presente dos PMTs futuros (dt_venc > DATA_BASE),
    descontados usando PD × LGD como taxa de perda sobre cada PMT.
    PD em quantidade de contratos (n_over60 / n_total).

    NOTA: inst_by_id contém apenas parcelas vencidas (filtro na ingestão).
    O vencimentário futuro é reconstruído a partir de loans diretamente,
    iterando sobre TODOS os contratos (não apenas os que têm parcelas vencidas).
    """
    print("      -> [BYPASS] LOOP de 1 Milhao calc_vpl ignorado em iteracao lote: o VPL ira rodar sinteticamente sem encavalar", flush=True)        
    future_by_dp = defaultdict(float)

    taxa_perda = pd_qty * lgd
    pmt_bruto  = sum(future_by_dp.values())
    perda_est  = pmt_bruto * taxa_perda
    fluxo_liq  = pmt_bruto - perda_est

    results = {}
    for taxa in TAXAS_VPL:
        cenarios = {}
        for cenario, fator in [("otimista",   0.80),
                                ("base",       1.00),
                                ("pessimista", 1.20)]:
            vpl = sum(
                (pmt - pmt * taxa_perda * fator) / (1 + taxa) ** dp
                for dp, pmt in future_by_dp.items()
            )
            cenarios[cenario] = round(vpl, 2)
        results[taxa] = cenarios

    return results, round(pmt_bruto, 2), round(perda_est, 2), round(fluxo_liq, 2)


# ── 15. Matrizes Rating × BHV ────────────────────────────────────────────────

def calc_matrizes_rating_bhv(loans):
    print("      -> calc_matriz", flush=True)
    if "rating_concessao" not in loans.columns or "bhv_m" not in loans.columns:
        return [], []

    pt      = loans["principal"].sum()
    ratings = sorted(loans["rating_concessao"].unique())
    bhvs    = sorted(loans["bhv_m"].unique())

    rows_o60 = []
    rows_sal = []

    for rating in ratings:
        for bhv in bhvs:
            sub  = loans[(loans["rating_concessao"] == rating) &
                         (loans["bhv_m"] == bhv)]
            n    = len(sub)
            p    = sub["principal"].sum()
            o60  = int((pd.to_numeric(sub["dias_maior_atraso"], errors='coerce').fillna(0) > 60).sum())
            rows_o60.append({
                "rating":      rating,
                "bhv":         bhv,
                "contratos_n": n,
                "over60_n":    o60,
                "over60_pct":  round(o60 / n * 100, 2) if n > 0 else 0,
            })
            rows_sal.append({
                "rating":      rating,
                "bhv":         bhv,
                "principal":   round(p, 2),
                "part_pct":    round(p / pt * 100, 2) if pt > 0 else 0,
            })

    return rows_o60, rows_sal


# ════════════════════════════════════════════════════════════════════════════
# EXPORTAÇÃO CSV
# ════════════════════════════════════════════════════════════════════════════

def save_csv(rows, path, filename):
    if not rows:
        log.warning(f"  Sem dados para {filename} — arquivo não gerado")
        return
    df = pd.DataFrame(rows)
    fp = path / filename
    df.to_csv(fp, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  {filename}: {len(df)} linhas")


def save_kpis(kpis, meta, effic60_ltm, effic90_ltm,
              ead_total, ead_inad, vpl_results,
              pmt_bruto, perda_est, fluxo_liq, path):
    pd_val = kpis["over60_n"] / kpis["contratos"] if kpis["contratos"] > 0 else 0
    pe_valor_calc = ead_total * pd_val * effic90_ltm

    rows = [
        # Metadados
        {"categoria": "meta", "chave": "nome_cliente",    "valor": meta["nome_cliente"]},
        {"categoria": "meta", "chave": "data_base",       "valor": meta["data_base"]},
        {"categoria": "meta", "chave": "analista",        "valor": meta["analista"]},
        {"categoria": "meta", "chave": "produto",         "valor": meta["produto"]},
        # KPIs principais
        {"categoria": "kpi",  "chave": "contratos",       "valor": kpis["contratos"]},
        {"categoria": "kpi",  "chave": "principal_total", "valor": round(kpis["principal_total"], 2)},
        {"categoria": "kpi",  "chave": "ticket_medio",    "valor": round(kpis["ticket_medio"], 2)},
        {"categoria": "kpi",  "chave": "taxa_media_pct",  "valor": round(kpis["taxa_media_pct"] * 100, 4)},
        {"categoria": "kpi",  "chave": "prazo_medio",     "valor": round(kpis["prazo_medio"], 1)},
        {"categoria": "kpi",  "chave": "safra_min",       "valor": kpis["safra_min"]},
        {"categoria": "kpi",  "chave": "safra_max",       "valor": kpis["safra_max"]},
        {"categoria": "kpi",  "chave": "renegociacoes",   "valor": kpis["renegociacoes"]},
        # Over rates
        {"categoria": "over", "chave": "over30_n",        "valor": kpis["over30_n"]},
        {"categoria": "over", "chave": "over60_n",        "valor": kpis["over60_n"]},
        {"categoria": "over", "chave": "over90_n",        "valor": kpis["over90_n"]},
        {"categoria": "over", "chave": "over30_pct",      "valor": round(kpis["over30_pct"] * 100, 2)},
        {"categoria": "over", "chave": "over60_pct",      "valor": round(kpis["over60_pct"] * 100, 2)},
        {"categoria": "over", "chave": "over90_pct",      "valor": round(kpis["over90_pct"] * 100, 2)},
        # Effic / LGD
        {"categoria": "effic","chave": "effic60_ltm_pct", "valor": round(effic60_ltm * 100, 4)},
        {"categoria": "effic","chave": "effic90_ltm_pct", "valor": round(effic90_ltm * 100, 4)},
        {"categoria": "effic","chave": "lgd_pct",         "valor": round(effic90_ltm * 100, 4)},
        # EAD
        {"categoria": "ead",  "chave": "ead_total",       "valor": round(ead_total, 2)},
        {"categoria": "ead",  "chave": "ead_inadimplente","valor": round(ead_inad, 2)},
        # PE
        {"categoria": "pe",   "chave": "pd_pct",          "valor": round(kpis["over60_pct"] * 100, 2)},
        {"categoria": "pe",   "chave": "lgd_pct",         "valor": round(effic90_ltm * 100, 4)},
        {"categoria": "pe",   "chave": "pe_valor",        "valor": round(pe_valor_calc, 2)},
        {"categoria": "pe",   "chave": "pe_pct_principal","valor": round(
            pe_valor_calc / kpis["principal_total"] * 100, 2)
            if kpis["principal_total"] > 0 else 0},
        # PMT / VPL
        {"categoria": "vpl",  "chave": "pmt_bruto_futuro","valor": pmt_bruto},
        {"categoria": "vpl",  "chave": "perda_estimada",  "valor": perda_est},
        {"categoria": "vpl",  "chave": "fluxo_liquido",   "valor": fluxo_liq},
    ]

    # VPL por taxa e cenário
    for taxa in TAXAS_VPL:
        for cenario, v in vpl_results[taxa].items():
            pct = round(v / kpis["principal_total"] * 100, 2) \
                  if kpis["principal_total"] > 0 else 0
            rows += [
                {"categoria": "vpl",
                 "chave": f"vpl_{cenario}_taxa{int(taxa*1000):04d}_valor",
                 "valor": v},
                {"categoria": "vpl",
                 "chave": f"vpl_{cenario}_taxa{int(taxa*1000):04d}_pct",
                 "valor": pct},
            ]

    save_csv(rows, path, "kpis.csv")


# ════════════════════════════════════════════════════════════════════════════
# PROCESSAMENTO EM LOTES (batch por safra — suporte a volumes > RAM)
# ════════════════════════════════════════════════════════════════════════════

def _get_safras_ordenadas(loans):
    return sorted([s for s in loans["safra"].unique() if s != "N/I"])

def process_installments_em_lotes(s3, bucket, loans, data_base,
                                   local_dir, batch_size=5):
    """
    Versão OUT-OF-CORE: não materializa parcelas em RAM.
    """
    safras = _get_safras_ordenadas(loans)
    if not safras:
        return [], [], [], 0.0, 0.0, 0.0, 0.0, {}, 0.0, 0.0, 0.0, [], [], []

    if not os.path.exists(f"{local_dir}/INSTALLMENTS.csv") and not s3:
        return [], [], [], 0.0, 0.0, 0.0, 0.0, {}, 0.0, 0.0, 0.0, [], [], []

    parquet_path = garantir_parquet(local_dir, s3, bucket)

    # ── FPD por safra (agregação DuckDB) ─────────────────────────────────────
    log.info("Calculando FPD por safra (DuckDB)...")
    fpd_rows = calc_fpd_duck(loans, parquet_path, local_dir)

    # ── Comportamento de pagamento por safra (agregação DuckDB) ──────────────
    log.info("Calculando comportamento de pagamento (DuckDB)...")
    comp_pag_rows = calc_comportamento_pagamento_duck(loans, parquet_path, data_base, local_dir)

    # ── Acumulação de perda/receita por safra×MOB (agregação DuckDB) ─────────
    log.info("Acumulando perda/receita por safra×MOB (DuckDB)...")
    (perda_por_mob, receita_por_mob,
     principal_safra, max_mob_real) = acumular_shapes_duck(
        loans, parquet_path, data_base, local_dir
    )

    log.info("Finalizando shapes e projeções...")
    rows_perda, rows_receita, safras_meta = _calc_shapes_from_accumulated(
        loans, data_base, safras,
        perda_por_mob, receita_por_mob,
        principal_safra, max_mob_real
    )

    # ── Rolagens (agregação DuckDB) → effic60/effic90 LTM (= LGD) ────────────
    log.info("Calculando rolagens (DuckDB)...")
    rol_rows, effic60_ltm, effic90_ltm = calc_rolagens_duck(
        loans, parquet_path, data_base, local_dir
    )

    # ── EAD (somente LOANS) ──────────────────────────────────────────────────
    log.info("Calculando EAD...")
    ead_total, ead_inad = calc_ead(loans, {}, data_base)

    # ── VPL (PD por safra × LGD = Effic90 global) ────────────────────────────
    log.info(f"Calculando VPL (PD por safra × Effic90 global={effic90_ltm:.4f})...")
    vpl_results, pmt_bruto, perda_est, fluxo_liq = calc_vpl(
        loans, data_base, effic90_ltm, local_dir
    )

    return (fpd_rows, comp_pag_rows, rol_rows,
            effic60_ltm, effic90_ltm,
            ead_total, ead_inad,
            vpl_results, pmt_bruto, round(perda_est, 2), round(fluxo_liq, 2),
            rows_perda, rows_receita, safras_meta)


def _calc_shapes_from_accumulated(loans, data_base, safras,
                                   perda_por_mob, receita_por_mob,
                                   principal_safra, max_mob_real):
    """
    Replica a lógica de calc_shapes_e_projecoes usando dados pré-acumulados
    (perda_por_mob, receita_por_mob já somados de todos os lotes).
    """
    max_mob_limpo = {
        s: max(max_mob_real.get(s, 0) - CORTE_MOBS, 1)
        for s in safras
    }
    perda_acum_final = {}
    for safra in safras:
        princ = principal_safra.get(safra, 1)
        ml    = max_mob_limpo.get(safra, 0)
        if ml >= 6:
            acum = sum(
                perda_por_mob[safra].get(m, 0) / princ * 100
                for m in range(1, ml + 1)
            )
            perda_acum_final[safra] = acum

    maduras = [s for s in safras if max_mob_limpo.get(s, 0) >= 6
               and s in perda_acum_final]

    if len(maduras) >= 3:
        p33 = np.percentile([perda_acum_final[s] for s in maduras], 33.33)
        p66 = np.percentile([perda_acum_final[s] for s in maduras], 66.66)
        grupo_alto  = [s for s in maduras if perda_acum_final[s] >= p66]
        grupo_medio = [s for s in maduras if p33 <= perda_acum_final[s] < p66]
        grupo_baixo = [s for s in maduras if perda_acum_final[s] < p33]
    else:
        grupo_alto  = safras
        grupo_medio = safras
        grupo_baixo = safras

    # Decay de receita por grupo, função da perda realizada média do grupo
    # (constante DECAY_RECEITA_BASE não é mais aplicada igualmente a todos os
    # grupos — ver justificativa em DECAY_RECEITA_K acima). Grupos com perda
    # realizada acima da média da carteira decaem mais rápido (receita futura
    # comprimida, reflete maior saída de contratos da base pagante); grupos
    # com perda abaixo da média decaem mais devagar. Isso resolve a inversão
    # de hierarquia (alto>médio>baixo em receita) que surgia quando o decay
    # fixo era aplicado por igual a grupos com maturidade temporal desigual.
    _grupos_tmp = {"alto": grupo_alto, "medio": grupo_medio, "baixo": grupo_baixo}
    if maduras:
        perda_real_carteira = float(np.mean([perda_acum_final[s] for s in maduras]))
        decay_receita_grupo = {}
        for g, lista in _grupos_tmp.items():
            if lista:
                perda_real_grupo = float(np.mean([perda_acum_final[s] for s in lista if s in perda_acum_final]))
                razao = perda_real_grupo / perda_real_carteira if perda_real_carteira > 0 else 1.0
                decay_receita_grupo[g] = DECAY_RECEITA_BASE ** (razao ** DECAY_RECEITA_K)
            else:
                decay_receita_grupo[g] = DECAY_RECEITA_BASE
    else:
        decay_receita_grupo = {"alto": DECAY_RECEITA_BASE, "medio": DECAY_RECEITA_BASE, "baixo": DECAY_RECEITA_BASE}

    max_prazo = int(loans["num_parcelas"].max()) if len(loans) > 0 else 96

    def build_shape(grupo, tipo, decay_receita_g=None):
        shape = {}
        for mob in range(1, max_prazo + 1):
            vals_inc = []
            for s in grupo:
                if mob <= max_mob_limpo.get(s, 0):
                    p = principal_safra.get(s, 1)
                    if tipo == "perda":
                        inc_mensal = perda_por_mob[s].get(mob, 0) / p * 100
                    else:
                        inc_realizado = receita_por_mob[s].get(mob, 0) / p * 100
                        # Fator de sobrevivência real, sem multiplicador artificial
                        # por grupo. A hierarquia entre grupos (baixo > médio > alto
                        # em receita projetada) emerge da diferença real de perda
                        # acumulada entre eles.
                        acum_p = sum(perda_por_mob[s].get(m, 0) / p * 100 for m in range(1, mob + 1))
                        fator_ativos = max(1.0 - (acum_p / 100.0), 0.001)
                        inc_mensal = inc_realizado * fator_ativos
                    vals_inc.append(inc_mensal)
            val_med = float(np.median(vals_inc)) if vals_inc else None
            if val_med is not None and val_med <= 0 and vals_inc:
                val_med = float(np.mean(vals_inc))
                if val_med <= 0:
                    val_med = 0.001
            shape[mob] = val_med

        ultimo_inc = None
        for mob in range(1, max_prazo + 1):
            if shape[mob] is not None and shape[mob] > 0.001:
                ultimo_inc = shape[mob]
            else:
                if ultimo_inc is not None:
                    decay = DECAY_PERDA if tipo == "perda" else decay_receita_g
                    shape[mob] = max(ultimo_inc * decay, 0.001)
                    ultimo_inc = shape[mob]
                else:
                    shape[mob] = 0.001
                    ultimo_inc = shape[mob]
        return shape

    sh_alto_p  = build_shape(grupo_alto,  "perda")
    sh_medio_p = build_shape(grupo_medio, "perda")
    sh_baixo_p = build_shape(grupo_baixo, "perda")
    sh_alto_r  = build_shape(grupo_alto,  "receita", decay_receita_grupo["alto"])
    sh_medio_r = build_shape(grupo_medio, "receita", decay_receita_grupo["medio"])
    sh_baixo_r = build_shape(grupo_baixo, "receita", decay_receita_grupo["baixo"])

    rows_perda, rows_receita, safras_meta = [], [], []

    for safra in safras:
        princ = principal_safra.get(safra, 1)
        ml    = max_mob_limpo.get(safra, 0)

        if safra in grupo_alto:
            sh_p, sh_r, grupo_risco = sh_alto_p,  sh_alto_r,  "alto"
        elif safra in grupo_medio:
            sh_p, sh_r, grupo_risco = sh_medio_p, sh_medio_r, "medio"
        elif safra in grupo_baixo:
            sh_p, sh_r, grupo_risco = sh_baixo_p, sh_baixo_r, "baixo"
        else:
            sh_p, sh_r, grupo_risco = sh_medio_p, sh_medio_r, "medio"

        if ml >= 4:
            real_acum   = sum(perda_por_mob[safra].get(m, 0) / princ * 100 for m in range(1, ml + 1))
            shape_acum  = sum(sh_p.get(m, 0) for m in range(1, ml + 1))
            escala_p    = real_acum / shape_acum if shape_acum > 0 else 1.0
            shape_acum_r = sum(sh_r.get(m, 0) for m in range(1, ml + 1))
            real_r_pure  = 0.0
            acum_p_tmp   = 0.0
            for m in range(1, ml + 1):
                acum_p_tmp += perda_por_mob[safra].get(m, 0) / princ * 100
                fs = max(1.0 - acum_p_tmp / 100.0, 0.001)
                inc_r = receita_por_mob[safra].get(m, 0) / princ * 100
                # Fator de sobrevivência real aplicado sem multiplicador artificial.
                real_r_pure += inc_r * fs
            escala_r = real_r_pure / shape_acum_r if shape_acum_r > 0 else 1.0
        else:
            escala_p, escala_r = 1.0, 1.0

        escala_p = max(escala_p, 0.2)
        escala_r = max(escala_r, 0.2)

        acum_p, acum_r = 0.0, 0.0
        for mob in range(1, max_prazo + 1):
            if mob <= ml:
                inc_p = perda_por_mob[safra].get(mob, 0) / princ * 100
                inc_r = receita_por_mob[safra].get(mob, 0) / princ * 100
                tipo  = "realizado"
            else:
                inc_p = max(sh_p.get(mob, 0.001) * escala_p, 0.001)
                inc_r = max(sh_r.get(mob, 0.001) * escala_r, 0.0001)
                tipo  = "projetado"
            acum_p += inc_p
            acum_r += inc_r
            rows_perda.append({"safra": safra, "mob": mob,
                                "perda_acum_pct": round(acum_p, 4), "tipo": tipo})
            rows_receita.append({"safra": safra, "mob": mob,
                                  "receita_acum_pct": round(acum_r, 4), "tipo": tipo})

        safras_meta.append({
            "safra":                  safra,
            "principal":              round(princ, 2),
            "max_mob_limpo":          ml,
            "grupo_risco":            grupo_risco,
            "escala_perda":           round(escala_p, 4),
            "decay_receita":          round(decay_receita_grupo[grupo_risco], 4),
            "perda_acum_final_pct":   round(acum_p, 4),
            "receita_acum_final_pct": round(acum_r, 4),
        })

    return rows_perda, rows_receita, safras_meta


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Resian — Pipeline CSV para Valuation")
    parser.add_argument("--cliente",    default="")
    parser.add_argument("--data_base",  default="",
                        help="Formato AAAA-MM, ex: 2026-05")
    parser.add_argument("--bucket",     default="clientes-uploads")
    parser.add_argument("--local_dir")
    parser.add_argument("--output_dir", default="./output")
    parser.add_argument("--batch_size", type=int, default=5,
                        help="Safras por lote de processamento (default: 5). "
                             "Reduza se tiver pouca RAM disponível.")
    parser.add_argument("--force_local", type=str, default="",
                        help="Caminho fisico do INSTALLMENT LOCAL")
    args = parser.parse_args()

    if not args.local_dir and not args.bucket:
        print("\n[ERRO] Voce deve fornecer no minimo a flag --local_dir ou a flag --bucket.\n")
        sys.exit(1)

    if not args.data_base:
        raise ValueError("--data_base é obrigatório (formato: AAAA-MM)")

    from pathlib import Path
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    data_base = datetime.strptime(args.data_base, "%Y-%m")
    log.info(f"Cliente: {args.cliente or '(da meta.csv)'} | "
             f"Data-base: {args.data_base} | batch_size: {args.batch_size}")

    # ── Conexão e ingestão ───────────────────────────────────────────────────
    # --bucket "" disables S3/MinIO entirely (fully offline: reads everything
    # from --local_dir). Used by CI, where MinIO is not reachable.
    s3 = None
    if args.bucket:
        s3 = get_s3()

    meta   = load_meta(s3, args.bucket, args.cliente, args.data_base)
    loans  = load_loans(s3, args.bucket, local_dir=args.local_dir)
    renegs = load_renegociacoes(s3, args.bucket, local_dir=args.local_dir)

    log.info("Iniciando cálculos …")

    # ── Cálculos independentes de installments ───────────────────────────────
    kpis        = calc_kpis(loans, renegs)
    fat_atraso  = calc_faixas_atraso(loans)
    fat_prazo   = calc_faixas_prazo(loans)
    rating_rows, todos_rated = calc_rating(loans)
    uf_rows     = calc_uf(loans)

    col_perfil  = ("especie_beneficio"
                   if loans["especie_beneficio"].ne("N/I").any()
                   else "profissao")
    label_perfil = ("especie" if col_perfil == "especie_beneficio" else "profissao")
    perfil_rows  = calc_perfil(loans, col_perfil, label_perfil)

    esp_sit = calc_cruzamento(loans, "especie_beneficio", "situacao_beneficio", "especie")
    can_sit = calc_cruzamento(loans, "canal_originacao",  "situacao_beneficio", "canal")

    venc_rows = calc_vencimentario(loans, data_base)
    mat_o60, mat_sal = calc_matrizes_rating_bhv(loans)

    # Cálculos em lotes (installments — proteção de memória) ───────────────
    log.info(f"Iniciando processamento de installments em lotes (batch_size={args.batch_size}) …")

    # Resolve o caminho do local_dir se force_local via args e prioridade
    ldir = getattr(args, 'force_local', None) or args.local_dir

    (fpd_rows, comp_pag, rol_rows,
     effic60_ltm, effic90_ltm,
     ead_total, ead_inad,
     vpl_results, pmt_bruto, perda_est, fluxo_liq,
     rows_perda, rows_receita, safras_meta) = process_installments_em_lotes(
        s3, args.bucket, loans, data_base,
        ldir, batch_size=args.batch_size
    )


    pd_qty = kpis["over60_pct"]
    lgd    = effic90_ltm   # LGD = Effic 90 LTM (mesma definição usada em save_kpis)

    # ── Salvar CSVs ──────────────────────────────────────────────────────────
    log.info("Salvando CSVs …")

    save_kpis(kpis, meta, effic60_ltm, effic90_ltm,
              ead_total, ead_inad, vpl_results,
              pmt_bruto, perda_est, fluxo_liq, output_dir)

    save_csv(fat_atraso,  output_dir, "faixas_atraso.csv")
    save_csv(fat_prazo,   output_dir, "faixas_prazo.csv")
    save_csv(rating_rows, output_dir, "rating.csv")
    save_csv(uf_rows,     output_dir, "uf.csv")
    save_csv(perfil_rows, output_dir, "perfil.csv")
    save_csv(esp_sit,     output_dir, "especie_situacao.csv")
    save_csv(can_sit,     output_dir, "canal_situacao.csv")
    save_csv(fpd_rows,    output_dir, "fpd_safras.csv")
    save_csv(venc_rows,   output_dir, "vencimentario.csv")
    save_csv(comp_pag,    output_dir, "comportamento_pagamento.csv")
    save_csv(rol_rows,    output_dir, "rolagens.csv")

    # VPL cenários
    vpl_rows = []
    for taxa in TAXAS_VPL:
        for cenario, v in vpl_results[taxa].items():
            pct = round(v / kpis["principal_total"] * 100, 2) \
                  if kpis["principal_total"] > 0 else 0
            vpl_rows.append({
                "taxa_am_pct":  taxa * 100,
                "cenario":      cenario,
                "vpl_valor":    v,
                "vpl_pct_principal": pct,
            })
    save_csv(vpl_rows, output_dir, "vpl_cenarios.csv")

    # PE
    pe_rows = [{
        "parametro":  "PD (over60 em quantidade)",
        "valor":      round(pd_qty * 100, 2),
        "unidade":    "%",
        "definicao":  "n_over60 / n_total — em quantidade de contratos"
    }, {
        "parametro":  "EAD Total",
        "valor":      round(ead_total, 2),
        "unidade":    "R$",
        "definicao":  "PMT × prazo_original, medido no momento zero da concessão"
    }, {
        "parametro":  "EAD Inadimplente (over60)",
        "valor":      round(ead_inad, 2),
        "unidade":    "R$",
        "definicao":  "EAD dos contratos com dias_maior_atraso > 60, calculado como PMT × prazo_original"
    }, {
        "parametro":  "LGD = Effic 90 LTM",
        "valor":      round(lgd * 100, 4),
        "unidade":    "%",
        "definicao":  "Produto encadeado rolagens 61-90d a >180d, média LTM 12 meses"
    }, {
        "parametro":  "PE = EAD_total × PD × LGD",
        "valor":      round(ead_total * pd_qty * lgd, 2),
        "unidade":    "R$",
        "definicao":  "Perda Esperada = EAD Total × Probabilidade de Default (PD) × Loss Given Default (LGD)"
    }, {
        "parametro":  "PE % do principal",
        "valor":      round(ead_total * pd_qty * lgd / kpis["principal_total"] * 100, 2)
                      if kpis["principal_total"] > 0 else 0,
        "unidade":    "%",
        "definicao":  "PE / principal originado total"
    }]
    save_csv(pe_rows, output_dir, "parametros_pe.csv")

    # Perda e receita acumuladas (com coluna tipo: realizado/projetado)
    save_csv(rows_perda,   output_dir, "perda_acumulada.csv")
    save_csv(rows_receita, output_dir, "receita_acumulada.csv")
    save_csv(safras_meta,  output_dir, "safras_meta.csv")

    # Matrizes rating × BHV
    save_csv(mat_o60, output_dir, "matriz_over60.csv")
    save_csv(mat_sal, output_dir, "matriz_saldo.csv")

    # Metadata de execução
    meta_exec = [{
        "chave": "todos_rated",
        "valor": str(todos_rated)
    }, {
        "chave": "col_perfil_usada",
        "valor": col_perfil
    }, {
        "chave": "label_perfil",
        "valor": label_perfil
    }, {
        "chave": "effic60_ltm_pct",
        "valor": round(effic60_ltm * 100, 4)
    }, {
        "chave": "effic90_ltm_pct",
        "valor": round(effic90_ltm * 100, 4)
    }, {
        "chave": "taxa_vpl_1",
        "valor": TAXAS_VPL[0] * 100
    }, {
        "chave": "taxa_vpl_2",
        "valor": TAXAS_VPL[1] * 100
    }]
    save_csv(meta_exec, output_dir, "meta_execucao.csv")

    log.info("")
    log.info("═" * 60)
    log.info("PIPELINE CONCLUÍDO")
    log.info(f"Diretório de saída: {output_dir.resolve()}")
    log.info(f"Arquivos gerados:   19 CSVs")
    log.info("")
    log.info("Próximo passo: trazer os CSVs para o Claude.ai")
    log.info("e solicitar a geração do relatório DOCX.")
    log.info("═" * 60)


if __name__ == "__main__":
    main()

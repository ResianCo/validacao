# AGENTS.md

Two-script credit-portfolio valuation pipeline (Resian Consultoria, Brazilian
consignado / INSS). No tests, no `requirements.txt`. Inputs are large
CSVs; outputs are ~19 CSVs handed off to Claude.ai to produce a DOCX report.

## Layout

```
validacao/
├── bin/        # runnable scripts: pipeline_csv.py, passo0_validacao.py
├── input/      # raw CSVs (LOANS/INSTALLMENTS/RENEG/META) — LFS-tracked
├── output/     # generated 19 CSVs — gitignored, recreated each run
├── reconcile/  # audit scripts + reports (DOCX-vs-XLSX, pipeline-vs-DOCX)
├── Banco_ABC_Valuation_Resian_2026-05_1.docx   # GOLDEN reference (root)
└── 20260612 Teste - Análise de Carteira.xlsx.zip  # XLSX first-part (root)
```

The golden DOCX and XLSX live at the repo root (not in `input/`); `input/` holds
only the raw CSVs the pipeline consumes.

## Source of truth (golden values) — READ FIRST

- **The real golden copy is the DOCX** (`Banco_ABC_Valuation_Resian_2026-05_1.docx`,
  at repo root). Its values are the authoritative reference; `bin/pipeline_csv.py`
  reproduces them from the raw CSVs (principal 204,222,608.95; EAD 307,442,757.64;
  LGD 92.25%; Over-60 1,192/23.84%).
- **The XLSX (`20260612 Teste - Análise de Carteira.xlsx.zip`, at repo root) is NOT
  wrong — it uses a different calculation method.** Its money is scaled ÷10 and its
  LGD is 91.5301% (vs the DOCX's 92.25%); these are methodological differences, not
  errors. It is a "first part" working model (raw loans with UF/Idade, Ever60 curves,
  per-safra PE). The `.zip` is a zip-of-xlsx; the reconcile scripts auto-extract.
- **For now, compare CSV outputs against the DOCX ONLY.** Do not treat XLSX values as
  the target. See `reconcile/XLSX_vs_DOCX_differences.pdf` and `reconcile/AUDIT-REPORT.md`.

## Run order (mandatory)

```bash
pip install boto3 duckdb pandas numpy python-dateutil --break-system-packages

# 1. Validate input layout FIRST. Exits 1 on blocking errors → do NOT run pipeline.
python3 bin/passo0_validacao.py --data_base 2026-05 --local_dir input

# 2. Only if passo0 exits 0:
python3 bin/pipeline_csv.py --cliente "Banco ABC" --data_base 2026-05 --local_dir input --output_dir output
```

`--local_dir` reads CSVs from disk (offline). Omit it (and keep `--bucket`) to
stream from MinIO instead. `MINIO_URL` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`
override the hardcoded Resian defaults; bucket default is `clientes-uploads`.

`pipeline_csv.py --data_base` is **required** (format `AAAA-MM`). `--batch_size`
(default 5) only matters for memory pressure — lower it if RAM is tight.

## Reconcile (verify CSVs against the golden DOCX)

```bash
pip install pandas openpyxl pypdf   # reconcile deps (see reconcile/requirements.txt)

# Full audit: pipeline output vs DOCX (Layer A) + XLSX internal + UF lineage (Layer B).
python3 reconcile/audit.py output
# Run-once gate: DOCX vs XLSX only (where the two golden stages agree/diverge).
python3 reconcile/validate_docx_vs_xlsx.py
```

Both scripts resolve paths repo-relative (no hardcoded `/Users/...`). They read the
golden DOCX/XLSX read-only and write reports into `reconcile/`. Expect 99/107 checks
passing against the DOCX — the 8 known failures (1 PE, 6 VPL, 1 XLSX ever60
monotonicity) are pre-existing methodology differences, not regressions.

## Memory invariants — DO NOT BREAK (machine freezes, needs reboot)

Target VM: 24 GB RAM / 4 CPU / 100 GB disk, **no swap**. Scanning
`INSTALLMENTS.csv` (~72 M rows / ~4.9 GB) without the limits below OOMs the
kernel and freezes the box. Four "regras de ouro" in `pipeline_csv.py`:

1. Never `obj["Body"].read()` from S3 — always `s3.download_file` (stream to disk).
2. Cross-table joins via DuckDB `INNER JOIN` against the pandas-registered
   `df_loans`. Never build a giant Python `WHERE id IN (...)` string list.
3. Never call `.df()` or `.iterrows()` on INSTALLMENTS — iterate the DuckDB
   cursor with `fetchmany(50_000)`.
4. Each installment row becomes a `namedtuple` `Parcela` (~3–5× less RAM than a
   dict). Do not swap for dict.

DuckDB is pinned via `duck_connect()`: `memory_limit='6GB'`, `threads=3`,
`temp_directory=<local_dir>/duck_tmp`, spill up to 120 GB. Mirror these in
`passo0_validacao._aplicar_limites_duck` if you touch DuckDB there. Never raise
`memory_limit` without confirming free RAM at runtime.

## Silent-failure trap: `id_contrato` type

`load_loans()` forces `id_contrato = astype(str).str.strip()` (pipeline_csv.py:256-269).
The DuckDB side keys by `TRIM(CAST(... AS VARCHAR))` — i.e. **strings**. If
`id_contrato` is left numeric on the pandas side, `loan_map.get(id_str)` in
`calc_fpd` and `_build_buckets_mes` silently matches nothing and `rolagens.csv`
ships completely empty. Do not undo this normalization anywhere.

## INSTALLMENTS Parquet cache

First run converts `INSTALLMENTS.csv` → `INSTALLMENTS.parquet` (ZSTD) once and
caches it in `local_dir`; all later batches scan the parquet (column/predicate
pushdown) instead of re-sniffing the 4.9 GB CSV. **Delete the `.parquet` only
when the source CSV has changed.** `load_installments()` raises if neither
exists and no S3 client is configured — i.e. installments are NOT streamed
directly from S3; you must use `--local_dir` (or `--force_local`).

## Parsing conventions (preserve, don't simplify)

- **Dates**: `parse_date` accepts `%d/%m/%Y`, `%Y-%m-%d`, `%Y-%m-%d %H:%M:%S`
  (BR-first). Mirrored in `passo0_validacao.DATE_FORMATS`.
- **Decimals**: `parse_float` handles both `1.234,56` (BR) and `1,234.56` (US),
  plus bare-comma. LOANS numeric cols are routed through it.
- **`taxa_cliente`**: divided by 100 only when `> 1.0` — entries are either a
  fraction (0.0154) or a percent (1.54). Don't change the threshold.
- **Column aliases** in `load_loans`: `"unique id"` and `"[srm] codigo operacao"`
  both map to `id_contrato`. Different bank exports use different layouts.
- **Output CSVs**: written with `sep=";"`, `encoding="utf-8-sig"` (BOM) — for
  Brazilian Excel. Don't switch to `,`.

## Methodology constants — business-defined, not arbitrary

`pipeline_csv.py:51-55`: `CORTE_MOBS=2` (drop last 2 MOBs to avoid
contamination), `DECAY_PERDA=0.85`, `DECAY_RECEITA=0.92`,
`TAXAS_VPL=[0.01, 0.015]` (1.0 % and 1.5 % a.m.). LGD is defined as
`effic90_ltm` (rolagem chain 61–90 d → >180 d, LTM 12 m). Don't tweak without
sign-off — they drive the valuation output.

## Inputs / outputs

Inputs live in `input/` (one client per folder): `LOANS.csv`, `INSTALLMENTS.csv`,
`RENEGOCIACAO.csv` (may be empty), `META.csv` (case-insensitive on disk;
read as `META.csv` from bucket). `passo0_validacao.py` documents the exact
required/optional columns and null rules per file — keep it and the pipeline
in sync when changing schemas. These CSVs are tracked via Git LFS (see
`.gitattributes`); run `git lfs pull` after clone or they'll be pointer stubs.

Outputs land in `--output_dir` (default `output/`, gitignored): `kpis.csv`, `vpl_cenarios.csv`,
`parametros_pe.csv`, `rolagens.csv`, `faixas_atraso.csv`, `faixas_prazo.csv`,
`rating.csv`, `uf.csv`, `perfil.csv`, `especie_situacao.csv`, `canal_situacao.csv`,
`fpd_safras.csv`, `vencimentario.csv`, `comportamento_pagamento.csv`,
`perda_acumulada.csv`, `receita_acumulada.csv`, `safras_meta.csv`,
`matriz_over60.csv`, `matriz_saldo.csv`, `meta_execucao.csv`.

There is **no automated report step** — the DOCX (e.g.
`Banco_ABC_Valuation_Resian_2026-05_1.docx`) and any `*-EXECUTIVE_REPORT_*.pdf`
are produced externally (Claude.ai / analyst). Don't try to regenerate them
from this repo.

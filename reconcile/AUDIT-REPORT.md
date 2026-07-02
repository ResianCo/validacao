# AUDIT REPORT — validacao golden folder

**97/105 checks passed — RED**

Two-layer model: **XLSX = first part** (raw/intermediate), **DOCX = final** (delivered report).
`pipeline_csv.py` output is the engine that reproduces the DOCX from the CSV inputs.

## Priority-zero findings (anomalies needing your interpretation)

1. **Money ÷10 between stages.** XLSX (first part) carries all money at 1/10 of the DOCX (final). DOCX principal = R$ 204,222,608.95. See DOCS_VS_XLSX.md. Intentional stage scaling, or a workbook unit issue?

2. **LGD differs between stages.** DOCX = 92.25%; XLSX PE-sheet = 91.5301%. Methodology value changed between first-part and final.

3. **VPL pipeline vs DOCX ~0.1815% off** (beyond the 0.1% bound; e.g. base@1% pipe 136.35M vs DOCX 136.54M). The throwaway pipeline is not bit-exact on VPL.

4. **DOCX UF table sourced from XLSX, not LOANS.csv.** `LOANS.csv` has no `uf` column; the DOCX UF cut derives from the XLSX `Loans` sheet (which has UF). UF contrato counts reconcile XLSX→DOCX (see Layer B / lineage).

5. **PDF is an architecture report**, not a valuation render — checked only that it cites the DOCX golden PE/LGD.

## Notes

- XLSX ever60: 48 safra curves, 3 non-monotonic (2025-09, 2025-10, 2025-11 — all youngest safras, likely a workbook artifact).
- XLSX->DOCX UF lineage: 10/10 UF contrato counts match (principal is ÷10 in XLSX, expected).

## All checks

| check | computed | golden | diff | tol | result | note |
|---|---|---|---|---|---|---|
| A·KPI contratos | 5000.0 | 5000.0 | 0.0 | 0 | PASS |  |
| A·KPI principal_total | 204222608.95 | 204222608.95 | 0.0 | 1.0 | PASS |  |
| A·KPI over60_n | 1192.0 | 1192.0 | 0.0 | 0 | PASS |  |
| A·KPI over60_pct | 23.84 | 23.84 | 0.0 | 0.01 | PASS |  |
| A·KPI ead_total | 307442757.64 | 307442757.64 | 0.0 | 1.0 | PASS |  |
| A·PE valor | 67616819.25 | 67615873.39 | 945.86 | 1.0 | **FAIL** |  |
| A·rating A contratos | 830.0 | 830.0 | 0.0 | 0 | PASS |  |
| A·rating A principal | 34121915.81 | 34121916.0 | 0.19 | 1.0 | PASS |  |
| A·rating A over60_n | 201.0 | 201.0 | 0.0 | 0 | PASS |  |
| A·rating B contratos | 850.0 | 850.0 | 0.0 | 0 | PASS |  |
| A·rating B principal | 34265254.68 | 34265255.0 | 0.32 | 1.0 | PASS |  |
| A·rating B over60_n | 212.0 | 212.0 | 0.0 | 0 | PASS |  |
| A·rating C contratos | 829.0 | 829.0 | 0.0 | 0 | PASS |  |
| A·rating C principal | 34170008.08 | 34170008.0 | 0.08 | 1.0 | PASS |  |
| A·rating C over60_n | 187.0 | 187.0 | 0.0 | 0 | PASS |  |
| A·rating D contratos | 846.0 | 846.0 | 0.0 | 0 | PASS |  |
| A·rating D principal | 33597439.04 | 33597439.0 | 0.04 | 1.0 | PASS |  |
| A·rating D over60_n | 201.0 | 201.0 | 0.0 | 0 | PASS |  |
| A·rating E contratos | 819.0 | 819.0 | 0.0 | 0 | PASS |  |
| A·rating E principal | 35010910.69 | 35010911.0 | 0.31 | 1.0 | PASS |  |
| A·rating E over60_n | 205.0 | 205.0 | 0.0 | 0 | PASS |  |
| A·rating HR contratos | 826.0 | 826.0 | 0.0 | 0 | PASS |  |
| A·rating HR principal | 33057080.65 | 33057081.0 | 0.35 | 1.0 | PASS |  |
| A·rating HR over60_n | 186.0 | 186.0 | 0.0 | 0 | PASS |  |
| A·faixas 'Corrente (0 dias)' contratos | 3804.0 | 3804.0 | 0.0 | 0 | PASS |  |
| A·faixas 'Corrente (0 dias)' pct | 76.08 | 76.08 | 0.0 | 0.01 | PASS |  |
| A·faixas '1 a 30 dias' contratos | 0.0 | 0.0 | 0.0 | 0 | PASS |  |
| A·faixas '1 a 30 dias' pct | 0.0 | 0.0 | 0.0 | 0.01 | PASS |  |
| A·faixas '31 a 60 dias' contratos | 4.0 | 4.0 | 0.0 | 0 | PASS |  |
| A·faixas '31 a 60 dias' pct | 0.08 | 0.08 | 0.0 | 0.01 | PASS |  |
| A·faixas '61 a 90 dias' contratos | 16.0 | 16.0 | 0.0 | 0 | PASS |  |
| A·faixas '61 a 90 dias' pct | 0.32 | 0.32 | 0.0 | 0.01 | PASS |  |
| A·faixas 'Acima de 90 dias' contratos | 1176.0 | 1176.0 | 0.0 | 0 | PASS |  |
| A·faixas 'Acima de 90 dias' pct | 23.52 | 23.52 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2023-12 | 17.14 | 17.14 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2023-12 | 17.33 | 17.33 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-01 | 20.18 | 20.18 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-01 | 21.26 | 21.26 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-02 | 17.17 | 17.17 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-02 | 17.9 | 17.9 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-03 | 15.42 | 15.42 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-03 | 15.43 | 15.43 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-04 | 14.52 | 14.52 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-04 | 17.92 | 17.92 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-05 | 16.88 | 16.88 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-05 | 17.0 | 17.0 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-06 | 12.68 | 12.68 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-06 | 11.74 | 11.74 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-07 | 19.7 | 19.7 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-07 | 20.99 | 20.99 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-08 | 17.39 | 17.39 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-08 | 19.24 | 19.24 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-09 | 20.71 | 20.71 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-09 | 20.67 | 20.67 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-10 | 18.18 | 18.18 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-10 | 16.57 | 16.57 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-11 | 15.94 | 15.94 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-11 | 14.62 | 14.62 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2024-12 | 11.48 | 11.48 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2024-12 | 12.18 | 12.18 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-01 | 14.61 | 14.61 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-01 | 11.76 | 11.76 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-02 | 15.23 | 15.23 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-02 | 17.21 | 17.21 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-03 | 12.44 | 12.44 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-03 | 11.62 | 11.62 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-04 | 18.8 | 18.8 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-04 | 18.39 | 18.39 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-05 | 14.85 | 14.85 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-05 | 10.95 | 10.95 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-06 | 23.08 | 23.08 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-06 | 21.52 | 21.52 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-07 | 17.94 | 17.94 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-07 | 20.57 | 20.57 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-08 | 19.57 | 19.57 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-08 | 22.96 | 22.96 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-09 | 16.06 | 16.06 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-09 | 16.91 | 16.91 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-10 | 19.38 | 19.38 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-10 | 20.55 | 20.55 | 0.0 | 0.01 | PASS |  |
| A·FPD qtd% 2025-11 | 23.33 | 23.33 | 0.0 | 0.01 | PASS |  |
| A·FPD val% 2025-11 | 25.49 | 25.49 | 0.0 | 0.01 | PASS |  |
| A·VPL otimista@vpl_1pct | 144085150.12 | 144241042.0 | 0.001081 | 0.001 | **FAIL** | rel diff 0.108% |
| A·VPL otimista@vpl_1_5pct | 132069989.67 | 132206027.0 | 0.001029 | 0.001 | **FAIL** | rel diff 0.103% |
| A·VPL base@vpl_1pct | 136346961.28 | 136541826.0 | 0.001427 | 0.001 | **FAIL** | rel diff 0.143% |
| A·VPL base@vpl_1_5pct | 124979162.62 | 125149209.0 | 0.001359 | 0.001 | **FAIL** | rel diff 0.136% |
| A·VPL pessimista@vpl_1pct | 128608772.44 | 128842610.0 | 0.001815 | 0.001 | **FAIL** | rel diff 0.181% |
| A·VPL pessimista@vpl_1_5pct | 117888335.57 | 118092391.0 | 0.001728 | 0.001 | **FAIL** | rel diff 0.173% |
| A·self rating Σcontratos | 5000.0 | 5000.0 | 0.0 | 0 | PASS |  |
| A·self rating Σprincipal | 204222610.0 | 204222608.95 | 1.05 | 5.0 | PASS | docx rating principals are integer-rounded |
| A·self faixas Σcontratos | 5000.0 | 5000.0 | 0.0 | 0 | PASS |  |
| A·self faixas acumulado ends 100 | 100.0 | 100.0 | 0.0 | 0.01 | PASS |  |
| B·XLSX Loans count | 5000 | 5000.0 | 0.0 | 0 | PASS |  |
| B·XLSX Resumo contratos | 5000.0 | 5000.0 | 0.0 | 0 | PASS |  |
| B·Ever60 curves monotonic (non-decreasing) | 3 | 0 | 3.0 | 0 | **FAIL** | 48 safra curves checked; non-monotonic: 2025-09, 2025-10, 2025-11 |
| B·UF BA contratos (XLSX->DOCX) | 536 | 536.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF SC contratos (XLSX->DOCX) | 532 | 532.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF PE contratos (XLSX->DOCX) | 505 | 505.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF RJ contratos (XLSX->DOCX) | 499 | 499.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF RS contratos (XLSX->DOCX) | 509 | 509.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF GO contratos (XLSX->DOCX) | 496 | 496.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF SP contratos (XLSX->DOCX) | 490 | 490.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF PR contratos (XLSX->DOCX) | 477 | 477.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF MG contratos (XLSX->DOCX) | 487 | 487.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |
| B·UF CE contratos (XLSX->DOCX) | 469 | 469.0 | 0.0 | 0 | PASS | principal ratio xlsx/docx=0.1 |

## XLSX → DOCX UF lineage

| UF | xlsx count | docx count | match | principal ratio (xlsx/docx) |
|---|---|---|---|---|
| BA | 536 | 536.0 | yes | 0.1 |
| SC | 532 | 532.0 | yes | 0.1 |
| PE | 505 | 505.0 | yes | 0.1 |
| RJ | 499 | 499.0 | yes | 0.1 |
| RS | 509 | 509.0 | yes | 0.1 |
| GO | 496 | 496.0 | yes | 0.1 |
| SP | 490 | 490.0 | yes | 0.1 |
| PR | 477 | 477.0 | yes | 0.1 |
| MG | 487 | 487.0 | yes | 0.1 |
| CE | 469 | 469.0 | yes | 0.1 |
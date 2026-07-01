# XLSX vs DOCX — What's Different

**XLSX** (`20260612 Teste - Análise de Carteira.xlsx`) — **first part**: the raw / intermediate working model.
**DOCX** (`Banco_ABC_Valuation_Resian_2026-05_1.docx`) — **final part**: the delivered valuation report.

Both documents are golden, at different stages of the same pipeline. This report covers **only where they diverge**. Scale-invariant figures that agree (contratos = 5,000; FPD % on the overlapping safras) are omitted.

![Diagram](diagram.png)

---

## 1. Money is scaled ÷10 in the XLSX

Every monetary value in the XLSX equals the DOCX value **÷ 10**. The ratio is exactly **0.10** across all 10 UFs and at the raw-loan level. Counts (contratos, parcelas) and money-ratios (FPD %, Over %) are **not** affected — only absolute money.

| Field | DOCX (final) | XLSX (first part) | DOCX ÷ XLSX |
|---|---:|---:|---:|
| Principal total | R$ 204,222,608.95 | R$ 20,422,260.895 | 10.0× |
| Contract `…00001` principal | R$ 78,298.27 | R$ 7,829.827 | 10.0× |
| Contract `…00001` PMT | R$ 1,821.11 | R$ 182.111 | 10.0× |
| UF `BA` principal | R$ 22,367,824 | R$ 2,236,782.4 | 10.0× |
| UF `MG` principal | R$ 19,242,659 | R$ 1,924,265.9 | 10.0× |

> **Needs interpretation:** is the XLSX intentionally kept in a different unit (e.g. a scaled working column), or is this a workbook unit bug? The DOCX (final) values match `LOANS.csv` exactly, so the **DOCX is correct in absolute terms**; the XLSX is the one that's scaled.

## 2. LGD differs by ~0.72 pp

| Stage | LGD (Effic 90 LTM) |
|---|---:|
| DOCX (final) | **92.25%** |
| XLSX PE-sheet (first part) | **91.5301%** |

Because the two stages use different LGD, any LGD-driven figure (PE, VPL) diverges between them unless recomputed with a common LGD.

## 3. Coverage — each holds content the other doesn't

| Only in XLSX (first part) | Only in DOCX (final) |
|---|---|
| Raw `Loans` sheet with **UF / Idade / Região** | Narrative interpretation (diagnoses, recommendations) |
| Raw `Installments` (parcela-level) | **VPL 3×2** sensitivity grid (cenários × taxas) |
| **Ever60 curves** (`Inadimplência 60`, safra × MOB) | Rating snapshot table (contratos/principal/over60/taxa/part) |
| `Carteira` monthly aging by DPD bucket | `Faixas de atraso` distribution |
| Per-safra **PE model** (PD × LGD × EAD, fractional) | Espécie / Canal / Prazo cuts |
| `Feriados` holiday reference | FPD×24 with `sinalização` |

The DOCX's **10-UF table is sourced from the XLSX `Loans` sheet**, not from `LOANS.csv` (which has no `uf` column). UF contrato counts reconcile 10/10 between XLSX → DOCX.

## 4. FPD coverage: 20 vs 24 safras

- **XLSX** FPD sheet: **20 safras** (2024-04 → 2025-11).
- **DOCX** FPD table: **24 safras** (2023-12 → 2025-11).

The 4 earliest safras — **2023-12, 2024-01, 2024-02, 2024-03** — appear only in the DOCX. On the 20-safra overlap, FPD count% and value% agree within rounding ($\leq$ 0.005 pp).

## 5. EAD / PE are expressed differently

- **DOCX**: absolute R$ (EAD Total R$ 307,442,757.64; PE R$ 67,615,873.39).
- **XLSX**: per-safra **fractions of origination** (EAD $\approx$ 0.72, PE $\approx$ 0.17 of origination), and origination itself is ÷10. The XLSX PE-sheet also uses a different PE formulation per safra, so its absolute EAD/PE are not directly comparable to the DOCX.

## 6. Cohort metric differs: Ever60 vs FPD

- **XLSX** `Inadimplência 60` = **count-based Ever-60** curve by safra × MOB (fraction of the cohort that ever reached 60 DPD).
- **DOCX** cohort table = **FPD** (first-payment default). The throwaway `pipeline_csv.py` emits a money-based `perda_acumulada`, not a count-based Ever60 — so Ever60 is effectively **XLSX-only** in this folder.

## 7. Data-quality quirk inside the XLSX

3 of the 48 Ever60 curves are **non-monotonic** (an Ever-curve should only ever rise):

| Safra | Observed |
|---|---|
| 2025-09 | drops at MOB 2→3 (0.17431 → 0.12872) |
| 2025-10 | drops at MOB 2→3 (0.20705 → 0.12872) |
| 2025-11 | drops at MOB 1→2 (0.23333 → 0.17991) |

All three are the **youngest safras**, and the recurring `0.12872` value points to a workbook formula / interpolation artifact for cohorts with very few observed MOBs.

## 8. Verdict — which values are correct (per the Python computation)

Running `pipeline_csv.py` on the raw CSVs (independent of either document) reproduces the **DOCX** and contradicts the **XLSX** on every disputed value.

| Value | Python (from raw CSVs) | DOCX | XLSX |
|---|---:|---:|---:|
| Principal total | 204,222,608.95 | 204,222,608.95 $\checkmark$ | 20,422,260.895 (÷10) $\times$ |
| Contract `…00001` principal | 78,298.27 | ×10 scale $\checkmark$ | 7,829.827 (÷10) $\times$ |
| EAD total | 307,442,757.64 | 307,442,757.64 $\checkmark$ | fractional / ÷10 $\times$ |
| LGD (Effic90, computed) | 92.2538% | 92.25% $\checkmark$ | 91.5301% $\times$ |
| Over-60 (n) | 1,192 | 1,192 $\checkmark$ | 1,192 $\checkmark$ (scale-invariant) |
| Rating / faixas / FPD×24 | computed | all match $\checkmark$ | only FPD% matches |

Two checks that don't even depend on the pipeline both favor the DOCX:

- **Principal = direct sum of `LOANS.csv`** = 204,222,608.95 — matches the DOCX exactly, 10× the XLSX. The XLSX's money is at one-tenth scale.
- **LGD = computed Effic-90 roll-through from `INSTALLMENTS.csv`** = 92.2538% — rounds to the DOCX's 92.25%, not the XLSX's 91.53%.

The XLSX agrees only on **scale-invariant** figures (contratos = 5,000, FPD %). On absolute money and LGD it diverges from the raw-data computation.

Minor caveats (both still land closer to the DOCX): PE differs 0.0014% (pipeline LGD 92.2538% vs DOCX-stated 92.25%); VPL differs $\approx$ 0.10–0.18% (the throwaway is not bit-exact). Neither is comparable to the XLSX (fractional + ÷10).

**Conclusion: the DOCX values are the golden reference** — it is what `pipeline_csv.py` reproduces from the raw CSVs. The XLSX is **not wrong**; it uses a **different calculation method** (money at ÷10 scale, LGD 91.5301%), so its absolute money and LGD do not match the raw-data computation. **Going forward, compare CSV outputs against the DOCX only.**

---

*Agreements (for context, not the focus): contratos = 5,000 in both; the 20 overlapping FPD safras match on count% and value%; the DOCX UF table reconciles to the XLSX `Loans` sheet on contrato counts.*

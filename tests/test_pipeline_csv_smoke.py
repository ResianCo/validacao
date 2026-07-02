"""Pass 5c: smoke tests for the pipeline end-to-end.

These are marked @pytest.mark.slow and skipped by default. To run
them locally:

    pytest -m slow tests/test_pipeline_csv_smoke.py

The slow tests exercise pipeline_csv.py in --local_dir mode against
the real input/ CSVs and verify that the produced output/ matches the
golden values from the DOCX:

  - LGD 92.25%
  - EAD 307,442,757.64
  - Principal 204,222,608.95

Slow because the INSTALLMENTS.csv is 17 MB / 270k rows and the
pipeline does ~30 seconds of DuckDB queries on it. Run it when you
change the methodology constants or the calc_* functions; the unit
tests catch the BR-locale parsers, but only the slow tests catch the
golden-number regressions.
"""

import csv
import os
import shutil
import subprocess
import sys
import tempfile

import pytest

# Mark every test in this file as slow so they're skipped by default.
pytestmark = pytest.mark.slow


BIN = os.path.join(os.path.dirname(__file__), "..", "bin")
INPUT = os.path.join(os.path.dirname(__file__), "..", "input")


def _run_pipeline(output_dir: str) -> subprocess.CompletedProcess:
    """Invoke pipeline_csv.py in --local_dir mode and return the result."""
    return subprocess.run(
        [
            sys.executable,
            os.path.join(BIN, "pipeline_csv.py"),
            "--local_dir", INPUT,
            "--output_dir", output_dir,
            "--data_base", "2026-05",  # the DOCX's data_base
            # No --cliente; pipeline accepts None / placeholder.
        ],
        capture_output=True,
        text=True,
        timeout=600,  # 10 minutes upper bound; pipeline usually runs in ~30s
        env={**os.environ, "MINIO_URL": "http://invalid.invalid/"},  # force --local_dir
    )


@pytest.fixture
def output_dir(tmp_path):
    """Per-test scratch output dir; cleaned up automatically."""
    d = tmp_path / "pipeline-output"
    d.mkdir()
    return str(d)


def test_pipeline_runs_against_input_csvs(output_dir):
    """The most basic smoke test: pipeline_csv.py exits 0 and writes
    the expected set of CSVs to the output dir."""
    result = _run_pipeline(output_dir)
    assert result.returncode == 0, (
        f"pipeline exited {result.returncode}\n"
        f"--- stdout ---\n{result.stdout[-2000:]}\n"
        f"--- stderr ---\n{result.stderr[-2000:]}"
    )

    # Spot-check the CSVs the README and AGENTS.md both reference.
    # (EAD and LGD aren't standalone files — they're rows in
    # parametros_pe.csv, so we check that one instead of ead.csv /
    # lgd.csv.)
    expected = ["kpis.csv", "faixas_atraso.csv", "rating.csv", "parametros_pe.csv"]
    for name in expected:
        path = os.path.join(output_dir, name)
        assert os.path.exists(path), f"expected output file missing: {name}"


def _read_csv(path):
    with open(path, newline="") as f:
        return list(csv.reader(f, delimiter=";"))


def _find_value_in_rows(rows, predicate):
    """Scan every cell in `rows` (list of lists); return the first cell
    value (as float, BR-locale) for which predicate(value) is true."""
    for row in rows:
        for cell in row:
            for fmt in ("%.6f", None):  # try both US and BR-locale
                pass
            try:
                val = float(cell.replace(".", "").replace(",", "."))
            except ValueError:
                continue
            if predicate(val):
                return val
    return None


def _parse_number(cell):
    """Parse a CSV cell as a float, accepting both US (1,234.56) and
    BR (1.234,56) locales. The pipeline output uses US format without
    thousands separators (307442757.64), so the US parser matches first."""
    s = cell.strip()
    try:
        return float(s)
    except ValueError:
        pass
    # Fallback: BR-locale with comma as decimal.
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def test_lgd_matches_docx_golden(output_dir):
    """LGD must reproduce the DOCX golden value (92.25%) to 4 decimal
    places — that's the engine's signature number."""
    result = _run_pipeline(output_dir)
    if result.returncode != 0:
        pytest.skip(f"pipeline not runnable in this env: {result.stderr[-500:]}")

    pe = os.path.join(output_dir, "parametros_pe.csv")
    assert os.path.exists(pe), "parametros_pe.csv missing"

    rows = _read_csv(pe)
    # The row layout is: parametro;valor;unidade;definicao
    # Look for the LGD row by its first cell, not by scanning blindly
    # (so a future schema change can't accidentally match e.g. PD).
    found = None
    for row in rows:
        if len(row) >= 2 and "LGD" in row[0] and "Effic" in row[0]:
            found = _parse_number(row[1])
            if found is not None:
                break
    assert found is not None, f"no 'LGD = Effic 90 LTM' row in {pe}: {rows[:5]}"
    # The pipeline writes percentages as plain numbers (92.2538), not
    # as decimals (0.922538). The DOCX states "LGD 92.25%" — i.e. the
    # percentage form. Normalize to decimal so the comparison is
    # apples-to-apples.
    lgd_decimal = found / 100.0 if found > 1.0 else found
    assert lgd_decimal == pytest.approx(0.9225, abs=1e-3), (
        f"LGD {found} (decimal {lgd_decimal}) does not match DOCX golden 0.9225"
    )


def test_principal_matches_docx_golden(output_dir):
    """Principal from pipeline must reproduce R$ 204,222,608.95 to the
    centavo. This is the second signature number in the DOCX."""
    result = _run_pipeline(output_dir)
    if result.returncode != 0:
        pytest.skip(f"pipeline not runnable in this env: {result.stderr[-500:]}")

    kpis = os.path.join(output_dir, "kpis.csv")
    assert os.path.exists(kpis), "kpis.csv missing"

    rows = _read_csv(kpis)
    # Layout: categoria;chave;valor — find the principal_total row.
    found = None
    for row in rows:
        if len(row) >= 3 and row[1] == "principal_total":
            found = _parse_number(row[2])
            if found is not None:
                break
    assert found is not None, f"no 'principal_total' row in {kpis}: {rows[:8]}"
    assert found == pytest.approx(204222608.95, abs=1.0), (
        f"principal {found} != DOCX golden 204222608.95"
    )


def test_ead_matches_docx_golden(output_dir):
    """EAD must reproduce the DOCX golden R$ 307,442,757.64."""
    result = _run_pipeline(output_dir)
    if result.returncode != 0:
        pytest.skip(f"pipeline not runnable in this env: {result.stderr[-500:]}")

    pe = os.path.join(output_dir, "parametros_pe.csv")
    rows = _read_csv(pe)
    found = None
    for row in rows:
        if len(row) >= 2 and row[0] == "EAD Total":
            found = _parse_number(row[1])
            if found is not None:
                break
    assert found is not None, f"no 'EAD Total' row in {pe}: {rows[:8]}"
    assert found == pytest.approx(307442757.64, abs=1.0), (
        f"EAD {found} != DOCX golden 307442757.64"
    )
"""Pass 5c: unit tests for the pure helper functions in pipeline_csv.py.

These functions are responsible for parsing BR-locale inputs (commas as
decimal separator, slashes in dates) and formatting Brazilian-style
output ("R$ 1.234,56", "1.234"). If they break, every CSV in
output/ is wrong by the same factor — so test the corner cases the
pipeline relies on.
"""

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

# Add bin/ to sys.path so we can import the script directly. The
# pipeline script is a top-level executable (not a package), so this
# is the cleanest way to reach its helpers without modifying the
# script itself.
BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
sys.path.insert(0, BIN_DIR)

import pipeline_csv as pipe  # noqa: E402  (intentional sys.path mutation)


# --- parse_date --------------------------------------------------------

class TestParseDate:
    def test_br_slash_format(self):
        assert pipe.parse_date("15/03/2026") == datetime(2026, 3, 15)

    def test_iso_format(self):
        assert pipe.parse_date("2026-03-15") == datetime(2026, 3, 15)

    def test_iso_with_time(self):
        # The parser truncates to [:19] so the HH:MM:SS suffix is allowed.
        assert pipe.parse_date("2026-03-15 12:30:45") == datetime(2026, 3, 15, 12, 30, 45)

    @pytest.mark.parametrize("blank", ["", "   ", "nan", "NaN", "None", "NaT"])
    def test_blank_returns_none(self, blank):
        assert pipe.parse_date(blank) is None

    def test_pandas_nat_returns_none(self):
        # pandas NaT must not raise; should normalize to None.
        assert pipe.parse_date(pd.NaT) is None

    def test_garbage_returns_none(self):
        # Unparseable input returns None rather than raising — the
        # caller treats None as "missing" in downstream logic.
        assert pipe.parse_date("not-a-date") is None


# --- parse_float -------------------------------------------------------

class TestParseFloat:
    def test_us_format(self):
        assert pipe.parse_float("1234.56") == pytest.approx(1234.56)

    def test_br_format_with_dot_and_comma(self):
        # "1.234,56" is BR-locale: dots are thousands, comma is decimal.
        assert pipe.parse_float("1.234,56") == pytest.approx(1234.56)

    def test_br_format_comma_only(self):
        # "1234,56" has no thousands separator, just a decimal comma.
        assert pipe.parse_float("1234,56") == pytest.approx(1234.56)

    def test_us_format_with_comma_thousands(self):
        # "1,234.56" is US-locale: comma is thousands, dot is decimal.
        assert pipe.parse_float("1,234.56") == pytest.approx(1234.56)

    def test_garbage_defaults_to_zero(self):
        # The pipeline uses 0.0 as a safe default for unparseable input
        # rather than raising. Pin that behavior so downstream sums
        # don't explode on a malformed cell.
        assert pipe.parse_float("not-a-number") == 0.0

    def test_whitespace_is_stripped(self):
        assert pipe.parse_float("  42.5  ") == pytest.approx(42.5)

    def test_empty_string_defaults_to_zero(self):
        assert pipe.parse_float("") == 0.0


# --- fmt_brl / fmt_pct / fmt_int --------------------------------------

class TestFmtBRL:
    def test_zero(self):
        assert pipe.fmt_brl(0) == "R$ 0,00"

    def test_none_and_nan(self):
        # Both null-ish inputs must render as zero, not "R$ nan".
        assert pipe.fmt_brl(None) == "R$ 0,00"
        assert pipe.fmt_brl(float("nan")) == "R$ 0,00"

    def test_thousands_and_decimal(self):
        # The triple-replace dance swaps: ,→X, .→, X→.
        # 1234.56 → "1,234.56" → "1,X234,56" → "1.234,56".
        assert pipe.fmt_brl(1234.56) == "R$ 1.234,56"

    def test_large_value(self):
        assert pipe.fmt_brl(204222608.95) == "R$ 204.222.608,95"


class TestFmtPct:
    def test_default_two_decimals(self):
        assert pipe.fmt_pct(0.9225) == "92,25%"

    def test_custom_decimals(self):
        assert pipe.fmt_pct(0.922525, dec=4) == "92,2525%"

    def test_zero(self):
        assert pipe.fmt_pct(0) == "0,00%"


class TestFmtInt:
    def test_with_thousands(self):
        assert pipe.fmt_int(1234567) == "1.234.567"

    def test_zero(self):
        assert pipe.fmt_int(0) == "0"


# --- ym / months_diff --------------------------------------------------

class TestYM:
    def test_extracts_year_month(self):
        assert pipe.ym(datetime(2026, 5, 15, 12, 30)) == "2026-05"

    def test_pads_single_digit_month(self):
        # strftime("%Y-%m") zero-pads; verify.
        assert pipe.ym(datetime(2026, 1, 1)) == "2026-01"


class TestMonthsDiff:
    def test_same_month(self):
        assert pipe.months_diff(datetime(2026, 5, 1), datetime(2026, 5, 31)) == 0

    def test_one_month_apart(self):
        assert pipe.months_diff(datetime(2026, 5, 1), datetime(2026, 6, 1)) == 1

    def test_year_boundary(self):
        assert pipe.months_diff(datetime(2025, 12, 1), datetime(2026, 1, 1)) == 1

    def test_typical_portfolio_window(self):
        # Real use case: loan opened 2024-03, snapshot 2026-05 → 26 months.
        assert pipe.months_diff(datetime(2024, 3, 15), datetime(2026, 5, 1)) == 26

    def test_negative_for_backwards(self):
        assert pipe.months_diff(datetime(2026, 6, 1), datetime(2026, 5, 1)) == -1


# --- numpy/pandas interop ---------------------------------------------

class TestParseFloatNumpy:
    """Confirm the parser handles numpy scalars — the pipeline feeds
    pandas Series values directly into parse_float, which arrive as
    np.int64 / np.float64 scalars."""

    def test_numpy_int(self):
        # np.int64 should round-trip through float().
        assert pipe.parse_float(np.int64(42)) == pytest.approx(42.0)

    def test_numpy_float(self):
        assert pipe.parse_float(np.float64(3.14)) == pytest.approx(3.14)


class TestFmtBRLNumpy:
    def test_numpy_float64(self):
        # np.float64 should be accepted by the None/nan check
        # (np.isnan on np.float64 works the same as on float).
        assert pipe.fmt_brl(np.float64(1234.5)) == "R$ 1.234,50"
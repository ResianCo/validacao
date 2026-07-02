"""Pass 5c: unit tests for the pure helpers in passo0_validacao.py.

parse_date_safe and normalizar_colunas are the two pure helpers that
the script uses before any I/O. Pinning their behavior keeps the
pre-flight validator reliable.
"""

from datetime import datetime

import pandas as pd
import pytest

import passo0_validacao as p0


# --- parse_date_safe ---------------------------------------------------

class TestParseDateSafe:
    """parse_date_safe differs from parse_date in pipeline_csv.py by
    using a tri-state return:
      None  = absent (NaN / blank / None / NaT)
      False = present but invalid (unparseable)
      datetime = parsed successfully

    The validator uses False to flag "the row had a value but it was
    garbage" — a different failure mode from "the row was missing".

    The DATE_FORMATS list matches pipeline_csv.py so the two parsers
    agree on what counts as a valid date."""

    def test_br_format(self):
        assert p0.parse_date_safe("15/03/2026") == datetime(2026, 3, 15)

    def test_iso_format(self):
        assert p0.parse_date_safe("2026-03-15") == datetime(2026, 3, 15)

    def test_iso_with_time(self):
        assert p0.parse_date_safe("2026-03-15 12:30:45") == datetime(2026, 3, 15, 12, 30, 45)

    @pytest.mark.parametrize("blank", ["", "   ", "nan", "None", "NaT"])
    def test_absent_returns_none(self, blank):
        # None = "the cell was empty" → caller skips the row check.
        # The blank set is matched case-sensitively: lowercase "nan",
        # title-case "NaT", bare "None". "NaN" (with capital N) is NOT
        # in the set — see test_garbage_returns_false for that case.
        assert p0.parse_date_safe(blank) is None

    def test_pandas_nat_is_absent(self):
        assert p0.parse_date_safe(pd.NaT) is None

    def test_garbage_returns_false(self):
        # False = "the cell had content but it's unparseable" → caller
        # records an error. Critical distinction from None.
        assert p0.parse_date_safe("not-a-date") is False
        assert p0.parse_date_safe("13/13/2026") is False  # bad month
        assert p0.parse_date_safe("2026-13-45") is False   # bad date parts
        # "NaN" (capital N) is not in the absent set, so it falls through
        # to the strptime loop, which fails → False.
        assert p0.parse_date_safe("NaN") is False

    def test_distinguishes_none_from_false(self):
        # Pin the tri-state semantics: this is the behavior the validator
        # depends on for its error report counts.
        assert p0.parse_date_safe("") is None
        assert p0.parse_date_safe("garbage") is False
        assert p0.parse_date_safe("15/03/2026") == datetime(2026, 3, 15)


# --- normalizar_colunas -----------------------------------------------

class TestNormalizarColunas:
    def test_lowercases(self):
        df = pd.DataFrame(columns=["ID_Contrato", "PRINCIPAL", "Dias_Maior_Atraso"])
        out = p0.normalizar_colunas(df)
        assert list(out.columns) == ["id_contrato", "principal", "dias_maior_atraso"]

    def test_strips_whitespace(self):
        df = pd.DataFrame(columns=["  id_contrato  ", "\tprincipal\n", " dias_maior_atraso "])
        out = p0.normalizar_colunas(df)
        assert list(out.columns) == ["id_contrato", "principal", "dias_maior_atraso"]

    def test_lowercase_and_strip_combined(self):
        # Real Resian CSVs come from Excel-exported CSVs with mixed case
        # + surrounding whitespace; the normalizer must handle both at once.
        df = pd.DataFrame(columns=["  ID_Contrato  ", "PRINCIPAL", "\tDias_Maior_Atraso\n"])
        out = p0.normalizar_colunas(df)
        assert list(out.columns) == ["id_contrato", "principal", "dias_maior_atraso"]

    def test_preserves_column_order(self):
        df = pd.DataFrame(columns=["C", "A", "B"])
        out = p0.normalizar_colunas(df)
        assert list(out.columns) == ["c", "a", "b"]

    def test_no_op_on_already_clean_columns(self):
        df = pd.DataFrame(columns=["a", "b", "c"])
        out = p0.normalizar_colunas(df)
        assert list(out.columns) == ["a", "b", "c"]

    def test_returns_a_dataframe(self):
        # Caller chains methods like df.pipe(normalizar).pipe(...).
        df = pd.DataFrame(columns=["X"])
        out = p0.normalizar_colunas(df)
        assert isinstance(out, pd.DataFrame)


# --- cross-script consistency -----------------------------------------

class TestDateParsersAgree:
    """The two scripts must agree on what a parseable date looks like
    — otherwise validacao's passo0 won't catch a date that pipeline_csv
    can't read.

    Pinning this: pipeline_csv returns None for "absent OR unparseable";
    passo0 returns None for absent and False for unparseable. So the
    only inputs they agree on categorically are the obviously-parseable
    ones."""

    def test_both_parse_valid_dates(self):
        from datetime import datetime
        import pipeline_csv
        samples = ["15/03/2026", "2026-03-15", "2026-03-15 12:30:45"]
        for s in samples:
            assert isinstance(pipeline_csv.parse_date(s), datetime)
            assert isinstance(p0.parse_date_safe(s), datetime)

    def test_both_return_none_for_blanks(self):
        # Identical behavior on truly absent values: None for both.
        import pipeline_csv
        for s in ["", "   ", "nan", "None", "NaT"]:
            assert pipeline_csv.parse_date(s) is None
            assert p0.parse_date_safe(s) is None
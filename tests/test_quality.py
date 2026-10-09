import pandas as pd

from sd311.ingest import EXPECTED_COLUMNS
from sd311.quality import validate_frame, validate_raw_table


def _frame(n: int = 150) -> pd.DataFrame:
    df = pd.DataFrame({c: [None] * n for c in EXPECTED_COLUMNS}, dtype="object")
    df["service_request_id"] = [str(i) for i in range(n)]
    df["date_requested"] = "2026-01-01 08:00:00"
    df["status"] = "Closed"
    df["service_name"] = "Pothole"
    df["lat"] = "32.7"
    df["lng"] = "-117.1"
    return df


def test_sample_passes_gate(loaded_db):
    report = validate_raw_table(loaded_db)
    assert report.passed, report.failures
    assert report.rows == 4000


def test_clean_frame_passes():
    assert validate_frame(_frame()).passed


def test_duplicate_ids_fail():
    df = _frame()
    df.loc[1, "service_request_id"] = "0"
    report = validate_frame(df)
    assert not report.passed
    assert any("service_request_id" in f for f in report.failures)


def test_unknown_status_fails():
    df = _frame()
    df.loc[0, "status"] = "Lost"
    assert not validate_frame(df).passed


def test_unparseable_dates_fail():
    df = _frame()
    df["date_requested"] = "not a date"
    assert not validate_frame(df).passed


def test_missing_column_fails():
    report = validate_frame(_frame().drop(columns=["zipcode"]))
    assert any("missing columns" in f for f in report.failures)


def test_too_few_rows_fails():
    assert not validate_frame(_frame(10)).passed


def test_unexpected_column_fails():
    df = _frame()
    df["public_description"] = "noise"
    report = validate_frame(df)
    assert not report.passed
    assert any("unexpected columns" in f and "public_description" in f for f in report.failures)


def test_lineage_columns_are_allowed():
    df = _frame()
    df["_source_file"] = "sample.csv"
    df["_loaded_at"] = "2026-10-09"
    assert validate_frame(df).passed

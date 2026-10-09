"""Schema-contract unit tests for raw CSV header drift."""

from __future__ import annotations

from pathlib import Path

import pytest

from sd311.contracts import (
    EXPECTED_COLUMNS,
    SchemaDriftError,
    assert_csv_schema,
    check_headers,
)
from sd311.ingest import load_to_duckdb


def _write_csv(path: Path, columns: list[str], rows: list[list[str]] | None = None) -> Path:
    rows = rows or [["x"] * len(columns)]
    path.write_text(",".join(columns) + "\n" + "\n".join(",".join(r) for r in rows) + "\n")
    return path


def test_matching_headers_pass():
    check_headers(EXPECTED_COLUMNS, source="ok.csv")


def test_matching_headers_ignore_order():
    shuffled = list(reversed(EXPECTED_COLUMNS))
    check_headers(shuffled, source="ok.csv")


def test_removed_column_raises_with_clear_message():
    cols = [c for c in EXPECTED_COLUMNS if c != "zipcode"]
    with pytest.raises(
        SchemaDriftError,
        match=r"schema drift in gone\.csv.*missing columns:.*zipcode",
    ):
        check_headers(cols, source="gone.csv")


def test_added_column_raises_with_clear_message():
    cols = [*EXPECTED_COLUMNS, "public_description"]
    with pytest.raises(
        SchemaDriftError,
        match=r"schema drift in extra\.csv.*unexpected columns:.*public_description",
    ):
        check_headers(cols, source="extra.csv")


def test_assert_csv_schema_reads_header(tmp_path: Path):
    path = _write_csv(tmp_path / "good.csv", list(EXPECTED_COLUMNS))
    assert_csv_schema(path)


def test_assert_csv_schema_detects_drift(tmp_path: Path):
    path = _write_csv(tmp_path / "bad.csv", [*EXPECTED_COLUMNS, "extra_col"])
    with pytest.raises(SchemaDriftError, match="extra_col"):
        assert_csv_schema(path)


def test_load_to_duckdb_refuses_removed_column(db_path: Path, tmp_path: Path):
    cols = [c for c in EXPECTED_COLUMNS if c != "status"]
    path = _write_csv(tmp_path / "missing_status.csv", cols)
    with pytest.raises(SchemaDriftError, match=r"missing_status\.csv.*missing columns:.*status"):
        load_to_duckdb([path], db_path)


def test_load_to_duckdb_refuses_added_column(db_path: Path, tmp_path: Path):
    cols = [*EXPECTED_COLUMNS, "bonus_field"]
    path = _write_csv(tmp_path / "bonus.csv", cols)
    with pytest.raises(SchemaDriftError, match=r"bonus\.csv.*unexpected columns:.*bonus_field"):
        load_to_duckdb([path], db_path)

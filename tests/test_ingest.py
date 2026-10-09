import duckdb
import pytest

from sd311.ingest import EXPECTED_COLUMNS, load_to_duckdb, resolve_source_files


def test_sample_files_exist():
    files = resolve_source_files("sample")
    assert len(files) == 2
    assert all(f.exists() for f in files)


def test_unknown_mode_rejected():
    with pytest.raises(ValueError):
        resolve_source_files("bogus")


def test_load_creates_raw_table_with_lineage(db_path):
    result = load_to_duckdb(resolve_source_files("sample"), db_path)
    assert result.rows == 4000
    with duckdb.connect(str(db_path)) as con:
        cols = [r[0] for r in con.execute("describe raw.get_it_done_requests").fetchall()]
        files = con.execute(
            "select distinct _source_file from raw.get_it_done_requests order by 1"
        ).fetchall()
    assert cols[: len(EXPECTED_COLUMNS)] == EXPECTED_COLUMNS
    assert {"_source_file", "_loaded_at"} <= set(cols)
    assert [f[0] for f in files] == [
        "get_it_done_closed_sample.csv",
        "get_it_done_open_sample.csv",
    ]


def test_load_is_idempotent(db_path):
    files = resolve_source_files("sample")
    first = load_to_duckdb(files, db_path).rows
    second = load_to_duckdb(files, db_path).rows
    assert first == second


def test_missing_file_raises(db_path, tmp_path):
    with pytest.raises(FileNotFoundError):
        load_to_duckdb([tmp_path / "nope.csv"], db_path)

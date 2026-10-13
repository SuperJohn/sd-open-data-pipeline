import datetime as dt
from pathlib import Path

import duckdb
import pytest

from sd311 import config
from sd311.ingest import (
    EXPECTED_COLUMNS,
    closed_years,
    load_to_duckdb,
    resolve_source_files,
    source_year_from_path,
)


def test_sample_files_exist():
    files = resolve_source_files("sample", today=dt.date(2026, 10, 13))
    assert len(files) == 4  # open + closed 2024/2025/2026
    assert all(f.exists() for f in files)


def test_unknown_mode_rejected():
    with pytest.raises(ValueError):
        resolve_source_files("bogus")


def test_closed_years_default_range():
    assert closed_years(today=dt.date(2026, 6, 1)) == list(range(2016, 2027))
    assert closed_years(today=dt.date(2026, 6, 1), start_year=2024) == [2024, 2025, 2026]
    assert closed_years(years=[2020, 2018, 2020]) == [2018, 2020]


def test_source_year_from_path():
    assert source_year_from_path("get_it_done_requests_closed_2024_datasd.csv") == 2024
    assert source_year_from_path("get_it_done_closed_2025_sample.csv") == 2025
    assert source_year_from_path("get_it_done_requests_open_datasd.csv") is None
    with pytest.raises(ValueError):
        source_year_from_path("random.csv")


def test_resolve_live_expands_years(monkeypatch, tmp_path):
    downloaded: list[str] = []

    def fake_download(url: str, dest: Path, timeout: int = 120) -> Path:
        downloaded.append(url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text("service_request_id\n")  # placeholder; resolve only
        return dest

    monkeypatch.setattr("sd311.ingest._download", fake_download)
    monkeypatch.setattr(config, "RAW_DIR", tmp_path / "raw")

    files = resolve_source_files(
        "live",
        today=dt.date(2026, 10, 13),
        start_year=2024,
    )
    names = [f.name for f in files]
    assert names[0] == "get_it_done_requests_open_datasd.csv"
    assert names[1:] == [
        "get_it_done_requests_closed_2024_datasd.csv",
        "get_it_done_requests_closed_2025_datasd.csv",
        "get_it_done_requests_closed_2026_datasd.csv",
    ]
    assert len(downloaded) == 4
    assert all("seshat.datasd.org" in u for u in downloaded)


def test_resolve_sample_years_filter():
    files = resolve_source_files(
        "sample",
        years=[2024, 2025],
        include_open=False,
    )
    assert [f.name for f in files] == [
        "get_it_done_closed_2024_sample.csv",
        "get_it_done_closed_2025_sample.csv",
    ]


def test_load_creates_raw_table_with_lineage(db_path):
    result = load_to_duckdb(
        resolve_source_files("sample", today=dt.date(2026, 10, 13)),
        db_path,
        full_refresh=True,
    )
    assert result.rows == 4000
    with duckdb.connect(str(db_path)) as con:
        cols = [r[0] for r in con.execute("describe raw.get_it_done_requests").fetchall()]
        files = con.execute(
            "select distinct _source_file from raw.get_it_done_requests order by 1"
        ).fetchall()
        year_counts = con.execute(
            """
            select _source_year, count(*)
            from raw.get_it_done_requests
            group by 1
            order by 1 nulls first
            """
        ).fetchall()
    assert cols[: len(EXPECTED_COLUMNS)] == EXPECTED_COLUMNS
    assert {"_source_file", "_source_year", "_loaded_at"} <= set(cols)
    assert [f[0] for f in files] == [
        "get_it_done_closed_2024_sample.csv",
        "get_it_done_closed_2025_sample.csv",
        "get_it_done_closed_2026_sample.csv",
        "get_it_done_open_sample.csv",
    ]
    assert year_counts == [(None, 1500), (2024, 800), (2025, 800), (2026, 900)]


def test_load_is_idempotent(db_path):
    files = resolve_source_files("sample", today=dt.date(2026, 10, 13))
    first = load_to_duckdb(files, db_path).rows
    second = load_to_duckdb(files, db_path).rows
    assert first == second == 4000


def test_partial_year_reload_does_not_duplicate_or_wipe_others(db_path):
    all_files = resolve_source_files("sample", today=dt.date(2026, 10, 13))
    load_to_duckdb(all_files, db_path, full_refresh=True)

    only_2024 = resolve_source_files("sample", years=[2024], include_open=False)
    load_to_duckdb(only_2024, db_path)

    with duckdb.connect(str(db_path)) as con:
        total = con.execute("select count(*) from raw.get_it_done_requests").fetchone()[0]
        year_counts = dict(
            con.execute(
                """
                select coalesce(cast(_source_year as varchar), 'open'), count(*)
                from raw.get_it_done_requests
                group by 1
                """
            ).fetchall()
        )
        y2024_loads = con.execute(
            """
            select count(distinct _loaded_at)
            from raw.get_it_done_requests
            where _source_year = 2024
            """
        ).fetchone()[0]

    assert total == 4000
    assert year_counts == {"open": 1500, "2024": 800, "2025": 800, "2026": 900}
    assert y2024_loads == 1


def test_missing_file_raises(db_path, tmp_path):
    with pytest.raises(FileNotFoundError):
        load_to_duckdb([tmp_path / "nope.csv"], db_path)

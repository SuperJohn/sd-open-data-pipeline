import datetime as dt
from pathlib import Path

import duckdb

from sd311.freshness import (
    FreshnessThresholds,
    check_source_freshness,
    evaluate_freshness,
    read_freshness_clocks,
)

NOW = dt.datetime(2026, 10, 11, 17, 0, 0, tzinfo=dt.UTC)
THRESHOLDS = FreshnessThresholds(
    load_warn=dt.timedelta(hours=24),
    load_error=dt.timedelta(hours=48),
    feed_warn=dt.timedelta(hours=48),
    feed_error=dt.timedelta(hours=168),
)


def test_fresh_clocks_pass():
    report = evaluate_freshness(
        loaded_at=NOW - dt.timedelta(hours=1),
        latest_request_at=NOW - dt.timedelta(hours=6),
        row_count=1000,
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert report.passed
    assert report.severity == "ok"
    assert report.is_stale is False


def test_aging_load_warns():
    report = evaluate_freshness(
        loaded_at=NOW - dt.timedelta(hours=30),
        latest_request_at=NOW - dt.timedelta(hours=6),
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert report.passed
    assert report.severity == "warn"
    assert any("load is aging" in m for m in report.messages)


def test_stale_load_errors():
    report = evaluate_freshness(
        loaded_at=NOW - dt.timedelta(hours=60),
        latest_request_at=NOW - dt.timedelta(hours=6),
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert not report.passed
    assert report.severity == "error"
    assert report.is_stale
    assert any("load is stale" in m for m in report.messages)


def test_stale_city_feed_errors():
    report = evaluate_freshness(
        loaded_at=NOW - dt.timedelta(hours=1),
        latest_request_at=NOW - dt.timedelta(days=10),
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert not report.passed
    assert report.severity == "error"
    assert any("city feed is stale" in m for m in report.messages)


def test_aging_city_feed_warns():
    report = evaluate_freshness(
        loaded_at=NOW - dt.timedelta(hours=1),
        latest_request_at=NOW - dt.timedelta(hours=60),
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert report.passed
    assert report.severity == "warn"
    assert any("city feed is aging" in m for m in report.messages)


def test_missing_clocks_error():
    report = evaluate_freshness(
        loaded_at=None,
        latest_request_at=None,
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert not report.passed
    assert report.severity == "error"
    assert len(report.messages) >= 2


def test_naive_timestamps_are_treated_as_utc():
    report = evaluate_freshness(
        loaded_at=dt.datetime(2026, 10, 11, 16, 0, 0),  # naive
        latest_request_at=dt.datetime(2026, 10, 11, 12, 0, 0),
        now=NOW,
        thresholds=THRESHOLDS,
    )
    assert report.passed
    assert report.loaded_at is not None
    assert report.loaded_at.tzinfo is not None


def test_read_clocks_from_sample(loaded_db: Path):
    loaded_at, latest_request_at, rows = read_freshness_clocks(loaded_db)
    assert rows == 4000
    assert loaded_at is not None
    assert latest_request_at is not None


def test_check_source_freshness_on_sample(loaded_db: Path, monkeypatch):
    monkeypatch.setenv("SD311_DUCKDB_PATH", str(loaded_db))
    # Re-import path is via explicit db_path; also pin "now" near the sample's newest request.
    report = check_source_freshness(
        loaded_db,
        now=dt.datetime(2026, 10, 9, 12, 0, 0, tzinfo=dt.UTC),
        thresholds=THRESHOLDS,
    )
    assert report.passed, report.messages
    assert report.row_count == 4000


def test_missing_warehouse(tmp_path: Path):
    report = check_source_freshness(tmp_path / "missing.duckdb", now=NOW, thresholds=THRESHOLDS)
    assert not report.passed
    assert report.severity == "error"


def test_empty_table_errors(tmp_path: Path):
    db = tmp_path / "empty.duckdb"
    with duckdb.connect(str(db)) as con:
        con.execute("create schema raw")
        con.execute(
            "create table raw.get_it_done_requests as "
            "select cast(null as timestamp) as _loaded_at, "
            "cast(null as varchar) as date_requested "
            "where false"
        )
    report = check_source_freshness(db, now=NOW, thresholds=THRESHOLDS)
    assert not report.passed
    assert report.row_count == 0

"""Source freshness for the Get It Done landing table.

Two clocks matter:

* **Load freshness** — how long since we last wrote ``_loaded_at`` (our ingest).
* **Feed freshness** — how old the newest ``date_requested`` in the extract is
  (the city's published feed).

dbt ``source freshness`` covers the load clock via ``loaded_at_field``. This
module covers both clocks so a Dagster sensor can alert when either drifts.
"""

from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from sd311 import config

# Defaults sized for a daily city publish + daily Dagster refresh.
_DEFAULT_LOAD_WARN = dt.timedelta(hours=int(os.getenv("SD311_LOAD_WARN_HOURS", "24")))
_DEFAULT_LOAD_ERROR = dt.timedelta(hours=int(os.getenv("SD311_LOAD_ERROR_HOURS", "48")))
_DEFAULT_FEED_WARN = dt.timedelta(hours=int(os.getenv("SD311_FEED_WARN_HOURS", "48")))
_DEFAULT_FEED_ERROR = dt.timedelta(hours=int(os.getenv("SD311_FEED_ERROR_HOURS", "168")))


@dataclass(frozen=True)
class FreshnessThresholds:
    load_warn: dt.timedelta = field(default_factory=lambda: _DEFAULT_LOAD_WARN)
    load_error: dt.timedelta = field(default_factory=lambda: _DEFAULT_LOAD_ERROR)
    feed_warn: dt.timedelta = field(default_factory=lambda: _DEFAULT_FEED_WARN)
    feed_error: dt.timedelta = field(default_factory=lambda: _DEFAULT_FEED_ERROR)


@dataclass
class FreshnessReport:
    """Outcome of a freshness evaluation.

    ``severity`` is ``ok``, ``warn``, or ``error``. ``passed`` is False only
    on ``error`` (warnings surface without failing CI / blocking sensors).
    """

    passed: bool
    severity: str
    loaded_at: dt.datetime | None
    latest_request_at: dt.datetime | None
    load_age: dt.timedelta | None
    feed_age: dt.timedelta | None
    messages: list[str] = field(default_factory=list)
    row_count: int = 0

    @property
    def is_stale(self) -> bool:
        return self.severity == "error"


def _as_utc(value: dt.datetime | None) -> dt.datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def evaluate_freshness(
    *,
    loaded_at: dt.datetime | None,
    latest_request_at: dt.datetime | None,
    row_count: int = 0,
    now: dt.datetime | None = None,
    thresholds: FreshnessThresholds | None = None,
) -> FreshnessReport:
    """Pure evaluation of load + feed ages against thresholds."""
    thresholds = thresholds or FreshnessThresholds()
    now_utc = _as_utc(now or dt.datetime.now(dt.UTC))
    assert now_utc is not None
    loaded_at = _as_utc(loaded_at)
    latest_request_at = _as_utc(latest_request_at)

    messages: list[str] = []
    severity = "ok"
    load_age = (now_utc - loaded_at) if loaded_at is not None else None
    feed_age = (now_utc - latest_request_at) if latest_request_at is not None else None

    if loaded_at is None:
        messages.append("no _loaded_at timestamp in raw table (empty or missing warehouse)")
        severity = "error"
    elif load_age is not None and load_age > thresholds.load_error:
        messages.append(
            f"load is stale: last _loaded_at {loaded_at.isoformat()} "
            f"({load_age.total_seconds() / 3600:.1f}h ago; error after "
            f"{thresholds.load_error.total_seconds() / 3600:.0f}h)"
        )
        severity = "error"
    elif load_age is not None and load_age > thresholds.load_warn:
        messages.append(
            f"load is aging: last _loaded_at {loaded_at.isoformat()} "
            f"({load_age.total_seconds() / 3600:.1f}h ago; warn after "
            f"{thresholds.load_warn.total_seconds() / 3600:.0f}h)"
        )
        severity = "warn"

    if latest_request_at is None:
        messages.append("no parseable date_requested values in raw table")
        severity = "error"
    elif feed_age is not None and feed_age > thresholds.feed_error:
        messages.append(
            f"city feed is stale: newest date_requested {latest_request_at.isoformat()} "
            f"({feed_age.total_seconds() / 3600:.1f}h ago; error after "
            f"{thresholds.feed_error.total_seconds() / 3600:.0f}h)"
        )
        severity = "error"
    elif feed_age is not None and feed_age > thresholds.feed_warn:
        messages.append(
            f"city feed is aging: newest date_requested {latest_request_at.isoformat()} "
            f"({feed_age.total_seconds() / 3600:.1f}h ago; warn after "
            f"{thresholds.feed_warn.total_seconds() / 3600:.0f}h)"
        )
        if severity == "ok":
            severity = "warn"

    if severity == "ok":
        messages.append("source freshness ok")

    return FreshnessReport(
        passed=severity != "error",
        severity=severity,
        loaded_at=loaded_at,
        latest_request_at=latest_request_at,
        load_age=load_age,
        feed_age=feed_age,
        messages=messages,
        row_count=row_count,
    )


def read_freshness_clocks(
    db_path: Path | None = None,
) -> tuple[dt.datetime | None, dt.datetime | None, int]:
    """Return (max _loaded_at, max date_requested, row_count) from the raw table."""
    db_path = Path(db_path or config.DUCKDB_PATH)
    if not db_path.exists():
        return None, None, 0
    table = f"{config.RAW_SCHEMA}.{config.RAW_TABLE}"
    with duckdb.connect(str(db_path), read_only=True) as con:
        exists = con.execute(
            """
            select count(*) from information_schema.tables
            where table_schema = ? and table_name = ?
            """,
            [config.RAW_SCHEMA, config.RAW_TABLE],
        ).fetchone()[0]
        if not exists:
            return None, None, 0
        loaded_at, latest_request_at, rows = con.execute(
            f"""
            select
                max(_loaded_at),
                max(try_cast(date_requested as timestamp)),
                count(*)
            from {table}
            """
        ).fetchone()
    return loaded_at, latest_request_at, int(rows or 0)


def check_source_freshness(
    db_path: Path | None = None,
    *,
    now: dt.datetime | None = None,
    thresholds: FreshnessThresholds | None = None,
) -> FreshnessReport:
    """Evaluate freshness against the current DuckDB warehouse."""
    loaded_at, latest_request_at, rows = read_freshness_clocks(db_path)
    return evaluate_freshness(
        loaded_at=loaded_at,
        latest_request_at=latest_request_at,
        row_count=rows,
        now=now,
        thresholds=thresholds,
    )

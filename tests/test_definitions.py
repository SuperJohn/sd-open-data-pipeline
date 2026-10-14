import datetime as dt
import os
import subprocess
import sys

from dagster import SkipReason, build_sensor_context

from sd311 import config
from sd311.freshness import FreshnessReport


def _parse_dbt(tmp_path):
    env = {**os.environ, "SD311_DUCKDB_PATH": str(tmp_path / "x.duckdb")}
    dbt = os.path.join(os.path.dirname(sys.executable), "dbt")
    subprocess.run(
        [
            dbt,
            "parse",
            "--project-dir",
            str(config.DBT_PROJECT_DIR),
            "--profiles-dir",
            str(config.DBT_PROJECT_DIR),
            "--quiet",
        ],
        env=env,
        check=True,
    )


def test_dagster_definitions_load(tmp_path):
    # dagster-dbt needs a manifest; parse the project first (fast, offline).
    _parse_dbt(tmp_path)
    from sd311.definitions import defs

    repo = defs.get_repository_def()
    keys = {k.to_user_string() for k in repo.asset_graph.get_all_asset_keys()}
    assert "raw/get_it_done_requests" in keys
    assert "raw/council_districts" in keys
    assert "staging/stg_city_gis__council_districts" in keys
    assert "marts/fct_requests" in keys
    assert repo.get_schedule_def("sd311_daily_refresh_schedule").cron_schedule == "0 6 * * *"

    sensor_names = {s.name for s in repo.sensor_defs}
    assert "city_feed_freshness_sensor" in sensor_names

    check_names = {key.name for key in repo.asset_graph.asset_check_keys}
    assert "raw_schema_and_quality_gate" in check_names
    assert "source_freshness_gate" in check_names

    raw_key = next(
        k
        for k in repo.asset_graph.get_all_asset_keys()
        if k.to_user_string() == "raw/get_it_done_requests"
    )
    policy = repo.asset_graph.get(raw_key).freshness_policy
    assert policy is not None


def test_city_feed_sensor_skips_when_fresh(tmp_path, monkeypatch):
    _parse_dbt(tmp_path)
    monkeypatch.setenv("SD311_DUCKDB_PATH", str(tmp_path / "x.duckdb"))

    import sd311.definitions as definitions

    fresh = FreshnessReport(
        passed=True,
        severity="ok",
        loaded_at=None,
        latest_request_at=None,
        load_age=None,
        feed_age=None,
        messages=["source freshness ok"],
        row_count=0,
    )
    monkeypatch.setattr(definitions, "check_source_freshness", lambda: fresh)
    result = definitions.city_feed_freshness_sensor(build_sensor_context())
    assert isinstance(result, SkipReason)
    assert "fresh" in result.skip_message.lower()


def test_city_feed_sensor_requests_run_when_stale(tmp_path, monkeypatch):
    _parse_dbt(tmp_path)
    monkeypatch.setenv("SD311_DUCKDB_PATH", str(tmp_path / "x.duckdb"))

    import sd311.definitions as definitions

    stale = FreshnessReport(
        passed=False,
        severity="error",
        loaded_at=dt.datetime(2026, 10, 1, tzinfo=dt.UTC),
        latest_request_at=dt.datetime(2026, 9, 1, tzinfo=dt.UTC),
        load_age=dt.timedelta(days=10),
        feed_age=dt.timedelta(days=40),
        messages=["city feed is stale"],
        row_count=10,
    )
    monkeypatch.setattr(definitions, "check_source_freshness", lambda: stale)
    result = definitions.city_feed_freshness_sensor(build_sensor_context())
    assert result.tags["alert"] == "source_freshness"
    assert result.run_key.startswith("stale-feed-")

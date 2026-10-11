"""Dagster code location: ingest asset -> pandera gate -> dbt models/tests, on a daily schedule.

Also watches source freshness: dbt `source freshness` on `_loaded_at`, a non-blocking
asset check on load + city-feed ages, a FreshnessPolicy on the raw asset, and a sensor
that requests a live refresh when the warehouse goes stale.

dagster dev                          # UI at http://localhost:3000
dagster asset materialize -m sd311.definitions --select '*'
"""

import datetime as dt
import os

import dagster as dg
from dagster import DefaultSensorStatus, FreshnessPolicy, SkipReason
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from sd311 import config as settings
from sd311.freshness import check_source_freshness
from sd311.ingest import load_to_duckdb, resolve_source_files
from sd311.quality import validate_raw_table

os.environ.setdefault("SD311_DUCKDB_PATH", str(settings.DUCKDB_PATH))

dbt_project = DbtProject(
    project_dir=settings.DBT_PROJECT_DIR, profiles_dir=settings.DBT_PROJECT_DIR
)
# Parses the dbt project (writes target/manifest.json) when running under `dagster dev`.
dbt_project.prepare_if_dev()


class IngestConfig(dg.Config):
    source: str = (
        settings.SOURCE_MODE
    )  # "sample" (offline) or "live" (download from data.sandiego.gov)


@dg.asset(
    key_prefix="raw",
    name="get_it_done_requests",
    group_name="ingest",
    kinds={"python", "duckdb"},
    description="Get It Done 311 CSVs as VARCHAR; schema contract blocks column drift before load.",
    # Daily schedule: warn if no materialization within 24h, fail after 48h.
    freshness_policy=FreshnessPolicy.time_window(
        fail_window=dt.timedelta(hours=48),
        warn_window=dt.timedelta(hours=24),
    ),
)
def raw_get_it_done_requests(
    context: dg.AssetExecutionContext, config: IngestConfig
) -> dg.MaterializeResult:
    result = load_to_duckdb(resolve_source_files(config.source))
    context.log.info("loaded %s rows", result.rows)
    return dg.MaterializeResult(
        metadata={
            "rows": dg.MetadataValue.int(result.rows),
            "files": dg.MetadataValue.text(", ".join(result.files)),
            "source_mode": dg.MetadataValue.text(config.source),
        }
    )


@dg.asset_check(asset=raw_get_it_done_requests, blocking=True)
def raw_schema_and_quality_gate() -> dg.AssetCheckResult:
    """Pandera gate (+ ingest schema contract): blocks dbt if the landing table looks wrong."""
    report = validate_raw_table()
    return dg.AssetCheckResult(
        passed=report.passed,
        metadata={"rows": report.rows, "failures": "\n".join(report.failures) or "none"},
    )


@dg.asset_check(asset=raw_get_it_done_requests, blocking=False)
def source_freshness_gate(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Fail (non-blocking) when load or city-feed clocks exceed error thresholds."""
    report = check_source_freshness()
    for msg in report.messages:
        if report.severity == "error":
            context.log.error(msg)
        elif report.severity == "warn":
            context.log.warning(msg)
        else:
            context.log.info(msg)
    meta: dict[str, dg.MetadataValue] = {
        "severity": dg.MetadataValue.text(report.severity),
        "rows": dg.MetadataValue.int(report.row_count),
        "messages": dg.MetadataValue.text("\n".join(report.messages)),
    }
    if report.loaded_at is not None:
        meta["loaded_at"] = dg.MetadataValue.text(report.loaded_at.isoformat())
    if report.latest_request_at is not None:
        meta["latest_request_at"] = dg.MetadataValue.text(report.latest_request_at.isoformat())
    if report.load_age is not None:
        meta["load_age_hours"] = dg.MetadataValue.float(report.load_age.total_seconds() / 3600)
    if report.feed_age is not None:
        meta["feed_age_hours"] = dg.MetadataValue.float(report.feed_age.total_seconds() / 3600)
    return dg.AssetCheckResult(
        passed=report.passed,
        severity=(
            dg.AssetCheckSeverity.ERROR
            if report.severity == "error"
            else dg.AssetCheckSeverity.WARN
        ),
        metadata=meta,
    )


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def sd311_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models; dbt tests surface in Dagster as asset checks."""
    yield from dbt.cli(["build"], context=context).stream()


full_refresh_job = dg.define_asset_job(
    "sd311_daily_refresh",
    selection=dg.AssetSelection.all(),
    description="Download -> validate -> dbt build (models + tests).",
    config={"ops": {"raw__get_it_done_requests": {"config": {"source": "live"}}}},
)

daily_schedule = dg.ScheduleDefinition(
    job=full_refresh_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/Los_Angeles",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


@dg.sensor(
    name="city_feed_freshness_sensor",
    job=full_refresh_job,
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Polls DuckDB load + city-feed clocks. Requests a live refresh (and logs an alert) "
        "when either clock exceeds its error threshold."
    ),
)
def city_feed_freshness_sensor(context: dg.SensorEvaluationContext):
    report = check_source_freshness()
    summary = "; ".join(report.messages) or "no freshness messages"
    if report.severity == "error":
        context.log.error("source freshness alert: %s", summary)
        run_key = "stale-feed"
        if report.loaded_at is not None:
            run_key = f"stale-feed-{report.loaded_at.strftime('%Y%m%d%H%M%S')}"
        elif report.latest_request_at is not None:
            run_key = f"stale-feed-{report.latest_request_at.strftime('%Y%m%d%H%M%S')}"
        return dg.RunRequest(
            run_key=run_key,
            tags={
                "alert": "source_freshness",
                "severity": "error",
                "source": "live",
            },
            run_config={
                "ops": {"raw__get_it_done_requests": {"config": {"source": "live"}}},
            },
        )
    if report.severity == "warn":
        context.log.warning("source freshness warning: %s", summary)
        return SkipReason(f"feed aging (warn): {summary}")
    context.log.info("source freshness ok: %s", summary)
    return SkipReason(f"feed fresh: {summary}")


defs = dg.Definitions(
    assets=[raw_get_it_done_requests, sd311_dbt_models],
    asset_checks=[raw_schema_and_quality_gate, source_freshness_gate],
    jobs=[full_refresh_job],
    schedules=[daily_schedule],
    sensors=[city_feed_freshness_sensor],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)

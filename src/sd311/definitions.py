"""Dagster code location: ingest asset -> pandera gate -> dbt models/tests, on a daily schedule.

dagster dev                          # UI at http://localhost:3000
dagster asset materialize -m sd311.definitions --select '*'
"""

import os

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from sd311 import config as settings
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

defs = dg.Definitions(
    assets=[raw_get_it_done_requests, sd311_dbt_models],
    asset_checks=[raw_schema_and_quality_gate],
    jobs=[full_refresh_job],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)

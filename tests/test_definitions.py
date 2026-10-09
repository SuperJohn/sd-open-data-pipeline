import os
import subprocess
import sys

from sd311 import config


def test_dagster_definitions_load(tmp_path):
    # dagster-dbt needs a manifest; parse the project first (fast, offline).
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
    from sd311.definitions import defs

    repo = defs.get_repository_def()
    keys = {k.to_user_string() for k in repo.asset_graph.get_all_asset_keys()}
    assert "raw/get_it_done_requests" in keys
    assert "marts/fct_requests" in keys
    assert repo.get_schedule_def("sd311_daily_refresh_schedule").cron_schedule == "0 6 * * *"

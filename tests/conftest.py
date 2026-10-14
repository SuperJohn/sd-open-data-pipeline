from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from sd311 import config
from sd311.districts import load_districts, resolve_districts_file
from sd311.ingest import load_to_duckdb, resolve_source_files


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.duckdb"


@pytest.fixture()
def loaded_db(db_path: Path) -> Path:
    load_to_duckdb(resolve_source_files("sample"), db_path)
    return db_path


@pytest.fixture(scope="session")
def built_warehouse(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Load the sample and run `dbt build` once for the whole session."""
    db = tmp_path_factory.mktemp("wh") / "sd311.duckdb"
    load_to_duckdb(resolve_source_files("sample"), db)
    load_districts(resolve_districts_file("sample"), db)
    dbt = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    env = {**os.environ, "SD311_DUCKDB_PATH": str(db)}
    target = tmp_path_factory.mktemp("dbt_target")
    proc = subprocess.run(
        [
            dbt,
            "build",
            "--project-dir",
            str(config.DBT_PROJECT_DIR),
            "--profiles-dir",
            str(config.DBT_PROJECT_DIR),
            "--target-path",
            str(target),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout[-4000:] + proc.stderr[-2000:]
    return db

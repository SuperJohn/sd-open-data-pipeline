"""Static checks on the container setup, plus the entrypoint's argument handling.

The full `docker compose up` smoke test runs in CI (the `docker` job); these keep the
compose file, Dockerfile, and entrypoint consistent with the rest of the repo offline.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from sd311 import config

ROOT = config.REPO_ROOT
ENTRYPOINT = ROOT / "docker" / "entrypoint.sh"


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def dockerfile() -> str:
    return (ROOT / "Dockerfile").read_text(encoding="utf-8")


def test_compose_defines_pipeline_dagster_and_dashboard(compose: dict):
    services = compose["services"]
    assert set(services) == {"pipeline", "dagster", "dashboard"}
    assert services["pipeline"]["command"] == ["pipeline"]
    assert services["dagster"]["command"] == ["dagster"]
    assert services["dashboard"]["command"] == ["dashboard"]


def test_ui_services_wait_for_pipeline_and_publish_ports(compose: dict):
    services = compose["services"]
    for name, port in [("dagster", 3000), ("dashboard", 8501)]:
        svc = services[name]
        dep = svc["depends_on"]["pipeline"]
        assert dep["condition"] == "service_completed_successfully"
        assert any(p.endswith(f":{port}") for p in svc["ports"]), svc["ports"]
        assert "healthcheck" in svc


def test_services_share_one_warehouse(compose: dict):
    services = compose["services"]
    paths = {s["environment"]["SD311_DUCKDB_PATH"] for s in services.values()}
    assert len(paths) == 1
    (db_path,) = paths
    for svc in services.values():
        mounts = {v.split(":", 1)[1]: v.split(":", 1)[0] for v in svc["volumes"]}
        assert mounts.get(str(Path(db_path).parent)) == "warehouse"
    assert "warehouse" in compose["volumes"]


def test_compose_defaults_to_offline_sample(compose: dict):
    for svc in compose["services"].values():
        assert svc["environment"]["SD311_SOURCE"] == "${SD311_SOURCE:-sample}"


def test_dockerfile_matches_repo_python_and_lockfile(dockerfile: str):
    pinned = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    assert f"ARG PYTHON_VERSION={pinned}" in dockerfile
    assert "uv sync --locked --no-dev" in dockerfile
    assert "USER app" in dockerfile
    # Dagster reads the dbt manifest at import, so the image must ship a parsed project.
    assert "dbt parse" in dockerfile
    assert 'ENTRYPOINT ["sd311-entrypoint"]' in dockerfile


def test_dockerignore_keeps_state_and_secrets_out():
    ignored = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }
    for pattern in [".git", ".venv", ".env", "data/warehouse", "data/raw", "dbt/target"]:
        assert pattern in ignored, pattern
    # The sample data and dbt docs blocks are needed inside the image.
    assert "data" not in ignored
    assert "dbt" not in ignored


def _run_entrypoint(*args: str, **env: str) -> subprocess.CompletedProcess[str]:
    sh = shutil.which("sh")
    if sh is None:
        pytest.skip("no POSIX sh")
    return subprocess.run(
        [sh, str(ENTRYPOINT), *args],
        cwd=ROOT,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_entrypoint_rejects_unknown_source():
    result = _run_entrypoint("pipeline", SD311_SOURCE="s3://somewhere")
    assert result.returncode == 2
    assert "must be 'sample' or 'live'" in result.stderr


def test_entrypoint_passes_other_commands_through():
    result = _run_entrypoint("echo", "hello from the container")
    assert result.returncode == 0
    assert result.stdout.strip() == "hello from the container"


def test_entrypoint_is_executable():
    assert os.access(ENTRYPOINT, os.X_OK)

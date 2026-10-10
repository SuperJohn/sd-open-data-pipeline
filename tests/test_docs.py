"""Unit + light integration tests for dbt docs packaging."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from sd311 import config
from sd311.docs import REQUIRED_DOC_FILES, package_docs_site


def test_package_docs_site_copies_required_files(tmp_path: Path):
    target = tmp_path / "target"
    target.mkdir()
    for name in REQUIRED_DOC_FILES:
        (target / name).write_text("x", encoding="utf-8")
    (target / "graph.html").write_text("g", encoding="utf-8")
    (target / "run_results.json").write_text("{}", encoding="utf-8")

    out = tmp_path / "site"
    copied = package_docs_site(target, out)

    names = sorted(p.name for p in copied)
    assert names == ["catalog.json", "graph.html", "index.html", "manifest.json"]
    assert not (out / "run_results.json").exists()


def test_package_docs_site_requires_artifacts(tmp_path: Path):
    target = tmp_path / "empty"
    target.mkdir()
    with pytest.raises(FileNotFoundError, match="index.html"):
        package_docs_site(target, tmp_path / "site")


def test_dbt_docs_generate_produces_site(built_warehouse: Path, tmp_path: Path):
    """After a build, docs generate + package yields a usable static site."""
    dbt = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    env = {**os.environ, "SD311_DUCKDB_PATH": str(built_warehouse)}
    target = tmp_path / "dbt_target"
    docs = subprocess.run(
        [
            dbt,
            "docs",
            "generate",
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
    assert docs.returncode == 0, docs.stdout[-4000:] + docs.stderr[-2000:]

    site = tmp_path / "site"
    copied = package_docs_site(target, site)
    assert {p.name for p in copied} >= set(REQUIRED_DOC_FILES)

    catalog = json.loads((site / "catalog.json").read_text(encoding="utf-8"))
    assert "nodes" in catalog
    manifest = json.loads((site / "manifest.json").read_text(encoding="utf-8"))
    assert "nodes" in manifest
    assert "model.sd311.fct_requests" in manifest["nodes"]
    html = (site / "index.html").read_text(encoding="utf-8")
    assert "dbt" in html.lower() or "catalog.json" in html

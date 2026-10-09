"""Central configuration. Everything is overridable with environment variables."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
SAMPLE_DIR = DATA_DIR / "sample"
RAW_DIR = DATA_DIR / "raw"
DBT_PROJECT_DIR = REPO_ROOT / "dbt"

# "sample" uses the CSVs checked into data/sample (offline, used by tests + CI).
# "live" downloads the current files from the City of San Diego open data portal.
SOURCE_MODE = os.getenv("SD311_SOURCE", "sample")

DUCKDB_PATH = Path(
    os.getenv("SD311_DUCKDB_PATH", str(DATA_DIR / "warehouse" / "sd311.duckdb"))
).resolve()

# Get It Done (311) request extracts published at https://data.sandiego.gov/datasets/get-it-done-311/
SEED_BASE_URL = "https://seshat.datasd.org/get_it_done_reports"
LIVE_FILES = {
    "open": "get_it_done_requests_open_datasd.csv",
    "closed_current_year": "get_it_done_requests_closed_{year}_datasd.csv",
}
SAMPLE_FILES = {
    "open": "get_it_done_open_sample.csv",
    "closed_current_year": "get_it_done_closed_sample.csv",
}

RAW_SCHEMA = "raw"
RAW_TABLE = "get_it_done_requests"

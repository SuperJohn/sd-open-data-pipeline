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

# Closed extracts start in 2016 on the city portal. Override with SD311_CLOSED_START_YEAR.
CLOSED_START_YEAR = int(os.getenv("SD311_CLOSED_START_YEAR", "2016"))

LIVE_OPEN_FILE = "get_it_done_requests_open_datasd.csv"
LIVE_CLOSED_TEMPLATE = "get_it_done_requests_closed_{year}_datasd.csv"

SAMPLE_OPEN_FILE = "get_it_done_open_sample.csv"
# Year-tagged closed samples (small multi-year fixtures for offline backfill tests).
SAMPLE_CLOSED_TEMPLATE = "get_it_done_closed_{year}_sample.csv"

# Legacy aliases kept for callers/tests that still reference the old dict shape.
LIVE_FILES = {
    "open": LIVE_OPEN_FILE,
    "closed_current_year": LIVE_CLOSED_TEMPLATE,
}
SAMPLE_FILES = {
    "open": SAMPLE_OPEN_FILE,
    "closed_current_year": SAMPLE_CLOSED_TEMPLATE.format(year=2026),
}

RAW_SCHEMA = "raw"
RAW_TABLE = "get_it_done_requests"

# Open-request extract has no close-year; stamped as NULL in `_source_year`.
OPEN_SOURCE_YEAR: int | None = None

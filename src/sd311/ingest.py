"""Extract + load: land Get It Done CSVs in DuckDB as an append-only raw table.

Raw data is loaded as VARCHAR on purpose: typing, cleaning and deduplication
happen in dbt (staging). Schema *presence* is enforced by
``sd311.contracts`` before load so added/removed columns fail fast.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import requests

from sd311 import config
from sd311.contracts import EXPECTED_COLUMNS, assert_csv_schema

log = logging.getLogger(__name__)

# Re-export for callers that historically imported from ingest.
__all__ = ["EXPECTED_COLUMNS", "LoadResult", "load_to_duckdb", "resolve_source_files", "main"]


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows: int
    files: list[str]


def _download(url: str, dest: Path, timeout: int = 120) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log.info("downloading %s", url)
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            shutil.copyfileobj(resp.raw, fh, length=1 << 20)
    tmp.replace(dest)
    return dest


def resolve_source_files(mode: str | None = None, today: dt.date | None = None) -> list[Path]:
    """Return local CSV paths for the requested mode, downloading when mode == 'live'."""
    mode = mode or config.SOURCE_MODE
    if mode == "sample":
        return [config.SAMPLE_DIR / name for name in config.SAMPLE_FILES.values()]
    if mode == "live":
        year = (today or dt.date.today()).year
        paths = []
        for template in config.LIVE_FILES.values():
            name = template.format(year=year)
            paths.append(_download(f"{config.SEED_BASE_URL}/{name}", config.RAW_DIR / name))
        return paths
    raise ValueError(f"unknown source mode {mode!r}; expected 'sample' or 'live'")


def load_to_duckdb(files: list[Path], db_path: Path | None = None) -> LoadResult:
    """(Re)build raw.get_it_done_requests from the given CSVs.

    Full refresh is fine at this size (~400k rows/yr) and keeps the pipeline idempotent.
    Each row is stamped with its source file and load time for lineage.
    """
    db_path = Path(db_path or config.DUCKDB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table = f"{config.RAW_SCHEMA}.{config.RAW_TABLE}"
    file_list = [str(f) for f in files]
    for f in files:
        path = Path(f)
        if not path.exists():
            raise FileNotFoundError(f)
        assert_csv_schema(path)

    select_cols = ", ".join(EXPECTED_COLUMNS)
    with duckdb.connect(str(db_path)) as con:
        con.execute(f"create schema if not exists {config.RAW_SCHEMA}")
        con.execute(
            f"""
            create or replace table {table} as
            select
                {select_cols},
                regexp_extract(filename, '[^/]+$') as _source_file,
                current_timestamp as _loaded_at
            from read_csv(
                ?, header = true, all_varchar = true, union_by_name = true, filename = true
            )
            """,
            [file_list],
        )
        rows = con.execute(f"select count(*) from {table}").fetchone()[0]
    log.info("loaded %s rows into %s", rows, table)
    return LoadResult(table=table, rows=rows, files=[Path(f).name for f in files])


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load Get It Done 311 data into DuckDB")
    parser.add_argument("--source", choices=["sample", "live"], default=config.SOURCE_MODE)
    parser.add_argument("--db", type=Path, default=config.DUCKDB_PATH)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = load_to_duckdb(resolve_source_files(args.source), args.db)
    print(f"{result.table}: {result.rows:,} rows from {', '.join(result.files)}")


if __name__ == "__main__":
    main()

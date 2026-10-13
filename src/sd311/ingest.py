"""Extract + load: land Get It Done CSVs in DuckDB as a year-partitioned raw table.

Raw data is loaded as VARCHAR on purpose: typing, cleaning and deduplication
happen in dbt (staging). Schema *presence* is enforced by
``sd311.contracts`` before load so added/removed columns fail fast.

Each row is stamped with ``_source_file``, ``_source_year`` (close-year
partition from the filename; NULL for the open extract), and ``_loaded_at``.
Loads are idempotent per year: reloading a year deletes that partition and
re-inserts, leaving other years untouched.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import requests

from sd311 import config
from sd311.contracts import EXPECTED_COLUMNS, assert_csv_schema

log = logging.getLogger(__name__)

_CLOSED_YEAR_RE = re.compile(r"closed[_-](\d{4})", re.IGNORECASE)

__all__ = [
    "EXPECTED_COLUMNS",
    "LoadResult",
    "closed_years",
    "load_to_duckdb",
    "resolve_source_files",
    "source_year_from_path",
    "main",
]


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows: int
    files: list[str]
    years: list[int | None]


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


def source_year_from_path(path: Path | str) -> int | None:
    """Return the close-year partition for *path*, or None for the open extract."""
    name = Path(path).name
    match = _CLOSED_YEAR_RE.search(name)
    if match:
        return int(match.group(1))
    if "open" in name.lower():
        return config.OPEN_SOURCE_YEAR
    raise ValueError(
        f"cannot derive _source_year from {name!r}; expected open or closed_YYYY filename"
    )


def closed_years(
    *,
    today: dt.date | None = None,
    start_year: int | None = None,
    years: list[int] | None = None,
) -> list[int]:
    """Closed-extract years to load (inclusive), newest calendar year last."""
    if years is not None:
        if not years:
            return []
        return sorted({int(y) for y in years})
    end = (today or dt.date.today()).year
    start = config.CLOSED_START_YEAR if start_year is None else int(start_year)
    if start > end:
        raise ValueError(f"start_year {start} is after current year {end}")
    return list(range(start, end + 1))


def _sample_closed_path(year: int) -> Path:
    return config.SAMPLE_DIR / config.SAMPLE_CLOSED_TEMPLATE.format(year=year)


def resolve_source_files(
    mode: str | None = None,
    today: dt.date | None = None,
    *,
    start_year: int | None = None,
    years: list[int] | None = None,
    include_open: bool = True,
) -> list[Path]:
    """Return local CSV paths for the requested mode, downloading when mode == 'live'.

    Live mode resolves the open extract (unless ``include_open`` is False) plus
    every closed_YYYY file from ``start_year`` (default
    ``config.CLOSED_START_YEAR``) through the current calendar year, or the
    explicit ``years`` list when provided.

    Sample mode stays offline: it only includes year-tagged fixtures that exist
    under ``data/sample/``.
    """
    mode = mode or config.SOURCE_MODE
    today = today or dt.date.today()
    wanted = closed_years(today=today, start_year=start_year, years=years)
    paths: list[Path] = []

    if mode == "sample":
        if include_open:
            paths.append(config.SAMPLE_DIR / config.SAMPLE_OPEN_FILE)
        for year in wanted:
            candidate = _sample_closed_path(year)
            if candidate.exists():
                paths.append(candidate)
        if not paths:
            raise FileNotFoundError(
                f"no sample fixtures for years {wanted} under {config.SAMPLE_DIR}"
            )
        return paths

    if mode == "live":
        if include_open:
            name = config.LIVE_OPEN_FILE
            paths.append(_download(f"{config.SEED_BASE_URL}/{name}", config.RAW_DIR / name))
        for year in wanted:
            name = config.LIVE_CLOSED_TEMPLATE.format(year=year)
            paths.append(_download(f"{config.SEED_BASE_URL}/{name}", config.RAW_DIR / name))
        return paths

    raise ValueError(f"unknown source mode {mode!r}; expected 'sample' or 'live'")


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    schema, _, name = table.partition(".")
    row = con.execute(
        """
        select 1 from information_schema.tables
        where table_schema = ? and table_name = ?
        """,
        [schema, name],
    ).fetchone()
    return row is not None


def _ensure_raw_table(con: duckdb.DuckDBPyConnection, table: str) -> None:
    cols = ", ".join(f"{c} varchar" for c in EXPECTED_COLUMNS)
    con.execute(
        f"""
        create table if not exists {table} (
            {cols},
            _source_file varchar,
            _source_year integer,
            _loaded_at timestamp
        )
        """
    )


def _delete_partitions(con: duckdb.DuckDBPyConnection, table: str, years: set[int | None]) -> None:
    numbered = sorted(y for y in years if y is not None)
    if numbered:
        placeholders = ", ".join("?" for _ in numbered)
        con.execute(
            f"delete from {table} where _source_year in ({placeholders})",
            numbered,
        )
    if None in years:
        con.execute(f"delete from {table} where _source_year is null")


def load_to_duckdb(
    files: list[Path],
    db_path: Path | None = None,
    *,
    full_refresh: bool = False,
) -> LoadResult:
    """Load CSVs into ``raw.get_it_done_requests`` with year-partition idempotency.

    By default, rows for each file's ``_source_year`` are deleted and re-inserted
    so a partial backfill cannot duplicate or wipe other years. Pass
    ``full_refresh=True`` to drop and rebuild the table from *files* only.
    """
    db_path = Path(db_path or config.DUCKDB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table = f"{config.RAW_SCHEMA}.{config.RAW_TABLE}"
    if not files:
        raise ValueError("load_to_duckdb requires at least one file")

    file_years: list[tuple[Path, int | None]] = []
    for f in files:
        path = Path(f)
        if not path.exists():
            raise FileNotFoundError(f)
        assert_csv_schema(path)
        file_years.append((path, source_year_from_path(path)))

    select_cols = ", ".join(EXPECTED_COLUMNS)
    years = [y for _, y in file_years]

    with duckdb.connect(str(db_path)) as con:
        con.execute(f"create schema if not exists {config.RAW_SCHEMA}")
        if full_refresh or not _table_exists(con, table):
            con.execute(f"drop table if exists {table}")
            _ensure_raw_table(con, table)
        else:
            # Migrate legacy tables that predate `_source_year`.
            cols = {r[0] for r in con.execute(f"describe {table}").fetchall()}
            if "_source_year" not in cols:
                log.info("raw table missing _source_year; rebuilding")
                con.execute(f"drop table if exists {table}")
                _ensure_raw_table(con, table)
            else:
                _delete_partitions(con, table, set(years))

        for path, year in file_years:
            con.execute(
                f"""
                insert into {table}
                select
                    {select_cols},
                    regexp_extract(filename, '[^/]+$') as _source_file,
                    ?::integer as _source_year,
                    current_timestamp as _loaded_at
                from read_csv(
                    ?, header = true, all_varchar = true, filename = true
                )
                """,
                [year, str(path)],
            )

        rows = con.execute(f"select count(*) from {table}").fetchone()[0]

    log.info("loaded %s rows into %s (years=%s)", rows, table, years)
    return LoadResult(
        table=table,
        rows=rows,
        files=[path.name for path, _ in file_years],
        years=years,
    )


def _parse_years_arg(value: str | None) -> list[int] | None:
    if value is None or value.strip() == "":
        return None
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return [int(p) for p in parts]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load Get It Done 311 data into DuckDB")
    parser.add_argument("--source", choices=["sample", "live"], default=config.SOURCE_MODE)
    parser.add_argument("--db", type=Path, default=config.DUCKDB_PATH)
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help=f"first closed year to load (default {config.CLOSED_START_YEAR})",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="comma-separated closed years to load (overrides --start-year)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="drop and rebuild the raw table from the resolved files only",
    )
    parser.add_argument(
        "--closed-only",
        action="store_true",
        help="skip the open extract (useful for closed-year backfills)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    years = _parse_years_arg(args.years)
    files = resolve_source_files(
        args.source,
        start_year=args.start_year,
        years=years,
        include_open=not args.closed_only,
    )
    result = load_to_duckdb(files, args.db, full_refresh=args.full_refresh)
    year_labels = ["open" if y is None else str(y) for y in result.years]
    print(
        f"{result.table}: {result.rows:,} rows from {', '.join(result.files)} "
        f"(partitions: {', '.join(year_labels)})"
    )


if __name__ == "__main__":
    main()

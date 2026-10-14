"""Extract + load: City Council district polygons for spatial district assignment.

The city publishes the nine council districts as GeoJSON (WGS84 lon/lat). Sample
mode uses a checked-in copy simplified to ~10 m so tests and CI stay offline;
live mode downloads the full-resolution file.

Geometry is landed as GeoJSON text in ``raw.council_districts``. Parsing into a
GEOMETRY and the point-in-polygon join happen in dbt with the DuckDB ``spatial``
extension, so this loader has no spatial dependency of its own.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from sd311 import config
from sd311.ingest import _download

log = logging.getLogger(__name__)

_POLYGON_TYPES = {"Polygon", "MultiPolygon"}

__all__ = ["DistrictLoadResult", "load_districts", "parse_districts", "resolve_districts_file"]


@dataclass(frozen=True)
class DistrictLoadResult:
    table: str
    rows: int
    file: str
    districts: list[int]


def resolve_districts_file(mode: str | None = None) -> Path:
    """Return the district GeoJSON for *mode*, downloading when mode == 'live'."""
    mode = mode or config.SOURCE_MODE
    if mode == "sample":
        path = config.REFERENCE_DIR / config.DISTRICTS_SAMPLE_FILE
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    if mode == "live":
        return _download(config.DISTRICTS_LIVE_URL, config.RAW_DIR / config.DISTRICTS_LIVE_FILE)
    raise ValueError(f"unknown source mode {mode!r}; expected 'sample' or 'live'")


def parse_districts(path: Path) -> list[tuple[int, int | None, str]]:
    """Validate a district FeatureCollection; return (district, objectid, geojson) rows.

    Fails fast on anything that would silently mis-assign requests downstream: a
    missing or duplicated district, a district outside 1-9, or a non-polygon shape.
    """
    doc = json.loads(Path(path).read_text())
    if doc.get("type") != "FeatureCollection":
        raise ValueError(f"{path}: expected a GeoJSON FeatureCollection")

    rows: list[tuple[int, int | None, str]] = []
    for i, feature in enumerate(doc.get("features", [])):
        props = feature.get("properties") or {}
        geom = feature.get("geometry") or {}
        try:
            district = int(props["district"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{path}: feature {i} has no integer 'district'") from exc
        if geom.get("type") not in _POLYGON_TYPES:
            raise ValueError(
                f"{path}: district {district} geometry is {geom.get('type')!r}, not a polygon"
            )
        objectid = props.get("objectid")
        rows.append((district, None if objectid is None else int(objectid), json.dumps(geom)))

    seen = [d for d, _, _ in rows]
    dupes = sorted({d for d in seen if seen.count(d) > 1})
    if dupes:
        raise ValueError(f"{path}: duplicate districts {dupes}")
    if set(seen) != config.EXPECTED_DISTRICTS:
        missing = sorted(config.EXPECTED_DISTRICTS - set(seen))
        extra = sorted(set(seen) - config.EXPECTED_DISTRICTS)
        raise ValueError(f"{path}: district set mismatch (missing={missing}, unexpected={extra})")
    return sorted(rows)


def load_districts(path: Path, db_path: Path | None = None) -> DistrictLoadResult:
    """Replace ``raw.council_districts`` with the polygons in *path* (idempotent)."""
    path = Path(path)
    rows = parse_districts(path)
    db_path = Path(db_path or config.DUCKDB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table = f"{config.RAW_SCHEMA}.{config.DISTRICTS_TABLE}"

    with duckdb.connect(str(db_path)) as con:
        con.execute(f"create schema if not exists {config.RAW_SCHEMA}")
        con.execute(
            f"""
            create or replace table {table} (
                district integer,
                objectid integer,
                geometry_geojson varchar,
                _source_file varchar,
                _loaded_at timestamp
            )
            """
        )
        con.executemany(
            f"insert into {table} values (?, ?, ?, ?, current_timestamp)",
            [(d, o, g, path.name) for d, o, g in rows],
        )
        count = con.execute(f"select count(*) from {table}").fetchone()[0]

    log.info("loaded %s council districts into %s from %s", count, table, path.name)
    return DistrictLoadResult(
        table=table, rows=count, file=path.name, districts=[d for d, _, _ in rows]
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load council district polygons into DuckDB")
    parser.add_argument("--source", choices=["sample", "live"], default=config.SOURCE_MODE)
    parser.add_argument("--db", type=Path, default=config.DUCKDB_PATH)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = load_districts(resolve_districts_file(args.source), args.db)
    print(f"{result.table}: {result.rows} districts from {result.file}")


if __name__ == "__main__":
    main()

"""Council district loader (unit) and spatial district assignment (dbt integration)."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from sd311 import config
from sd311.districts import load_districts, parse_districts, resolve_districts_file

SAMPLE = config.REFERENCE_DIR / config.DISTRICTS_SAMPLE_FILE


def _square(x: float, y: float, d: float = 0.01) -> dict:
    ring = [[x, y], [x + d, y], [x + d, y + d], [x, y + d], [x, y]]
    return {"type": "Polygon", "coordinates": [ring]}


def _write(tmp_path: Path, features: list[dict]) -> Path:
    path = tmp_path / "districts.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return path


def _features(districts: list[int]) -> list[dict]:
    return [
        {
            "type": "Feature",
            "properties": {"district": d, "objectid": d},
            "geometry": _square(-117.2 + d * 0.02, 32.7),
        }
        for d in districts
    ]


def test_sample_reference_file_has_all_nine_districts():
    rows = parse_districts(resolve_districts_file("sample"))
    assert [d for d, _, _ in rows] == list(range(1, 10))
    assert resolve_districts_file("sample") == SAMPLE
    # Simplified copy stays small enough to check in.
    assert SAMPLE.stat().st_size < 200_000


@pytest.mark.parametrize(
    ("districts", "message"),
    [
        ([1, 2, 3, 4, 5, 6, 7, 8], "missing=[9]"),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "unexpected=[10]"),
        ([1, 2, 3, 4, 5, 6, 7, 8, 8, 9], "duplicate districts [8]"),
    ],
)
def test_parse_rejects_bad_district_sets(tmp_path, districts, message):
    with pytest.raises(ValueError, match=message.replace("[", r"\[").replace("]", r"\]")):
        parse_districts(_write(tmp_path, _features(districts)))


def test_parse_rejects_non_polygon_geometry(tmp_path):
    features = _features(list(range(1, 10)))
    features[0]["geometry"] = {"type": "Point", "coordinates": [-117.1, 32.7]}
    with pytest.raises(ValueError, match="not a polygon"):
        parse_districts(_write(tmp_path, features))


def test_load_is_idempotent(tmp_path):
    db = tmp_path / "d.duckdb"
    load_districts(SAMPLE, db)
    result = load_districts(SAMPLE, db)
    assert result.rows == 9
    assert result.districts == list(range(1, 10))
    with duckdb.connect(str(db), read_only=True) as con:
        ((n, distinct),) = con.execute(
            "select count(*), count(distinct district) from raw.council_districts"
        ).fetchall()
    assert n == distinct == 9


def test_unknown_mode_rejected():
    with pytest.raises(ValueError, match="unknown source mode"):
        resolve_districts_file("nope")


def _q(db, sql, params=None):
    with duckdb.connect(str(db), read_only=True) as con:
        con.execute("load spatial")
        return con.execute(sql, params or []).fetchall()


@pytest.mark.parametrize(
    ("place", "lat", "lng", "district"),
    [
        ("City Hall", 32.7165, -117.1611, 3),
        ("La Jolla Cove", 32.8503, -117.2713, 1),
        ("Rancho Penasquitos", 32.9595, -117.1031, 5),
        ("SDSU", 32.7757, -117.0719, 9),
        ("Chula Vista (not in the city)", 32.6401, -117.0842, None),
    ],
)
def test_landmarks_fall_in_the_right_district(built_warehouse, place, lat, lng, district):
    rows = _q(
        built_warehouse,
        "select council_district from staging.stg_city_gis__council_districts "
        "where st_contains(geom, st_point(?, ?))",
        [lng, lat],
    )
    assert [r[0] for r in rows] == ([] if district is None else [district]), place


def test_fact_prefers_gis_district(built_warehouse):
    ((bad,),) = _q(
        built_warehouse,
        "select count(*) from marts.fct_requests "
        "where gis_council_district is not null and council_district <> gis_council_district",
    )
    assert bad == 0


def test_most_geocoded_requests_get_a_gis_district(built_warehouse):
    ((valid, assigned, mismatched),) = _q(
        built_warehouse,
        "select count(*) filter (where has_valid_geo), "
        "count(*) filter (where council_district_source = 'gis'), "
        "count(*) filter (where not reported_district_matches_gis) "
        "from marts.fct_requests",
    )
    assert assigned >= 0.98 * valid
    assert mismatched <= 0.02 * assigned


def test_district_join_does_not_fan_out(built_warehouse):
    ((n,),) = _q(built_warehouse, "select count(*) from marts.fct_requests")
    assert n == 4000

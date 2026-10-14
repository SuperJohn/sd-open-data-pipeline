-- One row per City Council district with a GEOMETRY (WGS84 lon/lat) for spatial joins.
-- Materialized as a table so the polygons are parsed once, not on every request lookup.
{{ config(materialized='table') }}

select
    district                                    as council_district,
    objectid,
    st_geomfromgeojson(geometry_geojson)        as geom,
    _source_file,
    _loaded_at
from {{ source('raw', 'council_districts') }}

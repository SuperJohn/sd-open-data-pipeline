-- Requests with a valid in-city geocode should land inside a council district polygon.
-- A handful fall in county islands or neighboring cities inside the bounding box; more than
-- 2% means the boundary file is wrong or incomplete. Returns a row (fails) above that.
{{ config(severity='warn') }}

with geocoded as (
    select
        count(*)                                                as valid_geo,
        count(*) filter (where gis_council_district is null)    as outside_districts
    from {{ ref('int_requests__enriched') }}
    where has_valid_geo
)

select valid_geo, outside_districts, outside_districts / valid_geo as outside_fraction
from geocoded
where valid_geo > 0 and outside_districts / valid_geo > 0.02

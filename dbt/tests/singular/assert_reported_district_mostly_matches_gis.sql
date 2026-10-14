-- The city's reported council_district should agree with the official boundaries for almost
-- every geocoded request. A jump in disagreement means stale districts (e.g. after
-- redistricting), swapped lat/lng, or a geocoding problem upstream. Returns a row (fails)
-- when more than 2% of comparable requests disagree.
{{ config(severity='warn') }}

with compared as (
    select
        count(*) filter (where reported_district_matches_gis is not null) as comparable,
        count(*) filter (where not reported_district_matches_gis)         as mismatched
    from {{ ref('int_requests__enriched') }}
)

select comparable, mismatched, mismatched / comparable as mismatch_fraction
from compared
where comparable > 0 and mismatched / comparable > 0.02

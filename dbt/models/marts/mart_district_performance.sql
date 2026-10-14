-- Council-district scorecard. District comes from the official GIS boundaries when the
-- request has a valid geocode, else the city-reported value (null = Unassigned).
select
    coalesce(cast(council_district as varchar), 'Unassigned')        as council_district,
    count(*)                                                         as total_requests,
    count(*) filter (where is_open)                                  as open_backlog,
    round(median(days_to_close), 1)                                  as median_days_to_close,
    round(
        avg(case when closed_at is not null then closed_within_sla::int end) * 100, 1
    )                                                                as pct_closed_within_sla,
    mode(service_name)                                               as top_service,
    round(
        avg(case when council_district_source = 'gis' then 1.0 else 0.0 end) * 100, 1
    )                                                                as pct_assigned_by_gis,
    round(
        avg(case when reported_district_matches_gis is not null
                 then (not reported_district_matches_gis)::int end) * 100, 1
    )                                                                as pct_reported_district_mismatch
from {{ ref('fct_requests') }}
group by 1

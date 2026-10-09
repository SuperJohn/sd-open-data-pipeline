-- Council-district scorecard (null district = location outside a district / not geocoded).
select
    coalesce(cast(council_district as varchar), 'Unassigned')        as council_district,
    count(*)                                                         as total_requests,
    count(*) filter (where is_open)                                  as open_backlog,
    round(median(days_to_close), 1)                                  as median_days_to_close,
    round(
        avg(case when closed_at is not null then closed_within_sla::int end) * 100, 1
    )                                                                as pct_closed_within_sla,
    mode(service_name)                                               as top_service
from {{ ref('fct_requests') }}
group by 1

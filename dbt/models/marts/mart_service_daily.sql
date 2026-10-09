-- Daily volume by service: the time-series behind the dashboard trend chart.
select
    requested_date,
    service_name,
    count(*)                                         as requests_opened,
    count(*) filter (where closed_at is not null)    as requests_closed_to_date,
    count(*) filter (where is_open)                  as requests_still_open,
    median(days_to_close)                            as median_days_to_close
from {{ ref('fct_requests') }}
group by all

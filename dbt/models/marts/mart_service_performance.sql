-- Service-level performance: volume, resolution speed, SLA attainment and open backlog.
select
    service_name,
    count(*)                                                         as total_requests,
    count(*) filter (where closed_at is not null)                    as closed_requests,
    count(*) filter (where is_open)                                  as open_backlog,
    round(median(days_to_close), 1)                                  as median_days_to_close,
    round(quantile_cont(days_to_close, 0.9), 1)                      as p90_days_to_close,
    round(
        avg(case when closed_at is not null then closed_within_sla::int end) * 100, 1
    )                                                                as pct_closed_within_sla,
    round(median(open_age_days), 1)                                  as median_open_age_days,
    round(avg(case when channel = 'Self-service' then 1 else 0 end) * 100, 1)
                                                                     as pct_self_service
from {{ ref('fct_requests') }}
group by service_name

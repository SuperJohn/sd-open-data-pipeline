-- Reconciliation: the daily aggregate must account for every request in the fact table.
with fact as (select count(*) as n from {{ ref('fct_requests') }}),
     daily as (select sum(requests_opened) as n from {{ ref('mart_service_daily') }})
select fact.n as fact_rows, daily.n as daily_rows
from fact, daily
where fact.n != daily.n

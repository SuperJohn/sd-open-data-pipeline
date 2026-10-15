-- Analytics-ready fact table, one row per 311 request.
--
-- Incremental on requested_at (city date_requested) and _loaded_at. Upserts by
-- service_request_id so status / SLA fields refresh when staging picks up a newer
-- extract. Aggregate marts stay full-table rebuilds over this fact.
-- Rebuild with: dbt build --full-refresh   or   make dbt-full-refresh
{{
  config(
    materialized='incremental',
    unique_key='service_request_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
  )
}}

select
    service_request_id,
    is_child_request,
    requested_at,
    requested_date,
    requested_week,
    requested_dow,
    requested_hour,
    closed_at,
    closed_date,
    status,
    is_open,
    is_referred,
    days_to_close,
    open_age_days,
    days_to_close is not null and days_to_close <= {{ var('sla_days') }} as closed_within_sla,
    case_record_type,
    service_name,
    service_name_detail,
    case_origin,
    channel,
    council_district,
    council_district_source,
    reported_council_district,
    gis_council_district,
    reported_district_matches_gis,
    community_plan,
    zipcode,
    case when has_valid_geo then latitude end  as latitude,
    case when has_valid_geo then longitude end as longitude,
    has_valid_geo,
    _loaded_at
from {{ ref('int_requests__enriched') }}
{% if is_incremental() %}
where
    _loaded_at > (
        select coalesce(max(_loaded_at), '1900-01-01'::timestamp) from {{ this }}
    )
    or requested_at >= (
        select coalesce(
            max(requested_at) - interval '{{ var("incremental_lookback_days") }}' day,
            '1900-01-01'::timestamp
        )
        from {{ this }}
    )
{% endif %}

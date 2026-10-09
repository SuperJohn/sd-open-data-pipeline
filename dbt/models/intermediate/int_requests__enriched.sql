-- Business logic: lifecycle flags, resolution time, calendar attributes, geo validity.
with requests as (
    select * from {{ ref('stg_get_it_done__requests') }}
)

select
    service_request_id,
    parent_request_id,
    parent_request_id is not null                                       as is_child_request,
    requested_at,
    closed_at,
    cast(requested_at as date)                                          as requested_date,
    date_trunc('week', requested_at)::date                              as requested_week,
    dayname(requested_at)                                               as requested_dow,
    hour(requested_at)                                                  as requested_hour,
    cast(closed_at as date)                                             as closed_date,
    status,
    status in ('New', 'In Process')                                     as is_open,
    status = 'Referred'                                                 as is_referred,
    case
        when closed_at is not null
            then greatest(date_diff('day', cast(requested_at as date), cast(closed_at as date)), 0)
    end                                                                 as days_to_close,
    case
        when status in ('New', 'In Process')
            then date_diff('day', cast(requested_at as date), current_date)
    end                                                                 as open_age_days,
    case_record_type,
    service_name,
    service_name_detail,
    case_origin,
    case when case_origin in ('Mobile', 'Web') then 'Self-service'
         when case_origin in ('Phone', 'Email') then 'Assisted'
         else 'Internal / other' end                                    as channel,
    council_district,
    community_plan,
    zipcode,
    latitude,
    longitude,
    coalesce(
        latitude between {{ var('sd_lat_min') }} and {{ var('sd_lat_max') }}
        and longitude between {{ var('sd_lng_min') }} and {{ var('sd_lng_max') }},
        false
    )                                                                   as has_valid_geo,
    _source_file,
    _loaded_at
from requests

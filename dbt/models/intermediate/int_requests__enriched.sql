-- Business logic: lifecycle flags, resolution time, calendar attributes, geo validity,
-- and council district assigned from official GIS boundaries.
with requests as (
    select * from {{ ref('stg_get_it_done__requests') }}
),

districts as (
    select council_district, geom from {{ ref('stg_city_gis__council_districts') }}
),

gis_district as (
    -- Point-in-polygon on valid geocodes only. Shared borders can make a point fall in two
    -- polygons (or the ~10 m simplified sample shapes can overlap slightly); keep the lowest
    -- district so the join can never fan out.
    select
        r.service_request_id,
        d.council_district as gis_council_district
    from requests as r
    inner join districts as d
        on st_contains(d.geom, st_point(r.longitude, r.latitude))
    where r.latitude between {{ var('sd_lat_min') }} and {{ var('sd_lat_max') }}
      and r.longitude between {{ var('sd_lng_min') }} and {{ var('sd_lng_max') }}
    qualify row_number() over (
        partition by r.service_request_id order by d.council_district
    ) = 1
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
    requests.council_district                                          as reported_council_district,
    g.gis_council_district,
    coalesce(g.gis_council_district, requests.council_district)         as council_district,
    case
        when g.gis_council_district is not null then 'gis'
        when requests.council_district is not null then 'reported'
        else 'none'
    end                                                                 as council_district_source,
    case
        when g.gis_council_district is not null and requests.council_district is not null
            then g.gis_council_district = requests.council_district
    end                                                                 as reported_district_matches_gis,
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
left join gis_district as g using (service_request_id)

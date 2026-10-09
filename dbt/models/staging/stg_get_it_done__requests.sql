-- One row per service request: typed, trimmed, and deduplicated across extracts.
with source as (
    select * from {{ source('raw', 'get_it_done_requests') }}
),

typed as (
    select
        trim(service_request_id)                                   as service_request_id,
        nullif(trim(service_request_parent_id), '')                as parent_request_id,
        try_cast(date_requested as timestamp)                      as requested_at,
        try_cast(date_closed as timestamp)                         as closed_at,
        trim(status)                                               as status,
        nullif(trim(case_record_type), '')                         as case_record_type,
        coalesce(nullif(trim(service_name), ''), 'Unknown')        as service_name,
        nullif(trim(service_name_detail), '')                      as service_name_detail,
        coalesce(nullif(trim(case_origin), ''), 'Unknown')         as case_origin,
        try_cast(lat as double)                                    as latitude,
        try_cast(lng as double)                                    as longitude,
        nullif(trim(street_address), '')                           as street_address,
        nullif(left(trim(zipcode), 5), '')                         as zipcode,
        try_cast(council_district as integer)                      as council_district,
        nullif(trim(comm_plan_name), '')                           as community_plan,
        nullif(trim(park_name), '')                                as park_name,
        nullif(trim(referred), '')                                 as referred_to,
        _source_file,
        _loaded_at
    from source
),

deduped as (
    -- A request can appear in both the open and closed extracts while it transitions;
    -- prefer the record that carries a close date, then the most recent load.
    select *
    from typed
    qualify row_number() over (
        partition by service_request_id
        order by (closed_at is not null) desc, _loaded_at desc, _source_file
    ) = 1
)

select * from deduped

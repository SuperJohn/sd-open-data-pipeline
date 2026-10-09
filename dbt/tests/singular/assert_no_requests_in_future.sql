-- A request timestamp in the future means a timezone or parsing bug upstream.
select service_request_id, requested_at
from {{ ref('stg_get_it_done__requests') }}
where requested_at > current_timestamp + interval 1 day

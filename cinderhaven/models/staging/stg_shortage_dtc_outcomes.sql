with source as (
    select * from {{ source('raw', 'shortage_dtc_outcomes') }}
)

select
    order_id,
    hold_start_date,
    resolution,
    resolution_date,
    days_held,
    loaded_at
from source

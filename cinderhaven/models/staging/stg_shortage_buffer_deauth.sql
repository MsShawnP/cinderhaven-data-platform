with source as (
    select * from {{ source('raw', 'shortage_buffer_deauth') }}
)

select
    target_fill_rate,
    sku,
    retailer,
    trigger_type,
    original_status,
    simulated_status,
    loaded_at
from source

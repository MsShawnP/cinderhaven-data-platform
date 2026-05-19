with source as (
    select * from {{ source('raw', 'shortage_order_shorts') }}
)

select
    short_id,
    order_id,
    sku,
    quantity_shorted,
    short_reason,
    loaded_at
from source

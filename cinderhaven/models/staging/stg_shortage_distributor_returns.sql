with source as (
    select * from {{ source('raw', 'shortage_distributor_returns') }}
)

select
    return_id,
    order_id,
    sku,
    quantity_returned,
    return_reason,
    return_date,
    credit_amount,
    loaded_at
from source

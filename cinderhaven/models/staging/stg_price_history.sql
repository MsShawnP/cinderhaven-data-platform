with source as (
    select * from {{ source('raw', 'price_history') }}
)

select
    sku,
    retailer_id,
    effective_date,
    wholesale_price
from source

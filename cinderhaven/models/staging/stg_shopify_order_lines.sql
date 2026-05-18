with source as (
    select * from {{ source('raw', 'shopify_order_lines') }}
)

select
    order_id,
    sku,
    product_name,
    quantity,
    unit_price,
    line_total
from source

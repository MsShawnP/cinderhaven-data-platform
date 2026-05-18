with source as (
    select * from {{ source('raw', 'shopify_orders') }}
)

select
    order_id,
    order_number,
    created_at,
    email,
    financial_status,
    fulfillment_status,
    subtotal,
    shipping_cost,
    total_tax,
    total,
    discount_code,
    discount_amount
from source

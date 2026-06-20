with source as (
    select * from {{ source('raw', 'shopify_transactions') }}
)

select
    transaction_id,
    order_id,
    transaction_date,
    order_amount,
    processing_fee,
    platform_fee,
    net_amount,
    gateway,
    card_brand
from source

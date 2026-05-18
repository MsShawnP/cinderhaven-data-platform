with source as (
    select * from {{ source('raw', 'shopify_refunds') }}
)

select
    refund_id,
    order_id,
    refund_date,
    refund_amount,
    reason
from source

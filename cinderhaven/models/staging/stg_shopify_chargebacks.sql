with source as (
    select * from {{ source('raw', 'shopify_chargebacks') }}
)

select
    chargeback_id,
    order_id,
    chargeback_date,
    chargeback_amount,
    reason,
    outcome
from source

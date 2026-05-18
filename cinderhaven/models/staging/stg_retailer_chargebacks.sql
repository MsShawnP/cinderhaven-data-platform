with source as (
    select * from {{ source('raw', 'retailer_chargebacks') }}
)

select
    chargeback_id,
    month as chargeback_month,
    retailer_id,
    reason,
    sku,
    amount as chargeback_amount
from source

with source as (
    select * from {{ source('raw', 'shopify_payouts') }}
)

select
    payout_id,
    payout_date,
    gross_amount,
    fees_amount,
    net_amount,
    status as payout_status
from source

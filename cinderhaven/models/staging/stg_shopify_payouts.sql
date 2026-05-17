with source as (
    select * from {{ source('raw', 'shopify_payouts') }}
),

staged as (
    select
        payout_id,
        payout_date,
        gross_amount,
        fees_amount,
        refunds_amount,
        chargebacks_amount,
        net_amount,
        transaction_count,
        status
    from source
)

select * from staged

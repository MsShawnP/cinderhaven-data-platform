with source as (
    select * from {{ source('raw', 'shopify_chargebacks') }}
),

staged as (
    select
        chargeback_id,
        order_id,
        chargeback_date,
        chargeback_amount,
        chargeback_fee,
        reason,
        outcome
    from source
)

select * from staged

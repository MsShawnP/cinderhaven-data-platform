with source as (
    select * from {{ source('raw', 'shopify_transactions') }}
),

staged as (
    select
        transaction_id,
        order_id,
        transaction_date,
        order_amount,
        processing_fee,
        net_amount,
        payment_method,
        status
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'shopify_refunds') }}
),

staged as (
    select
        refund_id,
        order_id,
        refund_date,
        refund_amount,
        refund_type,
        reason
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'shopify_orders') }}
),

staged as (
    select
        order_id,
        order_number,
        created_at::timestamp as created_at,
        email,
        financial_status,
        fulfillment_status,
        shipping_first_name,
        shipping_last_name,
        shipping_state,
        discount_code,
        discount_amount,
        subtotal,
        shipping_cost,
        total_tax,
        total,
        carrier,
        tracking_number,
        fulfilled_at::timestamp as fulfilled_at
    from source
)

select * from staged

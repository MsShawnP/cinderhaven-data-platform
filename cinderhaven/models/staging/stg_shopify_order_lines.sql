with source as (
    select * from {{ source('raw', 'shopify_order_lines') }}
),

staged as (
    select
        line_id,
        order_id,
        sku,
        product_name,
        quantity,
        unit_price,
        line_total
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'order_lines') }}
),

staged as (
    select
        order_line_id,
        order_id,
        sku,
        units_ordered,
        unit_price,
        line_total
    from source
)

select * from staged

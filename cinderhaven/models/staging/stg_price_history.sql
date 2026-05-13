with source as (
    select * from {{ source('raw', 'price_history') }}
),

staged as (
    select
        sku,
        retailer,
        effective_date::date as effective_date,
        wholesale_price
    from source
)

select * from staged

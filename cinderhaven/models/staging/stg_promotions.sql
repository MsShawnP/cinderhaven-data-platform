with source as (
    select * from {{ source('raw', 'promotions') }}
),

staged as (
    select
        promo_id,
        sku,
        retailer,
        store_scope,
        start_week::date as start_week,
        end_week::date as end_week,
        duration_weeks,
        discount_depth_pct,
        promo_type,
        promo_cost,
        funding_mechanism
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'promotions') }}
)

select
    promo_id,
    sku,
    retailer_id,
    start_week,
    end_week,
    discount_depth_pct,
    promo_type,
    promo_cost,
    funding_mechanism
from source

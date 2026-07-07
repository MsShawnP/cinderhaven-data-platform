with promotions as (
    select * from {{ ref('stg_promotions') }}
),

retailers as (
    select retailer_id, retailer_name from {{ ref('stg_retailers') }}
)

select
    p.promo_id,
    p.sku,
    r.retailer_name as retailer,
    p.start_week,
    p.end_week,
    p.discount_depth_pct,
    p.promo_type,
    p.promo_cost,
    p.funding_mechanism

from promotions p
inner join retailers r on p.retailer_id = r.retailer_id

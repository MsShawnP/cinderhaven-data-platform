-- fct_promotions: Trade promotion events with cost and discount details.
--
-- Grain: one row per promotion event.

with promotions as (
    select * from {{ ref('stg_promotions') }}
),

retailers as (
    select * from {{ ref('stg_retailers') }}
),

final as (
    select
        promotions.promo_id,
        promotions.sku,
        retailers.retailer_id,
        promotions.retailer as retailer_name,
        promotions.store_scope,
        promotions.start_week,
        promotions.end_week,
        promotions.duration_weeks,
        promotions.discount_depth_pct,
        promotions.promo_type,
        promotions.promo_cost,
        promotions.funding_mechanism
    from promotions
    left join retailers
        on promotions.retailer = retailers.name
)

select * from final

-- Grain: one row per SKU
-- Annual cost to maintain a SKU on shelf = promotional trade spend
-- plus an estimated data/compliance overhead per active store.
--
-- Methodology:
--   1. Promotional costs from stg_promotions (actual promo events, SKU-level)
--   2. Active store count from distribution_log (deauthorized_date is null)
--   3. Maintenance proxy: $400/store/year for EDI compliance, data feeds,
--      broker account management, and retailer portal overhead.
--      This is a proxy estimate — documented in docs/methodology.md.

with promo_costs as (
    select
        sku,
        sum(promo_cost) as total_promo_cost
    from {{ ref('stg_promotions') }}
    group by sku
),

active_distribution as (
    select
        sku,
        count(distinct store_id) as active_store_count
    from {{ ref('stg_distribution_log') }}
    where deauthorized_date is null
    group by sku
),

all_skus as (
    select sku, product_line
    from {{ ref('stg_product_master') }}
)

select
    s.sku,
    s.product_line,

    coalesce(d.active_store_count, 0)                                    as active_store_count,
    coalesce(p.total_promo_cost, 0)                                      as annual_promo_cost,

    -- $400/store/year proxy for data maintenance and compliance overhead
    coalesce(d.active_store_count, 0) * 400                              as maintenance_cost_proxy,

    -- Total annual shelf-space cost
    coalesce(p.total_promo_cost, 0)
        + (coalesce(d.active_store_count, 0) * 400)                      as annual_shelf_space_cost

from all_skus s
left join active_distribution d on s.sku = d.sku
left join promo_costs p         on s.sku = p.sku

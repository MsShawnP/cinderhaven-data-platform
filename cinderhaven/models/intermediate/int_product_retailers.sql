-- SKU-across-systems resolution: maps each product to the retailers
-- that carry it, with retailer-specific pricing and distribution status.
--
-- Grain: one row per SKU × retailer combination.
-- Joins product_master → sku_costs (unpivoted) → distribution_log
-- to build a unified product-retailer view.

with products as (
    select * from {{ ref('stg_product_master') }}
),

costs as (
    select * from {{ ref('stg_sku_costs') }}
),

-- Unpivot retailer-specific wholesale prices from wide to long.
retailer_prices as (
    select sku, 'Walmart' as retailer, wholesale_walmart as wholesale_price, trade_spend_pct_walmart as trade_spend_pct from costs
    union all
    select sku, 'Costco', wholesale_costco, trade_spend_pct_costco from costs
    union all
    select sku, 'Whole Foods', wholesale_whole_foods, trade_spend_pct_whole_foods from costs
    union all
    select sku, 'Regional', wholesale_regional, trade_spend_pct_regional from costs
    union all
    select sku, 'UNFI', wholesale_unfi, trade_spend_pct_unfi from costs
    union all
    select sku, 'DTC', wholesale_dtc, trade_spend_pct_dtc from costs
),

-- Current distribution status: count of active stores per SKU × retailer.
distribution as (
    select
        dl.sku,
        s.retailer,
        count(distinct dl.store_id) as active_store_count,
        min(dl.authorized_date) as earliest_authorization,
        max(dl.authorized_date) as latest_authorization
    from {{ ref('stg_distribution_log') }} dl
    inner join {{ ref('stg_stores') }} s on dl.store_id = s.store_id
    where dl.deauthorized_date is null
    group by dl.sku, s.retailer
),

combined as (
    select
        products.sku,
        products.product_name,
        products.product_line,
        products.subcategory,
        products.msrp,
        rp.retailer,
        rp.wholesale_price,
        rp.trade_spend_pct,
        costs.cogs_per_unit,
        costs.landed_cost_per_unit,
        rp.wholesale_price - costs.cogs_per_unit as gross_margin_per_unit,
        case
            when costs.cogs_per_unit > 0
            then (rp.wholesale_price - costs.cogs_per_unit) / rp.wholesale_price
        end as gross_margin_pct,
        coalesce(dist.active_store_count, 0) as active_store_count,
        dist.earliest_authorization,
        dist.latest_authorization
    from products
    inner join costs on products.sku = costs.sku
    inner join retailer_prices rp on products.sku = rp.sku
    left join distribution dist
        on products.sku = dist.sku
        and rp.retailer = dist.retailer
)

select * from combined

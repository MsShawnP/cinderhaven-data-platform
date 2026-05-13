-- dim_products: Product dimension with GTIN hierarchy, pricing, and cost.
--
-- Grain: one row per SKU.
-- Combines product_master with sku_costs for a single product lookup.

with products as (
    select * from {{ ref('stg_product_master') }}
),

costs as (
    select * from {{ ref('stg_sku_costs') }}
),

final as (
    select
        products.sku,
        products.product_name,
        products.product_line,
        products.subcategory,
        products.gtin14,
        products.upc,
        -- GTIN hierarchy: GTIN-14 → UPC (GTIN-12) → SKU
        case
            when products.gtin14 is not null and length(products.gtin14) = 14
            then true else false
        end as has_valid_gtin,
        products.case_pack_qty,
        products.unit_weight_lbs,
        products.case_weight_lbs,
        products.case_length_in,
        products.case_width_in,
        products.case_height_in,
        products.msrp,
        costs.cogs_per_unit,
        costs.landed_cost_per_unit,
        costs.wholesale_price as wholesale_price_base,
        products.msrp - coalesce(costs.cogs_per_unit, 0) as msrp_margin,
        case
            when products.msrp > 0
            then (products.msrp - coalesce(costs.cogs_per_unit, 0)) / products.msrp
        end as msrp_margin_pct,
        products.serving_size,
        products.calories_per_serving,
        products.brand_owner,
        products.country_of_origin,
        products.oneworldsync_status,
        products.last_updated
    from products
    left join costs on products.sku = costs.sku
)

select * from final

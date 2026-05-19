-- Orders where line_total deviates from units_ordered * unit_price by more
-- than $0.01 (rounding tolerance). Validates that generated and source
-- data maintain arithmetic consistency at the line level.

with all_order_lines as (
    select order_id, sku, units_ordered, unit_price, line_total
    from {{ ref('stg_retailer_order_lines') }}
    union all
    select order_id, sku, units_ordered, unit_price, line_total
    from {{ ref('stg_distributor_order_lines') }}
    union all
    select order_id, sku, quantity as units_ordered, unit_price, line_total
    from {{ ref('stg_shopify_order_lines') }}
)

select
    order_id,
    sku,
    units_ordered,
    unit_price,
    line_total,
    abs(line_total - (units_ordered * unit_price)) as diff
from all_order_lines
where abs(line_total - (units_ordered * unit_price)) > 0.01

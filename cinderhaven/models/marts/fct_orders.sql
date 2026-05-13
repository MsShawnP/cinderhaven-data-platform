-- fct_orders: Unified order fact combining B2B and DTC channels.
--
-- Grain: one row per order line item.
-- B2B orders come from orders + order_lines.
-- DTC orders come from shopify_orders + shopify_order_lines.

with b2b_lines as (
    select
        ol.order_line_id::text as line_id,
        ol.order_id,
        o.retailer_id,
        'B2B' as channel,
        o.po_number,
        o.po_date as order_date,
        ol.sku,
        ol.units_ordered as quantity,
        ol.unit_price,
        ol.line_total,
        o.total_value as order_total
    from {{ ref('stg_order_lines') }} ol
    inner join {{ ref('stg_orders') }} o on ol.order_id = o.order_id
),

dtc_lines as (
    select
        sol.line_id::text as line_id,
        sol.order_id,
        null as retailer_id,
        'DTC' as channel,
        so.order_number::text as po_number,
        so.created_at::date as order_date,
        sol.sku,
        sol.quantity,
        sol.unit_price,
        sol.line_total,
        so.total as order_total
    from {{ ref('stg_shopify_order_lines') }} sol
    inner join {{ ref('stg_shopify_orders') }} so on sol.order_id = so.order_id
),

final as (
    select * from b2b_lines
    union all
    select * from dtc_lines
)

select * from final

with economics as (
    select * from {{ ref('int_dtc_order_economics') }}
),

order_cogs as (
    select
        ol.order_id,
        count(*) as line_count,
        count(distinct ol.sku) as sku_count,
        sum(ol.quantity) as total_units,
        sum(ol.quantity * c.cogs_per_unit) as total_cogs
    from {{ ref('stg_shopify_order_lines') }} ol
    inner join {{ ref('stg_sku_costs') }} c on ol.sku = c.sku
    group by ol.order_id
)

select
    e.order_id,
    e.order_number,
    e.created_at,
    e.email,
    e.financial_status,
    e.subtotal,
    e.shipping_cost,
    e.total_tax,
    e.total,
    e.discount_code,
    e.discount_amount,

    e.fulfillment_cost,

    e.transaction_id,
    e.processing_fee,
    e.platform_fee,
    e.net_payment,
    e.gateway,

    e.refund_count,
    e.total_refund_amount,
    e.chargeback_count,
    e.total_chargeback_amount,

    coalesce(oc.line_count, 0) as line_count,
    coalesce(oc.sku_count, 0) as sku_count,
    coalesce(oc.total_units, 0) as total_units,
    coalesce(oc.total_cogs, 0) as total_cogs,

    e.gross_revenue,
    e.total_fees,
    e.total_fulfillment,
    e.total_returns,
    e.net_revenue,
    e.net_revenue - coalesce(oc.total_cogs, 0) as gross_profit

from economics e
left join order_cogs oc on e.order_id = oc.order_id

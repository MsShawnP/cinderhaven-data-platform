with retailer as (
    select
        'Retailer' as channel,
        (select sum(total_value) from {{ ref('stg_retailer_orders') }}) as gross_revenue,
        -- units_ordered is already in units (priced per unit at order-line
        -- generation); multiplying by case_pack_qty inflated COGS ~13x
        (select sum(ol.units_ordered * c.cogs_per_unit)
         from {{ ref('stg_retailer_order_lines') }} ol
         inner join {{ ref('stg_sku_costs') }} c on ol.sku = c.sku
        ) as total_cogs,
        (select sum(deduction_amount) from {{ ref('int_retailer_payments') }}) as total_deductions,
        (select sum(coalesce(recovered_amount, 0)) from {{ ref('int_retailer_payments') }}) as total_recovered,
        (select sum(chargeback_amount) from {{ ref('stg_retailer_chargebacks') }}) as total_chargebacks,
        (select sum(promo_cost) from {{ ref('stg_promotions') }}) as total_trade_spend
),

distributor as (
    select
        'Distributor' as channel,
        (select sum(total_value) from {{ ref('stg_distributor_orders') }}) as gross_revenue,
        -- units_ordered is already in units; see retailer CTE note
        (select sum(ol.units_ordered * c.cogs_per_unit)
         from {{ ref('stg_distributor_order_lines') }} ol
         inner join {{ ref('stg_sku_costs') }} c on ol.sku = c.sku
        ) as total_cogs,
        (select sum(deduction_amount) from {{ ref('stg_distributor_deductions') }}) as total_deductions,
        (select sum(coalesce(recovered_amount, 0)) from {{ ref('stg_distributor_disputes') }}) as total_recovered,
        (select sum(chargeback_amount) from {{ ref('stg_distributor_chargebacks') }}) as total_chargebacks,
        0::numeric as total_trade_spend
),

dtc as (
    select
        'DTC' as channel,
        (select sum(total) from {{ ref('stg_shopify_orders') }}) as gross_revenue,
        (select sum(ol.quantity * c.cogs_per_unit)
         from {{ ref('stg_shopify_order_lines') }} ol
         inner join {{ ref('stg_sku_costs') }} c on ol.sku = c.sku
        ) as total_cogs,
        (select sum(refund_amount) from {{ ref('stg_shopify_refunds') }}) as total_deductions,
        0::numeric as total_recovered,
        (select sum(chargeback_amount) from {{ ref('stg_shopify_chargebacks') }}) as total_chargebacks,
        0::numeric as total_trade_spend
),

channels as (
    select * from retailer
    union all
    select * from distributor
    union all
    select * from dtc
),

totals as (
    select sum(gross_revenue) as total_gross_revenue from channels
)

select
    c.channel,
    c.gross_revenue,
    c.total_cogs,
    c.gross_revenue - c.total_cogs as gross_margin,
    c.total_deductions,
    c.total_recovered,
    c.total_chargebacks,
    c.total_trade_spend,
    c.gross_revenue
        - coalesce(c.total_deductions, 0)
        + coalesce(c.total_recovered, 0)
        - coalesce(c.total_chargebacks, 0) as net_revenue,
    c.gross_revenue
        - c.total_cogs
        - coalesce(c.total_deductions, 0)
        + coalesce(c.total_recovered, 0)
        - coalesce(c.total_chargebacks, 0)
        - coalesce(c.total_trade_spend, 0) as contribution_margin,
    case
        when t.total_gross_revenue > 0
        then round(c.gross_revenue / t.total_gross_revenue, 4)
        else 0
    end as revenue_share

from channels c
cross join totals t

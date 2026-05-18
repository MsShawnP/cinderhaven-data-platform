with order_stats as (
    select
        count(*) as total_orders,
        sum(total) as total_revenue,
        min(created_at)::date as first_order_date,
        max(created_at)::date as last_order_date,
        count(distinct email) as unique_customers
    from {{ ref('stg_shopify_orders') }}
),

payout_stats as (
    select
        count(*) as total_payouts,
        sum(gross_amount) as total_payout_gross,
        sum(fees_amount) as total_payout_fees,
        sum(net_amount) as total_payout_net
    from {{ ref('stg_shopify_payouts') }}
),

refund_stats as (
    select
        count(*) as total_refunds,
        sum(refund_amount) as total_refund_amount
    from {{ ref('stg_shopify_refunds') }}
),

chargeback_stats as (
    select
        count(*) as total_chargebacks,
        sum(chargeback_amount) as total_chargeback_amount
    from {{ ref('stg_shopify_chargebacks') }}
)

select
    'SHOPIFY' as channel_id,
    'Shopify DTC' as channel_name,
    'direct_to_consumer' as channel_type,
    os.total_orders,
    os.total_revenue,
    os.first_order_date,
    os.last_order_date,
    os.unique_customers,
    ps.total_payouts,
    ps.total_payout_gross,
    ps.total_payout_fees,
    ps.total_payout_net,
    rs.total_refunds,
    rs.total_refund_amount,
    cbs.total_chargebacks,
    cbs.total_chargeback_amount

from order_stats os
cross join payout_stats ps
cross join refund_stats rs
cross join chargeback_stats cbs

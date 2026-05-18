with order_totals as (
    select
        count(*) as order_count,
        sum(total) as total_order_revenue,
        sum(subtotal) as total_subtotal,
        sum(shipping_cost) as total_shipping,
        sum(total_tax) as total_tax,
        sum(discount_amount) as total_discounts
    from {{ ref('stg_shopify_orders') }}
),

transaction_totals as (
    select
        count(*) as transaction_count,
        sum(order_amount) as total_transaction_amount,
        sum(processing_fee) as total_processing_fees,
        sum(net_amount) as total_transaction_net
    from {{ ref('stg_shopify_transactions') }}
),

payout_totals as (
    select
        count(*) as payout_count,
        sum(gross_amount) as total_payout_gross,
        sum(fees_amount) as total_payout_fees,
        sum(net_amount) as total_payout_net
    from {{ ref('stg_shopify_payouts') }}
),

refund_totals as (
    select
        count(*) as refund_count,
        sum(refund_amount) as total_refund_amount
    from {{ ref('stg_shopify_refunds') }}
),

chargeback_totals as (
    select
        count(*) as chargeback_count,
        sum(chargeback_amount) as total_chargeback_amount
    from {{ ref('stg_shopify_chargebacks') }}
)

select
    'SHOPIFY' as channel_id,

    ot.order_count,
    ot.total_order_revenue,
    ot.total_subtotal,
    ot.total_shipping,
    ot.total_tax,
    ot.total_discounts,

    tt.transaction_count,
    tt.total_transaction_amount,
    tt.total_processing_fees,
    tt.total_transaction_net,

    pt.payout_count,
    pt.total_payout_gross,
    pt.total_payout_fees,
    pt.total_payout_net,

    rt.refund_count,
    rt.total_refund_amount,

    cbt.chargeback_count,
    cbt.total_chargeback_amount,

    ot.total_order_revenue - tt.total_transaction_amount as orders_vs_transactions_diff,
    tt.total_transaction_net - pt.total_payout_net as transactions_vs_payouts_diff,
    ot.total_order_revenue
        - coalesce(rt.total_refund_amount, 0)
        - coalesce(cbt.total_chargeback_amount, 0)
        - tt.total_processing_fees as computed_net_revenue,
    tt.total_transaction_net
        - coalesce(rt.total_refund_amount, 0)
        - coalesce(cbt.total_chargeback_amount, 0) as adjusted_net

from order_totals ot
cross join transaction_totals tt
cross join payout_totals pt
cross join refund_totals rt
cross join chargeback_totals cbt

with orders as (
    select * from {{ ref('stg_shopify_orders') }}
),

transactions as (
    select * from {{ ref('stg_shopify_transactions') }}
),

refunds as (
    select
        order_id,
        count(*) as refund_count,
        sum(refund_amount) as total_refund_amount
    from {{ ref('stg_shopify_refunds') }}
    group by order_id
),

chargebacks as (
    select
        order_id,
        count(*) as chargeback_count,
        sum(chargeback_amount) as total_chargeback_amount
    from {{ ref('stg_shopify_chargebacks') }}
    group by order_id
)

select
    o.order_id,
    o.order_number,
    o.created_at,
    o.email,
    o.financial_status,
    o.subtotal,
    o.shipping_cost,
    o.total_tax,
    o.total,
    o.discount_code,
    o.discount_amount,

    t.transaction_id,
    t.processing_fee,
    t.net_amount as net_payment,
    t.gateway,

    coalesce(r.refund_count, 0) as refund_count,
    coalesce(r.total_refund_amount, 0) as total_refund_amount,

    coalesce(cb.chargeback_count, 0) as chargeback_count,
    coalesce(cb.total_chargeback_amount, 0) as total_chargeback_amount,

    o.total as gross_revenue,
    coalesce(t.processing_fee, 0) as total_fees,
    coalesce(r.total_refund_amount, 0)
        + coalesce(cb.total_chargeback_amount, 0) as total_returns,
    o.total
        - coalesce(t.processing_fee, 0)
        - coalesce(r.total_refund_amount, 0)
        - coalesce(cb.total_chargeback_amount, 0) as net_revenue

from orders o
left join transactions t on o.order_id = t.order_id
left join refunds r on o.order_id = r.order_id
left join chargebacks cb on o.order_id = cb.order_id

with chargebacks as (
    select * from {{ ref('stg_shopify_chargebacks') }}
),

orders as (
    select order_id, order_number, total, created_at
    from {{ ref('stg_shopify_orders') }}
)

select
    cb.chargeback_id,
    cb.order_id,
    o.order_number,
    cb.chargeback_date,
    cb.chargeback_amount,
    cb.reason,
    cb.outcome,
    o.total as original_order_total,
    case
        when o.total > 0
        then round(cb.chargeback_amount / o.total, 4)
        else 0
    end as chargeback_pct_of_order,
    cb.chargeback_date - o.created_at::date as days_to_chargeback

from chargebacks cb
inner join orders o on cb.order_id = o.order_id

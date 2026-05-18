with refunds as (
    select * from {{ ref('stg_shopify_refunds') }}
),

orders as (
    select order_id, order_number, total, created_at
    from {{ ref('stg_shopify_orders') }}
)

select
    r.refund_id,
    r.order_id,
    o.order_number,
    r.refund_date,
    r.refund_amount,
    r.reason,
    o.total as original_order_total,
    case
        when o.total > 0
        then round(r.refund_amount / o.total, 4)
        else 0
    end as refund_pct_of_order,
    r.refund_date::date - o.created_at::date as days_to_refund

from refunds r
inner join orders o on r.order_id = o.order_id

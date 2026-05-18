with transactions as (
    select * from {{ ref('stg_shopify_transactions') }}
),

orders as (
    select order_id, order_number, created_at, financial_status
    from {{ ref('stg_shopify_orders') }}
)

select
    t.transaction_id,
    t.order_id,
    o.order_number,
    t.transaction_date,
    o.financial_status,
    t.order_amount,
    t.processing_fee,
    t.net_amount,
    t.gateway,
    t.card_brand,
    case
        when t.order_amount > 0
        then round(t.processing_fee / t.order_amount, 4)
        else 0
    end as fee_rate

from transactions t
inner join orders o on t.order_id = o.order_id

with orders as (
    select * from {{ ref('stg_retailer_orders') }}
),

line_counts as (
    select
        order_id,
        count(*) as line_count,
        count(distinct sku) as sku_count
    from {{ ref('stg_retailer_order_lines') }}
    group by order_id
),

shipments as (
    select * from {{ ref('stg_retailer_shipments') }}
)

select
    o.order_id,
    o.retailer_id,
    o.po_number,
    o.po_date,
    o.requested_ship_date,
    o.total_units,
    o.total_value,
    coalesce(lc.line_count, 0) as line_count,
    coalesce(lc.sku_count, 0) as sku_count,

    s.shipment_id,
    s.ship_date,
    s.delivery_date,
    s.carrier,
    s.units_shipped,
    s.pallets_shipped,
    s.asn_sent,
    s.asn_sent_late,

    case
        when s.ship_date is not null and o.requested_ship_date is not null
        then s.ship_date <= o.requested_ship_date
    end as is_on_time,
    case
        when s.ship_date > o.requested_ship_date
        then s.ship_date - o.requested_ship_date
        else 0
    end as days_late

from orders o
left join line_counts lc on o.order_id = lc.order_id
left join shipments s on o.order_id = s.order_id

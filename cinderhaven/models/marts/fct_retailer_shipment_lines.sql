with lines as (
    select * from {{ ref('stg_retailer_shipment_lines') }}
),

shipments as (
    select shipment_id, order_id, ship_date
    from {{ ref('stg_retailer_shipments') }}
),

orders as (
    select order_id, retailer_id
    from {{ ref('stg_retailer_orders') }}
)

select
    l.shipment_id,
    l.sku,
    o.retailer_id,
    s.order_id,
    s.ship_date,
    l.units_ordered,
    l.units_shipped,
    l.units_ordered - l.units_shipped as units_short,
    l.units_shipped < l.units_ordered as is_short,
    l.shortfall_reason

from lines l
inner join shipments s on l.shipment_id = s.shipment_id
inner join orders o on s.order_id = o.order_id

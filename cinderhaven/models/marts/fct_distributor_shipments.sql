with shipments as (
    select * from {{ ref('stg_distributor_shipments') }}
),

orders as (
    select order_id, distributor_id, po_date
    from {{ ref('stg_distributor_orders') }}
)

select
    s.shipment_id,
    s.order_id,
    o.distributor_id,
    s.ship_date,
    s.delivery_date,
    s.carrier,
    s.units_shipped,
    o.po_date

from shipments s
inner join orders o on s.order_id = o.order_id

with receipts as (
    select * from {{ ref('stg_retailer_receipt_lines') }}
),

lines as (
    select shipment_id, sku, units_ordered, units_shipped
    from {{ ref('stg_retailer_shipment_lines') }}
),

shipments as (
    select shipment_id, order_id
    from {{ ref('stg_retailer_shipments') }}
),

orders as (
    select order_id, retailer_id
    from {{ ref('stg_retailer_orders') }}
)

select
    r.shipment_id,
    r.sku,
    o.retailer_id,
    s.order_id,
    l.units_ordered,
    l.units_shipped,
    r.units_received,
    l.units_shipped - r.units_received as units_discrepant,
    r.units_received < l.units_shipped as has_discrepancy,
    r.discrepancy_reason

from receipts r
inner join lines l on r.shipment_id = l.shipment_id and r.sku = l.sku
inner join shipments s on r.shipment_id = s.shipment_id
inner join orders o on s.order_id = o.order_id

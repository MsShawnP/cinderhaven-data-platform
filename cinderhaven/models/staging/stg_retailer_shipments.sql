with source as (
    select * from {{ source('raw', 'retailer_shipments') }}
)

select
    shipment_id,
    order_id,
    ship_date,
    delivery_date,
    carrier,
    bol_number,
    units_shipped,
    pallets_shipped,
    asn_sent,
    asn_sent_late
from source

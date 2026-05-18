with source as (
    select * from {{ source('raw', 'distributor_shipments') }}
)

select
    shipment_id,
    order_id,
    ship_date,
    delivery_date,
    carrier,
    units_shipped
from source

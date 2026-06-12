with source as (
    select * from {{ source('raw', 'distributor_shipment_lines') }}
)

select
    shipment_id,
    sku,
    units_ordered,
    units_shipped,
    shortfall_reason
from source

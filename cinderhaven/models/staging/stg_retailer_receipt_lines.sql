with source as (
    select * from {{ source('raw', 'retailer_receipt_lines') }}
)

select
    shipment_id,
    sku,
    units_received,
    discrepancy_reason
from source

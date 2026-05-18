with source as (
    select * from {{ source('raw', 'retailer_pack_records') }}
)

select
    pack_record_id,
    order_id,
    shipment_id,
    pack_date,
    units_picked,
    units_packed,
    pack_verification,
    label_scannable,
    evidence_format
from source

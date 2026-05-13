with source as (
    select * from {{ source('raw', 'pack_records') }}
),

staged as (
    select
        pack_record_id,
        order_id,
        shipment_id,
        pack_date::date as pack_date,
        packer_initials,
        units_picked,
        units_packed,
        units_pick_pack_match::boolean as units_pick_pack_match,
        label_type_used,
        label_scannable::boolean as label_scannable,
        pack_verification,
        evidence_format,
        evidence_location,
        evidence_retrieval_minutes
    from source
)

select * from staged

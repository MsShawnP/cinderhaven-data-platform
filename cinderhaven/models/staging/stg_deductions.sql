with source as (
    select * from {{ source('raw', 'deductions') }}
),

staged as (
    select
        deduction_id,
        retailer_id,
        order_id,
        shipment_id,
        deduction_type,
        code_id,
        code_as_remitted,
        remittance_description,
        amount,
        deduction_date::date as deduction_date,
        dispute_deadline::date as dispute_deadline,
        is_vague::boolean as is_vague,
        is_post_audit::boolean as is_post_audit,
        is_double_dip::boolean as is_double_dip,
        remittance_id
    from source
)

select * from staged

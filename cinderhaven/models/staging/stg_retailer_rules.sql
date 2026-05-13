with source as (
    select * from {{ source('raw', 'retailer_rules') }}
),

staged as (
    select
        retailer_id,
        deduction_type,
        dispute_window_days,
        auto_deduct::boolean as auto_deduct,
        evidence_required,
        typical_recovery_rate,
        notes
    from source
)

select * from staged

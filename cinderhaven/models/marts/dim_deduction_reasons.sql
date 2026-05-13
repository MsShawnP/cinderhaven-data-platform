-- dim_deduction_reasons: Deduction reason code dimension from crosswalk.
--
-- Grain: one row per deduction code (code_id).

with crosswalk as (
    select * from {{ ref('int_deduction_code_crosswalk') }}
),

final as (
    select
        code_id,
        retailer_id,
        retailer_name,
        code,
        code_name,
        deduction_type,
        is_published,
        dispute_window_days,
        auto_deduct,
        evidence_required,
        typical_recovery_rate
    from crosswalk
)

select * from final

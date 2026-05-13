with source as (
    select * from {{ source('raw', 'post_audit_claims') }}
),

staged as (
    select
        claim_id,
        deduction_id,
        auditor_name,
        audit_period_start::date as audit_period_start,
        audit_period_end::date as audit_period_end,
        claim_type,
        lookback_months
    from source
)

select * from staged

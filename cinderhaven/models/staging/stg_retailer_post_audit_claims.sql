with source as (
    select * from {{ source('raw', 'retailer_post_audit_claims') }}
)

select
    claim_id,
    deduction_id,
    auditor_name,
    audit_period_start,
    audit_period_end,
    claim_type,
    lookback_months
from source

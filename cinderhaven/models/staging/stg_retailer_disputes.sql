with source as (
    select * from {{ source('raw', 'retailer_disputes') }}
)

select
    dispute_id,
    deduction_id,
    filed_date,
    filing_method,
    evidence_quality,
    outcome,
    recovered_amount,
    closed_date,
    labor_hours
from source

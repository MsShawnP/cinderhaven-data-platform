with retailer_disputes as (
    select * from {{ ref('stg_retailer_disputes') }}
),

distributor_disputes as (
    select * from {{ ref('stg_distributor_disputes') }}
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
from retailer_disputes

union all

select
    dispute_id,
    deduction_id,
    filed_date,
    null as filing_method,
    evidence_quality,
    outcome,
    recovered_amount,
    closed_date,
    labor_hours
from distributor_disputes

with source as (
    select * from {{ source('raw', 'distributor_disputes') }}
)

select
    dispute_id,
    deduction_id,
    filed_date,
    outcome,
    recovered_amount,
    closed_date,
    labor_hours
from source

-- Total deduction amounts must agree between staging and the fact table.
-- A mismatch means the intermediate join (int_retailer_payments) dropped
-- or duplicated deduction records.

with staging_total as (
    select sum(amount) as total_deductions from {{ ref('stg_deductions') }}
),

fct_total as (
    select sum(deduction_amount) as total_deductions from {{ ref('fct_deductions') }}
)

select
    s.total_deductions as staging_deductions,
    f.total_deductions as fct_deductions,
    abs(s.total_deductions - f.total_deductions) as delta
from staging_total s
cross join fct_total f
where abs(s.total_deductions - f.total_deductions) > 0.01

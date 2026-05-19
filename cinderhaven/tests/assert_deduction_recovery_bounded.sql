-- Disputed deductions where recovered amount exceeds the original
-- deduction. A recovery > 100% of the deduction is a data integrity
-- issue — you cannot recover more than was taken.

with all_deductions as (
    select deduction_id, deduction_amount, recovered_amount, deduction_status
    from {{ ref('fct_retailer_deductions') }}
    union all
    select deduction_id, deduction_amount, recovered_amount, deduction_status
    from {{ ref('fct_distributor_deductions') }}
)

select
    deduction_id,
    deduction_amount,
    recovered_amount
from all_deductions
where deduction_status = 'disputed'
  and recovered_amount > deduction_amount

-- Disputed deductions where recovered amount exceeds the original
-- deduction. A recovery > 100% of the deduction is a data integrity
-- issue — you cannot recover more than was taken.

select
    deduction_id,
    deduction_amount,
    dispute_recovered_amount
from {{ ref('fct_deductions') }}
where was_disputed
  and dispute_recovered_amount > deduction_amount

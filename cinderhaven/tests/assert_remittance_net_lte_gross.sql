-- Remittances where net amount exceeds gross. Net should always be
-- gross minus deductions — a net > gross indicates a data error in
-- the payment pipeline.

select
    remittance_id,
    gross_amount,
    net_amount
from {{ ref('fct_payments') }}
where net_amount > gross_amount

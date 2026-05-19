-- Remittances where net amount exceeds gross. Net should always be
-- gross minus deductions — a net > gross indicates a data error in
-- the payment pipeline.

with all_payments as (
    select remittance_id, gross_amount, net_amount
    from {{ ref('fct_retailer_payments') }}
    union all
    select remittance_id, gross_amount, net_amount
    from {{ ref('fct_distributor_payments') }}
)

select
    remittance_id,
    gross_amount,
    net_amount
from all_payments
where net_amount > gross_amount

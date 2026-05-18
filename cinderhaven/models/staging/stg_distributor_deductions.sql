with source as (
    select * from {{ source('raw', 'distributor_deductions') }}
)

select
    deduction_id,
    distributor_id,
    order_id,
    remittance_id,
    deduction_type,
    amount as deduction_amount,
    deduction_date
from source

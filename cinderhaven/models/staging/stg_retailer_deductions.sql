with source as (
    select * from {{ source('raw', 'retailer_deductions') }}
)

select
    deduction_id,
    retailer_id,
    order_id,
    remittance_id,
    deduction_type,
    code_id,
    amount as deduction_amount,
    deduction_date,
    dispute_deadline,
    is_post_audit
from source

with retailer_deductions as (
    select * from {{ ref('stg_retailer_deductions') }}
),

distributor_deductions as (
    select * from {{ ref('stg_distributor_deductions') }}
)

select
    deduction_id,
    retailer_id as partner_id,
    'retailer' as channel_type,
    order_id,
    remittance_id,
    deduction_type,
    code_id,
    deduction_amount,
    deduction_date,
    dispute_deadline,
    is_post_audit
from retailer_deductions

union all

select
    deduction_id,
    distributor_id as partner_id,
    'distributor' as channel_type,
    order_id,
    remittance_id,
    deduction_type,
    null as code_id,
    deduction_amount,
    deduction_date,
    null::date as dispute_deadline,
    null::boolean as is_post_audit
from distributor_deductions

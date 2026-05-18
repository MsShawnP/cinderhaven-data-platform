with payments as (
    select * from {{ ref('int_retailer_payments') }}
),

crosswalk as (
    select * from {{ ref('int_retailer_deduction_crosswalk') }}
)

select
    p.deduction_id,
    p.retailer_id,
    p.order_id,
    p.remittance_id,
    p.deduction_type,
    p.code_id,
    cw.code,
    cw.code_name,
    p.deduction_amount,
    p.deduction_date,
    p.dispute_deadline,
    p.is_post_audit,

    p.deduction_status,
    p.dispute_id,
    p.dispute_filed_date,
    p.dispute_outcome,
    p.recovered_amount,
    p.dispute_closed_date,
    p.dispute_labor_hours,
    p.net_deduction_amount,

    cw.dispute_window_days,
    cw.auto_deduct,
    cw.evidence_required,
    cw.typical_recovery_rate

from payments p
left join crosswalk cw on p.code_id = cw.code_id

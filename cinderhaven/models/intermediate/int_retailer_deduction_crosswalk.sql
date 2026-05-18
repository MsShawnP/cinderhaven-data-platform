with codes as (
    select * from {{ ref('stg_retailer_deduction_codes') }}
),

rules as (
    select * from {{ ref('stg_retailer_rules') }}
)

select
    c.code_id,
    c.retailer_id,
    c.code,
    c.code_name,
    c.deduction_type,
    c.is_published,

    r.dispute_window_days,
    r.auto_deduct,
    r.evidence_required,
    r.typical_recovery_rate

from codes c
left join rules r
    on c.retailer_id = r.retailer_id
    and c.deduction_type = r.deduction_type

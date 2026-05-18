with deductions as (
    select * from {{ ref('stg_retailer_deductions') }}
),

disputes as (
    select * from {{ ref('stg_retailer_disputes') }}
),

remittances as (
    select * from {{ ref('stg_retailer_remittances') }}
)

select
    d.deduction_id,
    d.retailer_id,
    d.order_id,
    d.remittance_id,
    d.deduction_type,
    d.code_id,
    d.deduction_amount,
    d.deduction_date,
    d.dispute_deadline,
    d.is_post_audit,

    r.received_date as remittance_date,
    r.gross_amount as remittance_gross,
    r.net_amount as remittance_net,

    disp.dispute_id,
    disp.filed_date as dispute_filed_date,
    disp.outcome as dispute_outcome,
    disp.recovered_amount,
    disp.closed_date as dispute_closed_date,
    disp.labor_hours as dispute_labor_hours,

    case
        when disp.dispute_id is not null then 'disputed'
        when d.dispute_deadline < current_date then 'expired'
        else 'open'
    end as deduction_status,
    d.deduction_amount - coalesce(disp.recovered_amount, 0) as net_deduction_amount

from deductions d
left join remittances r on d.remittance_id = r.remittance_id
left join disputes disp on d.deduction_id = disp.deduction_id

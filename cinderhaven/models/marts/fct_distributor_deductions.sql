with deductions as (
    select * from {{ ref('stg_distributor_deductions') }}
),

disputes as (
    select * from {{ ref('stg_distributor_disputes') }}
),

remittances as (
    select
        remittance_id,
        received_date
    from {{ ref('stg_distributor_remittances') }}
)

select
    d.deduction_id,
    d.distributor_id,
    d.order_id,
    d.remittance_id,
    d.deduction_type,
    d.deduction_amount,
    d.deduction_date,

    r.received_date as remittance_date,

    disp.dispute_id,
    disp.filed_date as dispute_filed_date,
    disp.outcome as dispute_outcome,
    disp.recovered_amount,
    disp.closed_date as dispute_closed_date,
    disp.labor_hours as dispute_labor_hours,

    case
        when disp.dispute_id is not null then 'disputed'
        else 'open'
    end as deduction_status,
    d.deduction_amount - coalesce(disp.recovered_amount, 0) as net_deduction_amount

from deductions d
left join remittances r on d.remittance_id = r.remittance_id
left join disputes disp on d.deduction_id = disp.deduction_id

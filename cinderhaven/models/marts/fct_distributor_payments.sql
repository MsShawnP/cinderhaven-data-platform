with remittances as (
    select * from {{ ref('stg_distributor_remittances') }}
),

deduction_summary as (
    select
        remittance_id,
        count(*) as deduction_count,
        sum(deduction_amount) as total_deduction_amount
    from {{ ref('stg_distributor_deductions') }}
    where remittance_id is not null
    group by remittance_id
)

select
    r.remittance_id,
    r.distributor_id,
    r.received_date,
    r.gross_amount,
    r.net_amount,
    r.total_deductions,

    coalesce(ds.deduction_count, 0) as deduction_count,

    r.gross_amount - r.net_amount as computed_deduction_gap,
    r.total_deductions - coalesce(ds.total_deduction_amount, 0) as deduction_reconciliation_diff

from remittances r
left join deduction_summary ds on r.remittance_id = ds.remittance_id

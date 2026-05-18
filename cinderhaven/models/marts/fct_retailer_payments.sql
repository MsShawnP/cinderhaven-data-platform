with remittances as (
    select * from {{ ref('stg_retailer_remittances') }}
),

deduction_summary as (
    select
        remittance_id,
        count(*) as deduction_count,
        sum(deduction_amount) as total_deduction_amount,
        count(case when deduction_status = 'disputed' then 1 end) as disputed_count,
        sum(coalesce(recovered_amount, 0)) as total_recovered
    from {{ ref('int_retailer_payments') }}
    where remittance_id is not null
    group by remittance_id
)

select
    r.remittance_id,
    r.retailer_id,
    r.received_date,
    r.remittance_format,
    r.gross_amount,
    r.net_amount,
    r.total_deductions,
    r.clarity,

    coalesce(ds.deduction_count, 0) as deduction_count,
    coalesce(ds.disputed_count, 0) as disputed_count,
    coalesce(ds.total_recovered, 0) as total_recovered,

    r.gross_amount - r.net_amount as computed_deduction_gap,
    r.total_deductions - coalesce(ds.total_deduction_amount, 0) as deduction_reconciliation_diff

from remittances r
left join deduction_summary ds on r.remittance_id = ds.remittance_id

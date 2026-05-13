-- fct_payments: Remittance (payment) fact with deduction summaries.
--
-- Grain: one row per remittance.

with remittances as (
    select * from {{ ref('stg_remittances') }}
),

retailers as (
    select * from {{ ref('stg_retailers') }}
),

deduction_summary as (
    select
        remittance_id,
        count(*) as deduction_count,
        sum(deduction_amount) as total_deduction_amount,
        count(*) filter (where was_disputed) as disputed_count,
        sum(dispute_recovered_amount) filter (where was_disputed) as total_recovered
    from {{ ref('int_retailer_payments') }}
    where remittance_id is not null
    group by remittance_id
),

final as (
    select
        remittances.remittance_id,
        remittances.retailer_id,
        retailers.name as retailer_name,
        remittances.received_date,
        remittances.format,
        remittances.gross_amount,
        remittances.net_amount,
        remittances.total_deductions,
        remittances.clarity,
        remittances.gross_amount - remittances.net_amount as implied_deduction_total,
        coalesce(ds.deduction_count, 0) as linked_deduction_count,
        coalesce(ds.total_deduction_amount, 0) as linked_deduction_amount,
        coalesce(ds.disputed_count, 0) as disputed_deduction_count,
        coalesce(ds.total_recovered, 0) as total_recovered_amount
    from remittances
    left join retailers on remittances.retailer_id = retailers.retailer_id
    left join deduction_summary ds on remittances.remittance_id = ds.remittance_id
)

select * from final

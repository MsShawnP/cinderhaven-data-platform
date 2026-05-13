-- Retailer-payment join: connects remittances to their deductions
-- and enriches with retailer metadata.
--
-- Grain: one row per deduction (deduction_id).
-- Joins deductions → remittances → retailers to build a payment-level
-- view of what was deducted, when, and from which remittance.

with deductions as (
    select * from {{ ref('stg_deductions') }}
),

remittances as (
    select * from {{ ref('stg_remittances') }}
),

retailers as (
    select * from {{ ref('stg_retailers') }}
),

disputes as (
    select * from {{ ref('stg_disputes') }}
),

payment_deductions as (
    select
        deductions.deduction_id,
        deductions.retailer_id,
        retailers.name as retailer_name,
        retailers.channel_type,
        deductions.order_id,
        deductions.shipment_id,
        deductions.deduction_type,
        deductions.code_id,
        deductions.code_as_remitted,
        deductions.remittance_description,
        deductions.amount as deduction_amount,
        deductions.deduction_date,
        deductions.dispute_deadline,
        deductions.is_vague,
        deductions.is_post_audit,
        deductions.is_double_dip,
        deductions.remittance_id,
        remittances.received_date as remittance_received_date,
        remittances.gross_amount as remittance_gross,
        remittances.net_amount as remittance_net,
        remittances.clarity as remittance_clarity,
        disputes.dispute_id,
        disputes.filed_date as dispute_filed_date,
        disputes.outcome as dispute_outcome,
        disputes.recovered_amount as dispute_recovered_amount,
        disputes.labor_hours as dispute_labor_hours,
        case
            when disputes.dispute_id is not null then true
            else false
        end as was_disputed
    from deductions
    left join remittances
        on deductions.remittance_id = remittances.remittance_id
    left join retailers
        on deductions.retailer_id = retailers.retailer_id
    left join disputes
        on deductions.deduction_id = disputes.deduction_id
)

select * from payment_deductions

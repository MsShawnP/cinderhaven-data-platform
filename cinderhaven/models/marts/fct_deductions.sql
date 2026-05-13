-- fct_deductions: Deduction fact with dispute outcomes and financial impact.
--
-- Grain: one row per deduction.
-- Built from the retailer_payments intermediate model.

with payments as (
    select * from {{ ref('int_retailer_payments') }}
),

final as (
    select
        deduction_id,
        retailer_id,
        retailer_name,
        channel_type,
        order_id,
        shipment_id,
        deduction_type,
        code_id,
        code_as_remitted,
        remittance_description,
        deduction_amount,
        deduction_date,
        dispute_deadline,
        is_vague,
        is_post_audit,
        is_double_dip,
        remittance_id,
        remittance_received_date,
        remittance_gross,
        remittance_net,
        remittance_clarity,
        was_disputed,
        dispute_id,
        dispute_filed_date,
        dispute_outcome,
        dispute_recovered_amount,
        dispute_labor_hours,
        -- Calculated fields
        case
            when was_disputed and dispute_recovered_amount > 0
            then dispute_recovered_amount
            else 0
        end as net_recovery,
        deduction_amount - coalesce(dispute_recovered_amount, 0) as net_loss,
        case
            when dispute_deadline is not null
            then dispute_deadline - deduction_date
        end as days_to_dispute_deadline
    from payments
)

select * from final

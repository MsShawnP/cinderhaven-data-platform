-- Trading partners whose itemized deduction ledger exceeds the dollars
-- actually withheld on remittances. A subset cannot exceed its superset:
-- every itemized deduction is money a partner held back, so the itemized
-- total must sit at or below the remittance withholding total.
--
-- Exists because the plausibility audit found the canonical file citing
-- a subset figure larger than its superset ($458K "data-attributable"
-- chargebacks vs $310K all-reason chargebacks). That contradiction lived
-- in prose, but the same class of error in data — itemizing more than
-- was withheld — would mean the remittance and deduction generators have
-- disconnected. After Phase 3, remittance withholding is constructed as
-- itemized deductions + chargebacks + allowances + a small residual, so
-- the inequality must hold by construction.

with all_partners as (
    select
        retailer_id as partner_id,
        retailer_name as partner_name,
        total_deduction_amount,
        total_remittance_deductions
    from {{ ref('mart_retailer_reconciliation') }}
    union all
    select
        distributor_id as partner_id,
        distributor_name as partner_name,
        total_deduction_amount,
        total_remittance_deductions
    from {{ ref('mart_distributor_reconciliation') }}
)

select
    partner_id,
    partner_name,
    total_deduction_amount,
    total_remittance_deductions
from all_partners
where coalesce(total_deduction_amount, 0) > coalesce(total_remittance_deductions, 0)

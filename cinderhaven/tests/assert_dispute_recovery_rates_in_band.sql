-- Per-tier effective dispute recovery outside ±2pts of the §2.4 curve
-- (combined wholesale: retailer + distributor disputes in one pool,
-- the Group D verification grain).
--
-- Effective recovery rate = recovered dollars / disputed deduction
-- dollars for the tier. Pending disputes recover nothing yet but their
-- dollars stay in the denominator — that is the design's definition
-- ("recovered dollars / disputed dollars for that tier"), and it is
-- what makes Strong land at 65% rather than the 72% a closed-only
-- denominator would produce.
--
-- Curve (CAUSAL_FULFILLMENT_DESIGN.md §2.4, frozen in seed_config
-- EVIDENCE_OUTCOME_WEIGHTS / PARTIAL_RECOVERY_RANGE):
--   strong 65% / moderate 27% / weak 13%, each ±2.0pts.
--
-- The data is deterministic (EVIDENCE_SEED streams), so this is a
-- stable regression gate, not a flaky statistical assertion: it fails
-- only when generation or the curve constants change.

with curve as (
    select * from (values
        ('strong', 0.65), ('moderate', 0.27), ('weak', 0.13)
    ) as t (tier, expected_rate)
),

disputed as (
    select disp.evidence_quality, disp.recovered_amount,
           ded.deduction_amount
    from {{ ref('stg_retailer_disputes') }} disp
    join {{ ref('stg_retailer_deductions') }} ded
        on ded.deduction_id = disp.deduction_id

    union all

    select disp.evidence_quality, disp.recovered_amount,
           ded.deduction_amount
    from {{ ref('stg_distributor_disputes') }} disp
    join {{ ref('stg_distributor_deductions') }} ded
        on ded.deduction_id = disp.deduction_id
),

by_tier as (
    select
        evidence_quality as tier,
        sum(coalesce(recovered_amount, 0)) as recovered_dollars,
        sum(deduction_amount) as disputed_dollars,
        sum(coalesce(recovered_amount, 0)) / nullif(sum(deduction_amount), 0)
            as effective_rate
    from disputed
    group by evidence_quality
)

select
    b.tier,
    b.recovered_dollars,
    b.disputed_dollars,
    round(b.effective_rate, 4) as effective_rate,
    c.expected_rate
from by_tier b
join curve c on c.tier = b.tier
where abs(b.effective_rate - c.expected_rate) > 0.02

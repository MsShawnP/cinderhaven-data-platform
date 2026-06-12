-- Fails when wholesale trade leakage (itemized deductions plus
-- compliance chargebacks) falls outside 1% to 9% of invoiced wholesale
-- revenue. CPG norm for the deduction problem runs 3-10% of revenue;
-- Cinderhaven is built to sit at the bad end of normal, not outside it.
--
-- Currently ~3.4% ($2.59M leakage on $76.1M invoiced, 36 months). A
-- regen that 10x-es deduction generation, zeroes it, or breaks the
-- order-value linkage moves this ratio out of band. Structural trade
-- (rate x scan revenue) is intentionally excluded: it is validated
-- against seed_config rates by check_canonical.py, while this test
-- guards the generated dollar flows.
--
-- DTC is excluded: deductions and chargebacks are wholesale phenomena.

with leakage as (
    select
        (select sum(deduction_amount) from {{ ref('fct_retailer_deductions') }})
      + (select sum(deduction_amount) from {{ ref('fct_distributor_deductions') }})
      + (select sum(chargeback_amount) from {{ ref('stg_retailer_chargebacks') }})
      + (select sum(chargeback_amount) from {{ ref('stg_distributor_chargebacks') }})
        as total_leakage,
        (select sum(total_value) from {{ ref('stg_retailer_orders') }})
      + (select sum(total_value) from {{ ref('stg_distributor_orders') }})
        as invoiced_revenue
)

select
    total_leakage,
    invoiced_revenue,
    round(total_leakage / nullif(invoiced_revenue, 0), 4) as leakage_rate
from leakage
where invoiced_revenue > 0
  and (
      total_leakage / invoiced_revenue < 0.01
      or total_leakage / invoiced_revenue > 0.09
  )

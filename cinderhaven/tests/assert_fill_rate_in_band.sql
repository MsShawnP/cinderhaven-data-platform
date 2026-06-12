-- Trading partners whose unit fill rate (units shipped / units ordered)
-- falls outside the plausible band, or who ship more than was ordered.
--
-- Band: 85% to 100% per retailer / distributor. The platform currently
-- ships 100.00% of ordered units; the causal fulfillment model (Phase 3
-- of the causal-fulfillment arc) targets 90-94% per retailer. Both
-- worlds pass. What cannot pass: a regen that reintroduces the 69% fill
-- universe, zeroes shipments, or ships more units than were ordered.
-- Tighten the lower bound toward 88% at the Phase 4 relock.

with retailer_fill as (
    select
        o.retailer_id as partner_id,
        'retailer' as channel,
        sum(o.total_units) as units_ordered,
        sum(s.units_shipped) as units_shipped
    from {{ ref('stg_retailer_orders') }} o
    join {{ ref('stg_retailer_shipments') }} s on o.order_id = s.order_id
    group by o.retailer_id
),

distributor_fill as (
    select
        o.distributor_id as partner_id,
        'distributor' as channel,
        sum(o.total_units) as units_ordered,
        sum(s.units_shipped) as units_shipped
    from {{ ref('stg_distributor_orders') }} o
    join {{ ref('stg_distributor_shipments') }} s on o.order_id = s.order_id
    group by o.distributor_id
),

all_fill as (
    select * from retailer_fill
    union all
    select * from distributor_fill
)

select
    partner_id,
    channel,
    units_ordered,
    units_shipped,
    round(units_shipped::numeric / nullif(units_ordered, 0), 4) as fill_rate
from all_fill
where units_ordered > 0
  and (
      units_shipped::numeric / units_ordered < 0.85
      or units_shipped::numeric / units_ordered > 1.0
  )

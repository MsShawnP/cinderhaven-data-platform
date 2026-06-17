-- SPPD = Total Units ÷ Carrying Stores ÷ Days in Period
-- Single source of truth for Scans Per Point of Distribution per Day.
-- Downstream tools (spinrate, velocity decision tool) query this
-- instead of re-aggregating fct_scan_data at runtime.

with quarterly_scans as (
    select
        sku,
        extract(year from week_ending)::int as year,
        extract(quarter from week_ending)::int as quarter,
        sum(units_sold) as total_units,
        count(distinct store_id) as carrying_stores
    from {{ ref('fct_scan_data') }}
    group by 1, 2, 3
),

quarter_days as (
    select
        extract(year from week_ending)::int as year,
        extract(quarter from week_ending)::int as quarter,
        (max(week_ending) - min(week_ending) + 7)::int as days_in_period
    from {{ ref('fct_scan_data') }}
    group by 1, 2
)

select
    qs.sku,
    qs.year,
    qs.quarter,
    qs.total_units,
    qs.carrying_stores,
    qd.days_in_period,
    round(
        qs.total_units::numeric
        / nullif(qs.carrying_stores, 0)
        / nullif(qd.days_in_period, 0),
        4
    ) as sppd
from quarterly_scans qs
inner join quarter_days qd
    on qs.year = qd.year
    and qs.quarter = qd.quarter

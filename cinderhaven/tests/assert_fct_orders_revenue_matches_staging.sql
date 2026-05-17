-- Total revenue in fct_orders must equal the sum of staging sources.
-- B2B revenue comes from stg_order_lines, DTC from stg_shopify_order_lines.
-- A delta > $0.01 indicates a transformation dropped or duplicated rows.

with b2b_staging as (
    select sum(line_total) as revenue from {{ ref('stg_order_lines') }}
),

dtc_staging as (
    select sum(line_total) as revenue from {{ ref('stg_shopify_order_lines') }}
),

staging_total as (
    select b2b.revenue + dtc.revenue as total_revenue
    from b2b_staging b2b
    cross join dtc_staging dtc
),

fct_total as (
    select sum(line_total) as total_revenue from {{ ref('fct_orders') }}
)

select
    s.total_revenue as staging_revenue,
    f.total_revenue as fct_revenue,
    abs(s.total_revenue - f.total_revenue) as delta
from staging_total s
cross join fct_total f
where abs(s.total_revenue - f.total_revenue) > 0.01

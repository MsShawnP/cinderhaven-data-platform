-- mart_channel_contribution gross revenue must agree with fct_orders
-- line totals, grouped by channel. A mismatch means the mart
-- aggregation diverged from the fact table.

with fct_by_channel as (
    select
        coalesce(retailer_id, 'DTC') as channel_id,
        sum(line_total) as revenue
    from {{ ref('fct_orders') }}
    group by 1
),

mart_by_channel as (
    select
        channel_id,
        gross_revenue as revenue
    from {{ ref('mart_channel_contribution') }}
)

select
    f.channel_id,
    f.revenue as fct_revenue,
    m.revenue as mart_revenue,
    abs(f.revenue - m.revenue) as delta
from fct_by_channel f
inner join mart_by_channel m on f.channel_id = m.channel_id
where abs(f.revenue - m.revenue) > 0.01

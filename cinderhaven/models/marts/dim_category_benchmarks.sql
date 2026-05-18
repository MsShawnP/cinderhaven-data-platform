with scan_summary as (
    select
        p.product_line,
        count(distinct sd.sku) as sku_count,
        count(distinct sd.store_id) as store_count,
        avg(sd.units_sold)::numeric(10,2) as avg_weekly_units_per_store,
        sum(sd.units_sold) as total_units,
        sum(sd.dollars_sold) as total_dollars
    from {{ ref('stg_scan_data') }} sd
    inner join {{ ref('stg_product_master') }} p on sd.sku = p.sku
    group by p.product_line
),

cost_summary as (
    select
        p.product_line,
        avg(c.cogs_per_unit)::numeric(8,2) as avg_cogs,
        avg(p.msrp)::numeric(8,2) as avg_msrp,
        avg(p.msrp - c.cogs_per_unit)::numeric(8,2) as avg_margin_per_unit,
        round(avg(
            case when p.msrp > 0
            then (p.msrp - c.cogs_per_unit) / p.msrp
            else 0 end
        )::numeric, 4) as avg_margin_pct
    from {{ ref('stg_product_master') }} p
    inner join {{ ref('stg_sku_costs') }} c on p.sku = c.sku
    group by p.product_line
)

select
    ss.product_line,
    ss.sku_count,
    ss.store_count,
    ss.avg_weekly_units_per_store,
    ss.total_units,
    ss.total_dollars,
    cs.avg_cogs,
    cs.avg_msrp,
    cs.avg_margin_per_unit,
    cs.avg_margin_pct

from scan_summary ss
inner join cost_summary cs on ss.product_line = cs.product_line

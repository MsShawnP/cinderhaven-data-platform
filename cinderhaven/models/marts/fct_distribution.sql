with distribution as (
    select * from {{ ref('stg_distribution_log') }}
),

stores as (
    select store_id, retailer_id, chain_name, region, state, volume_tier
    from {{ ref('stg_stores') }}
),

velocity as (
    select
        sku,
        store_id,
        count(*) as weeks_with_sales,
        sum(units_sold) as total_units,
        sum(dollars_sold) as total_dollars,
        avg(units_sold)::numeric(10,2) as avg_weekly_units,
        min(week_ending) as first_scan_week,
        max(week_ending) as last_scan_week
    from {{ ref('stg_scan_data') }}
    group by sku, store_id
)

select
    d.sku,
    d.store_id,
    s.retailer_id,
    s.chain_name,
    s.region,
    s.state,
    s.volume_tier,
    d.authorized_date,
    d.deauthorized_date,
    d.deauthorized_date is null as is_active,

    coalesce(v.weeks_with_sales, 0) as weeks_with_sales,
    coalesce(v.total_units, 0) as total_units,
    coalesce(v.total_dollars, 0) as total_dollars,
    v.avg_weekly_units,
    v.first_scan_week,
    v.last_scan_week

from distribution d
inner join stores s on d.store_id = s.store_id
left join velocity v on d.sku = v.sku and d.store_id = v.store_id

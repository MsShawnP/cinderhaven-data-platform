with products as (
    select * from {{ ref('stg_product_master') }}
),

costs as (
    select * from {{ ref('stg_sku_costs') }}
),

retailer_distribution as (
    select sku, count(distinct retailer_id) as retailer_count
    from {{ ref('int_product_retailers') }}
    group by sku
),

distributor_distribution as (
    select sku, count(distinct distributor_id) as distributor_count
    from {{ ref('int_product_distributors') }}
    group by sku
),

store_distribution as (
    select sku, count(distinct store_id) as authorized_store_count
    from {{ ref('stg_distribution_log') }}
    where deauthorized_date is null
    group by sku
)

select
    p.sku,
    p.product_name,
    p.product_line,
    p.subcategory,
    p.case_pack_qty,
    p.msrp,
    c.cogs_per_unit,
    c.landed_cost_per_unit,
    c.wholesale_price,
    c.wholesale_dtc,
    p.msrp - c.cogs_per_unit as dtc_margin_per_unit,
    case
        when p.msrp > 0
        then round((p.msrp - c.cogs_per_unit) / p.msrp, 4)
        else 0
    end as dtc_margin_pct,
    coalesce(rd.retailer_count, 0) as retailer_count,
    coalesce(dd.distributor_count, 0) as distributor_count,
    coalesce(sd.authorized_store_count, 0) as authorized_store_count

from products p
inner join costs c on p.sku = c.sku
left join retailer_distribution rd on p.sku = rd.sku
left join distributor_distribution dd on p.sku = dd.sku
left join store_distribution sd on p.sku = sd.sku

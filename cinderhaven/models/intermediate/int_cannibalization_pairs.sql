-- Grain: one row per SKU
-- Proxy cannibalization signal: does this SKU's velocity drop when
-- sibling SKUs from the same product_line are also distributed in a store?
--
-- Methodology (proxy — cross-sectional):
--   - "Solo store": store carries this SKU and no other SKU from its product_line
--   - "Shared store": store carries this SKU + at least one sibling from same product_line
--   - velocity_delta_pct = (shared_uspw - solo_uspw) / solo_uspw
--   - Negative value = velocity is lower when siblings are present (cannibalization signal)
--
-- Limitation: this is a cross-sectional comparison, not a causal estimate.
-- Store composition differences (chain, region, volume tier) may confound results.
-- A rigorous DiD was not feasible because all SKU authorizations occurred within
-- the same 6-month window, leaving insufficient pre-variant temporal depth.

with distribution as (
    select sku, store_id
    from {{ ref('stg_distribution_log') }}
    where deauthorized_date is null
),

product_master as (
    select sku, product_line
    from {{ ref('stg_product_master') }}
),

-- Count siblings per store per focal SKU
store_sibling_counts as (
    select
        d1.sku                              as focal_sku,
        d1.store_id,
        count(sib.sku)                      as sibling_count_in_store
    from distribution d1
    inner join product_master pm1 on d1.sku = pm1.sku
    -- Find other SKUs in the same product_line distributed to the same store
    left join (
        select d2.sku, d2.store_id, pm2.product_line
        from distribution d2
        inner join product_master pm2 on d2.sku = pm2.sku
    ) sib
        on d1.store_id = sib.store_id
        and pm1.product_line = sib.product_line
        and d1.sku != sib.sku
    group by d1.sku, d1.store_id
),

-- Categorise each store as solo or shared for this SKU
store_categories as (
    select
        focal_sku,
        store_id,
        case when sibling_count_in_store = 0 then 'solo' else 'shared' end as store_type
    from store_sibling_counts
),

-- Weekly velocity per SKU per store
scan as (
    select
        sku,
        store_id,
        avg(units_sold) as avg_weekly_units
    from {{ ref('stg_scan_data') }}
    group by sku, store_id
),

-- Velocity by store category
velocity_by_type as (
    select
        sc.focal_sku                        as sku,
        sc.store_type,
        count(distinct sc.store_id)         as store_count,
        avg(s.avg_weekly_units)             as avg_uspw
    from store_categories sc
    left join scan s
        on sc.focal_sku = s.sku
        and sc.store_id = s.store_id
    group by sc.focal_sku, sc.store_type
),

solo as (
    select sku, store_count as solo_stores, avg_uspw as solo_uspw
    from velocity_by_type where store_type = 'solo'
),

shared as (
    select sku, store_count as shared_stores, avg_uspw as shared_uspw
    from velocity_by_type where store_type = 'shared'
),

sibling_totals as (
    select
        focal_sku as sku,
        count(distinct store_id) as total_distribution_stores,
        max(sibling_count_in_store) as max_siblings_in_store
    from store_sibling_counts
    group by focal_sku
)

select
    pm.sku,
    pm.product_line,

    coalesce(st.total_distribution_stores, 0)   as total_distribution_stores,
    coalesce(so.solo_stores, 0)                  as solo_stores,
    coalesce(sh.shared_stores, 0)                as shared_stores,

    so.solo_uspw,
    sh.shared_uspw,

    -- Core cannibalization signal: negative = lower velocity when siblings present
    case
        when so.solo_uspw is not null and so.solo_uspw > 0
        then round(
            ((coalesce(sh.shared_uspw, so.solo_uspw) - so.solo_uspw) / so.solo_uspw)::numeric,
            4
        )
        else null
    end                                          as velocity_delta_pct,

    -- Confidence weight: models with few solo stores are less reliable
    coalesce(so.solo_stores, 0)                  as solo_store_count_for_confidence,

    'proxy: cross-sectional velocity comparison' as methodology_note

from product_master pm
left join sibling_totals st on pm.sku = st.sku
left join solo so            on pm.sku = so.sku
left join shared sh          on pm.sku = sh.sku

-- fct_scan_data: Point-of-sale scan data by SKU, store, and week.
--
-- Grain: one row per SKU × store × week.

with scans as (
    select * from {{ ref('stg_scan_data') }}
),

final as (
    select
        scans.sku,
        scans.store_id,
        scans.week_ending,
        scans.units_sold,
        scans.dollars_sold,
        case
            when scans.units_sold > 0
            then scans.dollars_sold / scans.units_sold
        end as avg_unit_price
    from scans
)

select * from final

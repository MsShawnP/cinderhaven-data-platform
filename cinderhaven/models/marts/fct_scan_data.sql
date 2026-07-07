with scans as (
    select * from {{ ref('stg_scan_data') }}
)

select
    sku,
    store_id,
    week_ending,
    units_sold,
    dollars_sold

from scans

-- Composite uniqueness: one row per sku × year × quarter.
select
    sku, year, quarter,
    count(*) as row_count
from {{ ref('mart_quarterly_sppd') }}
group by 1, 2, 3
having count(*) > 1

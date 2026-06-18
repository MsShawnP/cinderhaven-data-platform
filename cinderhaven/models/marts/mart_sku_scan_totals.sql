-- Full-history scan totals at SKU grain (~50 rows).
-- Eliminates the runtime GROUP BY over 465K fct_scan_data rows
-- that was the last full-table-scan bottleneck in spinrate.

select
    sku,
    sum(units_sold) as total_units,
    sum(dollars_sold) as total_dollars,
    count(distinct store_id) as door_count
from {{ ref('fct_scan_data') }}
group by sku

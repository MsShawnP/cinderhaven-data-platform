with source as (
    select * from {{ source('raw', 'distributor_order_lines') }}
)

select
    order_id,
    sku,
    units_ordered,
    unit_price,
    line_total
from source

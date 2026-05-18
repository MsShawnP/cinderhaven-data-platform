with source as (
    select * from {{ source('raw', 'retailer_orders') }}
)

select
    order_id,
    retailer_id,
    po_number,
    po_date,
    requested_ship_date,
    total_units,
    total_value
from source

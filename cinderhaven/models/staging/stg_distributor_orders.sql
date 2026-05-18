with source as (
    select * from {{ source('raw', 'distributor_orders') }}
)

select
    order_id,
    distributor_id,
    po_number,
    po_date,
    total_units,
    total_value
from source

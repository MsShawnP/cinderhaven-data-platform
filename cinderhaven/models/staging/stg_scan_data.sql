with source as (
    select * from {{ source('raw', 'scan_data') }}
)

select
    sku,
    store_id,
    week_ending,
    units_sold,
    dollars_sold
from source

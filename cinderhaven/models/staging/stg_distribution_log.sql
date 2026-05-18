with source as (
    select * from {{ source('raw', 'distribution_log') }}
)

select
    sku,
    store_id,
    authorized_date,
    deauthorized_date
from source

with source as (
    select * from {{ source('raw', 'sku_distributors') }}
)

select
    sku,
    distributor_id
from source

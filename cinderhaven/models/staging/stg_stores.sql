with source as (
    select * from {{ source('raw', 'stores') }}
)

select
    store_id,
    retailer_id,
    chain_name,
    region,
    state,
    volume_tier
from source

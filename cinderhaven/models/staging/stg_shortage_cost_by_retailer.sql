with source as (
    select * from {{ source('raw', 'shortage_cost_by_retailer') }}
)

select
    dimension,
    retailer,
    cost,
    loaded_at
from source

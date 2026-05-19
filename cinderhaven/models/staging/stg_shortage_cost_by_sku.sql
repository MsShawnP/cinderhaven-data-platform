with source as (
    select * from {{ source('raw', 'shortage_cost_by_sku') }}
)

select
    dimension,
    sku,
    cost,
    loaded_at
from source

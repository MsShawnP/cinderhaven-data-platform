with source as (
    select * from {{ source('raw', 'shortage_cost_by_month') }}
)

select
    dimension,
    month,
    cost,
    loaded_at
from source

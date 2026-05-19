with source as (
    select * from {{ source('raw', 'shortage_cost_parameters') }}
)

select
    name,
    value,
    unit,
    basis,
    level,
    description,
    source,
    loaded_at
from source

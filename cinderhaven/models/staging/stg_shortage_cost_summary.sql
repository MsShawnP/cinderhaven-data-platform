with source as (
    select * from {{ source('raw', 'shortage_cost_summary') }}
)

select
    dimension,
    total_cost,
    pct_of_shipped_revenue,
    description,
    loaded_at
from source

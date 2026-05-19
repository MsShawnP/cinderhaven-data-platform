with source as (
    select * from {{ source('raw', 'shortage_buffer_details') }}
)

select
    target_fill_rate,
    dimension,
    original_cost,
    simulated_cost,
    recovery_amount,
    recovery_pct,
    loaded_at
from source

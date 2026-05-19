with source as (
    select * from {{ source('raw', 'shortage_buffer_scenarios') }}
)

select
    target_fill_rate,
    actual_fill_rate_achieved,
    total_cost,
    total_recovery,
    recovery_pct,
    loaded_at
from source

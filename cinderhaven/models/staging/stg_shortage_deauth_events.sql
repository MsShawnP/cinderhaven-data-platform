with source as (
    select * from {{ source('raw', 'shortage_deauth_events') }}
)

select
    sku,
    retailer,
    trigger_type,
    velocity_without_shorts,
    velocity_with_shorts,
    threshold,
    fill_rate,
    consecutive_months_below_threshold,
    annualized_revenue_lost,
    loaded_at
from source

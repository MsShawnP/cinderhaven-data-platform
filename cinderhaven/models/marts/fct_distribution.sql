-- fct_distribution: SKU-store authorization and deauthorization history.
--
-- Grain: one row per SKU × store authorization event.

with dist as (
    select * from {{ ref('stg_distribution_log') }}
),

final as (
    select
        dist.sku,
        dist.store_id,
        dist.authorized_date,
        dist.deauthorized_date,
        dist.deauthorized_date is null as is_currently_authorized
    from dist
)

select * from final

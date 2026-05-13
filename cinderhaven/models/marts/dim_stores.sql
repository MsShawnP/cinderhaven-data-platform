-- dim_stores: Store dimension with retailer association and geography.
--
-- Grain: one row per store.

with stores as (
    select * from {{ ref('stg_stores') }}
),

retailers as (
    select * from {{ ref('stg_retailers') }}
),

final as (
    select
        stores.store_id,
        retailers.retailer_id,
        stores.retailer as retailer_name,
        stores.chain_name,
        stores.region,
        stores.state,
        stores.volume_tier,
        stores.is_aggregated_channel
    from stores
    left join retailers
        on stores.retailer = retailers.name
)

select * from final

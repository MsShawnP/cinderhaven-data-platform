with source as (
    select * from {{ source('raw', 'stores') }}
),

staged as (
    select
        store_id,
        retailer,
        chain_name,
        region,
        state,
        volume_tier,
        is_aggregated_channel::boolean as is_aggregated_channel
    from source
)

select * from staged

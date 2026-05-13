with source as (
    select * from {{ source('raw', 'retailers') }}
),

staged as (
    select
        retailer_id,
        name,
        channel_type,
        dispute_portal_name,
        dispute_portal_url,
        dispute_method,
        notes
    from source
)

select * from staged

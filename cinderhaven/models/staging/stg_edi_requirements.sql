with source as (
    select * from {{ source('raw', 'edi_requirements') }}
),

staged as (
    select
        requirement_id,
        retailer_id,
        category,
        requirement,
        penalty_if_violated,
        is_verified::boolean as is_verified,
        source_url
    from source
)

select * from staged

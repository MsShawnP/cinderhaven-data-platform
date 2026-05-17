with source as (
    select * from {{ source('raw', 'retailer_requirements') }}
),

staged as (
    select
        retailer,
        field,
        required::boolean as is_required
    from source
)

select * from staged

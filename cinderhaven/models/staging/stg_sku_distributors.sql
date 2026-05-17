with source as (
    select * from {{ source('raw', 'sku_distributors') }}
),

staged as (
    select
        sku,
        distributor_id
    from source
)

select * from staged

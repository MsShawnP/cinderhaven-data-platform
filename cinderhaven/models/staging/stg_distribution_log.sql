with source as (
    select * from {{ source('raw', 'distribution_log') }}
),

staged as (
    select
        sku,
        store_id,
        authorized_date::date as authorized_date,
        deauthorized_date::date as deauthorized_date
    from source
)

select * from staged

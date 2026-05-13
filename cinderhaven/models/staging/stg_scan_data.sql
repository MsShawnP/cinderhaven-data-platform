with source as (
    select * from {{ source('raw', 'scan_data') }}
),

staged as (
    select
        sku,
        store_id,
        week_ending::date as week_ending,
        units_sold,
        dollars_sold
    from source
)

select * from staged

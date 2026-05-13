with source as (
    select * from {{ source('raw', 'chargebacks') }}
),

staged as (
    select
        month,
        retailer,
        reason,
        amount,
        sku
    from source
)

select * from staged

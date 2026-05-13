with source as (
    select * from {{ source('raw', 'remittances') }}
),

staged as (
    select
        remittance_id,
        retailer_id,
        received_date::date as received_date,
        format,
        gross_amount,
        net_amount,
        total_deductions,
        clarity
    from source
)

select * from staged

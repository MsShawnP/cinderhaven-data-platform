with source as (
    select * from {{ source('raw', 'deduction_codes') }}
),

staged as (
    select
        code_id,
        retailer_id,
        code,
        name,
        deduction_type,
        is_published::boolean as is_published
    from source
)

select * from staged

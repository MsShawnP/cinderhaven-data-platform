with source as (
    select * from {{ source('raw', 'distributors') }}
),

staged as (
    select
        distributor_id,
        name,
        type,
        coverage,
        margin_pct,
        payment_terms_days,
        headquarters,
        notes
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'dispute_evidence') }}
),

staged as (
    select
        evidence_id,
        dispute_id,
        evidence_type,
        was_submitted::boolean as was_submitted,
        was_required::boolean as was_required,
        format,
        notes
    from source
)

select * from staged

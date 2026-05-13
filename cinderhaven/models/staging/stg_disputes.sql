with source as (
    select * from {{ source('raw', 'disputes') }}
),

staged as (
    select
        dispute_id,
        deduction_id,
        filed_date::date as filed_date,
        filing_method,
        evidence_quality,
        submitted_evidence_count,
        was_within_deadline::boolean as was_within_deadline,
        outcome,
        recovered_amount,
        closed_date::date as closed_date,
        labor_hours
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'retailer_dispute_evidence') }}
)

select
    evidence_id,
    dispute_id,
    evidence_type,
    was_submitted,
    was_required,
    format as evidence_format,
    notes
from source

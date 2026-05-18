with source as (
    select * from {{ source('raw', 'retailer_edi_requirements') }}
)

select
    retailer_id,
    category,
    requirement,
    penalty_if_violated,
    is_verified,
    source_url
from source

with source as (
    select * from {{ source('raw', 'retailer_requirements') }}
)

select
    retailer_id,
    field,
    required,
    notes
from source

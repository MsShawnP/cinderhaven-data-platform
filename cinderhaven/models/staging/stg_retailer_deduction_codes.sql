with source as (
    select * from {{ source('raw', 'retailer_deduction_codes') }}
)

select
    code_id,
    retailer_id,
    code,
    name as code_name,
    deduction_type,
    is_published
from source

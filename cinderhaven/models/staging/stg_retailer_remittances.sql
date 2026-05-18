with source as (
    select * from {{ source('raw', 'retailer_remittances') }}
)

select
    remittance_id,
    retailer_id,
    received_date,
    format as remittance_format,
    gross_amount,
    net_amount,
    total_deductions,
    clarity
from source

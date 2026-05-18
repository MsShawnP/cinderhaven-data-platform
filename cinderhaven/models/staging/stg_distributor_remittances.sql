with source as (
    select * from {{ source('raw', 'distributor_remittances') }}
)

select
    remittance_id,
    distributor_id,
    received_date,
    gross_amount,
    net_amount,
    total_deductions
from source

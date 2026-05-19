with retailer_remittances as (
    select * from {{ ref('stg_retailer_remittances') }}
),

distributor_remittances as (
    select * from {{ ref('stg_distributor_remittances') }}
)

select
    remittance_id,
    retailer_id as partner_id,
    'retailer' as channel_type,
    received_date,
    remittance_format,
    gross_amount,
    net_amount,
    total_deductions,
    clarity
from retailer_remittances

union all

select
    remittance_id,
    distributor_id as partner_id,
    'distributor' as channel_type,
    received_date,
    null as remittance_format,
    gross_amount,
    net_amount,
    total_deductions,
    null as clarity
from distributor_remittances

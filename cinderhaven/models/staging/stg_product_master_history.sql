with source as (
    select * from {{ source('raw', 'product_master_history') }}
)

select
    sku,
    snapshot_date,
    gtin14_present,
    upc_present,
    case_dims_present,
    case_weight_present,
    data_quality_score
from source

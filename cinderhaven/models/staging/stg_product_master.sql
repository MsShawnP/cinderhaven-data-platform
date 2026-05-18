with source as (
    select * from {{ source('raw', 'product_master') }}
)

select
    sku,
    product_name,
    product_line,
    subcategory,
    gtin14,
    upc,
    case_pack_qty,
    unit_weight_lbs,
    case_weight_lbs,
    case_length_in,
    case_width_in,
    case_height_in,
    msrp,
    brand_owner,
    country_of_origin,
    last_updated::date as last_updated
from source

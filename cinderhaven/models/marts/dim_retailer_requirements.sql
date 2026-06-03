with requirements as (
    select * from {{ ref('stg_retailer_requirements') }}
),

retailers as (
    select retailer_id, retailer_name from {{ ref('stg_retailers') }}
)

select
    r.retailer_name as retailer,
    rr.field,
    rr.required,
    rr.notes

from requirements rr
inner join retailers r on rr.retailer_id = r.retailer_id

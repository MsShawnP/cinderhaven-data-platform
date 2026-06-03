with stores as (
    select * from {{ ref('stg_stores') }}
),

retailers as (
    select retailer_id, retailer_name from {{ ref('stg_retailers') }}
)

select
    s.store_id,
    r.retailer_name as retailer,
    s.region,
    s.state,
    s.volume_tier

from stores s
inner join retailers r on s.retailer_id = r.retailer_id

with retailer_orders as (
    select * from {{ ref('stg_retailer_orders') }}
),

distributor_orders as (
    select * from {{ ref('stg_distributor_orders') }}
)

select
    order_id,
    retailer_id as partner_id,
    'retailer' as channel_type,
    po_number,
    po_date,
    requested_ship_date,
    total_units,
    total_value
from retailer_orders

union all

select
    order_id,
    distributor_id as partner_id,
    'distributor' as channel_type,
    po_number,
    po_date,
    null::date as requested_ship_date,
    total_units,
    total_value
from distributor_orders

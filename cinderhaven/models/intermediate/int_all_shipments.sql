with retailer_shipments as (
    select * from {{ ref('stg_retailer_shipments') }}
),

distributor_shipments as (
    select * from {{ ref('stg_distributor_shipments') }}
)

select
    shipment_id,
    order_id,
    ship_date,
    delivery_date,
    carrier,
    bol_number,
    units_shipped,
    pallets_shipped,
    asn_sent,
    asn_sent_late
from retailer_shipments

union all

select
    shipment_id,
    order_id,
    ship_date,
    delivery_date,
    carrier,
    null as bol_number,
    units_shipped,
    null::integer as pallets_shipped,
    null::boolean as asn_sent,
    null::boolean as asn_sent_late
from distributor_shipments

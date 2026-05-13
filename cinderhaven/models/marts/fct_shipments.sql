-- fct_shipments: Shipment fact with compliance flags and order linkage.
--
-- Grain: one row per shipment.

with shipments as (
    select * from {{ ref('stg_shipments') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

pack as (
    select * from {{ ref('stg_pack_records') }}
),

final as (
    select
        shipments.shipment_id,
        shipments.order_id,
        orders.retailer_id,
        orders.po_number,
        orders.po_date,
        shipments.ship_date,
        shipments.delivery_date,
        shipments.delivery_date - shipments.ship_date as transit_days,
        shipments.carrier,
        shipments.bol_number,
        shipments.bol_signed,
        shipments.bol_signed_short,
        shipments.bol_signed_damaged,
        shipments.pod_received,
        shipments.units_shipped,
        shipments.pallets_shipped,
        shipments.asn_sent,
        shipments.asn_sent_late,
        -- Compliance flags
        case
            when shipments.bol_signed and not shipments.bol_signed_short
                 and not shipments.bol_signed_damaged and shipments.pod_received
            then true else false
        end as clean_delivery,
        case
            when shipments.asn_sent and not shipments.asn_sent_late
            then true else false
        end as asn_compliant,
        -- Pack compliance
        pack.units_pick_pack_match,
        pack.label_scannable,
        pack.pack_verification,
        pack.evidence_format
    from shipments
    inner join orders on shipments.order_id = orders.order_id
    left join pack on shipments.shipment_id = pack.shipment_id
)

select * from final

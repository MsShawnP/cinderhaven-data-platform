with source as (
    select * from {{ source('raw', 'shipments') }}
),

staged as (
    select
        shipment_id,
        order_id,
        ship_date::date as ship_date,
        delivery_date::date as delivery_date,
        carrier,
        bol_number,
        bol_signed::boolean as bol_signed,
        bol_signed_short::boolean as bol_signed_short,
        bol_signed_damaged::boolean as bol_signed_damaged,
        pod_received::boolean as pod_received,
        units_shipped,
        pallets_shipped,
        asn_sent::boolean as asn_sent,
        asn_sent_late::boolean as asn_sent_late
    from source
)

select * from staged

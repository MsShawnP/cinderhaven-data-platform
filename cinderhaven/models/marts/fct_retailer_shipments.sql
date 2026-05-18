with shipments as (
    select * from {{ ref('stg_retailer_shipments') }}
),

orders as (
    select order_id, retailer_id, requested_ship_date
    from {{ ref('stg_retailer_orders') }}
),

pack_records as (
    select
        shipment_id,
        count(*) as pack_record_count,
        bool_and(label_scannable) as all_labels_scannable,
        sum(units_picked) as total_units_picked,
        sum(units_packed) as total_units_packed
    from {{ ref('stg_retailer_pack_records') }}
    where shipment_id is not null
    group by shipment_id
)

select
    s.shipment_id,
    s.order_id,
    o.retailer_id,
    s.ship_date,
    s.delivery_date,
    s.carrier,
    s.bol_number,
    s.units_shipped,
    s.pallets_shipped,
    s.asn_sent,
    s.asn_sent_late,

    o.requested_ship_date,
    case
        when o.requested_ship_date is not null
        then s.ship_date <= o.requested_ship_date
    end as is_on_time,
    case
        when o.requested_ship_date is not null and s.ship_date > o.requested_ship_date
        then s.ship_date - o.requested_ship_date
        else 0
    end as days_late,

    coalesce(pr.pack_record_count, 0) as pack_record_count,
    pr.all_labels_scannable,
    coalesce(pr.total_units_picked, 0) as total_units_picked,
    coalesce(pr.total_units_packed, 0) as total_units_packed

from shipments s
inner join orders o on s.order_id = o.order_id
left join pack_records pr on s.shipment_id = pr.shipment_id

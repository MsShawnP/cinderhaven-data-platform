with source as (
    select * from {{ source('raw', 'orders') }}
),

staged as (
    select
        order_id,
        retailer_id,
        po_number,
        po_date::date as po_date,
        requested_ship_date::date as requested_ship_date,
        requested_delivery_window_start::date as requested_delivery_window_start,
        requested_delivery_window_end::date as requested_delivery_window_end,
        dc_id,
        total_units,
        total_value
    from source
)

select * from staged

with source as (
    select * from {{ source('raw', 'product_master') }}
),

staged as (
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
        serving_size,
        calories_per_serving,
        total_fat_g,
        sodium_mg,
        total_carb_g,
        protein_g,
        brand_owner,
        country_of_origin,
        active_retailers,
        oneworldsync_status,
        last_updated::date as last_updated,
        updated_by
    from source
)

select * from staged

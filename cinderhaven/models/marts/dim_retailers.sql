-- dim_retailers: Retailer dimension with store counts and dispute metadata.
--
-- Grain: one row per retailer.

with retailers as (
    select * from {{ ref('stg_retailers') }}
),

store_counts as (
    select
        retailer,
        count(*) as total_stores,
        count(*) filter (where not is_aggregated_channel) as physical_stores,
        count(distinct state) as states_covered
    from {{ ref('stg_stores') }}
    group by retailer
),

final as (
    select
        retailers.retailer_id,
        retailers.name as retailer_name,
        retailers.channel_type,
        retailers.dispute_portal_name,
        retailers.dispute_portal_url,
        retailers.dispute_method,
        retailers.notes,
        coalesce(sc.total_stores, 0) as total_stores,
        coalesce(sc.physical_stores, 0) as physical_stores,
        coalesce(sc.states_covered, 0) as states_covered
    from retailers
    left join store_counts sc
        on retailers.name = sc.retailer
)

select * from final

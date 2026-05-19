-- Links deductions to the shortage events that caused them.
--
-- Join path: deduction → order_id → order_short (quantity shorted on that order)
--            deduction → retailer + sku → deauth event (if the shortage led to delisting)
--
-- This mart lets retailer-deduction-recovery trace a deduction back to the
-- specific shortage simulation that generated it, and see whether that
-- shortage ultimately caused a deauthorization event.

with retailer_deductions as (
    select
        deduction_id,
        retailer_id as partner_id,
        'retailer' as channel_type,
        order_id,
        deduction_type,
        deduction_amount,
        deduction_date,
        deduction_status,
        recovered_amount,
        net_deduction_amount
    from {{ ref('fct_retailer_deductions') }}
),

distributor_deductions as (
    select
        deduction_id,
        distributor_id as partner_id,
        'distributor' as channel_type,
        order_id,
        deduction_type,
        deduction_amount,
        deduction_date,
        deduction_status,
        recovered_amount,
        net_deduction_amount
    from {{ ref('fct_distributor_deductions') }}
),

all_deductions as (
    select * from retailer_deductions
    union all
    select * from distributor_deductions
),

shorts as (
    select * from {{ ref('stg_shortage_order_shorts') }}
),

deauth as (
    select * from {{ ref('stg_shortage_deauth_events') }}
),

-- Map partner_id to retailer name used in the shortage simulation
retailers as (
    select retailer_id, retailer_name from {{ ref('dim_retailers') }}
),

distributors as (
    select distributor_id, distributor_name from {{ ref('dim_distributors') }}
),

deductions_with_shorts as (
    select
        d.deduction_id,
        d.partner_id,
        d.channel_type,
        d.order_id,
        d.deduction_type,
        d.deduction_amount,
        d.deduction_date,
        d.deduction_status,
        d.recovered_amount,
        d.net_deduction_amount,

        s.short_id,
        s.sku as shorted_sku,
        s.quantity_shorted,
        s.short_reason,

        -- Resolve partner name for deauth join
        case
            when d.channel_type = 'retailer' then r.retailer_name
            when d.channel_type = 'distributor' then dist.distributor_name
        end as partner_name

    from all_deductions d
    left join shorts s
        on d.order_id = s.order_id
    left join retailers r
        on d.channel_type = 'retailer' and d.partner_id = r.retailer_id
    left join distributors dist
        on d.channel_type = 'distributor' and d.partner_id = dist.distributor_id
)

select
    dws.deduction_id,
    dws.partner_id,
    dws.channel_type,
    dws.partner_name,
    dws.order_id,
    dws.deduction_type,
    dws.deduction_amount,
    dws.deduction_date,
    dws.deduction_status,
    dws.recovered_amount,
    dws.net_deduction_amount,

    -- Shortage linkage
    dws.short_id,
    dws.shorted_sku,
    dws.quantity_shorted,
    dws.short_reason,
    case when dws.short_id is not null then true else false end as has_shortage_link,

    -- Deauthorization linkage
    da.trigger_type as deauth_trigger_type,
    da.velocity_without_shorts as deauth_velocity_clean,
    da.velocity_with_shorts as deauth_velocity_actual,
    da.threshold as deauth_threshold,
    da.annualized_revenue_lost as deauth_revenue_lost,
    case when da.sku is not null then true else false end as has_deauth_link

from deductions_with_shorts dws
left join deauth da
    on dws.shorted_sku = da.sku
    and dws.partner_name = da.retailer

-- mart_channel_contribution: Channel-level profitability with layered cost attribution.
--
-- Grain: one row per channel (retailer or DTC).
-- Layers: revenue → gross margin → post-deduction → post-compliance → net contribution.
-- Operational overhead uses actual dispute labor hours where available,
-- flat $35/hr estimate × 0.5hr per non-disputed deduction otherwise.

{% set labor_rate = 35 %}
{% set flat_triage_hours = 0.5 %}

with revenue as (
    select
        coalesce(o.retailer_id, 'DTC') as channel_id,
        case
            when o.channel = 'DTC' then 'DTC'
            else r.retailer_name
        end as channel_name,
        case
            when o.channel = 'DTC' then 'DTC'
            else r.channel_type
        end as channel_type,
        sum(o.line_total) as gross_revenue,
        sum(o.quantity * p.cogs_per_unit) as total_cogs
    from {{ ref('fct_orders') }} o
    left join {{ ref('dim_retailers') }} r
        on o.retailer_id = r.retailer_id
    left join {{ ref('dim_products') }} p
        on o.sku = p.sku
    group by 1, 2, 3
),

deductions_by_category as (
    select
        d.retailer_id as channel_id,
        sum(case
            when d.deduction_type in ('short_ship', 'promo_billback', 'slotting', 'vague')
            then d.deduction_amount else 0
        end) as trade_deductions,
        sum(case
            when d.deduction_type in ('label_fine', 'pallet_fine', 'spoilage', 'damaged')
            then d.deduction_amount else 0
        end) as quality_fines,
        sum(case
            when d.deduction_type = 'late_delivery'
            then d.deduction_amount else 0
        end) as logistics_fines,
        sum(d.deduction_amount) as total_deductions
    from {{ ref('fct_deductions') }} d
    group by 1
),

operational_overhead as (
    select
        d.retailer_id as channel_id,
        sum(
            case
                when d.was_disputed and d.dispute_labor_hours is not null
                then d.dispute_labor_hours * {{ labor_rate }}
                else {{ flat_triage_hours }} * {{ labor_rate }}
            end
        ) as overhead_estimate,
        sum(case when d.was_disputed then 1 else 0 end) as disputes_filed,
        count(*) as total_deduction_events
    from {{ ref('fct_deductions') }} d
    group by 1
),

promo_costs as (
    select
        r.retailer_id as channel_id,
        sum(pr.promo_cost) as total_promo_cost
    from {{ ref('stg_promotions') }} pr
    inner join {{ ref('dim_retailers') }} r
        on pr.retailer = r.retailer_name
    group by 1
),

final as (
    select
        rev.channel_id,
        rev.channel_name,
        rev.channel_type,
        rev.gross_revenue,
        rev.total_cogs,
        rev.gross_revenue - rev.total_cogs as gross_margin,
        coalesce(ded.trade_deductions, 0) as trade_deductions,
        coalesce(ded.quality_fines, 0) as quality_fines,
        coalesce(ded.logistics_fines, 0) as logistics_fines,
        coalesce(ded.total_deductions, 0) as total_deductions,
        coalesce(promo.total_promo_cost, 0) as promo_costs,
        coalesce(ops.overhead_estimate, 0) as operational_overhead,
        coalesce(ops.disputes_filed, 0) as disputes_filed,
        coalesce(ops.total_deduction_events, 0) as total_deduction_events,
        -- Layered contribution calculation
        rev.gross_revenue - rev.total_cogs as layer_1_gross_margin,
        rev.gross_revenue - rev.total_cogs
            - coalesce(ded.trade_deductions, 0)
            - coalesce(promo.total_promo_cost, 0)
            as layer_2_post_deductions,
        rev.gross_revenue - rev.total_cogs
            - coalesce(ded.trade_deductions, 0)
            - coalesce(promo.total_promo_cost, 0)
            - coalesce(ded.quality_fines, 0)
            - coalesce(ded.logistics_fines, 0)
            as layer_3_post_compliance,
        rev.gross_revenue - rev.total_cogs
            - coalesce(ded.trade_deductions, 0)
            - coalesce(promo.total_promo_cost, 0)
            - coalesce(ded.quality_fines, 0)
            - coalesce(ded.logistics_fines, 0)
            - coalesce(ops.overhead_estimate, 0)
            as layer_4_net_contribution
    from revenue rev
    left join deductions_by_category ded
        on rev.channel_id = ded.channel_id
    left join operational_overhead ops
        on rev.channel_id = ops.channel_id
    left join promo_costs promo
        on rev.channel_id = promo.channel_id
)

select * from final

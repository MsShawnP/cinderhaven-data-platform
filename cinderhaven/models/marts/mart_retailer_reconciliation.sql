with order_totals as (
    select
        retailer_id,
        count(*) as order_count,
        sum(total_value) as total_order_value
    from {{ ref('stg_retailer_orders') }}
    group by retailer_id
),

shipment_totals as (
    select
        o.retailer_id,
        count(distinct s.shipment_id) as shipment_count,
        count(distinct case when s.shipment_id is null then o.order_id end) as unshipped_orders,
        sum(o.total_units) as total_units_ordered,
        sum(s.units_shipped) as total_units_shipped
    from {{ ref('stg_retailer_orders') }} o
    left join {{ ref('stg_retailer_shipments') }} s on o.order_id = s.order_id
    group by o.retailer_id
),

remittance_totals as (
    select
        retailer_id,
        count(*) as remittance_count,
        sum(gross_amount) as total_remittance_gross,
        sum(net_amount) as total_remittance_net,
        sum(total_deductions) as total_remittance_deductions,
        sum(trade_allowance) as total_trade_allowance,
        sum(chargebacks_applied) as total_chargebacks_applied,
        sum(timing_residual) as total_timing_residual
    from {{ ref('stg_retailer_remittances') }}
    group by retailer_id
),

deduction_totals as (
    select
        retailer_id,
        count(*) as deduction_count,
        sum(deduction_amount) as total_deduction_amount,
        count(case when deduction_status = 'disputed' then 1 end) as disputed_count,
        sum(case when dispute_outcome = 'won' then recovered_amount else 0 end) as total_recovered_won,
        sum(case when dispute_outcome = 'partial' then recovered_amount else 0 end) as total_recovered_partial,
        sum(coalesce(recovered_amount, 0)) as total_recovered
    from {{ ref('int_retailer_payments') }}
    group by retailer_id
),

chargeback_totals as (
    select
        retailer_id,
        count(*) as chargeback_count,
        sum(chargeback_amount) as total_chargeback_amount
    from {{ ref('stg_retailer_chargebacks') }}
    group by retailer_id
)

select
    r.retailer_id,
    r.retailer_name,

    ot.order_count,
    ot.total_order_value,

    st.shipment_count,
    st.unshipped_orders,
    st.total_units_ordered,
    st.total_units_shipped,
    case
        when st.total_units_ordered > 0
        then round(st.total_units_shipped::numeric / st.total_units_ordered, 4)
    end as unit_fill_rate,

    rt.remittance_count,
    rt.total_remittance_gross,
    rt.total_remittance_net,
    rt.total_remittance_deductions,
    coalesce(rt.total_trade_allowance, 0) as total_trade_allowance,
    coalesce(rt.total_chargebacks_applied, 0) as total_chargebacks_applied,
    coalesce(rt.total_timing_residual, 0) as total_timing_residual,

    dt.deduction_count,
    dt.total_deduction_amount,
    dt.disputed_count,
    dt.total_recovered_won,
    dt.total_recovered_partial,
    dt.total_recovered,

    cbt.chargeback_count,
    coalesce(cbt.total_chargeback_amount, 0) as total_chargeback_amount,

    ot.total_order_value - coalesce(rt.total_remittance_gross, 0) as orders_vs_remittance_diff,
    coalesce(rt.total_remittance_deductions, 0)
        - coalesce(dt.total_deduction_amount, 0) as remittance_vs_deduction_diff,
    ot.total_order_value
        - coalesce(rt.total_remittance_net, 0)
        - coalesce(dt.total_deduction_amount, 0)
        - coalesce(cbt.total_chargeback_amount, 0)
        - coalesce(rt.total_trade_allowance, 0) as net_reconciliation_gap

from {{ ref('stg_retailers') }} r
left join order_totals ot on r.retailer_id = ot.retailer_id
left join shipment_totals st on r.retailer_id = st.retailer_id
left join remittance_totals rt on r.retailer_id = rt.retailer_id
left join deduction_totals dt on r.retailer_id = dt.retailer_id
left join chargeback_totals cbt on r.retailer_id = cbt.retailer_id

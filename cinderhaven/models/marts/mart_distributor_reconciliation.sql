with order_totals as (
    select
        distributor_id,
        count(*) as order_count,
        sum(total_value) as total_order_value
    from {{ ref('stg_distributor_orders') }}
    group by distributor_id
),

shipment_totals as (
    select
        o.distributor_id,
        count(distinct s.shipment_id) as shipment_count,
        count(distinct case when s.shipment_id is null then o.order_id end) as unshipped_orders,
        sum(o.total_units) as total_units_ordered,
        sum(s.units_shipped) as total_units_shipped
    from {{ ref('stg_distributor_orders') }} o
    left join {{ ref('stg_distributor_shipments') }} s on o.order_id = s.order_id
    group by o.distributor_id
),

remittance_totals as (
    select
        distributor_id,
        count(*) as remittance_count,
        sum(gross_amount) as total_remittance_gross,
        sum(net_amount) as total_remittance_net,
        sum(total_deductions) as total_remittance_deductions,
        sum(trade_allowance) as total_trade_allowance,
        sum(chargebacks_applied) as total_chargebacks_applied,
        sum(timing_residual) as total_timing_residual
    from {{ ref('stg_distributor_remittances') }}
    group by distributor_id
),

deduction_totals as (
    select
        distributor_id,
        count(*) as deduction_count,
        sum(deduction_amount) as total_deduction_amount
    from {{ ref('stg_distributor_deductions') }}
    group by distributor_id
),

dispute_totals as (
    select
        dd.distributor_id,
        count(*) as dispute_count,
        sum(coalesce(disp.recovered_amount, 0)) as total_recovered
    from {{ ref('stg_distributor_deductions') }} dd
    inner join {{ ref('stg_distributor_disputes') }} disp on dd.deduction_id = disp.deduction_id
    group by dd.distributor_id
),

chargeback_totals as (
    select
        distributor_id,
        count(*) as chargeback_count,
        sum(chargeback_amount) as total_chargeback_amount
    from {{ ref('stg_distributor_chargebacks') }}
    group by distributor_id
)

select
    d.distributor_id,
    d.distributor_name,

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

    coalesce(dispt.dispute_count, 0) as dispute_count,
    coalesce(dispt.total_recovered, 0) as total_recovered,

    coalesce(cbt.chargeback_count, 0) as chargeback_count,
    coalesce(cbt.total_chargeback_amount, 0) as total_chargeback_amount,

    ot.total_order_value - coalesce(rt.total_remittance_gross, 0) as orders_vs_remittance_diff,
    coalesce(rt.total_remittance_deductions, 0)
        - coalesce(dt.total_deduction_amount, 0) as remittance_vs_deduction_diff,
    ot.total_order_value
        - coalesce(rt.total_remittance_net, 0)
        - coalesce(dt.total_deduction_amount, 0)
        - coalesce(cbt.total_chargeback_amount, 0)
        - coalesce(rt.total_trade_allowance, 0) as net_reconciliation_gap

from {{ ref('stg_distributors') }} d
left join order_totals ot on d.distributor_id = ot.distributor_id
left join shipment_totals st on d.distributor_id = st.distributor_id
left join remittance_totals rt on d.distributor_id = rt.distributor_id
left join deduction_totals dt on d.distributor_id = dt.distributor_id
left join dispute_totals dispt on d.distributor_id = dispt.distributor_id
left join chargeback_totals cbt on d.distributor_id = cbt.distributor_id

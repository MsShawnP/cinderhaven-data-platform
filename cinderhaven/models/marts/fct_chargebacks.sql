-- fct_chargebacks: Legacy chargeback events (pre-deductions pipeline).
--
-- Grain: one row per chargeback event (month × retailer × reason × SKU).

with chargebacks as (
    select * from {{ ref('stg_chargebacks') }}
),

final as (
    select
        month,
        retailer,
        reason,
        sku,
        amount
    from chargebacks
)

select * from final

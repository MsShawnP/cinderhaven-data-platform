with source as (
    select * from {{ source('raw', 'sku_costs') }}
),

staged as (
    select
        sku,
        cogs_per_unit,
        landed_cost_per_unit,
        wholesale_price,
        wholesale_walmart,
        wholesale_costco,
        wholesale_whole_foods,
        wholesale_regional,
        wholesale_unfi,
        wholesale_dtc,
        trade_spend_pct_walmart,
        trade_spend_pct_costco,
        trade_spend_pct_whole_foods,
        trade_spend_pct_regional,
        trade_spend_pct_unfi,
        trade_spend_pct_dtc
    from source
)

select * from staged

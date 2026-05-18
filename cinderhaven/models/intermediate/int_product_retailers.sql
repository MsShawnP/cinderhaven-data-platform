with products as (
    select * from {{ ref('stg_product_master') }}
),

costs as (
    select * from {{ ref('stg_sku_costs') }}
),

retailers as (
    select * from {{ ref('stg_retailers') }}
),

sku_retailer_pricing as (
    select sku, 'RET-WALMART' as retailer_id, wholesale_walmart as channel_wholesale, trade_spend_pct_walmart as trade_spend_pct from costs
    union all
    select sku, 'RET-COSTCO', wholesale_costco, trade_spend_pct_costco from costs
    union all
    select sku, 'RET-WHOLEFOODS', wholesale_whole_foods, trade_spend_pct_whole_foods from costs
    union all
    select sku, 'RET-SPROUTS', wholesale_sprouts, trade_spend_pct_sprouts from costs
    union all
    select sku, 'RET-KROGER', wholesale_regional, trade_spend_pct_regional from costs
    union all
    select sku, 'RET-REGIONAL', wholesale_regional, trade_spend_pct_regional from costs
)

select
    p.sku,
    srp.retailer_id,
    ret.retailer_name,
    p.product_name,
    p.product_line,
    p.case_pack_qty,
    c.cogs_per_unit,
    c.landed_cost_per_unit,
    srp.channel_wholesale as wholesale_price,
    srp.trade_spend_pct,
    srp.channel_wholesale - c.cogs_per_unit as margin_per_unit,
    case
        when srp.channel_wholesale > 0
        then round((srp.channel_wholesale - c.cogs_per_unit) / srp.channel_wholesale, 4)
        else 0
    end as margin_pct

from sku_retailer_pricing srp
inner join products p on srp.sku = p.sku
inner join costs c on srp.sku = c.sku
inner join retailers ret on srp.retailer_id = ret.retailer_id

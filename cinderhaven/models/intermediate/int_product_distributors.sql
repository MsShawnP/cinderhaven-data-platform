with products as (
    select * from {{ ref('stg_product_master') }}
),

costs as (
    select * from {{ ref('stg_sku_costs') }}
),

distributors as (
    select * from {{ ref('stg_distributors') }}
),

sku_distributors as (
    select * from {{ ref('stg_sku_distributors') }}
)

select
    p.sku,
    sd.distributor_id,
    dist.distributor_name,
    p.product_name,
    p.product_line,
    p.case_pack_qty,
    c.cogs_per_unit,
    c.landed_cost_per_unit,
    case sd.distributor_id
        when 'DIST-UNFI' then c.wholesale_unfi
        when 'DIST-KEHE' then c.wholesale_kehe
        else c.wholesale_price
    end as wholesale_price,
    case sd.distributor_id
        when 'DIST-UNFI' then c.trade_spend_pct_unfi
        when 'DIST-KEHE' then c.trade_spend_pct_kehe
        else 0
    end as trade_spend_pct,
    dist.margin_pct as distributor_margin_pct,
    case sd.distributor_id
        when 'DIST-UNFI' then c.wholesale_unfi
        when 'DIST-KEHE' then c.wholesale_kehe
        else c.wholesale_price
    end - c.cogs_per_unit as margin_per_unit,
    case
        when case sd.distributor_id
                when 'DIST-UNFI' then c.wholesale_unfi
                when 'DIST-KEHE' then c.wholesale_kehe
                else c.wholesale_price
             end > 0
        then round(
            (case sd.distributor_id
                when 'DIST-UNFI' then c.wholesale_unfi
                when 'DIST-KEHE' then c.wholesale_kehe
                else c.wholesale_price
             end - c.cogs_per_unit)
            / case sd.distributor_id
                when 'DIST-UNFI' then c.wholesale_unfi
                when 'DIST-KEHE' then c.wholesale_kehe
                else c.wholesale_price
              end,
            4)
        else 0
    end as margin_pct

from sku_distributors sd
inner join products p on sd.sku = p.sku
inner join costs c on sd.sku = c.sku
inner join distributors dist on sd.distributor_id = dist.distributor_id

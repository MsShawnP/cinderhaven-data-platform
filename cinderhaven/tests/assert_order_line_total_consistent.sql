-- Orders where line_total deviates from quantity * unit_price by more
-- than $0.01 (rounding tolerance). Validates that generated and source
-- data maintain arithmetic consistency at the line level.

select
    line_id,
    quantity,
    unit_price,
    line_total,
    abs(line_total - (quantity * unit_price)) as diff
from {{ ref('fct_orders') }}
where abs(line_total - (quantity * unit_price)) > 0.01

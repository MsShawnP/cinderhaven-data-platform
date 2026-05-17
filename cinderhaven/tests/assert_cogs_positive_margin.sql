-- Every channel should have positive gross margin. Negative margin
-- means COGS exceeds revenue, which indicates a unit/case calculation
-- error in mart_channel_contribution.

select
    channel_id,
    channel_name,
    gross_revenue,
    total_cogs,
    gross_margin
from {{ ref('mart_channel_contribution') }}
where gross_margin < 0

-- Channels whose contribution margin falls outside the plausible band
-- for a specialty food brand (5% to 85% of gross revenue).
--
-- Exists because the 392 structural tests all passed while
-- mart_channel_contribution reported -522% retailer margins (COGS
-- inflated ~13x by a case-pack multiplication). Structural tests check
-- nulls, uniqueness, and referential integrity — none of them notice a
-- company losing five times its revenue. This one does.
--
-- Current values (post case-pack fix): Retailer +49.9%, Distributor
-- +44.5%, DTC +78.1%.

select
    channel,
    gross_revenue,
    total_cogs,
    contribution_margin,
    round(contribution_margin / nullif(gross_revenue, 0), 4) as margin_pct
from {{ ref('mart_channel_contribution') }}
where gross_revenue > 0
  and (
      contribution_margin / gross_revenue < 0.05
      or contribution_margin / gross_revenue > 0.85
  )

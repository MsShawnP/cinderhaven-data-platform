# Cinderhaven Canonical Figure Plausibility Audit

**Date:** 2026-06-12
**Scope:** Do the figures in `CINDERHAVEN_CANONICAL.md` describe a coherent, plausible company? This is not a drift check (`check_canonical.py` covers that); it is a coherence and credibility check against CPG operating norms.
**Constraint honored:** Read-only. No data changed, nothing regenerated in the SSOT, no edits to `CINDERHAVEN_CANONICAL.md`.

## Method and certification

Direct access to the Fly.io production Postgres was not opened for this audit. Instead, a disposable local replica was built and **certified equivalent to the locked canonical state**:

1. `docker compose up` (local Postgres 16, untouched by and untouching prod)
2. `python scripts/seed_all.py` at repo HEAD — deterministic generators, `SEED=42`, frozen constants → 1,940,140 rows
3. `dbt build` — 392/392 PASS (all models and tests green)
4. `python scripts/check_canonical.py` — **PASS on all 10 guard checks** (677/160/837 chargebacks, $32.80M trailing-52w scan, $3.43M all-in trade, 10.5% rate, structural/waste rates, OTIF self-consistency)

Because the guard passes, every recomputation below is authoritative for the figures the canonical file locks. Engagement-repo derivations (short-ship, OTIF, contract-to-cash, deduction recovery, channel delta, launch, SKU) were read from their published source in `projects\published\`.

A note on the audit brief: several figures named in the brief ($296K chargebacks, $33.1M short-ship, 86.5¢ lifecycle, $1.03M waste / 21.7% all-in, $361K product-data cost) are in the canonical SUPERSEDES list. This audit evaluates the **current locked values** ($458K, $32.8M, 83¢, $480K / 10.5%, $458K) and notes the stale ones where relevant.

---

## Phase 1 — The revenue base

Recomputed from the replica (queries: `SUM(total_value)` from `raw.retailer_orders` + `raw.distributor_orders`, `SUM(total)` from `raw.shopify_orders`, grouped by calendar year of `po_date`/`created_at`):

| Measure | 2023 | 2024 | 2025 | 36-mo total |
|---|---|---|---|---|
| Invoiced orders — retailer | $17.02M | $17.69M | $17.29M | $52.13M |
| Invoiced orders — distributor | $8.06M | $7.95M | $7.86M | $23.94M |
| Invoiced orders — DTC | $0.19M | $0.19M | $0.18M | $0.57M |
| **Invoiced total** | **$25.28M** | **$25.83M** | **$25.33M** | **$76.63M** |
| Retail scan (sell-through, retail $) | $28.95M | $33.71M | $32.80M | $95.46M |
| Payments received (gross) | $23.07M | $25.28M | $24.45M | $76.07M¹ |

¹ Includes $3.27M received in early 2026 for late-2025 invoices; gross payments ≈ invoiced, confirming the platform bills and collects essentially everything it orders.

**The "$25M brand" claim is exactly right.** Invoiced revenue runs $25.3–25.8M per year, inside the canonical's $23–27M acceptable band. CY2024 = $25.83M.

**The "$53M shipped over three years" claim comes from a different universe.** $25M × 3 ≈ $76.6M, not $53M, and the difference is not a measure mismatch inside the platform:

- The short-ship engagement (`short-ship-cost`) rebuilds order lines **on the platform's own order skeleton** (74,299 orders: 46,760 retail + 9,042 distributor + 18,497 DTC — identical counts to the replica) but injects shortages: demand $76.48M, shipped $53.04M, **fill rate 69.35%**.
- $53.0M = 69.2% × $76.6M. The relationship, stated explicitly: **$53M is the platform's three-year invoiced demand after the short-ship repo's modeled 31% shortage haircut.**
- The platform itself contradicts that haircut: `raw.retailer_shipments`/`raw.distributor_shipments` ship **100.00% of ordered units** (12,938,208 of 12,938,208 retailer units; 6,912,888 of 6,912,888 distributor units — `units_shipped` is copied from the order in `seed_retailer.py:128`), and remittances show retailers **paying $76.1M gross**. A company cannot collect $76M cash on $53M of shipped goods.

So the public pairing "a $25M brand" + "$53M shipped over three years" mixes two irreconcilable fulfillment realities. The denominator used for normalization below is invoiced revenue: **$25.5M/yr average, $76.6M per 36 months** (scan base $32.8M trailing-52w noted where the canonical uses it).

---

## Phase 2 — Recomputed canonical figures

All queries run against the certified replica; channel = retailer + distributor unless noted.

### 2.1 Chargebacks / product-data cost ($458K/yr claimed)

```sql
SELECT COUNT(*), SUM(amount), MIN(month), MAX(month) FROM raw.retailer_chargebacks;
-- 677 | $686,533.96 | 2023-01-01 | 2026-01-01  (36 months)
```

- Retailer chargebacks: 677 / **$686,534 over 36 months** — $228,845/yr. Distributor: 160 / $244,227 → combined **$310K/yr**.
- The canonical note states "677 retailer chargebacks totaling $686,534 **over 18 months**, annualized to $457,689." The replica proves the $686,534 covers the **full 36-month window** (first month 2023-01, last 2026-01; trailing-18-month total is only $320,355 / 322 events). $686,534 × 12/18 = $457,689 exactly — **the $458K/yr figure annualizes a 36-month total as if it were 18 months, overstating by 2×**.
- The same arithmetic shows the data-defect attribution passed through 100% of dollars. The PDHA classification (`is_data = !grepl("Late delivery|Short shipment|Damaged goods", reason)`) cannot match the raw snake_case reasons (`late_delivery`, `short_ship`, `damaged`), so nothing was excluded. By reason over 36 months: short_ship $215.8K + late_delivery $191.4K + damaged $125.9K are operational; **strictly data-linked (label_fine $137.0K + pricing_error $16.4K) = $153.4K/36mo ≈ $51K/yr** (≈$93K/yr if `damaged` is counted as data-linked, which the seeder's defect-conditional logic supports).
- Normalized: total compliance chargebacks $310K/yr = **1.2% of invoiced** — inside the <1–2% norm band. The honest data-attributable figure is **0.2–0.4% of invoiced**.
- The canonical's own SUPERSEDES table kills the phrase "18 months" ("Always was 36 months") two sections below the note that asserts it.

### 2.2 Deductions ($1.66M backlog, 16%→65% recovery, $861K exposure)

```sql
SELECT SUM(amount), COUNT(*) FROM raw.retailer_deductions;     -- $1,332,704.39 | 13,960
SELECT SUM(amount), COUNT(*) FROM raw.distributor_deductions;  -- $330,589.81  | 2,063
```

- Cross-channel total **$1,663,294 / 16,023 rows** — matches the canonical to the dollar, scope note confirmed.
- Per year ≈ $554K = **2.2% of invoiced**. The 3–10% "deduction problem" band starts above this; as itemized, Cinderhaven's deduction book is *clean-side*, not distressed. (The distressed scenario exists precisely because of this; it is labeled, which is correct practice.)
- "Backlog" is a mislabel: the $1.66M is **every deduction taken in 37 months**, including 3,012 already resolved won/partial disputes and $282,600 already recovered — not an open, workable backlog.
- Recovery baseline: recovered $235,053 (retailer) + $47,547 (distributor) = **$282,600 on 6,217 disputes filed** → 16.99% **of all deduction dollars** — replica matches the published summary.json exactly. The 65% "max recovery" is the assumed win probability of the best evidence tier (`digital_complete: 65%` in `frontend/src/domain.ts`). Two different denominators: recovered-÷-everything vs. win-rate-per-dispute-at-best-evidence.
- More fundamental: in the certified data, **dispute outcomes are independent of evidence quality** — win-equivalent rates are 44.8% (strong), 45.5% (moderate), 44.7% (weak) (`raw.retailer_disputes` grouped by `evidence_quality`). The engagement's central causal claim (better evidence → better recovery) has zero support in its own dataset; the 16.5%→65% journey is an assumption stack, not a data finding.
- $861K forward exposure: not recomputable from platform tables (post-audit model internal to the repo; 286 `retailer_post_audit_claims` exist as input). Repo history shows a $714K variant of the same headline; provenance is not pinned.

### 2.3 The remittance seam (affects lifecycle 83¢ and any deduction narrative)

```sql
SELECT SUM(gross_amount - net_amount), SUM(total_deductions) FROM raw.retailer_remittances;
-- $6,841,276 | $6,841,276   (13.1% haircut on retailer gross)
SELECT SUM(amount) FROM raw.retailer_deductions;  -- $1,332,704
```

Remittances withhold **$6.84M** over 36 months; the itemized deduction table explains **$1.33M (19.5%)** of it. The other **$5.51M (~$1.8M/yr, 10.6% of retailer invoiced) is unexplained by any record in the platform** — `generate_remittances` draws each remittance's haircut as `uniform(8%, 18%)` of gross, independent of the deductions it generates (`seed_retailer.py:160`). The platform's own `mart_retailer_reconciliation` dutifully reports this as `remittance_vs_deduction_diff` (e.g., Kroger: $1.42M withheld vs $263K itemized).

### 2.4 Revenue lifecycle (83¢ per gross invoiced dollar)

Recomputed CY2024 exactly as `contract-to-cash/scripts/export_json.py` does:
(retailer remit net $14,585,446 + DTC net $177,177) ÷ (retailer invoiced $17,689,036 + DTC $190,826) = **82.56¢** ✓ (canonical 83¢; the brief's 86.5¢ is the dead pre-shift value).

- **In the 75–90¢ norm band.** The headline survives.
- The decomposition does not: of the 17.4¢ leakage, itemized deductions explain ~2.6¢, DTC fees/refunds a sliver, and **~13.4¢ is the "unclassified shortfall" plug** ($1,757,783 in the published lifecycle.json) — i.e., the uniform-draw seam from §2.3. A skeptical reviewer asking "where exactly does the 17¢ go?" gets an answer for a fifth of it, from a platform whose stated purpose is tracing exactly this.

### 2.5 Trade spend ($3.4M/yr all-in, 10.5%; $480K/yr waste)

- Operational waste: retailer $1,187,622 + distributor $267,534 = $1,455,156/36mo (excl. promo_billback) → **$485K/yr ✓**, 1.5% of scan, 1.9% of invoiced.
- All-in $3.43M / 10.5% of trailing-52w scan reproduces ✓ — but the structural component embeds a derivation bug: `check_canonical.py`'s `rate_map` has no Kroger or Sprouts entries, so both fall to the 7% regional fallback instead of their actual `sku_costs` rates (10%, 9%). With true per-channel rates, structural = **$3.23M (9.9%)** and all-in = **$3.72M (11.3% of scan)** — the locked figure understates by ~$285K/yr.
- The canonical's EBITDA row says "13.7% trade": that is the same $3.4M expressed on the **invoiced** base (~$25M) while the headline 10.5% uses the **scan** base ($32.8M). Both true, neither labeled; one file, two denominators.
- Normalized vs CPG norms (15–25% of gross all-in): Cinderhaven runs **10.5–14.6% depending on base — below the band**. For a brand the portfolio describes as bleeding trade money, it is actually lean; the published narrative wisely leans on waste composition and visibility rather than the all-in rate, which is the right call. (Brief's "21.7% all-in / $1.03M waste" are dead values.)

### 2.6 OTIF (95% internal / 86% retailer-scored; $433K exposure)

- The platform encodes **no shortage** (fill = 100.00%) and no OTIF measure. The 95% internal figure is a hardcoded portfolio target in `otif-blind-spot`; the 86.15% retailer score, the $136K/yr fines (COGS × 3% with a ×14.0 "brand magnitude" multiplier), and the $297K/yr velocity damage ($20.00 per unit short, assumed) are all synthesized in that repo over a 27-month window.
- Magnitudes are norm-plausible: 86% sits at the bottom of the 85–95% typical OTIF band (right place for a "blind spot" story); fines of $136K = 0.5% of invoiced is realistic; $433K total = 1.7%.
- Provenance is the problem, plus a 5× cross-repo contradiction (see §4 and Cross-figure findings).
- The platform does encode chronic **lateness**: mean delivery is 4.0 days after `requested_ship_date`, and ~71% of shipments deliver more than 2 days after it — uniformly 70.9–71.7% across all six retailers, which is its own realism problem (no retailer differentiation).

### 2.7 Channel delta ($91K per $1M deployed)

- `where-the-money-comes-from` computes contribution margins of 81.08% (retail) vs 90.16% (distributor) from **hardcoded COGS ratios** (`00_export_snapshot.py:99`): retail 14.1–16.9%, distributor 7.6–8.5%, DTC 17.4% of gross revenue.
- Catalog-true COGS (from `sku_costs` and `WHOLESALE_MULT`): COGS ≈ 25% of MSRP; retailers buy at 48–58% of MSRP → COGS ≈ **46% of retail-channel revenue**; distributors buy at 44–46% → ≈ **53% of distributor revenue** (replica: `SUM(units_ordered × cogs_per_unit)` = $23.9M on $52.1M retailer revenue = 45.9%; $12.8M on $23.9M distributor = 53.3%).
- The hardcoded ratios are therefore ~3× too low **and inverted**: the same physical unit sold cheaper to a distributor must carry a *higher* COGS ratio, not half the retail one. Roughly 8 of the 9.1 margin points behind the $91K delta come from that inversion. With true COGS, the distributor advantage shrinks toward zero and may flip.
- The platform mart can't arbitrate, because `mart_channel_contribution` is itself broken (§2.9).
- Channel revenue inputs are real (hardcoded `FISCAL_REVENUE` matches replica 3-yr invoiced within ~1%); only the cost stack is fabricated.

### 2.8 Launch economics ($499,200 / −$36,320) and SKU rationalization (19 / 22 of 50)

- **Launch:** openly an assumptions model, operator-validated, not platform-derived. Internal arithmetic verifies (1,200 doors × 4 SKUs × 2.0 units/door/wk × ~$1.00 wholesale ≈ $499K; stack of slotting $48K, free fills $86.4K, 12% trade, 5% broker, COGS $0.45/unit, $3,232/mo overhead → −$36,320). The replica's observed velocity (~2.2–2.3 units/door/wk/SKU at the four big chains) is consistent with the 2.0 assumption. This figure earns its "operator-validated" label.
- **SKU rationalization:** buckets are disjoint and sum (19 kill + 22 fix-or-kill + 7 maintain + 2 double-down = 50 ✓). But the underlying portfolio has almost no dispersion: per-SKU velocity spans **5.05–9.59 units/store/week** (min→max 1.9×; p10→p90 = 6.18→8.97), and the bottom revenue quartile of SKUs still contributes **16.8%** of scan revenue (top quartile 34.8%). Real 50-SKU portfolios spread 10×+ with bottom quartiles under 5%. Every "kill candidate" here sells 5–8 units/store/week — a velocity most specialty brands would celebrate. The 19/22 counts are percentile artifacts of forced relative ranking on a uniform portfolio, and the scoring repo's own note concedes loaded margins are negative for **all 50 SKUs** — a company with zero contribution-positive SKUs and 11% EBITDA (canonical's check) cannot both exist.

### 2.9 Platform mart defect found during recomputation

`mart_channel_contribution` computes COGS as `units_ordered × case_pack_qty × cogs_per_unit`, but order lines already store **units** (priced per unit, `seed_retailer.py:97–98`). The mart therefore inflates COGS by the case-pack factor (~13.45×):

| Channel | Gross revenue | Mart COGS | COGS % of rev | Mart contribution |
|---|---|---|---|---|
| Retailer | $52.13M | $322.03M | 617.8% | **−521.9%** |
| Distributor | $23.94M | $171.58M | 716.8% | **−619.0%** |
| DTC (no case-pack term) | $0.57M | $0.10M | 17.4% | +78.1% |

All 392 dbt tests pass while the flagship cross-channel mart reports the company losing 5–6× its revenue — the test suite checks nulls, uniqueness, and referential integrity, not economic sanity. Any reviewer who queries this mart (it feeds `export_revenue_truth.py`'s channel waterfall) sees the absurdity immediately.

---

## Phase 3 — Benchmarks against CPG norms

Base: invoiced $25.5M/yr (scan $32.8M where noted).

| Metric | Cinderhaven (recomputed) | Norm band | Position |
|---|---|---|---|
| All-in trade | 10.5% of scan / 13.7–14.6% of invoiced ($3.43–3.72M) | 15–25% of gross | **Below band** (lean, not distressed) |
| Deductions (itemized) | 2.2% of invoiced ($554K/yr) | 3–10% (problem brands high) | **Below band** |
| Remittance withholding (incl. unexplained) | 13.1% of retailer gross | 3–10% | **Above band, 80% unitemized** |
| Compliance chargebacks | 1.2% of invoiced ($310K/yr) | <1–2% of shipped | In band (high half) ✓ |
| Claimed product-data chargeback cost | 1.8% of invoiced ($458K/yr) | subset of the above | **Subset exceeds its superset** (see contradictions) |
| Fill rate | 100% (platform) / 69.35% (short-ship repo) | 90–98% | **Both outside band, opposite directions** |
| OTIF | 95% internal / 86% retailer (synthetic) | 85–95% | In band ✓ (provenance aside) |
| OTIF fines | 0.5% of invoiced ($136K/yr) | <1% typical | In band ✓ |
| Net revenue realization | 82.6¢ per invoiced $ | 75–90¢ | In band ✓ |
| Launch Y1 net | −7.3% of Y1 gross; slotting+free-fill = 27% of Y1 revenue | typical for new national placement | In band ✓ |
| DTC share | 0.74% of revenue | 1–10% for wholesale-led brands | Low but plausible |

The brief's premise — "a brand built to demonstrate problems should sit at the bad end of the bands, but inside them" — is met by OTIF, chargebacks, and realization. It is **missed in both directions** by the deduction/trade rates (too clean) and by the short-ship fill rate (catastrophically outside). The canonical describes a company that is simultaneously better-run than the median brand (trade 10.5%, deductions 2.2%, fill 100%) and worse-run than almost any surviving brand (69% fill, 44% of revenue lost to fulfillment) depending on which figure you read.

---

## Phase 4 — Decomposing the short-ship figure ($32.8M / 3yr on $53.0M shipped)

From `short-ship-cost`'s cost engine (`scripts/cost_engine/*`, `data/short_ship_cost.db:cost_summary`); current values:

| # | Dimension | $ (3yr) | % of total cost | % of $53.0M shipped | Classification |
|---|---|---|---|---|---|
| 1 | Lost revenue | $23,425,382 | 71.5% | 44.2% | **Mislabeled** — unshipped demand at full wholesale invoice value |
| 2 | Deauthorization | $6,202,090 | 18.9% | 11.7% | Soft — 12-mo forward revenue per modeled delist trigger |
| 3 | OTIF fines | $2,069,890 | 6.3% | 3.9% | Hard (contractual schedules) |
| 4 | Triage labor | $502,218 | 1.5% | 0.9% | Semi-fixed — $9/order × 90% of all orders, flat across scenarios |
| 5 | Distributor returns | $464,393 | 1.4% | 0.9% | Hard, but promo-driven (12% unsold + 5% claims), not short-caused |
| 6 | Chargebacks | $78,413 | 0.24% | 0.15% | Hard, modeled at 0.3–0.5% of shorted value |
| 7 | DTC cancellations | $9,387 | 0.03% | 0.02% | Hard |
| 8 | DTC margin leakage | $1,010 | 0.003% | 0.002% | Soft |
| | **Total** | **$32,752,783** | | **61.8% of shipped / 42.8% of demand** | |

**Methodology assessment.**

- **90% of the total is dimensions 1+2, and neither is a cost in the P&L sense.** Lost revenue values every unshipped unit at full wholesale price; the economic loss of an unshipped unit is its forgone *contribution* (price − COGS ≈ 46–54%), so even inside the 69%-fill universe the defensible figure is ~$11–13M, not $23.4M. Deauthorization adds $6.2M of *future* revenue-at-risk on top — partially double-counting demand already counted as lost, and again at revenue rather than contribution.
- **The hard-cash dimensions sum to ~$3.1M over 3 years (~$1.05M/yr = 4.1% of revenue).** That is a striking, defensible, operator-credible number. The $32.8M headline buries it.
- **No explicit inflators** (no compounding, no NPV games) — the inflation is structural: revenue-as-cost framing plus a 31% shortage assumption.
- **Coexistence with the other canonical figures fails three ways:**
  1. *vs. 95% internal OTIF / 86% retailer-scored:* a 69.35% fill brand does not score 86% OTIF anywhere, and the platform it claims to describe ships 100%. Three fulfillment realities — 100% (platform), 95/86% (OTIF repo), 69% (short-ship) — are cited side by side in one canonical table.
  2. *vs. chargebacks:* the model prices short-ship compliance chargebacks at $26K/yr (dim 6) while the platform's actual chargeback table carries $144K/yr of `short_ship` + `late_delivery` chargebacks **on orders that shipped complete** — and the canonical's $458K/yr figure is bigger than both combined.
  3. *vs. OTIF fines:* dim 3 says $690K/yr; the canonical OTIF row says ~$136K/yr. Same company, same metric, **5× apart**, both ✅-confirmed in the same file.
- A skeptical reader's one-line objection stands: **"$11M/yr of fulfillment cost at a $25M/yr company that retains all six retailers and 11% EBITDA"** — 44% of revenue. The figure cannot survive contact with an operator without relabeling to its hard-cost core or regenerating the shortage model to a band-plausible fill (e.g., 92–95%, which would also let the OTIF and chargeback stories cohere).

---

## Phase 5 — Verdict table

| Figure (canonical) | Recomputed | % of base | Norm band | Verdict | Recommended fix |
|---|---|---|---|---|---|
| Annual revenue ~$25M | $25.3–25.8M/yr invoiced; CY2024 $25.83M | — | — | **PLAUSIBLE** | None. |
| Scan revenue $32.8M (t-52w) | $32,802,453 ✓ | 129% of invoiced | retail markup ~2× wholesale ✓ | **PLAUSIBLE** | None. |
| Short-ship shipped revenue $53.0M | = 69.2% × $76.6M platform demand; platform ships 100% & collects $76.1M | — | fill 90–98% | **REGENERATE** | Either inject a 92–95% shortage model into the platform itself (orders/shipments/deductions tell one story) or restate as "modeled scenario: if Cinderhaven shipped only 69% of demand." Do not present as the brand's actual shipped dollars. |
| Short-ship total cost $32.8M (8-dim) | $32.75M reproduced from repo; hard-cash core $3.1M/3yr | 42.8% of demand/yr; hard core 4.1%/yr | no norm; OTIF-fine and chargeback subcomponents 5×/competing with other canonical rows | **REGENERATE** (with RELABEL fallback) | Lead with contribution-based lost margin (~$11–13M) or the $1.05M/yr hard-cost core; reconcile dim-3 fines with the OTIF row and dim-6 with the chargeback table. If kept as-is, retitle "modeled cost of a 69%-fill scenario," not a measurement. |
| OTIF 95% internal / 86% retailer | Not in platform data (fill=100%); synthesized in otif-blind-spot (hardcoded 95% target; ×14 COGS multiplier; $20/unit velocity damage) | gap 8.85pts | 85–95% ✓ | **RELABEL** | Keep the numbers (band-plausible); label provenance: "modeled on synthesized OTIF events; platform order data does not record shortages." Reconcile with short-ship or drop one universe. |
| OTIF exposure $433K = $136K fines + $297K velocity | Self-consistent ✓ (guard checks sum) | 1.7% of invoiced | fines <1% ✓ | **RELABEL** | Mark velocity damage as assumption ($20/unit). Resolve the 5× conflict with short-ship dim 3. |
| Product-data cost $458K/yr | $686,534 is the **36-month** total → $229K/yr all-reason; strictly data-linked $51K/yr (incl. damaged $93K/yr) | claimed 1.8% vs actual 0.2–0.4% of invoiced | <1–2% total compliance ✓ (total = 1.2%) | **REGENERATE** | Recompute PDHA with the true 36-mo window (÷3) and a reason classifier that matches the actual snake_case reasons. Expect ~$50–95K/yr attributable; update canonical note, About page, and ten-decisions. The $458K is arithmetically wrong, not just mislabeled. |
| Deductions backlog $1.66M | $1,663,294 / 16,023 ✓ exact | 2.2%/yr of invoiced | 3–10% (below band) | **RELABEL** | "36-month cross-channel deduction history" — it includes resolved disputes and recovered dollars; open workable backlog is smaller. Magnitude is fine. |
| Recovery 16% → 65% | 16.99% = $282,600 ÷ all deduction $ ✓; 65% = assumed best-tier win probability; outcomes in data are independent of evidence quality (44.7–45.5% across tiers) | — | 25–60% typical recovery-on-disputed | **RELABEL** (REGENERATE if the causal story must hold) | State both numbers on one denominator ("recovered 17¢ of every deduction dollar; industry-benchmark ceiling with digital evidence ≈ 65¢ of disputed dollars"). To make the evidence-quality thesis data-true, regenerate dispute outcomes conditioned on evidence tier. |
| Forward exposure $861K | Not recomputable from platform tables; repo history also carries $714K | 3.4% of invoiced | — | **RELABEL** | Pin the derivation (window, claim universe, evidence weights) in the repo and canonical note; reconcile $861K vs $714K provenance. |
| Channel delta $91K per $1M | Rests on hardcoded COGS ratios 7.6–17.4% vs catalog-true 46% (retail) / 53% (distributor); inversion supplies ~8 of 9.1 margin points | — | food COGS 40–60% of wholesale | **REGENERATE** | Recompute channel P&L with `sku_costs` COGS. Expect the delta to compress toward $0 or invert; if the story survives on deductions/promo/overhead differences alone, it will be smaller but honest. |
| Lifecycle 83¢ per invoiced $ | 82.56¢ ✓ (CY2024) | 17.4¢ leakage | 75–90¢ ✓ | **RELABEL** (headline) + **REGENERATE** (decomposition) | Keep 83¢. Fix the story of the 17¢: either generate remittance deductions as the sum of itemized records (kills the $1.76M "unclassified shortfall" plug) or relabel the plug as "trade allowances withheld on remittance, not yet itemized" and say so in contract-to-cash. |
| Launch $499,200 / −$36,320 | Arithmetic verifies; assumptions operator-validated; consistent with platform velocity (~2.2 u/door/wk) | −7.3% of Y1 gross | typical Y1 national-launch economics | **PLAUSIBLE** | None. This is the best-provenanced figure in the set. |
| SKU 19 kill / 22 fix-or-kill of 50 | Buckets disjoint, sum to 50 ✓; but velocity spread 5.05–9.59 u/st/wk (1.9×), bottom quartile = 16.8% of revenue, all 50 loaded margins negative | — | real portfolios: 10×+ spread, bottom quartile <5% | **REGENERATE** | Widen seed dispersion (velocity, margin) so kill candidates are genuinely weak; or relabel as "relative-rank screening demo — thresholds are portfolio percentiles, not absolute viability tests." As published, an operator sees 19 'kill' SKUs each selling 5–8 u/store/wk and stops trusting the framework. |
| Trade all-in $3.4M/yr @ 10.5% | Reproduces ✓, but rate_map bug prices Kroger+Sprouts at 7% (true 10%/9%) → true all-in $3.72M @ 11.3%; "13.7%" EBITDA row is the same dollars on the invoiced base | 10.5% scan / 13.7% invoiced | 15–25% (below) | **RELABEL** + guard fix | Add Kroger/Sprouts to `check_canonical.py` rate_map and relock ($3.7M/11.3%); label the two denominators (scan vs invoiced) wherever rates are quoted. |
| Operational waste $480K/yr | $485K/yr ✓ (excl. promo_billback) | 1.9% of invoiced | recoverable-waste norm 1–3% ✓ | **PLAUSIBLE** | None. |
| Chargebacks 837 (677+160) | 677 + 160 ✓ exact | — | — | **PLAUSIBLE** | None (counts; the dollars story is the PDHA row above). |
| Thesis range $1.4–3.1M/yr | Sums generic per-decision ranges; sits beside Cinderhaven callouts of $11M/yr (short-ship) and $2.18M (leakage) that it excludes | 5.5–12% of revenue | — | **RELABEL** | State that the range is the practitioner-benchmark recoverable layer and explicitly exclude the modeled-scenario figures; refresh stale embedded numbers ($461K, $33.13M, 86.5¢, 69.19% fill). |

### What survives a skeptical CPG operator as-is

**Revenue base ($25M), scan/wholesale relationship, deduction magnitude ($1.66M / 2.2%), operational waste ($480K), recovery baseline (17¢ on the dollar), lifecycle headline (83¢), OTIF magnitudes (95/86, $433K), chargeback counts (837), launch economics ($499K/−$36K).** These are either recomputed exactly or sit inside their norm bands with honest arithmetic. Launch economics is the strongest piece in the portfolio: transparent assumptions, external validation, and the platform's own velocity data agrees with it.

### Priority order for fixes

1. **$458K/yr product-data cost** — the only figure that is arithmetically wrong (2× window error, 100% attribution pass-through). It's also the most-cited number (About page, ten-decisions, canonical approved phrasing). Correct value ~$50–95K/yr attributable (~$229K/yr all-reason). One R-pipeline fix + canonical relock.
2. **The fulfillment story (short-ship $53M/$32.8M + OTIF 95/86 + 100%-fill platform)** — pick one universe. Cheapest coherent fix: regenerate platform shipments at 92–95% fill, rebuild short-ship and OTIF from it, retire the 69% scenario or label it as a stress case. Until then these rows cannot share a table headed "authoritative facts."
3. **`mart_channel_contribution` case-pack bug** (−522% margins) and the **check_canonical rate_map bug** (Kroger/Sprouts at 7%) — small SQL/Python fixes, large credibility exposure since both are in the public platform repo a technical reviewer will read first.
4. **Channel delta $91K** — recompute with real COGS before anyone with a P&L background reads `where-the-money-comes-from`.
5. **Remittance seam / 17¢ decomposition** — either reconcile generated remittances to itemized deductions or label the plug; this also upgrades the contract-to-cash story from "20% explained" to fully traced.
6. **Labels:** "backlog," recovery denominators, OTIF provenance, trade's two denominators, SKU percentile framing.
7. **Stale copy sweep:** ten-decisions ($461K, $33.13M, 86.5¢, $2.06M fines, 69.19% fill), trade-spend-diagnostic README ($32.5M scan, 9.2%, 864 chargebacks), WTMCF "FY2024–2026" window note.

### Cross-figure contradictions (found en route; some pairs pass individually)

1. **Three fulfillment universes**: platform 100% fill (and $76.1M cash collected) vs OTIF repo 95%/86% vs short-ship 69.35% — all cited as facts about one company.
2. **OTIF fines 5× apart**: ~$136K/yr (OTIF row) vs $690K/yr (short-ship dim 3), both ✅ in the canonical.
3. **A subset larger than its superset**: $458K/yr "data-attributable chargebacks" vs $310K/yr total chargebacks of all reasons (platform, both channels).
4. **Short-ship money on complete shipments**: $433K/36mo of `short_ship` deductions+chargebacks in a dataset whose shipments table shorts zero units.
5. **Channel economics span −619% to +90%** for the same channels: mart (case-pack bug) vs WTMCF (fabricated COGS) vs SKU repo (all-negative loaded margins) vs canonical EBITDA row (75.3% available for COGS+SG&A — itself fine vs catalog COGS of 46%).
6. **One file, two denominators**: 10.5% (scan) and 13.7% (invoiced) both describe the same $3.4M trade cost; nothing in the file says so.
7. **The canonical contradicts itself on the chargeback window**: the $458K note asserts "over 18 months" while the SUPERSEDES table declares "18 months" a dead value ("Always was 36 months").
8. **Synthetic uniformity fingerprints** (realism, not arithmetic): nine deduction types within ±2% of each other in count and $93–99 average (slotting averaging $96.72 per record is not how slotting works); late-delivery rates 70.9–71.7% across all six retailers; velocity spread 1.9×; DTC steady at 0.74% of revenue all three years. Any operator who has lived a deduction ledger will clock these in minutes; consider a dispersion pass in the seeders.

---

*Audit artifacts: replica certified via `check_canonical.py` (10/10 PASS) and `dbt build` (392/392 PASS) on 2026-06-12; query results archived at `%TEMP%\claude\cinderhaven-audit\audit_results*.json`. The local Docker replica can be discarded (`docker compose down`) or kept for spot-checks; the Fly.io SSOT was not touched.*

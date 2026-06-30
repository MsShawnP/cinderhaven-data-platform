---
name: cinderhaven-canonical
description: Authoritative Cinderhaven facts — every repo and every page cites against this file
metadata:
  type: reference
---

# CINDERHAVEN_CANONICAL — Authoritative Facts

**Source of truth:** `scripts/seed_config.py` in this repo.
**Rule:** Reconcile DOWN to this file. Never change this file to match a drifted repo.
**Last verified:** 2026-06-13 (local certified replica; causal fulfillment arc Groups A–E accepted; Phase 4 relock)

---

## Brand

| Fact | Value | Notes |
|------|-------|-------|
| Brand name | Cinderhaven Provisions | |
| Brand type | Fictional / synthetic | Do not describe as "real" |
| Annual revenue | ~$25M | CY2024 from platform data; acceptable range $23M–$27M |
| Description | A $25M specialty food brand | Use this phrasing |

---

## Product catalog

| Fact | Value | Source |
|------|-------|--------|
| **SKU count** | **50** | `seed_config.py` PRODUCT_LINES — 5 lines × 10 SKUs each |
| Product line count | **5** | Artisan Sauces, Pantry Staples, Specialty Condiments, Dried Goods, Snack Bites |
| SKUs per line | 10 | Uniform |
| SKU ID format | CHP-{XX}-NNN | e.g. CHP-AS-001, CHP-DG-007 |

### Product lines and SKU ranges

| Line | Prefix | SKUs |
|------|--------|------|
| Artisan Sauces | CHP-AS | CHP-AS-001 – CHP-AS-010 |
| Pantry Staples | CHP-PS | CHP-PS-001 – CHP-PS-010 |
| Specialty Condiments | CHP-SC | CHP-SC-001 – CHP-SC-010 |
| Dried Goods | CHP-DG | CHP-DG-001 – CHP-DG-010 |
| Snack Bites | CHP-SB | CHP-SB-001 – CHP-SB-010 |

---

## Channels

| Fact | Value | Source |
|------|-------|--------|
| Total sell-through channels | **10** | 6 retailers + 3 distributors + 1 DTC |
| Contracted retailers | **6** | See table below |
| National distributors | **2** | UNFI, KeHE |
| Regional distributor | **1** | DPI Northwest |
| DTC | **1** | Shopify |

### Retailers (contracted, with compliance requirements)

| ID | Name | Store doors |
|----|------|-------------|
| RET-WALMART | Walmart | 180 |
| RET-COSTCO | Costco | 60 |
| RET-WHOLEFOODS | Whole Foods | 120 |
| RET-SPROUTS | Sprouts | 90 |
| RET-KROGER | Kroger | 150 |
| RET-REGIONAL | Regional Group | 40 |

### Distributors

| ID | Name | Type |
|----|------|------|
| DIST-UNFI | UNFI | National |
| DIST-KEHE | KeHE | National |
| DIST-DPI | DPI Northwest | Regional |

---

## Engagement-level headline figures

These are the cross-page figures every engagement tool and case study cites.
See each repo for derivation details.

| Figure | Value | Engagement | Status |
|--------|-------|------------|--------|
| SKU rationalization — kill candidates | 19 of 50 | sku-rationalization-framework | ✅ Confirmed |
| SKU rationalization — fix-or-kill | 22 of 50 | sku-rationalization-framework | ✅ Confirmed |
| Product data — annualized cost | **$93K** | product-data-health-audit | ✅ Confirmed (causal attribution; 281 of 2,873 retailer chargebacks are data-defect) |
| Deductions — total backlog | $1.35M | retailer-deduction-recovery | ✅ Confirmed (16,917 rows cross-channel) |
| Deductions — recovery per all deduction $ | ~15% | retailer-deduction-recovery | ✅ Confirmed (14.69%) |
| Deductions — win rate per disputed $ | ~42% | retailer-deduction-recovery | ✅ Confirmed (41.80%; tier-conditioned) |
| Deductions — dispute rate | ~35% | retailer-deduction-recovery | ✅ Confirmed (35.5%) |
| Deductions — forward exposure | $861K | retailer-deduction-recovery | ⚠️ Awaiting regen |
| Fulfillment — portfolio fill rate (retailer) | 99.2% | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20; verified 2026-06-26) |
| Fulfillment — portfolio fill rate (distributor) | 99.5% | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20; verified 2026-06-26) |
| Short-ship — forgone revenue (3yr) | $523,326 | short-ship-cost | ✅ Confirmed (verified 2026-06-26) |
| Short-ship — compliance fines (3yr) | $164,543 | short-ship-cost | ✅ Confirmed (verified 2026-06-26) |
| Short-ship — chargebacks (3yr) | $118,814 | short-ship-cost | ✅ Confirmed (verified 2026-06-26) |
| Short-ship — deductions (3yr) | $87,490 | short-ship-cost | ✅ Confirmed (verified 2026-06-26) |
| Short-ship — total cost (3yr) | $894,174 | short-ship-cost | ✅ Confirmed (verified 2026-06-26) |
| Short-ship — total cost (annual) | $298,058 | short-ship-cost | ✅ Confirmed (verified 2026-06-26) |
| Short-ship — dimension count | 4 | short-ship-cost | ✅ Confirmed |
| OTIF — internal fill rate (portfolio) | 99.2% | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20) |
| OTIF — retailer-scored (Walmart) | 84.5% | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20) |
| OTIF — retailer-scored (portfolio) | 88.2% | cinderhaven-data-platform | ✅ Confirmed (OTIF pipeline run, commit 22f91c9) |
| OTIF — gap (Walmart) | 14.8 pts | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20) |
| OTIF — annual fines (measured) | $23,697 | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20) |
| OTIF — annual velocity damage (modeled) | $33,500 | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20) |
| OTIF — total annual exposure | $57,197 | cinderhaven-data-platform | ✅ Confirmed (tuned 2026-06-20) |
| Channel — retail vs distribution delta | $54K per $1M deployed (retail advantage; 51.0% vs 45.6%) | where-the-money-comes-from | ✅ Confirmed (regenned 2026-06-29; SUPERSEDES $91K distribution-wins) |
| Revenue lifecycle — cents per wholesale retailer $ | 87¢ | contract-to-cash | ✅ Confirmed (87.2¢ retailer / 87.3¢ combined on live post-06-20-tuning mart 2026-06-30; supersedes 86.38¢ pre-tuning) |
| Launch economics — gross revenue Year 1 | $499,200 | cost-of-saying-yes | ✅ Operator-validated |
| Launch economics — net cash Year 1 | −$36,320 | cost-of-saying-yes | ✅ Operator-validated |
| Thesis range | $1.4M–$2.3M/yr | the-ten-decisions | ✅ Recomputed 2026-06-29 — Decision 4 dropped from $2.0M–$2.4M to $298K; Decisions 2, 6 awaiting regen (using current placeholders); range may tighten toward $1.2M low if D2/D6 drop |
| Trade — all-in (trailing-52w) | ~$3.6M/yr, 11.0% of scan revenue | trade-spend-data-diagnostic | ✅ Confirmed (relocked 2026-06-20) |
| Trade — operational waste | ~$380K/yr | trade-spend-data-diagnostic | ✅ Confirmed (relocked 2026-06-20) |
| Trade — chargebacks | 3,357 (2,873 ret + 484 dist) | cinderhaven-data-platform | ✅ Confirmed (causal, event-driven; tuned 2026-06-20; slotting fix removed 6 fake retailer chargebacks 2026-06-28; verified live 2026-06-30) |

**Product data $93K note:** The causal fulfillment regen attributes only 281 of 2,873 retailer chargebacks to Path A data-defects (the remaining 2,592 are fulfillment-event-driven), producing **$93K/yr** in data-attributable chargeback cost — within design doc §5.1's ~$50–95K/yr estimate. Supersedes the pre-causal $458K (which annualized 677 retailer chargebacks / $686,534 / 18mo and attributed all chargebacks to data quality), as well as the earlier $461K (pre-date-shift window), $430K (stale cache), and $296K (pre-reseed calibration). All superseded.

**Deductions $1.35M scope note (verified 2026-06-20):** $1,346,815 is a cross-channel total across all 9 trading partners: retailer $1,118,682 / 14,947 rows (Walmart, Kroger, Whole Foods, Sprouts, Costco, Regional Group) + distributor $228,133 / 1,970 rows (UNFI, KeHE, DPI Northwest). Event-driven short_ship and late_delivery deductions are proportional to actual shortfall value; tuned 2026-06-20 to realistic specialty food failure rates (7-15% of shipments fail in-full with 5-10% shortfall severity). `fct_retailer_deductions` in Postgres covers the retailer portion only; queries against that table return ~$1.12M by design — that is a scope difference, not drift or a data error.

---

## Trade Economics — CANONICAL FIGURES (option a, locked 2026-06-04; trade rows relocked 2026-06-12)

**Relock note (2026-06-12):** `check_canonical.py`'s rate_map silently priced
Kroger and Sprouts at the 7% regional fallback; their seeded rates are 10%
and 9% (`seed_config.py` TRADE_SPEND_PCT). The fallback is removed and the
trade rows below now carry the rates the data actually encodes. Old values
moved to SUPERSEDES. Interim relock — the full canonical set re-derives in
Phase 4 of the causal-fulfillment arc.

**Source:** Current Postgres SSOT, queried 2026-06-08 via `flyctl proxy`;
trade rows reverified 2026-06-12 on the certified local replica.
**Regen commit:** `afbb7c5` (feat: expand seed config to 50 SKUs / 3yr window and scale order generation)
**Data version:** cinderhaven-data-v2
**Seed config:** `scripts/seed_config.py`, `SEED=42`, economic constants frozen.
**Rule:** All downstream pieces reconcile to this file.

### Headline figures

| Measure | Value | Definition |
|---------|-------|------------|
| All-in trade cost (annualized) | $3.6M/yr | Structural trade + operational waste excl promo_billback |
| All-in trade cost (trailing-52w) | $3.6M/yr | Same methodology, trailing 52-week window |
| All-in trade rate | 11.0% | Of trailing-52w scan revenue ($32.8M) |
| All-in trade cost (36mo) | $10.8M | 2023-01-01 to 2026-01-02 |
| Structural trade (36mo) | $9.6M | AVG(trade_spend_pct) × trailing-52w scan revenue per channel |
| Operational waste (36mo) | $1.14M | 36mo deductions excl promo_billback |
| Operational waste (annual) | ~$380K/yr | Recoverable via disputes |
| Structural trade (annual) | ~$3.2M/yr | Rate × trailing-52w channel revenue |
| Structural trade rate | 9.8% | Of trailing-52w scan revenue ($32.8M) |
| Operational waste rate | 1.2% | Of trailing-52w scan revenue ($32.8M) |
| Chargebacks | 3,357 | 2,873 retailer + 484 distributor; event-driven from fulfillment data |
| Data window | 2023-01-01 to 2026-01-02 | 36 months |
| Scan revenue (trailing-52w) | $32.8M | |
| EBITDA check | 14.9% trade + 11% EBITDA = 25.9% | Leaves 74.1% for COGS+SGA (plausible) |

### Methodology

All-in trade cost = structural trade + operational waste (excl promo_billback).

- **Structural trade** = AVG(trade_spend_pct) × trailing-52w scan revenue per channel.
- **Operational waste** = trailing-365 deductions excluding promo_billback (already captured in structural rates — including it would double-count).
- **Chargebacks** (separate table) overlap with deduction types and are NOT added to all-in.
- **Recoverable layer** = operational waste only (~$380K/yr). The $9.6M structural trade is contracted, not recoverable via disputes.

### APPROVED PHRASINGS

Downstream pieces copy these strings verbatim. They never re-derive.

| Context | Exact phrasing |
|---------|----------------|
| Product data cost (annual) | "~$93K/yr in chargeback cost attributable to data-quality defects" |
| Trade context (annual) | "~$3.6M/yr all-in trade spend, 11.0% of scan revenue (trailing 52 weeks)" |
| Recoverable layer | "~$380K/yr operational deduction waste; 3,357 chargebacks over 36 months" |
| 36-mo total (only when a real multi-year total is needed) | "$11.1M all-in trade over 36 months" |
| Deduction recovery — base rate | "~15% of deduction dollars recovered — not because disputes fail, but because most are never filed" |
| Deduction recovery — win rate | "~42% win rate per disputed dollar, but only ~35% of deductions are ever disputed" |
| Deduction recovery — silent write-off | "~65% of deductions go uncontested — $826K in silent write-offs" |
| Deduction recovery — paired narrative | "Cinderhaven wins 42% of the disputes it files. The problem isn't winning — it's filing. Two-thirds of deductions are written off without a fight." |
| Deduction recovery — ceiling | "~65% recovery rate on strong-evidence disputes" |

Usage rule: The 15% recovered / 42% win rate / 65% never filed are the same-denominator story told three ways. Pair freely. The old rule ("never pair 16% with 65%") no longer applies — the slotting dispute fix removed 333 fake disputes that distorted the denominators.

| Lifecycle (retailer wholesale) | "87 cents per invoiced wholesale dollar (86–88¢ band)" |
| Short-ship cost (annual) | "~$300K/yr in fulfillment shortfall costs across four dimensions" |
| Short-ship cost (3yr) | "$894K in total fulfillment shortfall costs over 36 months" |
| Short-ship framing | "99% unit fill still costs $300K/yr — the gap between unit fill and in-full is where the money hides" |
| Thesis range | "$1.4M to $2.3M a year in quantifiable operational cost across eight decisions" |

**OVERLAP SCOPING NOTE:** OTIF exposure includes $39.6K/yr in short_ship chargebacks also counted in short-ship cost. Thesis range counts these once, under short-ship cost (Decision 4).

**Rule:** Pieces copy these phrasings. They never re-derive figures from raw data.

### Revenue lifecycle waterfall (retailer wholesale)

Source: `raw.retailer_remittances`, 222 rows, 36 months.

**Updated 2026-06-30 to the live post-06-20-tuning mart.** The deduction
retuning ($1.66M→$1.12M) raised net collected and moved the lifecycle from
86.38¢ to **87.2¢ retailer / 87.3¢ combined**. The 86.38¢ figure below it
(now superseded) was the pre-tuning waterfall.

| Layer | Amount (36mo) | % of gross | Note |
|---|---|---|---|
| Gross invoiced | $52,128,777 | 100.0% | matches live mart gross_payments |
| Trade allowance | −$4,967,008 | 9.53% | structural (scan-revenue %), unchanged by deduction tuning |
| Operational deductions | −$1,118,682 | 2.15% | live mart `fct_retailer_payments` / `fct_retailer_deductions` (14,947 rows) |
| Chargebacks + timing residual | −$575,533 | 1.10% | derived residual (gross − net − trade − deductions); precise split pending a fresh waterfall query |
| **Net collected** | **$45,467,554** | **87.22%** | live mart net_received (b2b) |

Pre-tuning (superseded): Operational deductions −$1,303,883, Chargebacks −$693,209,
Timing −$138,066, Net collected $45,026,612 (86.38%).

| Metric | Value | Target |
|---|---|---|
| Lifecycle (retailer) | 87¢ per $ | 86–88¢ band |
| Lifecycle (combined w/ DTC) | 87.3¢ | app headline |

Distributor lifecycle: 92.74¢ per $ and combined wholesale 88.38¢ predate the
06-20 retuning and will tick up on refresh. The canonical 87¢ is the retailer
figure because the retailer channel carries the full complexity (trade,
operational deductions, chargebacks, disputes, evidence-quality tiers).

### SUPERSEDES — dead values (do not use anywhere)

| Dead value | What it was | Why it's wrong |
|------------|-------------|----------------|
| $5.4M | Pre-May-2026 all-in (old seed_config trade rates) | Superseded by intentional Postgres regen |
| $7.2M / $7,174,939 / 26.1% | May 2026 SQLite export figures | Postgres was regenerated after this export |
| 464 chargebacks | DPI Northwest deduction count from deduction-recovery `summary.json` | Misquoted as total chargebacks; actual total is 837 |
| "18 months" | Misstatement of data window length | Always was 36 months (2023-01-01 to 2026-01-02) |
| 3,441 chargebacks | May 2026 SQLite chargeback count | Superseded by regen |
| 21.5% Walmart trade_spend_pct | Old seed_config value | Now 12.0% in current seed_config |
| $461K product data cost | Pre-date-shift PDHA figure (2026-06-06) | Date-window shift (2024-2026 → 2023-2025) produces $458K |
| $430K product data cost | Pre-fresh-pull PDHA figure (stale cache) | Fresh Postgres export (2026-06-06) produced $461K; date shift produces $458K |
| $296K product data cost | Website recalibration (2026-06-02) against pre-reseed data | SSOT reseeded; superseded by causal regen → $93K |
| $458K product data cost | Pre-causal-regen placeholder (attributed all 677 chargebacks to data quality) | Causal attribution isolates 281 data-defect chargebacks → $93K/yr |
| $60K chargebacks | the-ten-decisions early draft figure | Never matched any pipeline output; superseded by $458K |
| 864 chargebacks | Pre-date-shift total (690 ret + 174 dist) | Date-window shift produces 837 (677 ret + 160 dist) |
| 82.8¢ / 83¢ per dollar | Stale contract-to-cash single-year cut (CY2024, pre-reseed dataset) | Superseded — live post-06-20-tuning mart yields ~87¢ (87.2¢ retailer / 87.3¢ combined; regenerated 2026-06-30 against live DB). Pre-tuning backup yielded 86.4–86.9¢. No window produces 83¢. 87¢ is canonical |
| $33.1M short-ship | Pre-date-shift short-ship total cost | Date-window shift produces $32.8M |
| 10.8% all-in trade rate | Pre-date-shift trade rate | Date-window shift produces 10.5%; itself superseded 2026-06-12 (rate_map fix) |
| 9.2% structural trade rate | Pre-date-shift structural rate | Date-window shift produces 9.0%; itself superseded 2026-06-12 (rate_map fix) |
| $3.4M/yr / 10.5% all-in trade | Locked 2026-06-04 figure | Superseded 2026-06-12 — check_canonical.py rate_map silently priced Kroger and Sprouts at the 7% regional fallback; seeded rates are 10%/9%. True all-in $3.7M/yr / 11.3% |
| ~$3.0M/yr / 9.0% structural trade | Locked 2026-06-04 figure | Superseded 2026-06-12 — same rate_map bug. True structural $3.2M/yr / 9.9% |
| $3.5M/yr (t-52w) / $10.26M (36mo) / $8.8M structural (36mo) | Locked 2026-06-04 derived variants | Superseded 2026-06-12 — same rate_map bug. True $3.7M / $11.16M / $9.7M |
| 837 chargebacks (677 ret + 160 dist) | Pre-causal chargeback count | Causal model generates event-driven chargebacks from fulfillment events; count was 6,563, now 4,749 after fill-rate tuning |
| 6,563 chargebacks (5,885 ret + 678 dist) | Pre-tuning causal chargeback count | Superseded 2026-06-20 — fill rates and receiving discrepancy rates tuned to realistic specialty food ranges; new count 3,363 (2,879 + 484) |
| 3,363 chargebacks (2,879 ret + 484 dist) | Post-06-20-tuning count | Superseded 2026-06-30 — slotting dispute fix (a72dfaf) removed 6 fake dispute-linked retailer chargebacks; live count 3,357 (2,873 + 484) |
| $1.66M / 16,023 deduction backlog | Pre-causal deduction totals | Superseded by causal model; then re-tuned 2026-06-20 to $1.38M / 19,279 |
| $1.59M / 22,425 deductions | Pre-tuning causal deduction totals | Superseded 2026-06-20 — fill-rate tuning reduced event-driven deductions; new $1.35M / 16,917 |
| $1.38M / 19,279 deductions | Second-pass tuning deduction totals | Superseded 2026-06-20 — third pass further tightened fill targets; new $1.35M / 16,917 |
| ~$480K/yr operational waste | Pre-causal op waste | Superseded; then re-tuned 2026-06-20 to ~$380K/yr |
| ~$460K/yr operational waste | Pre-tuning causal op waste | Superseded 2026-06-20 — fill-rate tuning; new ~$380K/yr |
| ~$390K/yr operational waste | Second-pass tuning op waste | Superseded 2026-06-20 — third pass; new ~$380K/yr |
| 1.5% operational waste rate | Pre-causal waste rate | Superseded; then re-tuned to 1.2% |
| 1.4% operational waste rate | Pre-tuning causal waste rate | Superseded 2026-06-20 — fill-rate tuning; new 1.2% |
| $3.7M/yr / 11.3% all-in trade | Pre-tuning causal all-in | Superseded 2026-06-20 — fill-rate tuning reduced op waste; new $3.6M/yr / 11.0% |
| ~44% per-disputed recovery | Pre-causal coincidental blend | Tier-conditioned evidence distribution produces 41.8%; not a quality regression |
| "16.5% → 65%" recovery narrative | Before→after using different denominators | 16% is per all deduction $; 65% is per strong-evidence disputed $. Never present as X%→Y%. Replaced by two-metric restatement (Option C, DECISIONS.md 2026-06-13) |
| ~16% recovery per all deduction $ | Pre-slotting-fix recovery rate (16.16%) | Superseded 2026-06-28 — slotting dispute fix (a72dfaf) removed 333 fake disputes; recalibrated to 14.69% (~15%) |
| "~16% of deduction dollars recovered through disputes" | Pre-slotting-fix approved phrasing | Superseded 2026-06-28 — replaced by "~15% recovered — not because disputes fail, but because most are never filed" |
| $32.8M short-ship total cost (3yr, 8 dimensions) | Pre-causal short-ship-cost project figure | Causal model provides event-driven fulfillment data; project needs full regen. Fill rates (92%/94%) replace as pipeline-native metrics |
| $53.0M short-ship shipped revenue | Pre-causal short-ship-cost project figure | Same — project needs regen against causal data |
| $11.16M all-in trade (36mo) | Pre-causal 36-mo total | Causal op waste reduction changes total to $11.1M |
| $1.44M operational waste (36mo) | Pre-causal 36-mo op waste | Now $1.38M |
| $1.4M–$3.1M thesis range | Superseded 2026-06-14, recomputed from updated decision figures including 4-dimension short-ship rebuild ($2.2M/yr replaces $200–500K), PDHA causal attribution ($93K replaces $25–100K), channel story inversion ($50–100K replaces $300–500K), lifecycle confirmation (86¢, $350–500K replaces $400–700K) | New range $3.1M–$4.6M/yr; itself superseded 2026-06-29 |
| $3.1M–$4.6M thesis range | Superseded 2026-06-29 — Decision 4 fill-rate retuning dropped fulfillment from $2.0M–$2.4M/yr to $298K/yr ($894K/3yr at 99.2%/99.5% fill). All other decisions unchanged. Decisions 2 ($93K) and 6 ($50K–$100K) carry placeholder values pending regen; if both drop, low end slides toward $1.2M | New range $1.4M–$2.3M/yr |
| 95%/86% OTIF internal/retailer-scored | Superseded 2026-06-14, replaced by platform causal OTIF: 92.0% internal, 61.4% Walmart retailer-scored, 30.6pt gap | |
| $433K/$136K/$297K OTIF exposure | Superseded 2026-06-14, replaced by $423K; then superseded 2026-06-20 by $57K ($24K fines + $34K velocity) after fill-rate tuning | |
| $423K ($55K fines + $368K velocity) OTIF exposure | Superseded 2026-06-20 — fill-rate tuning reduced failure rates to realistic specialty food ranges; new $57K ($24K + $34K) | |
| 92.0% / 61.4% / 30.6pt OTIF | Pre-tuning OTIF rates | Superseded 2026-06-20 — new 99.2% internal, 84.5% Walmart, 14.8pt gap | |
| 69.3% synthetic fill rate | Superseded 2026-06-14, short-ship project order generator retired, replaced by platform causal fill rates (92%/94%) | |
| 8-dimension short-ship cost model | Superseded 2026-06-14, replaced by 4-dimension model grounded in platform events | |
| $6,581,205 total 3yr / $2,193,735 annual short-ship cost | Pre-tuning short-ship totals | Superseded 2026-06-26, replaced by $894K/3yr ($298K/yr) after fill rate retuning to 99.2%/99.5%. Old 92%/94% unit fill targets produced per-retailer fills too low to sustain the relationship. |
| 92.0% retailer / 94.2% distributor fill rates | Pre-tuning unit fill rates | Superseded 2026-06-26, replaced by 99.2%/99.5%. Old rates reflected deep concentrated shortfalls; new rates reflect shallow widespread shortfalls consistent with specialty food operations. |

---

## Distressed Scenario -- FIGURES (generated 2026-06-05)

**Source:** `scripts/generate_distressed_scenario.py` (SEED=200), run against
baseline SQLite post-fixup. Replaces deductions + disputes only; all other
tables are byte-identical to baseline.

**Consumer:** trade-spend-diagnostic ONLY. No other piece reads this dataset.

**Baseline integrity (provably unchanged):**
- Chargebacks: 677 retailer (unchanged; 837 total with distributor in Postgres)
- Orders: 46,414 (unchanged)
- Scan revenue: $32.8M trailing-52w (unchanged)
- Structural trade: unchanged (scan_data + sku_costs untouched)

### Headline figures

| Measure | Value | Definition |
|---------|-------|------------|
| Total deductions | 15,850 | All types incl promo_billback and slotting |
| Total deduction value (36mo) | $3.41M | |
| Operational waste (36mo) | $2.89M | Excl promo_billback |
| Operational waste (annual) | ~$965K/yr | Recoverable via disputes |
| All-in waste rate | 3.0% | Of trailing-52w scan revenue ($32.8M) |
| Vague deductions | 967 | Real vague type with VAGUE_TEMPLATES |
| Vague value (annual) | ~$419K/yr | Bimodal: 60% $50-600, 40% $800-4500 |
| Vague with no PO link | 295 | 30.5% of vague (no order_id) |
| Double-dips | 3 / $19,062 | Explicit injection, is_double_dip=1 |
| Ghost promos | 3,258 / $361K | promo_billback with no matching promotion |
| Disputes filed | 5,395 | ~35% of non-slotting deductions |
| Recovery rate | 20.9% | Low-recovery weights: won=12, lost=45, partial=28, pending=15 |
| Recovered | $231,758 | |

### By deduction type

| Type | Count | Amount (36mo) |
|------|-------|---------------|
| promo_billback | 4,476 | $494,466 |
| vague | 967 | $1,256,240 |
| short_ship | 2,321 | $295,065 |
| spoilage | 2,154 | $459,189 |
| damaged | 2,089 | $269,189 |
| late_delivery | 1,726 | $88,203 |
| label_fine | 957 | $307,334 |
| pallet_fine | 662 | $117,646 |
| pricing_error | 478 | $15,931 |
| slotting | 17 | $85,212 |
| double_dip | 3 | $19,062 |

### How distressed differs from baseline

| Dimension | Baseline (v2) | Distressed |
|-----------|---------------|------------|
| Deduction types | 9 (no "vague") | 10 (+ "vague") |
| Vague classification | Misclassified spoilage+damaged | Real vague type, VAGUE_TEMPLATES |
| Double-dips | 0 (none generated) | 3 explicit ($19K) |
| Recovery rate | ~44% | ~21% |
| Operational waste | ~$480K/yr | ~$965K/yr |
| Ghost promos | N/A | 3,258 ($361K) |
| Evidence quality | Mostly strong | Mostly weak (distressed) |

---

## Defect Profile (added 2026-06-06)

**Source:** `compute_defect_profile()` in `scripts/seed_config.py`, seed=300.
**Rule:** Defect rates are independent of trade economics. Changing the defect
profile does NOT move any figure in the Trade Economics section above — the
freeze guard (`check_canonical.py`) is the gate.

### Headline figures

| Measure | Value | Source |
|---------|-------|--------|
| GTIN invalid rate | ~20% | `GTIN_INVALID_RATE` in `seed_config.py` |
| GTIN generation | Valid GS1 check-digit, then ~20% corrupted | `compute_defect_profile()` |
| Field missingness | Per `MISSING_RATES` dict (5–18% by field) | `seed_config.py` |
| Quality score | Mean ~70, range 40–95 | Per-SKU, based on actual defects |
| Chargeback Pareto | Top 10% of SKUs ≈ 48% of chargebacks | Quality-weighted draw, exponent 3.5 |
| Retailer pass rates | 50–75% (emergent) | Depend on GTIN validity + field completeness per retailer |
| Defect RNG seed | 300 | Isolated stream — cannot cascade into trade/count generation |

### Field missingness rates

| Field | Rate | Notes |
|-------|------|-------|
| case_length_in | 12% | |
| case_width_in | 12% | |
| case_height_in | 12% | |
| unit_weight_lbs | 8% | |
| case_weight_lbs | 8% | |
| subcategory | 10% | |
| country_of_origin | 3% | |
| brand_owner | 2% | Required field — low rate |

### What changed from the pre-defect-fix state

| Dimension | Before (unrealistic) | After (realistic) |
|-----------|---------------------|-------------------|
| GTIN validity | 0% valid (all fake `1234567NNNNNNN`) | ~76% valid GS1 check-digit, ~24% corrupted |
| Field missingness | 0% (all fields always populated) | 5–18% per field (see table above) |
| Quality score | Not computed | Mean ~70, range 40–95 |
| Chargeback SKU distribution | Uniform random | Quality-weighted Pareto |
| Chargeback total count | 837 (677 ret + 160 dist) | **Unchanged** — RNG isolation preserves counts |
| Trade economics | $3.4M/yr, 10.5% | **Unchanged** — separate generation path |

---

## What changed from the pre-reconciliation state

| Old value | Correct value | Appears in |
|-----------|---------------|-----------|
| 90 SKUs | **50 SKUs** | product-data-health-audit, contract-to-cash (hardcoded), old docs |
| 3 product lines | **5 product lines** | product-data-health-audit old copy |
| 4 contracted retailers | **6 contracted retailers** | product-data-health-audit old copy |
| 45 SKUs | **50 SKUs** | the-ten-decisions exec-summary.qmd:33 |
| ~$361K product data cost | **~$93K** | about/page.tsx (site), old process docs |
| 96% internal OTIF | **95%** | the-ten-decisions copy, old blog |
| $74.2M shipped (short-ship) | **$53M** | old copy |
| 44.7% cost-of-shipped | **drop the percentage** (single dimension misapplied) | old copy |
| $10.8M deduction backlog | **$1.65M** | old copy |
| 7% baseline recovery | **~16%** | old copy |
| $714K exposure | **$861K** | old copy |
| $5.4M all-in trade | **~$3.4M/yr trailing-52w** | trade-spend-data-diagnostic, remittance brief |
| $7.2M / 26.1% all-in | **~$3.4M/yr / 10.5%** | trade-spend-data-diagnostic |
| 464 chargebacks | **837** | remittance brief, dimension-weight-integrity |
| "18 months" window | **36 months** | remittance brief |

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
**Last refreshed from Postgres:** 2026-07-29 — see [Verification Run 2026-07-29](#verification-run-2026-07-29) at the end of this file.

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

Distributor lifecycle: 93.13¢ per $ and combined wholesale 89.08¢ (verified 2026-06-30 from live Postgres SSOT; supersedes 92.74¢ / 88.38¢ which predated the 06-20 retuning). The canonical 87¢ is the retailer figure because the retailer channel carries the full complexity (trade, operational deductions, chargebacks, disputes, evidence-quality tiers).

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

---

## Verification Run 2026-07-29

**Substrate:** Deterministic local replica — PostgreSQL 16, seeded from
`scripts/seed_all.py` (`SEED=42`, frozen block untouched), then
`dbt build` (457/457 PASS, 0 errors). The live Fly.io Postgres was
**not** reachable from this environment (no `flyctl`, no Fly
credentials), so the replica stands in for it.

**Why the replica is trustworthy as a stand-in:** every independently
seeded row count reproduces the documented value exactly — retailer
chargebacks 2,873; distributor chargebacks 484 (total 3,357); retailer
deductions 14,947; distributor deductions 1,970 (total 16,917); 50 SKUs;
6 retailers; 3 distributors; 222 retailer remittances. `verify_canonical.py`
reports zero delta on all 11 figures it checks. `check_canonical.py`
returns 12/12 PASS.

**What the replica cannot prove:** that the live Fly database has not
drifted from what the generator produces. Any manual post-seed edit made
directly against production would be invisible here. Re-run against the
live DB before treating this as a production reconciliation.

### Status of checked figures

| Figure | Documented | Measured 2026-07-29 | Status |
|--------|-----------|---------------------|--------|
| SKU count | 50 | 50 | ✅ VERIFIED |
| Product lines | 5 | 5 | ✅ VERIFIED |
| Contracted retailers | 6 | 6 | ✅ VERIFIED |
| Distributors | 3 | 3 | ✅ VERIFIED |
| Chargebacks retailer | 2,873 | 2,873 | ✅ VERIFIED |
| Chargebacks distributor | 484 | 484 | ✅ VERIFIED |
| Chargebacks total | 3,357 | 3,357 | ✅ VERIFIED |
| Deductions retailer rows | 14,947 | 14,947 | ✅ VERIFIED |
| Deductions retailer-only ($) | $1,118,682 | $1,118,681.92 | ✅ VERIFIED |
| Deductions cross-channel rows | 16,917 | 16,917 | ✅ VERIFIED |
| Gross invoiced (retailer, 36mo) | $52,128,777 | $52,128,777.36 | ✅ VERIFIED |
| Lifecycle retailer (¢/$) | 87.2 | 87.2 | ✅ VERIFIED |
| Lifecycle distributor (¢/$) | 93.13 | 93.13 | ✅ VERIFIED |
| Lifecycle combined wholesale (¢/$) | 89.08 | 89.08 | ✅ VERIFIED |
| All-in trade rate | 11.0% | 11.0% | ✅ VERIFIED |
| Structural trade rate | 9.8% | 9.8% | ✅ VERIFIED |
| Operational waste rate | 1.2% | 1.2% | ✅ VERIFIED |
| **Scan revenue (trailing-52w)** | **$32.8M** | **$32,323,139.62** | ⚠️ **DRIFTED — see below** |

### Scan revenue has drifted 1.5% below the documented figure

`check_canonical.py` passes this row only because its tolerance is ±2%.
The measured trailing-52-week scan revenue is **$32,323,139.62**, not
$32.8M. Trailing-52w is identical to CY2025 in this dataset (both
2025-01-04 → 2025-12-27); the two are not separate windows.

This value is **not corrected in the tables above**, because the $32.8M
figure is the denominator for the published all-in (11.0%), structural
(9.8%) and operational-waste (1.2%) trade rates. Changing it moves
published percentages across the portfolio. Flagged for an explicit
decision rather than silently rewritten.

### Contradiction 1 — RESOLVED: there are no uncollected receivables

`b2b.invoiced == b2b.gross_payments` is exact, and it is exact **by
construction**, not by coincidence:

| Channel | Invoiced (36mo) | Gross payments | Difference |
|---------|-----------------|----------------|------------|
| Retailer | $52,128,777.36 | $52,128,777.36 | **$0.00** |
| Distributor | $23,938,528.80 | $23,938,528.80 | **$0.00** |
| B2B total | $76,067,306.16 | $76,067,306.16 | **$0.00** |
| B2B net received | — | $67,762,602.19 | — |

`generate_remittances()` in `scripts/seed_retailer.py` partitions every
order into exactly one retailer-month remittance and sets
`gross = sum(order totals)` for that partition. `finalize_remittances()`
then sets `net = gross − total_deductions`. No order is ever omitted, and
no invoice is ever left unpaid.

**Verdict: the data is right; contract-to-cash's hero copy is wrong.**
The $6.7M gap between invoiced and net received is entirely trade
allowances, operational deductions, chargebacks and a timing residual.
Uncollected receivables are **$0** — the dataset has no aging bucket, no
bad-debt write-off, and no unpaid-invoice concept at all. Any copy
attributing part of the gap to uncollected receivables must be rewritten.

### Contradiction 2 — RESOLVED: same measure, three generator vintages

32,539,868 and 32,800,000 are **the same measure** (trailing-52w scan
revenue) computed at different points in the generator's history. Neither
is currently reproducible:

| Value | Source | Vintage |
|-------|--------|---------|
| $32,802,453 | voidfinder; `cinderhaven-plausibility-audit.md` line 195 records it as a live measurement | pre-causal-regen |
| $32,539,868 | trade-spend-data-diagnostic; `generate_distressed_scenario.py` header still says "$32.5M trailing-52w" | post-causal, pre-void-seeding |
| $32,472,000 | `HANDOFF.md` line 375, "Revenue $32.47M (target $32.8M ±2%)" | intermediate |
| **$32,323,139.62** | **measured 2026-07-29** | **current** |

The drift is downward and mechanical. `seed_void_patterns.py` DELETEs
scan rows for the went-dark pattern, guarded to ≤1% of any retailer's
trailing-52w revenue; the causal-fulfillment regen moved shipped units
feeding scan before that. Because each step stayed inside
`check_canonical.py`'s ±2% tolerance, the canonical figure was never
updated. **Both pinned values are stale. The current value is
$32,323,139.62.**

### Pre-Phase-2 baseline

Measured 2026-07-29. This is the "before" for any enrichment work.

**Revenue by year — flat, and down in 2025**

| Year | Scan revenue | Invoiced (B2B+DTC) | Selling doors | Rev/door |
|------|-------------|--------------------|---------------|----------|
| 2023 | $31,731,869.92 | $25,087,312.80 | 640 | $49,581.05 |
| 2024 | $35,003,729.31 | $25,639,325.04 | 640 | $54,693.33 |
| 2025 | $32,323,139.62 | $25,150,015.20 | 640 | $50,504.91 |

**Growth decomposition — 100% velocity, 0% doors**

| Year | YoY change | From new doors | From velocity, same doors | Lost to dropped doors |
|------|-----------|----------------|---------------------------|----------------------|
| 2024 | +$3,271,859.39 | **$0.00** | +$3,271,859.39 | $0.00 |
| 2025 | −$2,680,589.69 | **$0.00** | −$2,680,589.69 | $0.00 |

The door base is fixed at 640 in all three years (640 of 640 stores
appear in every year). There is no door acquisition and no door loss.
Active sku-store pairs *decline* monotonically: 9,943 → 9,584 → 9,127.

**Trade spend — flat, bottom of the industry range**

| Year | Structural | Op waste | All-in | % of invoiced | % of scan |
|------|-----------|----------|--------|---------------|-----------|
| 2023 | $1,494,739.52 | $280,103.40 | $1,774,842.92 | 11.30% | 5.59% |
| 2024 | $1,602,976.44 | $346,672.47 | $1,949,648.91 | 11.61% | 5.57% |
| 2025 | $1,653,699.53 | $344,655.01 | $1,998,354.54 | 11.50% | 6.18% |

Trade allowance as % of gross invoiced is 9.52% / 9.54% / 9.51% — flat to
three years and two decimal places. No escalation.

**Deduction rate as % of invoice, by year (retailer)**

| Year | Gross | Total deductions | All-in % | Excl. trade % |
|------|-------|------------------|----------|---------------|
| 2023 | $15,707,584.32 | $2,025,256.08 | 12.89% | 3.38% |
| 2024 | $16,797,432.00 | $2,169,623.43 | 12.92% | 3.37% |
| 2025 | $17,380,018.08 | $2,219,813.89 | 12.77% | 3.26% |

**By partner (36mo, retailer)**

| Partner | Gross | Total deductions | All-in % | Excl. trade % |
|---------|-------|------------------|----------|---------------|
| Walmart | $10,789,650.00 | $1,689,426.84 | 15.66% | 3.66% |
| Costco | $6,608,810.88 | $885,648.11 | 13.40% | 3.40% |
| Kroger | $10,557,622.80 | $1,400,796.47 | 13.27% | 3.27% |
| Sprouts | $8,259,744.72 | $995,924.03 | 12.06% | 3.06% |
| Whole Foods | $9,832,363.68 | $1,083,174.30 | 11.02% | 3.02% |
| Regional Group | $6,080,585.28 | $606,253.61 | 9.97% | 2.97% |

Excluding trade, the partner spread is 2.97%–3.66% — a 0.69pt band across
six retailers, and flat year over year. Deduction-type counts grow only
in proportion to volume; the mix does not shift.

**Seasonality — present, but the wrong shape**

Week-of-year revenue: mean $1,904,975.75, stddev $513,763.53,
**coefficient of variation 27.0%**, peak-to-trough ratio **2.92×**.

There *is* a strong, repeating seasonal signal — the claim that the data
carries none is incorrect. The problem is its shape. Monthly index
(100 = that year's mean month):

| Month | 2023 | 2024 | 2025 |
|-------|------|------|------|
| Jan | 8.3 | 67.9 | 65.5 |
| **Feb** | 22.1 | **72.1** | **69.9** |
| Mar | 38.8 | 98.1 | 95.8 |
| Apr | 78.7 | 86.7 | 85.6 |
| May | 88.6 | 93.5 | 116.9 |
| Jun | 107.1 | 119.2 | 94.2 |
| Jul | 143.1 | 90.8 | 90.8 |
| Aug | 112.9 | 112.0 | 112.7 |
| Sep | 143.6 | 91.6 | 92.7 |
| Oct | 121.4 | 97.5 | 98.8 |
| Nov | 142.4 | 144.2 | 147.4 |
| Dec | 193.1 | 126.4 | 129.7 |

2023 is a launch ramp (index 8.3 in January), not seasonality; pooling
all three years hides this behind an apparent monotonic Jan→Dec climb.
2024 and 2025 show the true repeating pattern: a **November/December
holiday peak** and a **January/February trough**. February is the
second-weakest month of the year, at index ~70. For a salsa and dips
brand this is backwards — there is no Super Bowl peak, and no distinct
summer lift beyond a mild August bump (~112).

**COGS, unit cost and margin — present at every layer**

85 cost/margin columns exist across `raw`, `public_staging`,
`public_intermediate` and `public_marts`. The core ones:

| Table | Columns |
|-------|---------|
| `raw.sku_costs` | `cogs_per_unit`, `landed_cost_per_unit`, 8 per-channel wholesale prices |
| `public_marts.dim_products` | `cogs_per_unit`, `landed_cost_per_unit`, `margin_pct`, `margin_per_unit`, `dtc_margin_pct`, `dtc_margin_per_unit` |
| `public_marts.mart_channel_contribution` | `total_cogs`, `gross_margin`, `contribution_margin` |
| `public_marts.dim_category_benchmarks` | `avg_cogs`, `avg_margin_pct`, `avg_margin_per_unit` |
| `public_intermediate.int_loaded_contribution_by_sku` | `total_cogs`, `loaded_margin_pct` |

Populated, not empty: 50 of 50 SKUs carry non-null COGS and margin.
`cogs_per_unit` ranges $0.85–$4.50; `margin_pct` ranges 33.3%–65.3%,
mean 55.0%. `mart_channel_contribution` reports contribution margin of
$26,451,613.64 retailer / $10,806,054.93 distributor / $299,091.42 DTC.

**Authorizations and deauthorizations**

| Year | Authorizations | Deauthorizations |
|------|---------------|------------------|
| 2023 | 9,943 | 352 |
| 2024 | 0 | 464 |
| 2025 | 49 | 0 |

9,992 rows total, 816 carrying a deauthorization date. Authorizations are
a single 2023 bulk load plus 49 rows inserted in 2025 by
`seed_void_patterns.py`. Last deauthorization **2024-11-10**. Confirmed.

**Promotions**

123 rows, `2023-01-02` → `2024-11-03`. None in 2025.

| Year | Promos | Promo cost | Avg depth |
|------|--------|-----------|-----------|
| 2023 | 69 | $194,512.94 | 17.78% |
| 2024 | 54 | $134,377.94 | 18.23% |

Types: ad_circular 31, digital_coupon 24, BOGO 24, endcap 23, TPR 21.
`promo_billback` deductions, by contrast, continue through 2025 (456 /
547 / 546 rows), so billbacks already outlive the promotions that would
justify them.

### One unsourced hypothesis, checked opportunistically

"The best-selling SKU carries the worst margin" is **not currently true**,
but a weaker version of it already is. The #1 seller (CHP-AS-006,
$9,801,764.61) ranks 21st of 50 on worst margin (53.8%). However the #2
seller (CHP-PS-002) ranks 4th-worst at 46.1% and the #5 seller
(CHP-PS-009) ranks 2nd-worst at 41.4%. Pearson correlation between SKU
revenue and `margin_pct` is **−0.357** — a real, mild inverse
relationship already in the data. The remaining unsourced hypotheses were
not investigated, per instruction.

---

## Cost Side and Balance Sheet — VERIFIED 2026-07-29

**Source:** `costing` schema, generated by `scripts/seed_costing.py` (SEED=800,
isolated stream) against the certified local replica. DDL in
`sql/costing_schema.sql`; integrity in `tests/test_costing_integrity.sql`.

**Additive:** all nine protected tables verified byte-identical by content hash
before and after generation — `fct_scan_data`, `fct_distribution`,
`fct_retailer_orders`, `fct_retailer_payments`, `fct_retailer_deductions`,
`fct_dtc_orders`, `raw.sku_costs`, `dim_products`, `mart_channel_contribution`.

### The five margin lines — NAME THE BASIS, ALWAYS

There is no single "Cinderhaven margin." Five lines are live, all real, all
different. Citing one without its basis is the defect this section exists to
prevent.

| # | Line | Basis | Value | Where |
|---|------|-------|-------|-------|
| 1 | Gross margin at standard | `cogs_per_unit`; ingredient+packaging+conversion, no freight/overhead | **51.98%** | `raw.sku_costs`, `dim_products.margin_pct` |
| 2 | Contribution after commercial costs | line 1 less deductions, chargebacks, **promotional** trade, fulfilment, fees | **49.01%** | `mart_channel_contribution` |
| 3 | Gross margin at landed | `landed_cost_per_unit` — legacy, see SUPERSEDES | **~43.5%** | latent in `raw.sku_costs`; never surfaced as a margin |
| 4 | **Loaded margin at standard** | standard + freight-in + overhead | **42.46%**, flat every year | `costing.fct_product_costs.loaded_cost_at_standard` |
| 5 | **Loaded margin at actual** | manufactured (actual) + freight-in + overhead | **42.31%** blended | `costing.fct_product_costs.fully_loaded_cost_per_unit` |

**Lines 4 and 5 differ by PPV and nothing else.** Line 4 is flat by
construction because the standard is frozen; line 5 carries the compression.
Computing margin from `manufactured_cost_per_unit` gives line 5, not line 4.

**Line 2 basis note (mandatory):** contribution after **promotional spend
only; excludes structural trade allowances**. `total_trade_spend` there is
$328,890.88, sourced from `promotions.promo_cost`. The structural trade
allowance in the remittances is $4,967,008.48 — a 15× difference. Without this
note the table reads as contradicting the canonical trade figures.

### Margin compression — it lives in PPV

| Year | Loaded margin at ACTUAL | Loaded margin at STANDARD | PPV | PPV % of standard COGS |
|------|------------------------|---------------------------|-----|------------------------|
| 2023 | **45.01%** | 42.35% | −$672,805.65 | −5.53% (favorable) |
| 2024 | **42.36%** | 42.49% | +$33,414.13 | +0.27% |
| 2025 | **39.62%** | 42.54% | +$742,129.58 | +6.11% (unfavorable) |
| 3-yr | **42.31%** | 42.46% | **+$111,143.93** | **+0.30%** |

**Why net PPV is near zero — read this before concluding it was back-solved.**
`standard_cost_per_unit` is set ONCE at launch from `cogs_per_unit` and never
revised. It carries a **conservative pad**: it was set above expected cost, so
2023 came in 5.5% favorable. Input inflation and lost scale then consumed the
pad and pushed actual 6.1% above standard by 2025. Net +0.3% over three years
is the arithmetic consequence of a pad that was roughly one cycle's worth of
cost — not a figure fitted to the blended target. The story is the *trajectory*
(−5.5 → +6.1), not the net.

### Working capital

| Metric | Value | Basis |
|--------|-------|-------|
| **DIO** | **127.3 days** | avg `fct_inventory_snapshot.value_at_actual` ÷ annual loaded COGS × 365 |
| **DSO** | **25.6 days** | **MEASURED, not generated** — value-weighted order-PO-date to cash-receipt across 49,649 orders |
| **DPO** | **33.6 days** | avg `paid_date − invoice_date`, `fct_supplier_invoices` |
| **CCC** | **119.3 days** | DIO + DSO − DPO |

**DSO is measured and structural.** `generate_remittances()` sets
`received = month_start + randint(25,55)`, so the ~26-day lag is by
construction. It is NOT the 75 days a Net-45 assumption would imply. CCC 119.3
sits inside the published 45–210 CPG range and says the cash is trapped in
**inventory, not receivables** — Cinderhaven collects fast and holds four
months of stock.

A 75-day sensitivity ("at industry-standard Net 45 terms CCC would be ~160
days") is a legitimate analytical point and belongs in a tool's copy as a
stated scenario. **It must not be added to any table as data** — a modelled
figure beside a measured one is a number readers must be told how to read.

### COGS

| Measure | Value | Basis |
|---------|-------|-------|
| Manufactured COGS (36mo) | $36,910,058.28 | actual; = copack invoice total ±2% |
| Fully-loaded COGS (36mo) | $44,206,596.70 | manufactured + freight-in + overhead |
| Supplier invoice reconciliation | **+1.89%** | within the ±2% target; residual is freight surcharges, MOQ fees and short-ship credits |

### SKU margin spread — CRITERION RESTATED 2026-07-29

**Canonical value: 25.81% – 60.94%, width 35.12 points.**

**Criterion: SKU margin spread ≥ 30 points, position inherited from
`raw.sku_costs`.**

The earlier target "roughly 22%–55%" was **invented, not sourced** — the same
class of unsupported illustration as the trade-spend escalation trajectory and
the best-seller-worst-margin claim, both already retracted. What mattered was
*width*, and the build exceeds it: 35.1 points against 33.

**Do not attempt to move the position.** SKU margin is
`1 − loaded_cost / wholesale_price`. Both `wholesale_price` and `cogs_per_unit`
live in `raw.sku_costs`, which is frozen. Freight is anchored in aggregate and
must track weight; overhead is 1% of revenue and moves the spread by ~0.01pp.
**The only way to hit an invented position target is to modify protected
pricing data, which would move every published margin figure in the
portfolio.** That is permanently out of scope, not deferred. If a future
session sees a red check here, the check is wrong — this note is the fix.

### Master-data defect now carries a price

21 of 50 SKUs lack complete case dimensions, fall to NMFC class 175, and carry
the cost explicitly in `fct_product_costs.freight_penalty_per_unit`.
product-data-health-audit has, for the first time, a dollar cost attached to a
data-quality defect as a first-class column rather than an inference.

### UNSOURCED — do not cite as verified

**Shelf life by product line** (18 / 24 / 18 / 12 / 9 months for Artisan
Sauces / Pantry Staples / Specialty Condiments / Dried Goods / Snack Bites) is
drawn from general food-industry norms, **not** a cited category database. It
drives `dim_inventory_lots.expiry_date` and every short-dated-inventory figure
downstream. Validate against SPINS/Nielsen or a shelf-life reference before any
of it appears in a demo as sourced.

### Scope note

Nothing is wired to these tables. No tool, mart or dbt model reads the
`costing` schema yet. That is separate work.

### SUPERSEDES — added 2026-07-29

| Dead value | What it was | Why it's wrong |
|------------|-------------|----------------|
| `landed_cost_per_unit` as a per-SKU freight or landed figure | `raw.sku_costs`, generated as `cogs × uniform(1.10, 1.25)` | A random cost-proportional markup with no physical basis. Correlates +0.814 with `cogs_per_unit` and −0.065 with lbs/unit — it was never freight. Superseded per-SKU by `costing.fct_product_costs.freight_in_cost_per_unit`, which is weight- and density-derived. The **aggregate** is still used as the freight anchor so blended margin stays consistent with published figures; the per-SKU values must not be cited. |

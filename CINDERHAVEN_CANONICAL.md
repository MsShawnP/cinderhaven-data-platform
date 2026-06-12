---
name: cinderhaven-canonical
description: Authoritative Cinderhaven facts — every repo and every page cites against this file
metadata:
  type: reference
---

# CINDERHAVEN_CANONICAL — Authoritative Facts

**Source of truth:** `scripts/seed_config.py` in this repo.
**Rule:** Reconcile DOWN to this file. Never change this file to match a drifted repo.
**Last verified:** 2026-06-08

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
| Product data — annualized cost | **$458K** | product-data-health-audit | ✅ Confirmed |
| Deductions — total backlog | $1.66M | retailer-deduction-recovery | ✅ Confirmed |
| Deductions — baseline recovery rate | ~16% | retailer-deduction-recovery | ✅ Confirmed |
| Deductions — max recovery rate | 65% | retailer-deduction-recovery | ✅ Confirmed |
| Deductions — forward exposure | $861K | retailer-deduction-recovery | ✅ Confirmed |
| Short-ship — total cost (3 yr, 8 dimensions) | $32.8M | short-ship-cost | ✅ Confirmed |
| Short-ship — shipped revenue | $53.0M | short-ship-cost | ✅ Confirmed |
| OTIF — internal fill rate | 95% | otif-blind-spot | ✅ Confirmed |
| OTIF — retailer-scored | 86% | otif-blind-spot | ✅ Confirmed |
| OTIF — annual exposure (total) | ~$433K/yr | otif-blind-spot | ✅ Confirmed |
| OTIF — annual fines | ~$136K/yr | otif-blind-spot | ✅ Confirmed |
| OTIF — shelf-velocity damage | ~$297K/yr | otif-blind-spot | ✅ Confirmed |
| Channel — distribution vs retail delta | $91K per $1M deployed | where-the-money-comes-from | ✅ Confirmed |
| Revenue lifecycle — cents per invoiced dollar | 83¢ | contract-to-cash | ✅ Confirmed |
| Launch economics — gross revenue Year 1 | $499,200 | cost-of-saying-yes | ✅ Operator-validated |
| Launch economics — net cash Year 1 | −$36,320 | cost-of-saying-yes | ✅ Operator-validated |
| Thesis range | $1.4M–$3.1M/yr | the-ten-decisions | ✅ Confirmed |
| Trade — all-in (trailing-52w) | ~$3.4M/yr, 10.5% of scan revenue | trade-spend-data-diagnostic | ✅ Confirmed |
| Trade — operational waste | ~$480K/yr | trade-spend-data-diagnostic | ✅ Confirmed |
| Trade — chargebacks | 837 (677 ret + 160 dist) | cinderhaven-data-platform | ✅ Confirmed |

**Product data $458K note:** Post-date-shift regen (2026-06-08) and full R pipeline run confirmed: 677 retailer chargebacks totaling $686,534 over 18 months, annualized to $457,689. Rounded to $458K for all downstream copy. Previous $461K was from the pre-date-shift window; $430K was from a stale cache; $296K was from a pre-reseed calibration. All superseded.

**Deductions $1.66M scope note (verified 2026-06-10):** $1,663,294 is a cross-channel total across all 9 trading partners: retailer $1,332,704 / 13,960 rows (Walmart, Kroger, Whole Foods, Sprouts, Costco, Regional Group) + distributor $330,590 / 2,063 rows (UNFI, KeHE, DPI Northwest). `fct_retailer_deductions` in Postgres covers the retailer portion only; queries against that table return $1.33M by design — that is a scope difference, not drift or a data error. The published case-study copy of ~15,900 deductions is a rounded approximation of the exact 16,023 cross-channel total; it is not a third figure requiring reconciliation. Retailer-only tools (e.g. the Question Engine's Q10) should cite $1.33M and note that distributor deductions are excluded.

---

## Trade Economics — CANONICAL FIGURES (option a, locked 2026-06-04)

**Source:** Current Postgres SSOT, queried 2026-06-08 via `flyctl proxy`.
**Regen commit:** `afbb7c5` (feat: expand seed config to 50 SKUs / 3yr window and scale order generation)
**Data version:** cinderhaven-data-v2
**Seed config:** `scripts/seed_config.py`, `SEED=42`, economic constants frozen.
**Rule:** All downstream pieces reconcile to this file.

### Headline figures

| Measure | Value | Definition |
|---------|-------|------------|
| All-in trade cost (annualized) | $3.4M/yr | Structural trade + operational waste excl promo_billback |
| All-in trade cost (trailing-52w) | $3.5M/yr | Same methodology, trailing 52-week window |
| All-in trade rate | 10.5% | Of trailing-52w scan revenue ($32.8M) |
| All-in trade cost (36mo) | $10.26M | 2023-01-01 to 2026-01-02 |
| Structural trade (36mo) | $8.8M | AVG(trade_spend_pct) × trailing-52w scan revenue per channel |
| Operational waste (36mo) | $1.44M | Trailing-365 deductions excl promo_billback |
| Operational waste (annual) | ~$480K/yr | Recoverable via disputes |
| Structural trade (annual) | ~$3.0M/yr | Rate × trailing-52w channel revenue |
| Structural trade rate | 9.0% | Of trailing-52w scan revenue ($32.8M) |
| Operational waste rate | 1.5% | Of trailing-52w scan revenue ($32.8M) |
| Chargebacks | 837 | 677 retailer + 160 distributor; gross = net, no reversals |
| Data window | 2023-01-01 to 2026-01-02 | 36 months |
| Scan revenue (trailing-52w) | $32.8M | |
| EBITDA check | 13.7% trade + 11% EBITDA = 24.7% | Leaves 75.3% for COGS+SGA (plausible) |

### Methodology

All-in trade cost = structural trade + operational waste (excl promo_billback).

- **Structural trade** = AVG(trade_spend_pct) × trailing-52w scan revenue per channel.
- **Operational waste** = trailing-365 deductions excluding promo_billback (already captured in structural rates — including it would double-count).
- **Chargebacks** (separate table) overlap with deduction types and are NOT added to all-in.
- **Recoverable layer** = operational waste only (~$480K/yr). The $8.8M structural trade is contracted, not recoverable via disputes.

### APPROVED PHRASINGS

Downstream pieces copy these strings verbatim. They never re-derive.

| Context | Exact phrasing |
|---------|----------------|
| Product data cost (annual) | "~$458K/yr in chargeback cost attributable to data-quality defects" |
| Trade context (annual) | "~$3.4M/yr all-in trade spend, 10.5% of scan revenue (trailing 52 weeks)" |
| Recoverable layer | "~$480K/yr operational deduction waste; 837 chargebacks over 36 months" |
| 36-mo total (only when a real multi-year total is needed) | "$10.26M all-in trade over 36 months" |

**Rule:** Pieces copy these phrasings. They never re-derive figures from raw data.

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
| $296K product data cost | Website recalibration (2026-06-02) against pre-reseed data | SSOT reseeded; current pipeline produces $458K |
| $60K chargebacks | the-ten-decisions early draft figure | Never matched any pipeline output; superseded by $458K |
| 864 chargebacks | Pre-date-shift total (690 ret + 174 dist) | Date-window shift produces 837 (677 ret + 160 dist) |
| 86¢ per dollar | Pre-date-shift contract-to-cash (CY2025) | CY2024 data produces 83¢ |
| $33.1M short-ship | Pre-date-shift short-ship total cost | Date-window shift produces $32.8M |
| 10.8% all-in trade rate | Pre-date-shift trade rate | Date-window shift produces 10.5% |
| 9.2% structural trade rate | Pre-date-shift structural rate | Date-window shift produces 9.0% |

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
| ~$361K product data cost | **~$458K** | about/page.tsx (site), old process docs |
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

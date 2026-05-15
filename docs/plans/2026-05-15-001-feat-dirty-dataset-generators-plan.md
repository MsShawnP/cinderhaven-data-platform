---
title: "feat: Build realistically degraded Cinderhaven dataset"
status: completed
origin: docs/brainstorms/dirty-dataset-requirements.md
plan_depth: standard
created: 2026-05-15
---

# Build Realistically Degraded Cinderhaven Dataset

## Summary

Create a standalone repository (`cinderhaven-data-dirty`) containing
Python degradation generators that take the clean Cinderhaven SQLite
database as input and produce a realistically corrupted clone. Defects
are organized by six root causes that cascade across tables in the
order they would in a real CPG company's data ecosystem. Output is
deterministic (seeded RNG) with configurable severity (light /
moderate / heavy).

(see origin: `docs/brainstorms/dirty-dataset-requirements.md`)

---

## Problem Frame

The clean Cinderhaven dataset is too clean for data-hygiene portfolio
work. Joins always resolve, types are consistent, defects don't
cascade. A separate dirty dataset — isolated from the 4-6 analytical
consumers that depend on clean marts — lets future portfolio pieces
demonstrate systematic data quality work without breaking anything
downstream.

---

## Key Technical Decisions

**Degradation pipeline, not forked generators.** The pipeline copies
the clean SQLite, adjusts structural parameters, then applies
root-cause degradation modules in cascade order. This keeps the clean
generators untouched and makes the dirty layer additive. No
off-the-shelf tool (PuckTrick, Faker, SDV) produces CPG-realistic
defects — custom degraders organized by root cause are the right
approach.

**Cascade order: upstream → downstream.** Product master defects are
applied first because they're the source of truth. Distribution and
pricing second. POS/scan data third. Transaction pipeline fourth.
Financial reconciliation last. This ensures type-coercion damage on
GTINs naturally cascades into orphaned scan records and unresolvable
deductions, rather than requiring synthetic cross-referencing.

**Per-defect RNG isolation.** Each root-cause degrader gets its own
`random.Random` instance derived from the master seed + defect name.
Adding, removing, or re-parameterizing one degrader doesn't change
which rows other degraders select. This is essential for debugging
and iterating on individual defect types.

**Mutation log as ground truth.** Every degradation function records
exactly what it changed (table, primary key, column, original value,
degraded value, root cause, DAMA dimension). This log becomes the
defect manifest and enables future portfolio pieces to score their
detection accuracy.

**Severity controls root-cause activation + rate scaling.** Light
severity activates 3 root causes at conservative rates. Moderate
activates all 6 at standard rates. Heavy activates all 6 at
aggressive rates with full cascading. Rates are per-defect-type
within each root cause (see defect catalog below).

**Python with zero external dependencies beyond stdlib + sqlite3.**
The clean generators use only stdlib. The dirty generators follow
suit — no pandas, no SQLAlchemy. `random.Random` for RNG, `sqlite3`
for database access, `json` for mutation log serialization. This
keeps the repo self-contained and trivially runnable.

---

## Defect Catalog (Six Root Causes)

The degradation pipeline applies six root causes in cascade order.
Each maps to DAMA data quality dimensions and affects specific tables.

### RC1: Excel/CSV Mangling

The single most common source of CPG data quality damage. Someone
opens a product master CSV in Excel, saves it, and Excel silently
destroys identifiers and formatting.

| Defect | DAMA Dimension | Tables | Light | Moderate | Heavy |
|---|---|---|---|---|---|
| UPC leading zeros stripped | Validity | product_master | 3% | 5% | 8% |
| GTIN in scientific notation | Validity | product_master | 2% | 4% | 7% |
| Numbers stored as text | Validity | sku_costs, price_history | 0% | 3% | 5% |
| Encoding damage (accented chars → mojibake) | Accuracy | product_master | 0% | 5% | 10% |
| Date auto-formatting (YYYY-MM-DD → M/D/YY) | Consistency | price_history | 0% | 3% | 5% |

Cascade: Damaged UPCs/GTINs in product_master break joins to
scan_data, order_lines, chargebacks, and distribution_log wherever
the identifier is used as a join key.

### RC2: Governance Decay

New SKUs are added over the 104-week window by staff who don't follow
the original naming conventions. Data quality degrades as the catalog
grows without oversight.

| Defect | DAMA Dimension | Tables | Light | Moderate | Heavy |
|---|---|---|---|---|---|
| Near-duplicate SKUs (same product, different code) | Uniqueness | product_master | 3 dupes | 6 dupes | 10 dupes |
| Missing dimensions on newer items | Completeness | product_master | 30% of new | 50% of new | 80% of new |
| Inconsistent casing (product names) | Consistency | product_master | 10% | 20% | 35% |
| Stale last_updated timestamps | Timeliness | product_master | 8% | 15% | 25% |
| Serving size format variations | Consistency | product_master | 10% | 20% | 30% |
| Wholesale price below COGS | Accuracy | sku_costs | 1% | 2% | 4% |

Cascade: Duplicate SKUs split transaction history — some orders,
scan records, and deductions land on the original, some on the
duplicate, so neither shows complete velocity or exposure. New SKUs
with bad dimensions trigger chargebacks.

### RC3: Retailer Integration Gaps

Each retailer's systems have different quirks. Defect patterns vary
by retailer, making cross-retailer analysis unreliable without
normalization.

| Defect | DAMA Dimension | Tables | Light | Moderate | Heavy |
|---|---|---|---|---|---|
| Field truncation (Retailer B, 30 chars) | Accuracy | orders, order_lines | 1 retailer | 2 retailers | 3 retailers |
| Date format shift (MM/DD/YYYY vs ISO) | Consistency | orders, shipments | 0 retailers | 1 retailer | 2 retailers |
| Retailer name inconsistency across tables | Consistency | all retailer-referencing | 1 variant | 3 variants | 5 variants |
| Store-retailer misattribution | Accuracy | stores | 0% | 0.5% | 1% |
| Negative units (returns mixed into sales) | Validity | scan_data | 0% | 0.5% | 1% |
| ASN flag contradictions | Consistency | shipments | 0% | 2% | 4% |

Cascade: Date format shifts cause temporal impossibilities (ship
before order) when mixed with ISO-formatted dates. Retailer name
variants break GROUP BY and pivot analyses.

### RC4: ETL Pipeline Failures

Systematic failures in the data pipeline that processes raw feeds
into the warehouse.

| Defect | DAMA Dimension | Tables | Light | Moderate | Heavy |
|---|---|---|---|---|---|
| Missing weeks in time series | Completeness | scan_data | 2% | 4% | 7% |
| Duplicate week reporting | Uniqueness | scan_data | 0% | 1% | 2% |
| Dollar/unit price mismatch | Accuracy | scan_data | 0% | 3% | 5% |
| Orphan store_ids (not in stores table) | Referential integrity | scan_data | 0% | 1% | 2% |
| Week-ending date shift (Sat vs Sun) | Consistency | scan_data | 0 retailers | 1 retailer | 2 retailers |
| Late-arriving records (future timestamps) | Timeliness | orders, deductions | 0% | 1% | 2% |

Cascade: Missing scan_data weeks create ambiguity — was it truly
zero sales, or a reporting gap? Duplicate weeks inflate totals on
naive SUM. Orphan store_ids break joins to the store dimension.

### RC5: Manual Process Errors

Human mistakes in deduction processing, order entry, and dispute
management.

| Defect | DAMA Dimension | Tables | Light | Moderate | Heavy |
|---|---|---|---|---|---|
| Vague deduction codes (MISC, OTHER, NULL) | Completeness | deductions | 10% | 18% | 25% |
| Deduction amount exceeds order total | Accuracy | deductions | 0% | 3% | 5% |
| Deduction date before ship date | Validity | deductions | 0% | 1.5% | 3% |
| Missing code_id foreign key | Referential integrity | deductions | 3% | 7% | 12% |
| Ship date before PO date | Validity | shipments | 0% | 0.5% | 1.5% |
| Order total != sum of lines | Accuracy | orders | 0% | 3% | 5% |
| Duplicate PO number | Uniqueness | orders | 0% | 0.5% | 1% |
| Pack units != shipped units | Consistency | pack_records | 0% | 3% | 5% |
| Dispute status contradictions | Consistency | disputes | 0% | 2% | 4% |

Cascade: Missing code_ids break joins to deduction_codes and
retailer_rules, making it impossible to determine dispute windows
or evidence requirements.

### RC6: Business Process Gaps

Organizational failures where processes don't enforce data integrity.

| Defect | DAMA Dimension | Tables | Light | Moderate | Heavy |
|---|---|---|---|---|---|
| Double-dip deductions | Uniqueness | deductions | 2% | 5% | 8% |
| Sales at unauthorized stores | Referential integrity | scan_data | 0% | 2% | 4% |
| Promo deduction with no matching promo | Referential integrity | deductions | 0% | 4% | 7% |
| Post-audit claim outside lookback window | Validity | post_audit_claims | 0% | 8% | 15% |
| Remittance gross != net - deductions | Accuracy | remittances | 0% | 2% | 4% |
| Overlapping authorization periods | Uniqueness | distribution_log | 0% | 1% | 3% |
| Promotion dates outside scan window | Validity | promotions | 0% | 3% | 5% |

Cascade: Double-dip deductions inflate deduction totals. Sales at
unauthorized stores create orphan records in any authorization-based
analysis. Remittance math errors make reconciliation impossible.

---

## Output Structure

```
cinderhaven-data-dirty/
├── README.md
├── degrade.py                     # CLI entry point
├── degraders/
│   ├── __init__.py
│   ├── pipeline.py                # Orchestrator: copy, adjust, degrade, manifest
│   ├── config.py                  # Severity presets + per-defect rate configs
│   ├── manifest.py                # Mutation log → defect manifest generator
│   ├── rc1_excel_csv_damage.py    # Root cause 1
│   ├── rc2_governance_decay.py    # Root cause 2
│   ├── rc3_retailer_integration.py # Root cause 3
│   ├── rc4_etl_pipeline_failures.py # Root cause 4
│   ├── rc5_manual_process_errors.py # Root cause 5
│   └── rc6_business_process_gaps.py # Root cause 6
├── tests/
│   ├── test_determinism.py
│   ├── test_severity_levels.py
│   ├── test_cascading.py
│   └── test_defect_rates.py
├── data/
│   └── cinderhaven_dirty.db       # Generated at moderate severity (canonical)
└── docs/
    └── defect-manifest.md         # Generated: full defect documentation
```

---

## Implementation Units

### U1. Repository scaffold and pipeline framework

**Goal:** Create the new repo with the core degradation framework —
CLI entry point, pipeline orchestrator, config system, mutation log,
and the non-round parameter adjustment step.

**Requirements:** R9, R15, R16, R17, R19, R20, R21

**Dependencies:** None

**Files:**
- `README.md`
- `degrade.py`
- `degraders/__init__.py`
- `degraders/pipeline.py`
- `degraders/config.py`
- `degraders/manifest.py`
- `tests/test_determinism.py`

**Approach:**

The pipeline orchestrator (`pipeline.py`) is the spine. It:
1. Copies the clean SQLite to the output path
2. Adjusts structural parameters to non-round values (remove 13
   random Walmart stores → 487, remove 1 Costco → 79, shift SKU
   distribution to 28/31/31 per line, remove corresponding downstream
   records for dropped stores/SKUs)
3. Iterates through enabled degrader modules in cascade order
4. Passes each degrader a `random.Random` instance derived from
   `master_seed + degrader_index` (each degrader gets a fixed
   integer offset: RC1=1, RC2=2, …, RC6=6 — no `hash()`, which is
   non-deterministic across Python processes due to PYTHONHASHSEED)
5. Collects mutation logs from each degrader
6. Passes accumulated mutations to the manifest generator

The config system (`config.py`) holds three severity presets as
Python dicts — `LIGHT`, `MODERATE`, `HEAVY` — each mapping
`(root_cause, defect_type)` to a rate float. No YAML dependency.

The CLI (`degrade.py`) accepts:
- `--input` path to clean SQLite (required)
- `--output` path for dirty SQLite (default: `data/cinderhaven_dirty.db`)
- `--severity` light|moderate|heavy (default: moderate)
- `--seed` integer (default: 42)

Non-round parameter adjustment happens before any degradation. It
removes a small number of stores and their downstream data (scan_data,
distribution_log) to break the exact 500/120/80 pattern. Also shifts
SKU distribution so product lines have 28/31/31 instead of 30/30/30.

**Patterns to follow:** The existing generators in cinderhaven-data
use `random.Random` with explicit seeds and a step-numbered execution
pattern (`01_generate_stores.py`, `02_generate_distribution.py`, etc.).
Follow the same RNG discipline.

**Test scenarios:**
- Run pipeline twice with same seed and severity → output databases
  are byte-identical
- Run pipeline with different seeds → outputs differ
- Run pipeline with each severity level → completes without error
- Non-round adjustment removes stores and downstream data correctly
  (no orphan scan_data records for removed stores)
- Mutation log contains at least one entry per active root cause
- CLI rejects invalid severity values with a clear error message

**Verification:** `python degrade.py --input <clean.db> --output
<dirty.db> --severity moderate` runs to completion. Output database
opens in SQLite and has the same tables as the input. Mutation log is
non-empty.

---

### U2. Excel/CSV damage degrader (RC1)

**Goal:** Implement root cause 1 — the defects that result from
opening product data in Excel and saving it. This is the highest-
leverage degrader because it damages identifiers that cascade into
join failures across the entire dataset.

**Requirements:** R1, R2, R3, R4, R5

**Dependencies:** U1

**Files:**
- `degraders/rc1_excel_csv_damage.py`
- `tests/test_cascading.py`

**Approach:**

Five defect functions, each operating on the copied database:

1. **UPC leading zero strip:** Select affected product_master rows
   by rate. For each, strip the leading zero from the UPC field.
   Then scan downstream tables (scan_data, order_lines, chargebacks,
   distribution_log) — for a configurable fraction of downstream
   records that reference the affected SKU, update the UPC/identifier
   to the stripped version. The remainder keep the original UPC,
   creating a split: some records join, some don't.

2. **GTIN scientific notation:** Convert affected GTINs to scientific
   notation string (e.g., `"10614141000415"` → `"1.06141E+13"`).
   Same downstream cascade logic as UPC stripping.

3. **Numbers as text:** In sku_costs and price_history, wrap some
   numeric values in whitespace or store with comma separators
   (e.g., `1234.56` → `"1,234.56"` as text). These won't fail on
   storage (SQLite is type-flexible) but will break numeric operations.

4. **Encoding damage:** Replace accented characters and special
   characters in product descriptions with mojibake patterns (e.g.,
   `"jalapeño"` → `"jalapeÃ±o"`). Mimics UTF-8 data read as
   Latin-1 and re-encoded.

5. **Date auto-formatting:** In price_history, convert some ISO dates
   to M/D/YY format. Mimics Excel's date column auto-detect.

Each function returns a list of mutations (table, PK, column,
old value, new value, defect type).

Covers AE1: The Excel CSV damage root cause fires → UPCs lose leading
zeros → scan_data records referencing those UPCs become orphans →
deductions referencing the same UPCs are unresolvable.

**Patterns to follow:** The clean generators use direct `cursor.execute`
with parameterized queries. Follow the same pattern.

**Test scenarios:**
- Covers AE1. After RC1 at moderate severity, at least 3 product_master
  UPCs have leading zeros stripped. At least 50 scan_data records
  reference the stripped UPC and fail to join back to product_master
  on the original UPC value.
- GTIN scientific notation produces strings matching the pattern
  `\d\.\d+E\+\d+`, not valid GTIN-14 format.
- Encoding damage produces non-ASCII bytes in product descriptions
  that weren't there in the clean data.
- Mutation log for RC1 accurately records every changed value with
  correct original and new values.
- At light severity, only UPC stripping and GTIN notation fire (per
  rate table). At heavy, all five defect types fire.

**Verification:** Query `SELECT COUNT(*) FROM scan_data sd LEFT JOIN
product_master pm ON sd.sku = pm.sku WHERE pm.sku IS NULL` returns
nonzero on the dirty database (zero on clean). The number of orphan
rows scales with severity.

---

### U3. Governance decay degrader (RC2)

**Goal:** Implement root cause 2 — quality degradation over time as
new SKUs are added without governance. This degrader introduces
near-duplicate SKUs and degrades newer product data, producing split
transaction histories.

**Requirements:** R1, R2, R6, R7

**Dependencies:** U1

**Files:**
- `degraders/rc2_governance_decay.py`
- `tests/test_defect_rates.py`

**Approach:**

1. **Near-duplicate SKU insertion:** Generate N new product_master
   rows (3/6/10 depending on severity) that are near-copies of
   existing SKUs. Each duplicate has:
   - A new stock code (e.g., `CH-AS-001` → `CH-AS-001A` or `CHAS001`)
   - Slightly different description (truncated, different casing,
     abbreviated words: "Smoky Chipotle Hot Sauce" → "SMOKY CHIPOTLE
     HOT SCE")
   - Some missing fields (dimensions, brand_owner)
   - An `authorized_date` in the middle of the 104-week window
     (weeks 40-60), simulating when the ops hire added them

2. **Transaction history splitting:** For each duplicate SKU, take
   a fraction of the original SKU's downstream records (scan_data,
   order_lines, deductions) from the duplicate's authorization date
   forward and reassign them to the duplicate's stock code. This
   splits the history — neither the original nor the duplicate shows
   complete data.

3. **Temporal quality gradient:** SKUs with later `authorized_date`
   values get progressively worse data: more missing fields,
   inconsistent naming, missing sku_costs entries. The original
   90 SKUs from week 0 are the cleanest; anything added after week
   30 degrades.

4. **Casing inconsistency:** Apply random casing changes to product
   names on affected rows (e.g., ALL CAPS, Title Case, lowercase).

Covers AE2: 8 new SKUs added weeks 40-60 by simulated ops hire,
near-duplicate descriptions, inconsistent casing, missing dimensions,
transaction history splits.

**Patterns to follow:** The clean `seed_product_master.sql` defines
the exact schema and intentional defect patterns. Mirror the column
structure for new rows.

**Test scenarios:**
- Covers AE2. At moderate severity, exactly 6 near-duplicate SKUs
  are inserted. Each has a matching original with high string
  similarity (>0.7 on normalized description) but different stock
  code.
- Transaction history is split: for each duplicate, the original
  SKU's scan_data count after the duplicate's authorization date
  is lower than in the clean dataset, and the duplicate has scan_data
  rows that account for the difference.
- Temporal gradient: SKUs with authorized_date after week 30 have
  higher null rates in dimension columns than SKUs from week 0.
- No duplicate SKU shares an exact stock code with an existing SKU.
- Duplicate SKUs appear in distribution_log with correct
  authorization dates.

**Verification:** `SELECT sku, description FROM product_master`
shows near-duplicate pairs. `SELECT sku, COUNT(*) FROM scan_data
GROUP BY sku` shows the original SKU's count is lower than in clean
data for affected products.

---

### U4. Retailer integration and ETL degraders (RC3 + RC4)

**Goal:** Implement root causes 3 and 4 — cross-system integration
defects that vary by retailer, and ETL pipeline failures in
POS/scan data. These are grouped because they both primarily affect
the transaction and POS tables and share the retailer-specific
targeting logic.

**Requirements:** R1, R3, R4, R8

**Dependencies:** U1, U2 (needs RC1 applied first so cascading
identifier damage is already present)

**Files:**
- `degraders/rc3_retailer_integration.py`
- `degraders/rc4_etl_pipeline_failures.py`
- `tests/test_severity_levels.py`

**Approach:**

**RC3 — Retailer integration gaps:**

Assign defect profiles to specific retailers based on severity:
- Light: 1 retailer gets field truncation
- Moderate: 2 retailers get unique defect profiles
- Heavy: 3 retailers, each with distinct problems

Defect profiles (assigned to retailers by the config):
- **Profile A (e.g., assigned to a regional chain):** Field truncation
  to 30 chars on product descriptions in order_lines. Date format
  shift to MM/DD/YYYY on order and shipment dates.
- **Profile B (e.g., assigned to another chain):** Encoding damage
  on text fields. Negative unit quantities (returns mixed into
  sales) in scan_data. ASN flag contradictions in shipments.
- **Cross-retailer:** Retailer name variants across tables ("Whole
  Foods" vs "Whole Foods Market" vs "WFM"). Store-retailer
  misattribution (small number of stores assigned to wrong retailer).

Covers AE3: Three retailers with different defect profiles produce
different failure modes from the same analytical query.

**RC4 — ETL pipeline failures:**

- **Missing weeks:** Remove a percentage of scan_data rows to create
  gaps in the weekly time series. Cluster removals (2-3 consecutive
  weeks for a store) to mimic scanner outages, not random drops.
- **Duplicate weeks:** For a small percentage of scan_data, insert
  a second row for the same (sku, store_id, week_ending) with
  slightly different values. Mimics re-processed feeds.
- **Dollar/unit mismatch:** On some scan_data rows, adjust
  dollars_sold so it doesn't match units_sold × any known price.
- **Orphan store_ids:** Insert scan_data rows referencing store_ids
  that don't exist in the stores table (new stores not in master).
- **Week-ending date shift:** For one retailer's stores, shift all
  week_ending dates by +1 day (Saturday ending instead of Sunday).

**Patterns to follow:** The clean `05_generate_scan_data.py` builds
scan data using the distribution_log authorization windows. The
degraders should respect the same time boundaries for realism.

**Test scenarios:**
- Covers AE3. At moderate severity, at least 2 retailers have
  distinct defect profiles. A query joining orders to shipments
  for Retailer A succeeds cleanly; the same query for Retailer B
  fails on date parsing; for Retailer C, some rows have negative
  quantities.
- Covers AE4 (partial). At light severity, scan_data defects
  are minimal. At heavy severity, missing weeks + duplicates + orphan
  stores affect 10%+ of scan_data rows.
- Missing weeks appear as consecutive gaps (2-3 weeks), not random
  individual drops.
- Duplicate scan_data rows have the same (sku, store_id, week_ending)
  composite key but different units_sold/dollars_sold values.
- Orphan store_ids are not present in the stores table.
- Retailer name variants produce at least 3 distinct spellings for
  one retailer across different tables.

**Verification:** `SELECT store_id, COUNT(DISTINCT week_ending) as
weeks FROM scan_data GROUP BY store_id` shows uneven week counts
across stores. `SELECT COUNT(*) FROM scan_data WHERE store_id NOT IN
(SELECT store_id FROM stores)` returns nonzero at moderate+.

---

### U5. Transaction and financial degraders (RC5 + RC6)

**Goal:** Implement root causes 5 and 6 — manual process errors in
the order-to-deduction chain and business process gaps that break
financial reconciliation. These are grouped because they share the
transaction pipeline tables and often interact.

**Requirements:** R1, R3, R5, R6

**Dependencies:** U1, U2, U3, U4 (needs product master degradation,
duplicate SKUs, and scan_data modifications in place so deduction
defects and unauthorized-store insertions can reference them)

**Files:**
- `degraders/rc5_manual_process_errors.py`
- `degraders/rc6_business_process_gaps.py`

**Approach:**

**RC5 — Manual process errors:**

- **Vague deduction codes:** Replace code_id with NULL or a generic
  catch-all code on affected deductions. Replace deduction_type with
  "miscellaneous" or "other".
- **Amount exceeds order:** On some deductions, inflate the amount
  above the linked order's total value.
- **Temporal impossibilities:** Set deduction_date before the linked
  shipment's ship_date. Set some ship_dates before their order's
  po_date.
- **Missing foreign keys:** Null out code_id, breaking the join to
  deduction_codes and retailer_rules.
- **Order math errors:** Adjust total_value on some orders so it
  doesn't equal the sum of order_lines.
- **Duplicate PO numbers:** Give two different orders the same
  po_number.
- **Pack/ship mismatch:** Set units_packed != units_shipped on some
  pack_records.
- **Dispute contradictions:** Set dispute outcome to "won_full" but
  recovered_amount to 0, or outcome "pending" with a non-null
  closed_date.

**RC6 — Business process gaps:**

- **Double-dip deductions:** Duplicate some deduction rows with new
  IDs but same order_id, amount, and deduction_type. Link them to
  different remittances.
- **Sales at unauthorized stores:** Insert scan_data rows for
  (sku, store_id) combinations where distribution_log shows no
  active authorization.
- **Promo-deduction mismatch:** Set deduction_type to "promotion"
  on some deductions that have no matching promotions row.
- **Post-audit outside window:** Adjust audit_period_start on some
  post_audit_claims to fall before the contractual lookback.
- **Remittance math errors:** Adjust amounts on some remittances so
  the gross - deductions != net calculation fails.
- **Overlapping authorizations:** Insert duplicate distribution_log
  rows creating two active authorization windows for the same
  (sku, store_id).

**Patterns to follow:** The clean `11_generate_deductions.py` already
models double-dip logic (3 cases). Extend the same pattern to a
configurable rate.

**Test scenarios:**
- Covers AE4 (partial). At heavy severity, deduction defects cascade:
  vague codes prevent determining dispute windows, missing FKs break
  retailer_rules joins, double-dips inflate totals.
- Double-dip deductions have different deduction_ids but identical
  order_id + amount + type. They appear in different remittances.
- Temporal impossibilities: at least some deductions have
  deduction_date < linked shipment.ship_date.
- Remittance math: `SELECT remittance_id, gross_amount,
  SUM(d.amount), net_amount FROM remittances r JOIN deductions d`
  shows discrepancies at moderate+ severity.
- Dispute contradictions: at least one dispute has
  outcome="won_full" and recovered_amount=0.
- Sales at unauthorized stores: scan_data rows exist for (sku,
  store_id) pairs with no active distribution_log entry.

**Verification:** Financial reconciliation queries that worked on
the clean dataset now produce discrepancies. The number and magnitude
of discrepancies scale with severity.

---

### U6. Defect manifest, canonical generation, and packaging

**Goal:** Generate the defect manifest documentation from the mutation
log, produce the canonical dirty SQLite at moderate severity, write
the README, and validate the output.

**Requirements:** R18, R19, R20, R21

**Dependencies:** U1, U2, U3, U4, U5 (all degraders must be
complete)

**Files:**
- `degraders/manifest.py` (update from U1 scaffold)
- `docs/defect-manifest.md` (generated)
- `README.md` (update from U1 scaffold)
- `data/cinderhaven_dirty.db` (generated)
- `tests/test_defect_rates.py` (update)

**Approach:**

1. **Manifest generation:** The manifest generator reads the mutation
   log and produces a markdown document organized by root cause:
   - Root cause name and narrative description
   - Affected tables and columns
   - Defect rates at each severity level (actual from the run,
     not just configured)
   - Cascade effects (which downstream tables are affected)
   - DAMA dimension classification
   - Example mutations (3-5 per root cause, showing before/after)

2. **Canonical generation:** Run the full pipeline at moderate
   severity with seed 42. Commit the resulting SQLite to
   `data/cinderhaven_dirty.db`. This is the default dataset
   future portfolio pieces use.

3. **Validation script:** A validation function (called at the end
   of the pipeline) checks:
   - All original tables exist in the output
   - Row counts are within expected bounds (not drastically different
     from clean — degradation modifies, it doesn't delete entire
     tables)
   - At least one defect was applied per active root cause
   - The mutation log is non-empty and well-formed

4. **README:** Document what the repo is, how to run the generators,
   severity levels, and link to the defect manifest.

**Patterns to follow:** The cinderhaven-data-platform README is the
style reference for documentation.

**Test scenarios:**
- Generated manifest contains sections for all 6 root causes at
  moderate severity.
- Manifest defect counts match the mutation log totals.
- Canonical dirty.db opens in SQLite and has the same table set as
  the clean input.
- Canonical dirty.db at moderate severity has nonzero orphan join
  counts, near-duplicate SKUs, and remittance discrepancies.
- Running `python degrade.py` twice with defaults produces
  identical dirty.db files (byte-level comparison).

**Verification:** The defect manifest is human-readable and a
reviewer can identify specific known defects by table and column.
The canonical SQLite is functional (queries return results, no
corruption).

---

## System-Wide Impact

**No impact on existing systems.** This is a new standalone
repository. The clean cinderhaven-data repo is read-only input.
The cinderhaven-data-platform and its 4-6 analytical consumers are
untouched.

**Future impact:** Portfolio pieces built against the dirty dataset
will depend on its schema matching the clean dataset's schema. If
cinderhaven-data's schema changes, the degradation generators may
need updating. The defect manifest will serve as the contract
between this repo and its consumers.

---

## Scope Boundaries

- No changes to cinderhaven-data or cinderhaven-data-platform
- No Postgres loading or deployment
- No dbt models or tests for the dirty data
- No building the data-hygiene portfolio pieces themselves
- No Docker, CI/CD, or cross-platform setup
- No external Python dependencies beyond stdlib

### Deferred to Follow-Up Work

- Postgres ingestion of the dirty dataset (when a hygiene portfolio
  piece needs it)
- dbt project that models the dirty → clean pipeline (separate arc)
- Additional root causes (EDI-specific defects, deferred to EDI
  Pre-flight delivery)
- Dashboard or visualization of defect distribution

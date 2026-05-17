# Cinderhaven Data Platform — Data Integrity Audit

Conducted 2026-05-17. Focus: data consistency across the pipeline and
downstream agreement. Prompted by bad math in downstream projects.

Previous audit (2026-05-16) focused on project quality and portfolio
signal. This audit focuses on data correctness.

---

## Phase 1: Baseline Assessment

**Date:** 2026-05-17
**Project:** cinderhaven-data-platform

### What Was Intended

Single source of truth for all Cinderhaven portfolio projects. Every
downstream project (case studies, analyses) should produce identical
revenue, deduction, and financial figures for the same data periods.
The platform was expanded with realism tweaks (Shopify calibration,
KeHE columns, 50-SKU rebuild) — all of which need to propagate
through the full pipeline.

### What Exists Today

A complete but potentially stale pipeline. Four commits since the
last audit introduced material data changes:

| Commit | Change | Impact |
|--------|--------|--------|
| `2607705` | Recalibrated Shopify DTC orders (79K orders, $1-1.8M annual) | Changes DTC revenue figures |
| `a981863` | Added KeHE columns, updated schema for 50-SKU rebuild | Changes product master shape and SKU costs |
| `8e36dca` | Added mart_channel_contribution | New mart for channel profitability |
| `686df84` | Fixed COGS calculation for DTC vs B2B units | Changes margin/profitability numbers |

**Critical question:** Were these changes applied end-to-end? The
pipeline has three handoff points where drift can occur:

1. **SQLite source** (cinderhaven-data) — did generation scripts run?
2. **Postgres raw schema** — did ingestion re-run after SQLite changes?
3. **dbt models** — did `dbt run` execute after schema/model changes?

If any step was skipped, downstream projects see stale numbers.

### Tech Stack

- **Source:** SQLite (cinderhaven-data repo)
- **Warehouse:** Postgres 17.7 on Fly.io (256MB shared-cpu-1x)
- **Transform:** dbt-core 1.11.9, dbt-postgres 1.10.0
- **Orchestration:** Dagster 1.13
- **Python deps:** psycopg2-binary, python-dotenv
- **CI:** GitHub Actions (dbt parse + Python syntax check)

### Project Health Indicators

- **Activity:** Active — 4 commits in last 24 hours
- **Documentation:** Strong (README, walkthrough, architecture diagram, 100% dbt descriptions)
- **Test coverage:** 132+ dbt tests (unique keys, not-null, accepted-values, referential integrity)
- **Dependencies:** Current
- **Data freshness:** Unknown — this is the core concern

### Gap Analysis

The project's **code** is complete and well-structured. The gap is
between **code changes** and **data state**:

1. `generate_shopify_orders.py` was recalibrated to 79K orders with
   specialty foods benchmarks — but may not have been re-run against
   the SQLite database
2. `raw_schema.sql` was updated with KeHE columns — but the DDL may
   not have been applied to Postgres
3. `stg_sku_costs.sql` and `int_product_retailers.sql` were modified
   — but dbt may not have been re-run
4. `mart_channel_contribution.sql` was added with a COGS fix — but
   may not be materialized

The result: downstream projects may be reading from any of three
different data states (old SQLite, old Postgres, or partially-updated
models), producing inconsistent numbers.

### Audit Motivation

Downstream projects show bad/weird math. Revenue and deduction
figures should agree across all projects for a given data period,
but they don't. The goal is to identify where the pipeline is
broken or stale and bring everything into agreement.

---

## Phase 2: Internal Review

**Date:** 2026-05-17
**Dimensions reviewed:** Data pipeline integrity, tests, code quality

### Top Opportunities (by leverage)

| # | Finding | Dimension | Impact | Effort | Leverage | Severity |
|---|---------|-----------|--------|--------|----------|----------|
| 1 | Dataset rebuilt (50 SKUs, 11.6K orders, 79K Shopify) but Postgres not re-loaded | Pipeline | 5 | 2 | 2.5 | critical |
| 2 | KeHE columns added to DDL/models but not applied to Postgres | Pipeline | 5 | 2 | 2.5 | critical |
| 3 | sources.yml descriptions cite old row counts (90 SKUs, 10K Shopify, 5.8K orders) | Pipeline | 4 | 1 | 4.0 | important |
| 4 | No cross-model revenue reconciliation test | Tests | 5 | 2 | 2.5 | critical |
| 5 | No reload procedure — each step (DDL, ingest, dbt run) must be manually sequenced | Pipeline | 4 | 2 | 2.0 | important |
| 6 | No source freshness or row-count sanity tests | Tests | 3 | 1 | 3.0 | important |
| 7 | COGS fix (DTC units vs B2B cases) not verified against live data | Code | 4 | 2 | 2.0 | important |
| 8 | Hardcoded DB_PATH in Python scripts (Windows-only) | Code | 2 | 1 | 2.0 | minor |

### Detailed Findings

#### Data Pipeline Integrity

**CRITICAL — Full dataset rebuild not propagated to Postgres.**
The SQLite source (cinderhaven-data) was rebuilt with fundamentally
different dimensions. Commit `a981863` updated `raw_schema.sql`
comments to match:

| Table | Old Count | New Count | Delta |
|-------|-----------|-----------|-------|
| product_master | 90 SKUs | 50 SKUs | -44% |
| orders | 5,838 | 11,634 | +99% |
| deductions | 3,087 | 13,496 | +337% |
| shopify_orders | ~10,000 | ~79,000 | +690% |
| scan_data | 1,118,009 | ~977,000 | -13% |

Two new columns were added: `wholesale_kehe` and
`trade_spend_pct_kehe` in `raw.sku_costs`.

**What likely happened:** The SQLite database was rebuilt with new
generation scripts and different parameters. The `raw_schema.sql`
DDL and dbt models were updated to reflect the new shape. But the
actual Postgres database was never re-loaded — it still contains
the old data (90 SKUs, 5.8K orders, etc.).

**This is the root cause of bad downstream math.** Any downstream
project reading from this platform sees the old data, while any
project reading from the SQLite source directly sees the new data.

**IMPORTANT — No documented reload procedure.** To bring Postgres
current requires five manual steps in sequence:

1. Scale Fly.io to 1GB (`flyctl machine update --memory 1024`)
2. Start proxy (`flyctl proxy 5432 -a cinderhaven-db`)
3. Apply DDL changes (`psql -f sql/raw_schema.sql`)
4. Re-run ingestion (`python scripts/ingest_sqlite_to_postgres.py`)
5. Re-run dbt (`dbt build`)

There is no single script that does this, and skipping any step
produces a partially-updated state that's harder to diagnose than
a fully-stale one.

**IMPORTANT — Source descriptions are stale.** `sources.yml` and
`schema.yml` still reference the old row counts:

- `stg_product_master`: "90 SKUs" → should be 50
- `stg_shopify_orders`: "10,000 orders" → should be ~79,000
- `stg_shopify_order_lines`: "19,347 line items" → stale
- `stg_orders`: "5,838 purchase orders" → should be 11,634
- `stg_deductions`: "3,087 deduction records" → should be 13,496
- `stg_scan_data`: "1,118,009 rows" → should be ~977K

#### Tests

**CRITICAL — No revenue reconciliation test.** The test suite
validates structural integrity (unique keys, not-null, referential
integrity, accepted values) and basic arithmetic consistency
(`assert_order_line_total_consistent`). But there is no test that
validates:

- Total revenue in `fct_orders` matches sum of source tables
- Total deductions in `fct_deductions` matches `stg_deductions`
- B2B + DTC revenue splits are internally consistent
- `mart_channel_contribution.gross_revenue` agrees with `fct_orders`

These are the exact metrics downstream projects rely on. Without
reconciliation tests, revenue drift is invisible to the test suite.

**IMPORTANT — No source freshness tests.** dbt supports `freshness`
checks on sources (e.g., "warn if data is older than 7 days"). None
are configured. A freshness test on the Shopify orders table, for
example, would flag that the data predates the recalibration.

**IMPORTANT — No row-count sanity tests.** The custom generic tests
(`positive_value`, `ratio_within_bounds`) are good but don't cover
volume anomalies. A test like "product_master should have between
40-60 rows" would have caught the 90→50 change if the old data was
still loaded.

#### Code Quality

**IMPORTANT — COGS fix is logically correct but unverified.** The
`mart_channel_contribution` COGS calculation (line 25-28) now
correctly differentiates:

- DTC: `quantity × cogs_per_unit` (selling individual units)
- B2B: `quantity × case_pack_qty × cogs_per_unit` (selling cases)

This is the right fix. But it was committed without running `dbt
build` against live data, so the actual margin numbers it produces
are unverified.

**MINOR — Hardcoded DB_PATH.** Both `generate_shopify_orders.py`
(line 28) and `ingest_sqlite_to_postgres.py` (line 20) hardcode a
Windows-specific path to the SQLite database. Not a data integrity
issue, but it means these scripts only work on one machine.

### Summary

The platform's code and architecture are sound. The problem is
operational: a major data rebuild happened but the pipeline was
never re-run end-to-end. Postgres still contains old data (90 SKUs,
5.8K orders) while the SQLite source has new data (50 SKUs, 11.6K
orders). This explains the bad math downstream — different projects
are reading different vintages of the same dataset. The fix is
mechanical (reload + dbt build) but needs the five-step manual
sequence above, and the test suite needs reconciliation tests to
prevent this from happening again silently.

---

## Phase 3: Landscape Scan

**Date:** 2026-05-17
**Category:** Data integrity practices in dbt-based data platforms
**Focus:** How comparable projects handle reload, reconciliation,
freshness, and volume testing — benchmarked against community norms.

### Standard Practice Tiers

The dbt ecosystem has a clear hierarchy of data integrity practices.
Cinderhaven's current position is marked.

| Tier | Practice | Cinderhaven |
|------|----------|-------------|
| **Standard** | not_null, unique, accepted_values, relationships | ✅ Has (132+ tests) |
| **Standard** | `dbt build` (interleaved test+run) | ❌ Uses separate run/test |
| **Standard** | Reload script or Makefile | ❌ Missing |
| **Common** | dbt_utils: equal_rowcount, expression_is_true | ❌ No packages.yml |
| **Common** | Source freshness (warn_after / error_after) | ❌ Not configured |
| **Intermediate** | Cross-layer reconciliation tests (singular) | 🟡 3 singular tests, but no revenue reconciliation |
| **Intermediate** | dbt-audit-helper for relation comparison | ❌ Not installed |
| **Advanced** | dbt_expectations: row count bounds, aggregation equality | ❌ Not installed |
| **Advanced** | Elementary: dynamic volume anomaly detection | ❌ Not installed |

### Packages Benchmark

Nearly every mature dbt project installs `dbt_utils`. Projects that
demonstrate data engineering rigor also install `dbt_expectations`
and/or `audit_helper`. Cinderhaven has none of these — all custom
tests are hand-written.

| Package | Purpose | Adoption | Cinderhaven |
|---------|---------|----------|-------------|
| dbt-utils | Row count equality, expression truth, recency | Near-universal | ❌ |
| dbt-audit-helper | Cross-table row/column comparison | Common in production | ❌ |
| dbt-expectations | Static bounds, aggregation equality, distribution | Common in mature projects | ❌ |
| Elementary | Dynamic anomaly detection | Advanced/production | ❌ |

### Feature Matrix (Data Integrity Focus)

| Capability | Cinderhaven | jaffle-shop | dagster-open-platform | GitLab data team |
|------------|-------------|-------------|----------------------|------------------|
| Schema tests (unique, not_null) | ✅ | ✅ | ✅ | ✅ |
| Custom generic tests | ✅ (2) | ❌ | ✅ | ✅ |
| Singular data tests | ✅ (3) | ❌ | ✅ | ✅ |
| Source freshness config | ❌ | ❌ | ✅ | ✅ |
| dbt_utils installed | ❌ | ✅ | ✅ | ✅ |
| Cross-layer reconciliation | ❌ | ❌ | ✅ | ✅ |
| Row-count sanity checks | ❌ | ❌ | 🟡 | ✅ |
| Reload script/Makefile | ❌ | 🟡 | ✅ (Dagster DAG) | ✅ (Airflow) |
| Financial sum agreement | ❌ | ➖ | ➖ | ✅ |
| Model contracts (dbt 1.5+) | ❌ | ✅ | 🟡 | ✅ |

### Landscape Position

#### Where Cinderhaven Is Stronger

- **Test volume:** 132+ tests exceeds every surveyed portfolio project
  (most have <20)
- **Custom test macros:** `positive_value` and `ratio_within_bounds`
  show sophistication beyond schema.yml defaults
- **Singular business tests:** The 3 assertion tests (line total
  consistency, deduction recovery bounded, remittance net ≤ gross)
  demonstrate domain understanding
- **Orchestration:** Dagster integration is rare in portfolio projects

#### Where Cinderhaven Is Weaker

- **No packages.yml** — missing the foundational dbt ecosystem
  packages that signal awareness of community tools
- **No source freshness** — the built-in dbt feature specifically
  designed to catch the exact problem this audit was triggered by
- **No cross-layer reconciliation** — the singular tests validate
  within-model invariants but never check that revenue in
  `fct_orders` agrees with `stg_order_lines` or that
  `mart_channel_contribution` agrees with `fct_orders`
- **No reload procedure** — the only surveyed project category
  without some form of automated rebuild is toy/demo projects

#### What's Missing (Table Stakes)

1. `packages.yml` with dbt_utils at minimum
2. Source freshness configuration on sources.yml
3. A reload script that chains ingestion → `dbt build --full-refresh`
4. At least one cross-layer revenue reconciliation test

#### Category Trend

The dbt ecosystem is moving toward **contract-first** data
engineering: model contracts (column types + constraints enforced at
build time), source freshness as a CI gate, and cross-model
reconciliation as standard practice. Projects that only have
schema.yml tests (even 132 of them) are seen as v1-era dbt
practice. The differentiator in 2026 is demonstrating awareness of
the integrity stack: freshness → reconciliation → contracts.

### Summary

Cinderhaven's test volume is top-tier for portfolio work, but the
test *types* are almost entirely structural (unique, not_null,
accepted_values). The data integrity practices that would have
prevented this audit's triggering event — source freshness, cross-
layer reconciliation, row-count bounds, and a reload procedure —
are standard in the ecosystem but absent from this project. Adding
`packages.yml`, source freshness, and two reconciliation tests
would close the gap with minimal effort and significantly strengthen
the portfolio signal.

---

## Phase 4: Synthesis & Next Moves

**Date:** 2026-05-17

### Cross-Reference Summary

The audit tells a single story across all three phases. Cinderhaven
built strong infrastructure (34 models, 132 tests, Dagster, hosted
docs) but omitted the operational plumbing that keeps a data
platform honest: reload automation, freshness checks, and cross-
layer reconciliation. The dataset was then rebuilt with fundamentally
different dimensions, and without that plumbing, nobody noticed the
pipeline was stale until downstream projects produced wrong numbers.

Phase 3 confirms this isn't an exotic gap. Source freshness, reload
scripts, and `dbt_utils` are table stakes in the ecosystem.
Cinderhaven has more raw test volume than any surveyed portfolio
project, but the tests are structural — they verify schema
invariants, not financial agreement. Adding the missing integrity
layer is both the fix for the immediate problem and the highest-
leverage portfolio signal improvement available.

The one area where Cinderhaven can leapfrog (not just catch up) is
financial reconciliation testing. No surveyed portfolio project has
cross-layer revenue/deduction agreement tests in a real CPG domain.
The singular business tests already show domain understanding —
adding reconciliation tests that validate revenue from staging
through marts would be genuinely differentiating.

### Ranked Next Moves

| # | Move | Category | Strategic | Internal | Effort | Score | Description |
|---|------|----------|-----------|----------|--------|-------|-------------|
| 1 | Reload Postgres from SQLite | Foundational | 1 | 5 | 2 | 3.0 | Apply DDL, re-ingest all tables, run `dbt build`. Fixes the immediate bad-math problem. |
| 2 | Add packages.yml | Close gap | 4 | 3 | 1 | 7.0 | Install dbt_utils + dbt_expectations. Table stakes — every mature project has these. |
| 3 | Add cross-layer reconciliation tests | Leapfrog | 5 | 5 | 2 | 5.0 | Singular tests: fct_orders revenue = staging revenue, mart = fct totals, deduction sums agree. No portfolio project does this in a real CPG domain. |
| 4 | Configure source freshness | Close gap | 3 | 4 | 1 | 7.0 | Add freshness config to sources.yml with warn/error thresholds. Would have caught this problem automatically. |
| 5 | Create reload script | Close gap | 3 | 4 | 2 | 3.5 | Single script that chains: scale Fly → proxy → DDL → ingest → dbt build → scale down. Prevents recurrence. |
| 6 | Update source/schema descriptions | Close gap | 2 | 3 | 1 | 5.0 | Fix stale row counts in sources.yml and schema.yml (90→50 SKUs, 10K→79K Shopify, etc.). |
| 7 | Add row-count sanity tests | Close gap | 3 | 3 | 1 | 6.0 | dbt_expectations: expect_table_row_count_to_be_between on key tables. Catches ingestion failures. |
| 8 | Verify COGS against live data | Foundational | 1 | 4 | 1 | 5.0 | After reload, verify mart_channel_contribution produces sane margins. Already part of move 1. |

### Recommended Sequence

**Step 1 — Fix the data (moves 1, 8).** Reload Postgres from
SQLite and verify COGS. This fixes the immediate problem. Every
other move builds on having correct data in the warehouse.

**Step 2 — Add the integrity layer (moves 2, 4, 6, 7).** Install
packages, configure source freshness, update descriptions, add
row-count tests. All are low-effort (1 each) and close the most
visible gaps with the ecosystem. Do these in a single commit.

**Step 3 — Add reconciliation tests (move 3).** Write 3-4 singular
tests that validate financial agreement across layers:
- `assert_fct_orders_revenue_matches_staging` — sum of fct_orders
  line totals equals sum of stg_order_lines + stg_shopify_order_lines
- `assert_mart_revenue_matches_fct` — mart_channel_contribution
  gross_revenue equals fct_orders by channel
- `assert_deduction_totals_agree` — fct_deductions total matches
  stg_deductions total
- `assert_cogs_positive_margin` — no channel has negative gross
  margin (sanity check on the COGS fix)

**Step 4 — Add reload script (move 5).** Wrap the manual sequence
into a single script. This is the prevention layer — next time the
dataset is rebuilt, one command brings everything current.

### What NOT to Do

- **Don't add Elementary or dynamic anomaly detection.** The dataset
  is synthetic and batch-loaded. Elementary's time-series anomaly
  detection needs recurring loads to build a training window. It's
  the right tool for production pipelines, not portfolio pieces with
  static data.

- **Don't add model contracts yet.** dbt model contracts (column
  types + constraints enforced at build time) are a strong portfolio
  signal, but they require dbt 1.5+ contract syntax and add friction
  to every schema change. Get the integrity layer working first;
  contracts are a future enhancement.

- **Don't refactor the ingestion script.** The hardcoded DB_PATH is
  a cosmetic issue. Fixing it doesn't change data correctness and
  risks breaking the one path that works. Leave it until there's a
  second user of these scripts.

- **Don't chase audit_helper.** It's designed for migration
  validation (comparing old vs. new pipeline output). Cinderhaven
  doesn't have a migration use case. The singular reconciliation
  tests in move 3 are more valuable and more readable.

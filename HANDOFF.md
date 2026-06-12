# Cinderhaven Data Platform — Handoff Log

Session-by-session state. Updated by /log mid-session and /wrap at
session end.

For durable choices, see DECISIONS.md.
For the current work arc, see PLAN.md.
For things that didn't work, see FAILURES.md.

---

## 2026-06-12 15:48

**What changed:** Phase 2 bugfixes complete — mart case-pack COGS fix, check_canonical rate_map fix with interim trade relock, and 4 dbt plausibility assertions here; PDHA annualization and COGS-ratio fixes landed in three sibling repos.

**Why:** Phase 2 of the causal-fulfillment arc (design approved by Shawn this morning). Small SQL/Python fixes with large credibility exposure, fixed before any new generation.

**State:** Guard 10/10 PASS, dbt build 396/396, all repos pushed. Trade canon relocked $3.4M/10.5% → $3.7M/11.3% (old values in SUPERSEDES) — pending Shawn's review. Docker replica running. Seeders untouched.

**Next:** Shawn reviews Phase 2 (especially the trade relock). Then Phase 3: causal fulfillment model in the seeders per CAUSAL_FULFILLMENT_DESIGN.md.

---

## 2026-06-12 12:28

**What changed:** Wrote cinderhaven-plausibility-audit.md — read-only plausibility audit of every canonical figure, recomputed on a freeze-guard-certified local replica.

**Why:** check_canonical.py proves figures haven't drifted; this audit asked whether they describe a coherent company. Five REGENERATE-grade findings needed recording before any fix work.

**State:** Audit complete: 6 PLAUSIBLE / 7 RELABEL / 5 REGENERATE. Headliners: $458K/yr product-data cost is a 2× window error (36-mo total annualized as 18; true attributable ~$51–93K/yr); three irreconcilable fill-rate universes (platform 100% / OTIF 95–86% / short-ship 69%); mart_channel_contribution case-pack COGS bug (−522% margins); check_canonical rate_map prices Kroger+Sprouts at the 7% fallback (~$285K/yr understated); $91K channel delta rests on inverted COGS assumptions. Fly.io SSOT untouched; local Docker replica stopped but volume kept. Replica certification: seed 1,940,140 rows → dbt 392/392 PASS → guard 10/10 PASS.

**Next:** Pick fix order from the report's priority list — recommended start: PDHA $458K recomputation (arithmetic error in the most-cited figure), then the two platform bugs (mart COGS, guard rate_map).

---

## 2026-06-03 18:25

**Started from:** No active arc. Mart layer existed but consumers read raw.* instead.

**Did:** Added 5 consumer mart models (fct_chargebacks, dim_stores, fct_scan_data, fct_promotions, dim_retailer_requirements), expanded dim_products with 19 columns. Refactored Product Data Health Audit as first consumer — verified output parity. Audited 4 other consumers: CPA and C2C clean, RDR and RVDT need refactors.

**State:** All new models materialized (0 errors). Audit refactor complete and verified. Not pushed.

**Next:** RDR refactor — repoint from int_all_*/stg_retailer_* to fct_*/dim_* marts. May need new mart models for deduction_codes, dispute_evidence, pack_records, EDI requirements, post_audit_claims. Then RVDT refactor.

---

## Session — 2026-05-18 (session 8)

**Started from:** Previous schema had 23 generic tables with no channel isolation. Consumer projects couldn't reconcile across channels because everything was merged at staging.

**Did:** Greenfield rebuild of entire Postgres schema and dbt project.
- Rewrote raw DDL: 23 tables → 37 tables across 3 isolated pipelines (retailer, distributor, DTC) + shared
- Built 7 Python seed generators producing ~1M deterministic rows with realistic patterns (seasonal orders, delist-risk velocity, correlated deductions/disputes)
- Rebuilt all dbt layers: 37 staging views (142 tests), 5 intermediate views (27 tests), 22 mart tables (75 tests)
- Per-channel reconciliation marts (retailer, distributor, DTC) + mart_channel_contribution cross-channel rollup
- Full `dbt build`: 308/308 PASS
- Regenerated dbt docs for GitHub Pages
- Force-pushed to remote (superseded 4 stale data-integrity-arc commits)

**State:** Channel-isolated schema live in Postgres. All 64 models and 244 tests green. dbt docs updated. GitHub Pages will rebuild automatically. Branch protection re-enabled.

**Next:** Update consumer projects (7 repos) to use new channel-prefixed table names. Old names like stg_orders, stg_deductions, fct_orders are gone — replaced by stg_retailer_orders, stg_retailer_deductions, fct_retailer_orders, etc. Start in a new session.

---

## Session — 2026-05-16 (session 7)

**Started from:** Both arcs complete, no active work. Repo public but missing CI, setup docs, and presentation polish.

**Did:** Full four-phase audit (baseline, internal review, landscape scan, synthesis). Executed 7 improvement moves: GitHub Actions CI, .env.example, requirements.txt, portable dbt path, 2 custom generic test macros + 3 singular tests, README badges. Wrote AUDIT.md and logged decisions.

**State:** All audit moves complete. CI green. dbt docs live at msshawnp.github.io/cinderhaven-data-platform/. Custom tests parse clean (not yet run against live DB). No active arc.

**Next:** No active arc. Options: (1) run custom tests against Fly.io Postgres to confirm pass, (2) start data-hygiene portfolio piece using cinderhaven-data-dirty, (3) address dbt deprecation warnings about test argument syntax.

---

## Session — 2026-05-15 (session 6)

**Phase:** Dirty dataset arc — complete.
**Goal:** Execute dirty dataset generator plan — build cinderhaven-data-dirty repo with 6 root-cause degraders.
**Completed:**
- U1+U2: Pipeline framework + RC1 Excel/CSV damage with P0 cascade fix (sku_whitespace)
- U3: RC2 governance decay (near-duplicate SKUs, temporal drift, casing)
- U4: RC3 retailer integration gaps + RC4 ETL pipeline failures
- U5: RC5 manual process errors + RC6 business process gaps
- U6: README, defect manifest generator, 18 tests (all passing)
- GitHub repo created and pushed: MsShawnP/cinderhaven-data-dirty (private)
- Feature branch merged to main, default branch set
- Plan status flipped to completed
- 2 decisions logged, 2 failures logged
**Tried, didn't work:**
- Plan's cascade model assumed UPC/GTIN damage breaks joins — actually all downstream tables join on `sku`. Fixed with sku_whitespace defect.
- UPDATE week_ending hit UNIQUE constraint — fixed with UPDATE OR IGNORE.
- PowerShell inline Python with SQL `||` operator caused parser errors — used temp .py files.
**State:** Dirty dataset arc fully shipped. Both arcs (platform + dirty dataset) complete.
**Next concrete action:** No active arc. User chooses next work (data-hygiene portfolio piece, or other).
**Blockers:** None.

---

## Session — 2026-05-13 (session 5)

**Phase:** All 6 phases complete. Arc nearly done.
**Goal:** Fix Dagster UI, complete Phase 5, execute full Phase 6.
**Completed:**
- P5.3 DONE: Dagster schedule + asset graph verified in UI
  - Root cause of empty graph: Dagster launched from repo root but
    cinderhaven_orchestration package lives under orchestration/. Fixed
    by adding `--working-directory orchestration` to dagster dev args.
  - Daily schedule (6 AM UTC) visible on Automation page
  - 34 assets with full lineage rendered in Global Asset Lineage view
  - .claude/launch.json updated with the fix
- P6.1 DONE: Architecture diagram (Mermaid in docs/architecture.md + README)
- P6.2 DONE: dbt docs generated with 100% description coverage
  - 34/34 models described, 96/96 columns described
  - Site files (index.html + manifest.json + catalog.json) in docs/dbt-docs/
  - Needs: enable GitHub Pages in repo settings (Source: main, /docs folder)
- P6.3 DONE: Walkthrough article (docs/walkthrough.md)
  - Covers source contracts, staging conventions, crosswalk design,
    test philosophy, orchestration approach
- P6.4 DONE: Professional README with architecture diagram, repo structure,
  stack table, documentation links. Removed dbt scaffold boilerplate.
- Final validation: `dbt build` 166/166 PASS (34 models + 132 tests)
- 2 new decisions logged (Dagster working directory, manifest parse pattern)
**Tried, didn't work:** Nothing notable — one clean session.
**State:** All 19 plan tasks complete. `dbt build` green. Dagster running.
**Remaining for arc completion (user actions):**
1. Enable GitHub Pages: repo Settings → Pages → Source: main, /docs folder
2. Final review and flip repo to public
**Environment notes:**
- flyctl proxy is running (PID 12660) — will die on reboot
- Dagster dev was running on port 3000 — may need restart
- dbt.exe full path: C:\Users\mssha\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\LocalCache\local-packages\Python313\Scripts\dbt.exe
- Must cd into cinderhaven/ and set POSTGRES_PASSWORD for dbt commands
**Next concrete action:** Enable GitHub Pages, review repo, flip public.
**Blockers:** None.

---

## Session — 2026-05-12 (continued, session 4)

**Phase:** Phases 3 and 4 complete. Ready for Phase 5 (Dagster).
**Goal:** Build full dbt pipeline — staging through marts.
**Completed:**
- P3.1 DONE: dbt project initialized (dbt-core 1.11.9, dbt-postgres 1.10.0)
  - profiles.yml at ~/.dbt/profiles.yml, uses env_var for password
  - dbt_project.yml: staging (views), intermediate (views), marts (tables)
  - dbt.exe at C:\Users\mssha\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\LocalCache\local-packages\Python313\Scripts\dbt.exe
  - Must cd into cinderhaven/ dir before running dbt commands
  - Must set POSTGRES_PASSWORD env var and have flyctl proxy running
- P3.2 DONE: 23 sources defined (models/staging/sources.yml)
- P3.3 DONE: 23 staging models (views) — date casts, boolean conversions
- P3.4 DONE: 93 staging tests passing
- P4.1 DONE: 3 intermediate models
  - int_deduction_code_crosswalk (codes → rules → retailers)
  - int_product_retailers (SKU × retailer with pricing, margins, distribution)
  - int_retailer_payments (deductions → remittances → retailers → disputes)
- P4.2 DONE: 3 dimension tables (dim_products, dim_retailers, dim_deduction_reasons)
- P4.3 DONE: 5 fact tables (fct_orders, fct_shipments, fct_deductions, fct_chargebacks, fct_payments)
  - fct_orders unifies B2B + DTC (49,474 rows)
- P4.4 DONE: 132 total tests passing (93 staging + 6 intermediate + 33 mart)
  - Referential integrity: fct_orders.sku → dim_products, fct_*.retailer_id → dim_retailers
- Phases 1-4 fully complete
**Tried, didn't work:** Initial dispute outcome accepted_values wrong — fixed
  by checking actual data values in SQLite.
**State:** Full dbt pipeline running. 34 models, 132 tests, zero errors.
**Next concrete action:** P5.1 — Initialize Dagster project, integrate with dbt.
**Blockers:** None.

---

## Session — 2026-05-12 (continued, session 3)

**Phase:** Phase 2 — build it right (infrastructure + data generation)
**Goal:** Complete Phases 1 and 2 — ingestion, data generation, full load.
**Completed:**
- P1.3 DONE: Ingestion script built (scripts/ingest_sqlite_to_postgres.py)
  - Uses Postgres COPY for bulk loading, chunked with reconnection
  - Supports --resume to skip already-loaded tables
  - Required scaling Fly.io to 1GB for scan_data (1.1M rows), scaled back after
- P2.2 DONE: Shopify DTC orders generated (scripts/generate_shopify_orders.py)
  - 10,000 orders, 19,347 line items, $426k total DTC revenue
  - 3,610 unique customers, 2.8 avg orders/customer
  - 18-month window with seasonal patterns, discounts, fulfillment statuses
  - Two normalized tables: shopify_orders + shopify_order_lines
- P2.3 DONE: New Shopify tables loaded into Postgres raw schema
  - raw_schema.sql updated with DTC/e-commerce section
  - All 23 tables verified — 1,217,942 total rows
- Phases 1 and 2 fully complete
**Tried, didn't work:** 256MB Fly instance crashes under scan_data COPY load.
  Tried smaller batches (5k, 500 rows) — still crashed. Fixed by scaling to 1GB
  temporarily. See FAILURES.md.
**State:** All data in Postgres. Ready for Phase 3 (dbt).
**Next concrete action:** P3.1 — Initialize dbt project and configure connection.
**Blockers:** None.

---

## Session — 2026-05-12 (continued)

**Phase:** Phase 2 — build it right (infrastructure)
**Goal:** Begin infrastructure buildout — Postgres, raw schema, data audit.
**Completed:**
- P1.1 DONE: Postgres 17.7 provisioned on Fly.io (cinderhaven-db, iad region, shared-cpu-1x)
  - Database `cinderhaven` created, schema `raw` created
  - Verified: CREATE TABLE, INSERT, SELECT all working
  - Credentials saved to .env (gitignored)
- P1.2 DONE: Raw schema DDL written (sql/raw_schema.sql)
  - All 21 data tables mapped SQLite → Postgres with type annotations
  - Organized by domain: reference, pricing, distribution, transactions, sales, deductions
- P2.1 DONE: Data gap assessment written (docs/data-gap-assessment.md)
  - 21 of 21 tables ready to ingest as-is
  - 1 real gap: Shopify DTC orders (needs generation)
  - 1 partial gap: Payment records (remittances covers v1, defer)
  - 1 deferred by design: EDI (from EDI Pre-flight)
- Existing cinderhaven-data DB path confirmed: C:\Users\mssha\projects\active\cinderhaven-data\data\cinderhaven_product_master.db
- psycopg2-binary installed for Python ingestion
- Enabled remoteControlAtStartup in global settings
**Tried, didn't work:** Nothing notable.
**State:** Infrastructure up, schema designed, gaps assessed. Ready for P1.3.
**Next concrete action:** P1.3 — Build ingestion script (SQLite → Postgres raw schema).
**Blockers:** None.

---

## Session — 2026-05-12

**Phase:** Phase 1 — build the right thing (clarify complete, gates skipped)
**Goal:** Scaffold project and scope the build through /clarify.
**Completed:**
- Scaffolded project via /new-project (directory, state files, git, GitHub remote)
- Private repo at github.com/MsShawnP/cinderhaven-data-platform, tagged v0.1-foundation
- /clarify interview complete — locked in scope, quality bar, role split, deployment targets, data lifecycle
- PLAN.md written with full scope and definition of done
- Decomposed into 6 phases / 19 sub-tasks with dependencies and verification
- Skipped Heavy tier gstack gates — brief + /clarify provided sufficient rigor
- 3 decisions logged to DECISIONS.md
**Tried, didn't work:** Nothing notable — planning session, no code.
**State:** Project scaffolded, plan decomposed, ready to build.
**Next concrete action:** Start P1.1 — Provision Postgres on Fly.io.
**Blockers:** None. User needs Fly.io account access for P1.1.

---

## 2026-05-12 — Project initialized

**Started from:** New project setup via /new-project.

**Did:** Created repo, set up CLAUDE.md/DECISIONS.md/HANDOFF.md/PLAN.md/
FAILURES.md. Brainstorm brief completed — covers pain, portfolio gap,
technical spec, build estimate, and Cinderhaven integration plan.

**State:** Foundation in place. Stack not yet locked in. Ready for
/clarify to scope the first arc.

**Next:** Run /clarify to reach 95% confidence on scope, then
/office-hours and /plan-ceo-review (Heavy tier gates).

---

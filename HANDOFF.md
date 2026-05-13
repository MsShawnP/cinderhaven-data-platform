# Cinderhaven Data Platform — Handoff Log

Session-by-session state. Updated by /log mid-session and /wrap at
session end.

For durable choices, see DECISIONS.md.
For the current work arc, see PLAN.md.
For things that didn't work, see FAILURES.md.

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

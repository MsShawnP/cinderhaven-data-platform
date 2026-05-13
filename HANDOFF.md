# Cinderhaven Data Platform — Handoff Log

Session-by-session state. Updated by /log mid-session and /wrap at
session end.

For durable choices, see DECISIONS.md.
For the current work arc, see PLAN.md.
For things that didn't work, see FAILURES.md.

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

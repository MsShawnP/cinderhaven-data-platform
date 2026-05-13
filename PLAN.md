# Cinderhaven Data Platform — Current Work Plan

The current arc of work. Updated when the arc changes, not every
session. For session-by-session state, see HANDOFF.md.

---

## Goal

Build a portfolio-quality modern data platform (Postgres + dbt +
Dagster) that demonstrates the practice can build data infrastructure.
Primary audience: technical reviewers (client CTOs, fractional CTOs,
hiring managers).

## Why this arc, why now

Every shipped portfolio piece is analytical — queries, dashboards,
reports. None demonstrate data engineering. The platform fills this
gap and becomes the substrate every future piece runs on. Building it
now unblocks Contract-to-Cash (#193) and compounds with every
downstream build.

## Business question this arc answers

Can the practice build real data infrastructure, not just analytical
scripts on bundled files?

## Scope (from /clarify — 2026-05-12)

- **Data source:** Existing cinderhaven-data repo (21 tables, 1.1M+
  rows, SQLite) is the bootstrap. Platform ingests into Postgres,
  transforms via dbt. cinderhaven-data becomes dormant once platform
  is live. Data gaps (Shopify DTC, additional POS) assessed during
  build. EDI deferred to EDI Pre-flight delivery.
- **Deployment:** Postgres on Fly.io, dbt docs on GitHub Pages,
  Dagster hosted or screenshot-documented.
- **Quality bar:** Production polish. No errors, no clunky interfaces,
  professional design/layout. Portfolio impressiveness wins when it
  conflicts with pragmatism.
- **Role split:** Claude Code does technical implementation. User
  directs domain modeling, business logic, narrative, quality review.
- **Consumer migration:** Not in v1. Existing projects stay on SQLite
  submodules. Migration is future work.

## Tasks

See decomposition below for detailed sub-tasks.

---

## Decomposition: Full Platform Build

Goal: Deliver a portfolio-quality data platform with Postgres, dbt,
Dagster, hosted docs, and professional documentation.

### Phase 1: Infrastructure

- [x] P1.1: Provision Postgres on Fly.io
    - Depends on: none
    - Done when: `psql` connects to remote database, can CREATE TABLE
      and INSERT a test row
- [x] P1.2: Design raw schema DDL from existing SQLite tables
    - Depends on: none (can work locally)
    - Done when: SQL file exists with CREATE TABLE for all 16 data
      tables, column types mapped from SQLite → Postgres
- [x] P1.3: Build ingestion script (SQLite → Postgres raw schema)
    - Depends on: P1.1, P1.2
    - Done when: Python script loads all 16 tables from
      cinderhaven-data SQLite into Postgres `raw` schema, row counts
      match source

### Phase 2: Data Gap Assessment + Generation

- [x] P2.1: Audit existing data for gaps against brief
    - Depends on: none
    - Done when: written assessment of what exists vs. what the brief
      specifies (Shopify DTC, POS shape, any missing layers), with
      recommendation on what to generate
- [x] P2.2: Generate missing synthetic data layers
    - Depends on: P2.1 (need gap assessment to know what to build)
    - Done when: new data tables exist in cinderhaven-data SQLite with
      realistic volume and quality, generation scripts documented
- [x] P2.3: Load new data into Postgres raw schema
    - Depends on: P1.3, P2.2
    - Done when: all new tables loaded into Postgres `raw` schema,
      row counts verified

### Phase 3: dbt Foundation + Staging

- [x] P3.1: Initialize dbt project and configure connection
    - Depends on: P1.1 (need Postgres running)
    - Done when: `dbt debug` passes, profiles.yml configured,
      project compiles with no errors
- [x] P3.2: Define dbt sources (raw schema tables)
    - Depends on: P3.1, P1.3 (need dbt project + data in Postgres)
    - Done when: `sources.yml` defines all raw tables, `dbt compile`
      succeeds
- [x] P3.3: Build staging models (raw → typed, cleaned, deduped)
    - Depends on: P3.2
    - Done when: one staging model per source table, `dbt run`
      materializes all staging models without errors
- [x] P3.4: Write staging tests
    - Depends on: P3.3
    - Done when: `dbt test` passes — unique keys, not-null on
      required columns, accepted values where applicable

### Phase 4: dbt Transformation + Marts

- [ ] P4.1: Build intermediate models (entity resolution, crosswalks)
    - Depends on: P3.3
    - Done when: deduction reason code crosswalk, SKU-across-systems
      resolution, and retailer-payment joins modeled as intermediate
      models, `dbt run` succeeds
- [ ] P4.2: Build dimension marts (dim_products, dim_retailers,
      dim_deduction_reasons)
    - Depends on: P4.1
    - Done when: dimension tables materialized, GTIN hierarchy
      modeled, retailer-specific attributes included, `dbt run`
      succeeds
- [ ] P4.3: Build fact marts (fct_orders, fct_shipments,
      fct_chargebacks, fct_deductions, fct_payments)
    - Depends on: P4.1, P4.2
    - Done when: fact tables materialized with correct foreign keys
      to dimensions, `dbt run` succeeds
- [ ] P4.4: Write mart tests (data contracts on critical joins)
    - Depends on: P4.2, P4.3
    - Done when: `dbt test` passes — referential integrity between
      facts and dimensions, deduction-to-order lineage valid,
      financial totals reconcile to source

### Phase 5: Dagster Orchestration

- [ ] P5.1: Initialize Dagster project, integrate with dbt
    - Depends on: P3.1 (need dbt project)
    - Done when: `dagster dev` launches, dbt assets appear in
      Dagster UI
- [ ] P5.2: Define asset dependencies and lineage
    - Depends on: P5.1, P4.3 (need full dbt model graph)
    - Done when: Dagster asset graph shows correct dependency chain
      from raw → staging → intermediate → marts
- [ ] P5.3: Configure scheduling and capture screenshots
    - Depends on: P5.2
    - Done when: schedule defined for full pipeline refresh, asset
      graph screenshot saved to repo docs

### Phase 6: Documentation + Polish

- [ ] P6.1: Create architecture diagram
    - Depends on: none (can draft early, finalize after Phase 4)
    - Done when: SVG/PNG diagram showing source → ingestion →
      warehouse → transformation → marts → consumers, included
      in README
- [ ] P6.2: Generate and host dbt docs on GitHub Pages
    - Depends on: P4.4 (need complete models with descriptions)
    - Done when: dbt docs site live on GitHub Pages, lineage
      graph navigable, every model and column has a description
- [ ] P6.3: Write walkthrough article
    - Depends on: P4.4, P5.3 (need complete platform to write about)
    - Done when: markdown article in repo covering source contracts,
      staging decisions, crosswalk design, test philosophy,
      orchestration approach
- [ ] P6.4: Polish README, repo structure, final validation
    - Depends on: P6.1, P6.2, P6.3
    - Done when: README is professional with architecture diagram,
      repo structure is clean, `dbt build` runs end-to-end with
      zero errors, all docs accurate — ready to flip public

## Out of scope for this arc

- Migrating existing consumer projects to Postgres
- EDI data layers (deferred to EDI Pre-flight)
- Docker Compose / cross-platform "clone and run" setup
- Real-time / streaming ingestion
- Snowflake, BigQuery, or alternative warehouses
- MLOps / feature stores
- Data lake / lakehouse architecture
- Reverse ETL
- Multi-tenant or multi-environment setup
- Metabase/Lightdash dashboard (nice-to-have, not v1 requirement)

## Definition of done for this arc

- [ ] Postgres on Fly.io has all Cinderhaven data loaded
- [ ] dbt models build clean: staging → intermediate → marts
- [ ] dbt tests pass with no failures
- [ ] dbt docs hosted on GitHub Pages with lineage visible
- [ ] Dagster asset graph visible and running
- [ ] Architecture diagram on README
- [ ] Written walkthrough in repo explaining design decisions
- [ ] Repo is private, clean, professional — ready to flip public
- [ ] A technical reviewer browsing the repo sees infrastructure
      capability, not scripts

---

## Arc history

When an arc completes, archive its goal, completion date, and outcome
here. Then start a new arc above. Provides continuity without bloating
the active plan.

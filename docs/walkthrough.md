# Building the Cinderhaven Data Platform

A walkthrough of how the platform was designed, built, and tested —
covering source contracts, staging conventions, crosswalk design,
test philosophy, and orchestration.

---

## Starting point

Cinderhaven is a fictional $25M specialty food brand with 21 tables
of operational data: product master, retailer relationships, EDI
requirements, B2B orders, shipments, deductions, chargebacks,
payments, and POS scan data. All of it lived in a SQLite database
used by analytical scripts. None of the data was in a warehouse.
None of the transformations were tested. There was no lineage.

The platform replaces that with Postgres, dbt, and Dagster.

## Source contracts

The 23 raw tables form explicit source contracts. Each table maps to
a dbt source definition with a description, column inventory, and
data type documentation. The raw schema is append-only: ingestion
scripts write to it, nothing else touches it.

Two tables were generated specifically for this platform:
`shopify_orders` and `shopify_order_lines`. The existing data covered
B2B wholesale but had no direct-to-consumer channel. The generation
script produces 10,000 realistic Shopify orders across 18 months with
seasonal patterns, discount codes, and a 4,000-customer pool. This
fills a real gap in the data — without it, the platform can only
model one side of a CPG business.

### Ingestion approach

The ingestion script uses Postgres COPY for bulk loading — 10-50x
faster than row-by-row inserts. Data is loaded in 25,000-row chunks,
each in its own transaction with reconnection between chunks. This
was necessary because the Fly.io Postgres instance runs on 256MB of
memory. Larger transactions caused out-of-memory kills.

The script supports `--resume` to skip tables already at their
expected row count, allowing recovery from partial failures without
re-ingesting everything.

## Staging conventions

Every raw table gets exactly one staging model. The pattern is
consistent across all 23:

```sql
with source as (
    select * from {{ source('cinderhaven', 'table_name') }}
),

staged as (
    select
        column::type as column_name,
        ...
    from source
)

select * from staged
```

The staging layer does three things and nothing else:

1. **Type casting** — SQLite stores everything as text. Staging
   casts to proper Postgres types: integers, numerics, dates,
   timestamps, booleans.

2. **Column renaming** — Only where the source name is ambiguous
   or inconsistent. Most columns keep their source names.

3. **Null handling** — Explicit null handling for columns that use
   empty strings or sentinel values in the source data.

Staging models are views, not tables. At this data volume (1.2M rows
total, dominated by scan_data's 1.1M), materialized views add
storage cost without meaningful query performance benefit. The 256MB
Fly.io instance benefits from lower storage pressure.

## Crosswalk design

Three intermediate models handle the entity resolution that raw data
can't express:

### Deduction code crosswalk (`int_deduction_code_crosswalk`)

Joins deduction codes to retailer rules to retailers. The raw data
stores deduction reason codes as opaque strings (`SH01`, `QA03`).
The crosswalk resolves these to human-readable descriptions, maps
them to the retailer that uses each code, and attaches the dispute
window and required evidence type. This powers `dim_deduction_reasons`
and makes deduction analysis possible without a lookup table in every
query.

### Product-retailer matrix (`int_product_retailers`)

The raw `sku_costs` table stores retailer-specific pricing as wide
columns: `cost_walmart`, `cost_kroger`, `cost_target`, etc. This
intermediate model unpivots the pricing into a normalized shape
(one row per SKU-retailer pair), calculates margins against base
cost, and counts how many retailers carry each product. This avoids
duplicating the unpivot logic in every mart that needs cross-retailer
analysis.

### Retailer payment resolution (`int_retailer_payments`)

Joins deductions to remittances to retailers, and left-joins to
disputes to attach a `was_disputed` flag. This resolves the payment
chain: a retailer sends a remittance, takes deductions, and some
deductions get disputed. Without this intermediate model, every
payment analysis would need a four-table join.

## Mart structure

The marts layer has three dimensions and five facts.

### Dimensions

- **`dim_products`** — Product master with GTIN validation (13-digit
  check), unit-of-measure hierarchy, case-pack calculations, and
  retailer margin ranges from the product-retailer intermediate.

- **`dim_retailers`** — Retailer profiles with store counts (from
  the stores table), EDI compliance flags, and deduction pattern
  summaries.

- **`dim_deduction_reasons`** — From the crosswalk intermediate.
  Maps reason codes to descriptions, evidence requirements, dispute
  windows, and the retailer that uses each code.

### Facts

- **`fct_orders`** — Unified order fact combining B2B (wholesale
  order_lines + orders) and DTC (Shopify) channels via `UNION ALL`.
  A `channel` column distinguishes `b2b` from `dtc`. Same grain:
  one row per order line.

- **`fct_shipments`** — Shipments joined to orders and pack records.
  Calculates `clean_delivery` (on-time with no damage) and
  `asn_compliant` (advance ship notice meets retailer requirements).

- **`fct_deductions`** — From the retailer payment intermediate.
  Adds calculated `net_recovery` and `net_loss` based on dispute
  outcomes.

- **`fct_chargebacks`** — Chargeback records with retailer and
  reason code references.

- **`fct_payments`** — Remittances joined to retailers with a
  deduction summary: total deducted, deduction count, and net
  payment per remittance.

## Test philosophy

132 dbt tests validate the pipeline at every layer:

**Staging (93 tests):**
- Unique keys on every staging model's primary key
- Not-null on required business columns
- Accepted values on enumerated fields (status codes, channels,
  boolean-like flags)

**Intermediate (6 tests):**
- Unique keys on composite keys (e.g., SKU + retailer)
- Not-null on join keys to catch broken crosswalks early

**Marts (33 tests):**
- Referential integrity: every fact table's foreign keys reference
  valid dimension keys (`relationships` tests)
- Unique keys on fact primary keys
- Not-null on measure columns

The test suite catches real problems. During development, an
`accepted_values` test on `dispute_outcome` failed because the
assumed values (`won`/`lost`/`partial`/`pending`/`expired`) didn't
match actual data (`won_full`/`won_partial`/`lost_deadline`/
`lost_evidence`/`lost_no_response`/`lost_other`/`pending`/
`abandoned`). The test forced the model to be corrected before it
could pass.

## Orchestration approach

Dagster loads all 34 dbt models as software-defined assets via the
`dagster-dbt` integration. The asset graph preserves the full
dependency chain: staging views depend on raw sources, intermediate
views depend on staging, and mart tables depend on intermediate
and staging models.

A single job (`dbt_full_refresh`) materializes all assets. A daily
schedule triggers it at 6 AM UTC. The `@dbt_assets` decorator
handles the translation — each dbt model becomes a Dagster asset
with correct upstream/downstream relationships, visible in the
Dagster UI lineage graph.

The dbt manifest is parsed at Dagster load time (not checked into
the repo as a static artifact). This ensures the asset graph always
reflects the current state of the dbt project.

## Infrastructure decisions

**Postgres on Fly.io** — shared-cpu-1x machine. Scales to 1GB for
bulk ingestion, runs at 256MB steady-state. Cost: near-zero for
portfolio use.

**dbt 1.11** — Latest stable release. Postgres adapter 1.10.
Profiles stored in `~/.dbt/profiles.yml` with `env_var` for the
password.

**Dagster 1.13** — Runs locally via `dagster dev`. The Dagster UI
serves as proof of orchestration capability — the asset graph and
schedule are visible and operational, even without a hosted Dagster
deployment.

## What this demonstrates

The platform is not a production system processing live data. It is
a portfolio piece that demonstrates specific capabilities:

- Designing a multi-layer warehouse schema from raw operational data
- Writing source contracts and enforcing them with tests
- Building crosswalks and entity resolution for messy CPG data shapes
- Modeling facts and dimensions that support real analytical questions
- Orchestrating the pipeline with dependency-aware scheduling
- Documenting lineage so a reviewer can trace any number back to its
  source

These are the same skills required to build a real data platform for
a real $25M brand. The data is synthetic. The engineering is not.

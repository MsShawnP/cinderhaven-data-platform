# Cinderhaven Data Platform — Failure Log

What was attempted that didn't work, why it didn't work, and what was
tried next.

Lower bar than DECISIONS.md — capture failures even when they didn't
produce a durable rule. The whole point: future-you (or future-Claude)
shouldn't re-attempt dead ends because the lesson got lost.

---

## Format

### YYYY-MM-DD — [One-line failure description]

**Attempted:** [What was tried]

**Why it didn't work:** [Concrete reason, not "it broke." If the
failure mode was technical, name the specific issue. If the failure
mode was scope or approach, name that.]

**What we tried instead:** [The next attempt, which may also have
failed and may have its own entry below]

**Status:** Resolved / open / abandoned

**Tags:** [keywords for future text-search]

---

## Entries

### 2026-06-12 — Theoretical fill calibration undershot targets by ~1pt annually

**Attempted:** Back the constrained-order probability out of the §2.1 fill
targets using a theoretical expected-loss constant (0.46) and apply the Q4
dip as a plain target reduction in Nov/Dec.

**Why it didn't work:** Two compounding effects. Q4 months carry ~23% of
annual units (SEASONALITY-weighted), so dipping Q4 below an uncompensated
base drags the annual blend ~0.9pt under target. And the realized per-order
loss was 0.467 (retailer) / 0.474 (distributor), not 0.46 — distributor
lines run larger and cut deeper. Combined: annual fills 0.8–1.5pt low.

**What we tried instead:** Measured both quantities from the generated data
and folded them in: per-channel EXPECTED_CONSTRAINED_LOSS from realized
shorted-order loss, and base rate = target + 0.23 × Q4_FILL_DIP. Annual
fills landed within ±0.64pt of every target on the next run.

**Status:** Resolved

**Tags:** fill rate, calibration, Q4, seasonality, constrained order, Group B

---

### 2026-06-12 — data_defect shortfall share landed at half the §1.4 mix

**Attempted:** Draw shortfall reasons from the §1.4 per-retailer mixes,
then reassign data_defect to allocation when the SKU's data quality score
was ≥ 75 (clean-data SKUs can't have data-defect shorts).

**Why it didn't work:** Only 23/50 SKUs (46%) are eligible, so the
reassignment halved the realized data_defect share (Whole Foods .108 vs
the approved .25) and inflated allocation.

**What we tried instead:** Exact compensation — eligible SKUs carry the
data weight scaled by (1−w)/(e−w), ineligible SKUs redistribute it
proportionally across the other reasons. Same single RNG draw either way,
so fills were untouched. WF data_defect landed at .246 vs .25.

**Status:** Resolved

**Tags:** shortfall reason, mix, data_defect, eligibility, compensation, Group B

---

### 2026-06-12 — .venv pip.exe shim dead; install via python -m pip

**Attempted:** `.venv/Scripts/pip.exe install -r requirements.txt` to
restore the platform venv (dbt etc.).

**Why it didn't work:** pip.exe produced no output and did nothing — a
stale shim (venv created from a since-updated base Python). dbt.exe was
also absent because the venv had no packages at all.

**What we tried instead:** `.venv\Scripts\python.exe -m pip install -r
requirements.txt` — installed cleanly (dbt-core 1.11.11, dbt-postgres
1.10.0).

**Status:** Resolved

**Tags:** venv, pip, dbt, windows, environment

---

### 2026-05-15 — Plan's cascade model assumed UPC/GTIN damage breaks joins

**Attempted:** Design RC1 (Excel/CSV damage) around corrupting UPC and GTIN14
fields, expecting this to cascade into orphan records when downstream tables
(scan_data, order_lines) JOIN back to product_master.

**Why it didn't work:** All downstream tables join on `sku`, not `upc` or
`gtin14`. UPC/GTIN columns only exist in product_master. Corrupting them
creates data quality issues within that table but zero cascade effect on joins.

**What we tried instead:** Added `sku_whitespace` defect type to RC1 — injects
trailing/leading whitespace on `sku` values in scan_data and order_lines. This
creates real orphan records on `LEFT JOIN product_master pm ON sd.sku = pm.sku`
because `'SKU-001 ' != 'SKU-001'`. Produces 22K+ orphans at moderate severity.

**Status:** Resolved

**Tags:** dirty-dataset, cascade, joins, sku, upc, gtin, rc1

---

### 2026-05-15 — UPDATE week_ending hits UNIQUE constraint on shifted dates

**Attempted:** `UPDATE scan_data SET week_ending = date(week_ending, '+1 day')`
for RC4 ETL pipeline failures — shifting week-ending dates by one day for
target retailers to simulate reporting discrepancies.

**Why it didn't work:** scan_data has a UNIQUE constraint on
`(sku, store_id, week_ending)`. Shifting a date +1 day can collide with an
existing row that already has that (sku, store_id, next_week) combination,
causing `sqlite3.IntegrityError`.

**What we tried instead:** Changed to `UPDATE OR IGNORE scan_data SET ...`
so collisions are silently skipped. The skipped rows don't get shifted, which
is acceptable — the defect still affects most target rows and the skip rate
is low.

**Status:** Resolved

**Tags:** dirty-dataset, sqlite, unique-constraint, week-ending, rc4

---

### 2026-05-12 — Fly.io shared-cpu-1x (256MB) crashes during scan_data ingestion

**Attempted:** Load 1.1M-row scan_data table into Postgres via proxy tunnel.
Tried three approaches at 256MB: (1) execute_batch with 5000-row batches,
(2) COPY with 50,000-row chunks, (3) COPY with 5,000-row chunks and
per-chunk reconnection. All three crashed the Postgres process — "server
closed the connection unexpectedly."

**Why it didn't work:** The 256MB shared instance runs out of memory during
sustained write operations. The COPY command, WAL writes, and transaction
log together exceed available RAM on scan_data's volume. Smaller tables
(up to 30k rows) load fine at 256MB.

**What we tried instead:** Scaled machine to 1GB temporarily
(`flyctl machine update --memory 1024`), loaded with 25k-row COPY chunks,
scaled back to 256MB. Completed in ~4.5 minutes for all 21 tables.

**Status:** Resolved

**Tags:** fly.io, postgres, memory, ingestion, scan_data, COPY, OOM

---

### 2026-05-17 — mart_channel_contribution COGS was 3.4% instead of ~50-60%

**Attempted:** `sum(o.quantity * p.cogs_per_unit)` in
mart_channel_contribution.sql — treating quantity as individual units.

**Why it didn't work:** fct_orders.quantity for B2B is *cases*, not
individual units. cogs_per_unit is per individual unit. Missing the
case_pack_qty multiplier produced COGS at 3.4% of revenue instead of
the expected 50-60% for wholesale CPG.

**What we tried instead:** Added `p.case_pack_qty` multiplier. Then
discovered a second bug: DTC quantity is individual units (from Shopify),
so applying case_pack_qty uniformly inflated DTC COGS to 457%. Final
fix is channel-aware: `case when o.channel = 'DTC' then quantity *
cogs_per_unit else quantity * case_pack_qty * cogs_per_unit end`.

**Status:** Resolved

**Tags:** dbt, cogs, mart, case-pack, dtc, channel-contribution, units

---

### 2026-06-03 — dbt --select without + prefix misses upstream dependencies

**Attempted:** `dbt run --select dim_products fct_chargebacks dim_stores fct_scan_data fct_promotions dim_retailer_requirements` to materialize new/modified mart models.

**Why it didn't work:** dim_products depends on int_product_retailers and int_product_distributors (intermediate views). dim_stores depends on stg_stores (staging view). Without the `+` prefix, dbt only materializes the named models, not their upstream dependencies. If those views don't already exist in the database, the mart models fail with "relation does not exist."

**What we tried instead:** `dbt run --select +dim_products +dim_stores +fct_scan_data +fct_chargebacks +fct_promotions +dim_retailer_requirements` — the `+` prefix includes all upstream ancestors. 19 models materialized (13 staging views, 2 intermediate views, 4 mart tables), 0 errors.

**Status:** Resolved

**Tags:** dbt, select, dependencies, upstream, materialization

---

### 2026-05-17 — contract-to-cash headline showed 114 cents per dollar (impossible)

**Attempted:** export_json.py computed "cents per dollar" as
`net_payments / orders_invoiced`, where orders came from fct_orders
filtered by order_date and payments from fct_payments filtered by
received_date, both over CY2025.

**Why it didn't work:** Payment lag means payments received in CY2025
include payments for orders placed in late 2024. The two date filters
capture different cohorts — $16.3M invoiced but $21.6M received,
producing a ratio > 1.0 (114.4 cents per dollar received for every
dollar invoiced).

**What we tried instead:** Changed denominator from orders-based
`b2b_invoiced` to payments-based `pay["b2b_gross"]` so both sides of
the ratio come from the same remittance records. Ratio is now 86.8
cents per dollar — internally consistent. Headline changed from
"invoiced" to "collected" to match.

**Status:** Resolved

**Tags:** temporal-mismatch, cohort, contract-to-cash, date-filter, payments

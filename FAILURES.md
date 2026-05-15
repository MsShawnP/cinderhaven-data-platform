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

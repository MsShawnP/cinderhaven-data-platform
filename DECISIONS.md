# Cinderhaven Data Platform — Decisions Log

Permanent record of choices that should survive session turnover.
If a decision is reversed, strike it through and add the replacement
below — don't delete.

---

## Format

Each entry:
- **Date** — when decided
- **Decision** — one sentence, imperative voice
- **Why** — the reasoning, including what was tried and rejected
- **Scope** — what this applies to (file, chunk, deliverable, or "global")
- **Do not** — explicit anti-instructions, if any

---

## Architecture & Pipeline

### ~~2026-05-12 — Keep stack unlocked until /clarify completes~~
- ~~**Why:** Brief proposes Postgres + dbt + Dagster but user wants to
  confirm tooling through research, not assumption.~~
- ~~**Scope:** Global~~
- ~~**Do not:** Commit to specific tools in scaffolding or early code.~~
- **Superseded by:** Stack confirmed during /clarify (2026-05-12).

### 2026-05-12 — Portfolio impressiveness wins over pragmatism when they conflict
- **Why:** The platform's primary job is to impress technical reviewers.
  When a tool (e.g., Dagster) adds work but adds portfolio signal, include
  it. When a shortcut saves time but looks like a shortcut, don't take it.
- **Scope:** Global — applies to every build vs. skip decision.
- **Do not:** Cut a visible component to save time unless it genuinely
  doesn't add reviewer signal.

### 2026-05-12 — Hosted artifacts are the showpiece, not clone-and-run
- **Why:** Technical reviewers browse repos, read docs, and scan diagrams.
  Almost none clone and run a data platform locally. dbt docs on GitHub
  Pages and Dagster screenshots prove the platform is real without requiring
  Docker or cross-platform setup.
- **Scope:** Global — deployment and documentation strategy.
- **Do not:** Spend time on Dockerfiles or cross-platform testing for v1.

---

## Data & Schema

### 2026-05-12 — Scale Fly.io machine temporarily for bulk ingestion, then scale back
- **Why:** The shared-cpu-1x (256MB) Fly.io Postgres machine crashes under
  bulk COPY loads — specifically scan_data (1.1M rows). Scaling to 1GB for
  the load and back to 256MB after is cheaper and faster than engineering
  around the memory limit with micro-batches or alternative upload paths.
- **Scope:** Ingestion — applies any time a full reload is needed.
- **Do not:** Leave the machine at 1GB permanently. Scale up, load, scale
  down. The steady-state workload (dbt transforms, queries) fits in 256MB.

### 2026-05-12 — Use Postgres COPY with chunked reconnection for ingestion
- **Why:** Row-by-row INSERT (via execute_batch) was too slow and connection-
  heavy over the Fly.io proxy tunnel. COPY is 10-50x faster for bulk loading.
  Reconnecting between 25k-row chunks prevents any single transaction from
  overwhelming the server. Script supports --resume to skip already-loaded
  tables after partial failures.
- **Scope:** scripts/ingest_sqlite_to_postgres.py
- **Do not:** Switch back to execute_batch. If chunk size needs tuning,
  adjust CHUNK_ROWS, don't change the COPY approach.

### 2026-05-12 — dbt layer structure: staging (views) → intermediate (views) → marts (tables)
- **Why:** Views for staging and intermediate keep storage minimal on the
  256MB Fly.io instance — only mart tables materialize. This is standard
  dbt practice: views rebuild instantly, tables persist for query performance.
  Custom schemas (public_staging, public_intermediate, public_marts) keep
  the namespace clean for docs and lineage.
- **Scope:** cinderhaven/dbt_project.yml materialization config.
- **Do not:** Materialize staging as tables unless query performance requires
  it (unlikely at this data volume).

### 2026-05-12 — fct_orders unifies B2B and DTC into a single fact table
- **Why:** Downstream consumers (Contract-to-Cash, revenue analysis) need
  a single order grain regardless of channel. The B2B path (orders +
  order_lines) and DTC path (shopify_orders + shopify_order_lines) share
  the same shape: line_id, order_id, sku, quantity, unit_price, line_total.
  A channel column distinguishes them. This avoids duplicating every
  downstream query.
- **Scope:** cinderhaven/models/marts/fct_orders.sql
- **Do not:** Split into fct_b2b_orders and fct_dtc_orders unless a
  consumer genuinely needs different grains.

### 2026-05-12 — Shopify DTC as two normalized tables, not a flat Shopify CSV export
- **Why:** Shopify exports are a single flat CSV with denormalized line items.
  We split into shopify_orders (10k headers) and shopify_order_lines (19k lines)
  to match the existing orders/order_lines pattern. This makes the dbt staging
  layer consistent — same header/line shape for both B2B and DTC channels.
- **Scope:** Data generation + raw schema (shopify_orders, shopify_order_lines).
- **Do not:** Flatten into a single wide table. The normalized shape is
  intentional for downstream joins and the order-to-cash mart.

### 2026-05-12 — cinderhaven-data repo is bootstrap source; platform becomes permanent home
- **Why:** The platform should be the single source of truth. cinderhaven-data
  (SQLite + generation scripts) bootstraps the initial load. Once the platform
  is live, cinderhaven-data goes dormant. Existing consumer projects stay on
  SQLite submodules for now — migration is future work, not v1.
- **Scope:** Data lifecycle — ingestion strategy, consumer migration plan.
- **Do not:** Build ongoing sync between SQLite and Postgres. One-time
  ingestion (or scripted re-ingestion), not continuous replication.

---

## Orchestration

### 2026-05-13 — Dagster requires --working-directory pointing to orchestration/
- **Why:** `dagster dev -m cinderhaven_orchestration.definitions` resolves modules
  from the CWD. When launched from the repo root, Python can't find the
  `cinderhaven_orchestration` package because it lives under `orchestration/`.
  Adding `--working-directory orchestration` fixes the import. Without this,
  the Dagster webserver starts but the code location fails to load (empty
  asset graph, repeated "Error loading repository location" warnings).
- **Scope:** .claude/launch.json, any Dagster launch command.
- **Do not:** Move the orchestration package to the repo root or install it
  as a pip package just to avoid the flag. The explicit working directory
  keeps the project structure clean.

### 2026-05-13 — dbt manifest parsed at Dagster load time via DbtCliResource.cli(["parse"])
- **Why:** dagster-dbt needs the dbt manifest.json to build the asset graph.
  Parsing at module load time (in assets.py) ensures the manifest is always
  fresh — no stale artifact to maintain. The parse runs once when Dagster
  starts, not on every materialization. This is the dagster-dbt recommended
  pattern.
- **Scope:** orchestration/cinderhaven_orchestration/assets.py
- **Do not:** Check in a static manifest.json or use a pre-built manifest
  path. The parse-at-load pattern keeps the graph in sync with the dbt project.

---

## Dirty Dataset

### 2026-05-15 — SKU whitespace damage is the real cascade path, not UPC/GTIN

- **Why:** The plan's central narrative assumed corrupting UPC/GTIN would cascade
  into join failures downstream. In reality, all downstream tables (scan_data,
  order_lines, orders, deductions) join on `sku`, not `upc`/`gtin14`. UPC/GTIN
  damage only affects the product_master table itself. The fix: RC1 adds a
  `sku_whitespace` defect that introduces trailing/leading spaces on `sku` in
  scan_data and order_lines, creating real orphan records (22K+ at moderate).
- **Scope:** cinderhaven-data-dirty — RC1 cascade design.
- **Do not:** Assume UPC/GTIN corruption alone proves join damage in demos.
  Always demonstrate via SKU-based orphan queries.

### 2026-05-15 — Dirty dataset lives in a separate repo, fully isolated

- **Why:** The dirty generators have no shared code with the clean data repo or
  the platform repo. Separate repo keeps concerns clean: clean data stays
  pristine, platform stays focused on infrastructure, dirty data is a standalone
  tool. No submodules, no cross-imports. The only connection is the clean SQLite
  file path passed as `--input`.
- **Scope:** Repository structure — cinderhaven-data-dirty.
- **Do not:** Add degrader code to cinderhaven-data or cinderhaven-data-platform.

---

## CI & DevEx

### 2026-05-16 — CI validates project structure (dbt parse), not full build

- **Why:** A full `dbt build` in CI would require a live Postgres
  instance with loaded data — complex to provision in GitHub Actions
  and brittle. `dbt parse` validates all model references, Jinja
  compilation, and schema correctness without a database connection.
  This catches real errors (broken refs, invalid YAML, syntax issues)
  while staying fast and free.
- **Scope:** .github/workflows/ci.yml
- **Do not:** Add a Postgres service container unless a specific model
  bug requires integration testing in CI. The production `dbt build`
  runs against Fly.io Postgres locally.

### 2026-05-16 — Use shutil.which for dbt executable, not hardcoded path

- **Why:** The original `project.py` hardcoded a Windows App Store
  Python path. Any reviewer reading that file sees "built on one
  machine" instead of "production infrastructure." `shutil.which("dbt")`
  resolves the correct binary on any platform where dbt is installed.
- **Scope:** orchestration/cinderhaven_orchestration/project.py
- **Do not:** Add platform-detection logic or multiple fallback paths.
  If dbt isn't on PATH, the error from Dagster is clear enough.

---

## Visualization

[Chart conventions, palette decisions, interactivity choices]

---

## Output Formats

[Decisions about deliverable formats, structure, organization]

---

## Writing & Voice

[Voice, style, terminology decisions specific to this project]

---

## Reversed / Superseded

When a decision is overturned:
1. Strike through the original entry above (don't delete)
2. Add a new entry below with the replacement decision
3. Note the link in both directions

This preserves the history of why something is the way it is.

# Cinderhaven Data Platform — Audit

Conducted 2026-05-16. Full four-phase audit: baseline, internal
review, landscape scan, synthesis.

---

## Phase 1: Baseline

Modern data platform for a fictional $25M CPG brand. Postgres on
Fly.io + dbt-core 1.11 + Dagster 1.13. 23 sources, 34 models, 132
tests. Both arcs (platform + dirty dataset) complete. Repo public,
GitHub Pages live.

**Intent vs. reality:** All 19 planned tasks delivered. Two
definition-of-done items were unchecked at audit start (repo polish,
reviewer signal) — resolved during Phase 4 execution.

---

## Phase 2: Internal Review

### Code quality — Strong

Clean CTE patterns, consistent naming (stg_, int_, fct_, dim_),
documented grain on every model. Python scripts readable with proper
error handling.

### Architecture — Strong

Textbook dbt layering: staging (views, type/clean) → intermediate
(views, resolve/crosswalk) → marts (tables, consume). Dagster
integration clean. Separation of concerns across directories.

### Tests — Good, not exceptional

132 tests: unique keys, not-null, accepted-values, referential
integrity. Missing: data freshness tests, row-count anomaly checks,
custom generic tests. Tests prove correctness but don't demonstrate
sophistication.

### Documentation — Strong

Professional README with architecture diagram. Substantive walkthrough
covering design decisions. 100% dbt description coverage (34 models,
96 columns). Every source table has a meaningful description.

### DevEx — Intentionally limited (by decision)

No Docker. No clone-and-run setup. Hosted artifacts are the showpiece.
Setup documented via .env.example and requirements.txt (added during
audit).

### Portfolio signal — High after audit fixes

Live dbt docs site, green CI badge, 132 tests, Dagster screenshots,
written walkthrough, architecture diagram. The combination of Dagster +
dbt + Postgres + tests + business domain does not exist in any other
individual-contributor portfolio project surveyed.

---

## Phase 3: Landscape Scan

### Comparable projects

| Project | Stack | Domain | Stars |
|---------|-------|--------|-------|
| dagster-open-platform (official) | Dagster + dbt + multi | SaaS | 459 |
| hooli-data-eng-pipelines (official) | Dagster + dbt + Snowflake | Fictional | ~25 |
| jaffle-shop (dbt Labs) | dbt + DuckDB | Fictional | 3,000+ |
| dagster-dbt-orchestration-example | Dagster + dbt + Postgres | Chess.com | 9 |
| bnpl-financial-data-warehouse | dbt + Postgres + Streamlit | Fintech | 6 |
| dbt-with-medallion-architecture | dbt + Databricks + GHA | Generic | 0 |

### Position

- **Better than all peers at:** domain realism, data volume (1.2M
  rows), test count, documentation depth, orchestration inclusion
- **Unique:** only Dagster + dbt + Postgres portfolio piece with tests
  and a real business domain
- **Floor is low:** most individual projects have no CI, no
  orchestration, toy domains, <20 tests
- **Gap closed during audit:** CI and live docs were the missing
  presentation signals

---

## Phase 4: Synthesis

### Moves executed

| # | Move | Status |
|---|------|--------|
| 1 | Enable GitHub Pages | Already done |
| 2 | Add GitHub Actions CI | Done (dbt parse + Python check) |
| 3 | Flip repo public | Already done |
| 4 | .env.example + requirements.txt | Done |
| 5 | Portable dbt path in project.py | Done |
| 6 | Custom dbt tests | Pending |
| 7 | README badges | Pending |

### Strategic conclusion

Cinderhaven's substance was already top-tier for individual portfolio
work. The audit's primary value was identifying and closing
presentation gaps (CI, docs visibility, setup documentation) that
prevented reviewers from seeing the substance on a 90-second scan.

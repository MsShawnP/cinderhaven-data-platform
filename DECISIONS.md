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

### 2026-05-12 — cinderhaven-data repo is bootstrap source; platform becomes permanent home
- **Why:** The platform should be the single source of truth. cinderhaven-data
  (SQLite + generation scripts) bootstraps the initial load. Once the platform
  is live, cinderhaven-data goes dormant. Existing consumer projects stay on
  SQLite submodules for now — migration is future work, not v1.
- **Scope:** Data lifecycle — ingestion strategy, consumer migration plan.
- **Do not:** Build ongoing sync between SQLite and Postgres. One-time
  ingestion (or scripted re-ingestion), not continuous replication.

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

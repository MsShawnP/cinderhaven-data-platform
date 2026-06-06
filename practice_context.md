# Practice Context

This file contains the stable facts about the consulting practice. Every portfolio brainstorm should reference this to stay aligned.

---

## Target Buyer

- **Industry:** Specialty food manufacturers (also applies to adjacent CPG: supplements, beverage, specialty beauty)
- **Revenue range:** $10M–$100M
- **Growth stage:** Scaling into or through national retail
- **Pain profile:** Grew faster than their data systems. Lean operations, no dedicated data person, reporting held together with spreadsheets and tribal knowledge.
- **Key personas:** CEO/founder, COO, VP Operations, CFO. Marketing is a secondary persona — the primary buyer is operationally minded and skeptical of marketing as a revenue channel.
- **What they respond to:** Financial and operational framing. Margin math, not feature pitches. "Here's what this costs you" beats "here's what this tool does."
- **What kills credibility:** Getting industry-specific details wrong (GS1 standards, retailer systems, chargeback mechanics, distribution economics). Generic data consulting language. Anything that reads like it was written by someone who hasn't worked with a food brand.

## Consulting Positioning

- **Role:** 1099 Data Consultant
- **Practice identity:** Decision-framework consulting for growing specialty food brands. Not "data analytics" — the value proposition is frameworks that turn data into operational decisions.
- **Differentiation:** Deep specialty food/CPG domain knowledge + data skills. Most data consultants don't know GS1, 1WorldSync, retailer item setup, slotting fees, chargebacks, distributor economics. Most food industry consultants can't build the tools.

## Fictional Company

- **Name:** Cinderhaven Provisions (previously referenced as "Cedar Hollow Provisions" in early briefs — Cinderhaven is the canonical name going forward)
- **Profile:** Fictional $25M specialty food manufacturer, ~90 SKUs across three product lines (Artisan Sauces, Specialty Condiments, Pantry Staples)
- **Retailers:** Walmart, Costco, Whole Foods, UNFI + DTC
- **Growth trajectory:** Targeting $40M–$55M
- **Purpose:** Consistent case study subject across portfolio pieces. Allows all tools and frameworks to appear naturally without forcing them. Fabricated but realistic enough that an industry reader doesn't flinch.
- **Dataset:** Synthetic dataset, 90 SKUs across three product lines, **36 months** of data. The **Cinderhaven Postgres SSOT** (Postgres + dbt + Dagster platform) is the single source of truth; SQLite + CSV + XLSX exports are derived artifacts. ⚠ **Canonical figures (all-in trade cost, trade rate %, chargeback count) are pending a fresh lock** — the Postgres regen of June 2026 (intentional) superseded the May 2026 export, so the all-in figure and rate must be re-read from current Postgres and recorded in `canonical.md` (see governance note below). Definitively retired figures: ~~464 chargebacks~~ (misquote from another project), ~~18 months~~ (always 36), ~~$5.4M all-in~~ (superseded May 2026), ~~$7,174,939 / 26.1%~~ (stale May 2026 SQLite export). Do not cite any of these.

### Canonical Figures Governance (added 2026-06-04)

**Every time Cinderhaven's Postgres is regenerated, a brand-new `canonical.md` must be created — the prior one is superseded and archived, never edited in place.** This is non-negotiable. A May 2026 regen silently moved the trade figures and left every downstream piece citing stale numbers — precisely the SSOT-drift failure this practice sells against. The synthetic data must be governed the way we tell clients to govern theirs.

`canonical.md` records every figure **with its definition attached** (a number without its definition is how the drift went undetected for weeks):

- Regeneration date + the `seed_config.py` params / commit that produced it
- All-in trade cost: exact $, the period it covers (36-month total vs annual), and the component breakdown that defines it
- Trade rate %: the exact denominator (annual net revenue / 36-month total / gross sales)
- Chargeback count: a single integer + counting definition (net of reversals?)
- Deduction counts, window (start–end dates), EBITDA / margin, SKU count
- A statement that all downstream pieces must reconcile to this file

After a new `canonical.md` is locked, every dependent piece is updated to match: Trade Spend Leakage, Deduction Recovery, Contract-to-Cash, Where the Money Comes From, 150 Cases, Chargeback Prediction, Remittance Stub Parsing, Item Setup Form Pre-flight, and the trade-spend diagnostic (which re-exports and re-locks). Lock `seed_config.py` alongside the figure set so a re-run can't move canonical silently again.

## Tool Stack

Current skills — portfolio pieces should use whichever tools fit naturally:

- **Python** — scripting, data processing, web apps, automation
- **SQL** — diagnostic queries, data modeling
- **R / Quarto** — statistical analysis, reproducible reports
- **Streamlit** — interactive web apps and tools
- **HTML/CSS/JS** — standalone reports, web tools
- **Jupyter Notebooks** — exploratory analysis, documented walkthroughs
- **Power BI** — executive dashboards (available but not yet used in a published piece)
- **VS Code / Claude Code** — development environment

New tools and frameworks are welcome when they fit the project and stretch skills.

## Core Principles

These came out of the brainstorming process and should guide every new piece:

1. **Niche everything to the same buyer segment.** Portfolio pieces should compound, not dilute. Each artifact should feel like it belongs to the same coherent practice.
2. **Financial reframe beats feature pitch.** Lead with margin math, cost of inaction, or revenue impact — not technical sophistication or marketing enthusiasm.
3. **Credibility comes from knowing the landmines.** Addressing risks, edge cases, and industry-specific gotchas signals practitioner-level awareness. Getting a detail wrong destroys credibility instantly.
4. **Sequence over parallelism.** Don't build everything at once. Each piece should inform the next.
5. **The fabricated company must feel almost too real.** Lazy fictional companies kill the piece. Cinderhaven should feel like a company the reader might know.
6. **Name real systems and real retailers.** Walmart Item 360, 1WorldSync GDSN, Costco item setup workbook, UNFI new item form. Specificity is the tell.
7. **Every piece should have a natural "foot in the door" offering attached.** A productized engagement (audit, assessment, build) that the portfolio piece is effectively the sales collateral for.

## Domain Knowledge in Play

Specialty food retail mechanics the practice draws on:

- GS1 / GTIN structure and validation
- 1WorldSync / GDSN data pools
- Retailer item setup (Walmart Item 360, Costco, UNFI, KeHE)
- Slotting fees, chargebacks, MCBs (manufacturer chargebacks)
- Distributor margins and economics (UNFI, KeHE)
- Trade spend and promotional deductions
- Case pack math (each/inner/case/pallet)
- OTIF (on-time in-full) requirements
- Velocity reporting and category management
- DTC economics (Shopify, contribution margin vs. retail)
- FSMA Rule 204 and GS1 Sunrise 2027 compliance

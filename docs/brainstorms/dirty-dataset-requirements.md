---
date: 2026-05-15
topic: dirty-dataset
---

# Realistically Degraded Cinderhaven Dataset

## Summary

A new repository containing a clone of the Cinderhaven dataset with
realistic data quality defects introduced through root-cause simulation.
Generators take the clean SQLite database as input and produce a
degraded version with configurable severity, deterministic output, and
defects that cascade the way they do in real CPG operations. Purpose:
supply future data-hygiene portfolio pieces with data that is genuinely
hard to clean.

---

## Problem Frame

The existing Cinderhaven dataset powers 4-6 analytical portfolio pieces
and a full dbt/Dagster platform. It contains some intentional defects
(invalid GTIN check digits, missing brand_owner, inconsistent serving
sizes), but these are self-contained and labeled. The data is too clean
in five specific ways that a technical reviewer would notice:

1. Joins always resolve. No orphan records, no duplicate keys, no
   foreign keys that point nowhere. The generation log fixed
   authorization-mismatch bugs that were actually the realistic part.
2. No type-coercion damage. The most common real product-data defect
   is absent: UPCs with leading zeros stripped by Excel, GTINs in
   scientific notation, numbers stored as text.
3. No duplicate or near-duplicate SKUs. Real catalogs have the same
   product entered twice under different stock codes with slightly
   different descriptions. Cinderhaven's 90 SKUs are all distinct.
4. Defects are a static snapshot. Product master quality doesn't drift
   over the 104-week window. Real masters degrade as SKUs get added
   without governance.
5. Round parameters. 500 / 120 / 80 stores, exactly 30 SKUs per
   product line. Real retail data has uneven counts.

The Olist Brazilian E-Commerce dataset — one of the most heavily
analyzed public e-commerce datasets — demonstrates what realistic
data quality problems look like: status fields contradicting timestamp
columns, category names with spelling variants and versioning
artifacts, geolocation tables with encoding corruption and coordinate
fan-out, payment tables that silently double-count on naive joins,
and referential integrity gaps that drop 2% of rows. These are the
kinds of problems a data hygiene consultant actually encounters.

The clean dataset cannot serve double duty. It powers analytical
consumers that depend on joinable, typed data in Postgres marts. A
separate dirty dataset, isolated in its own repository, lets future
hygiene pieces demonstrate systematic cleaning without risk to
existing products.

---

## Requirements

**Root-cause defect model**

- R1. Defects are organized by root cause (e.g., "Excel opened a CSV
  and stripped leading zeros," "new ops hire bulk-imported SKUs without
  following naming conventions," "Retailer B's EDI system truncates
  fields"), not by symptom. Each root cause produces cascading effects
  across multiple tables.
- R2. The defect catalog includes all five user-identified areas: dirty
  joins, type-coercion damage, duplicate/near-duplicate SKUs, temporal
  quality drift, and non-round parameters.
- R3. The defect catalog extends beyond the five identified areas to
  include other realistic retail/CPG data quality patterns: encoding
  damage, inconsistent casing/whitespace, date format inconsistency,
  status-field contradictions, payment/remittance rounding discrepancies,
  late-arriving or out-of-order records, free-text fields with low
  fill rates, and column-name artifacts (typos, version suffixes).
- R4. Defect patterns draw inspiration from the Olist Brazilian
  E-Commerce dataset's documented quality issues, adapted to
  Cinderhaven's CPG/retail context.

**Cascading interactions**

- R5. Type-coercion damage on identifiers (GTINs, UPCs, stock codes)
  cascades into join failures downstream — scan data, chargebacks,
  and deductions that reference the damaged identifiers cannot resolve
  to the product master.
- R6. Duplicate/near-duplicate SKUs produce split history — some
  transactions land on one entry, some on the other, so neither shows
  complete velocity or deduction exposure.
- R7. Temporal quality drift means records from earlier weeks are
  cleaner than records from later weeks. New SKUs added without
  governance have worse data than the original catalog.
- R8. Cross-system integration gaps mean defect patterns vary by
  retailer. One retailer sends clean data; another truncates fields;
  a third uses inconsistent date formats.

**Configurable severity**

- R9. Generators accept a severity parameter with at least three
  levels (light, moderate, heavy) that controls how many root causes
  activate and how aggressively defects cascade.
- R10. Light severity: a subset of root causes active, mostly contained
  blast radius, roughly 5-10% of records affected. Suitable for
  targeted audit demos.
- R11. Moderate severity: all root causes active, moderate cascading,
  roughly 15-25% of records affected. Suitable for data profiling
  exercises.
- R12. Heavy severity: all root causes active, full cascading, 30%+
  of records affected. Suitable for building a systematic data quality
  framework.

**Same-universe fidelity**

- R13. The dirty dataset uses the same Cinderhaven brand, product
  lines, retailers, and store universe as the clean dataset. A
  reviewer can cross-reference the two.
- R14. The dirty dataset preserves the same table structure and schema
  as the clean dataset. Defects are in the data, not the schema.
- R15. Store counts, SKU-per-line distribution, and similar parameters
  are adjusted to non-round, realistic values.

**Generator design**

- R16. Generators take the clean cinderhaven-data SQLite database as
  input and produce a degraded SQLite database as output.
- R17. Output is deterministic: the same input + same seed + same
  severity level always produces identical output.
- R18. A defect manifest ships with the dataset documenting every
  defect category introduced, the root cause it simulates, affected
  tables and columns, expected rates at each severity level, and
  how defects cascade. Future portfolio pieces reference this manifest
  to know what problems exist and where.

**Repository and packaging**

- R19. The dirty dataset lives in a new, standalone repository
  separate from cinderhaven-data and cinderhaven-data-platform.
- R20. The repository contains the generators, the generated dirty
  SQLite database (at a canonical severity level), and the defect
  manifest.
- R21. No Postgres loading, no dbt models, no orchestration. The
  repo is a data artifact, not a pipeline.

---

## Acceptance Examples

- AE1. **Covers R1, R5.** Given a clean product_master with valid
  GTINs, when the "Excel CSV damage" root cause fires, UPCs on
  affected SKUs lose leading zeros. When scan_data references those
  UPCs, the join to product_master fails — scan records become
  orphans. A downstream deduction referencing the same UPC also
  becomes unresolvable.

- AE2. **Covers R6, R7.** Given the 104-week timeline, when
  temporal drift is active, 8 new SKUs are added in weeks 40-60
  by a simulated ops hire who doesn't follow naming conventions.
  These SKUs have near-duplicate descriptions of existing products,
  inconsistent casing, and missing dimensions. Transaction history
  splits between the original and duplicate entries.

- AE3. **Covers R8.** Given three retailers, Retailer A sends clean
  order data; Retailer B truncates product descriptions to 30
  characters and uses MM/DD/YYYY dates; Retailer C encodes special
  characters incorrectly and occasionally sends negative unit
  quantities (returns mixed into sales rows). The same analytical
  query produces different failure modes per retailer.

- AE4. **Covers R9, R10, R12.** Given the same clean input and
  seed, when severity is set to "light," roughly 5-10% of records
  are affected and defects are mostly contained to their source
  tables. When severity is set to "heavy" with the same seed, 30%+
  of records are affected and type-coercion damage on GTINs cascades
  into orphaned scan records, misattributed chargebacks, and
  unresolvable deductions.

---

## Success Criteria

- A technical reviewer browsing the dirty dataset finds defects that
  feel like they came from a real company's systems, not from a random
  noise generator.
- A future data-hygiene portfolio piece can use the dirty dataset
  without modification — the defect manifest provides enough context
  to build cleaning logic against known problems.
- Running generators twice with the same parameters produces
  identical output.
- The dirty dataset at moderate severity exposes problems in every
  major table (product_master, scan_data, orders, deductions,
  remittances) and requires cross-table investigation to diagnose
  root causes.

---

## Scope Boundaries

- No changes to cinderhaven-data (clean dataset stays untouched)
- No changes to cinderhaven-data-platform (existing consumers unaffected)
- No Postgres loading or deployment in this arc
- No dbt models or tests for the dirty data
- No building the data-hygiene portfolio pieces themselves
- No Docker, CI/CD, or cross-platform setup

---

## Key Decisions

- **Separate repo, not dirty-in-pipeline:** The clean platform powers
  4-6 analytical consumers via Postgres marts. Introducing dirty raw
  data would require reworking 34 dbt models and 132 tests. A separate
  repo avoids that risk entirely and answers a different portfolio
  question ("can you find and fix data problems" vs "can you build
  infrastructure").
- **Root-cause organization over symptom-based:** Defects organized by
  root cause (Excel damage, governance decay, integration gaps) produce
  realistic cascading effects. Symptom-based defects (sprinkle nulls,
  add typos) feel synthetic and don't demonstrate real diagnostic work.
- **Approach B (root-cause ecosystem) over Approach A (sprinkled
  defects):** Stronger portfolio story, more realistic, directly
  supports the "systematic data quality framework" use case the user
  prioritized.
- **Deterministic output:** Seeded RNG so portfolio pieces can
  reference specific known defects. Reproducibility is essential for
  demo reliability.

---

## Dependencies / Assumptions

- The clean cinderhaven-data SQLite database is the input. Generators
  depend on its current schema and table structure.
- If cinderhaven-data's schema changes, generators may need updating.
- The defect catalog assumes a $25M specialty food brand's data
  ecosystem. Defect types and rates are calibrated to CPG/retail, not
  generalized e-commerce.

---

## Outstanding Questions

### Deferred to Planning

- [Affects R2, R3][Needs research] Full defect catalog: which specific
  root causes to simulate, what tables each affects, and what realistic
  rates look like at each severity level. Requires analysis of the
  existing clean dataset's structure to design cascading effects.
- [Affects R15][Technical] Specific non-round parameter values for
  store counts, SKU distribution, and similar adjustments.
- [Affects R18][Technical] Format and structure of the defect manifest
  (markdown, YAML, or embedded in generator docstrings).
- [Affects R19][Technical] Repository name and structure.

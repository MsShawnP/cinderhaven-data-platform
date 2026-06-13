# Cinderhaven Data Platform — Handoff Log

Session-by-session state. Updated by /log mid-session and /wrap at
session end.

For durable choices, see DECISIONS.md.
For the current work arc, see PLAN.md.
For things that didn't work, see FAILURES.md.

---

## 2026-06-13 16:30 — Group E ACCEPTED

**Started from:** Group E mid-implementation. finalize_remittances()
existed in both seed files; schema and staging had new columns;
mart_distributor_reconciliation CTE updated but select/gap formula not.
No reseed or verification run yet.

**Did:** Completed mart_distributor_reconciliation.sql. Fixed FK
circular dependency (skeleton INSERT then UPDATE). Fixed cp1252 Unicode
error. Applied Docker stale-socket workaround. Ran seed ×2
(determinism verified). dbt build 437/437. Built lifecycle waterfall —
86¢ confirmed as honest result (trade 9.53%, fulfillment 0.52%).
Restated lifecycle target 80–85¢ → 85–87¢; canonical relocked to 86¢.

**State:** Groups A–E complete, committed, pushed (ae77cfb). All 437
dbt tests pass. Both channels deterministic, classification rates
>97%, residuals 1–3%. Canonical lifecycle 86¢.

**Next:** Group F — validation + Phase 4 package. Build
PHASE4_DRIFT_REPORT.md with every guard check old-vs-new, headline
recomputes, and the proposed new canonical set for Shawn's approval.

---

## 2026-06-13 — Group D ACCEPTED, GO on Group E

**Started from:** Group D docs complete, stopped at acceptance gate.

**Did:** Shawn reviewed and accepted Group D. All 8 judgment calls
approved as implemented. Denominator mismatch resolved — two separately
denominated canonical metrics replace the retired "16.5% → 65%" pairing:
(1) Recovery rate ~16% per all deduction $ (standalone diagnostic),
(2) Win rate ~42% → ~65% per disputed $ (same denominator, apples to
apples). Old decision struck through in DECISIONS.md with superseded-by
note; two new entries added (denominator decision + Group D acceptance).
Both repos pushed.

**State:** Both repos at origin. Causal repo: 0526d57. Platform repo
synced. Group D is the current certified replica state.

**Next:** Group E — remittance reconstruction (§3.3). generate_remittances:
net = gross − itemized deductions − trade allowances − applied chargebacks
− timing residual (~2% target). Verify: classification rate ≥97% with
actual rate recorded, residual 1–3%, subset≤superset and net≤gross dbt
tests, mart_retailer_reconciliation unexplained gap ≈ residual only.

---

## 2026-06-13 — Group D documentation complete, stopped at acceptance gate

**Started from:** Group D verification 6/8 done, docs remaining (steps
7–8 of the checklist).

**Did:** Recovered C2-state dispute baselines from existing checkpoint
docs (GROUP-C2-VERIFICATION.md §6 and DRIFT-LEDGER.md Group C section) —
no reseed needed. Wrote all three documents in the causal repo:
1. GROUP-D-VERIFICATION.md — 8 judgment calls with alternatives
   considered (tier-conditioned selection, weight_check mapping, DQ
   factor source, ≥75 boundary, DDL column, distributor binary POD,
   filing-delay clip, universal factor application), 4 verification
   gates documented, aggregate statistics, known simplifications
2. DRIFT-LEDGER.md Group D section — disputes 4,233+462 → 7,756+926,
   evidence 12,711 → 34,743, per-disputed ~44% → 41.8% restatement
3. PLAN.md Group D entry marked verified with full summary

C2-state retailer recovered $ was not captured in any checkpoint (only
distributor: $29,458.26 from C2 verification §6). The Group D column
has the complete figures; the restatement from ~44% to ~41% per-disputed
is the meaningful signal for the drift ledger.

**State:** Replica on Group D state (main branch, 2,399,045 rows).
All verification steps complete (8/8). Causal repo committed locally
(bdf9b3c, 4 commits ahead of origin). Platform repo synced with origin.
Nothing pushed per mid-group rule.

**Next:** Shawn reviews Group D — 8 judgment calls in
GROUP-D-VERIFICATION.md §1, especially §1.1 (tier-conditioned selection
is structurally required, not stylistic) and the ~19/51/30 underlying
tier mix (moderate-centered, not 50/25/25 as PLAN sketched). On
acceptance: push both repos, then Group E (remittance reconstruction).

---

## 2026-06-12 (wrap) — Group D verification 6/8 complete, docs remaining

**Started from:** Group D implementation committed (1a9279d), run1
reseed clean, verification pending (8-step checklist).

**Did:** Steps 1–6 of the verification checklist COMPLETE:
1. Spot-checked run1 state — counts match calibration expectations
2. Wrote verify_groupD.py (4 gates + diagnostics, replay from platform
   generators, SCENARIO_B_REDRAW_SEED = 550)
3. Ran all gates — results:
   - dbt: 437/437 PASS
   - Guard: 7/10 (3 expected count REDs, all dollar checks green)
   - Determinism: 41/41 byte-identical (run1 vs run2)
   - Stream preservation: 3/41 changed (exactly retailer_disputes,
     retailer_dispute_evidence, distributor_disputes)
   - Replay linkage: 100% both channels
   - Scenario (a): combined 16.16% of all deduction dollars
   - Scenario (b): redraw 64.76% per disputed dollar
   - Per-tier recovery: 9/10 cells green; distributor strong 60.77%
     RED per-channel (sampling noise on n=478; combined wholesale
     strong 64.37% passes, dbt test grain is combined)
4. Checksums: groupD-run1 and groupD-run2 created in causal repo
5. Determinism: 41/41 identical across independent reseeds
6. dbt build 437/437, guard 7/10

**Retailer dispute aggregates:** 7,756 disputes (3,316/3,515/925
S/M/W), $209,342.28 recovered, $505,414.90 disputed, 14,256 labor
hours, 1,259 pending.

**Distributor dispute aggregates:** 926 disputes (478/343/105),
$47,257.57 recovered, $108,399.17 disputed, 1,689 labor hours,
122 pending.

**NOT done (Task 7):** GROUP-D-VERIFICATION.md, DRIFT-LEDGER.md
Group D section, PLAN.md update. These need C2-state dispute baselines
for the drift ledger's before/after comparison. Attempted to recover
C2 baselines (checked out 6fce555, reseeded C2 state) but the disputes
tables live in Postgres, not the SQLite source file — need to query
Postgres after a C2 reseed, or compute from the C2 checksum state.

**State:** Replica is on Group D state (main branch, Postgres reseed
complete, 2,399,045 rows). verify_groupD.py and both checksum files
exist in the causal repo. Nothing pushed (mid-group rule). Platform
repo 2 commits ahead of origin.

**Next concrete action:** Fresh session should:
1. Recover C2-state dispute baselines — either checkout 6fce555 +
   reseed + query Postgres for retailer_disputes/distributor_disputes
   aggregates (recovered $, disputed $, per-disputed rates), OR
   derive from the C2 verification doc if already recorded there
2. Checkout main, reseed back to Group D, verify 2,399,045 rows
3. Write GROUP-D-VERIFICATION.md (use GROUP-C2-VERIFICATION.md as
   template; §1 = 8 judgment calls from causal repo HANDOFF.md;
   include distributor strong per-channel flag as known observation)
4. Write DRIFT-LEDGER.md Group D section (disputes before→after,
   evidence 12,711→34,743, recovered $ old→new, note 44% restatement)
5. Update causal repo PLAN.md Group D entry
6. STOP at gate — push only at Shawn's acceptance

---

## 2026-06-12 (wrap) — Group D implementation committed, verification pending

**Started from:** C2 accepted; GO on Group D (evidence quality + dispute
outcomes, both channels in one treatment).

**Did:** Group D implementation complete and committed locally (WIP —
unverified, mid-group). §2.5 weakest-link evidence tiers from real
fulfillment factors; §2.4 tier-conditioned outcomes; tier-conditioned
dispute selection (the brand triages by winnability) across ALL written
deductions both channels; evidence rows mirror factor states (POD state
persisted via was_submitted/notes); distributor_disputes gains an
evidence_quality column (DDL); constants frozen after an analytic
calibration dry-run against the certified C2 state
(cinderhaven-causal-fulfillment/verification/calibrate_groupD.py):
expected combined scenario (a) 16.49% of deduction dollars, retailer
dispute rate 40.0% (legacy 40%), distributor 37.2%. Two permanent dbt
tests written (outcome→evidence-assessment linkage with weakest-link
bounds; per-tier recovery ±2pts of curve). Key data findings: flat-rate
disputing cannot reach the 16.5% endpoint (caps ~15.0% even at a 50%
rate) — evidence-correlated selection is structurally required;
product_master_history is EMPTY in the certified state and its 0–4
score scale doesn't match the §2.5 thresholds, so the DQ factor uses
the 40–95 defect-profile score of the order's largest-line_total SKU.

**State:** NOT verified, but run1 reseed COMPLETED clean just before
wrap (282.5s; replica is on the unverified run1 state). Realized
counts landed on calibration: retailer_disputes 7,756 (tiers
3,316/3,515/925 S/M/W), retailer_dispute_evidence 34,743,
distributor_disputes 926 (478/343/105); deductions unchanged
(20,002 / 2,423). Nothing pushed (mid-group rule). Guard expectation
unchanged: 7/10 with the same 3 count REDs (no dispute checks in the
guard).

**Next:** Fresh session: (1) re-run `scripts\seed_all.py` from scratch
(POSTGRES_PASSWORD=postgres, local Docker replica), (2) write
verify_groupD.py + run all gates, (3) reseed ×2 determinism +
checksums vs groupC2-run1 (expect exactly 3 changed tables:
retailer_disputes, retailer_dispute_evidence, distributor_disputes),
(4) dbt build, guard, (5) GROUP-D-VERIFICATION.md (§1 judgment calls
listed in the causal repo's HANDOFF.md; post-fix mix derivation must
be explicit) + DRIFT-LEDGER.md Group D section, (6) stop at gate.
Full cold-start detail in the causal repo's HANDOFF.md.

---

## 2026-06-12 19:00 (wrap)

**Started from:** Group C accepted; C2 (distributor parallel) queued
with cold-start notes in the causal repo.

**Did:** Group C2 shipped (1bb5d0a) and verified — every gate on the
first calibration: short+late 0.318% of distributor shipped $ (band
0.2–0.45), total 0.975% (0.7–1.1), linkage 100% (permanent dbt test),
kept rows identical to the cent, 38/41 tables byte-identical to the
Group C state, determinism 41/41, dbt 431/431, guard 7/10 with exactly
the 3 expected count REDs (distributor 160→678 new). The open
late-trigger question resolved as the 12-day order-to-door window —
Shawn ACCEPTED C2 and APPROVED the window rule as implemented.
Decisions recorded in the causal repo (acceptance/window rule;
standing push-at-every-acceptance rule). Both repos pushed.

**State:** Replica certified on Group C2
(cinderhaven-causal-fulfillment/verification/groupC2-run1-checksums.txt).
Both wholesale channels causal on operational money. Prod untouched,
guard-vs-prod green. Both repos synced with origin.

**Next:** Group D in a fresh session — GO is given. Evidence quality +
dispute outcomes, both channels in one treatment; verification per the
approved decomposition plus the scenario amendment (per-tier ±2pts;
both endpoints as scenarios: current mix ≈16.5%, post-fix mix ≈65%,
each ±2pts). Cold-start notes in the causal repo's HANDOFF.md.

---

## 2026-06-12 — Phase 3 Group C2 complete (causal distributor chargebacks + deductions)

**What changed:** Distributor parallel of Group C. Distributor
short_ship/late_delivery chargebacks and deductions are now triggered
by real distributor_shipment_lines events (streams
FULFILLMENT_SEED+11/+12); late = delivery beyond the 12-day
order-to-door window (distributor orders have no requested_ship_date —
rule choice flagged in the causal repo's GROUP-C2-VERIFICATION.md). No
receiving_discrepancy category distributor-side (no receipt lines per
design §1.6). Quality-linked chargebacks (damaged/pricing_error) and
the three non-operational deduction types ride the legacy stream
unchanged; legacy generators still run verbatim for stream
preservation. Permanent dbt linkage test added; stale distributor
staging descriptions refreshed.

**Verification:** component 0.318% of distributor shipped $ (proposed
band 0.2–0.45), total 0.975% (0.7–1.1) — first calibration; linkage
100% (400+183 cb, 892+290 event deductions); kept rows identical to
the cent; 38/41 tables byte-identical to the Group C state (exactly
the 3 intended distributor money tables); determinism 41/41; dbt
431/431; guard-vs-replica 7/10 with exactly the 3 expected count REDs
(distributor 160→678 new); all dollar checks green; prod untouched.

**State:** Replica certified on Group C2. Drift ledger updated in the
causal repo. Both wholesale channels now causal on operational money.

**Next:** Shawn's review of C2 (the 12-day window rule is the
judgment call), then Group D — causal evidence quality + dispute
outcomes across both channels.

---

## 2026-06-12 23:50 (wrap)

**Started from:** Groups A–B accepted; GO on Group C (causal
chargebacks + deductions, first canon-divergence commit).

**Did:** Group C shipped (fbdaf3b) and verified — every gate on the
first calibration: compliance 1.444% of shipped $, short+late 0.701%,
100% linkage (5,604/5,604, permanent dbt test), Path A and kept
deduction types byte-identical, 36/41 tables identical to the Group B
rollback state, determinism 41/41, dbt 430/430, guard 8/10 with
exactly the 2 expected count REDs. Drift ledger opened in the causal
repo. Shawn accepted Group C and directed **Group C2** — distributor
operational money tables go causal BEFORE Group D (decision in causal
repo DECISIONS.md).

**State:** Replica certified on Group C
(cinderhaven-causal-fulfillment/verification/groupC-run1-checksums.txt
is the C2 baseline). Prod untouched, guard-vs-prod green. Distributor
money seeders still legacy — C2 territory.

**Next:** Group C2 in a fresh session — cold-start notes in the causal
repo's HANDOFF.md (scope, seed streams +11/+12, no requested_ship_date
on distributor orders so the late-trigger rule needs a decision,
distributor band proposal from §1.6).

---

## 2026-06-12 — Phase 3 Group C complete (causal chargebacks + deductions)

**What changed:** First canon-divergence commit. Retailer
short_ship/late_delivery chargebacks and deductions are now triggered
by real Group B fulfillment events; NEW receiving_discrepancy
chargeback category from damage/quality receipt lines (decision #2 —
separate, never folded); Path A data-defect chargebacks unchanged to
the cent (281/$279,330). Legacy generators still run verbatim for
stream preservation; replaced rows are filtered at write time and the
event-driven rows ride FULFILLMENT_SEED+3/+4. Permanent dbt linkage
test added.

**Verification:** compliance 1.444% of shipped $ (band 1.0–1.5),
short+late 0.701% (band 0.5–0.8), all-reason $231K/yr (~$230K design
target) — in band on the first calibration; linkage 100% (5,604/5,604
operational chargebacks join to triggering events); kept deduction
types byte-identical (10,809/$1,032,702), deduction $ −2.2%; 36/41
tables byte-identical to the Group B rollback state (5 intended
changes); determinism 41/41; dbt 430/430; guard-vs-replica 8/10 with
exactly the 2 expected count REDs (677→5,885; 837→6,045), all dollar
checks green; prod untouched.

**State:** Replica on Group C state, fully verified. Drift ledger open
at cinderhaven-causal-fulfillment/verification/DRIFT-LEDGER.md (resolves
at the Phase 4 relock). Event-driven deductions undisputed until Group
D (declared intermediate state). Disputes/evidence/claims drift is
subtractive-only.

**Next:** Shawn's go on Group D — causal evidence quality + dispute
outcomes (§2.5 weakest-link tiers on EVIDENCE_SEED, §2.4 outcome curve,
endpoints as scenarios per decision #4).

---

## 2026-06-12 17:30 (wrap)

**Started from:** Phase 1 design doc at the hard gate awaiting Shawn's review.

**Did:** Gate cleared (6 decisions). Phase 2 complete: mart COGS fix (a6b4d20), rate_map fix + interim trade relock $3.4M/10.5%→$3.7M/11.3% (6e05e33), 4 dbt plausibility assertions (8c41c84), PDHA 12/18→12/36 (e7ce057), COGS ratios in WTMCF (4428f95) + CPA (81e1c34). Phase 3 decomposed into six gated groups; Group A schema+constants (fe62b4d) and Group B causal fulfillment events (0eb21ad) shipped with byte-identical stream-preservation proofs.

**State:** Replica certified on Group B — guard 10/10, dbt 427/427, determinism 41/41, portfolio fill 91.98%, money tables byte-identical to pre-causal state. Rollback point in cinderhaven-causal-fulfillment/verification/. Money seeders (remittances/deductions/disputes/chargebacks) still legacy.

**Next:** Shawn's go on Group C — causal operational chargebacks + deductions. First canon-divergence commit; drift ledger starts. Arc tracking lives in cinderhaven-causal-fulfillment (PLAN.md + HANDOFF.md there are current).

---

## 2026-06-12 17:27

**What changed:** Phase 3 Groups A and B complete — fulfillment schema + frozen causal-model constants (fe62b4d), then causal shipment events: per-line shortfalls on §2.1 fill targets, per-retailer timing, receipt lines, distributor parallel (0eb21ad). Stopped at the Group C gate.

**Why:** Causal-fulfillment arc Phase 3: orders ≠ shipments must originate in the platform data itself, with money tables untouched until Group C makes them causal.

**State:** Replica certified on Group B state — guard 10/10, dbt 427/427, determinism 41/41, portfolio fill 91.98% with every retailer within ±0.64pt of target. 35/41 tables byte-identical to pre-causal state including every money table (chargebacks exactly 677+160). Rollback point: cinderhaven-causal-fulfillment/verification/groupB-certified-state-checksums.txt. Seeders for remittances/deductions/disputes/chargebacks still legacy — Group C territory.

**Next:** Shawn's go on Group C — causal operational chargebacks + deductions. First canon-divergence commit; drift ledger starts there.

---

## 2026-06-12 15:48

**What changed:** Phase 2 bugfixes complete — mart case-pack COGS fix, check_canonical rate_map fix with interim trade relock, and 4 dbt plausibility assertions here; PDHA annualization and COGS-ratio fixes landed in three sibling repos.

**Why:** Phase 2 of the causal-fulfillment arc (design approved by Shawn this morning). Small SQL/Python fixes with large credibility exposure, fixed before any new generation.

**State:** Guard 10/10 PASS, dbt build 396/396, all repos pushed. Trade canon relocked $3.4M/10.5% → $3.7M/11.3% (old values in SUPERSEDES) — pending Shawn's review. Docker replica running. Seeders untouched.

**Next:** Shawn reviews Phase 2 (especially the trade relock). Then Phase 3: causal fulfillment model in the seeders per CAUSAL_FULFILLMENT_DESIGN.md.

---

## 2026-06-12 12:28

**What changed:** Wrote cinderhaven-plausibility-audit.md — read-only plausibility audit of every canonical figure, recomputed on a freeze-guard-certified local replica.

**Why:** check_canonical.py proves figures haven't drifted; this audit asked whether they describe a coherent company. Five REGENERATE-grade findings needed recording before any fix work.

**State:** Audit complete: 6 PLAUSIBLE / 7 RELABEL / 5 REGENERATE. Headliners: $458K/yr product-data cost is a 2× window error (36-mo total annualized as 18; true attributable ~$51–93K/yr); three irreconcilable fill-rate universes (platform 100% / OTIF 95–86% / short-ship 69%); mart_channel_contribution case-pack COGS bug (−522% margins); check_canonical rate_map prices Kroger+Sprouts at the 7% fallback (~$285K/yr understated); $91K channel delta rests on inverted COGS assumptions. Fly.io SSOT untouched; local Docker replica stopped but volume kept. Replica certification: seed 1,940,140 rows → dbt 392/392 PASS → guard 10/10 PASS.

**Next:** Pick fix order from the report's priority list — recommended start: PDHA $458K recomputation (arithmetic error in the most-cited figure), then the two platform bugs (mart COGS, guard rate_map).

---

## 2026-06-03 18:25

**Started from:** No active arc. Mart layer existed but consumers read raw.* instead.

**Did:** Added 5 consumer mart models (fct_chargebacks, dim_stores, fct_scan_data, fct_promotions, dim_retailer_requirements), expanded dim_products with 19 columns. Refactored Product Data Health Audit as first consumer — verified output parity. Audited 4 other consumers: CPA and C2C clean, RDR and RVDT need refactors.

**State:** All new models materialized (0 errors). Audit refactor complete and verified. Not pushed.

**Next:** RDR refactor — repoint from int_all_*/stg_retailer_* to fct_*/dim_* marts. May need new mart models for deduction_codes, dispute_evidence, pack_records, EDI requirements, post_audit_claims. Then RVDT refactor.

---

## Session — 2026-05-18 (session 8)

**Started from:** Previous schema had 23 generic tables with no channel isolation. Consumer projects couldn't reconcile across channels because everything was merged at staging.

**Did:** Greenfield rebuild of entire Postgres schema and dbt project.
- Rewrote raw DDL: 23 tables → 37 tables across 3 isolated pipelines (retailer, distributor, DTC) + shared
- Built 7 Python seed generators producing ~1M deterministic rows with realistic patterns (seasonal orders, delist-risk velocity, correlated deductions/disputes)
- Rebuilt all dbt layers: 37 staging views (142 tests), 5 intermediate views (27 tests), 22 mart tables (75 tests)
- Per-channel reconciliation marts (retailer, distributor, DTC) + mart_channel_contribution cross-channel rollup
- Full `dbt build`: 308/308 PASS
- Regenerated dbt docs for GitHub Pages
- Force-pushed to remote (superseded 4 stale data-integrity-arc commits)

**State:** Channel-isolated schema live in Postgres. All 64 models and 244 tests green. dbt docs updated. GitHub Pages will rebuild automatically. Branch protection re-enabled.

**Next:** Update consumer projects (7 repos) to use new channel-prefixed table names. Old names like stg_orders, stg_deductions, fct_orders are gone — replaced by stg_retailer_orders, stg_retailer_deductions, fct_retailer_orders, etc. Start in a new session.

---

## Session — 2026-05-16 (session 7)

**Started from:** Both arcs complete, no active work. Repo public but missing CI, setup docs, and presentation polish.

**Did:** Full four-phase audit (baseline, internal review, landscape scan, synthesis). Executed 7 improvement moves: GitHub Actions CI, .env.example, requirements.txt, portable dbt path, 2 custom generic test macros + 3 singular tests, README badges. Wrote AUDIT.md and logged decisions.

**State:** All audit moves complete. CI green. dbt docs live at msshawnp.github.io/cinderhaven-data-platform/. Custom tests parse clean (not yet run against live DB). No active arc.

**Next:** No active arc. Options: (1) run custom tests against Fly.io Postgres to confirm pass, (2) start data-hygiene portfolio piece using cinderhaven-data-dirty, (3) address dbt deprecation warnings about test argument syntax.

---

## Session — 2026-05-15 (session 6)

**Phase:** Dirty dataset arc — complete.
**Goal:** Execute dirty dataset generator plan — build cinderhaven-data-dirty repo with 6 root-cause degraders.
**Completed:**
- U1+U2: Pipeline framework + RC1 Excel/CSV damage with P0 cascade fix (sku_whitespace)
- U3: RC2 governance decay (near-duplicate SKUs, temporal drift, casing)
- U4: RC3 retailer integration gaps + RC4 ETL pipeline failures
- U5: RC5 manual process errors + RC6 business process gaps
- U6: README, defect manifest generator, 18 tests (all passing)
- GitHub repo created and pushed: MsShawnP/cinderhaven-data-dirty (private)
- Feature branch merged to main, default branch set
- Plan status flipped to completed
- 2 decisions logged, 2 failures logged
**Tried, didn't work:**
- Plan's cascade model assumed UPC/GTIN damage breaks joins — actually all downstream tables join on `sku`. Fixed with sku_whitespace defect.
- UPDATE week_ending hit UNIQUE constraint — fixed with UPDATE OR IGNORE.
- PowerShell inline Python with SQL `||` operator caused parser errors — used temp .py files.
**State:** Dirty dataset arc fully shipped. Both arcs (platform + dirty dataset) complete.
**Next concrete action:** No active arc. User chooses next work (data-hygiene portfolio piece, or other).
**Blockers:** None.

---

## Session — 2026-05-13 (session 5)

**Phase:** All 6 phases complete. Arc nearly done.
**Goal:** Fix Dagster UI, complete Phase 5, execute full Phase 6.
**Completed:**
- P5.3 DONE: Dagster schedule + asset graph verified in UI
  - Root cause of empty graph: Dagster launched from repo root but
    cinderhaven_orchestration package lives under orchestration/. Fixed
    by adding `--working-directory orchestration` to dagster dev args.
  - Daily schedule (6 AM UTC) visible on Automation page
  - 34 assets with full lineage rendered in Global Asset Lineage view
  - .claude/launch.json updated with the fix
- P6.1 DONE: Architecture diagram (Mermaid in docs/architecture.md + README)
- P6.2 DONE: dbt docs generated with 100% description coverage
  - 34/34 models described, 96/96 columns described
  - Site files (index.html + manifest.json + catalog.json) in docs/dbt-docs/
  - Needs: enable GitHub Pages in repo settings (Source: main, /docs folder)
- P6.3 DONE: Walkthrough article (docs/walkthrough.md)
  - Covers source contracts, staging conventions, crosswalk design,
    test philosophy, orchestration approach
- P6.4 DONE: Professional README with architecture diagram, repo structure,
  stack table, documentation links. Removed dbt scaffold boilerplate.
- Final validation: `dbt build` 166/166 PASS (34 models + 132 tests)
- 2 new decisions logged (Dagster working directory, manifest parse pattern)
**Tried, didn't work:** Nothing notable — one clean session.
**State:** All 19 plan tasks complete. `dbt build` green. Dagster running.
**Remaining for arc completion (user actions):**
1. Enable GitHub Pages: repo Settings → Pages → Source: main, /docs folder
2. Final review and flip repo to public
**Environment notes:**
- flyctl proxy is running (PID 12660) — will die on reboot
- Dagster dev was running on port 3000 — may need restart
- dbt.exe full path: C:\Users\mssha\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\LocalCache\local-packages\Python313\Scripts\dbt.exe
- Must cd into cinderhaven/ and set POSTGRES_PASSWORD for dbt commands
**Next concrete action:** Enable GitHub Pages, review repo, flip public.
**Blockers:** None.

---

## Session — 2026-05-12 (continued, session 4)

**Phase:** Phases 3 and 4 complete. Ready for Phase 5 (Dagster).
**Goal:** Build full dbt pipeline — staging through marts.
**Completed:**
- P3.1 DONE: dbt project initialized (dbt-core 1.11.9, dbt-postgres 1.10.0)
  - profiles.yml at ~/.dbt/profiles.yml, uses env_var for password
  - dbt_project.yml: staging (views), intermediate (views), marts (tables)
  - dbt.exe at C:\Users\mssha\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0\LocalCache\local-packages\Python313\Scripts\dbt.exe
  - Must cd into cinderhaven/ dir before running dbt commands
  - Must set POSTGRES_PASSWORD env var and have flyctl proxy running
- P3.2 DONE: 23 sources defined (models/staging/sources.yml)
- P3.3 DONE: 23 staging models (views) — date casts, boolean conversions
- P3.4 DONE: 93 staging tests passing
- P4.1 DONE: 3 intermediate models
  - int_deduction_code_crosswalk (codes → rules → retailers)
  - int_product_retailers (SKU × retailer with pricing, margins, distribution)
  - int_retailer_payments (deductions → remittances → retailers → disputes)
- P4.2 DONE: 3 dimension tables (dim_products, dim_retailers, dim_deduction_reasons)
- P4.3 DONE: 5 fact tables (fct_orders, fct_shipments, fct_deductions, fct_chargebacks, fct_payments)
  - fct_orders unifies B2B + DTC (49,474 rows)
- P4.4 DONE: 132 total tests passing (93 staging + 6 intermediate + 33 mart)
  - Referential integrity: fct_orders.sku → dim_products, fct_*.retailer_id → dim_retailers
- Phases 1-4 fully complete
**Tried, didn't work:** Initial dispute outcome accepted_values wrong — fixed
  by checking actual data values in SQLite.
**State:** Full dbt pipeline running. 34 models, 132 tests, zero errors.
**Next concrete action:** P5.1 — Initialize Dagster project, integrate with dbt.
**Blockers:** None.

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

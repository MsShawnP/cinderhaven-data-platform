"""Verify CINDERHAVEN_CANONICAL.md against the live SSOT.

Run via Claude Code with the live DB reachable:
    flyctl proxy 5432:5432 -a cinderhaven-db          # in one shell
    DATABASE_URL=postgresql://postgres:<pw>@localhost:5432/cinderhaven \
        python scripts/verify_canonical.py             # in another

Prints, for each headline canonical figure: the value queried from the live
DB (or read from an owner repo's fresh JSON export) next to the value
currently documented in CINDERHAVEN_CANONICAL.md, and flags mismatches.

This does NOT edit canonical. It produces the report a human (or Claude)
uses to reconcile canonical down to the SSOT.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2 not installed. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
# projects/ root = two levels up from the platform repo (…/projects/active datasources/cinderhaven-data-platform)
PROJECTS = ROOT.parent.parent

# (label, documented canonical value) — keep in sync with CINDERHAVEN_CANONICAL.md
DOCUMENTED = {
    "SKU count": 50,
    "Product lines": 5,
    "Contracted retailers": 6,
    "Distributors": 3,
    "Lifecycle retailer (¢/$)": 87.2,
    "Lifecycle combined (¢/$)": 87.3,
    "Deductions backlog cross-channel ($)": 1_350_000,
    "Deductions rows cross-channel": 16_917,
    "Deductions retailer-only ($)": 1_118_682,
    "Deductions retailer rows": 14_947,
    "Chargebacks total": 3_363,
    "Chargebacks retailer": 2_879,
    "Chargebacks distributor": 484,
    "OTIF internal fill": 0.992,
    "OTIF Walmart retailer-scored": 0.845,
    "OTIF total exposure ($)": 57_197,
    "Short-ship total 3yr ($)": 894_174,
    "Channel retail advantage / $1M ($)": 54_000,
    "PDHA product-data cost ($/yr)": 93_000,
    "Trade all-in ($/yr)": 3_600_000,
    "Trade all-in rate": 0.110,
    "Distributor lifecycle (¢/$)": 92.74,   # flagged: predates 06-20 retuning
    "Combined wholesale lifecycle (¢/$)": 88.38,  # flagged
}

rows: list[tuple[str, object, object]] = []  # (label, live, documented)


def rec(label, live):
    rows.append((label, live, DOCUMENTED.get(label)))


def q1(cur, sql):
    cur.execute(sql)
    r = cur.fetchone()
    return r[0] if r else None


def from_db():
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("DATABASE_URL not set — skipping DB queries (JSON checks only).\n")
        return
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    cur.execute("SET search_path TO public_marts, public_staging, raw, public")

    def trydb(label, sql):
        try:
            rec(label, q1(cur, sql))
        except Exception as e:
            conn.rollback()
            rec(label, f"ERR: {e.__class__.__name__}")

    trydb("SKU count", "SELECT COUNT(DISTINCT sku) FROM raw.product_master")
    trydb("Product lines", "SELECT COUNT(DISTINCT product_line) FROM raw.product_master")
    trydb("Contracted retailers", "SELECT COUNT(*) FROM raw.retailers")
    trydb("Distributors", "SELECT COUNT(*) FROM raw.distributors")

    # Lifecycle (retailer b2b): net / gross from payments mart.
    try:
        cur.execute("SELECT SUM(gross_amount), SUM(net_amount) FROM fct_retailer_payments")
        g, n = cur.fetchone()
        rec("Lifecycle retailer (¢/$)", round(float(n) / float(g) * 100, 1) if g else None)
    except Exception as e:
        conn.rollback(); rec("Lifecycle retailer (¢/$)", f"ERR: {e.__class__.__name__}")

    # Deductions retailer-only.
    try:
        cur.execute("SELECT COUNT(*), SUM(deduction_amount) FROM fct_retailer_deductions")
        c, s = cur.fetchone()
        rec("Deductions retailer rows", c)
        rec("Deductions retailer-only ($)", round(float(s)) if s else None)
    except Exception as e:
        conn.rollback(); rec("Deductions retailer rows", f"ERR: {e.__class__.__name__}")

    # Chargebacks — table name uncertain; try a few candidates.
    for label, sql in [
        ("Chargebacks retailer", "SELECT COUNT(*) FROM raw.retailer_chargebacks"),
        ("Chargebacks distributor", "SELECT COUNT(*) FROM raw.distributor_chargebacks"),
    ]:
        trydb(label, sql)

    conn.close()


def from_json():
    """Read derived figures from owner repos' fresh JSON exports if present."""
    def load(rel):
        p = PROJECTS / rel
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    s = load("published/contract-to-cash/frontend/public/json/summary.json")
    if s:
        rec("Lifecycle combined (¢/$)", s.get("combined", {}).get("cents_per_dollar"))

    d = load("published/retailer-deduction-recovery/frontend/public/json/summary.json")
    if d:
        t = d.get("totals", {})
        rec("Deductions backlog cross-channel ($)", round(t.get("deductions_dollar", 0)))
        rec("Deductions rows cross-channel", t.get("deductions_count"))

    o = load("published/otif-blind-spot/frontend/src/data/summary.json")
    if o:
        rec("OTIF internal fill", o.get("internal_fill_rate"))
        rec("OTIF Walmart retailer-scored", o.get("retailer_otif"))
    oe = load("published/otif-blind-spot/frontend/src/data/exposure.json")
    if oe:
        rec("OTIF total exposure ($)", round(oe.get("total_exposure", 0)))

    ss = load("published/short-ship-cost/web/dist/data/validation.json")
    if ss:
        rec("Short-ship total 3yr ($)", round(ss.get("baseline_totals", {}).get("total", 0)))


def main():
    print("=" * 78)
    print("  CANONICAL vs LIVE SSOT")
    print("=" * 78)
    from_db()
    from_json()
    print(f"\n  {'Figure':<42}{'Live':>16}{'Documented':>16}  Δ")
    print("  " + "-" * 76)
    for label, live, doc in rows:
        flag = ""
        if isinstance(live, (int, float)) and isinstance(doc, (int, float)) and doc:
            if abs(live - doc) / abs(doc) > 0.02:
                flag = "  <-- MISMATCH"
        print(f"  {label:<42}{str(live):>16}{str(doc):>16}{flag}")
    print("\n  Reconcile any MISMATCH lines in CINDERHAVEN_CANONICAL.md.")
    print("  (Distributor / combined lifecycle have no live query here — refresh from contract-to-cash distributor run.)")


if __name__ == "__main__":
    main()

"""Assert that Cinderhaven Postgres SSOT matches CINDERHAVEN_CANONICAL.md.

Passes on an additive regen (new tables, new rows in non-cited tables).
Fails when a cited figure moves beyond tolerance.

Requires DATABASE_URL env var or flyctl proxy on localhost:5432.
Set PGPORT to override the default port (e.g. PGPORT=5434).
"""
from __future__ import annotations

import os
import re
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2

CANONICAL_PATH = Path(__file__).resolve().parent.parent / "CINDERHAVEN_CANONICAL.md"

TOLERANCE_PCT = 0.02  # 2% tolerance on dollar figures
TOLERANCE_RATE_PP = 0.005  # 0.5 percentage points on rates

QUERIES = {
    "chargeback_count_retailer": "SELECT COUNT(*) FROM raw.retailer_chargebacks",
    "chargeback_count_distributor": "SELECT COUNT(*) FROM raw.distributor_chargebacks",
    "trailing_52w_revenue": """
        WITH ranked_weeks AS (
            SELECT DISTINCT week_ending
            FROM raw.scan_data
            ORDER BY week_ending DESC
            LIMIT 52
        )
        SELECT SUM(sd.dollars_sold)
        FROM raw.scan_data sd
        WHERE sd.week_ending >= (SELECT MIN(week_ending) FROM ranked_weeks)
    """,
    "channel_revenue": """
        WITH ranked_weeks AS (
            SELECT DISTINCT week_ending
            FROM raw.scan_data
            ORDER BY week_ending DESC
            LIMIT 52
        )
        SELECT r.name, SUM(sd.dollars_sold) AS rev
        FROM raw.scan_data sd
        JOIN raw.stores s ON sd.store_id = s.store_id
        JOIN raw.retailers r ON s.retailer_id = r.retailer_id
        WHERE sd.week_ending >= (SELECT MIN(week_ending) FROM ranked_weeks)
        GROUP BY r.name
    """,
    "channel_rates": """
        SELECT
            AVG(trade_spend_pct_walmart)     AS rate_walmart,
            AVG(trade_spend_pct_costco)      AS rate_costco,
            AVG(trade_spend_pct_whole_foods)  AS rate_whole_foods,
            AVG(trade_spend_pct_unfi)         AS rate_unfi,
            AVG(trade_spend_pct_kehe)         AS rate_kehe,
            AVG(trade_spend_pct_dtc)          AS rate_dtc,
            AVG(trade_spend_pct_regional)     AS rate_regional
        FROM raw.sku_costs
    """,
    "operational_waste_retailer": """
        SELECT SUM(amount)
        FROM raw.retailer_deductions
        WHERE deduction_type != 'promo_billback'
    """,
    "operational_waste_distributor": """
        SELECT COALESCE(SUM(amount), 0)
        FROM raw.distributor_deductions
        WHERE deduction_type != 'promo_billback'
    """,
}


def to_float(val):
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    return float(val)


def parse_canonical():
    text = CANONICAL_PATH.read_text(encoding="utf-8")
    expected = {}

    m = re.search(
        r'\| Chargebacks\s*\|\s*(\d+)\s*\|\s*(\d+) retailer \+ (\d+) distributor',
        text,
    )
    if m:
        expected["chargeback_count_total"] = int(m.group(1))
        expected["chargeback_count_retailer"] = int(m.group(2))
        expected["chargeback_count_distributor"] = int(m.group(3))

    m = re.search(r'\| Scan revenue \(trailing-52w\)\s*\|\s*\$([0-9.]+)M\s*\|', text)
    if m:
        expected["trailing_52w_revenue"] = float(m.group(1)) * 1_000_000

    m = re.search(
        r'\| All-in trade cost \(annualized\)\s*\|\s*\$([0-9.]+)M/yr\s*\|', text
    )
    if m:
        expected["all_in_annual"] = float(m.group(1)) * 1_000_000

    m = re.search(r'\| All-in trade rate\s*\|\s*([0-9.]+)%\s*\|', text)
    if m:
        expected["trade_rate"] = float(m.group(1)) / 100

    return expected


def get_connection():
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    password = os.environ.get("POSTGRES_PASSWORD", "")
    port = int(os.environ.get("PGPORT", "5432"))
    return psycopg2.connect(
        host="localhost", port=port,
        dbname="cinderhaven", user="postgres", password=password,
    )


def approx(actual, expected, tolerance_pct):
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= tolerance_pct


def run_checks():
    expected = parse_canonical()
    required = [
        "chargeback_count_total", "chargeback_count_retailer",
        "chargeback_count_distributor", "trailing_52w_revenue",
        "all_in_annual", "trade_rate",
    ]
    missing = [k for k in required if k not in expected]
    if missing:
        print(f"FAIL: Could not parse from CINDERHAVEN_CANONICAL.md: {missing}")
        return 1

    results = []

    try:
        conn = get_connection()
    except Exception as e:
        print(f"FAIL: Cannot connect to Postgres: {e}")
        print("Set DATABASE_URL or PGPORT, or run flyctl proxy first.")
        return 1

    cur = conn.cursor()

    # 1. Chargeback counts (exact)
    cb_counts = {}
    for key in ("chargeback_count_retailer", "chargeback_count_distributor"):
        cur.execute(QUERIES[key])
        actual = cur.fetchone()[0]
        cb_counts[key] = actual
        ok = actual == expected[key]
        results.append(("PASS" if ok else "FAIL", key, expected[key], actual))

    total_cb = sum(cb_counts.values())
    ok = total_cb == expected["chargeback_count_total"]
    results.append(("PASS" if ok else "FAIL", "chargeback_count_total",
                    expected["chargeback_count_total"], total_cb))

    # 2. Trailing-52w revenue
    cur.execute(QUERIES["trailing_52w_revenue"])
    revenue = to_float(cur.fetchone()[0])
    exp_rev = expected["trailing_52w_revenue"]
    ok = approx(revenue, exp_rev, TOLERANCE_PCT)
    results.append(("PASS" if ok else "FAIL", "trailing_52w_revenue",
                    f"${exp_rev/1e6:.1f}M", f"${revenue/1e6:.2f}M"))

    # 3. Structural trade (annual: rate × trailing-52w channel revenue)
    cur.execute(QUERIES["channel_revenue"])
    channel_rev = {name: to_float(rev) for name, rev in cur.fetchall()}
    cur.execute(QUERIES["channel_rates"])
    rates_row = cur.fetchone()
    rate_keys = ["rate_walmart", "rate_costco", "rate_whole_foods",
                 "rate_unfi", "rate_kehe", "rate_dtc", "rate_regional"]
    rates = {k: to_float(v) for k, v in zip(rate_keys, rates_row)}

    rate_map = {
        "Walmart": rates["rate_walmart"],
        "Costco": rates["rate_costco"],
        "Whole Foods": rates["rate_whole_foods"],
        "UNFI": rates["rate_unfi"],
        "KeHE": rates["rate_kehe"],
        "DTC": rates["rate_dtc"],
    }

    structural_annual = 0.0
    for channel, rev in channel_rev.items():
        rate = rate_map.get(channel, rates["rate_regional"])
        structural_annual += rev * rate

    # 4. Operational waste (36mo total → annualized)
    cur.execute(QUERIES["operational_waste_retailer"])
    ret_waste = to_float(cur.fetchone()[0])
    cur.execute(QUERIES["operational_waste_distributor"])
    dist_waste = to_float(cur.fetchone()[0])
    op_waste_annual = (ret_waste + dist_waste) / 3

    # All-in annual = structural (already annual) + waste (annualized)
    all_in_annual = structural_annual + op_waste_annual

    exp_all_in = expected["all_in_annual"]
    ok = approx(all_in_annual, exp_all_in, TOLERANCE_PCT)
    results.append(("PASS" if ok else "FAIL", "all_in_annual",
                    f"${exp_all_in/1e6:.1f}M", f"${all_in_annual/1e6:.2f}M"))

    # Trade rate (all-in annual / trailing-52w revenue)
    trade_rate = all_in_annual / revenue if revenue else 0
    exp_rate = expected["trade_rate"]
    ok = abs(trade_rate - exp_rate) <= TOLERANCE_RATE_PP
    results.append(("PASS" if ok else "FAIL", "trade_rate",
                    f"{exp_rate*100:.1f}%", f"{trade_rate*100:.1f}%"))

    conn.close()

    # Report
    print("\n=== Canonical Freeze Guard ===\n")
    print(f"  Source: {CANONICAL_PATH.name}")
    print()
    any_fail = False
    for status, name, exp_val, actual in results:
        marker = "OK" if status == "PASS" else "XX"
        print(f"  {marker} {name}: expected {exp_val}, got {actual}")
        if status == "FAIL":
            any_fail = True

    if any_fail:
        print("\nFAIL: Canonical figures have drifted. See CINDERHAVEN_CANONICAL.md.")
        return 1
    else:
        print("\nPASS: All canonical figures match Postgres SSOT.")
        return 0


if __name__ == "__main__":
    sys.exit(run_checks())

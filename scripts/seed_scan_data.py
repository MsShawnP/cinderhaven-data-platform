"""Seed generator for scan_data with velocity delist signals.

Generates weekly POS scan data (sku × store × week_ending) covering
~2 years. Most SKU-store combinations have healthy velocity. A curated
subset of 3-5 SKUs per retailer have organically low/declining velocity
to trigger realistic delist risk signals in the Velocity Decision Tool.

Delist-risk patterns:
  - Low baseline units_sold (1-3 per week vs healthy 5-20)
  - Sparse weeks (gaps in reporting — missing weeks)
  - Declining trend over time (units drop quarter over quarter)
  - Different underperforming SKUs at different retailers

Delist thresholds stay at realistic CPG industry values (10-15 USPW).
The data tells the story — no artificial threshold inflation.

Requires seed_shared.py to have been run first (stores, products exist).

Usage:
    python scripts/seed_scan_data.py
"""
from __future__ import annotations

import io
import psycopg2
from datetime import date, timedelta

from seed_config import (
    ALL_SKUS, DATABASE_URL, RETAILERS, SEASONALITY,
    WINDOW_START, WINDOW_END, init_rng,
)

# Map each retailer to 3-5 SKUs that will underperform at that retailer.
# Different SKUs struggle at different retailers — realistic pattern.
DELIST_RISK_SKUS = {
    "RET-WALMART": ["CHP-SC-008", "CHP-PS-007", "CHP-AS-009", "CHP-SC-010"],
    "RET-COSTCO": ["CHP-AS-003", "CHP-PS-006", "CHP-SC-005"],
    "RET-WHOLEFOODS": ["CHP-PS-001", "CHP-AS-010", "CHP-SC-003", "CHP-AS-005"],
    "RET-SPROUTS": ["CHP-AS-006", "CHP-PS-004", "CHP-SC-007"],
    "RET-KROGER": ["CHP-SC-002", "CHP-PS-008", "CHP-AS-004", "CHP-SC-009"],
    "RET-REGIONAL": ["CHP-PS-010", "CHP-AS-007", "CHP-SC-006"],
}


def get_week_endings(start: date, end: date) -> list[date]:
    """Generate Saturday week-ending dates covering the window."""
    weeks = []
    current = start + timedelta(days=(5 - start.weekday()) % 7)
    while current <= end:
        weeks.append(current)
        current += timedelta(weeks=1)
    return weeks


def generate_healthy_velocity(rng, sku_info, week: date, volume_tier: str) -> tuple[int, float]:
    """Generate normal healthy scan data for a SKU-store-week."""
    base_units = {"high": 15, "medium": 8, "low": 4}[volume_tier]
    seasonal = SEASONALITY.get(week.month, 1.0)

    # Some product lines sell better
    line_mult = {
        "Artisan Sauces": 1.2,
        "Pantry Staples": 1.0,
        "Specialty Condiments": 0.85,
    }.get(sku_info["product_line"], 1.0)

    units = max(1, int(rng.gauss(base_units * seasonal * line_mult, base_units * 0.3)))
    unit_price = sku_info["msrp"]
    dollars = round(units * unit_price * rng.uniform(0.85, 1.05), 2)
    return units, dollars


def generate_delist_risk_velocity(rng, sku_info, week: date, volume_tier: str,
                                  week_index: int, total_weeks: int) -> tuple[int, float] | None:
    """Generate declining/sparse velocity for delist-risk SKUs.

    Returns None to simulate a missing week (sparse reporting).
    """
    # Sparse weeks: 15-30% chance of no data, increasing over time
    sparsity_rate = 0.15 + 0.15 * (week_index / total_weeks)
    if rng.random() < sparsity_rate:
        return None

    # Low baseline: 1-4 units (vs healthy 5-20)
    base = rng.uniform(1.5, 3.5)

    # Declining trend: units decrease over time
    decline_factor = 1.0 - 0.5 * (week_index / total_weeks)
    decline_factor = max(0.2, decline_factor)

    seasonal = SEASONALITY.get(week.month, 1.0)
    units = max(1, int(base * decline_factor * seasonal + rng.gauss(0, 0.5)))

    unit_price = sku_info["msrp"]
    dollars = round(units * unit_price * rng.uniform(0.80, 1.0), 2)
    return units, dollars


def main():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    rng = init_rng(seed=400)

    # Load stores from DB
    cur.execute("SELECT store_id, retailer_id, volume_tier FROM raw.stores")
    stores = cur.fetchall()
    print(f"Loaded {len(stores)} stores")

    # Build lookup
    sku_map = {p["sku"]: p for p in ALL_SKUS}
    weeks = get_week_endings(WINDOW_START, WINDOW_END)
    total_weeks = len(weeks)
    print(f"Generating scan data for {total_weeks} weeks...")

    # Load distribution_log to know which SKUs are authorized at which stores
    cur.execute("SELECT sku, store_id, authorized_date, deauthorized_date FROM raw.distribution_log")
    dist_log = cur.fetchall()
    authorized = {}
    for sku, store_id, auth_date, deauth_date in dist_log:
        auth = auth_date if isinstance(auth_date, date) else (date.fromisoformat(auth_date) if auth_date else WINDOW_START)
        deauth = deauth_date if isinstance(deauth_date, date) else (date.fromisoformat(deauth_date) if deauth_date else WINDOW_END + timedelta(days=365))
        if not deauth:
            deauth = WINDOW_END + timedelta(days=365)
        authorized[(sku, store_id)] = (auth, deauth)

    print(f"Loaded {len(authorized)} distribution authorizations")

    # Generate in chunks to manage memory
    CHUNK_SIZE = 50000
    rows_buf = []
    total_rows = 0

    for week_idx, week in enumerate(weeks):
        for store_id, retailer_id, volume_tier in stores:
            # Find SKUs authorized at this store during this week
            store_skus = [
                sku for sku in sku_map
                if (sku, store_id) in authorized
                and authorized[(sku, store_id)][0] <= week <= authorized[(sku, store_id)][1]
            ]

            delist_skus = set(DELIST_RISK_SKUS.get(retailer_id, []))

            for sku in store_skus:
                sku_info = sku_map[sku]

                if sku in delist_skus:
                    result = generate_delist_risk_velocity(
                        rng, sku_info, week, volume_tier or "medium",
                        week_idx, total_weeks
                    )
                    if result is None:
                        continue
                    units, dollars = result
                else:
                    units, dollars = generate_healthy_velocity(
                        rng, sku_info, week, volume_tier or "medium"
                    )

                rows_buf.append((sku, store_id, str(week), units, dollars))

                if len(rows_buf) >= CHUNK_SIZE:
                    buf = io.StringIO()
                    for row in rows_buf:
                        buf.write("\t".join(str(v) for v in row) + "\n")
                    buf.seek(0)
                    sql = "COPY raw.scan_data (sku, store_id, week_ending, units_sold, dollars_sold) FROM STDIN WITH (FORMAT text)"
                    cur.copy_expert(sql, buf)
                    total_rows += len(rows_buf)
                    rows_buf = []
                    if total_rows % 200000 == 0:
                        print(f"  ...{total_rows:,} rows written")

    # Flush remaining
    if rows_buf:
        buf = io.StringIO()
        for row in rows_buf:
            buf.write("\t".join(str(v) for v in row) + "\n")
        buf.seek(0)
        sql = "COPY raw.scan_data (sku, store_id, week_ending, units_sold, dollars_sold) FROM STDIN WITH (FORMAT text)"
        cur.copy_expert(sql, buf)
        total_rows += len(rows_buf)

    conn.commit()
    print(f"\n  scan_data: {total_rows:,} rows committed")

    # Print delist-risk summary
    print("\nDelist-risk SKU assignments:")
    for rid, skus in DELIST_RISK_SKUS.items():
        names = [sku_map[s]["product_name"] for s in skus if s in sku_map]
        print(f"  {rid}: {', '.join(names)}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

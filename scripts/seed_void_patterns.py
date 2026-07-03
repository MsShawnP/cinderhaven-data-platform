"""Seed void patterns for Void Finder (tool #5).

Two patterns, mirroring how voids happen in the wild:

1. NEVER-SCANNED CLUSTER — the "aha": a mid-2025 planogram reset at
   one retailer+region authorized a product line's star items in
   stores that didn't previously carry them, and the shelf set never
   happened. Implemented as INSERTs into raw.distribution_log for
   (sku, store) pairs that were NOT previously authorized. No scan
   rows exist for those pairs, so nothing is deleted and the locked
   canonical revenue figures cannot move.

2. WENT-DARK SCATTER — items that were selling and quietly stopped:
   a deterministic sample of healthy (sku, store) pairs across all
   retailers loses its scan rows after a cutoff week. Deletions are
   guarded: the script aborts if any retailer loses more than 1% of
   its trailing-52-week scan revenue (canonical tolerance is 2%).

Also creates the voidfinder schema (copack pattern — survives raw
reseeds) with deterministic broker-facing store addresses.

Runs AFTER seed_scan_data. Deterministic: isolated RNG streams
(seed=700 for went-dark, seed=710 for addresses); the cluster is
rule-based (no randomness).

Usage:
    python scripts/seed_void_patterns.py
"""
from __future__ import annotations

from datetime import date, timedelta

import psycopg2

from seed_config import DATABASE_URL, WINDOW_END, init_rng

# ── FROZEN void-pattern constants (cinderhaven-data-v2 addendum) ──
VOID_SEED = 700
ADDRESS_SEED = 710

# Never-scanned cluster: the botched mod reset.
CLUSTER_RETAILER = "RET-KROGER"
CLUSTER_REGION = "Southeast"
# Artisan Sauces spring line extension — star SKUs, deliberately NOT
# in any curated delist-risk/borderline list for this retailer, so
# the Velocity Decision Tool's stories stay untouched.
CLUSTER_SKUS = ["CHP-AS-001", "CHP-AS-002", "CHP-AS-006"]
MOD_RESET_AUTH_DATE = date(2025, 6, 30)

# Went-dark scatter.
WENT_DARK_PAIRS = 30
WENT_DARK_MIN_WEEKS = 8    # dark at least this many weeks by WINDOW_END
WENT_DARK_MAX_WEEKS = 20
# Never touch SKUs whose velocity patterns other tools curate.
CURATED_SKUS = {
    # seed_scan_data.DELIST_RISK_SKUS + BORDERLINE_SKUS, flattened.
    "CHP-SC-008", "CHP-PS-007", "CHP-AS-009", "CHP-SC-010",
    "CHP-AS-003", "CHP-PS-006", "CHP-SC-005",
    "CHP-PS-001", "CHP-AS-010", "CHP-SC-003", "CHP-AS-005",
    "CHP-AS-006", "CHP-PS-004", "CHP-SC-007",
    "CHP-SC-002", "CHP-PS-008", "CHP-AS-004", "CHP-SC-009",
    "CHP-PS-010", "CHP-AS-007", "CHP-SC-006",
    "CHP-AS-002", "CHP-SC-004", "CHP-PS-009",
    "CHP-AS-008", "CHP-SC-001", "CHP-PS-003",
} | set(CLUSTER_SKUS)

# Deletion guard: abort if any retailer loses more than this share of
# its trailing-52-week scan revenue (locked tolerance is 2%).
MAX_RETAILER_REVENUE_DELETION = 0.01

_CITIES = {
    "NY": ["Albany", "Buffalo", "Rochester"], "NJ": ["Newark", "Trenton"],
    "PA": ["Pittsburgh", "Allentown"], "CT": ["Hartford", "Stamford"],
    "MA": ["Worcester", "Springfield"], "NH": ["Manchester", "Nashua"],
    "VT": ["Burlington", "Rutland"], "ME": ["Portland", "Bangor"],
    "RI": ["Providence", "Warwick"], "FL": ["Orlando", "Tampa", "Jacksonville"],
    "GA": ["Atlanta", "Savannah", "Macon"], "NC": ["Charlotte", "Raleigh"],
    "SC": ["Columbia", "Greenville"], "VA": ["Richmond", "Norfolk"],
    "TN": ["Nashville", "Memphis"], "AL": ["Birmingham", "Huntsville"],
    "MS": ["Jackson", "Gulfport"], "LA": ["Baton Rouge", "Shreveport"],
    "IL": ["Springfield", "Peoria", "Rockford"], "OH": ["Columbus", "Dayton"],
    "MI": ["Grand Rapids", "Lansing"], "IN": ["Indianapolis", "Fort Wayne"],
    "WI": ["Madison", "Green Bay"], "MN": ["St. Paul", "Duluth"],
    "IA": ["Des Moines", "Cedar Rapids"], "MO": ["Kansas City", "Springfield"],
    "KS": ["Wichita", "Topeka"], "CA": ["Fresno", "Sacramento", "Bakersfield"],
    "WA": ["Spokane", "Tacoma"], "OR": ["Salem", "Eugene"],
    "CO": ["Denver", "Colorado Springs"], "AZ": ["Phoenix", "Tucson"],
    "NV": ["Reno", "Henderson"], "UT": ["Salt Lake City", "Provo"],
    "HI": ["Honolulu", "Hilo"], "TX": ["Austin", "Fort Worth", "El Paso"],
    "NM": ["Albuquerque", "Santa Fe"], "OK": ["Oklahoma City", "Tulsa"],
    "AR": ["Little Rock", "Fayetteville"],
}
_STREETS = [
    "Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Elm St", "Pine Rd",
    "Washington Blvd", "Lake Ave", "Hill St", "River Rd", "Park Ave",
    "Market St", "Union Ave", "Highland Dr", "Sunset Blvd",
]


def seed_cluster(cur) -> int:
    """Authorize the cluster SKUs across every cluster store that
    doesn't already carry them. Returns pairs inserted."""
    cur.execute(
        "SELECT store_id FROM raw.stores WHERE retailer_id = %s AND region = %s "
        "ORDER BY store_id",
        (CLUSTER_RETAILER, CLUSTER_REGION),
    )
    cluster_stores = [r[0] for r in cur.fetchall()]

    inserted = 0
    for sku in CLUSTER_SKUS:
        cur.execute(
            "SELECT store_id FROM raw.distribution_log WHERE sku = %s",
            (sku,),
        )
        already = {r[0] for r in cur.fetchall()}
        for store_id in cluster_stores:
            if store_id in already:
                continue
            cur.execute(
                "INSERT INTO raw.distribution_log "
                "(sku, store_id, authorized_date, deauthorized_date) "
                "VALUES (%s, %s, %s, NULL)",
                (sku, store_id, MOD_RESET_AUTH_DATE),
            )
            inserted += 1

    # Defensive: a reseeded scan table generated after these auths
    # would have scans for the new pairs — the mod reset means the
    # set never happened, so those rows must not exist.
    cur.execute(
        "DELETE FROM raw.scan_data sd USING raw.stores s "
        "WHERE sd.store_id = s.store_id "
        "AND s.retailer_id = %s AND s.region = %s "
        "AND sd.sku = ANY(%s) AND sd.week_ending >= %s",
        (CLUSTER_RETAILER, CLUSTER_REGION, CLUSTER_SKUS, MOD_RESET_AUTH_DATE),
    )
    return inserted


def pick_went_dark_pairs(cur):
    """Deterministic sample of healthy, currently-scanning pairs."""
    rng = init_rng(seed=VOID_SEED)
    recent_cutoff = WINDOW_END - timedelta(weeks=4)
    cur.execute(
        """
        SELECT sd.sku, sd.store_id
        FROM raw.scan_data sd
        JOIN raw.stores s ON s.store_id = sd.store_id
        WHERE sd.week_ending >= %s
          AND NOT (s.retailer_id = %s AND s.region = %s)
        GROUP BY sd.sku, sd.store_id
        ORDER BY sd.sku, sd.store_id
        """,
        (recent_cutoff, CLUSTER_RETAILER, CLUSTER_REGION),
    )
    candidates = [
        (sku, store_id) for sku, store_id in cur.fetchall()
        if sku not in CURATED_SKUS
    ]
    picked = rng.sample(candidates, min(WENT_DARK_PAIRS, len(candidates)))
    cutoffs = {
        pair: WINDOW_END - timedelta(
            weeks=rng.randint(WENT_DARK_MIN_WEEKS, WENT_DARK_MAX_WEEKS)
        )
        for pair in picked
    }
    return cutoffs


def apply_went_dark(cur, cutoffs) -> dict:
    """Delete scans after each pair's cutoff. Returns deleted dollars
    per retailer for the guard check."""
    deleted_by_retailer: dict[str, float] = {}
    trailing_start = WINDOW_END - timedelta(weeks=52)
    for (sku, store_id), cutoff in sorted(cutoffs.items()):
        cur.execute(
            "SELECT s.retailer_id, COALESCE(SUM(sd.dollars_sold), 0) "
            "FROM raw.scan_data sd JOIN raw.stores s ON s.store_id = sd.store_id "
            "WHERE sd.sku = %s AND sd.store_id = %s AND sd.week_ending > %s "
            "AND sd.week_ending >= %s "
            "GROUP BY s.retailer_id",
            (sku, store_id, cutoff, trailing_start),
        )
        for retailer_id, dollars in cur.fetchall():
            deleted_by_retailer[retailer_id] = (
                deleted_by_retailer.get(retailer_id, 0.0) + float(dollars)
            )
        cur.execute(
            "DELETE FROM raw.scan_data WHERE sku = %s AND store_id = %s "
            "AND week_ending > %s",
            (sku, store_id, cutoff),
        )
    return deleted_by_retailer


def check_deletion_guard(cur, deleted_by_retailer):
    """Abort (raise) if any retailer's trailing-52w revenue drops more
    than MAX_RETAILER_REVENUE_DELETION."""
    trailing_start = WINDOW_END - timedelta(weeks=52)
    cur.execute(
        "SELECT s.retailer_id, SUM(sd.dollars_sold) "
        "FROM raw.scan_data sd JOIN raw.stores s ON s.store_id = sd.store_id "
        "WHERE sd.week_ending >= %s GROUP BY s.retailer_id",
        (trailing_start,),
    )
    remaining = {r: float(d) for r, d in cur.fetchall()}
    for retailer_id, deleted in sorted(deleted_by_retailer.items()):
        original = remaining.get(retailer_id, 0.0) + deleted
        share = deleted / original if original else 0.0
        print(f"  went-dark deletions {retailer_id}: ${deleted:,.0f} "
              f"({share:.2%} of trailing-52w)")
        if share > MAX_RETAILER_REVENUE_DELETION:
            raise RuntimeError(
                f"Deletion guard tripped: {retailer_id} would lose "
                f"{share:.2%} of trailing-52w revenue "
                f"(limit {MAX_RETAILER_REVENUE_DELETION:.0%}). "
                f"Shrink WENT_DARK_PAIRS or the week range."
            )


def seed_addresses(cur) -> int:
    """Deterministic broker-facing addresses for every store."""
    rng = init_rng(seed=ADDRESS_SEED)
    cur.execute("CREATE SCHEMA IF NOT EXISTS voidfinder")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voidfinder.store_addresses (
            store_id TEXT PRIMARY KEY,
            street   TEXT NOT NULL,
            city     TEXT NOT NULL,
            state    TEXT NOT NULL,
            zip      TEXT NOT NULL
        )
        """
    )
    cur.execute("DELETE FROM voidfinder.store_addresses")
    cur.execute("SELECT store_id, state FROM raw.stores ORDER BY store_id")
    rows = cur.fetchall()
    for store_id, state in rows:
        cities = _CITIES.get(state, ["Springfield"])
        street = f"{rng.randint(100, 9899)} {rng.choice(_STREETS)}"
        city = rng.choice(cities)
        zip_code = f"{rng.randint(10000, 99899):05d}"
        cur.execute(
            "INSERT INTO voidfinder.store_addresses "
            "(store_id, street, city, state, zip) VALUES (%s, %s, %s, %s, %s)",
            (store_id, street, city, state, zip_code),
        )
    return len(rows)


def main():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    print("Seeding never-scanned cluster "
          f"({CLUSTER_RETAILER} × {CLUSTER_REGION} × {len(CLUSTER_SKUS)} SKUs)...")
    inserted = seed_cluster(cur)
    print(f"  cluster authorizations inserted: {inserted}")

    print(f"Seeding went-dark scatter ({WENT_DARK_PAIRS} pairs)...")
    cutoffs = pick_went_dark_pairs(cur)
    deleted = apply_went_dark(cur, cutoffs)
    check_deletion_guard(cur, deleted)
    print(f"  pairs darkened: {len(cutoffs)}")

    print("Seeding voidfinder.store_addresses...")
    addr = seed_addresses(cur)
    print(f"  addresses: {addr}")

    conn.commit()
    print("Void patterns committed.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent))
    main()

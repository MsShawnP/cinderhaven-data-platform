"""Generate synthetic Shopify DTC orders for the Cinderhaven platform.

Produces ~10,000 orders over the 18-month window (Dec 2024 - May 2026)
with realistic e-commerce patterns: repeat customers, seasonal demand,
discount codes, and Shopify-style export fields.

Writes two tables into the cinderhaven-data SQLite database:
  - shopify_orders:      order headers
  - shopify_order_lines: line items (1-4 per order)

Pricing uses MSRP (DTC sells at retail). Joins to product_master on SKU.

Usage:
    python scripts/generate_shopify_orders.py
"""

from __future__ import annotations

import hashlib
import random
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

DB_PATH = Path(r"C:\Users\mssha\projects\active\cinderhaven-data\data\cinderhaven_product_master.db")
SEED = 42

WINDOW_START = date(2024, 12, 1)
WINDOW_END = date(2026, 5, 31)
TARGET_ORDERS = 10_000

# Seasonal multipliers by month (1.0 = baseline).
# Higher in Nov-Dec (holiday), lower in Jan-Feb (post-holiday).
SEASONALITY = {
    1: 0.70, 2: 0.75, 3: 0.85, 4: 0.90, 5: 0.95, 6: 1.00,
    7: 0.95, 8: 0.90, 9: 1.00, 10: 1.10, 11: 1.40, 12: 1.50,
}

# Discount codes and their frequencies/depths.
DISCOUNT_CODES = [
    ("WELCOME10", 0.10, 0.08),   # code, depth, probability
    ("HOLIDAY15", 0.15, 0.05),
    ("PANTRY20", 0.20, 0.03),
    ("FREESHIP", 0.00, 0.06),    # free shipping, no line discount
    (None, 0.00, 0.78),          # no discount
]

US_STATES = [
    "CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]
# Weight toward populous states.
STATE_WEIGHTS = [
    15, 12, 11, 10, 8, 7, 6, 6, 5, 5,
    5, 4, 4, 4, 4, 3, 3, 3, 3, 3,
    3, 3, 2, 2, 2, 2, 2, 2, 2, 2,
]

CARRIERS = ["USPS", "UPS", "FedEx"]
CARRIER_WEIGHTS = [50, 30, 20]

FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "James", "Mia", "Benjamin", "Charlotte", "Lucas", "Amelia",
    "Henry", "Harper", "Alexander", "Evelyn", "Daniel", "Abigail", "Jack",
    "Emily", "Sebastian", "Elizabeth", "Owen", "Sofia", "Michael", "Ella",
    "William", "Grace", "David", "Chloe", "Joseph", "Victoria", "Samuel",
    "Riley", "Carter", "Aria", "Wyatt",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]


def generate_email(first: str, last: str, rng: random.Random) -> str:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com"]
    sep = rng.choice([".", "_", ""])
    suffix = rng.randint(1, 999) if rng.random() < 0.4 else ""
    return f"{first.lower()}{sep}{last.lower()}{suffix}@{rng.choice(domains)}"


def make_order_id(n: int) -> str:
    return f"SH-{n:06d}"


def pick_discount(rng: random.Random):
    r = rng.random()
    cumulative = 0.0
    for code, depth, prob in DISCOUNT_CODES:
        cumulative += prob
        if r <= cumulative:
            return code, depth
    return None, 0.0


def generate_customers(rng: random.Random, n: int = 4000) -> list[dict]:
    """Pre-generate a pool of customers. Orders draw from this pool
    with replacement to create realistic repeat-buyer patterns."""
    customers = []
    for _ in range(n):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        email = generate_email(first, last, rng)
        state = rng.choices(US_STATES, weights=STATE_WEIGHTS, k=1)[0]
        customers.append({
            "first_name": first,
            "last_name": last,
            "email": email,
            "state": state,
        })
    return customers


def distribute_orders_by_month(target: int, rng: random.Random) -> dict[tuple[int, int], int]:
    """Spread target orders across months using seasonality weights."""
    months = []
    d = WINDOW_START
    while d <= WINDOW_END:
        months.append((d.year, d.month))
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)

    raw_weights = [SEASONALITY[m] for _, m in months]
    total_weight = sum(raw_weights)
    counts = {}
    allocated = 0
    for i, ym in enumerate(months):
        if i == len(months) - 1:
            counts[ym] = target - allocated
        else:
            c = round(target * raw_weights[i] / total_weight)
            counts[ym] = c
            allocated += c
    return counts


def random_datetime_in_month(year: int, month: int, rng: random.Random) -> datetime:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    start = date(year, month, 1)
    days_in_month = (next_month - start).days
    day = rng.randint(1, days_in_month)
    hour = rng.choices(range(24), weights=[
        1, 1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8,
        7, 6, 5, 5, 6, 7, 8, 9, 8, 6, 4, 2,
    ], k=1)[0]
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return datetime(year, month, day, hour, minute, second)


def main():
    rng = random.Random(SEED)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Load SKUs and MSRP.
    cur.execute("SELECT sku, product_name, msrp FROM product_master WHERE msrp IS NOT NULL ORDER BY sku")
    skus = cur.fetchall()
    if not skus:
        print("ERROR: No SKUs with MSRP found in product_master")
        return

    print(f"Loaded {len(skus)} SKUs from product_master")

    # Drop and recreate tables.
    cur.execute("DROP TABLE IF EXISTS shopify_order_lines")
    cur.execute("DROP TABLE IF EXISTS shopify_orders")

    cur.execute("""
        CREATE TABLE shopify_orders (
            order_id                TEXT PRIMARY KEY,
            order_number            INTEGER NOT NULL,
            created_at              TEXT NOT NULL,
            email                   TEXT NOT NULL,
            financial_status        TEXT NOT NULL,
            fulfillment_status      TEXT NOT NULL,
            shipping_first_name     TEXT NOT NULL,
            shipping_last_name      TEXT NOT NULL,
            shipping_state          TEXT NOT NULL,
            discount_code           TEXT,
            discount_amount         REAL NOT NULL DEFAULT 0.0,
            subtotal                REAL NOT NULL,
            shipping_cost           REAL NOT NULL,
            total_tax               REAL NOT NULL,
            total                   REAL NOT NULL,
            carrier                 TEXT,
            tracking_number         TEXT,
            fulfilled_at            TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE shopify_order_lines (
            line_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id                TEXT NOT NULL,
            sku                     TEXT NOT NULL,
            product_name            TEXT NOT NULL,
            quantity                INTEGER NOT NULL,
            unit_price              REAL NOT NULL,
            line_total              REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES shopify_orders(order_id)
        )
    """)

    customers = generate_customers(rng)
    month_counts = distribute_orders_by_month(TARGET_ORDERS, rng)

    orders = []
    lines = []
    order_num = 1001  # Shopify-style order numbers start above 1000.
    line_count = 0

    for (year, month), count in sorted(month_counts.items()):
        for _ in range(count):
            customer = rng.choice(customers)
            created_at = random_datetime_in_month(year, month, rng)

            # 1-4 line items per order, weighted toward 1-2.
            num_lines = rng.choices([1, 2, 3, 4], weights=[40, 35, 18, 7], k=1)[0]
            chosen_skus = rng.sample(skus, min(num_lines, len(skus)))

            discount_code, discount_depth = pick_discount(rng)

            subtotal = 0.0
            order_lines = []
            for sku_row in chosen_skus:
                sku, product_name, msrp = sku_row
                qty = rng.choices([1, 2, 3], weights=[60, 30, 10], k=1)[0]
                line_total = round(msrp * qty, 2)
                subtotal += line_total
                order_lines.append((sku, product_name, qty, msrp, line_total))

            subtotal = round(subtotal, 2)
            discount_amount = round(subtotal * discount_depth, 2)
            discounted_subtotal = subtotal - discount_amount

            # Shipping: free over $50 or with FREESHIP code, else $5.99-$9.99.
            if discount_code == "FREESHIP" or discounted_subtotal >= 50:
                shipping_cost = 0.0
            else:
                shipping_cost = round(rng.uniform(5.99, 9.99), 2)

            # Tax: 5-10% depending on state (simplified).
            state_hash = int(hashlib.md5(customer["state"].encode()).hexdigest()[:4], 16)
            tax_rate = 0.05 + (state_hash % 50) / 1000.0
            total_tax = round(discounted_subtotal * tax_rate, 2)

            total = round(discounted_subtotal + shipping_cost + total_tax, 2)

            # Fulfillment: 95% fulfilled, 3% unfulfilled, 2% partial.
            roll = rng.random()
            if roll < 0.95:
                fulfillment_status = "fulfilled"
                fulfilled_at = (created_at + timedelta(days=rng.randint(1, 5))).strftime("%Y-%m-%d %H:%M:%S")
                carrier = rng.choices(CARRIERS, weights=CARRIER_WEIGHTS, k=1)[0]
                tracking = f"1Z{rng.randint(100000000, 999999999)}"
            elif roll < 0.98:
                fulfillment_status = "unfulfilled"
                fulfilled_at = None
                carrier = None
                tracking = None
            else:
                fulfillment_status = "partial"
                fulfilled_at = None
                carrier = None
                tracking = None

            # Financial status: mostly paid, some refunded.
            if fulfillment_status == "fulfilled":
                fin_roll = rng.random()
                if fin_roll < 0.94:
                    financial_status = "paid"
                elif fin_roll < 0.98:
                    financial_status = "partially_refunded"
                else:
                    financial_status = "refunded"
            else:
                financial_status = "paid"

            order_id = make_order_id(order_num)
            orders.append((
                order_id, order_num, created_at.strftime("%Y-%m-%d %H:%M:%S"),
                customer["email"], financial_status, fulfillment_status,
                customer["first_name"], customer["last_name"], customer["state"],
                discount_code, discount_amount, subtotal, shipping_cost,
                total_tax, total, carrier, tracking, fulfilled_at,
            ))

            for sku, product_name, qty, unit_price, line_total in order_lines:
                lines.append((order_id, sku, product_name, qty, unit_price, line_total))
                line_count += 1

            order_num += 1

    # Bulk insert.
    cur.executemany("""
        INSERT INTO shopify_orders (
            order_id, order_number, created_at, email, financial_status,
            fulfillment_status, shipping_first_name, shipping_last_name,
            shipping_state, discount_code, discount_amount, subtotal,
            shipping_cost, total_tax, total, carrier, tracking_number,
            fulfilled_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, orders)

    cur.executemany("""
        INSERT INTO shopify_order_lines (order_id, sku, product_name, quantity, unit_price, line_total)
        VALUES (?, ?, ?, ?, ?, ?)
    """, lines)

    conn.commit()

    # Verify.
    cur.execute("SELECT COUNT(*) FROM shopify_orders")
    order_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM shopify_order_lines")
    line_count_db = cur.fetchone()[0]
    cur.execute("SELECT SUM(total) FROM shopify_orders")
    total_revenue = cur.fetchone()[0]
    cur.execute("SELECT MIN(created_at), MAX(created_at) FROM shopify_orders")
    date_range = cur.fetchone()

    print(f"\nGenerated:")
    print(f"  {order_count:,} orders")
    print(f"  {line_count_db:,} line items")
    print(f"  ${total_revenue:,.2f} total DTC revenue")
    print(f"  Date range: {date_range[0][:10]} to {date_range[1][:10]}")

    # Repeat buyer stats.
    cur.execute("""
        SELECT COUNT(DISTINCT email) as customers,
               ROUND(1.0 * COUNT(*) / COUNT(DISTINCT email), 1) as avg_orders
        FROM shopify_orders
    """)
    cust = cur.fetchone()
    print(f"  {cust[0]:,} unique customers, {cust[1]} avg orders/customer")

    conn.close()
    print("\nDone. Tables written to SQLite.")


if __name__ == "__main__":
    main()

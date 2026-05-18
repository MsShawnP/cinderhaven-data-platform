"""Seed generator for the distributor pipeline.

Tables generated:
  - distributor_orders (~2,000 orders)
  - distributor_order_lines (~5,000 lines)
  - distributor_shipments (~2,000)
  - distributor_remittances (~150)
  - distributor_deductions (~600)
  - distributor_disputes (~200)
  - distributor_chargebacks (~150)

Requires seed_shared.py to have been run first (distributors, products exist).

Usage:
    python scripts/seed_distributor.py
"""
from __future__ import annotations

import io
import psycopg2
from collections import defaultdict
from datetime import date, timedelta

from seed_config import (
    ALL_SKUS, DATABASE_URL, DISTRIBUTORS, CARRIERS,
    DISPUTE_OUTCOMES, SEASONALITY,
    WINDOW_START, WINDOW_END, init_rng, WHOLESALE_MULT,
)


def copy_rows(cur, table: str, columns: list[str], rows: list[tuple]):
    buf = io.StringIO()
    for row in rows:
        line = "\t".join("\\N" if v is None else str(v) for v in row)
        buf.write(line + "\n")
    buf.seek(0)
    cols = ", ".join(columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
    cur.copy_expert(sql, buf)


def generate_orders_and_lines(rng):
    orders = []
    lines = []
    order_num = 0

    # Load SKU-distributor mappings at generation time
    # For now, assume all SKUs available to all distributors
    current = WINDOW_START
    while current <= WINDOW_END:
        month_mult = SEASONALITY.get(current.month, 1.0)
        for dist in DISTRIBUTORS:
            orders_this_week = int(rng.gauss(5, 2) * month_mult)
            orders_this_week = max(1, orders_this_week)

            for _ in range(orders_this_week):
                order_num += 1
                order_id = f"DO-{order_num:06d}"
                po_number = f"DPO-{dist['distributor_id'][-4:]}-{order_num:06d}"
                po_date = current + timedelta(days=rng.randint(0, 6))
                if po_date > WINDOW_END:
                    continue

                n_lines = rng.choices([2, 3, 5, 8, 10], weights=[10, 25, 35, 20, 10])[0]
                chosen_skus = rng.sample(ALL_SKUS, min(n_lines, len(ALL_SKUS)))

                total_units = 0
                total_value = 0.0
                order_lines = []

                key = dist["name"].lower().replace(" ", "_")
                if key == "dpi_northwest":
                    key = "regional"
                mult = WHOLESALE_MULT.get(key, 0.45)

                for sku_info in chosen_skus:
                    units = rng.choices(
                        [12, 24, 48, 96, 144],
                        weights=[10, 25, 35, 20, 10]
                    )[0]
                    unit_price = round(sku_info["msrp"] * mult, 2)
                    line_total = round(units * unit_price, 2)

                    total_units += units
                    total_value += line_total
                    order_lines.append((
                        order_id, sku_info["sku"], units, unit_price, line_total,
                    ))

                orders.append((
                    order_id, dist["distributor_id"], po_number, str(po_date),
                    total_units, round(total_value, 2),
                ))
                lines.extend(order_lines)

        current += timedelta(weeks=1)

    return orders, lines


def generate_shipments(rng, orders):
    shipments = []
    for i, order in enumerate(orders):
        order_id = order[0]
        po_date = date.fromisoformat(order[3])
        ship_date = po_date + timedelta(days=rng.randint(1, 4))
        delivery_date = ship_date + timedelta(days=rng.randint(2, 10))
        carrier = rng.choice(CARRIERS)
        units_shipped = order[4]

        shipments.append((
            f"DS-{i+1:06d}", order_id, str(ship_date),
            str(delivery_date) if delivery_date <= WINDOW_END else None,
            carrier, units_shipped,
        ))
    return shipments


def generate_remittances(rng, orders):
    by_dist_month = defaultdict(list)
    for order in orders:
        dist_id = order[1]
        po_date = date.fromisoformat(order[3])
        month_key = (dist_id, po_date.year, po_date.month)
        by_dist_month[month_key].append(order)

    remittances = []
    rem_num = 0
    remittance_order_map = {}

    for (did, year, month), month_orders in sorted(by_dist_month.items()):
        rem_num += 1
        rem_id = f"DREM-{rem_num:04d}"
        gross = sum(float(o[5]) for o in month_orders)
        deduction_rate = rng.uniform(0.05, 0.12)
        total_ded = round(gross * deduction_rate, 2)
        net = round(gross - total_ded, 2)
        received = date(year, month, 1) + timedelta(days=rng.randint(28, 60))
        if received > WINDOW_END:
            received = WINDOW_END

        remittances.append((
            rem_id, did, str(received),
            round(gross, 2), round(net, 2), total_ded,
        ))
        for o in month_orders:
            remittance_order_map[o[0]] = rem_id

    return remittances, remittance_order_map


def generate_deductions(rng, orders, remittance_map):
    dist_ded_types = ["short_ship", "pricing_error", "promo_billback", "damaged", "late_delivery"]
    deductions = []
    ded_num = 0

    for order in orders:
        if rng.random() > 0.20:
            continue
        order_id = order[0]
        dist_id = order[1]
        po_date = date.fromisoformat(order[3])
        order_value = float(order[5])
        rem_id = remittance_map.get(order_id)

        n_deds = rng.choices([1, 2], weights=[80, 20])[0]
        for _ in range(n_deds):
            ded_num += 1
            ded_type = rng.choice(dist_ded_types)
            amount = round(order_value * rng.uniform(0.02, 0.10), 2)
            ded_date = po_date + timedelta(days=rng.randint(20, 50))
            if ded_date > WINDOW_END:
                continue

            deductions.append((
                f"DD-{ded_num:06d}", dist_id, order_id, rem_id,
                ded_type, amount, str(ded_date),
            ))

    return deductions


def generate_disputes(rng, deductions):
    disputes = []
    disp_num = 0

    for ded in deductions:
        if rng.random() > 0.35:
            continue
        disp_num += 1
        ded_date = date.fromisoformat(ded[6])
        filed = ded_date + timedelta(days=rng.randint(3, 25))
        if filed > WINDOW_END:
            continue

        outcome = rng.choices(DISPUTE_OUTCOMES, weights=[25, 30, 30, 15])[0]
        ded_amount = float(ded[5])
        if outcome == "won":
            recovered = ded_amount
        elif outcome == "partial":
            recovered = round(ded_amount * rng.uniform(0.25, 0.70), 2)
        elif outcome == "pending":
            recovered = None
        else:
            recovered = 0.0

        closed = None
        if outcome != "pending":
            closed = filed + timedelta(days=rng.randint(14, 75))
            if closed > WINDOW_END:
                closed = None

        labor = round(rng.uniform(0.25, 3.0), 2)

        disputes.append((
            f"DDISP-{disp_num:05d}", ded[0], str(filed),
            outcome, recovered, str(closed) if closed else None, labor,
        ))

    return disputes


def generate_chargebacks(rng):
    reasons = ["short_ship", "pricing_error", "damaged", "late_delivery"]
    rows = []
    current = WINDOW_START
    while current <= WINDOW_END:
        month_date = date(current.year, current.month, 1)
        for dist in DISTRIBUTORS:
            n_cbs = rng.randint(0, 3)
            for _ in range(n_cbs):
                sku = rng.choice(ALL_SKUS)["sku"]
                reason = rng.choice(reasons)
                amount = round(rng.uniform(100, 3000), 2)
                rows.append((str(month_date), dist["distributor_id"], reason, sku, amount))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return rows


def main():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    rng = init_rng(seed=200)

    print("\nGenerating distributor orders and lines...")
    orders, lines = generate_orders_and_lines(rng)
    copy_rows(cur, "raw.distributor_orders",
              ["order_id", "distributor_id", "po_number", "po_date",
               "total_units", "total_value"],
              orders)
    print(f"  distributor_orders: {len(orders)} rows")

    copy_rows(cur, "raw.distributor_order_lines",
              ["order_id", "sku", "units_ordered", "unit_price", "line_total"],
              lines)
    print(f"  distributor_order_lines: {len(lines)} rows")

    print("Generating shipments...")
    shipments = generate_shipments(rng, orders)
    copy_rows(cur, "raw.distributor_shipments",
              ["shipment_id", "order_id", "ship_date", "delivery_date",
               "carrier", "units_shipped"],
              shipments)
    print(f"  distributor_shipments: {len(shipments)} rows")

    print("Generating remittances...")
    remittances, rem_map = generate_remittances(rng, orders)
    copy_rows(cur, "raw.distributor_remittances",
              ["remittance_id", "distributor_id", "received_date",
               "gross_amount", "net_amount", "total_deductions"],
              remittances)
    print(f"  distributor_remittances: {len(remittances)} rows")

    print("Generating deductions...")
    deductions = generate_deductions(rng, orders, rem_map)
    copy_rows(cur, "raw.distributor_deductions",
              ["deduction_id", "distributor_id", "order_id", "remittance_id",
               "deduction_type", "amount", "deduction_date"],
              deductions)
    print(f"  distributor_deductions: {len(deductions)} rows")

    print("Generating disputes...")
    disputes = generate_disputes(rng, deductions)
    copy_rows(cur, "raw.distributor_disputes",
              ["dispute_id", "deduction_id", "filed_date",
               "outcome", "recovered_amount", "closed_date", "labor_hours"],
              disputes)
    print(f"  distributor_disputes: {len(disputes)} rows")

    print("Generating chargebacks...")
    chargebacks = generate_chargebacks(rng)
    copy_rows(cur, "raw.distributor_chargebacks",
              ["month", "distributor_id", "reason", "sku", "amount"],
              chargebacks)
    print(f"  distributor_chargebacks: {len(chargebacks)} rows")

    conn.commit()
    print("\nDistributor pipeline committed.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

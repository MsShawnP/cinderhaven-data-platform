"""Seed generator for the retailer pipeline.

Tables generated (50 SKUs, 3-year window):
  - retailer_orders (~45,000 orders)
  - retailer_order_lines (~110,000 lines)
  - retailer_shipments (~45,000)
  - retailer_remittances (~1,500)
  - retailer_deductions (~11,000)
  - retailer_disputes (~4,500)
  - retailer_dispute_evidence (~10,000)
  - retailer_chargebacks (~1,500)
  - retailer_post_audit_claims (~220)
  - retailer_pack_records (~45,000)

Requires seed_shared.py to have been run first (retailers, products, stores exist).

Usage:
    python scripts/seed_retailer.py
"""
from __future__ import annotations

import io
import psycopg2
from datetime import date, timedelta

from seed_config import (
    ALL_SKUS, DATABASE_URL, RETAILERS, CARRIERS, DEDUCTION_TYPES,
    DISPUTE_OUTCOMES, EVIDENCE_TYPES, SEASONALITY,
    WINDOW_START, WINDOW_END, init_rng, compute_defect_profile,
    DEFECT_SEED,
)


COPY_CHUNK_SIZE = 50_000

def copy_rows(cur, table: str, columns: list[str], rows: list[tuple]):
    cols = ", ".join(columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
    for start in range(0, len(rows), COPY_CHUNK_SIZE):
        chunk = rows[start:start + COPY_CHUNK_SIZE]
        buf = io.StringIO()
        for row in chunk:
            line = "\t".join("\\N" if v is None else str(v) for v in row)
            buf.write(line + "\n")
        buf.seek(0)
        cur.copy_expert(sql, buf)


RETAILER_VOLUME_WEIGHT = {
    "RET-WALMART": 1.3,
    "RET-COSTCO": 0.8,
    "RET-WHOLEFOODS": 1.0,
    "RET-KROGER": 1.2,
    "RET-SPROUTS": 0.9,
    "RET-REGIONAL": 0.7,
}

def generate_orders_and_lines(rng):
    """Generate retailer orders and their line items."""
    orders = []
    lines = []
    order_num = 0

    current = WINDOW_START
    while current <= WINDOW_END:
        month_mult = SEASONALITY.get(current.month, 1.0)
        for ret in RETAILERS:
            weight = RETAILER_VOLUME_WEIGHT.get(ret["retailer_id"], 1.0)
            orders_this_week = int(rng.gauss(50, 10) * month_mult * weight)
            orders_this_week = max(8, orders_this_week)

            for _ in range(orders_this_week):
                order_num += 1
                order_id = f"RO-{order_num:06d}"
                po_number = f"PO-{ret['retailer_id'][-3:]}-{order_num:06d}"
                po_date = current + timedelta(days=rng.randint(0, 6))
                if po_date > WINDOW_END:
                    continue

                n_lines = rng.choices([2, 3, 4, 5, 7], weights=[10, 25, 35, 20, 10])[0]
                chosen_skus = rng.sample(ALL_SKUS, min(n_lines, len(ALL_SKUS)))

                total_units = 0
                total_value = 0.0
                order_lines = []

                for sku_info in chosen_skus:
                    units = rng.choices(
                        [24, 48, 72, 96, 144],
                        weights=[15, 30, 30, 15, 10]
                    )[0]
                    key = ret["name"].lower().replace(" ", "_")
                    if key == "regional_group":
                        key = "regional"
                    from seed_config import WHOLESALE_MULT
                    mult = WHOLESALE_MULT.get(key, 0.52)
                    unit_price = round(sku_info["msrp"] * mult, 2)
                    line_total = round(units * unit_price, 2)

                    total_units += units
                    total_value += line_total
                    order_lines.append((
                        order_id, sku_info["sku"], units, unit_price, line_total,
                    ))

                ship_date = po_date + timedelta(days=rng.randint(1, 5))
                orders.append((
                    order_id, ret["retailer_id"], po_number, str(po_date),
                    str(ship_date), total_units, round(total_value, 2),
                ))
                lines.extend(order_lines)

        current += timedelta(weeks=1)

    return orders, lines


def generate_shipments(rng, orders):
    """One shipment per order."""
    shipments = []
    for i, order in enumerate(orders):
        order_id = order[0]
        po_date = date.fromisoformat(order[3])
        ship_date = date.fromisoformat(order[4])
        delivery_date = ship_date + timedelta(days=rng.randint(1, 7))
        carrier = rng.choice(CARRIERS)
        units_shipped = order[5]
        pallets = max(1, units_shipped // 48)
        asn_sent = rng.random() < 0.85
        asn_late = asn_sent and rng.random() < 0.10

        shipments.append((
            f"RS-{i+1:06d}", order_id, str(ship_date),
            str(delivery_date) if delivery_date <= WINDOW_END else None,
            carrier, f"BOL-{i+1:06d}",
            units_shipped, pallets, asn_sent, asn_late,
        ))
    return shipments


def generate_remittances(rng, orders):
    """Group orders into monthly remittances per retailer."""
    from collections import defaultdict
    by_retailer_month = defaultdict(list)
    for order in orders:
        retailer_id = order[1]
        po_date = date.fromisoformat(order[3])
        month_key = (retailer_id, po_date.year, po_date.month)
        by_retailer_month[month_key].append(order)

    remittances = []
    rem_num = 0
    remittance_order_map = {}

    for (rid, year, month), month_orders in sorted(by_retailer_month.items()):
        rem_num += 1
        rem_id = f"RREM-{rem_num:04d}"
        gross = sum(float(o[6]) for o in month_orders)
        deduction_rate = rng.uniform(0.08, 0.18)
        total_ded = round(gross * deduction_rate, 2)
        net = round(gross - total_ded, 2)
        received = date(year, month, 1) + timedelta(days=rng.randint(25, 55))
        if received > WINDOW_END:
            received = WINDOW_END
        fmt = rng.choice(["EDI 820", "PDF", "CSV", "portal_export"])
        clarity = rng.choice(["clear", "clear", "moderate", "vague"])

        remittances.append((
            rem_id, rid, str(received), fmt,
            round(gross, 2), round(net, 2), total_ded, clarity,
        ))
        for o in month_orders:
            remittance_order_map[o[0]] = (rem_id, total_ded / len(month_orders))

    return remittances, remittance_order_map


def generate_deductions(rng, orders, remittance_map, deduction_codes_by_retailer):
    """Generate deductions from remittances. ~25% of orders get a deduction."""
    deductions = []
    ded_num = 0

    for order in orders:
        if rng.random() > 0.25:
            continue
        order_id = order[0]
        retailer_id = order[1]
        po_date = date.fromisoformat(order[3])
        order_value = float(order[6])

        rem_info = remittance_map.get(order_id)
        rem_id = rem_info[0] if rem_info else None

        n_deds = rng.choices([1, 1, 2, 3], weights=[60, 20, 15, 5])[0]
        for _ in range(n_deds):
            ded_num += 1
            ded_type = rng.choice(DEDUCTION_TYPES)
            amount = round(order_value * rng.uniform(0.02, 0.15), 2)
            ded_date = po_date + timedelta(days=rng.randint(20, 60))
            if ded_date > WINDOW_END:
                continue
            deadline = ded_date + timedelta(days=rng.choice([30, 45, 60, 90]))

            codes = deduction_codes_by_retailer.get(retailer_id, [])
            matching = [c for c in codes if c[4] == ded_type]
            code_id = matching[0][0] if matching else None

            deductions.append((
                f"RD-{ded_num:06d}", retailer_id, order_id, rem_id,
                ded_type, code_id, amount, str(ded_date),
                str(deadline) if deadline <= WINDOW_END + timedelta(days=90) else None,
                rng.random() < 0.03,
            ))

    return deductions


def generate_disputes(rng, deductions):
    """~40% of deductions get disputed."""
    disputes = []
    disp_num = 0

    for ded in deductions:
        if rng.random() > 0.40:
            continue
        disp_num += 1
        ded_id = ded[0]
        ded_date = date.fromisoformat(ded[7])
        filed = ded_date + timedelta(days=rng.randint(1, 30))
        if filed > WINDOW_END:
            continue

        outcome = rng.choices(DISPUTE_OUTCOMES, weights=[30, 25, 30, 15])[0]
        ded_amount = float(ded[6])
        if outcome == "won":
            recovered = ded_amount
        elif outcome == "partial":
            recovered = round(ded_amount * rng.uniform(0.20, 0.80), 2)
        elif outcome == "pending":
            recovered = None
        else:
            recovered = 0.0

        closed = None
        if outcome != "pending":
            closed = filed + timedelta(days=rng.randint(14, 90))
            if closed > WINDOW_END:
                closed = None

        labor = round(rng.uniform(0.25, 4.0), 2)
        quality = rng.choice(["strong", "strong", "moderate", "weak"])
        method = rng.choice(["portal", "email", "phone"])

        disputes.append((
            f"RDISP-{disp_num:05d}", ded_id, str(filed), method,
            quality, outcome, recovered,
            str(closed) if closed else None, labor,
        ))

    return disputes


def generate_dispute_evidence(rng, disputes):
    """2-4 evidence items per dispute."""
    evidence = []
    for disp in disputes:
        dispute_id = disp[0]
        n_items = rng.randint(2, 4)
        chosen_types = rng.sample(EVIDENCE_TYPES, min(n_items, len(EVIDENCE_TYPES)))
        for etype in chosen_types:
            submitted = rng.random() < 0.80
            required = rng.random() < 0.60
            fmt = rng.choice(["PDF", "PNG", "CSV", "EDI"])
            evidence.append((
                dispute_id, etype, submitted, required, fmt, None,
            ))
    return evidence


def generate_chargebacks(rng):
    """Legacy chargeback records by month/retailer/reason/sku.

    SKU distribution is quality-weighted (lower score → more chargebacks)
    using an isolated defect_rng stream. Main rng consumption is preserved
    exactly so the total count stays at 690.
    """
    defect = compute_defect_profile()
    defect_rng = init_rng(DEFECT_SEED + 1)

    # Quality-weighted SKU selection: weight = (101 - score)^2
    sku_list = [p["sku"] for p in ALL_SKUS]
    weights = [(101 - defect[s]["quality_score"]) ** 3.5 for s in sku_list]

    rows = []
    reasons = ["short_ship", "late_delivery", "label_fine", "damaged", "pricing_error"]
    current = WINDOW_START
    while current <= WINDOW_END:
        month_date = date(current.year, current.month, 1)
        for ret in RETAILERS:
            n_cbs = rng.randint(1, 5)
            for _ in range(n_cbs):
                # Dummy call: preserve main rng stream exactly
                rng.choice(ALL_SKUS)
                reason = rng.choice(reasons)
                amount = round(rng.uniform(50, 2000), 2)
                # Actual SKU from quality-weighted draw (isolated stream)
                sku = defect_rng.choices(sku_list, weights=weights)[0]
                rows.append((str(month_date), ret["retailer_id"], reason, sku, amount))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return rows


def generate_post_audit_claims(rng, deductions):
    """~2% of deductions become post-audit claims."""
    claims = []
    claim_num = 0
    auditors = ["Naviga", "PRGX", "Retail Solutions Inc", "CoStar Group"]
    claim_types = ["pricing", "promotion", "allowance", "new_item", "freight"]

    for ded in deductions:
        if rng.random() > 0.02:
            continue
        claim_num += 1
        ded_date = date.fromisoformat(ded[7])
        lookback = rng.choice([6, 12, 18, 24])
        period_start = ded_date - timedelta(days=lookback * 30)
        period_end = ded_date

        claims.append((
            f"PAC-{claim_num:04d}", ded[0],
            rng.choice(auditors),
            str(period_start), str(period_end),
            rng.choice(claim_types), lookback,
        ))
    return claims


def generate_pack_records(rng, orders, shipments):
    """One pack record per order/shipment pair."""
    rows = []
    ship_map = {s[1]: s for s in shipments}
    for order in orders:
        order_id = order[0]
        ship = ship_map.get(order_id)
        if not ship:
            continue
        ship_id = ship[0]
        ship_date = ship[2]
        units = order[5]
        pick_error = rng.random() < 0.03
        packed = units if not pick_error else units - rng.randint(1, max(1, units // 10))
        verification = rng.choice(["scan_verified", "manual_count", "weight_check"])
        scannable = rng.random() < 0.95
        fmt = rng.choice(["photo", "scan_log", "checklist"])

        rows.append((
            order_id, ship_id, ship_date,
            units, packed, verification, scannable, fmt,
        ))
    return rows


def main():
    print("Connecting to Postgres...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    rng = init_rng(seed=100)

    # Load existing deduction codes for FK references
    cur.execute("SELECT code_id, retailer_id, code, name, deduction_type FROM raw.retailer_deduction_codes")
    all_codes = cur.fetchall()
    from collections import defaultdict
    codes_by_retailer = defaultdict(list)
    for row in all_codes:
        codes_by_retailer[row[1]].append(row)

    print("\nGenerating retailer orders and lines...")
    orders, lines = generate_orders_and_lines(rng)
    copy_rows(cur, "raw.retailer_orders",
              ["order_id", "retailer_id", "po_number", "po_date",
               "requested_ship_date", "total_units", "total_value"],
              orders)
    print(f"  retailer_orders: {len(orders)} rows")

    copy_rows(cur, "raw.retailer_order_lines",
              ["order_id", "sku", "units_ordered", "unit_price", "line_total"],
              lines)
    print(f"  retailer_order_lines: {len(lines)} rows")

    print("Generating shipments...")
    shipments = generate_shipments(rng, orders)
    copy_rows(cur, "raw.retailer_shipments",
              ["shipment_id", "order_id", "ship_date", "delivery_date",
               "carrier", "bol_number", "units_shipped", "pallets_shipped",
               "asn_sent", "asn_sent_late"],
              shipments)
    print(f"  retailer_shipments: {len(shipments)} rows")

    print("Generating remittances...")
    remittances, rem_map = generate_remittances(rng, orders)
    copy_rows(cur, "raw.retailer_remittances",
              ["remittance_id", "retailer_id", "received_date", "format",
               "gross_amount", "net_amount", "total_deductions", "clarity"],
              remittances)
    print(f"  retailer_remittances: {len(remittances)} rows")

    print("Generating deductions...")
    deductions = generate_deductions(rng, orders, rem_map, codes_by_retailer)
    copy_rows(cur, "raw.retailer_deductions",
              ["deduction_id", "retailer_id", "order_id", "remittance_id",
               "deduction_type", "code_id", "amount", "deduction_date",
               "dispute_deadline", "is_post_audit"],
              deductions)
    print(f"  retailer_deductions: {len(deductions)} rows")

    print("Generating disputes...")
    disputes = generate_disputes(rng, deductions)
    copy_rows(cur, "raw.retailer_disputes",
              ["dispute_id", "deduction_id", "filed_date", "filing_method",
               "evidence_quality", "outcome", "recovered_amount",
               "closed_date", "labor_hours"],
              disputes)
    print(f"  retailer_disputes: {len(disputes)} rows")

    print("Generating dispute evidence...")
    evidence = generate_dispute_evidence(rng, disputes)
    copy_rows(cur, "raw.retailer_dispute_evidence",
              ["dispute_id", "evidence_type", "was_submitted",
               "was_required", "format", "notes"],
              evidence)
    print(f"  retailer_dispute_evidence: {len(evidence)} rows")

    print("Generating chargebacks...")
    chargebacks = generate_chargebacks(rng)
    copy_rows(cur, "raw.retailer_chargebacks",
              ["month", "retailer_id", "reason", "sku", "amount"],
              chargebacks)
    print(f"  retailer_chargebacks: {len(chargebacks)} rows")

    print("Generating post-audit claims...")
    pac = generate_post_audit_claims(rng, deductions)
    copy_rows(cur, "raw.retailer_post_audit_claims",
              ["claim_id", "deduction_id", "auditor_name",
               "audit_period_start", "audit_period_end",
               "claim_type", "lookback_months"],
              pac)
    print(f"  retailer_post_audit_claims: {len(pac)} rows")

    print("Generating pack records...")
    pack = generate_pack_records(rng, orders, shipments)
    copy_rows(cur, "raw.retailer_pack_records",
              ["order_id", "shipment_id", "pack_date",
               "units_picked", "units_packed", "pack_verification",
               "label_scannable", "evidence_format"],
              pack)
    print(f"  retailer_pack_records: {len(pack)} rows")

    conn.commit()
    print("\nRetailer pipeline committed.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

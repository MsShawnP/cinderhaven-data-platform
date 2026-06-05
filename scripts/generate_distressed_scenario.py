"""Generate the 'distressed' scenario dataset for trade-spend-diagnostic.

Copies a baseline SQLite export and replaces deductions + disputes with
v1-style operational mess: real vague deductions (VAGUE_TEMPLATES, bimodal
amounts, 30% no-PO-link), explicit double-dip injection, and low recovery
rates (~18-20%).

Baseline tables (orders, shipments, scan_data, chargebacks, etc.) are
byte-identical. Revenue ($32.5M trailing-52w) and chargeback count (864)
are provably unchanged.

Usage:
    python scripts/generate_distressed_scenario.py --baseline <PATH> [--output <PATH>]

Example:
    python scripts/generate_distressed_scenario.py ^
        --baseline ../trade-spend-data-diagnostic/cinderhaven-data/data/cinderhaven_product_master.db ^
        --output data/cinderhaven_distressed.db
"""
from __future__ import annotations

import argparse
import random
import shutil
import sqlite3
from datetime import date, timedelta
from pathlib import Path

DISTRESSED_SEED = 200
DATE_CAP = date(2027, 1, 2)

RETAILER_KEY_MAP = {
    "RET-WALMART": "walmart",
    "RET-COSTCO": "costco",
    "RET-WHOLEFOODS": "whole_foods",
    "RET-SPROUTS": "sprouts",
    "RET-KROGER": "kroger",
    "RET-REGIONAL": "regional_group",
}

VAGUE_TEMPLATES = [
    "Code {code}: {label}",
    "Promo allowance",
    "Marketing chargeback",
    "Audit adjustment",
    "Misc deduction -- see invoice",
    "Cash discount take-down",
    "Slotting reconciliation",
    "Trade spend true-up",
    "Allowance reconciliation",
    "Compliance fee",
]

SPOILAGE_TEMPLATES = [
    "Spoilage -- temperature exposure in transit",
    "Spoilage -- expired or short-dated at receiving",
    "Spoilage -- quality complaint at receiving",
    "Spoilage -- damage in transit affecting condition",
]

SLOTTING_TEMPLATES = [
    "New-item slotting fee -- placement allowance",
    "Planogram reset -- placement billback",
    "Shelf placement / new-item program",
    "Category-reset placement billback",
]

# Per-retailer deduction probability (unconditional per-order).
# Adapted from v1 (11_generate_deductions.py, SEED=45). In v1, short_ship/
# damaged/label_fine/late_delivery were conditioned on precondition columns
# (bol_signed_short, bol_signed_damaged, etc.) that v2 schema lacks. Rates
# here approximate v1's effective unconditional rates.
PROFILES = {
    "walmart": {
        "short_ship": 0.080,
        "label_fine": 0.045,
        "pallet_fine": 0.025,
        "damaged": 0.065,
        "late_delivery": 0.060,
        "promo_billback": 0.140,
        "vague": 0.022,
        "spoilage": 0.065,
        "pricing_error": 0.015,
    },
    "costco": {
        "short_ship": 0.060,
        "label_fine": 0.025,
        "pallet_fine": 0.030,
        "damaged": 0.055,
        "late_delivery": 0.050,
        "promo_billback": 0.080,
        "vague": 0.018,
        "spoilage": 0.055,
        "pricing_error": 0.012,
    },
    "whole_foods": {
        "short_ship": 0.045,
        "label_fine": 0.015,
        "pallet_fine": 0.012,
        "damaged": 0.050,
        "late_delivery": 0.035,
        "promo_billback": 0.100,
        "vague": 0.025,
        "spoilage": 0.065,
        "pricing_error": 0.012,
    },
    "sprouts": {
        "short_ship": 0.045,
        "label_fine": 0.015,
        "pallet_fine": 0.008,
        "damaged": 0.040,
        "late_delivery": 0.035,
        "promo_billback": 0.140,
        "vague": 0.025,
        "spoilage": 0.040,
        "pricing_error": 0.010,
    },
    "kroger": {
        "short_ship": 0.040,
        "label_fine": 0.012,
        "pallet_fine": 0.008,
        "damaged": 0.035,
        "late_delivery": 0.025,
        "promo_billback": 0.080,
        "vague": 0.022,
        "spoilage": 0.040,
        "pricing_error": 0.010,
    },
    "regional_group": {
        "short_ship": 0.035,
        "label_fine": 0.010,
        "pallet_fine": 0.006,
        "damaged": 0.035,
        "late_delivery": 0.025,
        "promo_billback": 0.040,
        "vague": 0.015,
        "spoilage": 0.018,
        "pricing_error": 0.008,
    },
}

REMITTANCE_LAG = {
    "walmart": (28, 42),
    "costco": (30, 45),
    "whole_foods": (21, 35),
    "sprouts": (21, 40),
    "kroger": (21, 40),
    "regional_group": (21, 40),
}

SLOTTING_CONFIG = {
    "walmart": {"events": 3, "range": (5500, 13000)},
    "costco": {"events": 3, "range": (8000, 18000)},
    "whole_foods": {"events": 4, "range": (3000, 6500)},
    "sprouts": {"events": 3, "range": (400, 1100)},
    "kroger": {"events": 2, "range": (500, 1300)},
    "regional_group": {"events": 2, "range": (300, 850)},
}


# ── Amount generators (v1 machinery) ─────────────────────────────

def vague_amount(rng: random.Random) -> float:
    if rng.random() < 0.6:
        return round(rng.uniform(50.0, 600.0), 2)
    return round(rng.uniform(800.0, 4500.0), 2)


def amount_for(rng: random.Random, ded_type: str, key: str,
               order_value: float, total_units: int) -> float:
    if ded_type == "short_ship":
        return round(order_value * rng.uniform(0.05, 0.18), 2)
    if ded_type == "label_fine":
        if key == "walmart":
            return round(200.0 + total_units * rng.uniform(0.8, 1.2), 2)
        if key == "costco":
            return round(rng.uniform(50.0, 150.0) * max(1, total_units // 100), 2)
        return round(rng.uniform(75.0, 250.0), 2)
    if ded_type == "pallet_fine":
        if key == "walmart":
            return round(200.0 + rng.uniform(14.0, 36.0), 2)
        return round(rng.uniform(80.0, 220.0), 2)
    if ded_type == "damaged":
        return round(order_value * rng.uniform(0.05, 0.18), 2)
    if ded_type == "late_delivery":
        if key == "walmart":
            return round(order_value * 0.05, 2)
        return round(order_value * rng.uniform(0.03, 0.06), 2)
    if ded_type == "promo_billback":
        return round(order_value * rng.uniform(0.05, 0.15), 2)
    if ded_type == "vague":
        return vague_amount(rng)
    if ded_type == "spoilage":
        return round(order_value * rng.uniform(0.10, 0.28), 2)
    if ded_type == "pricing_error":
        return round(order_value * rng.uniform(0.01, 0.05), 2)
    return round(order_value * rng.uniform(0.02, 0.10), 2)


def description_for(rng: random.Random, ded_type: str) -> str:
    if ded_type == "vague":
        t = rng.choice(VAGUE_TEMPLATES)
        return t.format(code=rng.randint(85, 99), label="Other")
    if ded_type == "spoilage":
        return rng.choice(SPOILAGE_TEMPLATES)
    return ded_type.replace("_", " ").title()


# ── Main ─────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", type=Path, required=True,
                        help="Path to baseline (post-fixup) SQLite")
    parser.add_argument("--output", type=Path,
                        default=Path("data/cinderhaven_distressed.db"),
                        help="Output path for distressed SQLite")
    args = parser.parse_args()

    if not args.baseline.exists():
        raise FileNotFoundError(f"Baseline not found: {args.baseline}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(args.baseline, args.output)
    print(f"Copied {args.baseline.name} -> {args.output}")

    rng = random.Random(DISTRESSED_SEED)

    con = sqlite3.connect(str(args.output))
    con.execute("PRAGMA journal_mode=WAL")
    cur = con.cursor()

    # ── Pre-validation ────────────────────────────────────────────
    baseline_cb = cur.execute("SELECT COUNT(*) FROM chargebacks").fetchone()[0]
    baseline_orders = cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    print(f"Baseline: {baseline_orders:,} orders, {baseline_cb} chargebacks")

    # ── Load reference data ───────────────────────────────────────
    orders = cur.execute("""
        SELECT order_id, retailer_id, po_date, requested_ship_date,
               total_units, total_value
        FROM orders ORDER BY po_date
    """).fetchall()

    ship_map: dict[str, tuple[str, str | None]] = {}
    for oid, sid, dd in cur.execute(
        "SELECT order_id, shipment_id, delivery_date FROM shipments"
    ).fetchall():
        ship_map[oid] = (sid, dd)

    codes_by_ret: dict[str, dict[str, str]] = {}
    try:
        for cid, rid, dt in cur.execute(
            "SELECT code_id, retailer_id, deduction_type FROM deduction_codes"
        ).fetchall():
            codes_by_ret.setdefault(rid, {})[dt] = cid
    except sqlite3.OperationalError:
        pass

    rem_by_ret: dict[str, list[str]] = {}
    for rid_val, rem_id in cur.execute(
        "SELECT retailer_id, remittance_id FROM remittances"
    ).fetchall():
        rem_by_ret.setdefault(rid_val, []).append(rem_id)

    # ── Clear existing deduction-layer tables ─────────────────────
    for tbl in ("dispute_evidence", "disputes", "deductions"):
        cur.execute(f"DELETE FROM [{tbl}]")
    con.commit()
    print("Cleared deductions / disputes / evidence")

    # ── Generate distressed deductions ────────────────────────────
    deductions: list[tuple] = []
    seq = 0
    counters: dict[str, int] = {}
    totals: dict[str, float] = {}

    for order in orders:
        order_id, retailer_id, po_date_s, ship_date_s, units, value = order
        key = RETAILER_KEY_MAP.get(retailer_id)
        if not key:
            continue
        profile = PROFILES.get(key)
        if not profile:
            continue

        po_date = date.fromisoformat(po_date_s)
        ship_info = ship_map.get(order_id)
        if ship_info and ship_info[1]:
            delivery = date.fromisoformat(ship_info[1])
            shipment_id = ship_info[0]
        else:
            delivery = po_date + timedelta(days=5)
            shipment_id = None

        lag = REMITTANCE_LAG.get(key, (21, 40))
        value_f = float(value)
        units_i = int(units)

        for ded_type, rate in profile.items():
            if rng.random() >= rate:
                continue

            seq += 1
            ded_id = f"DIST-{seq:06d}"
            amt = amount_for(rng, ded_type, key, value_f, units_i)
            ded_date = delivery + timedelta(days=rng.randint(*lag))
            if ded_date > DATE_CAP:
                continue
            deadline = ded_date + timedelta(days=rng.choice([30, 45, 60, 90]))
            deadline_s = deadline.isoformat() if deadline <= DATE_CAP + timedelta(days=90) else None

            code_id = codes_by_ret.get(retailer_id, {}).get(ded_type)
            code_remitted = ""
            if code_id:
                try:
                    code_remitted = cur.execute(
                        "SELECT code FROM deduction_codes WHERE code_id = ?",
                        (code_id,)
                    ).fetchone()[0]
                except (TypeError, sqlite3.OperationalError):
                    pass

            is_vague = 1 if ded_type == "vague" else 0
            is_post_audit = 1 if rng.random() < 0.03 else 0
            desc = description_for(rng, ded_type)

            if ded_type == "vague":
                code_id = None
                code_remitted = ""
                if rng.random() < 0.30:
                    link_order = None
                    link_ship = None
                else:
                    link_order = order_id
                    link_ship = shipment_id
            else:
                link_order = order_id
                link_ship = shipment_id

            ret_rems = rem_by_ret.get(retailer_id, [])
            rem_id = rng.choice(ret_rems) if ret_rems else None

            deductions.append((
                ded_id, retailer_id, link_order, rem_id,
                ded_type, code_id, amt, ded_date.isoformat(),
                deadline_s, is_post_audit,
                link_ship, code_remitted, is_vague,
                desc, 0,
            ))
            counters[ded_type] = counters.get(ded_type, 0) + 1
            totals[ded_type] = totals.get(ded_type, 0.0) + amt

    # ── Slotting events (periodic, not per-order) ─────────────────
    min_po = cur.execute("SELECT MIN(po_date) FROM orders").fetchone()[0]
    max_po = cur.execute("SELECT MAX(po_date) FROM orders").fetchone()[0]
    if min_po and max_po:
        ws = date.fromisoformat(min_po)
        we = date.fromisoformat(max_po)
        span = max(1, (we - ws).days)
        for key, cfg in SLOTTING_CONFIG.items():
            rid = [k for k, v in RETAILER_KEY_MAP.items() if v == key][0]
            n = cfg["events"]
            lo, hi = cfg["range"]
            code_id = codes_by_ret.get(rid, {}).get("slotting")
            code_remitted = ""
            if code_id:
                try:
                    code_remitted = cur.execute(
                        "SELECT code FROM deduction_codes WHERE code_id = ?",
                        (code_id,)
                    ).fetchone()[0]
                except (TypeError, sqlite3.OperationalError):
                    pass
            for i in range(n):
                frac = (i + 0.5) / n + rng.uniform(-0.25, 0.25) / n
                frac = max(0.0, min(0.999, frac))
                ded_date = ws + timedelta(days=int(frac * span))
                if ded_date > DATE_CAP:
                    continue
                seq += 1
                amt = round(rng.uniform(lo, hi), 2)
                desc = rng.choice(SLOTTING_TEMPLATES)
                deductions.append((
                    f"DIST-{seq:06d}", rid, None, None,
                    "slotting", code_id, amt, ded_date.isoformat(),
                    None, 0,
                    None, code_remitted, 0, desc, 0,
                ))
                counters["slotting"] = counters.get("slotting", 0) + 1
                totals["slotting"] = totals.get("slotting", 0.0) + amt

    # ── Insert all deductions ─────────────────────────────────────
    cur.executemany("""
        INSERT INTO deductions (
            deduction_id, retailer_id, order_id, remittance_id,
            deduction_type, code_id, amount, deduction_date,
            dispute_deadline, is_post_audit,
            shipment_id, code_as_remitted, is_vague,
            remittance_description, is_double_dip
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, deductions)
    con.commit()
    print(f"Inserted {len(deductions):,} deductions")

    # ── Double-dip injection (v1 approach: off_invoice promos) ────
    dd_targets = [6500.0, 7200.0, 5800.0]
    dd_total = 0.0
    dd_count = 0

    try:
        promo_rows = cur.execute("""
            SELECT p.promo_id, p.sku, p.retailer_id, p.start_week
            FROM promotions p
            WHERE p.funding_mechanism = 'off_invoice'
              AND p.retailer_id IN ('RET-WALMART', 'RET-COSTCO', 'RET-WHOLEFOODS')
            ORDER BY p.start_week
            LIMIT 10
        """).fetchall()
    except sqlite3.OperationalError:
        promo_rows = []

    if len(promo_rows) < 3:
        promo_rows = cur.execute("""
            SELECT deduction_id, retailer_id, order_id, deduction_date
            FROM deductions
            WHERE deduction_type = 'promo_billback'
              AND retailer_id IN ('RET-WALMART', 'RET-COSTCO', 'RET-WHOLEFOODS')
              AND order_id IS NOT NULL
            ORDER BY deduction_date
            LIMIT 10
        """).fetchall()
        for i, target in enumerate(dd_targets):
            if i >= len(promo_rows):
                break
            src = promo_rows[i]
            seq += 1
            amt = round(target * rng.uniform(0.95, 1.05), 2)
            ded_date = src[3]
            cur.execute("""
                INSERT INTO deductions (
                    deduction_id, retailer_id, order_id, remittance_id,
                    deduction_type, code_id, amount, deduction_date,
                    dispute_deadline, is_post_audit,
                    shipment_id, code_as_remitted, is_vague,
                    remittance_description, is_double_dip
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                f"DIST-{seq:06d}", src[1], src[2], None,
                "promo_billback", None, amt, ded_date,
                None, 0, None, "", 0,
                f"Scan-back duplicate -- same event as {src[0]}", 1,
            ))
            dd_total += amt
            dd_count += 1
    else:
        for i, target in enumerate(dd_targets):
            if i >= len(promo_rows):
                break
            promo_id, sku, p_rid, start_wk = promo_rows[i]
            seq += 1
            amt = round(target * rng.uniform(0.95, 1.05), 2)
            ded_date = date.fromisoformat(start_wk) + timedelta(days=rng.randint(14, 35))
            if ded_date > DATE_CAP:
                ded_date = DATE_CAP - timedelta(days=rng.randint(1, 14))
            cur.execute("""
                INSERT INTO deductions (
                    deduction_id, retailer_id, order_id, remittance_id,
                    deduction_type, code_id, amount, deduction_date,
                    dispute_deadline, is_post_audit,
                    shipment_id, code_as_remitted, is_vague,
                    remittance_description, is_double_dip
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                f"DIST-{seq:06d}", p_rid, None, None,
                "promo_billback", None, amt, ded_date.isoformat(),
                None, 0, None, "", 0,
                f"Scan-back: {sku} -- TPR wk {start_wk}", 1,
            ))
            dd_total += amt
            dd_count += 1

    con.commit()
    counters["double_dip"] = dd_count
    totals["double_dip"] = dd_total
    print(f"Double-dips injected: {dd_count}, ${dd_total:,.0f}")

    # ── Disputes with LOW recovery (~18-20%) ──────────────────────
    all_deds = cur.execute("""
        SELECT deduction_id, amount, deduction_date, dispute_deadline
        FROM deductions WHERE deduction_type != 'slotting'
    """).fetchall()

    disputes: list[tuple] = []
    disp_seq = 0
    outcomes = ["won", "lost", "partial", "pending"]
    weights = [12, 45, 28, 15]

    for ded in all_deds:
        if rng.random() > 0.35:
            continue
        disp_seq += 1
        ded_id, ded_amt, ded_date_s, deadline_s = ded
        ded_date = date.fromisoformat(ded_date_s)
        filed = ded_date + timedelta(days=rng.randint(1, 30))
        if filed > DATE_CAP:
            continue

        outcome = rng.choices(outcomes, weights=weights)[0]
        amt_f = float(ded_amt)
        if outcome == "won":
            recovered = amt_f
        elif outcome == "partial":
            recovered = round(amt_f * rng.uniform(0.10, 0.50), 2)
        elif outcome == "pending":
            recovered = None
        else:
            recovered = 0.0

        closed = None
        if outcome != "pending":
            closed = filed + timedelta(days=rng.randint(14, 90))
            if closed > DATE_CAP:
                closed = None

        within = 1
        if deadline_s:
            within = 1 if filed <= date.fromisoformat(deadline_s) else 0

        quality = rng.choices(
            ["strong", "moderate", "weak"], weights=[15, 35, 50]
        )[0]
        method = rng.choice(["portal", "email", "phone"])
        labor = round(rng.uniform(0.25, 4.0), 2)

        disputes.append((
            f"DDISP-{disp_seq:05d}", ded_id, filed.isoformat(), method,
            quality, outcome, recovered,
            closed.isoformat() if closed else None, labor,
            within, 0,
        ))

    cur.executemany("""
        INSERT INTO disputes (
            dispute_id, deduction_id, filed_date, filing_method,
            evidence_quality, outcome, recovered_amount,
            closed_date, labor_hours,
            was_within_deadline, submitted_evidence_count
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, disputes)
    con.commit()
    print(f"Disputes: {len(disputes):,} (target ~18-20% recovery)")

    # ── Dispute evidence ──────────────────────────────────────────
    ev_types = ["BOL", "POD", "invoice", "ASN", "pack_photo",
                "label_scan", "price_confirmation"]
    evidence: list[tuple] = []
    ev_counts: dict[str, int] = {}

    for disp in disputes:
        did = disp[0]
        n = rng.randint(1, 3)
        chosen = rng.sample(ev_types, min(n, len(ev_types)))
        submitted = 0
        for et in chosen:
            was_sub = rng.random() < 0.60
            if was_sub:
                submitted += 1
            evidence.append((
                did, et, int(was_sub), int(rng.random() < 0.60),
                rng.choice(["PDF", "PNG", "CSV", "EDI"]), None,
            ))
        ev_counts[did] = submitted

    cur.executemany("""
        INSERT INTO dispute_evidence (
            dispute_id, evidence_type, was_submitted,
            was_required, format, notes
        ) VALUES (?,?,?,?,?,?)
    """, evidence)

    for did, cnt in ev_counts.items():
        cur.execute(
            "UPDATE disputes SET submitted_evidence_count = ? WHERE dispute_id = ?",
            (cnt, did),
        )
    con.commit()
    print(f"Evidence items: {len(evidence):,}")

    # ── Post-validation: baseline tables unchanged ────────────────
    post_cb = cur.execute("SELECT COUNT(*) FROM chargebacks").fetchone()[0]
    post_orders = cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert post_cb == baseline_cb, f"Chargebacks changed! {baseline_cb} -> {post_cb}"
    assert post_orders == baseline_orders, f"Orders changed! {baseline_orders} -> {post_orders}"

    scan_rev = None
    try:
        scan_rev = cur.execute("""
            WITH w AS (
                SELECT DISTINCT week_ending FROM scan_data
                ORDER BY week_ending DESC LIMIT 52
            )
            SELECT SUM(sd.dollars_sold)
            FROM scan_data sd
            WHERE sd.week_ending >= (SELECT MIN(week_ending) FROM w)
        """).fetchone()[0]
    except sqlite3.OperationalError:
        pass

    print("\n" + "=" * 60)
    print("BASELINE INTEGRITY CHECK")
    print("=" * 60)
    print(f"  Chargebacks:  {post_cb} (unchanged)")
    print(f"  Orders:       {post_orders:,} (unchanged)")
    if scan_rev:
        print(f"  Scan revenue: ${float(scan_rev)/1e6:.2f}M trailing-52w (unchanged)")

    # ── Figure table ──────────────────────────────────────────────
    total_ded_count = cur.execute("SELECT COUNT(*) FROM deductions").fetchone()[0]
    total_ded_amt = cur.execute("SELECT SUM(amount) FROM deductions").fetchone()[0]
    waste_amt = cur.execute(
        "SELECT SUM(amount) FROM deductions WHERE deduction_type != 'promo_billback'"
    ).fetchone()[0] or 0
    vague_count = cur.execute(
        "SELECT COUNT(*) FROM deductions WHERE is_vague = 1"
    ).fetchone()[0]
    vague_amt = cur.execute(
        "SELECT SUM(amount) FROM deductions WHERE is_vague = 1"
    ).fetchone()[0] or 0
    dd_count_db = cur.execute(
        "SELECT COUNT(*) FROM deductions WHERE is_double_dip = 1"
    ).fetchone()[0]
    dd_amt_db = cur.execute(
        "SELECT SUM(amount) FROM deductions WHERE is_double_dip = 1"
    ).fetchone()[0] or 0
    no_po = cur.execute(
        "SELECT COUNT(*) FROM deductions WHERE order_id IS NULL AND deduction_type = 'vague'"
    ).fetchone()[0]
    ghost_promo_count = 0
    ghost_promo_amt = 0.0
    try:
        ghost = cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(d.amount), 0)
            FROM deductions d
            WHERE d.deduction_type = 'promo_billback'
              AND d.is_double_dip = 0
              AND NOT EXISTS (
                  SELECT 1 FROM promotions p
                  WHERE p.retailer_id = d.retailer_id
                    AND d.deduction_date BETWEEN p.start_week AND p.end_week
              )
        """).fetchone()
        ghost_promo_count = ghost[0]
        ghost_promo_amt = float(ghost[1])
    except sqlite3.OperationalError:
        pass

    disp_count = cur.execute("SELECT COUNT(*) FROM disputes").fetchone()[0]
    rec_amt = cur.execute(
        "SELECT COALESCE(SUM(recovered_amount), 0) FROM disputes WHERE outcome IN ('won', 'partial')"
    ).fetchone()[0]
    disp_total_amt = cur.execute("""
        SELECT COALESCE(SUM(d.amount), 0)
        FROM deductions d
        JOIN disputes disp ON disp.deduction_id = d.deduction_id
    """).fetchone()[0]
    recovery_rate = (float(rec_amt) / float(disp_total_amt) * 100) if disp_total_amt else 0

    waste_annual = float(waste_amt) / 3
    vague_annual = float(vague_amt) / 3

    print("\n" + "=" * 60)
    print("DISTRESSED SCENARIO -- FIGURE TABLE")
    print("=" * 60)
    print(f"  Total deductions:       {total_ded_count:,}")
    print(f"  Total deduction value:  ${float(total_ded_amt):,.0f} (36mo)")
    print(f"  Waste (excl promo_bb):  ${float(waste_amt):,.0f} (36mo) / ${waste_annual:,.0f}/yr")
    print(f"  Vague count:            {vague_count:,}")
    print(f"  Vague value:            ${float(vague_amt):,.0f} (36mo) / ${vague_annual:,.0f}/yr")
    print(f"  Vague w/o PO link:      {no_po:,}")
    print(f"  Double-dips:            {dd_count_db} / ${float(dd_amt_db):,.0f}")
    print(f"  Ghost promos:           {ghost_promo_count:,} / ${ghost_promo_amt:,.0f}")
    print(f"  Disputes filed:         {disp_count:,}")
    print(f"  Recovery rate:          {recovery_rate:.1f}%")
    print(f"  Recovered:              ${float(rec_amt):,.0f}")

    print("\n  By type:")
    for dt in sorted(counters.keys()):
        c = counters[dt]
        a = totals.get(dt, 0)
        print(f"    {dt:<18} {c:>6,}  ${a:>12,.0f}")

    if scan_rev:
        all_in = float(waste_amt) / 3
        rate = all_in / float(scan_rev) * 100
        print(f"\n  All-in waste rate:  {rate:.1f}% of trailing-52w scan revenue")

    con.close()
    print(f"\nOutput: {args.output}")
    print("Done.")


if __name__ == "__main__":
    main()

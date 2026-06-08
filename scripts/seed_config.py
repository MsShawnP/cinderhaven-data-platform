"""Shared configuration for all Cinderhaven seed generators.

Defines the product catalog, retailers, distributors, and date windows
that every pipeline generator imports. Single source of truth for IDs,
names, and relationships.
"""
from __future__ import annotations

import os
import random
from datetime import date

# ── FROZEN (cinderhaven-data-v2) ────────────────────────────────────
# Editing any value below re-baselines the entire portfolio.
# Additive regens MUST NOT touch this block.
# Verified against Postgres SSOT 2026-06-04. See CINDERHAVEN_CANONICAL.md.
SEED = 42
WINDOW_START = date(2023, 1, 1)
WINDOW_END = date(2026, 1, 2)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"host=localhost port=5432 dbname=cinderhaven user=postgres password={os.environ.get('POSTGRES_PASSWORD', '')}"
)

# -- Retailers (5 contracted + 1 regional group) --

RETAILERS = [
    {"retailer_id": "RET-WALMART",    "name": "Walmart",      "dispute_portal_name": "RetailLink", "dispute_portal_url": "https://retaillink.walmart.com", "dispute_method": "portal"},
    {"retailer_id": "RET-COSTCO",     "name": "Costco",       "dispute_portal_name": "Costco Vendor Portal", "dispute_portal_url": "https://vendor.costco.com", "dispute_method": "portal"},
    {"retailer_id": "RET-WHOLEFOODS", "name": "Whole Foods",  "dispute_portal_name": "Whole Foods Vendor", "dispute_portal_url": "https://vendor.wholefoods.com", "dispute_method": "email"},
    {"retailer_id": "RET-SPROUTS",    "name": "Sprouts",      "dispute_portal_name": None, "dispute_portal_url": None, "dispute_method": "email"},
    {"retailer_id": "RET-KROGER",     "name": "Kroger",       "dispute_portal_name": "Kroger Vendor Net", "dispute_portal_url": "https://vendornet.kroger.com", "dispute_method": "portal"},
    {"retailer_id": "RET-REGIONAL",   "name": "Regional Group", "dispute_portal_name": None, "dispute_portal_url": None, "dispute_method": "email"},
]

# -- Distributors --

DISTRIBUTORS = [
    {"distributor_id": "DIST-UNFI",  "name": "UNFI",  "type": "national", "margin_pct": 0.22, "payment_terms_days": 30},
    {"distributor_id": "DIST-KEHE",  "name": "KeHE",  "type": "national", "margin_pct": 0.20, "payment_terms_days": 30},
    {"distributor_id": "DIST-DPI",   "name": "DPI Northwest", "type": "regional", "margin_pct": 0.18, "payment_terms_days": 21},
]

# -- Product lines (50 SKUs) --

PRODUCT_LINES = {
    "Artisan Sauces": [
        ("CHP-AS-001", "Smoky Chipotle BBQ Sauce", 8.99, 12, 2.10),
        ("CHP-AS-002", "Roasted Garlic Marinara", 7.49, 12, 1.85),
        ("CHP-AS-003", "Spicy Habanero Hot Sauce", 6.99, 24, 1.45),
        ("CHP-AS-004", "Sweet Thai Chili Sauce", 7.99, 12, 1.90),
        ("CHP-AS-005", "Classic Tomato Basil", 6.49, 12, 1.60),
        ("CHP-AS-006", "Balsamic Fig Glaze", 9.99, 12, 2.40),
        ("CHP-AS-007", "Lemon Herb Chimichurri", 8.49, 12, 2.00),
        ("CHP-AS-008", "Mango Jalapeño Salsa", 6.99, 12, 1.55),
        ("CHP-AS-009", "Truffle Mushroom Sauce", 11.99, 12, 3.20),
        ("CHP-AS-010", "Carolina Gold BBQ", 8.49, 12, 2.05),
    ],
    "Pantry Staples": [
        ("CHP-PS-001", "Stone Ground Mustard", 5.99, 12, 1.30),
        ("CHP-PS-002", "Wildflower Honey", 9.99, 12, 2.80),
        ("CHP-PS-003", "Apple Cider Vinegar", 6.49, 12, 1.40),
        ("CHP-PS-004", "Extra Virgin Olive Oil", 12.99, 6, 4.50),
        ("CHP-PS-005", "Sea Salt Blend", 4.99, 24, 0.90),
        ("CHP-PS-006", "Cracked Black Pepper", 5.49, 24, 1.00),
        ("CHP-PS-007", "Smoked Paprika", 5.99, 24, 1.10),
        ("CHP-PS-008", "Italian Seasoning Blend", 4.49, 24, 0.85),
        ("CHP-PS-009", "Maple Syrup Grade A", 11.49, 12, 3.50),
        ("CHP-PS-010", "Raw Agave Nectar", 7.99, 12, 2.20),
    ],
    "Specialty Condiments": [
        ("CHP-SC-001", "Bourbon Bacon Jam", 9.99, 12, 2.60),
        ("CHP-SC-002", "Caramelized Onion Spread", 8.49, 12, 2.15),
        ("CHP-SC-003", "Pickled Jalapeño Relish", 6.99, 12, 1.50),
        ("CHP-SC-004", "Sun-Dried Tomato Tapenade", 8.99, 12, 2.30),
        ("CHP-SC-005", "Roasted Red Pepper Hummus", 5.99, 12, 1.35),
        ("CHP-SC-006", "Artichoke Spinach Dip", 7.49, 12, 1.95),
        ("CHP-SC-007", "Everything Bagel Spread", 6.99, 12, 1.55),
        ("CHP-SC-008", "Herb Compound Butter", 7.99, 6, 2.40),
        ("CHP-SC-009", "Raspberry Chipotle Glaze", 8.99, 12, 2.25),
        ("CHP-SC-010", "Lemon Curd", 7.49, 12, 1.80),
    ],
    "Dried Goods": [
        ("CHP-DG-001", "Wild Rice Blend", 7.99, 12, 1.85),
        ("CHP-DG-002", "Quinoa Medley", 8.49, 12, 2.00),
        ("CHP-DG-003", "Steel Cut Oats", 5.49, 24, 1.10),
        ("CHP-DG-004", "Organic Lentils", 4.99, 24, 0.95),
        ("CHP-DG-005", "Sun-Dried Tomatoes", 9.99, 12, 2.60),
        ("CHP-DG-006", "Roasted Chickpeas", 6.49, 12, 1.40),
        ("CHP-DG-007", "Trail Mix Premium", 8.99, 12, 2.30),
        ("CHP-DG-008", "Dried Mango Slices", 7.49, 12, 1.75),
        ("CHP-DG-009", "Coconut Flakes Unsweetened", 5.99, 12, 1.25),
        ("CHP-DG-010", "Mixed Nut Butter Granola", 9.49, 12, 2.45),
    ],
    "Snack Bites": [
        ("CHP-SB-001", "Dark Chocolate Sea Salt Bites", 6.99, 24, 1.50),
        ("CHP-SB-002", "Almond Butter Protein Bites", 7.99, 24, 1.80),
        ("CHP-SB-003", "Coconut Cashew Clusters", 8.49, 12, 2.10),
        ("CHP-SB-004", "Spicy Sriracha Crackers", 4.99, 24, 0.95),
        ("CHP-SB-005", "Rosemary Olive Oil Crisps", 5.49, 24, 1.05),
        ("CHP-SB-006", "Honey Walnut Bites", 7.49, 12, 1.70),
        ("CHP-SB-007", "Cheddar Herb Popcorn", 4.49, 24, 0.85),
        ("CHP-SB-008", "Maple Pecan Clusters", 8.99, 12, 2.25),
        ("CHP-SB-009", "Everything Seasoning Pretzels", 5.99, 24, 1.15),
        ("CHP-SB-010", "Tahini Date Energy Bites", 7.99, 12, 1.90),
    ],
}

# Flatten for easy access: list of (sku, name, msrp, case_pack, cogs)
ALL_SKUS = []
for line, products in PRODUCT_LINES.items():
    for sku, name, msrp, case_pack, cogs in products:
        ALL_SKUS.append({
            "sku": sku,
            "product_name": name,
            "product_line": line,
            "msrp": msrp,
            "case_pack_qty": case_pack,
            "cogs_per_unit": cogs,
        })

# -- Retailer store counts and volume tiers --

RETAILER_STORE_COUNTS = {
    "RET-WALMART": 180,
    "RET-COSTCO": 60,
    "RET-WHOLEFOODS": 120,
    "RET-SPROUTS": 90,
    "RET-KROGER": 150,
    "RET-REGIONAL": 40,
}

# Club-channel stores (Costco) move 2.5x the units per location vs grocery.
CHANNEL_VELOCITY_MULT = {
    "RET-WALMART": 1.0,
    "RET-COSTCO": 2.5,
    "RET-WHOLEFOODS": 1.0,
    "RET-SPROUTS": 1.0,
    "RET-KROGER": 1.0,
    "RET-REGIONAL": 1.0,
}

VOLUME_TIERS = ["high", "medium", "low"]
REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
STATES_BY_REGION = {
    "Northeast": ["NY", "NJ", "PA", "CT", "MA", "NH", "VT", "ME", "RI"],
    "Southeast": ["FL", "GA", "NC", "SC", "VA", "TN", "AL", "MS", "LA"],
    "Midwest": ["IL", "OH", "MI", "IN", "WI", "MN", "IA", "MO", "KS"],
    "West": ["CA", "WA", "OR", "CO", "AZ", "NV", "UT", "HI"],
    "Southwest": ["TX", "NM", "OK", "AR"],
}

CARRIERS = ["FedEx", "UPS", "USPS", "LTL Freight", "R+L Carriers"]

DEDUCTION_TYPES = [
    "short_ship", "promo_billback", "slotting", "late_delivery",
    "label_fine", "pallet_fine", "spoilage", "damaged", "pricing_error",
]

DISPUTE_OUTCOMES = ["won", "lost", "partial", "pending"]
EVIDENCE_TYPES = ["BOL", "POD", "invoice", "ASN", "pack_photo", "label_scan", "price_confirmation"]

SEASONALITY = {
    1: 0.75, 2: 0.80, 3: 0.90, 4: 0.95, 5: 1.00, 6: 1.00,
    7: 0.95, 8: 0.90, 9: 1.05, 10: 1.10, 11: 1.35, 12: 1.45,
}

# ── FROZEN (cinderhaven-data-v2) ────────────────────────────────────
# These economic constants drive cited portfolio figures.
# Editing re-baselines the entire portfolio.
# Additive regens MUST NOT touch this block.

# Wholesale pricing multipliers (off MSRP)
# Retailers buy at ~50-65% of MSRP depending on channel
WHOLESALE_MULT = {
    "walmart": 0.50,
    "costco": 0.48,
    "whole_foods": 0.58,
    "sprouts": 0.55,
    "kroger": 0.52,
    "regional": 0.52,
    "unfi": 0.45,
    "kehe": 0.46,
    "dpi": 0.44,
    "dtc": 1.00,  # DTC sells at MSRP
}

TRADE_SPEND_PCT = {  # drives structural trade cost ($8.8M/36mo)
    "walmart": 0.12,
    "costco": 0.10,
    "whole_foods": 0.08,
    "sprouts": 0.09,
    "kroger": 0.10,
    "regional": 0.07,
    "unfi": 0.05,
    "kehe": 0.05,
    "dpi": 0.05,
    "dtc": 0.03,
}
# ── END FROZEN BLOCK ───────────────────────────────────────────────

# ── SCENARIO SUPPORT ──────────────────────────────────────────────
# "baseline" = canonical v2, read by all portfolio pieces.
# "distressed" = v1-style operational mess, consumed ONLY by
# trade-spend-diagnostic. Controlled by env var; default is baseline.
SCENARIO = os.environ.get("CINDERHAVEN_SCENARIO", "baseline")

DISTRESSED_DEDUCTION_TYPES = DEDUCTION_TYPES + ["vague"]

VAGUE_TEMPLATES = [
    "Code {code}: {label}",
    "Promo allowance",
    "Marketing chargeback",
    "Audit adjustment",
    "Misc deduction — see invoice",
    "Cash discount take-down",
    "Slotting reconciliation",
    "Trade spend true-up",
    "Allowance reconciliation",
    "Compliance fee",
]
# ── END SCENARIO SUPPORT ─────────────────────────────────────────


def init_rng(seed: int = SEED) -> random.Random:
    """Create a seeded RNG for deterministic generation."""
    return random.Random(seed)


# ── DEFECT PROFILE (isolated RNG stream, seed=300) ────────────────
# Changes here CANNOT cascade into trade/deduction/chargeback-count
# generation because the defect_rng is a separate stream.
DEFECT_SEED = 300
GTIN_INVALID_RATE = 0.20  # ~20% of SKUs get corrupted check digit

MISSING_RATES = {
    "case_length_in": 0.12,
    "case_width_in": 0.12,
    "case_height_in": 0.12,
    "unit_weight_lbs": 0.08,
    "case_weight_lbs": 0.08,
    "brand_owner": 0.02,
    "country_of_origin": 0.03,
    "subcategory": 0.10,
}


def _gtin14_check_digit(digits_13: str) -> str:
    """Compute GS1 check digit for a 13-digit GTIN-14 prefix."""
    total = 0
    for i, d in enumerate(digits_13):
        weight = 3 if i % 2 == 0 else 1
        total += int(d) * weight
    return str((10 - total % 10) % 10)


def compute_defect_profile(seed: int = DEFECT_SEED) -> dict:
    """Deterministic defect profile for all 50 SKUs.

    Returns {sku: {gtin14, upc, gtin_valid, missing_fields, quality_score}}
    where quality_score is 0-100 (mean ~70, range 40-95).

    Called by seed_shared (to populate product_master) and by
    seed_retailer/seed_distributor (to weight chargeback distribution).
    Same seed → same result everywhere.
    """
    rng = init_rng(seed)
    profile = {}

    for i, p in enumerate(ALL_SKUS):
        sku = p["sku"]

        # Valid GTIN-14: indicator "0" + company prefix "061414" + 6-digit item ref + check
        prefix_13 = f"0061414{i:06d}"
        check = _gtin14_check_digit(prefix_13)
        valid_gtin = prefix_13 + check

        # Corrupt ~20% by flipping the check digit
        is_gtin_valid = rng.random() >= GTIN_INVALID_RATE
        if is_gtin_valid:
            gtin14 = valid_gtin
        else:
            bad_check = str((int(check) + rng.randint(1, 9)) % 10)
            gtin14 = prefix_13 + bad_check

        upc = gtin14[1:]  # UPC-13 = GTIN-14 without indicator digit

        # Field missingness per MISSING_RATES
        missing_fields = {}
        for field, rate in MISSING_RATES.items():
            missing_fields[field] = rng.random() < rate

        # Quality score: calibrated to mean ~70, range 40-95
        score = 82
        if not is_gtin_valid:
            score -= rng.randint(20, 35)
        n_missing = sum(1 for v in missing_fields.values() if v)
        score -= n_missing * rng.randint(6, 12)
        score += rng.randint(-3, 5)
        score = max(40, min(95, score))

        profile[sku] = {
            "gtin14": gtin14,
            "upc": upc,
            "gtin_valid": is_gtin_valid,
            "missing_fields": missing_fields,
            "quality_score": score,
        }

    return profile
# ── END DEFECT PROFILE ────────────────────────────────────────────

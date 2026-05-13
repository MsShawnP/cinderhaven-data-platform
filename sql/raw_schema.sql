-- Cinderhaven Data Platform: Raw Schema DDL
-- Maps all 21 data tables from cinderhaven-data SQLite to Postgres.
-- Schema: raw (landing zone, mirrors source structure)
--
-- Type mapping rationale:
--   SQLite TEXT dates → Postgres DATE or TIMESTAMP (cast during staging)
--   SQLite TEXT IDs  → Postgres TEXT (preserve source format)
--   SQLite INTEGER booleans (0/1) → Postgres BOOLEAN (cast during staging)
--   SQLite REAL → Postgres NUMERIC(12,2) for money, NUMERIC(8,2) for weights/dims
--   Raw schema keeps source types loose; staging models enforce types.

CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================================
-- REFERENCE / DIMENSION SOURCES
-- ============================================================

-- 90 SKUs across 3 product lines. Contains intentional data quality
-- defects (invalid GTINs, missing brand_owner, inconsistent serving_size).
CREATE TABLE IF NOT EXISTS raw.product_master (
    sku                     TEXT,
    product_name            TEXT,
    product_line            TEXT,
    subcategory             TEXT,
    gtin14                  TEXT,
    upc                     TEXT,
    case_pack_qty           INTEGER,
    unit_weight_lbs         NUMERIC(8,4),
    case_weight_lbs         NUMERIC(8,4),
    case_length_in          NUMERIC(8,2),
    case_width_in           NUMERIC(8,2),
    case_height_in          NUMERIC(8,2),
    msrp                    NUMERIC(8,2),
    serving_size            TEXT,
    calories_per_serving    NUMERIC(8,2),
    total_fat_g             NUMERIC(8,2),
    sodium_mg               INTEGER,
    total_carb_g            NUMERIC(8,2),
    protein_g               NUMERIC(8,2),
    brand_owner             TEXT,
    country_of_origin       TEXT,
    active_retailers        TEXT,
    oneworldsync_status     TEXT,
    last_updated            TEXT,
    updated_by              TEXT
);

-- 902 retail locations across 11 retailers.
CREATE TABLE IF NOT EXISTS raw.stores (
    store_id                TEXT PRIMARY KEY,
    retailer                TEXT NOT NULL,
    chain_name              TEXT NOT NULL,
    region                  TEXT,
    state                   TEXT,
    volume_tier             TEXT,
    is_aggregated_channel   INTEGER NOT NULL DEFAULT 0
);

-- 11 retailer entities (6 contracted + 5 regional chains).
CREATE TABLE IF NOT EXISTS raw.retailers (
    retailer_id             TEXT PRIMARY KEY,
    name                    TEXT NOT NULL,
    channel_type            TEXT NOT NULL,
    dispute_portal_name     TEXT,
    dispute_portal_url      TEXT,
    dispute_method          TEXT,
    notes                   TEXT
);

-- 90 retailer-specific dispute rules per deduction type.
CREATE TABLE IF NOT EXISTS raw.retailer_rules (
    retailer_id             TEXT NOT NULL,
    deduction_type          TEXT NOT NULL,
    dispute_window_days     INTEGER,
    auto_deduct             INTEGER NOT NULL DEFAULT 0,
    evidence_required       TEXT,
    typical_recovery_rate   NUMERIC(5,4),
    notes                   TEXT,
    PRIMARY KEY (retailer_id, deduction_type)
);

-- 97 retailer-specific deduction reason codes.
CREATE TABLE IF NOT EXISTS raw.deduction_codes (
    code_id                 TEXT PRIMARY KEY,
    retailer_id             TEXT NOT NULL,
    code                    TEXT NOT NULL,
    name                    TEXT NOT NULL,
    deduction_type          TEXT NOT NULL,
    is_published            INTEGER NOT NULL DEFAULT 0
);

-- 42 EDI compliance requirements by retailer.
CREATE TABLE IF NOT EXISTS raw.edi_requirements (
    requirement_id          INTEGER PRIMARY KEY,
    retailer_id             TEXT NOT NULL,
    category                TEXT NOT NULL,
    requirement             TEXT NOT NULL,
    penalty_if_violated     TEXT,
    is_verified             INTEGER NOT NULL,
    source_url              TEXT
);

-- ============================================================
-- PRICING SOURCES
-- ============================================================

-- 90 SKU-level cost and pricing structure.
CREATE TABLE IF NOT EXISTS raw.sku_costs (
    sku                         TEXT PRIMARY KEY,
    cogs_per_unit               NUMERIC(8,2) NOT NULL,
    landed_cost_per_unit        NUMERIC(8,2) NOT NULL,
    wholesale_price             NUMERIC(8,2) NOT NULL,
    wholesale_walmart           NUMERIC(8,2) NOT NULL,
    wholesale_costco            NUMERIC(8,2) NOT NULL,
    wholesale_whole_foods       NUMERIC(8,2) NOT NULL,
    wholesale_regional          NUMERIC(8,2) NOT NULL,
    wholesale_unfi              NUMERIC(8,2) NOT NULL,
    wholesale_dtc               NUMERIC(8,2) NOT NULL,
    trade_spend_pct_walmart     NUMERIC(5,4) NOT NULL,
    trade_spend_pct_costco      NUMERIC(5,4) NOT NULL,
    trade_spend_pct_whole_foods NUMERIC(5,4) NOT NULL,
    trade_spend_pct_regional    NUMERIC(5,4) NOT NULL,
    trade_spend_pct_unfi        NUMERIC(5,4) NOT NULL,
    trade_spend_pct_dtc         NUMERIC(5,4) NOT NULL
);

-- 398 historical price changes by SKU/retailer.
CREATE TABLE IF NOT EXISTS raw.price_history (
    sku                     TEXT NOT NULL,
    retailer                TEXT NOT NULL,
    effective_date          TEXT NOT NULL,
    wholesale_price         NUMERIC(8,2) NOT NULL,
    PRIMARY KEY (sku, retailer, effective_date)
);

-- ============================================================
-- DISTRIBUTION & PROMOTION SOURCES
-- ============================================================

-- 12,507 SKU-store authorization records.
CREATE TABLE IF NOT EXISTS raw.distribution_log (
    sku                     TEXT NOT NULL,
    store_id                TEXT NOT NULL,
    authorized_date         TEXT NOT NULL,
    deauthorized_date       TEXT,
    PRIMARY KEY (sku, store_id, authorized_date)
);

-- 188 promotional events.
CREATE TABLE IF NOT EXISTS raw.promotions (
    promo_id                TEXT NOT NULL,
    sku                     TEXT NOT NULL,
    retailer                TEXT NOT NULL,
    store_scope             TEXT NOT NULL,
    start_week              TEXT NOT NULL,
    end_week                TEXT NOT NULL,
    duration_weeks          INTEGER NOT NULL,
    discount_depth_pct      NUMERIC(5,4) NOT NULL,
    promo_type              TEXT NOT NULL,
    promo_cost              NUMERIC(10,2),
    funding_mechanism       TEXT,
    PRIMARY KEY (promo_id, sku, retailer)
);

-- ============================================================
-- TRANSACTION SOURCES
-- ============================================================

-- 5,838 purchase orders.
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                        TEXT PRIMARY KEY,
    retailer_id                     TEXT NOT NULL,
    po_number                       TEXT NOT NULL,
    po_date                         TEXT NOT NULL,
    requested_ship_date             TEXT NOT NULL,
    requested_delivery_window_start TEXT,
    requested_delivery_window_end   TEXT,
    dc_id                           TEXT,
    total_units                     INTEGER NOT NULL,
    total_value                     NUMERIC(12,2) NOT NULL
);

-- 30,127 order line items.
CREATE TABLE IF NOT EXISTS raw.order_lines (
    order_line_id           INTEGER PRIMARY KEY,
    order_id                TEXT NOT NULL,
    sku                     TEXT NOT NULL,
    units_ordered           INTEGER NOT NULL,
    unit_price              NUMERIC(8,2) NOT NULL,
    line_total              NUMERIC(10,2) NOT NULL
);

-- 5,838 shipment records.
CREATE TABLE IF NOT EXISTS raw.shipments (
    shipment_id             TEXT PRIMARY KEY,
    order_id                TEXT NOT NULL,
    ship_date               TEXT NOT NULL,
    delivery_date           TEXT,
    carrier                 TEXT,
    bol_number              TEXT,
    bol_signed              INTEGER NOT NULL DEFAULT 0,
    bol_signed_short        INTEGER NOT NULL DEFAULT 0,
    bol_signed_damaged      INTEGER NOT NULL DEFAULT 0,
    pod_received            INTEGER NOT NULL DEFAULT 0,
    units_shipped           INTEGER NOT NULL,
    pallets_shipped         INTEGER,
    asn_sent                INTEGER NOT NULL DEFAULT 0,
    asn_sent_late           INTEGER NOT NULL DEFAULT 0
);

-- 5,838 pack/label compliance records.
CREATE TABLE IF NOT EXISTS raw.pack_records (
    pack_record_id              INTEGER PRIMARY KEY,
    order_id                    TEXT NOT NULL,
    shipment_id                 TEXT,
    pack_date                   TEXT NOT NULL,
    packer_initials             TEXT,
    units_picked                INTEGER NOT NULL,
    units_packed                INTEGER NOT NULL,
    units_pick_pack_match       INTEGER NOT NULL,
    label_type_used             TEXT NOT NULL,
    label_scannable             INTEGER NOT NULL,
    pack_verification           TEXT NOT NULL,
    evidence_format             TEXT NOT NULL,
    evidence_location           TEXT,
    evidence_retrieval_minutes  INTEGER
);

-- ============================================================
-- SALES / POS SOURCES
-- ============================================================

-- 1,118,009 weekly scan data (POS) records.
CREATE TABLE IF NOT EXISTS raw.scan_data (
    sku                     TEXT NOT NULL,
    store_id                TEXT NOT NULL,
    week_ending             TEXT NOT NULL,
    units_sold              INTEGER NOT NULL,
    dollars_sold            NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (sku, store_id, week_ending)
);

-- 381 chargeback events (legacy format, pre-deductions pipeline).
CREATE TABLE IF NOT EXISTS raw.chargebacks (
    month                   TEXT NOT NULL,
    retailer                TEXT NOT NULL,
    reason                  TEXT NOT NULL,
    amount                  NUMERIC(10,2) NOT NULL,
    sku                     TEXT NOT NULL
);

-- ============================================================
-- DEDUCTION / DISPUTE SOURCES
-- ============================================================

-- 3,087 deduction records.
CREATE TABLE IF NOT EXISTS raw.deductions (
    deduction_id            TEXT PRIMARY KEY,
    retailer_id             TEXT NOT NULL,
    order_id                TEXT,
    shipment_id             TEXT,
    deduction_type          TEXT NOT NULL,
    code_id                 TEXT,
    code_as_remitted        TEXT,
    remittance_description  TEXT,
    amount                  NUMERIC(10,2) NOT NULL,
    deduction_date          TEXT NOT NULL,
    dispute_deadline        TEXT,
    is_vague                INTEGER NOT NULL DEFAULT 0,
    is_post_audit           INTEGER NOT NULL DEFAULT 0,
    is_double_dip           INTEGER NOT NULL DEFAULT 0,
    remittance_id           TEXT
);

-- 1,410 dispute filing records.
CREATE TABLE IF NOT EXISTS raw.disputes (
    dispute_id                  TEXT PRIMARY KEY,
    deduction_id                TEXT NOT NULL,
    filed_date                  TEXT,
    filing_method               TEXT,
    evidence_quality            TEXT NOT NULL,
    submitted_evidence_count    INTEGER NOT NULL,
    was_within_deadline         INTEGER,
    outcome                     TEXT NOT NULL,
    recovered_amount            NUMERIC(10,2),
    closed_date                 TEXT,
    labor_hours                 NUMERIC(6,2) NOT NULL
);

-- 3,092 evidence items linked to disputes.
CREATE TABLE IF NOT EXISTS raw.dispute_evidence (
    evidence_id             INTEGER PRIMARY KEY,
    dispute_id              TEXT NOT NULL,
    evidence_type           TEXT NOT NULL,
    was_submitted           INTEGER NOT NULL,
    was_required            INTEGER NOT NULL,
    format                  TEXT,
    notes                   TEXT
);

-- 515 remittance (payment) bundles from retailers.
CREATE TABLE IF NOT EXISTS raw.remittances (
    remittance_id           TEXT PRIMARY KEY,
    retailer_id             TEXT NOT NULL,
    received_date           TEXT NOT NULL,
    format                  TEXT NOT NULL,
    gross_amount            NUMERIC(12,2) NOT NULL,
    net_amount              NUMERIC(12,2) NOT NULL,
    total_deductions        NUMERIC(12,2) NOT NULL,
    clarity                 TEXT NOT NULL
);

-- 45 post-audit (retroactive) claims.
CREATE TABLE IF NOT EXISTS raw.post_audit_claims (
    claim_id                TEXT PRIMARY KEY,
    deduction_id            TEXT NOT NULL,
    auditor_name            TEXT,
    audit_period_start      TEXT,
    audit_period_end        TEXT,
    claim_type              TEXT,
    lookback_months         INTEGER
);

-- ============================================================
-- DTC / E-COMMERCE SOURCES
-- ============================================================

-- ~10,000 Shopify DTC order headers.
CREATE TABLE IF NOT EXISTS raw.shopify_orders (
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
    discount_amount         NUMERIC(10,2) NOT NULL DEFAULT 0.0,
    subtotal                NUMERIC(10,2) NOT NULL,
    shipping_cost           NUMERIC(10,2) NOT NULL,
    total_tax               NUMERIC(10,2) NOT NULL,
    total                   NUMERIC(10,2) NOT NULL,
    carrier                 TEXT,
    tracking_number         TEXT,
    fulfilled_at            TEXT
);

-- ~19,000 Shopify DTC order line items.
CREATE TABLE IF NOT EXISTS raw.shopify_order_lines (
    line_id                 INTEGER PRIMARY KEY,
    order_id                TEXT NOT NULL,
    sku                     TEXT NOT NULL,
    product_name            TEXT NOT NULL,
    quantity                INTEGER NOT NULL,
    unit_price              NUMERIC(8,2) NOT NULL,
    line_total              NUMERIC(10,2) NOT NULL
);

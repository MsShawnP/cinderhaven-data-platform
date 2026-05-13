# Data Gap Assessment — Cinderhaven Data Platform

Comparing existing cinderhaven-data (21 tables, SQLite) against the
brief's specified source layers.

## What exists (21 tables, ready to ingest)

| Domain | Table | Rows | Status |
|--------|-------|------|--------|
| Product master | product_master | 90 | Exists — includes intentional defects |
| Stores | stores | 902 | Exists |
| Retailers | retailers | 11 | Exists |
| Retailer rules | retailer_rules | 90 | Exists |
| Deduction codes | deduction_codes | 97 | Exists |
| EDI requirements | edi_requirements | 42 | Exists |
| Costs/pricing | sku_costs | 90 | Exists |
| Price history | price_history | 398 | Exists |
| Distribution | distribution_log | 12,507 | Exists |
| Promotions | promotions | 188 | Exists |
| Orders | orders | 5,838 | Exists |
| Order lines | order_lines | 30,127 | Exists |
| Shipments | shipments | 5,838 | Exists |
| Pack records | pack_records | 5,838 | Exists |
| Scan data (POS) | scan_data | 1,118,009 | Exists |
| Chargebacks | chargebacks | 381 | Exists (legacy format) |
| Deductions | deductions | 3,087 | Exists |
| Disputes | disputes | 1,410 | Exists |
| Dispute evidence | dispute_evidence | 3,092 | Exists |
| Remittances | remittances | 515 | Exists |
| Post-audit claims | post_audit_claims | 45 | Exists |

## What the brief specifies vs. reality

| Brief's source layer | Represented by | Gap? |
|----------------------|----------------|------|
| Product master (NetSuite/1WorldSync) | product_master | No gap |
| Chargeback ledger (retailer remittance) | chargebacks + deductions | No gap |
| Shipment records (3PL/WMS) | shipments + pack_records | No gap |
| Sales orders/contracts (NetSuite) | orders + order_lines | No gap |
| Remittance stubs (retailer AP portals) | remittances + deductions | No gap |
| Payment records (bank/AR ledger) | remittances (partial) | **Partial gap** |
| POS/sell-through (Walmart RL, UNFI Connect) | scan_data (1.1M rows) | No gap |
| Shopify orders (DTC) | — | **Gap** |
| EDI traffic (EDI provider exports) | DEFERRED to EDI Pre-flight | No gap (by design) |

## Gap details

### 1. Shopify DTC orders — GAP

The brief specifies ~10,000 synthetic Shopify DTC orders. Nothing
exists in the current dataset. The `scan_data` table covers retail
POS but not DTC e-commerce.

**Recommendation:** Generate synthetic Shopify-style orders with
fields matching a real Shopify export (order_id, email, created_at,
financial_status, fulfillment_status, line items with SKU/quantity/
price, shipping address state, discount codes, etc.). ~10,000 orders
over the 18-month window. This is new generation work.

**Effort:** Medium — need to design realistic Shopify export schema
and generate data that joins to existing product_master SKUs.

### 2. Payment records — PARTIAL GAP

The brief specifies a separate payment records table (bank/AR ledger).
The `remittances` table covers retailer payments but is structured as
payment bundles, not individual payment events mapped to invoices.

**Recommendation:** The remittances table is sufficient for v1. A
separate `payments` table could be generated later to model bank-side
AR reconciliation, but it's not critical for the deduction crosswalk
or order-to-cash mart that are the platform's showpieces.

**Effort:** Low if deferred. Medium if generated.

### 3. POS/sell-through shape — NO GAP

The brief estimated ~50,000 weekly SKU/store/retailer POS records.
The actual scan_data table has 1,118,009 rows — far exceeding the
estimate. It includes retailer information via the store_id → stores
join. This is not a gap; the brief's estimate was conservative.

## Summary

- **21 of 21 tables** ready to ingest as-is
- **1 genuine gap:** Shopify DTC orders (new generation needed)
- **1 partial gap:** Payment records (remittances covers most use
  cases; defer dedicated payments table)
- **1 deferred by design:** EDI traffic (comes from EDI Pre-flight)

**Recommendation for v1:** Generate Shopify DTC orders. Defer
dedicated payment records table. Proceed with all 21 existing tables.

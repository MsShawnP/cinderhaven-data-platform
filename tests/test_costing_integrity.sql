-- Referential and rollup integrity for the costing schema.
--
-- These replace the cross-schema FOREIGN KEYs that were deliberately NOT
-- created: `DROP SCHEMA raw CASCADE` silently drops FK constraints living in
-- `costing` while leaving the tables and rows intact, after which orphan rows
-- are accepted. Constraints that CASCADE eats are not protection.
--
-- Returns one row per failing check. Zero rows = pass.

SELECT 'orphan sku' AS check_name, COUNT(*) AS failures FROM (
    SELECT sku FROM costing.fct_product_costs
    UNION SELECT sku FROM costing.dim_inventory_lots
    UNION SELECT sku FROM costing.fct_inventory_snapshot
    UNION SELECT sku FROM costing.fct_lot_shipments
    UNION SELECT sku FROM costing.fct_lot_balances
) x WHERE sku NOT IN (SELECT sku FROM raw.product_master)
HAVING COUNT(*) > 0

UNION ALL
SELECT 'orphan store_id', COUNT(*) FROM costing.fct_lot_shipments
WHERE store_id NOT IN (SELECT store_id FROM raw.stores)
HAVING COUNT(*) > 0

UNION ALL
SELECT 'orphan shipment_id', COUNT(*) FROM costing.fct_lot_shipments
WHERE shipment_id NOT IN (SELECT shipment_id FROM raw.retailer_shipments)
HAVING COUNT(*) > 0

UNION ALL
SELECT 'lot balance exceeds lot size', COUNT(*)
FROM costing.fct_lot_balances b JOIN costing.dim_inventory_lots l USING (lot_code)
WHERE b.units_remaining > l.units_produced
HAVING COUNT(*) > 0

UNION ALL
SELECT 'lot balance increased over time', COUNT(*) FROM (
    SELECT lot_code, units_remaining,
           LAG(units_remaining) OVER (PARTITION BY lot_code ORDER BY snapshot_date) AS prev
    FROM costing.fct_lot_balances
) z WHERE prev IS NOT NULL AND units_remaining > prev
HAVING COUNT(*) > 0

UNION ALL
SELECT 'supplier invoices vs COGS outside 2pct', 1
WHERE ABS(
    ((SELECT SUM(invoice_amount) FROM costing.fct_supplier_invoices)
   - (SELECT SUM(units_basis * manufactured_cost_per_unit) FROM costing.fct_product_costs))
  / NULLIF((SELECT SUM(units_basis * manufactured_cost_per_unit) FROM costing.fct_product_costs), 0)
) > 0.02;

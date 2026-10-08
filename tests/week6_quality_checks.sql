SELECT 'FAIL: dup customer_ids' AS test, COUNT(*) AS issues
FROM (SELECT customer_id FROM dim_customers 
      GROUP BY customer_id HAVING COUNT(*) > 1) x
UNION ALL
SELECT 'FAIL: null customer_ids', COUNT(*)
FROM dim_customers WHERE customer_id IS NULL
UNION ALL
SELECT 'FAIL: sales w/o matching product', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_products p ON f.product_id = p.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'FAIL: sales w/o matching store', COUNT(*)
FROM fact_sales f
LEFT JOIN dim_store s ON f.store_id = s.store_id
WHERE f.store_id IS NOT NULL AND s.store_id IS NULL
UNION ALL
SELECT 'FAIL: negative inventory', COUNT(*)
FROM fact_inventory_snapshot WHERE quantity_on_hand < 0
UNION ALL
SELECT 'FAIL: inventory w/o product', COUNT(*)
FROM fact_inventory_snapshot i
LEFT JOIN dim_products p ON i.product_id = p.product_id
WHERE p.product_id IS NULL
UNION ALL
SELECT 'FAIL: inventory w/o warehouse', COUNT(*)
FROM fact_inventory_snapshot i
LEFT JOIN dim_warehouse w ON i.warehouse_id = w.warehouse_id
WHERE w.warehouse_id IS NULL
ORDER BY test;
-- Aggregates and extended grouping.

SELECT status, COUNT(*) AS orders_count, SUM(total_cents) AS total_cents
FROM rt_sales_orders
GROUP BY status
HAVING SUM(total_cents) >= 52000
ORDER BY total_cents DESC;

SELECT b.city, SUM(so.total_cents) AS total_cents
FROM rt_sales_orders AS so
JOIN rt_branches AS b ON so.branch_id = b.id
GROUP BY ROLLUP(b.city)
ORDER BY total_cents DESC;

SELECT p.sku, SUM(line.quantity) AS units_sold, SUM(line.quantity * line.price_cents) AS gross_cents
FROM rt_sales_order_lines AS line
JOIN rt_products AS p ON line.product_id = p.id
GROUP BY p.sku
HAVING SUM(line.quantity) >= 1
ORDER BY gross_cents DESC;

EXPLAIN
SELECT status, SUM(total_cents) AS total_cents
FROM rt_sales_orders
GROUP BY status
HAVING SUM(total_cents) > 0;


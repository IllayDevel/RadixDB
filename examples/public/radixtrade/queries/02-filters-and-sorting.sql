-- WHERE, boolean logic, IN, LIKE, ORDER BY, LIMIT and OFFSET.

SELECT customer_no, name, city, email
FROM rt_customers
WHERE active = true
  AND __deleted_at IS NULL
ORDER BY customer_no;

SELECT sku, name
FROM rt_products
WHERE sku IN ('TEA-ALT-100', 'KBD-MECH-01')
   OR name LIKE '%coffee%'
ORDER BY sku;

SELECT order_no, order_date, status, total_cents
FROM rt_sales_orders
WHERE status IN ('paid', 'reserved', 'new')
  AND total_cents >= 52000
ORDER BY total_cents DESC
LIMIT 2 OFFSET 0;


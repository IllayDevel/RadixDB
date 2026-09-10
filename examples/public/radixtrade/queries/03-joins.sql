-- Joins across orders, customers, branches, warehouses and products.

SELECT so.order_no, c.name AS customer, b.city AS branch_city, so.status, so.total_cents
FROM rt_sales_orders AS so
JOIN rt_customers AS c ON so.customer_id = c.id
JOIN rt_branches AS b ON so.branch_id = b.id
ORDER BY so.order_no;

SELECT so.order_no, p.sku, p.name, line.quantity, line.price_cents
FROM rt_sales_order_lines AS line
JOIN rt_sales_orders AS so ON line.sales_order_id = so.id
JOIN rt_products AS p ON line.product_id = p.id
ORDER BY so.order_no, p.sku;

SELECT so.order_no, sh.shipment_no, wh.code AS warehouse, sh.status
FROM rt_shipments AS sh
JOIN rt_sales_orders AS so ON sh.sales_order_id = so.id
JOIN rt_warehouses AS wh ON sh.warehouse_id = wh.id
ORDER BY so.order_no;

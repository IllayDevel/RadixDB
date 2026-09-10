-- First look at the imported RadixTrade database.

SHOW TABLES;

DESCRIBE rt_sales_orders;

SHOW INDEXES FROM rt_customers;

SELECT code, name, city
FROM rt_branches
ORDER BY code;

SELECT sku, name, unit
FROM rt_products
ORDER BY sku;


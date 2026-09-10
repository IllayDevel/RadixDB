-- Optimistic update: update only when the caller still sees the expected
-- revision. Client code would bind id and expected revision as parameters.

SELECT order_no, status, revision
FROM rt_sales_orders
WHERE id = 3;

UPDATE rt_sales_orders
SET status = 'reserved',
    revision = revision + 1
WHERE id = 3
  AND revision = 1;

SELECT order_no, status, revision
FROM rt_sales_orders
WHERE id = 3;

UPDATE rt_sales_orders
SET status = 'paid',
    revision = revision + 1
WHERE id = 3
  AND revision = 1;

SELECT order_no, status, revision
FROM rt_sales_orders
WHERE id = 3;

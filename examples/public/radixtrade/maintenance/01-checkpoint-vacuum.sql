-- RadixTrade maintenance smoke.
-- Run this only after importing schema.sql and seed-small.sql.

SHOW TABLES;
DESCRIBE rt_sales_orders;
SHOW INDEXES FROM rt_sales_orders;

PRAGMA VOLUME_STATS;
PRAGMA CHECKPOINT;

VACUUM rt_app_events;

SELECT COUNT(*) AS order_count
FROM rt_sales_orders;


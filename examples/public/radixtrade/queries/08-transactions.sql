-- BEGIN, COMMIT and ROLLBACK.

BEGIN;

INSERT INTO rt_app_events (event_type, entity_name, entity_id, payload)
VALUES ('transaction_committed', 'rt_sales_orders', 1, '{"demo":"commit"}');

COMMIT;

SELECT COUNT(*) AS committed_events
FROM rt_app_events
WHERE event_type = 'transaction_committed';

BEGIN;

INSERT INTO rt_app_events (event_type, entity_name, entity_id, payload)
VALUES ('transaction_rolled_back', 'rt_sales_orders', 1, '{"demo":"rollback"}');

ROLLBACK;

SELECT COUNT(*) AS rolled_back_events
FROM rt_app_events
WHERE event_type = 'transaction_rolled_back';

-- INSERT, UPDATE and DELETE on a small application event stream.

INSERT INTO rt_app_events (event_type, entity_name, entity_id, payload)
VALUES ('demo_event', 'rt_sales_orders', 3, '{"source":"query-tour"}');

SELECT event_type, entity_name, processed
FROM rt_app_events
WHERE event_type = 'demo_event';

UPDATE rt_app_events
SET processed = true
WHERE event_type = 'demo_event'
  AND processed = false;

SELECT event_type, processed
FROM rt_app_events
WHERE event_type = 'demo_event';

DELETE FROM rt_app_events
WHERE event_type = 'demo_event';

SELECT COUNT(*) AS demo_events_left
FROM rt_app_events
WHERE event_type = 'demo_event';

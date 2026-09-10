-- Small deterministic seed for RadixTrade Group.

INSERT INTO rt_branches (id, code, name, city)
VALUES (1, 'BRN-BRN', 'Barnaul central branch', 'Barnaul');

INSERT INTO rt_branches (id, code, name, city)
VALUES (2, 'BRN-NSK', 'Novosibirsk branch', 'Novosibirsk');

INSERT INTO rt_branches (id, code, name, city, active)
VALUES (3, 'BRN-OMS', 'Omsk branch', 'Omsk', true);

INSERT INTO rt_employees (id, branch_id, employee_no, full_name, role_name)
VALUES (1, 1, 'EMP-001', 'Anna Morozova', 'manager');

INSERT INTO rt_employees (id, branch_id, employee_no, full_name, role_name)
VALUES (2, 1, 'EMP-002', 'Ivan Sokolov', 'sales');

INSERT INTO rt_employees (id, branch_id, employee_no, full_name, role_name)
VALUES (3, 2, 'EMP-003', 'Maria Petrova', 'warehouse');

INSERT INTO rt_customer_groups (id, code, name, discount_percent)
VALUES (1, 'RETAIL', 'Retail customers', 0);

INSERT INTO rt_customer_groups (id, code, name, discount_percent)
VALUES (2, 'WHOLESALE', 'Wholesale customers', 7);

INSERT INTO rt_customers (id, group_id, customer_no, name, email, phone, city)
VALUES (1, 1, 'CUS-001', 'Northern Retail LLC', 'buy@northern.example', '+7-3852-100-001', 'Barnaul');

INSERT INTO rt_customers (id, group_id, customer_no, name, email, phone, city)
VALUES (2, 2, 'CUS-002', 'Siberian Wholesale JSC', 'orders@sibwholesale.example', '+7-383-200-002', 'Novosibirsk');

INSERT INTO rt_customers (id, group_id, customer_no, name, email, phone, city, __deleted_at)
VALUES (3, 1, 'CUS-003', 'Old Customer Archive', 'archive@example.test', '+7-000-000-003', 'Tomsk', '2026-01-10T09:00:00Z');

INSERT INTO rt_suppliers (id, supplier_no, name, city, rating)
VALUES (1, 'SUP-001', 'Altai Foods', 'Barnaul', 9);

INSERT INTO rt_suppliers (id, supplier_no, name, city, rating)
VALUES (2, 'SUP-002', 'Siberian Electronics', 'Novosibirsk', 8);

INSERT INTO rt_warehouses (id, branch_id, code, name, city)
VALUES (1, 1, 'WH-BRN-1', 'Barnaul main warehouse', 'Barnaul');

INSERT INTO rt_warehouses (id, branch_id, code, name, city)
VALUES (2, 2, 'WH-NSK-1', 'Novosibirsk warehouse', 'Novosibirsk');

INSERT INTO rt_product_categories (id, parent_id, code, name)
VALUES (1, NULL, 'FOOD', 'Food');

INSERT INTO rt_product_categories (id, parent_id, code, name)
VALUES (2, NULL, 'TECH', 'Electronics');

INSERT INTO rt_product_categories (id, parent_id, code, name)
VALUES (3, 1, 'TEA', 'Tea and coffee');

INSERT INTO rt_products (id, category_id, sku, name, unit)
VALUES (1, 3, 'TEA-ALT-100', 'Altai mountain tea 100g', 'pack');

INSERT INTO rt_products (id, category_id, sku, name, unit)
VALUES (2, 3, 'COF-SIB-250', 'Siberian roast coffee 250g', 'pack');

INSERT INTO rt_products (id, category_id, sku, name, unit)
VALUES (3, 2, 'KBD-MECH-01', 'Mechanical keyboard', 'pcs');

INSERT INTO rt_products (id, category_id, sku, name, unit)
VALUES (4, 2, 'MOU-WLS-02', 'Wireless mouse', 'pcs');

INSERT INTO rt_price_lists (id, code, name, currency)
VALUES (1, 'BASE-2026', 'Base retail prices 2026', 'RUB');

INSERT INTO rt_price_lists (id, code, name, currency)
VALUES (2, 'WHOLE-2026', 'Wholesale prices 2026', 'RUB');

INSERT INTO rt_price_list_items (id, price_list_id, product_id, price_cents)
VALUES (1, 1, 1, 29000);

INSERT INTO rt_price_list_items (id, price_list_id, product_id, price_cents)
VALUES (2, 1, 2, 52000);

INSERT INTO rt_price_list_items (id, price_list_id, product_id, price_cents)
VALUES (3, 1, 3, 890000);

INSERT INTO rt_price_list_items (id, price_list_id, product_id, price_cents)
VALUES (4, 1, 4, 210000);

INSERT INTO rt_stock_balances (id, warehouse_id, product_id, quantity, reserved)
VALUES (1, 1, 1, 1200, 80);

INSERT INTO rt_stock_balances (id, warehouse_id, product_id, quantity, reserved)
VALUES (2, 1, 3, 60, 4);

INSERT INTO rt_stock_balances (id, warehouse_id, product_id, quantity, reserved)
VALUES (3, 2, 2, 700, 20);

INSERT INTO rt_stock_balances (id, warehouse_id, product_id, quantity, reserved)
VALUES (4, 2, 4, 140, 7);

INSERT INTO rt_purchase_orders (id, supplier_id, branch_id, order_no, order_date, status, total_cents)
VALUES (1, 1, 1, 'PO-2026-0001', '2026-08-01', 'received', 2900000);

INSERT INTO rt_purchase_orders (id, supplier_id, branch_id, order_no, order_date, status, total_cents)
VALUES (2, 2, 2, 'PO-2026-0002', '2026-08-02', 'ordered', 11000000);

INSERT INTO rt_purchase_order_lines (id, purchase_order_id, product_id, quantity, price_cents)
VALUES (1, 1, 1, 100, 18000);

INSERT INTO rt_purchase_order_lines (id, purchase_order_id, product_id, quantity, price_cents)
VALUES (2, 1, 2, 50, 22000);

INSERT INTO rt_purchase_order_lines (id, purchase_order_id, product_id, quantity, price_cents)
VALUES (3, 2, 3, 10, 690000);

INSERT INTO rt_sales_orders (id, customer_id, branch_id, order_no, order_date, status, total_cents, paid_cents)
VALUES (1, 1, 1, 'SO-2026-0001', '2026-08-03', 'paid', 89000, 89000);

INSERT INTO rt_sales_orders (id, customer_id, branch_id, order_no, order_date, status, total_cents, paid_cents)
VALUES (2, 2, 2, 'SO-2026-0002', '2026-08-04', 'reserved', 1100000, 300000);

INSERT INTO rt_sales_orders (id, customer_id, branch_id, order_no, order_date, status, total_cents, paid_cents)
VALUES (3, 1, 1, 'SO-2026-0003', '2026-08-05', 'new', 52000, 0);

INSERT INTO rt_sales_order_lines (id, sales_order_id, product_id, quantity, price_cents, discount_cents)
VALUES (1, 1, 1, 2, 29000, 0);

INSERT INTO rt_sales_order_lines (id, sales_order_id, product_id, quantity, price_cents, discount_cents)
VALUES (2, 1, 2, 1, 52000, 21000);

INSERT INTO rt_sales_order_lines (id, sales_order_id, product_id, quantity, price_cents, discount_cents)
VALUES (3, 2, 3, 1, 890000, 0);

INSERT INTO rt_sales_order_lines (id, sales_order_id, product_id, quantity, price_cents, discount_cents)
VALUES (4, 2, 4, 1, 210000, 0);

INSERT INTO rt_sales_order_lines (id, sales_order_id, product_id, quantity, price_cents, discount_cents)
VALUES (5, 3, 2, 1, 52000, 0);

INSERT INTO rt_payments (id, sales_order_id, payment_no, payment_date, method, amount_cents, status)
VALUES (1, 1, 'PAY-2026-0001', '2026-08-03', 'card', 89000, 'captured');

INSERT INTO rt_payments (id, sales_order_id, payment_no, payment_date, method, amount_cents, status)
VALUES (2, 2, 'PAY-2026-0002', '2026-08-04', 'bank', 300000, 'captured');

INSERT INTO rt_shipments (id, sales_order_id, warehouse_id, shipment_no, shipped_at, status)
VALUES (1, 1, 1, 'SHP-2026-0001', '2026-08-04T10:30:00Z', 'shipped');

INSERT INTO rt_shipments (id, sales_order_id, warehouse_id, shipment_no, shipped_at, status)
VALUES (2, 2, 2, 'SHP-2026-0002', NULL, 'waiting');

INSERT INTO rt_app_events (id, event_type, entity_name, entity_id, payload, processed)
VALUES ('01940000-0017-7000-8000-000000000001', 'order_created', 'rt_sales_orders', 1, '{"order_no":"SO-2026-0001"}', true);

INSERT INTO rt_app_events (id, event_type, entity_name, entity_id, payload, processed)
VALUES ('01940000-0017-7000-8000-000000000002', 'payment_captured', 'rt_payments', 1, '{"payment_no":"PAY-2026-0001"}', false);


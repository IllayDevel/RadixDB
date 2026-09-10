-- RadixTrade Group tutorial schema.
-- This schema intentionally uses current RadixDB features only.
--
-- The core tutorial relations use INTEGER primary keys because they are easy
-- to read in printed SQL output and stable across file reopen/cold scans.
-- UUID primary keys are demonstrated separately in queries/07-uuid-primary-key.sql.

DROP TABLE IF EXISTS rt_expected_error_users;
DROP TABLE IF EXISTS rt_uuid_demo;
DROP TABLE IF EXISTS rt_index_soft_delete_demo;
DROP TABLE IF EXISTS rt_app_events;
DROP TABLE IF EXISTS rt_shipments;
DROP TABLE IF EXISTS rt_payments;
DROP TABLE IF EXISTS rt_sales_order_lines;
DROP TABLE IF EXISTS rt_sales_orders;
DROP TABLE IF EXISTS rt_purchase_order_lines;
DROP TABLE IF EXISTS rt_purchase_orders;
DROP TABLE IF EXISTS rt_stock_balances;
DROP TABLE IF EXISTS rt_price_list_items;
DROP TABLE IF EXISTS rt_price_lists;
DROP TABLE IF EXISTS rt_products;
DROP TABLE IF EXISTS rt_product_categories;
DROP TABLE IF EXISTS rt_warehouses;
DROP TABLE IF EXISTS rt_suppliers;
DROP TABLE IF EXISTS rt_customers;
DROP TABLE IF EXISTS rt_customer_groups;
DROP TABLE IF EXISTS rt_employees;
DROP TABLE IF EXISTS rt_branches;

CREATE TABLE rt_branches (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_branches_code_uidx
    ON rt_branches (code);

CREATE INDEX rt_branches_city_idx
    ON rt_branches (city);

CREATE TABLE rt_employees (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    branch_id INTEGER NOT NULL REFERENCES rt_branches(id),
    employee_no TEXT NOT NULL,
    full_name TEXT NOT NULL,
    role_name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_employees_no_uidx
    ON rt_employees (employee_no);

CREATE INDEX rt_employees_role_branch_idx
    ON rt_employees (role_name, branch_id);

CREATE TABLE rt_customer_groups (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    discount_percent INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX rt_customer_groups_code_uidx
    ON rt_customer_groups (code);

CREATE TABLE rt_customers (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    group_id INTEGER REFERENCES rt_customer_groups(id),
    customer_no TEXT NOT NULL,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    city TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    __deleted_at TIMESTAMP,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_customers_no_uidx
    ON rt_customers (customer_no);

CREATE UNIQUE INDEX rt_customers_email_active_uidx
    ON rt_customers (email)
    WHERE __deleted_at IS NULL;

CREATE INDEX rt_customers_city_idx
    ON rt_customers (city);

CREATE TABLE rt_suppliers (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    supplier_no TEXT NOT NULL,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    rating INTEGER NOT NULL DEFAULT 5,
    active BOOLEAN NOT NULL DEFAULT true,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_suppliers_no_uidx
    ON rt_suppliers (supplier_no);

CREATE INDEX rt_suppliers_city_rating_idx
    ON rt_suppliers (city, rating);

CREATE TABLE rt_warehouses (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    branch_id INTEGER NOT NULL REFERENCES rt_branches(id),
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    city TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_warehouses_code_uidx
    ON rt_warehouses (code);

CREATE TABLE rt_product_categories (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    parent_id INTEGER,
    code TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE UNIQUE INDEX rt_product_categories_code_uidx
    ON rt_product_categories (code);

CREATE TABLE rt_products (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    category_id INTEGER NOT NULL REFERENCES rt_product_categories(id),
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    unit TEXT NOT NULL DEFAULT 'pcs',
    active BOOLEAN NOT NULL DEFAULT true,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_products_sku_uidx
    ON rt_products (sku);

CREATE TABLE rt_price_lists (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'RUB',
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX rt_price_lists_code_uidx
    ON rt_price_lists (code);

CREATE TABLE rt_price_list_items (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    price_list_id INTEGER NOT NULL REFERENCES rt_price_lists(id),
    product_id INTEGER NOT NULL REFERENCES rt_products(id),
    price_cents INTEGER NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX rt_price_list_items_scope_uidx
    ON rt_price_list_items (price_list_id, product_id);

CREATE TABLE rt_stock_balances (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    warehouse_id INTEGER NOT NULL REFERENCES rt_warehouses(id),
    product_id INTEGER NOT NULL REFERENCES rt_products(id),
    quantity INTEGER NOT NULL DEFAULT 0,
    reserved INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_stock_balances_scope_uidx
    ON rt_stock_balances (warehouse_id, product_id);

CREATE TABLE rt_purchase_orders (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    supplier_id INTEGER NOT NULL REFERENCES rt_suppliers(id),
    branch_id INTEGER NOT NULL REFERENCES rt_branches(id),
    order_no TEXT NOT NULL,
    order_date TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_purchase_orders_no_uidx
    ON rt_purchase_orders (order_no);

CREATE INDEX rt_purchase_orders_status_supplier_idx
    ON rt_purchase_orders (status, supplier_id);

CREATE TABLE rt_purchase_order_lines (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    purchase_order_id INTEGER NOT NULL REFERENCES rt_purchase_orders(id),
    product_id INTEGER NOT NULL REFERENCES rt_products(id),
    quantity INTEGER NOT NULL,
    price_cents INTEGER NOT NULL
);

CREATE TABLE rt_sales_orders (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    customer_id INTEGER NOT NULL REFERENCES rt_customers(id),
    branch_id INTEGER NOT NULL REFERENCES rt_branches(id),
    order_no TEXT NOT NULL,
    order_date TEXT NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL DEFAULT 0,
    paid_cents INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX rt_sales_orders_no_uidx
    ON rt_sales_orders (order_no);

CREATE INDEX rt_sales_orders_status_customer_idx
    ON rt_sales_orders (status, customer_id);

CREATE INDEX rt_sales_orders_date_branch_idx
    ON rt_sales_orders (order_date, branch_id);

CREATE TABLE rt_sales_order_lines (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    sales_order_id INTEGER NOT NULL REFERENCES rt_sales_orders(id),
    product_id INTEGER NOT NULL REFERENCES rt_products(id),
    quantity INTEGER NOT NULL,
    price_cents INTEGER NOT NULL,
    discount_cents INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE rt_payments (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    sales_order_id INTEGER NOT NULL REFERENCES rt_sales_orders(id),
    payment_no TEXT NOT NULL,
    payment_date TEXT NOT NULL,
    method TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL
);

CREATE UNIQUE INDEX rt_payments_no_uidx
    ON rt_payments (payment_no);

CREATE TABLE rt_shipments (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    sales_order_id INTEGER NOT NULL REFERENCES rt_sales_orders(id),
    warehouse_id INTEGER NOT NULL REFERENCES rt_warehouses(id),
    shipment_no TEXT NOT NULL,
    shipped_at TEXT,
    status TEXT NOT NULL
);

CREATE UNIQUE INDEX rt_shipments_no_uidx
    ON rt_shipments (shipment_no);

CREATE TABLE rt_app_events (
    id UUID PRIMARY KEY AUTO_INCREMENT,
    event_type TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    entity_id INTEGER,
    payload TEXT,
    processed BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX rt_app_events_processed_idx
    ON rt_app_events (processed);

CREATE INDEX rt_app_events_entity_idx
    ON rt_app_events (entity_name, entity_id);


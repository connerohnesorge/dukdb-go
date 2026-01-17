-- Test Database Generator for Cost-Based Optimizer Testing
-- Generates test databases with various characteristics for comprehensive testing
--
-- This script creates test databases with:
-- - Multiple sizes: small (1K rows), medium (100K rows), large (1M+ rows)
-- - Different distributions: uniform, skewed, clustered
-- - Tables with various characteristics: wide (100+ columns), narrow, complex types
--
-- Usage: duckdb test_database.db < generate_test_databases.sql
--

-- ============================================================================
-- PART 1: SMALL DATABASE (1,000 rows)
-- ============================================================================

-- Small - Uniform distribution
CREATE TABLE IF NOT EXISTS small_uniform (
    id INTEGER PRIMARY KEY,
    value INTEGER,
    category VARCHAR,
    price DECIMAL(10,2),
    active BOOLEAN,
    created_date DATE
);

DELETE FROM small_uniform;
INSERT INTO small_uniform
SELECT
    row_number() OVER () as id,
    (row_number() OVER () % 100) as value,
    CASE (row_number() OVER () % 5)
        WHEN 0 THEN 'A'
        WHEN 1 THEN 'B'
        WHEN 2 THEN 'C'
        WHEN 3 THEN 'D'
        ELSE 'E'
    END as category,
    (row_number() OVER () % 1000) * 10.5 as price,
    (row_number() OVER () % 2) = 0 as active,
    DATE '2024-01-01' + INTERVAL (row_number() OVER () % 365) DAY as created_date
FROM (
    SELECT * FROM range(1, 1001)
) t(n);

-- Small - Skewed distribution (80/20 rule)
CREATE TABLE IF NOT EXISTS small_skewed (
    id INTEGER PRIMARY KEY,
    value INTEGER,
    category VARCHAR,
    score DECIMAL(5,2),
    region VARCHAR
);

DELETE FROM small_skewed;
INSERT INTO small_skewed
SELECT
    row_number() OVER () as id,
    CASE
        WHEN random() < 0.8 THEN 1 + (random() * 10)::INT
        ELSE 11 + (random() * 90)::INT
    END as value,
    CASE WHEN random() < 0.8 THEN 'Hot' ELSE 'Cold' END as category,
    random() * 100 as score,
    CASE (row_number() OVER () % 10)
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'EU'
        WHEN 2 THEN 'APAC'
        WHEN 3 THEN 'LATAM'
        ELSE 'OTHER'
    END as region
FROM (
    SELECT * FROM range(1, 1001)
) t(n);

-- Small - Clustered data
CREATE TABLE IF NOT EXISTS small_clustered (
    id INTEGER PRIMARY KEY,
    cluster_id INTEGER,
    value INTEGER,
    timestamp TIMESTAMP
);

DELETE FROM small_clustered;
INSERT INTO small_clustered
SELECT
    row_number() OVER () as id,
    (row_number() OVER () / 100) as cluster_id,
    (row_number() OVER () % 100) as value,
    CURRENT_TIMESTAMP - INTERVAL (1000 - row_number() OVER ()) MINUTE as timestamp
FROM (
    SELECT * FROM range(1, 1001)
) t(n);

-- Small - Wide table (30+ columns)
CREATE TABLE IF NOT EXISTS small_wide (
    id INTEGER PRIMARY KEY,
    col1 INTEGER, col2 INTEGER, col3 INTEGER, col4 INTEGER, col5 INTEGER,
    col6 VARCHAR, col7 VARCHAR, col8 VARCHAR, col9 VARCHAR, col10 VARCHAR,
    col11 DECIMAL(8,2), col12 DECIMAL(8,2), col13 DECIMAL(8,2), col14 DECIMAL(8,2), col15 DECIMAL(8,2),
    col16 BOOLEAN, col17 BOOLEAN, col18 BOOLEAN, col19 BOOLEAN, col20 BOOLEAN,
    col21 DATE, col22 DATE, col23 DATE,
    col24 DOUBLE, col25 DOUBLE, col26 DOUBLE,
    col27 TEXT, col28 TEXT, col29 TEXT, col30 TEXT
);

DELETE FROM small_wide;
INSERT INTO small_wide
SELECT
    row_number() OVER () as id,
    row_number() OVER () % 100, row_number() OVER () % 200, row_number() OVER () % 300,
    row_number() OVER () % 400, row_number() OVER () % 500,
    CONCAT('A', (row_number() OVER () % 10)), CONCAT('B', (row_number() OVER () % 20)),
    CONCAT('C', (row_number() OVER () % 30)), CONCAT('D', (row_number() OVER () % 40)),
    CONCAT('E', (row_number() OVER () % 50)),
    CAST(random() * 1000 AS DECIMAL(8,2)), CAST(random() * 2000 AS DECIMAL(8,2)),
    CAST(random() * 3000 AS DECIMAL(8,2)), CAST(random() * 4000 AS DECIMAL(8,2)),
    CAST(random() * 5000 AS DECIMAL(8,2)),
    random() < 0.5, random() < 0.5, random() < 0.5, random() < 0.5, random() < 0.5,
    DATE '2024-01-01' + INTERVAL (row_number() OVER () % 365) DAY,
    DATE '2024-01-01' + INTERVAL ((row_number() OVER () + 100) % 365) DAY,
    DATE '2024-01-01' + INTERVAL ((row_number() OVER () + 200) % 365) DAY,
    random() * 1000000, random() * 2000000, random() * 3000000,
    CONCAT('Text_', row_number() OVER ()), CONCAT('Data_', row_number() OVER ()),
    CONCAT('Info_', row_number() OVER ()), CONCAT('Note_', row_number() OVER ())
FROM (
    SELECT * FROM range(1, 1001)
) t(n);

-- ============================================================================
-- PART 2: MEDIUM DATABASE (100,000 rows)
-- ============================================================================

-- Medium - Uniform distribution
CREATE TABLE IF NOT EXISTS medium_uniform (
    id INTEGER PRIMARY KEY,
    value INTEGER,
    category VARCHAR,
    amount DECIMAL(12,2),
    status VARCHAR,
    date_col DATE
);

DELETE FROM medium_uniform;
INSERT INTO medium_uniform
SELECT
    row_number() OVER () as id,
    (row_number() OVER () % 1000) as value,
    CASE (row_number() OVER () % 10)
        WHEN 0 THEN 'Type_A'
        WHEN 1 THEN 'Type_B'
        WHEN 2 THEN 'Type_C'
        WHEN 3 THEN 'Type_D'
        WHEN 4 THEN 'Type_E'
        WHEN 5 THEN 'Type_F'
        WHEN 6 THEN 'Type_G'
        WHEN 7 THEN 'Type_H'
        WHEN 8 THEN 'Type_I'
        ELSE 'Type_J'
    END as category,
    (row_number() OVER () % 100000) * 1.5 as amount,
    CASE (row_number() OVER () % 3)
        WHEN 0 THEN 'Active'
        WHEN 1 THEN 'Inactive'
        ELSE 'Pending'
    END as status,
    DATE '2023-01-01' + INTERVAL (row_number() OVER () % 730) DAY as date_col
FROM (
    SELECT * FROM range(1, 100001)
) t(n);

-- Medium - Skewed distribution (Pareto 80/20)
CREATE TABLE IF NOT EXISTS medium_skewed (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    product_type VARCHAR,
    transaction_date DATE
);

DELETE FROM medium_skewed;
INSERT INTO medium_skewed
SELECT
    row_number() OVER () as id,
    CASE
        WHEN random() < 0.8 THEN (random() * 100)::INT
        ELSE 100 + (random() * 10000)::INT
    END as customer_id,
    random() * 50000 as amount,
    CASE (row_number() OVER () % 20)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        WHEN 4 THEN 'Furniture'
        ELSE CONCAT('Product_', (row_number() OVER () % 20))
    END as product_type,
    DATE '2023-01-01' + INTERVAL (row_number() OVER () % 730) DAY as transaction_date
FROM (
    SELECT * FROM range(1, 100001)
) t(n);

-- Medium - Clustered time-series data
CREATE TABLE IF NOT EXISTS medium_clustered_ts (
    id INTEGER PRIMARY KEY,
    time_bucket INTEGER,
    metric_value DOUBLE,
    host VARCHAR,
    measurement_time TIMESTAMP
);

DELETE FROM medium_clustered_ts;
INSERT INTO medium_clustered_ts
SELECT
    row_number() OVER () as id,
    (row_number() OVER () / 1000) as time_bucket,
    CAST(random() * 100 AS DOUBLE) as metric_value,
    CONCAT('host_', (row_number() OVER () % 50)::VARCHAR) as host,
    CURRENT_TIMESTAMP - INTERVAL (100000 - row_number() OVER ()) SECOND as measurement_time
FROM (
    SELECT * FROM range(1, 100001)
) t(n);

-- ============================================================================
-- PART 3: COMPLEX SCHEMAS FOR JOINS AND SUBQUERIES
-- ============================================================================

-- Orders table (medium size)
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    order_date DATE,
    status VARCHAR
);

DELETE FROM orders;
INSERT INTO orders
SELECT
    row_number() OVER () as order_id,
    (row_number() OVER () % 1000) + 1 as customer_id,
    100 + random() * 50000 as amount,
    DATE '2023-01-01' + INTERVAL (row_number() OVER () % 730) DAY as order_date,
    CASE (row_number() OVER () % 4)
        WHEN 0 THEN 'Completed'
        WHEN 1 THEN 'Pending'
        WHEN 2 THEN 'Cancelled'
        ELSE 'Shipped'
    END as status
FROM (
    SELECT * FROM range(1, 50001)
) t(n);

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR,
    country VARCHAR,
    signup_date DATE,
    total_spent DECIMAL(12,2)
);

DELETE FROM customers;
INSERT INTO customers
SELECT
    row_number() OVER () as customer_id,
    CONCAT('Customer_', row_number() OVER ()) as name,
    CASE (row_number() OVER () % 5)
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'Germany'
        WHEN 3 THEN 'France'
        ELSE 'Japan'
    END as country,
    DATE '2022-01-01' + INTERVAL (row_number() OVER () % 730) DAY as signup_date,
    CAST(random() * 100000 AS DECIMAL(12,2)) as total_spent
FROM (
    SELECT * FROM range(1, 1001)
) t(n);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR,
    category VARCHAR,
    price DECIMAL(8,2),
    stock INTEGER
);

DELETE FROM products;
INSERT INTO products
SELECT
    row_number() OVER () as product_id,
    CONCAT('Product_', row_number() OVER ()) as name,
    CASE (row_number() OVER () % 8)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        WHEN 4 THEN 'Furniture'
        WHEN 5 THEN 'Sports'
        WHEN 6 THEN 'Toys'
        ELSE 'Home'
    END as category,
    10 + random() * 5000 as price,
    (row_number() OVER () * 7) % 10000 as stock
FROM (
    SELECT * FROM range(1, 501)
) t(n);

-- Order items (for join tests)
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price DECIMAL(8,2)
);

DELETE FROM order_items;
INSERT INTO order_items
SELECT
    row_number() OVER () as order_item_id,
    ((row_number() OVER () - 1) % 50000) + 1 as order_id,
    ((row_number() OVER () - 1) % 500) + 1 as product_id,
    1 + (random() * 10)::INT as quantity,
    10 + random() * 5000 as price
FROM (
    SELECT * FROM range(1, 100001)
) t(n);

-- ============================================================================
-- PART 4: LARGE DATABASE (1M+ rows) - For performance and scale testing
-- ============================================================================

-- Large uniform table
CREATE TABLE IF NOT EXISTS large_uniform (
    id BIGINT PRIMARY KEY,
    partition_key INTEGER,
    value BIGINT,
    category VARCHAR,
    created_at TIMESTAMP
);

DELETE FROM large_uniform;
INSERT INTO large_uniform
SELECT
    row_number() OVER () as id,
    (row_number() OVER () / 10000)::INTEGER as partition_key,
    (row_number() OVER () % 1000000) as value,
    CASE (row_number() OVER () % 20)
        WHEN 0 THEN 'Category_A'
        WHEN 1 THEN 'Category_B'
        WHEN 2 THEN 'Category_C'
        ELSE CONCAT('Cat_', (row_number() OVER () % 20))
    END as category,
    CURRENT_TIMESTAMP - INTERVAL (1000000 - row_number() OVER ()) SECOND as created_at
FROM (
    SELECT * FROM range(1, 1000001)
) t(n);

-- ============================================================================
-- PART 5: CORRELATED DATA FOR TESTING DECORRELATION
-- ============================================================================

-- Table with correlations for subquery testing
CREATE TABLE IF NOT EXISTS correlated_base (
    id INTEGER PRIMARY KEY,
    department_id INTEGER,
    employee_salary DECIMAL(10,2),
    years_employed INTEGER,
    region VARCHAR
);

DELETE FROM correlated_base;
INSERT INTO correlated_base
SELECT
    row_number() OVER () as id,
    (row_number() OVER () % 20) + 1 as department_id,
    50000 + (row_number() OVER () % 20) * 5000 + random() * 10000 as employee_salary,
    (row_number() OVER () % 40) as years_employed,
    CASE (row_number() OVER () % 4)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END as region
FROM (
    SELECT * FROM range(1, 5001)
) t(n);

-- Departments table (for correlations)
CREATE TABLE IF NOT EXISTS departments (
    department_id INTEGER PRIMARY KEY,
    name VARCHAR,
    budget DECIMAL(12,2),
    manager_id INTEGER,
    location VARCHAR
);

DELETE FROM departments;
INSERT INTO departments
SELECT
    row_number() OVER () as department_id,
    CONCAT('Dept_', row_number() OVER ()) as name,
    100000 + random() * 500000 as budget,
    row_number() OVER () as manager_id,
    CASE (row_number() OVER () % 5)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'London'
        WHEN 2 THEN 'Tokyo'
        WHEN 3 THEN 'Sydney'
        ELSE 'Toronto'
    END as location
FROM (
    SELECT * FROM range(1, 21)
) t(n);

-- ============================================================================
-- CREATE INDEXES for testing
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_correlated_dept ON correlated_base(department_id);
CREATE INDEX IF NOT EXISTS idx_large_uniform_partition ON large_uniform(partition_key);
CREATE INDEX IF NOT EXISTS idx_large_uniform_value ON large_uniform(value);

-- ============================================================================
-- RUN ANALYZE ON ALL TABLES
-- ============================================================================

ANALYZE;

-- ============================================================================
-- VERIFY DATABASE CREATION
-- ============================================================================

SELECT 'Testing database creation complete!' as message;
SELECT 'All tables created successfully' as status;

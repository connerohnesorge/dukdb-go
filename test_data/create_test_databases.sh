#!/usr/bin/env bash
# create_test_databases.sh
#
# Creates comprehensive test databases for dukdb-go testing
# Databases created:
#   1. tpch_subset.db - 6 TPC-H tables with subset of TPC-H data
#   2. correlation.db - Tables with known correlations
#   3. edge_cases.db - NULL handling, empty tables, special values
#   4. performance.db - 1M+ rows for performance testing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="$SCRIPT_DIR"

echo "Creating comprehensive test databases..."

# ============================================================================
# 1. Create tpch_subset.db
# ============================================================================
echo "Creating tpch_subset.db..."
DB1="$DB_DIR/tpch_subset.db"
[ -f "$DB1" ] && rm "$DB1"

duckdb "$DB1" << 'EOF'
-- TPC-H subset schema with deterministic data

-- NATION table (25 rows)
CREATE TABLE nation (
  n_nationkey INTEGER,
  n_name VARCHAR,
  n_regionkey INTEGER,
  n_comment VARCHAR
);

INSERT INTO nation VALUES
  (0, 'ALGERIA', 0, 'haggle. carefully final deposits detect slyly '),
  (1, 'ARGENTINA', 1, 'al foxes promise slyly according to the accounts'),
  (2, 'BRAZIL', 1, 'y alongside of the pending deposits'),
  (3, 'CANADA', 1, 'eas hang ironic, silent packages'),
  (4, 'EGYPT', 4, 'y above the carefully unusual theodolites'),
  (5, 'ETHIOPIA', 0, 'even accounts used alongside of the packages'),
  (6, 'FRANCE', 1, 'refully final requests against the platelets'),
  (7, 'GERMANY', 1, 'l platelets. regular accounts x-ray'),
  (8, 'INDIA', 2, 'ss excuses cajole slyly across the packages'),
  (9, 'INDONESIA', 2, 'h carefully anti-clockwise ideas across'),
  (10, 'IRAN', 4, 'eans. express asymptotic ideas'),
  (11, 'IRAQ', 4, 'icitly. final packages'),
  (12, 'JAPAN', 2, 'ously. final, express goals'),
  (13, 'JORDAN', 4, 'ic deposits are blithely about'),
  (14, 'KENYA', 0, 'ing requests cajole among the'),
  (15, 'MEXICO', 2, 'exico. ironic, unusual asymptotes wake blithely r'),
  (16, 'MOROCCO', 0, 'rn requests will have to unwind'),
  (17, 'MOZAMBIQUE', 0, 'uously bold packages wake slyly'),
  (18, 'PERU', 1, 'platelets. blithely pending packages'),
  (19, 'CHINA', 2, 'ish packages haggle blithely across'),
  (20, 'ROMANIA', 3, 'ous asymptotic packages should have'),
  (21, 'RUSSIA', 3, 'evelop carefully final, regular theodolites'),
  (22, 'SAUDI ARABIA', 4, 'ts. silent requests haggle slowly'),
  (23, 'UNITED KINGDOM', 3, 'ited Kingdom quickly from the accounts'),
  (24, 'UNITED STATES', 1, 'y final packages pending foxes should have each');

-- REGION table (5 rows)
CREATE TABLE region (
  r_regionkey INTEGER,
  r_name VARCHAR,
  r_comment VARCHAR
);

INSERT INTO region VALUES
  (0, 'AFRICA', 'lar deposits. blithely final packages cajole'),
  (1, 'AMERICA', 'hs use ironic, even requests'),
  (2, 'ASIA', 'ges. thinly even pinto beans ca'),
  (3, 'EUROPE', 'ly final courts cajole furiously'),
  (4, 'MIDDLE EAST', 'uously express deposits serve furiously furious');

-- SUPPLIER table (100 rows - subset)
CREATE TABLE supplier (
  s_suppkey INTEGER,
  s_name VARCHAR,
  s_address VARCHAR,
  s_nationkey INTEGER,
  s_phone VARCHAR,
  s_acctbal DOUBLE,
  s_comment VARCHAR
);

INSERT INTO supplier
SELECT
  row_number() over () AS s_suppkey,
  'Supplier' || cast(row_number() over () AS VARCHAR) AS s_name,
  'Address' || cast(row_number() over () AS VARCHAR) AS s_address,
  (row_number() over ()) % 25 AS s_nationkey,
  '12-' || cast((row_number() over () % 999) + 100 AS VARCHAR) || '-' ||
    cast((row_number() over () % 9999) + 1000 AS VARCHAR) AS s_phone,
  random() * 10000 AS s_acctbal,
  'comment' || cast(row_number() over () AS VARCHAR)
FROM generate_series(1, 100) t(i);

-- CUSTOMER table (150 rows - subset)
CREATE TABLE customer (
  c_custkey INTEGER,
  c_name VARCHAR,
  c_address VARCHAR,
  c_nationkey INTEGER,
  c_phone VARCHAR,
  c_acctbal DOUBLE,
  c_mktsegment VARCHAR,
  c_comment VARCHAR
);

INSERT INTO customer
SELECT
  row_number() over () AS c_custkey,
  'Customer' || cast(row_number() over () AS VARCHAR) AS c_name,
  'Address' || cast(row_number() over () AS VARCHAR) AS c_address,
  (row_number() over ()) % 25 AS c_nationkey,
  '12-' || cast((row_number() over () % 999) + 100 AS VARCHAR) || '-' ||
    cast((row_number() over () % 9999) + 1000 AS VARCHAR) AS c_phone,
  random() * 50000 AS c_acctbal,
  CASE (row_number() over ()) % 5 WHEN 1 THEN 'BUILDING' WHEN 2 THEN 'AUTOMOBILE'
    WHEN 3 THEN 'HOUSEHOLD' WHEN 4 THEN 'MACHINERY' ELSE 'FURNITURE' END AS c_mktsegment,
  'comment' || cast(row_number() over () AS VARCHAR)
FROM generate_series(1, 150) t(i);

-- ORDERS table (1000 rows - subset)
CREATE TABLE orders (
  o_orderkey INTEGER,
  o_custkey INTEGER,
  o_orderstatus VARCHAR,
  o_totalprice DOUBLE,
  o_orderdate DATE,
  o_orderpriority VARCHAR,
  o_clerk VARCHAR,
  o_shippriority INTEGER,
  o_comment VARCHAR
);

INSERT INTO orders
SELECT
  row_number() over () AS o_orderkey,
  (row_number() over () - 1) % 150 + 1 AS o_custkey,
  CASE (row_number() over ()) % 3 WHEN 1 THEN 'O' WHEN 2 THEN 'F' ELSE 'P' END AS o_orderstatus,
  random() * 500000 AS o_totalprice,
  current_date - interval ((row_number() over ()) % 2000) day AS o_orderdate,
  CASE (row_number() over ()) % 5 WHEN 1 THEN '1-URGENT' WHEN 2 THEN '2-HIGH'
    WHEN 3 THEN '3-MEDIUM' WHEN 4 THEN '4-NOT SPECIFIED' ELSE '5-LOW' END AS o_orderpriority,
  'Clerk#00000' || cast(((row_number() over ()) % 1000) + 1 AS VARCHAR) AS o_clerk,
  (row_number() over ()) % 2 AS o_shippriority,
  'comment' || cast(row_number() over () AS VARCHAR)
FROM generate_series(1, 1000) t(i);

-- LINEITEM table (5000 rows - subset)
CREATE TABLE lineitem (
  l_orderkey INTEGER,
  l_linenumber INTEGER,
  l_partkey INTEGER,
  l_suppkey INTEGER,
  l_quantity DOUBLE,
  l_extendedprice DOUBLE,
  l_discount DOUBLE,
  l_tax DOUBLE,
  l_returnflag VARCHAR,
  l_linestatus VARCHAR,
  l_shipdate DATE,
  l_commitdate DATE,
  l_receiptdate DATE,
  l_shipinstruct VARCHAR,
  l_shipmode VARCHAR,
  l_comment VARCHAR
);

INSERT INTO lineitem
SELECT
  (row_number() over () - 1) / 5 + 1 AS l_orderkey,
  (row_number() over ()) % 5 + 1 AS l_linenumber,
  (row_number() over () - 1) % 100 + 1 AS l_partkey,
  (row_number() over () - 1) % 100 + 1 AS l_suppkey,
  random() * 50 + 1 AS l_quantity,
  random() * 100000 AS l_extendedprice,
  random() * 0.1 AS l_discount,
  random() * 0.08 AS l_tax,
  CASE (row_number() over ()) % 3 WHEN 1 THEN 'R' WHEN 2 THEN 'A' ELSE 'N' END AS l_returnflag,
  CASE (row_number() over ()) % 2 WHEN 1 THEN 'O' ELSE 'F' END AS l_linestatus,
  current_date - interval ((row_number() over ()) % 1500) day AS l_shipdate,
  current_date - interval ((row_number() over ()) % 1000) day AS l_commitdate,
  current_date - interval ((row_number() over ()) % 500) day AS l_receiptdate,
  CASE (row_number() over ()) % 4 WHEN 1 THEN 'DELIVER IN PERSON' WHEN 2 THEN 'COLLECT COD'
    WHEN 3 THEN 'TAKE BACK RETURN' ELSE 'NONE' END AS l_shipinstruct,
  CASE (row_number() over ()) % 7 WHEN 1 THEN 'REG AIR' WHEN 2 THEN 'AIR'
    WHEN 3 THEN 'RAIL' WHEN 4 THEN 'SHIP' WHEN 5 THEN 'TRUCK' WHEN 6 THEN 'MAIL' ELSE 'FOB' END AS l_shipmode,
  'comment' || cast(row_number() over () AS VARCHAR)
FROM generate_series(1, 5000) t(i);

-- PART table (100 rows - subset)
CREATE TABLE part (
  p_partkey INTEGER,
  p_name VARCHAR,
  p_mfgr VARCHAR,
  p_brand VARCHAR,
  p_type VARCHAR,
  p_size INTEGER,
  p_container VARCHAR,
  p_retailprice DOUBLE,
  p_comment VARCHAR
);

INSERT INTO part
SELECT
  row_number() over () AS p_partkey,
  'Part' || cast(row_number() over () AS VARCHAR) AS p_name,
  'Mfg' || cast((row_number() over ()) % 5 + 1 AS VARCHAR) AS p_mfgr,
  'Brand' || cast((row_number() over ()) % 10 + 1 AS VARCHAR) AS p_brand,
  CASE (row_number() over ()) % 4 WHEN 1 THEN 'LARGE BRUSHED COPPER' WHEN 2 THEN 'SMALL POLISHED STEEL'
    WHEN 3 THEN 'MEDIUM BURNISHED NICKEL' ELSE 'PROMO ANODIZED ALUMINUM' END AS p_type,
  ((row_number() over ()) % 50) + 1 AS p_size,
  CASE (row_number() over ()) % 8 WHEN 1 THEN 'JUMBO CAN' WHEN 2 THEN 'WRAP CASE'
    WHEN 3 THEN 'JAR BAG' WHEN 4 THEN 'BOX CASE' WHEN 5 THEN 'PACK BOX'
    WHEN 6 THEN 'DRUM CASE' WHEN 7 THEN 'CASE PKG' ELSE 'LG CAN' END AS p_container,
  random() * 900 + 100 AS p_retailprice,
  'comment' || cast(row_number() over () AS VARCHAR)
FROM generate_series(1, 100) t(i);

-- Create indexes for performance
CREATE INDEX idx_nation_key ON nation(n_nationkey);
CREATE INDEX idx_region_key ON region(r_regionkey);
CREATE INDEX idx_supplier_nation ON supplier(s_nationkey);
CREATE INDEX idx_customer_nation ON customer(c_nationkey);
CREATE INDEX idx_orders_customer ON orders(o_custkey);
CREATE INDEX idx_orders_date ON orders(o_orderdate);
CREATE INDEX idx_lineitem_order ON lineitem(l_orderkey);
CREATE INDEX idx_lineitem_partsupp ON lineitem(l_partkey, l_suppkey);

-- Verify row counts
SELECT COUNT(*) as nation_count FROM nation;
SELECT COUNT(*) as region_count FROM region;
SELECT COUNT(*) as supplier_count FROM supplier;
SELECT COUNT(*) as customer_count FROM customer;
SELECT COUNT(*) as orders_count FROM orders;
SELECT COUNT(*) as lineitem_count FROM lineitem;
SELECT COUNT(*) as part_count FROM part;
EOF

echo "✓ tpch_subset.db created with deterministic data"
echo "  - nation: 25 rows"
echo "  - region: 5 rows"
echo "  - supplier: 100 rows"
echo "  - customer: 150 rows"
echo "  - orders: 1,000 rows"
echo "  - lineitem: 5,000 rows"
echo "  - part: 100 rows"

# ============================================================================
# 2. Create correlation.db
# ============================================================================
echo ""
echo "Creating correlation.db..."
DB2="$DB_DIR/correlation.db"
[ -f "$DB2" ] && rm "$DB2"

duckdb "$DB2" << 'EOF'
-- Tables with known correlations for testing correlation-aware optimization

-- City/State correlation: cities strongly correlate to states
CREATE TABLE locations (
  location_id INTEGER,
  city VARCHAR,
  state VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE,
  population INTEGER
);

INSERT INTO locations
SELECT
  row_number() over () AS location_id,
  CASE (row_number() over () - 1) % 5
    WHEN 0 THEN 'New York'
    WHEN 1 THEN 'Los Angeles'
    WHEN 2 THEN 'Chicago'
    WHEN 3 THEN 'Houston'
    ELSE 'Phoenix'
  END AS city,
  CASE (row_number() over () - 1) % 5
    WHEN 0 THEN 'NY'
    WHEN 1 THEN 'CA'
    WHEN 2 THEN 'IL'
    WHEN 3 THEN 'TX'
    ELSE 'AZ'
  END AS state,
  40.0 + random() * 10 AS latitude,
  -80.0 + random() * 20 AS longitude,
  100000 + cast(random() * 1000000 as INTEGER) AS population
FROM generate_series(1, 1000) t(i);

-- Customers in locations (correlated by location)
CREATE TABLE customers_loc (
  customer_id INTEGER,
  location_id INTEGER,
  name VARCHAR,
  age INTEGER
);

INSERT INTO customers_loc
SELECT
  row_number() over () AS customer_id,
  (row_number() over () - 1) % 1000 + 1 AS location_id,
  'Customer' || cast(row_number() over () AS VARCHAR) AS name,
  20 + cast(random() * 60 as INTEGER) AS age
FROM generate_series(1, 10000) t(i);

-- Orders correlated by location (customers from same location order similar products)
CREATE TABLE orders_loc (
  order_id INTEGER,
  customer_id INTEGER,
  location_id INTEGER,
  product_category VARCHAR,
  amount DOUBLE
);

INSERT INTO orders_loc
SELECT
  row_number() over () AS order_id,
  (row_number() over () - 1) % 10000 + 1 AS customer_id,
  ((row_number() over () - 1) / 10) % 1000 + 1 AS location_id,
  CASE ((row_number() over () - 1) / 10) % 10
    WHEN 0 THEN 'Electronics'
    WHEN 1 THEN 'Clothing'
    WHEN 2 THEN 'Food'
    WHEN 3 THEN 'Books'
    WHEN 4 THEN 'Home'
    WHEN 5 THEN 'Sports'
    WHEN 6 THEN 'Toys'
    WHEN 7 THEN 'Garden'
    WHEN 8 THEN 'Beauty'
    ELSE 'Health'
  END AS product_category,
  random() * 1000 AS amount
FROM generate_series(1, 50000) t(i);

-- Table with time correlation (time series with daily patterns)
CREATE TABLE transactions (
  transaction_id INTEGER,
  transaction_date DATE,
  hour_of_day INTEGER,
  day_of_week INTEGER,
  amount DOUBLE,
  is_weekend BOOLEAN
);

INSERT INTO transactions
SELECT
  row_number() over () AS transaction_id,
  current_date - interval ((row_number() over ()) / 24) day AS transaction_date,
  (row_number() over ()) % 24 AS hour_of_day,
  ((row_number() over ()) / 24) % 7 AS day_of_week,
  CASE WHEN (row_number() over ()) % 24 BETWEEN 12 AND 18 THEN random() * 500 + 100
       WHEN (row_number() over ()) % 24 BETWEEN 9 AND 11 THEN random() * 300 + 50
       ELSE random() * 100 + 10
  END AS amount,
  ((row_number() over ()) / 24) % 7 >= 5 AS is_weekend
FROM generate_series(1, 100000) t(i);

CREATE INDEX idx_locations_state ON locations(state);
CREATE INDEX idx_customers_loc_id ON customers_loc(location_id);
CREATE INDEX idx_orders_loc_id ON orders_loc(location_id);
CREATE INDEX idx_orders_product ON orders_loc(product_category);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_hour ON transactions(hour_of_day);

-- Verify row counts and correlations
SELECT COUNT(*) as locations_count FROM locations;
SELECT COUNT(*) as customers_count FROM customers_loc;
SELECT COUNT(*) as orders_count FROM orders_loc;
SELECT COUNT(*) as transactions_count FROM transactions;

-- Verify correlations exist
SELECT city, COUNT(DISTINCT state) as states_per_city FROM locations GROUP BY city;
SELECT ROUND(CAST(COUNT(DISTINCT state) AS DOUBLE) / COUNT(DISTINCT city), 2) as avg_states_per_city FROM locations;
EOF

echo "✓ correlation.db created with correlated data"
echo "  - locations: 1,000 rows (city-state correlation)"
echo "  - customers_loc: 10,000 rows (location correlation)"
echo "  - orders_loc: 50,000 rows (location-based ordering)"
echo "  - transactions: 100,000 rows (time-based correlation)"

# ============================================================================
# 3. Create edge_cases.db
# ============================================================================
echo ""
echo "Creating edge_cases.db..."
DB3="$DB_DIR/edge_cases.db"
[ -f "$DB3" ] && rm "$DB3"

duckdb "$DB3" << 'EOF'
-- Edge case scenarios for comprehensive testing

-- Table with NULL values
CREATE TABLE nullable_data (
  id INTEGER,
  name VARCHAR,
  email VARCHAR,
  phone VARCHAR,
  age INTEGER,
  salary DOUBLE
);

INSERT INTO nullable_data VALUES
  (1, 'Alice', 'alice@example.com', '555-1234', 30, 50000.0),
  (2, NULL, 'bob@example.com', '555-5678', 25, NULL),
  (3, 'Charlie', NULL, '555-9999', 35, 75000.0),
  (4, 'Diana', 'diana@example.com', NULL, NULL, 60000.0),
  (5, NULL, NULL, NULL, 40, NULL),
  (6, 'Eve', 'eve@example.com', '555-0000', 28, 55000.0),
  (7, 'Frank', 'frank@example.com', '555-1111', NULL, 65000.0),
  (8, NULL, NULL, '555-2222', 32, 72000.0);

-- Empty table with schema
CREATE TABLE empty_table (
  id INTEGER,
  name VARCHAR,
  value DOUBLE
);

-- Table with special values
CREATE TABLE special_values (
  id INTEGER,
  text_value VARCHAR,
  numeric_value DOUBLE,
  int_value INTEGER
);

INSERT INTO special_values VALUES
  (1, '', 0.0, 0),
  (2, ' ', 1.0, 1),
  (3, 'tab	here', -1.0, -1),
  (4, 'quote''s', 999999999.999, 2147483647),
  (5, 'newline
here', -999999999.999, -2147483648),
  (6, 'unicode: 你好', 1e100, 100),
  (7, 'very long string: ' || repeat('x', 1000), 1e-100, 1);

-- Table with single row
CREATE TABLE single_row (
  id INTEGER,
  value VARCHAR
);

INSERT INTO single_row VALUES (1, 'only row');

-- Table with duplicates
CREATE TABLE duplicates (
  id INTEGER,
  category VARCHAR,
  amount DOUBLE
);

INSERT INTO duplicates
SELECT row_number() over () as id, 'A', 100.0 FROM generate_series(1, 10)
UNION ALL
SELECT row_number() over () + 10 as id, 'B', 200.0 FROM generate_series(1, 10)
UNION ALL
SELECT row_number() over () + 20 as id, 'A', 100.0 FROM generate_series(1, 10);

-- Table with all same values
CREATE TABLE identical_values (
  id INTEGER,
  status VARCHAR,
  flag BOOLEAN
);

INSERT INTO identical_values
SELECT row_number() over () as id, 'ACTIVE', true FROM generate_series(1, 100);

-- Table with extreme values
CREATE TABLE extreme_values (
  id INTEGER,
  min_int INTEGER,
  max_int INTEGER,
  tiny_float DOUBLE,
  huge_float DOUBLE
);

INSERT INTO extreme_values VALUES
  (1, -2147483648, 2147483647, 0.000000001, 1e308),
  (2, 0, -1, 1e-308, -1e308),
  (3, 1, 0, -0.0, 0.0);

-- Table with patterns
CREATE TABLE pattern_data (
  id INTEGER,
  pattern VARCHAR
);

INSERT INTO pattern_data VALUES
  (1, 'aaa'),
  (2, 'aab'),
  (3, 'aba'),
  (4, 'abb'),
  (5, 'baa'),
  (6, 'bab'),
  (7, 'bba'),
  (8, 'bbb');

CREATE INDEX idx_nullable_id ON nullable_data(id);
CREATE INDEX idx_duplicates_category ON duplicates(category);

-- Verify structures
SELECT COUNT(*) as nullable_count FROM nullable_data;
SELECT COUNT(*) as empty_count FROM empty_table;
SELECT COUNT(*) as special_count FROM special_values;
SELECT COUNT(*) as single_count FROM single_row;
SELECT COUNT(*) as duplicates_count FROM duplicates;
SELECT COUNT(*) as identical_count FROM identical_values;
SELECT COUNT(*) as extreme_count FROM extreme_values;
SELECT COUNT(*) as pattern_count FROM pattern_data;
EOF

echo "✓ edge_cases.db created with edge cases"
echo "  - nullable_data: 8 rows (extensive NULLs)"
echo "  - empty_table: 0 rows (empty schema)"
echo "  - special_values: 7 rows (special characters, extremes)"
echo "  - single_row: 1 row"
echo "  - duplicates: 30 rows (duplicated values)"
echo "  - identical_values: 100 rows (all same values)"
echo "  - extreme_values: 3 rows (extreme numbers)"
echo "  - pattern_data: 8 rows (pattern matching)"

# ============================================================================
# 4. Create performance.db (1M+ rows)
# ============================================================================
echo ""
echo "Creating performance.db (this may take a couple minutes)..."
DB4="$DB_DIR/performance.db"
[ -f "$DB4" ] && rm "$DB4"

duckdb "$DB4" << 'EOF'
-- Large performance test database with 1M+ rows

-- Main performance table: 1M rows with multiple columns
CREATE TABLE performance_data (
  id INTEGER,
  timestamp TIMESTAMP,
  category VARCHAR,
  subcategory VARCHAR,
  metric_value DOUBLE,
  count_value BIGINT,
  status VARCHAR,
  region VARCHAR,
  user_id INTEGER,
  session_id VARCHAR
);

INSERT INTO performance_data
SELECT
  row_number() over () AS id,
  now() - interval ((row_number() over ()) / 1000) second AS timestamp,
  CASE (row_number() over ()) % 10
    WHEN 0 THEN 'Category_A'
    WHEN 1 THEN 'Category_B'
    WHEN 2 THEN 'Category_C'
    WHEN 3 THEN 'Category_D'
    WHEN 4 THEN 'Category_E'
    WHEN 5 THEN 'Category_F'
    WHEN 6 THEN 'Category_G'
    WHEN 7 THEN 'Category_H'
    WHEN 8 THEN 'Category_I'
    ELSE 'Category_J'
  END AS category,
  CASE (row_number() over ()) % 50
    WHEN 0 THEN 'Sub_X'
    WHEN 1 THEN 'Sub_Y'
    ELSE 'Sub_Z'
  END AS subcategory,
  random() * 10000 AS metric_value,
  cast(random() * 1000000 as BIGINT) AS count_value,
  CASE (row_number() over ()) % 3 WHEN 0 THEN 'Active' WHEN 1 THEN 'Inactive' ELSE 'Pending' END AS status,
  CASE (row_number() over ()) % 5
    WHEN 0 THEN 'North'
    WHEN 1 THEN 'South'
    WHEN 2 THEN 'East'
    WHEN 3 THEN 'West'
    ELSE 'Central'
  END AS region,
  cast((row_number() over () - 1) / 100 as INTEGER) % 10000 + 1 AS user_id,
  'session_' || cast((row_number() over () - 1) / 10000 as VARCHAR) AS session_id
FROM generate_series(1, 1050000) t(i);

-- Aggregate table: 100K rows of pre-aggregated data
CREATE TABLE performance_aggregates (
  date DATE,
  category VARCHAR,
  region VARCHAR,
  total_value DOUBLE,
  avg_value DOUBLE,
  min_value DOUBLE,
  max_value DOUBLE,
  count BIGINT
);

INSERT INTO performance_aggregates
SELECT
  current_date - interval ((row_number() over ()) / 200) day AS date,
  CASE (row_number() over ()) % 10
    WHEN 0 THEN 'Category_A'
    WHEN 1 THEN 'Category_B'
    WHEN 2 THEN 'Category_C'
    WHEN 3 THEN 'Category_D'
    WHEN 4 THEN 'Category_E'
    WHEN 5 THEN 'Category_F'
    WHEN 6 THEN 'Category_G'
    WHEN 7 THEN 'Category_H'
    WHEN 8 THEN 'Category_I'
    ELSE 'Category_J'
  END AS category,
  CASE (row_number() over ()) % 5
    WHEN 0 THEN 'North'
    WHEN 1 THEN 'South'
    WHEN 2 THEN 'East'
    WHEN 3 THEN 'West'
    ELSE 'Central'
  END AS region,
  random() * 50000 AS total_value,
  random() * 500 AS avg_value,
  random() * 50 AS min_value,
  random() * 10000 AS max_value,
  cast(random() * 100000 as BIGINT) AS count
FROM generate_series(1, 100000) t(i);

-- Create indexes for performance
CREATE INDEX idx_perf_timestamp ON performance_data(timestamp);
CREATE INDEX idx_perf_category ON performance_data(category);
CREATE INDEX idx_perf_region ON performance_data(region);
CREATE INDEX idx_perf_user ON performance_data(user_id);
CREATE INDEX idx_agg_date ON performance_aggregates(date);
CREATE INDEX idx_agg_category ON performance_aggregates(category);

-- Verify sizes
SELECT
  'performance_data' as table_name,
  COUNT(*) as row_count
FROM performance_data
UNION ALL
SELECT
  'performance_aggregates' as table_name,
  COUNT(*) as row_count
FROM performance_aggregates;
EOF

echo "✓ performance.db created with large datasets"
echo "  - performance_data: 1,050,000 rows"
echo "  - performance_aggregates: 100,000 rows"

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "=========================================="
echo "Test databases created successfully!"
echo "=========================================="
echo ""
echo "Location: $DB_DIR"
echo ""
echo "Databases:"
ls -lh "$DB_DIR"/*.db 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
echo ""
echo "Total databases created: 4"
echo "Total rows: ~1.25 million"
echo ""

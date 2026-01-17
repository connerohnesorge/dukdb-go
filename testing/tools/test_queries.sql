-- Comprehensive Test Queries for Cost-Based Optimizer Validation
-- These queries test all major optimizer features:
-- - Correctness (same results as DuckDB)
-- - EXPLAIN structure (same plan structure as DuckDB)
-- - Cardinality estimation (estimates within 2x of actual)
-- - Performance (execution time comparable to DuckDB)

-- ============================================================================
-- SECTION 1: BASIC SELECT QUERIES
-- ============================================================================

-- Basic scan
SELECT * FROM small_uniform LIMIT 10;

-- Select with filter
SELECT * FROM small_uniform WHERE value > 50;

-- Select with multiple filters
SELECT * FROM small_uniform WHERE value > 50 AND category = 'A';

-- Select with ORDER BY
SELECT * FROM small_uniform ORDER BY value DESC LIMIT 20;

-- Select with aggregate
SELECT COUNT(*) FROM small_uniform;

-- Select with GROUP BY
SELECT category, COUNT(*) as count FROM small_uniform GROUP BY category;

-- Select with GROUP BY and HAVING
SELECT category, COUNT(*) as count FROM small_uniform GROUP BY category HAVING count > 100;

-- Select with DISTINCT
SELECT DISTINCT category FROM small_uniform;

-- Select with aggregate functions
SELECT
    category,
    COUNT(*) as cnt,
    SUM(price) as total,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM small_uniform
GROUP BY category;

-- ============================================================================
-- SECTION 2: JOIN QUERIES
-- ============================================================================

-- Inner join (basic)
SELECT o.order_id, c.name, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
LIMIT 20;

-- Inner join with filter
SELECT o.order_id, c.name, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 5000;

-- Left outer join
SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;

-- Multi-table join
SELECT o.order_id, oi.product_id, p.name, oi.quantity, oi.price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
LIMIT 20;

-- Multi-table join with filter
SELECT o.order_id, oi.product_id, p.name, oi.quantity, oi.price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE p.category = 'Electronics'
AND oi.quantity > 2;

-- Join with aggregate
SELECT
    c.country,
    COUNT(DISTINCT o.order_id) as order_count,
    SUM(o.amount) as total_amount,
    AVG(o.amount) as avg_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.country;

-- Self join
SELECT s1.id, s2.id, s1.value
FROM small_uniform s1
JOIN small_uniform s2 ON s1.category = s2.category
WHERE s1.id < s2.id
LIMIT 20;

-- ============================================================================
-- SECTION 3: SUBQUERY TESTS
-- ============================================================================

-- Scalar subquery (non-correlated)
SELECT
    id,
    value,
    (SELECT COUNT(*) FROM small_uniform) as total_rows
FROM small_uniform
LIMIT 10;

-- Scalar subquery (correlated)
SELECT
    s1.id,
    s1.value,
    (SELECT MAX(price) FROM small_uniform s2 WHERE s2.category = s1.category) as max_price_in_category
FROM small_uniform s1
LIMIT 10;

-- EXISTS subquery
SELECT customer_id, name
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- NOT EXISTS subquery
SELECT customer_id, name
FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- IN subquery (non-correlated)
SELECT order_id, amount
FROM orders
WHERE customer_id IN (SELECT customer_id FROM customers WHERE country = 'US');

-- IN subquery with aggregate
SELECT order_id, amount
FROM orders
WHERE customer_id IN (
    SELECT customer_id FROM customers
    WHERE total_spent > (SELECT AVG(total_spent) FROM customers)
);

-- NOT IN subquery
SELECT customer_id
FROM customers
WHERE customer_id NOT IN (SELECT DISTINCT customer_id FROM orders);

-- ANY subquery
SELECT order_id, amount
FROM orders o
WHERE amount > ANY (SELECT AVG(amount) FROM orders WHERE status = 'Completed');

-- ALL subquery
SELECT order_id, amount
FROM orders o
WHERE amount >= ALL (SELECT MIN(amount) FROM orders WHERE status = 'Completed');

-- Subquery in FROM clause (derived table)
SELECT customer_summary.country, customer_summary.avg_spent
FROM (
    SELECT country, AVG(total_spent) as avg_spent
    FROM customers
    GROUP BY country
) as customer_summary
WHERE avg_spent > 50000;

-- Nested subqueries (multi-level correlation)
SELECT id, value, category
FROM small_uniform s1
WHERE value > (
    SELECT AVG(value)
    FROM small_uniform s2
    WHERE s2.category = s1.category
)
AND price > (
    SELECT MIN(price)
    FROM small_uniform s3
    WHERE s3.category = s1.category
);

-- ============================================================================
-- SECTION 4: FILTER PUSHDOWN TESTS
-- ============================================================================

-- Simple filter (should push to scan)
SELECT * FROM orders WHERE amount > 10000;

-- Filter with AND (should push both conditions)
SELECT * FROM orders WHERE amount > 10000 AND customer_id > 100;

-- Filter with OR (complex pushdown)
SELECT * FROM orders WHERE amount > 50000 OR status = 'Completed';

-- Filter with function call
SELECT * FROM orders WHERE UPPER(status) = 'COMPLETED';

-- Filter past join
SELECT o.order_id, o.amount, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 5000
AND c.country = 'US';

-- Filter with subquery in WHERE
SELECT * FROM orders
WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE country = 'US'
)
AND amount > 5000;

-- Outer join filter preservation
SELECT c.customer_id, o.amount
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.amount > 5000 OR o.amount IS NULL;

-- ============================================================================
-- SECTION 5: MULTI-COLUMN STATISTICS TESTS
-- ============================================================================

-- Two-column filter (tests joint selectivity)
SELECT * FROM orders WHERE amount > 10000 AND status = 'Completed';

-- Three-column filter
SELECT * FROM small_uniform WHERE value > 50 AND active = true AND category = 'A';

-- Multi-column filter on medium table
SELECT * FROM medium_uniform WHERE value > 500 AND status = 'Active' AND category LIKE 'Type_%';

-- Group by multiple columns
SELECT category, active, COUNT(*) as count
FROM small_uniform
GROUP BY category, active;

-- Join filter with multiple columns
SELECT o.order_id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.amount > 5000
AND o.status = 'Completed'
AND c.country = 'US';

-- ============================================================================
-- SECTION 6: MEDIUM/LARGE TABLE TESTS
-- ============================================================================

-- Medium table full scan
SELECT COUNT(*) FROM medium_uniform;

-- Medium table with filter
SELECT COUNT(*) FROM medium_uniform WHERE value > 500;

-- Medium table GROUP BY
SELECT category, COUNT(*) FROM medium_uniform GROUP BY category;

-- Medium table join
SELECT COUNT(*) FROM medium_uniform m1
JOIN medium_uniform m2 ON m1.category = m2.category;

-- Large table scan (1M rows)
SELECT COUNT(*) FROM large_uniform;

-- Large table with selective filter
SELECT COUNT(*) FROM large_uniform WHERE value < 10000;

-- Large table GROUP BY partition
SELECT partition_key, COUNT(*) as cnt
FROM large_uniform
GROUP BY partition_key
LIMIT 20;

-- ============================================================================
-- SECTION 7: EDGE CASES
-- ============================================================================

-- Empty result set
SELECT * FROM orders WHERE customer_id = 999999;

-- All rows pass filter
SELECT COUNT(*) FROM small_uniform WHERE id > 0;

-- NULL values in filter
SELECT * FROM orders WHERE status IS NULL;

-- Complex AND/OR combinations
SELECT * FROM orders
WHERE (amount > 5000 AND status = 'Completed')
   OR (amount < 100 AND status = 'Cancelled');

-- Filter with CASE expression
SELECT * FROM orders
WHERE CASE
    WHEN status = 'Completed' THEN amount > 5000
    ELSE amount > 1000
END;

-- ============================================================================
-- SECTION 8: CORRELATION TESTS
-- ============================================================================

-- Correlated subquery with department
SELECT
    id,
    department_id,
    employee_salary,
    (SELECT AVG(employee_salary) FROM correlated_base cb2
     WHERE cb2.department_id = cb1.department_id) as dept_avg
FROM correlated_base cb1
LIMIT 20;

-- Correlated EXISTS with departments
SELECT DISTINCT department_id
FROM correlated_base cb
WHERE EXISTS (
    SELECT 1 FROM departments d
    WHERE d.department_id = cb.department_id
    AND d.budget > 200000
);

-- Multi-level correlation
SELECT id, department_id, employee_salary
FROM correlated_base cb1
WHERE employee_salary > (
    SELECT AVG(salary)
    FROM (
        SELECT AVG(employee_salary) as salary
        FROM correlated_base cb2
        WHERE cb2.department_id = cb1.department_id
    ) dept_stats
);

-- ============================================================================
-- SECTION 9: CTE (Common Table Expression) TESTS
-- ============================================================================

-- Simple CTE
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_amount
    FROM orders
    GROUP BY customer_id
)
SELECT c.customer_id, c.name, co.order_count, co.total_amount
FROM customers c
JOIN customer_orders co ON c.customer_id = co.customer_id
ORDER BY co.total_amount DESC
LIMIT 20;

-- CTE with filter
WITH high_value_orders AS (
    SELECT customer_id, COUNT(*) as cnt
    FROM orders
    WHERE amount > 10000
    GROUP BY customer_id
)
SELECT c.customer_id, c.name, hvo.cnt
FROM customers c
JOIN high_value_orders hvo ON c.customer_id = hvo.customer_id;

-- Multiple CTEs
WITH customer_stats AS (
    SELECT customer_id, COUNT(*) as order_count, SUM(amount) as total_spent
    FROM orders
    GROUP BY customer_id
),
high_value_customers AS (
    SELECT customer_id
    FROM customer_stats
    WHERE total_spent > 50000
)
SELECT c.customer_id, c.name, cs.order_count, cs.total_spent
FROM customers c
JOIN customer_stats cs ON c.customer_id = cs.customer_id
WHERE c.customer_id IN (SELECT customer_id FROM high_value_customers);

-- ============================================================================
-- SECTION 10: PERFORMANCE STRESS TESTS
-- ============================================================================

-- Aggregate on large table
SELECT COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value)
FROM large_uniform;

-- Group by on large table
SELECT partition_key, COUNT(*) as cnt, AVG(value) as avg_val
FROM large_uniform
GROUP BY partition_key;

-- Join on large table (smaller result)
SELECT l1.partition_key, COUNT(*) as cnt
FROM large_uniform l1
JOIN large_uniform l2 ON l1.partition_key = l2.partition_key
GROUP BY l1.partition_key
LIMIT 20;

-- Multi-level join (complexity)
SELECT
    o.order_id,
    c.name,
    p.name as product_name,
    oi.quantity
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.amount > 1000
AND oi.quantity > 1
LIMIT 50;

-- ============================================================================
-- EXPLAIN ANALYZE QUERIES (for cardinality estimation)
-- ============================================================================

-- Use EXPLAIN ANALYZE to compare estimates vs actual
EXPLAIN ANALYZE SELECT * FROM small_uniform WHERE value > 50;
EXPLAIN ANALYZE SELECT * FROM medium_uniform WHERE category = 'Type_A';
EXPLAIN ANALYZE SELECT COUNT(*) FROM orders WHERE amount > 5000;
EXPLAIN ANALYZE SELECT c.customer_id, COUNT(o.order_id) FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id;

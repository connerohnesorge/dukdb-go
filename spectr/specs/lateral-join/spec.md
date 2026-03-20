# Lateral Join Specification

## Requirements

### Requirement: Overview

The system MUST implement the following functionality.


This specification defines the implementation requirements for Lateral Joins in dukdb-go to achieve DuckDB v1.4.3 compatibility. Lateral joins enable subqueries in the FROM clause to reference columns from preceding tables, allowing for powerful correlated subqueries and row-by-row computations.


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Syntax

The system MUST implement the following functionality.


#### Basic Lateral Join

```sql
SELECT select_list
FROM table1 t1
JOIN LATERAL (
    SELECT ...
    FROM table2 t2
    WHERE t2.column = t1.column  -- Reference to outer query
) subq ON true;
```

#### Lateral with Different Join Types

```sql
-- INNER JOIN LATERAL
SELECT *
FROM customers c
JOIN LATERAL (
    SELECT * FROM orders o WHERE o.customer_id = c.id
    ORDER BY o.order_date DESC LIMIT 1
) latest_order ON true;

-- LEFT JOIN LATERAL
SELECT *
FROM customers c
LEFT JOIN LATERAL (
    SELECT * FROM orders o WHERE o.customer_id = c.id
    ORDER BY o.order_date DESC LIMIT 1
) latest_order ON true;
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Requirements

The system MUST implement the following functionality.


#### LJ-1: Basic Lateral Join Support

**Priority:** MUST
**Testability:** High

The system MUST support basic lateral joins where the right-hand subquery can reference columns from the left-hand table.

**Example:**
```sql
-- Find the most recent order for each customer
SELECT c.customer_name, lo.order_id, lo.order_date
FROM customers c
JOIN LATERAL (
    SELECT order_id, order_date
    FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
    LIMIT 1
) lo ON true;
```

**Test Cases:**
- [ ] Simple lateral join with equality predicate
- [ ] Lateral join with ORDER BY and LIMIT
- [ ] Lateral join with aggregation
- [ ] Lateral join returning multiple columns

#### LJ-2: Left Lateral Join

**Priority:** MUST
**Testability:** High

The system MUST support LEFT JOIN LATERAL to include rows from the left table even when the lateral subquery returns no rows.

**Example:**
```sql
-- All customers with their most recent order (if any)
SELECT c.customer_name, lo.order_id, lo.order_date
FROM customers c
LEFT JOIN LATERAL (
    SELECT order_id, order_date
    FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
    LIMIT 1
) lo ON true;
```

**Test Cases:**
- [ ] Left lateral join with matching rows
- [ ] Left lateral join with no matching rows (preserve left side)
- [ ] Left lateral join with NULL handling
- [ ] Left lateral join with complex predicates

#### LJ-3: Multiple Lateral Joins

**Priority:** MUST
**Testability:** High

The system MUST support multiple lateral joins in a single query, with later laterals able to reference earlier ones.

**Example:**
```sql
-- Complex analytics with multiple laterals
SELECT
    c.customer_name,
    latest_order.*,
    order_items.total_items,
    order_items.total_value
FROM customers c
JOIN LATERAL (
    SELECT * FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY o.order_date DESC
    LIMIT 1
) latest_order ON true
JOIN LATERAL (
    SELECT
        COUNT(*) as total_items,
        SUM(oi.quantity * oi.unit_price) as total_value
    FROM order_items oi
    WHERE oi.order_id = latest_order.order_id
) order_items ON true;
```

**Test Cases:**
- [ ] Chain of lateral joins
- [ ] Lateral referencing previous lateral
- [ ] Mixed lateral and regular joins
- [ ] Performance with multiple laterals

#### LJ-4: Lateral with Aggregations

**Priority:** MUST
**Testability:** High

The system MUST support lateral joins with aggregate functions.

**Example:**
```sql
-- Customer statistics using lateral join
SELECT
    c.customer_name,
    stats.order_count,
    stats.total_spent,
    stats.avg_order_value,
    stats.first_order_date,
    stats.last_order_date
FROM customers c
JOIN LATERAL (
    SELECT
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent,
        AVG(total_amount) as avg_order_value,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date
    FROM orders
    WHERE customer_id = c.customer_id
) stats ON true;
```

**Test Cases:**
- [ ] Lateral with COUNT, SUM, AVG
- [ ] Lateral with MIN, MAX
- [ ] Lateral with GROUP BY (if supported)
- [ ] Lateral with HAVING clause

#### LJ-5: Lateral Subquery with Set Operations

**Priority:** SHOULD
**Testability:** Medium

The system SHOULD support set operations (UNION, INTERSECT, EXCEPT) within lateral subqueries.

**Example:**
```sql
-- Combine different data sources in lateral
SELECT c.customer_name, combined_data.*
FROM customers c
JOIN LATERAL (
    SELECT 'online' as channel, order_date, total_amount
    FROM online_orders
    WHERE customer_id = c.customer_id
    UNION ALL
    SELECT 'store' as channel, sale_date, total_amount
    FROM store_sales
    WHERE customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 5
) combined_data ON true;
```

#### LJ-6: Correlated Lateral Subqueries

**Priority:** MUST
**Testability:** High

The system MUST support lateral subqueries that are correlated with the outer query through complex conditions.

**Example:**
```sql
-- Find customers whose spending is above their category average
SELECT c.customer_name, c.category, spending_stats.*
FROM customers c
JOIN LATERAL (
    SELECT
        customer_spending.total,
        category_avg.average,
        customer_spending.total - category_avg.average as difference
    FROM
        (SELECT SUM(total_amount) as total
         FROM orders
         WHERE customer_id = c.customer_id) customer_spending,
        (SELECT AVG(total_spending) as average
         FROM (
             SELECT SUM(total_amount) as total_spending
             FROM orders o2
             JOIN customers c2 ON o2.customer_id = c2.customer_id
             WHERE c2.category = c.category
             GROUP BY c2.customer_id
         ) category_totals) category_avg
    WHERE customer_spending.total > category_avg.average
) spending_stats ON true;
```

**Test Cases:**
- [ ] Multiple correlation predicates
- [ ] Complex boolean expressions
- [ ] Subqueries in lateral conditions
- [ ] Nested correlations

#### LJ-7: Performance Requirements

**Priority:** MUST
**Testability:** High

Lateral joins MUST meet the following performance requirements:

1. **Single Row Lateral**: <10ms per correlated execution
2. **Batch Processing**: Process in chunks of 2048 rows
3. **Memory Usage**: <100MB per lateral join operator
4. **Scalability**: Linear with number of outer rows

**Performance Test Cases:**
- [ ] Benchmark with 1K outer rows
- [ ] Benchmark with 10K outer rows
- [ ] Benchmark with complex subqueries
- [ ] Memory usage profiling

#### LJ-8: Lateral with Window Functions

**Priority:** SHOULD
**Testability:** Medium

The system SHOULD support window functions within lateral subqueries.

**Example:**
```sql
-- Ranking within each customer's orders
SELECT c.customer_name, ranked_orders.*
FROM customers c
JOIN LATERAL (
    SELECT
        order_id,
        order_date,
        total_amount,
        RANK() OVER (ORDER BY total_amount DESC) as amount_rank,
        ROW_NUMBER() OVER (ORDER BY order_date) as order_number
    FROM orders
    WHERE customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 3
) ranked_orders ON true;
```

#### LJ-9: Lateral in WHERE Clause

**Priority:** SHOULD NOT (Phase 2)
**Testability:** Low

The system MAY support lateral-derived tables in WHERE clause predicates.

**Example:**
```sql
-- Customers with above-average order frequency
SELECT c.customer_name
FROM customers c
WHERE (
    SELECT COUNT(*)
    FROM LATERAL (
        SELECT order_date
        FROM orders
        WHERE customer_id = c.customer_id
    ) o
) > (
    SELECT AVG(order_count)
    FROM (
        SELECT COUNT(*) as order_count
        FROM orders
        GROUP BY customer_id
    ) averages
);
```

#### LJ-10: Integration with Other Features

**Priority:** MUST
**Testability:** High

Lateral joins MUST integrate seamlessly with other dukdb-go features:

1. **Views**: Support lateral joins in view definitions
2. **CTEs**: Allow CTEs within lateral subqueries
3. **Transactions**: Respect transaction isolation levels
4. **Indexes**: Utilize indexes for lateral join predicates

**Integration Test Cases:**
- [ ] Lateral join in a view
- [ ] Lateral with CTE inside
- [ ] Lateral with recursive CTE
- [ ] Lateral join with index usage


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Implementation Details

The system MUST implement the following functionality.


#### Execution Model

Lateral joins execute the right-hand subquery once for each row from the left input:

```
For each row R in left input:
    1. Bind outer references from R to subquery
    2. Execute subquery with bound values
    3. Join results with R
    4. Output joined rows
```

#### Correlation Handling

```go
type CorrelationSpec struct {
    OuterColumns []string
    InnerColumns []string
    ValueMapping map[string]interface{}
}
```

#### Optimization Strategies

1. **Predicate Pushdown**: Push outer predicates into lateral subquery
2. **Caching**: Cache subquery results for repeated outer values
3. **Batching**: Process multiple outer rows together when possible
4. **Index Usage**: Utilize indexes on correlation predicates

#### Memory Management

- Process in chunks of 2048 rows
- Stream results to avoid materialization
- Bounded memory usage per operator


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Examples

The system MUST implement the following functionality.


#### Top-N per Group

```sql
-- Top 3 products by revenue for each category
SELECT c.category_name, top_products.*
FROM categories c
JOIN LATERAL (
    SELECT
        p.product_name,
        SUM(oi.quantity * oi.unit_price) as revenue
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    WHERE p.category_id = c.category_id
    GROUP BY p.product_id, p.product_name
    ORDER BY revenue DESC
    LIMIT 3
) top_products ON true;
```

#### Time Series Analysis

```sql
-- Compare each order with customer's previous order
SELECT
    c.customer_name,
    o.order_id,
    o.order_date,
    o.total_amount,
    prev_order.order_date as prev_order_date,
    prev_order.total_amount as prev_amount,
    o.total_amount - prev_order.total_amount as amount_change,
    o.order_date - prev_order.order_date as days_since_prev
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN LATERAL (
    SELECT order_date, total_amount
    FROM orders
    WHERE customer_id = c.customer_id
      AND order_date < o.order_date
    ORDER BY order_date DESC
    LIMIT 1
) prev_order ON true;
```

#### Running Totals

```sql
-- Running balance for each account
SELECT
    a.account_id,
    a.account_name,
    t.transaction_id,
    t.transaction_date,
    t.amount,
    balance.running_balance
FROM accounts a
JOIN transactions t ON a.account_id = t.account_id
JOIN LATERAL (
    SELECT SUM(amount) as running_balance
    FROM transactions t2
    WHERE t2.account_id = a.account_id
      AND t2.transaction_date <= t.transaction_date
) balance ON true
ORDER BY a.account_id, t.transaction_date;
```

#### Existential Queries

```sql
-- Products that have been ordered at least once
SELECT p.*
FROM products p
WHERE EXISTS (
    SELECT 1
    FROM LATERAL (
        SELECT 1
        FROM order_items oi
        WHERE oi.product_id = p.product_id
        LIMIT 1
    ) o
);
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Performance Considerations

The system MUST implement the following functionality.


#### Best Practices

1. **Use LIMIT**: Always use LIMIT in lateral subqueries when possible
2. **Index Outer Columns**: Ensure indexes exist on correlation columns
3. **Avoid Correlated Aggregates**: Consider pre-aggregation when possible
4. **Batch Processing**: Process multiple rows when correlation values repeat

#### Anti-Patterns

1. **Uncorrelated Lateral**: Don't use LATERAL when regular subquery works
2. **Missing Indexes**: Ensure proper indexes on correlation predicates
3. **Large Result Sets**: Avoid lateral subqueries returning many rows per outer row
4. **Deep Nesting**: Avoid deeply nested lateral joins


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Migration Guide

The system MUST implement the following functionality.


#### From Correlated Subqueries

```sql
-- Before: Correlated subquery in SELECT
SELECT
    c.customer_name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) as order_count
FROM customers c;

-- After: Lateral join (more efficient)
SELECT c.customer_name, order_stats.order_count
FROM customers c
JOIN LATERAL (
    SELECT COUNT(*) as order_count
    FROM orders o
    WHERE o.customer_id = c.customer_id
) order_stats ON true;
```

#### From Self-Joins

```sql
-- Before: Self-join for previous row
SELECT
    t1.id,
    t1.value,
    t2.value as prev_value
FROM my_table t1
LEFT JOIN my_table t2 ON t2.id = t1.id - 1;

-- After: Lateral join (handles gaps)
SELECT
    t.id,
    t.value,
    prev.value as prev_value
FROM my_table t
LEFT JOIN LATERAL (
    SELECT value
    FROM my_table
    WHERE id < t.id
    ORDER BY id DESC
    LIMIT 1
) prev ON true;
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Test Data

The system MUST implement the following functionality.


#### E-commerce Schema

```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    category VARCHAR(20),
    registration_date DATE
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE order_items (
    item_id INT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price DECIMAL(10,2)
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(200),
    category_id INT,
    price DECIMAL(10,2)
);

CREATE TABLE categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100)
);
```

#### Time Series Data

```sql
CREATE TABLE accounts (
    account_id INT PRIMARY KEY,
    account_name VARCHAR(100),
    account_type VARCHAR(20)
);

CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    account_id INT,
    transaction_date DATE,
    amount DECIMAL(10,2),
    transaction_type VARCHAR(10)
);
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: References

The system MUST implement the following functionality.


1. SQL:2011 Standard - Section 7.6 (<table reference>)
2. PostgreSQL Documentation - https://www.postgresql.org/docs/current/queries-table-expressions.html
3. DuckDB Lateral Joins - https://duckdb.org/docs/sql/query_syntax/from
4. "SQL and Relational Theory" - C.J. Date
5. "SQL Antipatterns" - Bill Karwin


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Appendix

The system MUST implement the following functionality.


#### A. Complete Examples

See `examples/lateral_join/` directory for complete working examples.

#### B. Performance Benchmarks

See `benchmarks/lateral_join/` directory for performance test results.

#### C. Error Message Catalog

See `docs/errors/lateral_join_errors.md` for complete error message documentation.

#### D. Implementation Notes

##### Correlation Detection

The system must detect which columns from the outer query are referenced in the lateral subquery:

```go
func detectCorrelations(subquery *SelectStmt, outerScope *Scope) []Correlation {
    var correlations []Correlation

    // Walk the AST and find column references
    ast.Walk(subquery, func(node ast.Node) {
        if colRef, ok := node.(*ast.ColumnRef); ok {
            if colRef.Table == "" {
                // Check if this column is from outer scope
                if outerScope.HasColumn(colRef.Column) {
                    correlations = append(correlations, Correlation{
                        OuterColumn: colRef.Column,
                        InnerColumn: generateUniqueName(),
                    })
                }
            }
        }
    })

    return correlations
}
```

##### Execution Strategy

The lateral join operator processes rows in batches for efficiency:

```go
func (l *LateralJoinOperator) execute() error {
    // Get batch of rows from left input
    leftChunk, err := l.leftInput.Next()
    if err != nil {
        return err
    }

    // Process each row
    for rowIdx := 0; rowIdx < leftChunk.Count(); rowIdx++ {
        row := leftChunk.GetRow(rowIdx)

        // Create correlated subquery for this row
        correlatedPlan := l.createCorrelatedPlan(row)

        // Execute subquery
        subqueryResult, err := l.executeSubquery(correlatedPlan)
        if err != nil {
            return err
        }

        // Join results
        joinedRows := l.joinWithOuter(row, subqueryResult)

        // Output joined rows
        l.output.Append(joinedRows)
    }

    return nil
}
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined


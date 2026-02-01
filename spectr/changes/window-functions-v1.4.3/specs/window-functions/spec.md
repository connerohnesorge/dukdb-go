# Window Functions Specification

## Overview

This specification defines the complete implementation of SQL window functions for dukdb-go, providing compatibility with DuckDB v1.4.3. Window functions perform calculations across sets of rows that are related to the current row, without collapsing them into a single output row.

## Syntax

```sql
window_function_call ::=
    function_name ([expression [, expression ...]])
    [FILTER (WHERE filter_condition)]
    OVER {window_name | (window_specification)}

window_specification ::=
    [existing_window_name]
    [PARTITION BY expression [, expression ...]]
    [ORDER BY expression [ASC|DESC] [NULLS {FIRST|LAST}] [, ...]]
    [frame_clause]

frame_clause ::=
    {ROWS | RANGE | GROUPS}
    BETWEEN frame_bound AND frame_bound
    [EXCLUDE {CURRENT ROW | GROUP | TIES | NO OTHERS}]

frame_bound ::=
    UNBOUNDED PRECEDING
    | offset PRECEDING
    | CURRENT ROW
    | offset FOLLOWING
    | UNBOUNDED FOLLOWING
```

## Window Function Categories

### 1. Ranking Functions

#### ROW_NUMBER()
Returns the sequential number of the current row within its partition, starting at 1.

```sql
-- Basic usage
SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as salary_rank
FROM employees;

-- With partitioning
SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
FROM employees;
```

**Properties:**
- Always produces unique values within a partition
- Non-deterministic if ORDER BY produces ties
- Returns NULL if the window frame is empty

#### RANK()
Returns the rank of the current row with gaps for ties.

```sql
-- Rank with ties
SELECT
    score,
    RANK() OVER (ORDER BY score DESC) as rank_with_gaps
FROM test_scores;

-- Example output:
-- score | rank_with_gaps
-- 100   | 1
-- 95    | 2
-- 95    | 2
-- 90    | 4
```

**Properties:**
- Rows with equal ORDER BY values get the same rank
- Next rank number skips for ties (1, 2, 2, 4)
- Returns 1 if the window frame has one row

#### DENSE_RANK()
Returns the rank of the current row without gaps for ties.

```sql
-- Dense rank with ties
SELECT
    score,
    DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank
FROM test_scores;

-- Example output:
-- score | dense_rank
-- 100   | 1
-- 95    | 2
-- 95    | 2
-- 90    | 3
```

**Properties:**
- Rows with equal ORDER BY values get the same rank
- Next rank number is consecutive (1, 2, 2, 3)
- Never skips rank numbers

#### PERCENT_RANK()
Returns the relative rank of the current row as a percentage.

```sql
SELECT
    score,
    PERCENT_RANK() OVER (ORDER BY score) as percent_rank
FROM test_scores;

-- Formula: (rank - 1) / (total_rows - 1)
-- Returns 0.0 for the first row
-- Returns 1.0 for the last row
-- Returns NULL if partition has only one row
```

**Properties:**
- Range: 0.0 to 1.0
- Uses RANK() calculation, so ties have the same percentage
- Returns NULL for partitions with one row

#### CUME_DIST()
Returns the cumulative distribution of the current row.

```sql
SELECT
    score,
    CUME_DIST() OVER (ORDER BY score) as cumulative_dist
FROM test_scores;

-- Formula: (number of rows ≤ current row) / total_rows
-- Includes all rows in the same peer group
```

**Properties:**
- Range: 1/n to 1.0 (where n is partition size)
- Always returns 1.0 for the last row
- Accounts for peer groups in calculation

#### NTILE(n)
Divides rows into n buckets and returns the bucket number.

```sql
-- Divide into quartiles
SELECT
    score,
    NTILE(4) OVER (ORDER BY score) as quartile
FROM test_scores;

-- Divide employees into performance tiers
SELECT
    employee_id,
    sales_amount,
    NTILE(3) OVER (ORDER BY sales_amount DESC) as performance_tier
FROM sales_data;
```

**Properties:**
- Buckets are numbered 1 through n
- If rows don't divide evenly, earlier buckets get extra rows
- Returns 1 if n ≤ 0 or partition is empty

### 2. Value Functions

#### LAG(expression [, offset [, default]])
Returns the value from a row that is offset rows before the current row.

```sql
-- Compare current month with previous month
SELECT
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month_revenue,
    revenue - LAG(revenue) OVER (ORDER BY month) as revenue_change
FROM monthly_revenue;

-- With offset and default
SELECT
    day,
    temperature,
    LAG(temperature, 7, 0) OVER (ORDER BY day) as temp_week_ago
FROM daily_weather;

-- With IGNORE NULLS
SELECT
    date,
    price,
    LAG(price, 1, NULL) IGNORE NULLS OVER (ORDER BY date) as last_known_price
FROM stock_prices;
```

**Properties:**
- offset defaults to 1 if not specified
- default is returned if offset goes beyond partition bounds
- Returns NULL if default is not specified and offset is out of bounds
- IGNORE NULLS skips NULL values when counting offset

#### LEAD(expression [, offset [, default]])
Returns the value from a row that is offset rows after the current row.

```sql
-- Compare current month with next month
SELECT
    month,
    revenue,
    LEAD(revenue) OVER (ORDER BY month) as next_month_revenue,
    LEAD(revenue, 3) OVER (ORDER BY month) as revenue_3_months_ahead
FROM monthly_revenue;

-- With IGNORE NULLS
SELECT
    date,
    LEAD(price, 1, NULL) IGNORE NULLS OVER (ORDER BY date) as next_known_price
FROM stock_prices;
```

**Properties:**
- Similar to LAG but looks forward instead of backward
- Same parameter rules as LAG
- Supports IGNORE NULLS

#### FIRST_VALUE(expression) [IGNORE NULLS]
Returns the first value in the window frame.

```sql
-- First value in the entire partition
SELECT
    employee_id,
    department,
    salary,
    FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY employee_id) as dept_starting_salary
FROM employees;

-- First value in a sliding window
SELECT
    date,
    stock_price,
    FIRST_VALUE(stock_price) OVER (
        ORDER BY date
        ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
    ) as month_start_price
FROM stock_data;

-- With IGNORE NULLS
SELECT
    date,
    FIRST_VALUE(temperature) IGNORE NULLS OVER (ORDER BY date) as first_temp_reading
FROM weather_data;
```

**Properties:**
- Returns NULL if frame is empty
- With IGNORE NULLS, skips NULL values
- Respects frame boundaries and EXCLUDE clauses

#### LAST_VALUE(expression) [IGNORE NULLS]
Returns the last value in the window frame.

```sql
-- Last value in default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
SELECT
    employee_id,
    salary,
    LAST_VALUE(salary) OVER (ORDER BY employee_id) as current_salary
FROM employees;

-- Last value in entire partition
SELECT
    employee_id,
    department,
    salary,
    LAST_VALUE(salary) OVER (
        PARTITION BY department
        ORDER BY employee_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as dept_ending_salary
FROM employees;
```

**Properties:**
- Default frame includes rows up to current row
- Returns NULL if frame is empty
- Supports IGNORE NULLS

#### NTH_VALUE(expression, n) [IGNORE NULLS]
Returns the nth value in the window frame (1-indexed).

```sql
-- Second highest score per class
SELECT
    student_id,
    class_id,
    score,
    NTH_VALUE(score, 2) OVER (
        PARTITION BY class_id
        ORDER BY score DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as second_highest_score
FROM test_scores;

-- With IGNORE NULLS
SELECT
    date,
    NTH_VALUE(temperature, 3) IGNORE NULLS OVER (ORDER BY date) as third_temp_reading
FROM weather_data;
```

**Properties:**
- n must be a positive integer
- Returns NULL if n is greater than frame size
- 1-indexed (first value is NTH_VALUE(expr, 1))
- Supports IGNORE NULLS

### 3. Aggregate Window Functions

All standard aggregate functions can be used as window functions:

#### SUM() OVER
```sql
-- Running total
SELECT
    date,
    sales,
    SUM(sales) OVER (ORDER BY date) as running_total
FROM daily_sales;

-- Sum within department
SELECT
    employee_id,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department) as dept_payroll
FROM employees;

-- Sum in sliding window
SELECT
    time,
    measurement,
    SUM(measurement) OVER (
        ORDER BY time
        ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
    ) as moving_sum
FROM sensor_data;
```

#### COUNT() OVER
```sql
-- Count of rows in partition
SELECT
    *,
    COUNT(*) OVER (PARTITION BY category) as category_count
FROM products;

-- Running count (excluding NULLs)
SELECT
    date,
    COUNT(value) OVER (ORDER BY date) as non_null_count
FROM measurements;

-- Count with DISTINCT
SELECT
    customer_id,
    order_date,
    COUNT(DISTINCT product_id) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as unique_products_so_far
FROM orders;
```

#### AVG() OVER
```sql
-- Moving average
SELECT
    date,
    price,
    AVG(price) OVER (
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as weekly_avg_price
FROM stock_prices;

-- Average with FILTER
SELECT
    month,
    AVG(amount) FILTER (WHERE amount > 0) OVER (ORDER BY month) as avg_positive
FROM monthly_data;
```

#### MIN()/MAX() OVER
```sql
-- Running minimum
SELECT
    date,
    temperature,
    MIN(temperature) OVER (ORDER BY date) as coldest_so_far
FROM weather_data;

-- Maximum in sliding window
SELECT
    time,
    cpu_usage,
    MAX(cpu_usage) OVER (
        ORDER BY time
        ROWS BETWEEN 60 PRECEDING AND CURRENT ROW
    ) as max_cpu_last_hour
FROM system_metrics;
```

## Frame Clauses

### Default Frames

When no frame is specified:
- With ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
- Without ORDER BY: ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

### ROWS Frame
Physical row-based boundaries:

```sql
-- Previous row only
SELECT
    value,
    LAG(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) as prev_value
FROM data;

-- Current row and next 2 rows
SELECT
    value,
    SUM(value) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) as sum_next_3
FROM data;
```

### RANGE Frame
Value-based boundaries:

```sql
-- All rows with the same value
SELECT
    score,
    COUNT(*) OVER (ORDER BY score RANGE BETWEEN CURRENT ROW AND CURRENT ROW) as same_score_count
FROM test_scores;

-- Range of +/- 10
SELECT
    temperature,
    AVG(temperature) OVER (ORDER BY temperature RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) as avg_nearby
FROM weather;
```

### GROUPS Frame
Peer group-based boundaries:

```sql
-- Current peer group and next 2 peer groups
SELECT
    score,
    COUNT(*) OVER (ORDER BY score GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING) as count_in_groups
FROM test_scores;
```

### EXCLUDE Clause

Exclude specific rows from the frame:

```sql
-- Exclude current row from sum
SELECT
    value,
    SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) as sum_others
FROM data;

-- Exclude peer group
SELECT
    score,
    AVG(score) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) as avg_other_scores
FROM test_scores;

-- Exclude ties (peer group except current row)
SELECT
    score,
    COUNT(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) as count_non_ties
FROM test_scores;
```

## Named Windows

Define and reuse window specifications:

```sql
-- Define named window
SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER w as row_num,
    RANK() OVER w as rank,
    DENSE_RANK() OVER w as dense_rank
FROM employees
WINDOW w AS (PARTITION BY department ORDER BY salary DESC);

-- Window with inheritance
SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER w as row_num,
    SUM(salary) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM employees
WINDOW w AS (PARTITION BY department ORDER BY salary DESC);

-- Multiple named windows
SELECT
    employee_id,
    department,
    salary,
    hire_date,
    ROW_NUMBER() OVER dept_window as dept_rank,
    ROW_NUMBER() OVER date_window as seniority_rank
FROM employees
WINDOW
    dept_window AS (PARTITION BY department ORDER BY salary DESC),
    date_window AS (ORDER BY hire_date);
```

## FILTER Clause

Apply filters to window functions:

```sql
-- Count only positive values
SELECT
    date,
    COUNT(*) FILTER (WHERE amount > 0) OVER (ORDER BY date) as positive_count
FROM transactions;

-- Sum with multiple conditions
SELECT
    customer_id,
    SUM(amount) FILTER (WHERE status = 'completed' AND amount > 100)
        OVER (PARTITION BY customer_id ORDER BY date) as qualified_sum
FROM orders;
```

## IGNORE NULLS

Skip NULL values in value functions:

```sql
-- Get last non-null value
SELECT
    date,
    LAG(value, 1, NULL) IGNORE NULLS OVER (ORDER BY date) as last_non_null
FROM sparse_data;

-- First non-null value in frame
SELECT
    date,
    FIRST_VALUE(reading) IGNORE NULLS OVER (
        ORDER BY date
        ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) as last_24h_reading
FROM sensor_data;
```

## Implementation Requirements

### 1. Function Resolution
- Function names are case-insensitive
- Validate function signatures (argument count and types)
- Support implicit type conversions where appropriate
- Provide clear error messages for invalid usage

### 2. Partition Processing
- Process partitions independently
- Maintain partition order for deterministic results
- Support parallel partition processing
- Handle empty partitions gracefully

### 3. Frame Computation
- Efficient frame boundary calculation
- Support for all frame types (ROWS, RANGE, GROUPS)
- Proper handling of default frames
- Incremental computation for sliding windows

### 4. NULL Handling
- Ranking functions ignore NULL ORDER BY values
- Value functions return NULL for out-of-bounds access
- Aggregate functions skip NULL values (except COUNT(*))
- IGNORE NULLS affects value function behavior

### 5. Performance Considerations
- Vectorized execution for batch processing
- Memory pooling for allocations
- Incremental aggregation for sliding windows
- Spilling to disk for large partitions

### 6. Error Cases
```sql
-- Invalid function arguments
SELECT NTILE(0) OVER (ORDER BY value);  -- Error: NTILE argument must be positive

-- Invalid frame specification
SELECT SUM(value) OVER (ORDER BY value RANGE BETWEEN 1 FOLLOWING AND 1 PRECEDING);  -- Error: invalid frame

-- Missing ORDER BY for certain functions
SELECT LAG(value) OVER ();  -- Error: LAG requires ORDER BY

-- Invalid window reference
SELECT ROW_NUMBER() OVER nonexistent_window;  -- Error: window not found
```

## Test Scenarios

### 1. Basic Functionality
- Single window function per query
- Multiple window functions per query
- Window functions with different PARTITION BY clauses
- Window functions with different ORDER BY clauses

### 2. Edge Cases
- Empty partitions
- Single row partitions
- All NULL ORDER BY values
- Duplicate ORDER BY values (ties)

### 3. Frame Scenarios
- Default frames (with and without ORDER BY)
- ROWS frames with various bounds
- RANGE frames with numeric and temporal offsets
- GROUPS frames with peer groups
- EXCLUDE clause variations

### 4. Performance Scenarios
- Large partitions (millions of rows)
- Many small partitions
- Deeply nested window functions
- Complex frame specifications

### 5. Integration Scenarios
- Window functions in subqueries
- Window functions with joins
- Window functions with grouping
- Window functions in HAVING and QUALIFY clauses

This specification provides comprehensive coverage of window function behavior, ensuring compatibility with DuckDB v1.4.3 while maintaining good performance and clear error handling."}
## ADDED Requirements

### Requirement: PIVOT Operator Execution

The executor SHALL implement PIVOT operation by transforming into conditional aggregation.

#### Scenario: PIVOT with single aggregate and grouping column
- WHEN executing `PIVOT sales ON quarter USING SUM(amount) GROUP BY product`
- THEN the executor SHALL compute `SUM(CASE WHEN quarter = 'Q1' THEN amount END)` for each quarter
- AND produce output with columns: product, Q1, Q2, Q3, Q4
- AND NULL used for missing combinations

#### Scenario: PIVOT with multiple aggregates
- WHEN executing `PIVOT sales ON quarter USING SUM(amount), COUNT(*) GROUP BY product`
- THEN the executor SHALL compute both SUM and COUNT for each pivot value
- AND produce output with columns: product, Q1_sum, Q1_count, Q2_sum, Q2_count, etc.

### Requirement: UNPIVOT Operator Execution

The executor SHALL implement UNPIVOT operation by transforming rows into columns.

#### Scenario: UNPIVOT with multiple columns
- WHEN executing `UNPIVOT sales INTO val FOR month IN (jan, feb, mar)`
- THEN for each input row, produce three output rows
- AND val column contains the original column value
- AND month column contains the pivot column name (jan, feb, or mar)

#### Scenario: UNPIVOT with data type preservation
- WHEN executing UNPIVOT on columns of various types
- THEN the executor SHALL preserve the original data types
- AND NULL values in source columns become NULL in unpivoted output

### Requirement: GROUPING SETS Execution

The executor SHALL support GROUP BY GROUPING SETS, ROLLUP, and CUBE by expanding into multiple grouping sets.

#### Scenario: GROUPING SETS with two sets
- WHEN executing `SELECT a, b, SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b))`
- THEN the executor SHALL compute aggregates for grouping set (a,) and (b,)
- AND produce output where columns not in grouping set are NULL
- AND maintain correct row ordering

#### Scenario: ROLLUP with three columns
- WHEN executing `SELECT a, b, c, SUM(x) FROM t GROUP BY ROLLUP (a, b, c)`
- THEN the executor SHALL compute aggregates for:
  - (a, b, c)
  - (a, b)
  - (a)
  - ()
- AND set appropriate NULL values for rolled-up columns

#### Scenario: CUBE with two columns
- WHEN executing `SELECT a, b, SUM(x) FROM t GROUP BY CUBE (a, b)`
- THEN the executor SHALL compute aggregates for:
  - (a, b)
  - (a)
  - (b)
  - ()
- AND set appropriate NULL values

### Requirement: GROUPING() Function Execution

The executor SHALL implement GROUPING() function to identify grouping set membership.

#### Scenario: GROUPING() with simple grouping set
- WHEN executing `SELECT a, b, GROUPING(a), GROUPING(b), SUM(c) FROM t GROUP BY GROUPING SETS ((a), (b))`
- THEN GROUPING(a) returns 1 when a is NULL (rolled up), 0 otherwise
- AND GROUPING(b) returns 1 when b is NULL (rolled up), 0 otherwise

#### Scenario: GROUPING() with composite key
- WHEN executing `SELECT a, b, GROUPING(a, b), SUM(c) FROM t GROUP BY CUBE (a, b)`
- THEN GROUPING(a, b) returns bitmask indicating which columns are rolled up

### Requirement: RECURSIVE CTE Execution

The executor SHALL implement recursive CTEs using iterative fixpoint algorithm.

#### Scenario: Simple recursive CTE (sequence)
- WHEN executing `WITH RECURSIVE cte AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cte WHERE n < 5) SELECT * FROM cte`
- THEN the executor SHALL iterate:
  - Iteration 0: Insert (1) into work table
  - Iteration 1: Compute (2) from (1)
  - Iteration 2: Compute (3) from (2)
  - Iteration 3: Compute (4) from (3)
  - Iteration 4: Compute (5) from (4)
  - Iteration 5: Stop (n >= 5)
- AND return 5 rows: 1, 2, 3, 4, 5

#### Scenario: Recursive CTE with MAX RECURSION hint
- WHEN executing `WITH RECURSIVE cte AS (...) SELECT ... OPTION (MAX_RECURSION 10)`
- THEN the executor SHALL stop after 10 iterations
- AND raise error if recursion exceeds limit without termination

#### Scenario: Recursive CTE with multiple UNION ALL parts
- WHEN executing `WITH RECURSIVE cte AS (SELECT ... UNION ALL SELECT ... FROM cte JOIN ...)`
- THEN the executor SHALL correctly join recursive reference with base table

### Requirement: LATERAL Join Execution

The executor SHALL implement LATERAL joins by re-evaluating subquery for each outer row.

#### Scenario: LATERAL correlated subquery
- WHEN executing `SELECT t.id, sub.x FROM t, LATERAL (SELECT t.id + 1 AS x) AS sub`
- THEN for each row in t, execute subquery with t's bindings available
- AND produce one output row per input row with correlated value

#### Scenario: LATERAL with aggregation
- WHEN executing `SELECT t.id, sub.cnt FROM t, LATERAL (SELECT COUNT(*) FROM orders WHERE orders.customer_id = t.id) AS sub`
- THEN for each t row, execute aggregation with correlation
- AND correctly handle cases where no orders exist (NULL result)

#### Scenario: LATERAL table function
- WHEN executing `SELECT g.val FROM t, LATERAL generate_series(1, t.n) AS g(val)`
- THEN for each t row, generate series up to t.n value
- AND produce multiple rows per input row

### Requirement: MERGE INTO Execution

The executor SHALL implement MERGE INTO using HashJoin for matching.

#### Scenario: MERGE with WHEN MATCHED UPDATE
- WHEN executing `MERGE INTO target t USING source s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = s.x`
- THEN the executor SHALL:
  1. Build hash table from source
  2. Probe target against source hash table
  3. For matching rows, update target columns from source
  4. Return count of updated rows

#### Scenario: MERGE with WHEN NOT MATCHED INSERT
- WHEN executing `MERGE INTO target t USING source s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, x) VALUES (s.id, s.x)`
- THEN the executor SHALL:
  1. Build hash table from source
  2. Probe target against source hash table
  3. For non-matching source rows, insert into target
  4. Return count of inserted rows

#### Scenario: MERGE with multiple WHEN MATCHED conditions
- WHEN executing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND t.version < s.version THEN UPDATE SET x = s.x WHEN MATCHED THEN DELETE`
- THEN the executor SHALL evaluate conditions in order
- AND apply first matching action per source row

#### Scenario: MERGE with RETURNING
- WHEN executing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = s.x RETURNING old.*, new.*`
- THEN the executor SHALL return rows with old and new column values
- AND include only modified rows in result set

### Requirement: RETURNING Clause Execution

The executor SHALL return modified rows for INSERT, UPDATE, DELETE with RETURNING clause.

#### Scenario: INSERT RETURNING generated values
- WHEN executing `INSERT INTO t (a, b) VALUES (1, 2) RETURNING *`
- THEN the executor SHALL return row with generated rowid and column values

#### Scenario: UPDATE RETURNING modified columns
- WHEN executing `UPDATE t SET x = x + 1 WHERE id = 5 RETURNING id, x, old.x`
- THEN the executor SHALL return row with updated values and old.x before update

#### Scenario: DELETE RETURNING deleted values
- WHEN executing `DELETE FROM t WHERE id = 5 RETURNING *`
- THEN the executor SHALL return row with values before deletion
- AND row is removed from table

### Requirement: DISTINCT ON Execution

The executor SHALL implement DISTINCT ON using sort and first aggregate per key.

#### Scenario: DISTINCT ON with ORDER BY
- WHEN executing `SELECT DISTINCT ON (a) a, b, c FROM t ORDER BY a, b`
- THEN the executor SHALL sort by (a, b)
- AND for each unique a, return first row
- AND maintain ordering by ORDER BY columns

#### Scenario: DISTINCT ON without ORDER BY
- WHEN executing `SELECT DISTINCT ON (a) a, b FROM t`
- THEN the executor SHALL return arbitrary first row per a
- AND behavior matches DuckDB for undefined ordering

### Requirement: QUALIFY Clause Execution

The executor SHALL filter results after window function evaluation.

#### Scenario: QUALIFY with ROW_NUMBER
- WHEN executing `SELECT a, ROW_NUMBER() OVER (ORDER BY a) AS rn FROM t QUALIFY rn <= 3`
- THEN window functions SHALL be evaluated first
- AND filter applied to keep only rows where rn <= 3

#### Scenario: QUALIFY with aggregate window
- WHEN executing `SELECT a, SUM(b) OVER (PARTITION BY a) AS sum_b FROM t QUALIFY sum_b > 100`
- THEN window frame computed before QUALIFY filter
- AND only partitions with sum > 100 included in output

### Requirement: SAMPLE Clause Execution

The executor SHALL implement SAMPLE clause using reservoir sampling algorithm.

#### Scenario: SAMPLE with percentage (BERNOULLI)
- WHEN executing `SELECT * FROM t SAMPLE 10 PERCENT`
- THEN each row SHALL have 10% probability of inclusion
- AND result size approximately 10% of input

#### Scenario: SAMPLE with row count (RESERVOIR)
- WHEN executing `SELECT * FROM t SAMPLE 100 ROWS`
- THEN the executor SHALL select exactly 100 rows
- AND all rows have equal probability of selection

#### Scenario: SAMPLE with seed
- WHEN executing `SELECT * FROM t SAMPLE 50 ROWS (RESERVOIR, 50, 42)`
- THEN with same seed, same input SHALL produce same sample
- AND seed parameter enables reproducibility

#### Scenario: SAMPLE applied before LIMIT
- WHEN executing `SELECT * FROM t SAMPLE 10 PERCENT LIMIT 10`
- THEN SAMPLE SHALL be applied first, then LIMIT on sampled data
- AND final result has at most 10 rows

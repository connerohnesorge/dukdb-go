## ADDED Requirements

### Requirement: PIVOT Operation Parsing

The parser SHALL parse PIVOT statements with the following syntax:
```
PIVOT [INTO] table_name
ON pivot_column
USING aggregate_function(expression) [AS alias]
GROUP BY group_column [, ...]
[ORDER BY ...]
[LIMIT ...]
```

#### Scenario: Basic PIVOT with single aggregate
- WHEN parsing `PIVOT sales ON quarter USING SUM(amount) GROUP BY product`
- THEN create `PivotStmt` with Source pointing to `sales` table
- AND PivotOn containing `quarter` column expression
- AND Using containing `SUM(amount)` aggregate specification
- AND GroupBy containing `product` column

#### Scenario: PIVOT with multiple aggregates
- WHEN parsing `PIVOT sales ON quarter USING SUM(amount), AVG(amount) GROUP BY product`
- THEN create `PivotStmt` with two `PivotAggregate` entries
- AND aggregate names bound to output column aliases

#### Scenario: PIVOT with multiple pivot columns
- WHEN parsing `PIVOT sales ON year, quarter USING SUM(amount) GROUP BY product`
- THEN create `PivotStmt` with two pivot column expressions
- AND output columns created for each combination of pivot values

### Requirement: UNPIVOT Operation Parsing

The parser SHALL parse UNPIVOT statements with the following syntax:
```
UNPIVOT [INTO] table_name
INTO column_name FOR column_name IN (column1, column2, ...)
```

#### Scenario: Basic UNPIVOT
- WHEN parsing `UNPIVOT sales INTO value FOR name IN (jan, feb, mar)`
- THEN create `UnpivotStmt` with Source pointing to `sales` table
- AND Into containing `value` column name
- AND For containing `name` column for pivot column names
- AND Using containing `[jan, feb, mar]` column references

### Requirement: GROUPING SETS Parsing

The parser SHALL parse GROUP BY with grouping sets using the following syntax:
```
GROUP BY GROUPING SETS ((col1), (col2), (col1, col2))
GROUP BY ROLLUP (col1, col2, col3)
GROUP BY CUBE (col1, col2)
```

#### Scenario: GROUPING SETS with explicit sets
- WHEN parsing `GROUP BY GROUPING SETS ((a), (b), (a, b))`
- THEN create `GroupingSetExpr` with Type=Simple
- AND Exprs containing three grouping sets: `[a]`, `[b]`, `[a, b]`

#### Scenario: ROLLUP expansion
- WHEN parsing `GROUP BY ROLLUP (a, b, c)`
- THEN create `GroupingSetExpr` with Type=Rollup
- AND Exprs containing all rollup combinations: `[a, b, c]`, `[a, b]`, `[a]`, `[]`

#### Scenario: CUBE expansion
- WHEN parsing `GROUP BY CUBE (a, b, c)`
- THEN create `GroupingSetExpr` with Type=Cube
- AND Exprs containing all cube combinations

### Requirement: RECURSIVE CTE Parsing

The parser SHALL parse recursive CTEs using the following syntax:
```
WITH RECURSIVE cte_name AS (
    non_recursive_part
    UNION ALL
    recursive_part
)
SELECT ...
```

#### Scenario: Basic RECURSIVE CTE
- WHEN parsing `WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte`
- THEN create `SelectStmt` with CTEs containing one CTE
- AND CTE.Recursive set to true
- AND CTE.Query containing UNION ALL of non-recursive and recursive parts

#### Scenario: Multiple CTEs with RECURSIVE
- WHEN parsing `WITH RECURSIVE cte1 AS (...), cte2 AS (...) SELECT ...`
- THEN create `SelectStmt` with CTEs where only recursive ones have Recursive=true
- AND non-recursive CTEs resolved normally

### Requirement: LATERAL Join Parsing

The parser SHALL parse LATERAL joins using the following syntax:
```
FROM table1, LATERAL (subquery) AS t(cols)
FROM table1 CROSS JOIN LATERAL table_function(col)
```

#### Scenario: LATERAL subquery in FROM clause
- WHEN parsing `SELECT * FROM t, LATERAL (SELECT t.a + x.b FROM x WHERE x.id = t.id) AS result`
- THEN create `TableRef` with Subquery and Lateral=true

#### Scenario: LATERAL table function
- WHEN parsing `SELECT * FROM t, LATERAL generate_series(1, t.n) AS g(n)`
- THEN create `TableRef` with TableFunction and Lateral=true

### Requirement: DISTINCT ON Parsing

The parser SHALL parse DISTINCT ON clauses with the following syntax:
```
SELECT DISTINCT ON (col1, col2) col1, col2, col3 FROM t
```

#### Scenario: DISTINCT ON with single column
- WHEN parsing `SELECT DISTINCT ON (a) a, b FROM t`
- THEN set `SelectStmt.DistinctOn` to `[a]`
- AND set `SelectStmt.Distinct` to true

#### Scenario: DISTINCT ON with multiple columns
- WHEN parsing `SELECT DISTINCT ON (a, b) a, b, c FROM t`
- THEN set `SelectStmt.DistinctOn` to `[a, b]`

### Requirement: QUALIFY Clause Parsing

The parser SHALL parse QUALIFY clauses with the following syntax:
```
SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM t QUALIFY rn <= 10
```

#### Scenario: QUALIFY with window function
- WHEN parsing `SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b) AS rn FROM t QUALIFY rn = 1`
- THEN set `SelectStmt.Qualify` to expression checking `rn = 1`
- AND QUALIFY evaluated after window functions

### Requirement: SAMPLE Clause Parsing

The parser SHALL parse SAMPLE clauses with the following syntax:
```
SELECT * FROM t SAMPLE 10 PERCENT
SELECT * FROM t SAMPLE 100 ROWS
SELECT * FROM t SAMPLE (bernoulli, 50, 42) -- method, percentage, seed
```

#### Scenario: SAMPLE with percentage
- WHEN parsing `SELECT * FROM t SAMPLE 25 PERCENT`
- THEN create `SampleOptions` with Method=System, Percentage=25.0

#### Scenario: SAMPLE with row count
- WHEN parsing `SELECT * FROM t SAMPLE 500 ROWS`
- THEN create `SampleOptions` with Method=Reservoir, Rows=500

#### Scenario: SAMPLE with method specification
- WHEN parsing `SELECT * FROM t SAMPLE (bernoulli, 10, 42)`
- THEN create `SampleOptions` with Method=Bernoulli, Percentage=10.0, Seed=42

### Requirement: MERGE INTO Parsing

The parser SHALL parse MERGE INTO statements with the following syntax:
```
MERGE INTO target_table AS t
USING source_table AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET col = s.col
WHEN NOT MATCHED THEN INSERT (col) VALUES (s.col)
```

#### Scenario: MERGE with single WHEN MATCHED
- WHEN parsing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1`
- THEN create `MergeStmt` with Into=target table, Using=source table
- AND On=t.id = s.id condition
- AND WhenMatched containing one update action

#### Scenario: MERGE with multiple actions
- WHEN parsing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND cond THEN DELETE WHEN NOT MATCHED THEN INSERT (a) VALUES (b)`
- THEN create `MergeStmt` with multiple MergeAction entries
- AND actions contain optional conditions

#### Scenario: MERGE with RETURNING
- WHEN parsing `MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1 RETURNING *`
- THEN set `MergeStmt.ReturningCols` to all columns

### Requirement: RETURNING Clause Parsing

The parser SHALL parse RETURNING clauses for INSERT, UPDATE, DELETE with the following syntax:
```
INSERT INTO t VALUES (...) RETURNING *
INSERT INTO t VALUES (...) RETURNING col1, col2
UPDATE t SET x = 1 RETURNING *
DELETE FROM t RETURNING old.*
```

#### Scenario: INSERT RETURNING all columns
- WHEN parsing `INSERT INTO t VALUES (1) RETURNING *`
- THEN set `InsertStmt.ReturningCols` with star expansion

#### Scenario: UPDATE RETURNING specific columns
- WHEN parsing `UPDATE t SET x = 1 RETURNING x, y`
- THEN set `UpdateStmt.ReturningCols` with specified columns

#### Scenario: DELETE RETURNING with OLD
- WHEN parsing `DELETE FROM t RETURNING old.id, old.name`
- THEN set `DeleteStmt.ReturningCols` with old.* references

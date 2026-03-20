## ADDED Requirements

### Requirement: All LATERAL Join Types

The executor SHALL support INNER, LEFT, RIGHT, FULL, and CROSS LATERAL joins with proper null handling and type coercion.

#### Scenario: INNER LATERAL join with no matches
- GIVEN a table `t1` with 3 rows and a LATERAL subquery returning 0 rows for each left row
- WHEN executing `SELECT * FROM t1 INNER JOIN LATERAL (SELECT ...) sub ON true`
- THEN 0 rows are returned (matched rows required)
- AND no NULLs appear in output

#### Scenario: LEFT LATERAL join with no matches
- GIVEN a table `t1` with 3 rows and a LATERAL subquery returning 0 rows for some rows
- WHEN executing `SELECT * FROM t1 LEFT JOIN LATERAL (SELECT ...) sub ON true`
- THEN for unmatched left rows, right columns are filled with NULLs
- AND all 3 left rows appear in output (even unmatched ones)

#### Scenario: RIGHT LATERAL join with unmatched left rows
- GIVEN tables `t1` (3 rows) and LATERAL subquery returning 4 rows for only 1 left row
- WHEN executing `SELECT * FROM t1 RIGHT JOIN LATERAL (SELECT ...) sub ON true`
- THEN for the 1 matched row, 4 output rows appear (one per right row)
- AND left columns are NULLs for right rows without matching left row (if applicable)

#### Scenario: FULL LATERAL join with selective matches
- GIVEN `t1` (3 rows) and LATERAL subquery returning varying rows per left row
- WHEN executing `SELECT * FROM t1 FULL JOIN LATERAL (SELECT ...) sub ON true`
- THEN all left rows appear (even if subquery returns 0 rows)
- AND NULLs fill right columns when subquery returns nothing
- AND NULLs fill left columns when subquery returns rows with no matching left row (edge case)

#### Scenario: CROSS LATERAL join (implicit)
- GIVEN `SELECT * FROM t1 CROSS JOIN LATERAL (SELECT ...) sub`
- WHEN executing without ON condition
- THEN produces Cartesian product of t1 rows with each row's subquery results
- AND equivalent to `INNER JOIN LATERAL ... ON true`

### Requirement: Full Correlated Subquery Resolution

The executor SHALL fully resolve correlated column references in LATERAL subqueries, allowing subqueries to reference all outer table columns.

#### Scenario: Single-level correlation
- GIVEN tables `orders(id, customer_id)` and LATERAL subquery `SELECT * FROM order_items WHERE order_id = orders.id`
- WHEN executing `SELECT * FROM orders o LEFT JOIN LATERAL (SELECT * FROM order_items WHERE order_id = o.id) items ON true`
- THEN for each orders row, the WHERE clause correctly binds `o.id` to the current order
- AND returns all matching order_items for each order
- AND NULLs appear if no items match

#### Scenario: Multiple outer column references
- GIVEN complex LATERAL subquery referencing multiple outer columns: `SELECT ... FROM t2 WHERE t2.col1 = outer.col_a AND t2.col2 >= outer.col_b AND t2.col3 < outer.col_c`
- WHEN executing
- THEN all three outer column references are correctly bound
- AND filter conditions produce correct results

#### Scenario: Outer aggregates in LATERAL
- GIVEN `SELECT outer.id, agg_func(outer.values) as agg_val FROM outer LEFT JOIN LATERAL (SELECT * FROM inner WHERE inner.x > outer.agg_computed) i ON true`
- WHEN executing (where outer.agg_computed is a computed aggregate value)
- THEN the aggregate value is computed for each outer row
- AND correctly used in the LATERAL subquery WHERE clause

#### Scenario: Outer column in ORDER BY of LATERAL subquery
- GIVEN `SELECT * FROM orders o LEFT JOIN LATERAL (SELECT * FROM items WHERE id = o.order_id ORDER BY price * o.multiplier DESC) items ON true`
- WHEN executing
- THEN outer column `o.multiplier` is correctly used in ORDER BY calculation
- AND results are sorted correctly per outer row

### Requirement: Type Coercion in LATERAL Joins

The executor SHALL automatically coerce types when combining left and right result columns, matching DuckDB's type promotion rules.

#### Scenario: Integer + float column coercion
- GIVEN `SELECT * FROM t1 LEFT JOIN LATERAL (SELECT int_col) sub WHERE t1.float_col = 3.14`
- WHEN combining int_col with float_col in output
- THEN result type is FLOAT
- AND int values are implicitly cast to float

#### Scenario: String + varchar column coercion
- GIVEN combining VARCHAR and STRING columns in output
- WHEN executing the LATERAL join
- THEN both are treated as compatible string types
- AND output uses the larger/more general type

#### Scenario: NULL type propagation
- GIVEN output containing NULL values from LATERAL subquery
- WHEN determining result type for a column
- THEN NULL doesn't override the type from the non-NULL values
- AND type is determined from non-NULL values in the column

### Requirement: LATERAL Join Optimization

The planner SHALL use cost-based decisions to optimize LATERAL joins, choosing between LATERAL evaluation and subquery optimization.

#### Scenario: Cost estimation for LATERAL
- GIVEN a LATERAL join with a selective outer table (1% of rows)
- WHEN planning the query
- THEN the planner estimates cost assuming row-by-row evaluation
- AND includes per-row subquery evaluation cost in plan

#### Scenario: Subquery optimization when possible
- GIVEN a LATERAL join with uncorrelated subquery (no outer references)
- WHEN planning
- THEN the planner MAY convert to a regular join (not LATERAL)
- AND reduces row-by-row overhead

#### Scenario: Materialization for high-cardinality outer
- GIVEN a LATERAL join with 10M outer rows and expensive subquery
- WHEN planning
- THEN the planner MAY suggest materialization hints
- AND includes estimated memory/time trade-offs in EXPLAIN output

### Requirement: Error Handling for Invalid LATERAL Specifications

The executor SHALL provide clear error messages for invalid LATERAL specifications and detect impossible correlations.

#### Scenario: Forward reference (inner references outer not in scope)
- GIVEN `SELECT * FROM (SELECT * FROM t1 INNER JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) sub ON true) inner_alias LEFT JOIN t3 ON inner_alias.id = t3.id`
- WHEN executing
- THEN if t3 tries to reference a LATERAL subquery, error is raised
- AND message indicates "LATERAL subquery not in scope for this context"

#### Scenario: Circular correlation (self-referencing outer)
- GIVEN LATERAL subquery that references a table that itself has a LATERAL subquery
- WHEN executing
- THEN cycles are detected
- AND error message indicates "circular LATERAL reference not allowed"

#### Scenario: Ambiguous outer column
- GIVEN `SELECT * FROM t1, t2, LATERAL (SELECT * FROM t3 WHERE t3.col = col) sub`
- WHEN executing (where `col` is ambiguous between t1 and t2)
- THEN error is raised
- AND message indicates which tables have `col` and requests qualified name

#### Scenario: Non-existent outer column
- GIVEN `SELECT * FROM t1 LEFT JOIN LATERAL (SELECT * FROM t2 WHERE t2.id = nonexistent_col) sub ON true`
- WHEN executing
- THEN error is raised
- AND message indicates "column 'nonexistent_col' not found in outer scope"
- AND lists available columns from outer tables


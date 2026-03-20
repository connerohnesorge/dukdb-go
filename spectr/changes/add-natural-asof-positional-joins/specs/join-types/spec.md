## ADDED Requirements

### Requirement: NATURAL JOIN

The system SHALL support NATURAL JOIN, NATURAL LEFT JOIN, NATURAL RIGHT JOIN, and NATURAL FULL JOIN syntax. NATURAL JOIN SHALL automatically generate equi-join conditions for all columns with matching names (case-insensitive) between the left and right tables. The result set SHALL contain each common column exactly once, followed by remaining left columns, then remaining right columns.

#### Scenario: NATURAL JOIN with common columns

- WHEN table `a` has columns (id, name, value) and table `b` has columns (id, name, score)
- AND user executes `SELECT * FROM a NATURAL JOIN b`
- THEN the result has columns (id, name, value, score) — common columns appear once
- AND only rows where `a.id = b.id AND a.name = b.name` are returned

#### Scenario: NATURAL LEFT JOIN

- WHEN table `a` has columns (id, x) and table `b` has columns (id, y)
- AND user executes `SELECT * FROM a NATURAL LEFT JOIN b`
- THEN all rows from `a` are returned with matching `b.y` values (NULL if no match)

#### Scenario: NATURAL JOIN with no common columns

- WHEN table `a` has columns (x) and table `b` has columns (y)
- AND user executes `SELECT * FROM a NATURAL JOIN b`
- THEN the result is a CROSS JOIN (all combinations)

#### Scenario: NATURAL JOIN with common column names but different types

- WHEN table `a` has columns (id INTEGER, name VARCHAR) and table `b` has columns (id VARCHAR, score INTEGER)
- AND user executes `SELECT * FROM a NATURAL JOIN b`
- THEN the binder SHALL attempt implicit type coercion for the equality condition on `id` using standard type casting rules
- AND if the types are not coercible (e.g., INTEGER vs BOOLEAN), the binder SHALL return an error indicating a type mismatch on the common column

### Requirement: USING Clause

The system SHALL support the USING clause for INNER, LEFT, RIGHT, and FULL joins. The USING clause SHALL specify a list of column names that must exist in both tables and generate equi-join conditions. Common columns listed in USING SHALL appear exactly once in the result set.

#### Scenario: JOIN with USING clause

- WHEN table `orders` has columns (id, customer_id, amount) and table `customers` has columns (customer_id, name)
- AND user executes `SELECT * FROM orders JOIN customers USING (customer_id)`
- THEN the result has columns (customer_id, id, amount, name) — customer_id appears once
- AND only matching rows are returned

#### Scenario: USING with multiple columns

- WHEN user executes `SELECT * FROM a LEFT JOIN b USING (col1, col2)`
- THEN the join matches on both col1 and col2
- AND both columns appear exactly once in the result

#### Scenario: USING with non-existent column

- WHEN user executes `SELECT * FROM a JOIN b USING (nonexistent)`
- THEN an error is returned indicating the column does not exist in one or both tables

#### Scenario: JOIN with both ON and USING

- WHEN user executes `SELECT * FROM a JOIN b ON a.id = b.id USING (id)`
- THEN the parser SHALL return an error indicating that ON and USING are mutually exclusive

#### Scenario: NATURAL JOIN with ON clause

- WHEN user executes `SELECT * FROM a NATURAL JOIN b ON a.id = b.id`
- THEN the parser SHALL return an error indicating that NATURAL JOIN cannot have an ON or USING clause

### Requirement: ASOF JOIN

The system SHALL support ASOF JOIN and ASOF LEFT JOIN for time-series and ordered data matching. ASOF JOIN SHALL find the nearest match in the right table for each row in the left table based on an inequality condition, optionally combined with exact equality conditions. The inequality condition SHALL support `>=` (find greatest value less than or equal) and `<=` (find smallest value greater than or equal) operators.

The ASOF JOIN ON clause SHALL contain exactly one inequality condition (`>=`, `>`, `<=`, or `<`) and zero or more equality conditions (`=`), combined with AND. Top-level OR in the ON clause SHALL produce a binder error. The binder SHALL decompose the ON clause by flattening top-level AND conjuncts and classifying each as equality or inequality. Exactly one inequality conjunct is required; zero or more than one SHALL produce an error.

The planner SHALL ensure both inputs to an ASOF JOIN are sorted by the equality key columns followed by the inequality key column (ASC for `>=`/`>`, DESC for `<=`/`<`). If the child operators do not already provide the required ordering, the planner SHALL insert Sort operators below the ASOF JOIN node.

#### Scenario: ASOF JOIN with timestamp matching

- WHEN table `trades` has rows with timestamps [10:01, 10:05, 10:10]
- AND table `quotes` has rows with timestamps [10:00, 10:03, 10:07, 10:12]
- AND user executes `SELECT * FROM trades t ASOF JOIN quotes q ON t.timestamp >= q.timestamp`
- THEN trade at 10:01 matches quote at 10:00
- AND trade at 10:05 matches quote at 10:03
- AND trade at 10:10 matches quote at 10:07

#### Scenario: ASOF JOIN with equality and inequality conditions

- WHEN user executes `SELECT * FROM trades t ASOF JOIN quotes q ON t.ticker = q.ticker AND t.time >= q.time`
- THEN the equality condition (ticker) groups rows
- AND the inequality condition (time) finds the nearest match within each group

#### Scenario: ASOF LEFT JOIN with no match

- WHEN a left row has no matching right row (inequality condition not satisfiable)
- AND user executes an ASOF LEFT JOIN
- THEN the left row is returned with NULL values for right columns

#### Scenario: ASOF JOIN with no match (inner semantics)

- WHEN a left row has no matching right row
- AND user executes an ASOF JOIN (not LEFT)
- THEN the left row is excluded from the result

#### Scenario: ASOF JOIN with invalid ON clause (OR at top level)

- WHEN user executes `SELECT * FROM a ASOF JOIN b ON a.t >= b.t OR a.id = b.id`
- THEN the binder SHALL return an error indicating that OR is not allowed in ASOF JOIN conditions

#### Scenario: ASOF JOIN with multiple inequality conditions

- WHEN user executes `SELECT * FROM a ASOF JOIN b ON a.t >= b.t AND a.v <= b.v`
- THEN the binder SHALL return an error indicating that exactly one inequality condition is required

### Requirement: POSITIONAL JOIN

The system SHALL support POSITIONAL JOIN which matches rows by their ordinal position (first row with first row, second with second, etc.). If one input is shorter than the other, the missing values SHALL be filled with NULLs (similar to a FULL OUTER JOIN by position). POSITIONAL JOIN SHALL NOT accept an ON or USING clause.

Position matching SHALL use global ordinal row numbers across the entire table, not per-chunk offsets. The executor SHALL maintain a persistent position counter that carries across chunk boundaries, ensuring that row N from the left input is always paired with row N from the right input regardless of how rows are batched into chunks.

#### Scenario: POSITIONAL JOIN with equal-length tables

- WHEN table `a` has 3 rows and table `b` has 3 rows
- AND user executes `SELECT * FROM a POSITIONAL JOIN b`
- THEN 3 rows are returned, each combining row N from `a` with row N from `b`

#### Scenario: POSITIONAL JOIN with unequal lengths

- WHEN table `a` has 3 rows and table `b` has 5 rows
- AND user executes `SELECT * FROM a POSITIONAL JOIN b`
- THEN 5 rows are returned
- AND rows 4-5 have NULL for all `a` columns

#### Scenario: POSITIONAL JOIN with empty table

- WHEN table `a` has 3 rows and table `b` has 0 rows
- AND user executes `SELECT * FROM a POSITIONAL JOIN b`
- THEN 3 rows are returned with NULL for all `b` columns

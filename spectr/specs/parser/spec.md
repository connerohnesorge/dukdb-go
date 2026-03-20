# Parser Specification

## Requirements

### Requirement: WindowExpr AST Node

The parser SHALL define WindowExpr AST node with all window-specific fields.

#### Scenario: WindowExpr contains all fields
- GIVEN the WindowExpr type definition
- THEN it contains Function, PartitionBy, OrderBy, Frame fields
- AND it contains IgnoreNulls boolean field
- AND it contains Filter expression field
- AND it contains Distinct boolean field

#### Scenario: WindowOrderBy contains NULLS FIRST/LAST
- GIVEN the WindowOrderBy type definition
- THEN it contains Expr, Desc, and NullsFirst fields

#### Scenario: WindowFrame contains EXCLUDE clause
- GIVEN the WindowFrame type definition
- THEN it contains Type (ROWS/RANGE/GROUPS), Start, End, and Exclude fields

### Requirement: Window Expression Parsing

The parser SHALL parse window function expressions with OVER clause.

#### Scenario: Simple window function
- GIVEN the SQL "SELECT ROW_NUMBER() OVER () FROM t"
- WHEN parsing the statement
- THEN a WindowExpr AST node is created
- AND Function.Name equals "row_number"
- AND PartitionBy is empty
- AND OrderBy is empty
- AND Frame is nil (default frame applied during binding)

#### Scenario: Window function with PARTITION BY
- GIVEN the SQL "SELECT SUM(x) OVER (PARTITION BY dept) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.PartitionBy contains one ColumnRef for "dept"

#### Scenario: Window function with ORDER BY
- GIVEN the SQL "SELECT RANK() OVER (ORDER BY salary DESC) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.OrderBy contains one OrderByExpr
- AND OrderByExpr.Desc equals true

#### Scenario: Window function with PARTITION BY and ORDER BY
- GIVEN the SQL "SELECT DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.PartitionBy contains one expression
- AND WindowExpr.OrderBy contains one expression

#### Scenario: Window function with multiple partition columns
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (PARTITION BY dept, region ORDER BY name) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.PartitionBy contains two ColumnRefs
- AND WindowExpr.OrderBy contains one OrderByExpr

### Requirement: Frame Specification Parsing

The parser SHALL parse frame specifications in OVER clause.

#### Scenario: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Type equals FrameTypeRows
- AND WindowExpr.Frame.Start.Type equals BoundUnboundedPreceding
- AND WindowExpr.Frame.End.Type equals BoundCurrentRow

#### Scenario: ROWS BETWEEN n PRECEDING AND n FOLLOWING
- GIVEN the SQL "SELECT AVG(x) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Type equals FrameTypeRows
- AND WindowExpr.Frame.Start.Type equals BoundPreceding
- AND WindowExpr.Frame.Start.Offset evaluates to 3
- AND WindowExpr.Frame.End.Type equals BoundFollowing
- AND WindowExpr.Frame.End.Offset evaluates to 3

#### Scenario: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
- GIVEN the SQL "SELECT SUM(x) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Type equals FrameTypeRange
- AND WindowExpr.Frame.Start.Type equals BoundUnboundedPreceding
- AND WindowExpr.Frame.End.Type equals BoundUnboundedFollowing

#### Scenario: ROWS UNBOUNDED PRECEDING shorthand
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Start.Type equals BoundUnboundedPreceding
- AND WindowExpr.Frame.End.Type equals BoundCurrentRow (implicit)

#### Scenario: ROWS CURRENT ROW shorthand
- GIVEN the SQL "SELECT x OVER (ORDER BY id ROWS CURRENT ROW) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Start.Type equals BoundCurrentRow
- AND WindowExpr.Frame.End.Type equals BoundCurrentRow

#### Scenario: GROUPS BETWEEN parsing
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Type equals FrameTypeGroups
- AND WindowExpr.Frame.Start.Type equals BoundPreceding
- AND WindowExpr.Frame.End.Type equals BoundFollowing

#### Scenario: GROUPS UNBOUNDED PRECEDING
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id GROUPS UNBOUNDED PRECEDING) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Type equals FrameTypeGroups
- AND WindowExpr.Frame.Start.Type equals BoundUnboundedPreceding
- AND WindowExpr.Frame.End.Type equals BoundCurrentRow

### Requirement: EXCLUDE Clause Parsing

The parser SHALL parse EXCLUDE clause in frame specification.

#### Scenario: EXCLUDE NO OTHERS
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE NO OTHERS) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Exclude equals ExcludeNoOthers

#### Scenario: EXCLUDE CURRENT ROW
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Exclude equals ExcludeCurrentRow

#### Scenario: EXCLUDE GROUP
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING EXCLUDE GROUP) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Exclude equals ExcludeGroup

#### Scenario: EXCLUDE TIES
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING EXCLUDE TIES) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Frame.Exclude equals ExcludeTies

### Requirement: NULLS FIRST/LAST Parsing

The parser SHALL parse NULLS FIRST and NULLS LAST in window ORDER BY.

#### Scenario: ORDER BY with NULLS FIRST
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (ORDER BY x NULLS FIRST) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.OrderBy[0].NullsFirst equals true

#### Scenario: ORDER BY with NULLS LAST
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (ORDER BY x NULLS LAST) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.OrderBy[0].NullsFirst equals false

#### Scenario: ORDER BY DESC NULLS FIRST
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (ORDER BY x DESC NULLS FIRST) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.OrderBy[0].Desc equals true
- AND WindowExpr.OrderBy[0].NullsFirst equals true

#### Scenario: Default NULLS ordering
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (ORDER BY x) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.OrderBy[0].NullsFirst equals false (NULLS LAST is default)

### Requirement: IGNORE NULLS Parsing

The parser SHALL parse IGNORE NULLS modifier for value functions.

#### Scenario: LAG with IGNORE NULLS
- GIVEN the SQL "SELECT LAG(x) IGNORE NULLS OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.IgnoreNulls equals true

#### Scenario: LEAD with IGNORE NULLS
- GIVEN the SQL "SELECT LEAD(x) IGNORE NULLS OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.IgnoreNulls equals true

#### Scenario: FIRST_VALUE with IGNORE NULLS
- GIVEN the SQL "SELECT FIRST_VALUE(x) IGNORE NULLS OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.IgnoreNulls equals true

#### Scenario: RESPECT NULLS (default)
- GIVEN the SQL "SELECT LAG(x) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.IgnoreNulls equals false

#### Scenario: Explicit RESPECT NULLS
- GIVEN the SQL "SELECT LAG(x) RESPECT NULLS OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.IgnoreNulls equals false

### Requirement: FILTER Clause Parsing

The parser SHALL parse FILTER clause for aggregate window functions.

#### Scenario: COUNT with FILTER
- GIVEN the SQL "SELECT COUNT(*) FILTER (WHERE x > 5) OVER (PARTITION BY dept) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Filter is a BinaryExpr comparing x > 5

#### Scenario: SUM with FILTER
- GIVEN the SQL "SELECT SUM(amount) FILTER (WHERE status = 'active') OVER () FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Filter is a comparison expression
- AND WindowExpr.Function.Name equals "sum"

#### Scenario: FILTER before OVER
- GIVEN the SQL "SELECT AVG(x) FILTER (WHERE y > 0) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Filter is not nil
- AND WindowExpr.Frame is nil (uses default frame)

### Requirement: DISTINCT Aggregate Windows Parsing

The parser SHALL parse DISTINCT modifier for aggregate window functions.

#### Scenario: COUNT DISTINCT with OVER
- GIVEN the SQL "SELECT COUNT(DISTINCT x) OVER (PARTITION BY dept) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Distinct equals true
- AND WindowExpr.Function.Name equals "count"

#### Scenario: SUM DISTINCT with OVER
- GIVEN the SQL "SELECT SUM(DISTINCT amount) OVER () FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Distinct equals true
- AND WindowExpr.Function.Name equals "sum"

### Requirement: Window Function Arguments Parsing

The parser SHALL parse window function arguments correctly.

#### Scenario: LAG with default arguments
- GIVEN the SQL "SELECT LAG(x) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Args contains one expression (x)

#### Scenario: LAG with offset
- GIVEN the SQL "SELECT LAG(x, 2) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Args contains two expressions
- AND Args[1] evaluates to 2

#### Scenario: LAG with offset and default
- GIVEN the SQL "SELECT LAG(x, 2, 0) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Args contains three expressions
- AND Args[2] evaluates to 0

#### Scenario: NTILE with bucket count
- GIVEN the SQL "SELECT NTILE(4) OVER (ORDER BY x) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Args contains one expression
- AND Args[0] evaluates to 4

#### Scenario: NTH_VALUE with index
- GIVEN the SQL "SELECT NTH_VALUE(x, 3) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Args contains two expressions
- AND Args[1] evaluates to 3

### Requirement: Multiple Window Functions

The parser SHALL support multiple window functions in a single SELECT.

#### Scenario: Multiple window functions with different partitions
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (PARTITION BY a), RANK() OVER (PARTITION BY b ORDER BY c) FROM t"
- WHEN parsing the statement
- THEN SelectStmt.Columns contains two SelectColumn entries
- AND each has a WindowExpr with different partition specifications

#### Scenario: Window function with alias
- GIVEN the SQL "SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t"
- WHEN parsing the statement
- THEN SelectColumn.Alias equals "rn"
- AND SelectColumn.Expr is a WindowExpr

### Requirement: Aggregate Functions as Windows

The parser SHALL allow aggregate functions with OVER clause.

#### Scenario: COUNT with OVER
- GIVEN the SQL "SELECT COUNT(*) OVER (PARTITION BY dept) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Name equals "count"
- AND WindowExpr.Function.Star equals true

#### Scenario: SUM with OVER and frame
- GIVEN the SQL "SELECT SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Name equals "sum"
- AND WindowExpr.Frame is not nil

#### Scenario: AVG with OVER
- GIVEN the SQL "SELECT AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date) FROM t"
- WHEN parsing the statement
- THEN WindowExpr.Function.Name equals "avg"
- AND WindowExpr.PartitionBy contains one expression
- AND WindowExpr.OrderBy contains one expression

### Requirement: Parser Error Handling

The parser SHALL return appropriate errors for invalid window syntax.

#### Scenario: OVER without parentheses
- GIVEN the SQL "SELECT ROW_NUMBER() OVER FROM t"
- WHEN parsing the statement
- THEN ErrorTypeParser is returned
- AND error message indicates expected '('

#### Scenario: Invalid frame specification
- GIVEN the SQL "SELECT SUM(x) OVER (ROWS BETWEEN FOLLOWING AND PRECEDING) FROM t"
- WHEN parsing the statement
- THEN ErrorTypeParser is returned
- AND error message indicates invalid frame bounds

#### Scenario: Missing frame end
- GIVEN the SQL "SELECT SUM(x) OVER (ROWS BETWEEN 1 PRECEDING) FROM t"
- WHEN parsing the statement
- THEN parsing succeeds (single bound is valid shorthand)
- AND Frame.End.Type equals BoundCurrentRow

#### Scenario: Negative offset
- GIVEN the SQL "SELECT LAG(x, -1) OVER (ORDER BY id) FROM t"
- WHEN parsing the statement
- THEN parsing succeeds (validation during binding)

### Requirement: CREATE VIEW Statement
The parser SHALL parse `CREATE VIEW` statements with the following syntax:
```
CREATE VIEW [IF NOT EXISTS] [schema_name.]view_name AS select_statement
```

#### Scenario: Basic CREATE VIEW
- WHEN parsing `CREATE VIEW my_view AS SELECT * FROM t`
- THEN CreateViewStmt SHALL have View="my_view", Schema="main", IfNotExists=false
- THEN CreateViewStmt.Query SHALL be a bound SelectStmt

#### Scenario: CREATE VIEW with schema and IF NOT EXISTS
- WHEN parsing `CREATE VIEW IF NOT EXISTS schema.my_view AS SELECT id, name FROM t`
- THEN CreateViewStmt SHALL have View="my_view", Schema="schema", IfNotExists=true

#### Scenario: CREATE VIEW with complex SELECT
- WHEN parsing `CREATE VIEW v AS SELECT a, SUM(b) AS total FROM t GROUP BY a`
- THEN CreateViewStmt.Query SHALL contain all SELECT clause elements

### Requirement: DROP VIEW Statement
The parser SHALL parse `DROP VIEW` statements with the following syntax:
```
DROP VIEW [IF EXISTS] [schema_name.]view_name
```

#### Scenario: Basic DROP VIEW
- WHEN parsing `DROP VIEW my_view`
- THEN DropViewStmt SHALL have View="my_view", Schema="main", IfExists=false

#### Scenario: DROP VIEW with IF EXISTS
- WHEN parsing `DROP VIEW IF EXISTS schema.my_view`
- THEN DropViewStmt SHALL have View="my_view", Schema="schema", IfExists=true

### Requirement: CREATE INDEX Statement
The parser SHALL parse `CREATE INDEX` statements with the following syntax:
```
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON table_name (column_name [, ...])
```

#### Scenario: Basic CREATE INDEX
- WHEN parsing `CREATE INDEX idx ON t (col1)`
- THEN CreateIndexStmt SHALL have Index="idx", Table="t", Columns=["col1"], IsUnique=false

#### Scenario: CREATE UNIQUE INDEX
- WHEN parsing `CREATE UNIQUE INDEX idx ON t (col1, col2)`
- THEN CreateIndexStmt SHALL have IsUnique=true, Columns=["col1", "col2"]

#### Scenario: CREATE INDEX with schema
- WHEN parsing `CREATE INDEX IF NOT EXISTS schema.idx ON schema.t (col1)`
- THEN CreateIndexStmt SHALL have Schema="schema", Index="idx", Table="t"

### Requirement: DROP INDEX Statement
The parser SHALL parse `DROP INDEX` statements with the following syntax:
```
DROP INDEX [IF EXISTS] [schema_name.]index_name
```

#### Scenario: Basic DROP INDEX
- WHEN parsing `DROP INDEX my_idx`
- THEN DropIndexStmt SHALL have Index="my_idx", Schema="main", IfExists=false

### Requirement: CREATE SEQUENCE Statement
The parser SHALL parse `CREATE SEQUENCE` statements with the following syntax:
```
CREATE SEQUENCE [IF NOT EXISTS] [schema_name.]sequence_name
[START WITH start_value]
[INCREMENT BY increment_value]
[MINVALUE min_value | NO MINVALUE]
[MAXVALUE max_value | NO MAXVALUE]
[CYCLE | NO CYCLE]
```

#### Scenario: Basic CREATE SEQUENCE
- WHEN parsing `CREATE SEQUENCE seq`
- THEN CreateSequenceStmt SHALL have Sequence="seq", Schema="main"

#### Scenario: CREATE SEQUENCE with all options
- WHEN parsing `CREATE SEQUENCE seq START WITH 100 INCREMENT BY 2 MINVALUE 1 MAXVALUE 1000 CYCLE`
- THEN CreateSequenceStmt SHALL have StartWith=100, IncrementBy=2, MinValue=1, MaxValue=1000, IsCycle=true

#### Scenario: CREATE SEQUENCE with NO MINVALUE/NO MAXVALUE
- WHEN parsing `CREATE SEQUENCE seq INCREMENT BY -1 NO MINVALUE NO MAXVALUE`
- THEN CreateSequenceStmt SHALL have MinValue=nil, MaxValue=nil

### Requirement: DROP SEQUENCE Statement
The parser SHALL parse `DROP SEQUENCE` statements with the following syntax:
```
DROP SEQUENCE [IF EXISTS] [schema_name.]sequence_name
```

#### Scenario: Basic DROP SEQUENCE
- WHEN parsing `DROP SEQUENCE my_seq`
- THEN DropSequenceStmt SHALL have Sequence="my_seq", Schema="main"

### Requirement: CREATE SCHEMA Statement
The parser SHALL parse `CREATE SCHEMA` statements with the following syntax:
```
CREATE SCHEMA [IF NOT EXISTS] schema_name
```

#### Scenario: Basic CREATE SCHEMA
- WHEN parsing `CREATE SCHEMA my_schema`
- THEN CreateSchemaStmt SHALL have Schema="my_schema", IfNotExists=false

#### Scenario: CREATE SCHEMA with IF NOT EXISTS
- WHEN parsing `CREATE SCHEMA IF NOT EXISTS my_schema`
- THEN CreateSchemaStmt SHALL have IfNotExists=true

### Requirement: DROP SCHEMA Statement
The parser SHALL parse `DROP SCHEMA` statements with the following syntax:
```
DROP SCHEMA [IF EXISTS] schema_name [CASCADE | RESTRICT]
```

#### Scenario: Basic DROP SCHEMA
- WHEN parsing `DROP SCHEMA my_schema`
- THEN DropSchemaStmt SHALL have Schema="my_schema", IfExists=false, Cascade=false

#### Scenario: DROP SCHEMA with CASCADE
- WHEN parsing `DROP SCHEMA my_schema CASCADE`
- THEN DropSchemaStmt SHALL have Cascade=true

### Requirement: ALTER TABLE Statement (Extended)
The parser SHALL parse `ALTER TABLE` statements with additional operations:
```
ALTER TABLE [IF EXISTS] [schema_name.]table_name
  RENAME TO new_table_name
  | RENAME COLUMN old_name TO new_name
  | DROP COLUMN column_name
  | SET (option = value [, ...])
```

#### Scenario: ALTER TABLE RENAME TO
- WHEN parsing `ALTER TABLE t RENAME TO new_t`
- THEN AlterTableStmt SHALL have Operation=AlterTableRenameTo, NewTableName="new_t"

#### Scenario: ALTER TABLE RENAME COLUMN
- WHEN parsing `ALTER TABLE t RENAME COLUMN old_col TO new_col`
- THEN AlterTableStmt SHALL have Operation=AlterTableRenameColumn, OldColumn="old_col", NewColumn="new_col"

#### Scenario: ALTER TABLE DROP COLUMN
- WHEN parsing `ALTER TABLE t DROP COLUMN col`
- THEN AlterTableStmt SHALL have Operation=AlterTableDropColumn, Column="col"

### Requirement: DDL Statement Type Detection
The parser SHALL correctly identify DDL statement types for the `Statement.Type()` interface.

#### Scenario: Statement type for CREATE VIEW
- WHEN calling `Type()` on a parsed `CreateViewStmt`
- THEN it SHALL return `dukdb.STATEMENT_TYPE_CREATE`

#### Scenario: Statement type for DROP VIEW
- WHEN calling `Type()` on a parsed `DropViewStmt`
- THEN it SHALL return `dukdb.STATEMENT_TYPE_DROP`

#### Scenario: Statement type for other DDL
- WHEN calling `Type()` on any parsed DDL statement
- THEN it SHALL return the appropriate statement type (CREATE or DROP)

### Requirement: DDL Parameter Collection
The parser SHALL correctly collect parameter placeholders from DDL statements.

#### Scenario: Parameters in CREATE VIEW AS SELECT
- WHEN collecting parameters from `CREATE VIEW v AS SELECT * FROM t WHERE id = ?`
- THEN the collector SHALL return a single parameter at position 1

#### Scenario: Parameters in CREATE SEQUENCE
- WHEN collecting parameters from `CREATE SEQUENCE s START WITH ?`
- THEN the collector SHALL return a single parameter at position 1

### Requirement: DDL Parameter Counting
The parser SHALL correctly count parameters in DDL statements.

#### Scenario: Parameter count in DDL
- WHEN counting parameters in `CREATE VIEW v AS SELECT * FROM t WHERE id = $1 AND name = $2`
- THEN the counter SHALL return 2

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

### Requirement: Excel Table Function Parsing
The parser SHALL parse read_excel and read_excel_auto table functions in FROM clause with full DuckDB options for visual fidelity verification.

#### Scenario: Basic read_excel parsing
- GIVEN `SELECT * FROM read_excel('data.xlsx')`
- WHEN parsing
- THEN TableRef.TableFunction.Name == \"read_excel\"
- AND Args == [Literal 'data.xlsx']

#### Scenario: read_excel_auto with options
- GIVEN `SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10')`
- WHEN parsing
- THEN NamedArgs contains \"sheet\"='Sheet1', \"range\"='A1:C10'

### Requirement: TableFunctionRef Excel Extensions
The TableFunctionRef SHALL handle Excel-specific named arguments without parsing errors, storing unknown options for executor handling.

#### Scenario: Unknown Excel option
- GIVEN `read_excel(..., unknown_opt='val')`
- WHEN parsing TableFunctionRef
- THEN NamedArgs[\"unknown_opt\"] == Literal 'val' (no parse error)

#### Scenario: Basic read_excel
- GIVEN `SELECT * FROM read_excel('data.xlsx')`
- THEN TableRef.TableFunction.Name == \"read_excel\"
- AND Args[0] == Literal string 'data.xlsx'

#### Scenario: read_excel_auto with sheet/range
- GIVEN `SELECT * FROM read_excel_auto('data.xlsx', sheet='Sheet1', range='A1:C10', header=true)`
- THEN TableFunction.NamedArgs[\"sheet\"] == Literal 'Sheet1'
- AND NamedArgs[\"range\"] == Literal 'A1:C10'
- AND NamedArgs[\"header\"] == Literal true

#### Scenario: All Excel options
- GIVEN full opts (header_row=0, skip_rows=1, dtype=map, na_values=list etc.)
- THEN all NamedArgs populated as Expr (Literal/Map/LIst)

### Requirement: CreateScalarUDFStmt Parsing
The parser SHALL parse CREATE FUNCTION for scalar UDFs with full syntax.

#### Scenario: Simple SQL UDF
- GIVEN `CREATE FUNCTION add(a INTEGER, b INTEGER) RETURNS INTEGER AS 'SELECT a + b'`
- THEN CreateScalarUDFStmt{Name:\"add\", Params:[{Name:\"a\",Type:INTEGER},{Name:\"b\",Type:INTEGER}], Returns:INTEGER, Lang:\"sql\", Body:\"SELECT a + b\"}

#### Scenario: Python UDF multi-line
- GIVEN `CREATE FUNCTION py_len(s VARCHAR) RETURNS INTEGER LANGUAGE python AS $$import sys; return len(s)$$`
- THEN Lang:\"python\", Body multi-line

#### Scenario: With attributes
- GIVEN `CREATE OR REPLACE IMMUTABLE FUNCTION safe_div(a DOUBLE, b DOUBLE) RETURNS DOUBLE AS 'SELECT CASE WHEN b=0 THEN NULL ELSE a/b END'`
- THEN OrReplace:true, Volatility:IMMUTABLE

### Requirement: UDF Param Parsing
Params SHALL be name TypeName.

#### Scenario: Multiple params
- GIVEN (id UUID, name VARCHAR(50))
- THEN Params list w/ types

### Requirement: EXPORT DATABASE Statement Parsing

The parser SHALL parse `EXPORT DATABASE 'path'` with optional parenthesized options including FORMAT specification.

#### Scenario: EXPORT DATABASE with default format

- WHEN parsing `EXPORT DATABASE '/tmp/mydb'`
- THEN the parser produces an ExportDatabaseStmt with Path="/tmp/mydb" and empty Options

#### Scenario: EXPORT DATABASE with FORMAT option

- WHEN parsing `EXPORT DATABASE '/tmp/mydb' (FORMAT PARQUET)`
- THEN the parser produces an ExportDatabaseStmt with Path="/tmp/mydb" and Options={"FORMAT": "PARQUET"}

#### Scenario: EXPORT DATABASE with multiple options

- WHEN parsing `EXPORT DATABASE '/tmp/mydb' (FORMAT CSV, DELIMITER '|', HEADER true)`
- THEN the parser produces an ExportDatabaseStmt with Options containing FORMAT, DELIMITER, and HEADER

### Requirement: IMPORT DATABASE Statement Parsing

The parser SHALL parse `IMPORT DATABASE 'path'` to load a previously exported database.

#### Scenario: IMPORT DATABASE basic

- WHEN parsing `IMPORT DATABASE '/tmp/mydb'`
- THEN the parser produces an ImportDatabaseStmt with Path="/tmp/mydb"

### Requirement: PREPARE Statement Parsing

The parser SHALL parse `PREPARE name AS statement` to create named prepared statements with parameter placeholders.

#### Scenario: PREPARE a SELECT statement

- WHEN parsing `PREPARE my_query AS SELECT * FROM users WHERE id = $1`
- THEN the parser produces a PrepareStmt with Name="my_query"
- AND Inner is a SelectStmt with a $1 parameter placeholder

#### Scenario: PREPARE an INSERT statement

- WHEN parsing `PREPARE my_insert AS INSERT INTO t (a, b) VALUES ($1, $2)`
- THEN the parser produces a PrepareStmt with Name="my_insert"
- AND Inner is an InsertStmt with $1 and $2 parameter placeholders

#### Scenario: PREPARE a DELETE statement

- WHEN parsing `PREPARE my_delete AS DELETE FROM t WHERE id = $1`
- THEN the parser produces a PrepareStmt with Name="my_delete"
- AND Inner is a DeleteStmt

### Requirement: EXECUTE Statement Parsing

The parser SHALL parse `EXECUTE name` and `EXECUTE name(params)` for executing named prepared statements.

#### Scenario: EXECUTE without parameters

- WHEN parsing `EXECUTE my_query`
- THEN the parser produces an ExecuteStmt with Name="my_query" and empty Params

#### Scenario: EXECUTE with parameters

- WHEN parsing `EXECUTE my_query(42, 'hello')`
- THEN the parser produces an ExecuteStmt with Name="my_query"
- AND Params contains IntLiteral(42) and StringLiteral('hello')

#### Scenario: EXECUTE with expressions as parameters

- WHEN parsing `EXECUTE my_query(1 + 2, CURRENT_DATE)`
- THEN the parser produces an ExecuteStmt with expression parameters
- AND Params[0] is a BinaryExpr (1 + 2)

### Requirement: DEALLOCATE Statement Parsing

The parser SHALL parse `DEALLOCATE [PREPARE] name` and `DEALLOCATE ALL` for releasing prepared statements.

#### Scenario: DEALLOCATE by name

- WHEN parsing `DEALLOCATE my_query`
- THEN the parser produces a DeallocateStmt with Name="my_query"

#### Scenario: DEALLOCATE PREPARE by name

- WHEN parsing `DEALLOCATE PREPARE my_query`
- THEN the parser produces a DeallocateStmt with Name="my_query"

#### Scenario: DEALLOCATE ALL

- WHEN parsing `DEALLOCATE ALL`
- THEN the parser produces a DeallocateStmt with Name="" (empty, meaning all)

### Requirement: UNIQUE Constraint Parsing

The parser SHALL parse UNIQUE constraints in CREATE TABLE at both column-level and table-level.

#### Scenario: Column-level UNIQUE

- WHEN parsing `CREATE TABLE t (id INTEGER, email VARCHAR UNIQUE)`
- THEN the parsed statement includes a UNIQUE constraint on column "email"

#### Scenario: Table-level UNIQUE on multiple columns

- WHEN parsing `CREATE TABLE t (a INTEGER, b INTEGER, UNIQUE (a, b))`
- THEN the parsed statement includes a UNIQUE constraint on columns ["a", "b"]

#### Scenario: Named UNIQUE constraint

- WHEN parsing `CREATE TABLE t (id INTEGER, CONSTRAINT uq_email UNIQUE (email))`
- THEN the constraint has Name="uq_email"

### Requirement: CHECK Constraint Parsing

The parser SHALL parse CHECK constraints with arbitrary boolean expressions.

#### Scenario: Column-level CHECK

- WHEN parsing `CREATE TABLE t (age INTEGER CHECK (age >= 0))`
- THEN the parsed statement includes a CHECK constraint with expression `age >= 0`

#### Scenario: Table-level CHECK with multiple columns

- WHEN parsing `CREATE TABLE t (start_date DATE, end_date DATE, CHECK (end_date > start_date))`
- THEN the CHECK constraint references both columns

### Requirement: FOREIGN KEY Constraint Parsing

The parser SHALL parse FOREIGN KEY constraints with REFERENCES clause and optional ON DELETE/UPDATE actions.

#### Scenario: Column-level REFERENCES

- WHEN parsing `CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))`
- THEN the parsed statement includes a FK constraint referencing users(id)

#### Scenario: Table-level FOREIGN KEY with RESTRICT

- WHEN parsing `CREATE TABLE orders (user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)`
- THEN the FK constraint has OnDelete=RESTRICT

#### Scenario: FOREIGN KEY rejects CASCADE action

- WHEN parsing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id) ON DELETE CASCADE)`
- THEN a parse error is returned: "FOREIGN KEY constraints cannot use CASCADE, SET NULL or SET DEFAULT"

#### Scenario: FOREIGN KEY with NO ACTION (default)

- WHEN parsing `CREATE TABLE t (ref_id INTEGER REFERENCES other(id))`
- THEN OnDelete defaults to NO ACTION and OnUpdate defaults to NO ACTION

### Requirement: UNION BY NAME Parsing

The parser SHALL parse `UNION [ALL] BY NAME` as a set operation variant that matches columns by name.

#### Scenario: UNION BY NAME

- WHEN parsing `SELECT a, b FROM t1 UNION BY NAME SELECT b, c FROM t2`
- THEN the parser produces a SelectStmt with SetOp=SetOpUnionByName

#### Scenario: UNION ALL BY NAME

- WHEN parsing `SELECT a FROM t1 UNION ALL BY NAME SELECT b FROM t2`
- THEN the parser produces a SelectStmt with SetOp=SetOpUnionAllByName

#### Scenario: Chained UNION BY NAME

- WHEN parsing `SELECT a FROM t1 UNION BY NAME SELECT b FROM t2 UNION BY NAME SELECT c FROM t3`
- THEN the parser produces a left-associative chain of SetOpUnionByName operations

### Requirement: ON CONFLICT Clause Parsing

The parser SHALL parse the `ON CONFLICT` clause in INSERT statements, supporting both `DO NOTHING` and `DO UPDATE SET` actions with optional conflict target columns and WHERE filters.

#### Scenario: INSERT ... ON CONFLICT DO NOTHING without conflict columns

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT DO NOTHING`
- THEN the parser produces an InsertStmt with OnConflict.Action = OnConflictDoNothing
- AND OnConflict.ConflictColumns is empty (infer from PK)

#### Scenario: INSERT ... ON CONFLICT (columns) DO NOTHING

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING`
- THEN the parser produces an InsertStmt with OnConflict.ConflictColumns = ["id"]
- AND OnConflict.Action = OnConflictDoNothing

#### Scenario: INSERT ... ON CONFLICT DO UPDATE SET

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN the parser produces an InsertStmt with OnConflict.Action = OnConflictDoUpdate
- AND OnConflict.UpdateSet contains one SetClause mapping "name" to a column ref of "EXCLUDED"."name"

#### Scenario: INSERT ... ON CONFLICT DO UPDATE SET with WHERE

- WHEN parsing `INSERT INTO t (id, val) VALUES (1, 10) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE EXCLUDED.val > t.val`
- THEN the parser produces an InsertStmt with OnConflict.UpdateWhere containing the comparison expression
- AND the WHERE filter references both EXCLUDED and the target table

#### Scenario: INSERT ... ON CONFLICT with multiple conflict columns

- WHEN parsing `INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a, b) DO NOTHING`
- THEN OnConflict.ConflictColumns = ["a", "b"]

#### Scenario: INSERT ... SELECT ... ON CONFLICT

- WHEN parsing `INSERT INTO t SELECT * FROM source ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`
- THEN the parser produces an InsertStmt with both a Select and an OnConflict clause

#### Scenario: ON CONFLICT with RETURNING

- WHEN parsing `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING *`
- THEN the parser produces an InsertStmt with both OnConflict and Returning populated
- AND ON CONFLICT is parsed before RETURNING

#### Scenario: ON CONFLICT conflict target WHERE (partial index)

- WHEN parsing `INSERT INTO t VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING`
- THEN OnConflict.ConflictWhere contains the predicate `id > 0`
- AND OnConflict.Action = OnConflictDoNothing

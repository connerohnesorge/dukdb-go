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


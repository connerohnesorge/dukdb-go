# Change: Add Advanced SQL Features

## Why

DuckDB v1.4.3 provides a rich set of advanced SQL features essential for analytical workloads, including PIVOT/UNPIVOT operations, GROUPING SETS/ROLLUP/CUBE for multi-dimensional aggregation, RECURSIVE CTEs for hierarchical queries, LATERAL joins for correlated subqueries, and other SQL enhancements like DISTINCT ON, QUALIFY, and SAMPLE. The current dukdb-go implementation is missing these features, which limits its utility for data analysis scenarios and creates compatibility gaps with the official DuckDB driver.

Without these features:
1. Data transformation workflows requiring pivoting/unpivoting cannot be expressed natively
2. Complex analytical queries with multiple grouping dimensions require workarounds
3. Hierarchical data queries (organizational charts, bill of materials) cannot use recursive CTEs
4. Correlated subqueries in FROM clauses are not supported
5. Modern SQL filtering patterns (QUALIFY) are unavailable
6. MERGE INTO statements are detected but not executed
7. DML statements cannot return modified rows

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

**1. PIVOT/UNPIVOT Operations**
- Add `PivotStmt` and `UnpivotStmt` AST nodes for parsing
- Implement PIVOT transformation into GROUP BY with conditional aggregation
- Implement UNPIVOT as a table function operation

**2. GROUPING SETS, ROLLUP, CUBE**
- Add parsing for `GROUP BY GROUPING SETS (...)`, `ROLLUP(...)`, `CUBE(...)`
- Add `GroupingSetExpr` expression type to represent grouping sets
- Extend `PhysicalHashAggregate` to handle multiple grouping sets
- Implement `GROUPING()` function to identify grouping set membership

**3. RECURSIVE CTE**
- Add `Recursive` flag to CTE parsing
- Add recursive CTE resolution algorithm in binder
- Implement `PhysicalRecursiveCTE` plan node
- Implement iterative execution with work table

**4. LATERAL Joins**
- Add `Lateral` flag to `TableRef` structure
- Implement lateral correlation resolution in binder
- Add `PhysicalLateralJoin` plan node
- Implement correlated subquery execution in FROM clause

**5. Additional SELECT Clauses**
- **DISTINCT ON**: Parse and implement using LIMIT 1 per unique key
- **QUALIFY**: Add filtering after window function evaluation
- **SAMPLE**: Implement reservoir sampling algorithm

**6. MERGE INTO Execution**
- Implement `PhysicalMerge` plan node
- Execute MERGE using HashJoin + conditional UPDATE/INSERT
- Support `WHEN MATCHED THEN UPDATE/DELETE`
- Support `WHEN NOT MATCHED THEN INSERT`

**7. RETURNING Clause**
- Add `ReturningCols` field to DML AST nodes
- Include returned rows in result set for INSERT/UPDATE/DELETE/MERGE

### Internal Changes

- New `internal/parser/ast_pivot.go` for PIVOT/UNPIVOT AST nodes
- New `internal/parser/ast_grouping.go` for GROUPING SETS/ROLLUP/CUBE expressions
- New `internal/executor/physical_pivot.go` for PIVOT operator
- New `internal/executor/physical_recursive_cte.go` for recursive CTE execution
- New `internal/executor/physical_lateral.go` for LATERAL join execution
- New `internal/executor/physical_merge.go` for MERGE execution
- Updated `internal/parser/ast.go` with DISTINCT ON, QUALIFY, SAMPLE fields
- Updated `internal/planner/physical.go` with new plan nodes
- Updated `internal/binder/bind_expr.go` for GROUPING() function

## Impact

### Affected Specs

- `parser`: New AST nodes for PIVOT, UNPIVOT, GROUPING SETS, DISTINCT ON, QUALIFY, SAMPLE
- `execution-engine`: New physical operators for pivot, recursive CTE, lateral join, merge
- `binder`: Recursive CTE resolution, lateral correlation, grouping set binding
- `planner`: New plan nodes for advanced SQL features

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/parser/ast.go` | MODIFIED | Add DistinctOn, Qualify, Sample fields to SelectStmt |
| `internal/parser/ast_pivot.go` | ADDED | PivotStmt, UnpivotStmt AST nodes |
| `internal/parser/ast_grouping.go` | ADDED | GroupingSetExpr, RollupExpr, CubeExpr types |
| `internal/parser/visitor.go` | MODIFIED | Add visitor methods for new nodes |
| `internal/binder/bind_stmt.go` | MODIFIED | Handle recursive CTEs, MERGE binding |
| `internal/binder/bind_expr.go` | MODIFIED | Add GROUPING() function binding |
| `internal/binder/bind_table_ref.go` | MODIFIED | Handle LATERAL correlations |
| `internal/planner/physical.go` | ADDED | PhysicalPivot, PhysicalRecursiveCTE, PhysicalLateral, PhysicalMerge |
| `internal/executor/physical_aggregate.go` | MODIFIED | Handle grouping sets, GROUPING() function |
| `internal/executor/physical_pivot.go` | ADDED | PIVOT operator implementation |
| `internal/executor/physical_recursive_cte.go` | ADDED | Recursive CTE execution |
| `internal/executor/physical_lateral.go` | ADDED | LATERAL join execution |
| `internal/executor/physical_merge.go` | ADDED | MERGE statement execution |
| `stmt_type.go` | MODIFIED | Add STATEMENT_TYPE_PIVOT, STATEMENT_TYPE_UNPIVOT |

### Dependencies

- This proposal depends on: (none)
- This proposal blocks: (none)

### Compatibility

- Full compatibility with DuckDB SQL syntax for advanced features
- API-compatible with duckdb-go for all existing operations
- All new features match DuckDB v1.4.3 behavior

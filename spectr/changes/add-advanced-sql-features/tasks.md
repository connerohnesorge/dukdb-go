## 1. Parser Changes

### 1.1 Add PIVOT/UNPIVOT AST Nodes
- [ ] 1.1.1 Create `internal/parser/ast_pivot.go` with `PivotStmt` struct
- [ ] 1.1.2 Add `PivotAggregate` struct for aggregate specifications
- [ ] 1.1.3 Create `UnpivotStmt` struct for UNPIVOT operations
- [ ] 1.1.4 Add `PivotRef` field to `TableRef` struct
- [ ] 1.1.5 Update `visitor.go` with `VisitPivotStmt` and `VisitUnpivotStmt` methods
- [ ] 1.1.6 Add statement type constants `STATEMENT_TYPE_PIVOT`, `STATEMENT_TYPE_UNPIVOT` in `backend.go`

### 1.2 Add GROUPING SETS/ROLLUP/CUBE Expressions
- [ ] 1.2.1 Create `internal/parser/ast_grouping.go` with `GroupingSetExpr` struct
- [ ] 1.2.2 Add `GroupingSetType` constants (Simple, Rollup, Cube)
- [ ] 1.2.3 Add `RollupExpr` and `CubeExpr` wrapper types
- [ ] 1.2.4 Update `SelectStmt.GroupBy` to accept `GroupingSetExpr`
- [ ] 1.2.5 Update `visitor.go` with `VisitGroupingSetExpr` method

### 1.3 Add RECURSIVE CTE Support
- [ ] 1.3.1 Add `Recursive` bool field to `CTE` struct in `ast.go`
- [ ] 1.3.2 Add `RecursiveCTESet` field to `SelectStmt` for WITH RECURSIVE

### 1.4 Add LATERAL Join Support
- [ ] 1.4.1 Add `Lateral` bool field to `TableRef` struct in `ast.go`

### 1.5 Add Additional SELECT Clauses
- [ ] 1.5.1 Add `DistinctOn []Expr` field to `SelectStmt` for DISTINCT ON
- [ ] 1.5.2 Add `Qualify Expr` field to `SelectStmt` for QUALIFY clause
- [ ] 1.5.3 Create `SampleOptions` struct with Method, Percentage, Rows, Seed fields
- [ ] 1.5.4 Add `Sample *SampleOptions` field to `SelectStmt`

### 1.6 Add MERGE INTO AST Support
- [ ] 1.6.1 Create `MergeStmt` struct with Into, Using, On, WhenMatched, WhenNotMatched fields
- [ ] 1.6.2 Add `MergeAction` struct with Type, Cond, Update, Insert fields
- [ ] 1.6.3 Add `MergeActionType` constants (Update, Delete, Insert)

### 1.7 Add RETURNING Clause Support
- [ ] 1.7.1 Add `ReturningCols []SelectColumn` field to `InsertStmt`
- [ ] 1.7.2 Add `ReturningCols []SelectColumn` field to `UpdateStmt`
- [ ] 1.7.3 Add `ReturningCols []SelectColumn` field to `DeleteStmt`
- [ ] 1.7.4 Add `ReturningCols []SelectColumn` field to `MergeStmt`

## 2. Binder Changes

### 2.1 Update CTE Binding for Recursive CTEs
- [ ] 2.1.1 Modify `bindCTE` in `bind_stmt.go` to handle recursive CTEs
- [ ] 2.1.2 Add self-reference detection for recursive CTE validation
- [ ] 2.1.3 Create work table binding for recursive CTE

### 2.2 Add GROUPING SETS/ROLLUP/CUBE Binding
- [ ] 2.2.1 Create `bindGroupingSetExpr` function in `bind_expr.go`
- [ ] 2.2.2 Add binding for `GROUPING()` function
- [ ] 2.2.3 Expand ROLLUP and CUBE into explicit grouping sets

### 2.3 Add LATERAL Correlation Binding
- [ ] 2.3.1 Create `bindLateralTableRef` function in `bind_table_ref.go`
- [ ] 2.3.2 Add correlation resolution for LATERAL subqueries
- [ ] 2.3.3 Handle lateral table function bindings

### 2.4 Add MERGE INTO Binding
- [ ] 2.4.1 Create `bindMerge` function in `bind_stmt.go`
- [ ] 2.4.2 Bind WHEN MATCHED/WHEN NOT MATCHED conditions
- [ ] 2.4.3 Bind UPDATE SET expressions and INSERT values

### 2.5 Add RETURNING Clause Binding
- [ ] 2.5.1 Modify `bindInsert`, `bindUpdate`, `bindDelete` to handle RETURNING
- [ ] 2.5.2 Add RETURNING column resolution

## 3. Planner Changes

### 3.1 Add PIVOT/UNPIVOT Planning
- [ ] 3.1.1 Create `LogicalPivot` and `LogicalUnpivot` nodes in `logical.go`
- [ ] 3.1.2 Create `PhysicalPivot` and `PhysicalUnpivot` nodes in `physical.go`
- [ ] 3.1.3 Implement `planPivot` transformation to conditional aggregation

### 3.2 Add GROUPING SETS/ROLLUP/CUBE Planning
- [ ] 3.2.1 Modify `planAggregate` to handle grouping sets
- [ ] 3.2.2 Expand grouping sets into multiple aggregate passes if needed
- [ ] 3.2.3 Add GROUPING function to aggregate output

### 3.3 Add RECURSIVE CTE Planning
- [ ] 3.3.1 Create `LogicalRecursiveCTE` node in `logical.go`
- [ ] 3.3.2 Create `PhysicalRecursiveCTE` node in `physical.go`
- [ ] 3.3.3 Implement `planRecursiveCTE` to create iterative plan

### 3.4 Add LATERAL Join Planning
- [ ] 3.4.1 Create `LogicalLateralJoin` node in `logical.go`
- [ ] 3.4.2 Create `PhysicalLateralJoin` node in `physical.go`
- [ ] 3.4.3 Implement `planLateralJoin` for correlated subqueries

### 3.5 Add MERGE INTO Planning
- [ ] 3.5.1 Create `LogicalMerge` node in `logical.go`
- [ ] 3.5.2 Create `PhysicalMerge` node in `physical.go`
- [ ] 3.5.3 Implement `planMerge` using HashJoin for matching

## 4. Executor Changes

### 4.1 Implement PIVOT Operator
- [ ] 4.1.1 Create `internal/executor/physical_pivot.go`
- [ ] 4.1.2 Implement `PhysicalPivotOperator` with conditional aggregation
- [ ] 4.1.3 Implement `PhysicalUnpivotOperator` as table function

### 4.2 Extend Aggregate for GROUPING SETS
- [ ] 4.2.1 Modify `PhysicalAggregateOperator` to handle grouping sets
- [ ] 4.2.2 Implement GROUPING() function evaluation
- [ ] 4.2.3 Add null handling for grouping set expansion

### 4.3 Implement RECURSIVE CTE Execution
- [ ] 4.3.1 Create `internal/executor/physical_recursive_cte.go`
- [ ] 4.3.2 Implement `PhysicalRecursiveCTEOperator` with work table
- [ ] 4.3.3 Add MAX RECURSION termination condition

### 4.4 Implement LATERAL Join Execution
- [ ] 4.4.1 Create `internal/executor/physical_lateral.go`
- [ ] 4.4.2 Implement `PhysicalLateralJoinOperator`
- [ ] 4.4.3 Handle correlated subquery re-evaluation

### 4.5 Implement MERGE INTO Execution
- [ ] 4.5.1 Create `internal/executor/physical_merge.go`
- [ ] 4.5.2 Implement `PhysicalMergeOperator` using HashJoin
- [ ] 4.5.3 Handle WHEN MATCHED UPDATE/DELETE
- [ ] 4.5.4 Handle WHEN NOT MATCHED INSERT

### 4.6 Implement RETURNING Support
- [ ] 4.6.1 Modify INSERT execution to return rows
- [ ] 4.6.2 Modify UPDATE execution to return modified rows
- [ ] 4.6.3 Modify DELETE execution to return deleted rows

### 4.7 Implement DISTINCT ON
- [ ] 4.7.1 Modify `planSelect` to handle DISTINCT ON
- [ ] 4.7.2 Implement DISTINCT ON using Sort + First aggregate

### 4.8 Implement QUALIFY Support
- [ ] 4.8.1 Modify `planSelect` to add QUALIFY filter after window
- [ ] 4.8.2 Implement QUALIFY evaluation after window functions

### 4.9 Implement SAMPLE Clause
- [ ] 4.9.1 Create `internal/executor/sample.go` with reservoir sampling
- [ ] 4.9.2 Implement BERNOULLI, SYSTEM, RESERVOIR sampling methods
- [ ] 4.9.3 Integrate sampling into query execution flow

## 5. Testing

### 5.1 Parser Tests
- [ ] 5.1.1 Add parser tests for PIVOT syntax
- [ ] 5.1.2 Add parser tests for UNPIVOT syntax
- [ ] 5.1.3 Add parser tests for GROUPING SETS/ROLLUP/CUBE
- [ ] 5.1.4 Add parser tests for RECURSIVE CTE
- [ ] 5.1.5 Add parser tests for LATERAL joins
- [ ] 5.1.6 Add parser tests for DISTINCT ON, QUALIFY, SAMPLE
- [ ] 5.1.7 Add parser tests for MERGE INTO
- [ ] 5.1.8 Add parser tests for RETURNING clause

### 5.2 Binder Tests
- [ ] 5.2.1 Add binding tests for recursive CTEs
- [ ] 5.2.2 Add binding tests for grouping sets
- [ ] 5.2.3 Add binding tests for LATERAL correlations
- [ ] 5.2.4 Add binding tests for MERGE INTO

### 5.3 Executor Tests
- [ ] 5.3.1 Add execution tests for PIVOT operations
- [ ] 5.3.2 Add execution tests for GROUPING SETS
- [ ] 5.3.3 Add execution tests for recursive CTEs
- [ ] 5.3.4 Add execution tests for LATERAL joins
- [ ] 5.3.5 Add execution tests for MERGE INTO
- [ ] 5.3.6 Add execution tests for RETURNING clause
- [ ] 5.3.7 Add execution tests for SAMPLE clause

### 5.4 Integration Tests
- [ ] 5.4.1 Add compatibility tests against DuckDB reference
- [ ] 5.4.2 Add integration tests for complex analytical queries

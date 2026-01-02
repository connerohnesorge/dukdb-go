## Context

This design document provides detailed technical specifications for implementing advanced SQL features in dukdb-go. The implementation must maintain:
- Pure Go implementation (no CGO)
- API compatibility with duckdb-go
- Vectorized execution model
- Memory efficiency for analytical workloads

## Goals / Non-Goals

### Goals
1. Implement PIVOT/UNPIVOT operations as native SQL syntax
2. Support GROUPING SETS, ROLLUP, CUBE for multi-dimensional aggregation
3. Implement RECURSIVE CTEs with iterative execution
4. Support LATERAL joins for correlated subqueries in FROM clause
5. Add DISTINCT ON, QUALIFY, SAMPLE clauses
6. Complete MERGE INTO implementation
7. Add RETURNING clause for DML statements

### Non-Goals
- Window function frame extensions (already partially implemented)
- Full recursive UNION ALL beyond CTE context
- Distributed query execution
- Materialized view maintenance

## Decisions

### 1. PIVOT Implementation Strategy

**Decision**: Transform PIVOT into GROUP BY with conditional aggregation

**Rationale**: This approach leverages existing aggregate infrastructure and handles null values correctly.

**Implementation**:
```
PIVOT src ON pivot_col USING agg(expr) GROUP BY group_col
→
SELECT group_col, pivot_col, SUM(CASE WHEN pivot_col = val1 THEN expr END) AS val1, ...
FROM src
GROUP BY group_col
```

**AST Structure** (`internal/parser/ast_pivot.go`):
```go
type PivotStmt struct {
    Source       TableRef
    PivotOn      []Expr
    Using        []PivotAggregate
    GroupBy      []Expr
    Alias        string
    ColumnPrefix string
}

type PivotAggregate struct {
    Function  string // "SUM", "COUNT", etc.
    Expr      Expr
    Alias     string
}

type UnpivotStmt struct {
    Source    TableRef
    Into      string        // Column name for unpivoted values
    For       string        // Column name for pivot column names
    Using     []string      // Columns to unpivot
    Alias     string
}
```

### 2. GROUPING SETS Implementation Strategy

**Decision**: Expand grouping sets into multiple aggregation passes and union results

**Rationale**: Simplifies implementation by reusing existing aggregate operator.

**AST Structure** (`internal/parser/ast_grouping.go`):
```go
type GroupingSetType int

const (
    GroupingSetSimple GroupingSetType = iota
    GroupingSetRollup
    GroupingSetCube
)

type GroupingSetExpr struct {
    Type  GroupingSetType
    Exprs [][]Expr // Each inner slice is one grouping set
}

func (e *GroupingSetExpr) exprNode() {}
```

**Executor Logic** (`internal/executor/physical_aggregate.go` extension):
```go
type GroupingSet struct {
    Columns   []int  // Column indices for this grouping set
    GroupMask uint64 // Bitmask for GROUPING() function
}

type PhysicalHashAggregate struct {
    Child           PhysicalPlan
    GroupBy         []binder.BoundExpr
    Aggregates      []binder.BoundExpr
    GroupingSets    []GroupingSet // Expanded grouping sets
    GroupingFuncs   []int         // Indices of GROUPING() functions
    // ... existing fields
}
```

### 3. RECURSIVE CTE Implementation Strategy

**Decision**: Iterative fixpoint algorithm with work table

**Rationale**: Standard approach for recursive queries, compatible with SQL standard.

**Plan Node** (`internal/planner/physical.go`):
```go
type PhysicalRecursiveCTE struct {
    CTEName     string
    LeftPlan    PhysicalPlan  // Non-recursive part (UNION ALL before recursive)
    RightPlan   PhysicalPlan  // Recursive part (references CTE)
    WorkTable   *WorkTableDef
    MaxRecursion int          // 0 = unlimited
    columns     []ColumnBinding
}

type WorkTableDef struct {
    Name    string
    Columns []ColumnBinding
}
```

**Executor** (`internal/executor/physical_recursive_cte.go`):
```go
type WorkTable struct {
    Name    string
    Chunks  []*storage.DataChunk
    Columns []ColumnBinding
}

type PhysicalRecursiveCTEOperator struct {
    cte         *PhysicalRecursiveCTE
    executor    *Executor
    ctx         *ExecutionContext
    workTable   *WorkTable
    iteration   int
    finished    bool
}

func (op *PhysicalRecursiveCTEOperator) Next() (*storage.DataChunk, error) {
    // Phase 1: Execute non-recursive part
    // Phase 2: Iteratively execute recursive part until no new rows
    // Phase 3: Return combined results
}
```

### 4. LATERAL Join Implementation Strategy

**Decision**: Correlated subquery execution with re-evaluation per row

**Rationale**: Matches SQL standard semantics and handles arbitrary correlations.

**AST Addition** (`internal/parser/ast.go` - TableRef):
```go
type TableRef struct {
    // ... existing fields
    Lateral bool // NEW: true for LATERAL subqueries
}
```

**Plan Node** (`internal/planner/physical.go`):
```go
type PhysicalLateralJoin struct {
    Left         PhysicalPlan
    Right        PhysicalPlan // Correlated subquery
    Correlation  []CorrelationRef // Column correlations
    JoinType     JoinType
    columns      []ColumnBinding
}

type CorrelationRef struct {
    OuterColumn ColumnBinding
    InnerColumn ColumnBinding
}
```

**Executor** (`internal/executor/physical_lateral.go`):
```go
type PhysicalLateralJoinOperator struct {
    join    *PhysicalLateralJoin
    left    PhysicalOperator
    right   PhysicalOperator // Re-initialized per left row
    executor *Executor
    ctx     *ExecutionContext
}
```

### 5. DISTINCT ON Implementation Strategy

**Decision**: Sort by DISTINCT ON columns, then LIMIT 1 per key

**Rationale**: Simple and efficient, leverages existing infrastructure.

**AST Addition** (`internal/parser/ast.go` - SelectStmt):
```go
type SelectStmt struct {
    // ... existing fields
    DistinctOn []Expr // NEW: Columns for DISTINCT ON
}
```

**Implementation**: Parse DISTINCT ON, transform to:
```sql
SELECT DISTINCT ON (col1, col2) col1, col2, col3 FROM t
→
SELECT * FROM (
    SELECT * FROM t ORDER BY col1, col2
) GROUP BY col1, col2
```

### 6. QUALIFY Implementation Strategy

**Decision**: Filter after window function evaluation

**Rationale**: Matches SQL:2003 standard behavior.

**AST Addition** (`internal/parser/ast.go` - SelectStmt):
```go
type SelectStmt struct {
    // ... existing fields
    Qualify Expr // NEW: Filter after window functions
}
```

**Planner transformation**:
```go
plan := PhysicalFilter{
    Child: PhysicalWindow{...},
    Condition: bound.Qualify,
}
```

### 7. SAMPLE Implementation Strategy

**Decision**: Reservoir sampling algorithm

**Rationale**: Provides statistically representative sampling.

**AST Addition** (`internal/parser/ast.go` - SelectStmt):
```go
type SampleOptions struct {
    Method       SampleMethod // BERNOULLI, SYSTEM, RESERVOIR
    Percentage   float64      // For BERNOULLI/SYSTEM
    Rows         int          // For RESERVOIR
    Seed         int          // Optional seed for reproducibility
}

type SelectStmt struct {
    // ... existing fields
    Sample *SampleOptions // NEW
}

type SampleMethod int

const (
    SampleBernoulli SampleMethod = iota
    SampleSystem
    SampleReservoir
)
```

### 8. MERGE INTO Implementation Strategy

**Decision**: HashJoin + conditional UPDATE/INSERT

**Rationale**: Efficient matching, leverages existing join infrastructure.

**AST** (`internal/parser/ast.go`):
```go
type MergeStmt struct {
    Into        TableRef
    Using       TableRef
    On          Expr
    WhenMatched []MergeAction
    WhenNotMatched []MergeAction
}

type MergeAction struct {
    Type    MergeActionType // UPDATE, DELETE, INSERT
    Cond    Expr            // Optional additional condition
    Update  []SetClause     // For UPDATE
    Insert  []SetClause     // For INSERT
}

type MergeActionType int

const (
    MergeActionUpdate MergeActionType = iota
    MergeActionDelete
    MergeActionInsert
)
```

**Plan Node** (`internal/planner/physical.go`):
```go
type PhysicalMerge struct {
    Target      PhysicalPlan
    Source      PhysicalPlan
    OnCondition binder.BoundExpr
    Matched     []PhysicalMergeAction
    NotMatched  PhysicalMergeAction
    columns     []ColumnBinding
}

type PhysicalMergeAction struct {
    Type   MergeActionType
    Cond   binder.BoundExpr // May be nil
    Set    []binder.BoundExpr
}
```

**Executor** (`internal/executor/physical_merge.go`):
```go
type PhysicalMergeOperator struct {
    merge       *PhysicalMerge
    targetScan  PhysicalOperator
    sourceScan  PhysicalOperator
    hashJoin    *PhysicalHashJoin
    executor    *Executor
    ctx         *ExecutionContext
    rowIds      []storage.RowID
}
```

### 9. RETURNING Clause Implementation Strategy

**Decision**: Modify DML execution to return modified rows

**AST Addition** (`internal/parser/ast.go`):
```go
type InsertStmt struct {
    // ... existing fields
    Returning []SelectColumn // NEW
}

type UpdateStmt struct {
    // ... existing fields
    Returning []SelectColumn // NEW
}

type DeleteStmt struct {
    // ... existing fields
    Returning []SelectColumn // NEW
}

type MergeStmt struct {
    // ... existing fields
    Returning []SelectColumn // NEW
}
```

**Executor Changes**:
- INSERT: Return generated row IDs and selected columns
- UPDATE: Return modified row IDs and columns
- DELETE: Return deleted row IDs and columns (before deletion)

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| Recursive CTE infinite loops | Add MaxRecursion counter |
| Memory explosion in LATERAL | Limit correlation scope, add warnings |
| GROUPING SETS performance | Optimize with single-pass algorithm |
| PIVOT column explosion | Add column count limits |

## Migration Plan

1. Add new AST nodes and parser support (non-breaking)
2. Add binder resolution for new constructs
3. Add planner transformations
4. Add executor implementations
5. Add integration tests against DuckDB reference

## Open Questions

1. Should we support UPDATE/DELETE with table functions as source?
2. How to handle PIVOT column ordering for consistent output?
3. Should SAMPLE be applied before or after LIMIT/OFFSET?

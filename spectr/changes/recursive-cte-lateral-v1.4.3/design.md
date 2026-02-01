# Design Document: Recursive CTEs and Lateral Joins Implementation

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Recursive CTE Design](#recursive-cte-design)
4. [Lateral Join Design](#lateral-join-design)
5. [Data Structures](#data-structures)
6. [Algorithms](#algorithms)
7. [Performance Optimizations](#performance-optimizations)
8. [Memory Management](#memory-management)
9. [Integration Points](#integration-points)
10. [Testing Strategy](#testing-strategy)
11. [Error Handling](#error-handling)

## Overview

This document provides detailed technical design for implementing recursive Common Table Expressions (CTEs) and Lateral Joins in dukdb-go. The implementation aims to achieve full DuckDB v1.4.3 compatibility while maintaining the project's zero-CGO constraint and pure Go architecture.

### Key Design Principles

1. **Modularity**: Clear separation between parsing, planning, and execution phases
2. **Performance**: Optimized for both shallow and deep recursion scenarios
3. **Memory Efficiency**: Bounded memory usage with streaming execution
4. **Extensibility**: Design allows for future enhancements like parallel execution
5. **Compatibility**: Exact DuckDB behavior replication

## Architecture

### High-Level Architecture

```
SQL Query
    ↓
Parser (with RECURSIVE/LATERAL support)
    ↓
Binder (CTE scope resolution, lateral correlation)
    ↓
Planner (Recursive CTE planning, lateral join placement)
    ↓
Optimizer (USING KEY optimization, join ordering)
    ↓
Executor (Recursive execution engine, lateral join operator)
    ↓
Results
```

### Component Interaction

```go
// Core interfaces
type RecursiveCTEPlanner interface {
    PlanRecursiveCTE(cte *ast.RecursiveCTE) (plan.PhysicalOperator, error)
}

type LateralJoinPlanner interface {
    PlanLateralJoin(join *ast.LateralJoin) (plan.PhysicalOperator, error)
}

type RecursiveExecutor interface {
    ExecuteRecursiveCTE(plan *plan.RecursiveCTEPlan) (exec.Operator, error)
}

type LateralJoinExecutor interface {
    ExecuteLateralJoin(plan *plan.LateralJoinPlan) (exec.Operator, error)
}
```

## Recursive CTE Design

### Parser Extensions

```go
// internal/parser/ast/cte.go
type RecursiveCTE struct {
    Name       string
    Columns    []string
    Anchor     SelectStatement    // Non-recursive base case
    Recursive  SelectStatement    // Recursive member
    UnionType  UnionType          // UNION ALL (initially)
    UsingKey   *UsingKeyClause    // Optional optimization hint
    IsCycle    bool               // Cycle detection enabled
    CycleCol   string             // Cycle detection column
}

type UsingKeyClause struct {
    Columns []string
    Table   string
}
```

### Planner Design

```go
// internal/planner/recursive_cte.go
type RecursiveCTEPlan struct {
    Name          string
    WorkTable     string
    AnchorPlan    plan.PhysicalOperator
    RecursivePlan plan.PhysicalOperator
    UnionAll      bool
    UsingKey      *UsingKeySpec
    CycleCheck    *CycleDetectionSpec
    MaxIterations int
}

type UsingKeySpec struct {
    KeyColumns    []string
    KeyTable      string
    Dictionary    *HashDictionary
    UpdateOnMatch bool
}
```

### Execution Model

The recursive CTE execution follows the standard iterative approach:

1. **Initialization Phase**: Execute anchor query and store results in work table
2. **Iteration Phase**: Repeatedly execute recursive query using previous iteration's results
3. **Termination Phase**: Stop when no new rows are produced or cycle/max depth detected

```go
// internal/executor/recursive_cte.go
type RecursiveCTEOperator struct {
    ctx           context.Context
    anchorOp      exec.Operator
    recursiveOp   exec.Operator
    workTable     *storage.DataTable
    resultTable   *storage.DataTable
    usingKeyDict  *UsingKeyDictionary
    iteration     int
    maxIterations int
    cycleDetector *CycleDetector
}

func (r *RecursiveCTEOperator) Next() (*storage.DataChunk, error) {
    // Iterative execution logic
    for r.iteration < r.maxIterations {
        // Execute recursive query on current work table
        newRows, err := r.executeRecursiveIteration()
        if err != nil {
            return nil, err
        }

        if newRows.Count() == 0 {
            break // No more rows, terminate
        }

        // Check for cycles if enabled
        if r.cycleDetector != nil {
            if hasCycle := r.cycleDetector.Detect(newRows); hasCycle {
                break
            }
        }

        // Apply USING KEY optimization if enabled
        if r.usingKeyDict != nil {
            newRows = r.usingKeyDict.FilterAndUpdate(newRows)
            if newRows.Count() == 0 {
                break
            }
        }

        // Update work table for next iteration
        r.workTable.Append(newRows)
        r.iteration++
    }

    return r.resultTable.ReadChunk(), nil
}
```

### USING KEY Optimization

The USING KEY optimization is crucial for graph algorithms. It maintains a dictionary of "best" rows seen so far and filters out inferior rows early.

```go
// internal/executor/using_key.go
type UsingKeyDictionary struct {
    keyColumns    []int
    valueColumn   int
    compareOp     CompareOperation
    entries       map[string]*DictionaryEntry
    mutex         sync.RWMutex
}

type DictionaryEntry struct {
    key       string
    bestValue interface{}
    rowID     int64
}

func (d *UsingKeyDictionary) FilterAndUpdate(chunk *storage.DataChunk) *storage.DataChunk {
    d.mutex.Lock()
    defer d.mutex.Unlock()

    result := storage.NewDataChunk(chunk.ColumnCount(), chunk.Count())

    for rowIdx := 0; rowIdx < chunk.Count(); rowIdx++ {
        key := d.extractKey(chunk, rowIdx)
        value := d.extractValue(chunk, rowIdx)

        entry, exists := d.entries[key]
        if !exists || d.compareOp(value, entry.bestValue) {
            // New best value for this key
            d.entries[key] = &DictionaryEntry{
                key:       key,
                bestValue: value,
                rowID:     int64(rowIdx),
            }
            result.Append(chunk.GetRow(rowIdx))
        }
    }

    return result
}
```

### Cycle Detection

Cycle detection prevents infinite recursion in malformed queries.

```go
// internal/executor/cycle_detector.go
type CycleDetector struct {
    pathColumn    string
    visited       map[string]bool
    currentPath   []string
}

func (c *CycleDetector) Detect(rows *storage.DataChunk) bool {
    for i := 0; i < rows.Count(); i++ {
        pathValue := rows.GetValue(c.pathColumn, i)
        pathStr := fmt.Sprintf("%v", pathValue)

        if c.visited[pathStr] {
            // Cycle detected
            return true
        }

        c.visited[pathStr] = true
        c.currentPath = append(c.currentPath, pathStr)
    }

    return false
}
```

## Lateral Join Design

### Parser Extensions

```go
// internal/parser/ast/join.go
type LateralJoin struct {
    Type        JoinType        // INNER, LEFT, etc.
    Left        TableExpression
    Right       *LateralSubquery
    Condition   Expression
}

type LateralSubquery struct {
    Subquery  SelectStatement
    Lateral   bool
}
```

### Planner Design

```go
// internal/planner/lateral_join.go
type LateralJoinPlan struct {
    JoinType      JoinType
    LeftPlan      plan.PhysicalOperator
    RightPlan     plan.PhysicalOperator
    Correlation   *CorrelationSpec
    Predicate     expression.Expression
}

type CorrelationSpec struct {
    OuterColumns []string
    InnerColumns []string
    Mapping      map[string]int
}
```

### Execution Model

Lateral joins execute the right-hand subquery once for each row from the left input, passing outer references.

```go
// internal/executor/lateral_join.go
type LateralJoinOperator struct {
    ctx           context.Context
    leftOp        exec.Operator
    rightOp       exec.Operator
    correlation   *CorrelationSpec
    joinType      JoinType
    predicate     expression.Expression
    currentChunk  *storage.DataChunk
    currentRow    int
    subqueryOp    exec.Operator
}

func (l *LateralJoinOperator) Next() (*storage.DataChunk, error) {
    result := storage.NewDataChunk(l.outputColumnCount(), 0)

    for result.Count() < storage.STANDARD_VECTOR_SIZE {
        // Get next row from left input if needed
        if l.currentRow >= l.currentChunk.Count() {
            l.currentChunk, _ = l.leftOp.Next()
            if l.currentChunk == nil || l.currentChunk.Count() == 0 {
                break
            }
            l.currentRow = 0
        }

        // Execute subquery for current row
        row := l.currentChunk.GetRow(l.currentRow)
        subqueryResult, err := l.executeSubqueryForRow(row)
        if err != nil {
            return nil, err
        }

        // Apply join predicate and add to result
        for i := 0; i < subqueryResult.Count(); i++ {
            if l.satisfiesPredicate(row, subqueryResult.GetRow(i)) {
                joinedRow := l.combineRows(row, subqueryResult.GetRow(i))
                result.Append(joinedRow)
            }
        }

        l.currentRow++
    }

    return result, nil
}

func (l *LateralJoinOperator) executeSubqueryForRow(outerRow *storage.Row) (*storage.DataChunk, error) {
    // Create correlated subquery operator with outer references
    correlatedOp := l.createCorrelatedOperator(outerRow)

    // Execute subquery
    var result *storage.DataChunk
    for {
        chunk, err := correlatedOp.Next()
        if err != nil {
            return nil, err
        }
        if chunk == nil || chunk.Count() == 0 {
            break
        }
        if result == nil {
            result = chunk
        } else {
            result.Append(chunk)
        }
    }

    return result, nil
}
```

## Data Structures

### Work Table Management

```go
// internal/storage/work_table.go
type WorkTable struct {
    name        string
    schema      *storage.Schema
    chunks      []*storage.DataChunk
    rowCount    int64
    memoryLimit int64
    spillPath   string
}

func (w *WorkTable) Append(chunk *storage.DataChunk) error {
    if w.memoryUsage() > w.memoryLimit {
        return w.spillToDisk()
    }

    w.chunks = append(w.chunks, chunk)
    w.rowCount += int64(chunk.Count())

    return nil
}
```

### Recursive State Management

```go
// internal/executor/recursive_state.go
type RecursiveState struct {
    iteration     int
    workTable     *WorkTable
    resultTable   *WorkTable
    usingKeyDict  *UsingKeyDictionary
    cycleDetector *CycleDetector

    // Statistics
    rowsProduced  int64
    rowsFiltered  int64
    maxDepth      int
}
```

## Algorithms

### Recursive CTE Algorithm

```
Algorithm: ExecuteRecursiveCTE
Input: RecursiveCTEPlan plan
Output: Iterator over result rows

1. Initialize
   - Create work table WT
   - Create result table RT
   - Initialize iteration = 0

2. Execute Anchor
   - Execute plan.AnchorPlan
   - Store results in WT
   - Append to RT

3. While WT has rows and iteration < max
   a. Create temp table TT
   b. For each row in WT
      - Execute plan.RecursivePlan with row
      - If USING KEY enabled
         - Apply using key filter
      - If Cycle detection enabled
         - Check for cycles
      - Store new rows in TT
   c. WT = TT
   d. Append WT to RT
   e. iteration++

4. Return RT.iterator()
```

### Using Key Algorithm

```
Algorithm: UsingKeyFilter
Input: DataChunk newRows, Dictionary dict
Output: Filtered DataChunk

1. result = empty DataChunk
2. For each row in newRows
   a. key = extractKey(row)
   b. value = extractValue(row)
   c. If key not in dict OR value better than dict[key]
      - dict[key] = value
      - Append row to result
3. Return result
```

### Lateral Join Algorithm

```
Algorithm: ExecuteLateralJoin
Input: LateralJoinPlan plan, Operator left, Operator right
Output: Joined rows

1. For each chunk C from left
   a. For each row R in C
      - Create correlated subquery S with R values
      - Execute S
      - For each row SR from S
         - If join predicate satisfied
            - Combine R and SR
            - Output combined row
```

## Performance Optimizations

### 1. USING KEY Dictionary Optimization

- **Hash Table**: O(1) lookup for key existence
- **String Interning**: Reduce memory for repeated keys
- **Batch Updates**: Process chunks rather than individual rows
- **Memory Pooling**: Reuse dictionary entries

### 2. Streaming Execution

- **Chunk-based Processing**: Process data in chunks (2048 rows)
- **Pipeline Parallelism**: Overlap computation stages
- **Early Termination**: Stop when possible (LIMIT, cycle detection)

### 3. Memory Management

- **Spill to Disk**: Automatic spilling when memory limits exceeded
- **Compression**: Compress work tables when memory pressure high
- **Reference Counting**: Share data between iterations when possible

### 4. Join Optimizations

- **Hash Join**: Build hash table for lateral join predicates
- **Bloom Filter**: Filter rows early in lateral join
- **Predicate Pushdown**: Push predicates into subqueries

## Memory Management

### Memory Limits

```go
const (
    DefaultWorkTableMemory = 256 * 1024 * 1024  // 256MB
    MaxRecursionDepth      = 10000
    SpillThreshold         = 0.8  // Spill at 80% memory usage
)
```

### Memory Tracking

```go
type MemoryTracker struct {
    currentUsage int64
    maxUsage     int64
    onExceeded   func() error
}

func (m *MemoryTracker) Allocate(size int64) error {
    newUsage := atomic.AddInt64(&m.currentUsage, size)
    if newUsage > m.maxUsage {
        return m.onExceeded()
    }
    return nil
}
```

### Spill Strategy

1. **Selection**: Spill least recently used work tables
2. **Compression**: Apply Snappy compression before spilling
3. **Async I/O**: Use separate goroutine for spilling
4. **Prefetching**: Preload spilled data when approaching

## Integration Points

### Parser Integration

```go
// internal/parser/parser_cte.go
func (p *Parser) parseWithClause() (*ast.WithClause, error) {
    if p.matchKeyword("RECURSIVE") {
        return p.parseRecursiveCTE()
    }
    return p.parseNonRecursiveCTE()
}
```

### Binder Integration

```go
// internal/binder/binder_cte.go
func (b *Binder) bindRecursiveCTE(cte *ast.RecursiveCTE) error {
    // Create recursive binding context
    recursiveCtx := b.createRecursiveContext(cte.Name)

    // Bind anchor member
    if err := b.bindCTEAnchor(cte.Anchor, recursiveCtx); err != nil {
        return err
    }

    // Bind recursive member with self-reference
    if err := b.bindCTERecursive(cte.Recursive, recursiveCtx); err != nil {
        return err
    }

    return nil
}
```

### Optimizer Integration

```go
// internal/optimizer/recursive_optimizer.go
func (o *Optimizer) optimizeRecursiveCTE(plan *plan.RecursiveCTEPlan) {
    // Apply USING KEY optimization
    if plan.UsingKey != nil {
        o.optimizeUsingKey(plan.UsingKey)
    }

    // Optimize anchor and recursive plans
    plan.AnchorPlan = o.Optimize(plan.AnchorPlan)
    plan.RecursivePlan = o.Optimize(plan.RecursivePlan)
}
```

## Testing Strategy

### Unit Tests

```go
// internal/executor/recursive_cte_test.go
func TestRecursiveCTE_Basic(t *testing.T) {
    query := `
        WITH RECURSIVE t(n) AS (
            SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 10
        ) SELECT * FROM t`

    result := executeQuery(query)
    assert.Equal(t, 10, len(result.Rows))
    assert.Equal(t, int64(55), sumColumn(result, "n"))
}

func TestRecursiveCTE_UsingKey(t *testing.T) {
    query := `
        WITH RECURSIVE shortest_path USING KEY(node) AS (
            SELECT 'A' as node, 0 as distance
            UNION ALL
            SELECT e.to_node, sp.distance + e.weight
            FROM shortest_path sp JOIN edges e ON sp.node = e.from_node
            WHERE sp.distance + e.weight < COALESCE(
                (SELECT distance FROM shortest_path WHERE node = e.to_node), 999999
            )
        ) SELECT * FROM shortest_path`

    result := executeQuery(query)
    assertShortestPathsCorrect(t, result)
}
```

### Integration Tests

```go
// test/recursive_cte_integration_test.go
func TestRecursiveCTE_HierarchicalQuery(t *testing.T) {
    // Setup employee hierarchy data
    setupEmployeeData(t)

    query := `
        WITH RECURSIVE employee_hierarchy AS (
            SELECT id, manager_id, name, 1 as level
            FROM employees WHERE manager_id IS NULL
            UNION ALL
            SELECT e.id, e.manager_id, e.name, h.level + 1
            FROM employees e JOIN employee_hierarchy h ON e.manager_id = h.id
        ) SELECT * FROM employee_hierarchy ORDER BY level, name`

    result := executeQuery(query)
    assertHierarchyCorrect(t, result)
}
```

### Performance Tests

```go
// test/performance/recursive_performance_test.go
func BenchmarkRecursiveCTE_GraphTraversal(b *testing.B) {
    // Create large graph (1M nodes, 10M edges)
    graph := createLargeGraph(1000000, 10000000)

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        query := `
            WITH RECURSIVE traverse USING KEY(node) AS (
                SELECT node_id, 0 as depth FROM nodes WHERE node_id = $1
                UNION ALL
                SELECT e.to_id, t.depth + 1
                FROM traverse t JOIN edges e ON t.node_id = e.from_id
                WHERE t.depth < 10
            ) SELECT COUNT(*) FROM traverse`

        executeQuery(query, graph.StartNode)
    }
}
```

## Error Handling

### Runtime Errors

```go
var (
    ErrMaxRecursionDepth     = errors.New("maximum recursion depth exceeded")
    ErrCycleDetected         = errors.New("infinite recursion detected")
    ErrMemoryLimitExceeded   = errors.New("work table memory limit exceeded")
    ErrInvalidRecursiveRef   = errors.New("invalid recursive reference")
)
```

### Validation Errors

```go
func (v *RecursiveCTEValidator) Validate(cte *ast.RecursiveCTE) error {
    // Check recursive reference exists
    if !v.hasRecursiveReference(cte.Recursive) {
        return ErrInvalidRecursiveRef
    }

    // Check column count match
    if err := v.validateColumnCount(cte); err != nil {
        return err
    }

    // Check USING KEY columns exist
    if cte.UsingKey != nil {
        if err := v.validateUsingKeyColumns(cte); err != nil {
            return err
        }
    }

    return nil
}
```

### Recovery Strategies

1. **Cycle Detection**: Terminate gracefully with partial results
2. **Memory Exhaustion**: Spill to disk or terminate with error
3. **Stack Overflow**: Use iterative execution instead of recursion
4. **Timeout**: Configurable query timeout with partial results

This comprehensive design provides the foundation for implementing recursive CTEs and lateral joins in dukdb-go while maintaining performance, compatibility, and reliability standards. The design emphasizes modularity, allowing for future enhancements and optimizations without major refactoring.
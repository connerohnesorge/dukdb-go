# Design: Window Functions Implementation

## Overview

This document describes the architectural design for adding window function support to dukdb-go. Window functions enable calculations across sets of rows related to the current row, without collapsing rows like GROUP BY.

## Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          SQL Query                               │
│  SELECT name, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal) │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Parser                                   │
│  - Recognizes OVER clause after function call                    │
│  - Parses PARTITION BY, ORDER BY, frame specification           │
│  - Creates WindowExpr AST node                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Binder                                   │
│  - Resolves column references in PARTITION BY / ORDER BY        │
│  - Validates function is window-capable                         │
│  - Infers result type based on function and arguments           │
│  - Creates BoundWindowExpr                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Planner                                   │
│  - Creates LogicalWindow node in logical plan                   │
│  - Transforms to PhysicalWindow in physical plan                │
│  - Window placed above child scan, below final projection       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PhysicalWindow Operator                        │
│  - Materializes child results into partitions                   │
│  - Sorts each partition by ORDER BY                             │
│  - Evaluates window functions per row with frame context        │
│  - Appends result columns to output DataChunk                   │
└─────────────────────────────────────────────────────────────────┘
```

## Data Structures

### AST: WindowExpr

```go
// WindowExpr represents a window function expression.
// Example: ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
type WindowExpr struct {
    Function    *FunctionCall    // The window function (ROW_NUMBER, RANK, etc.)
    PartitionBy []Expr           // PARTITION BY expressions
    OrderBy     []WindowOrderBy  // ORDER BY within window (with NULLS FIRST/LAST)
    Frame       *WindowFrame     // Optional frame specification
    IgnoreNulls bool             // IGNORE NULLS modifier (for LAG, LEAD, FIRST_VALUE, etc.)
    Filter      Expr             // FILTER (WHERE ...) clause for aggregate windows
    Distinct    bool             // DISTINCT modifier for aggregate windows
}

// WindowOrderBy extends OrderByExpr with NULLS FIRST/LAST.
type WindowOrderBy struct {
    Expr       Expr
    Desc       bool
    NullsFirst bool  // true for NULLS FIRST, false for NULLS LAST (default)
}

// WindowFrame represents ROWS/RANGE/GROUPS BETWEEN specification.
type WindowFrame struct {
    Type    FrameType      // ROWS, RANGE, or GROUPS
    Start   WindowBound    // Start boundary
    End     WindowBound    // End boundary
    Exclude ExcludeMode    // EXCLUDE clause
}

// FrameType distinguishes ROWS vs RANGE vs GROUPS semantics.
type FrameType int

const (
    FrameTypeRows   FrameType = iota  // ROWS BETWEEN (physical offset)
    FrameTypeRange                     // RANGE BETWEEN (logical offset)
    FrameTypeGroups                    // GROUPS BETWEEN (peer group offset)
)

// WindowBound represents a frame boundary.
type WindowBound struct {
    Type   BoundType // UNBOUNDED, CURRENT, or OFFSET
    Offset Expr      // For N PRECEDING / N FOLLOWING (must be non-negative constant)
}

type BoundType int

const (
    BoundUnboundedPreceding BoundType = iota
    BoundPreceding          // N PRECEDING
    BoundCurrentRow         // CURRENT ROW
    BoundFollowing          // N FOLLOWING
    BoundUnboundedFollowing
)

// ExcludeMode specifies which rows to exclude from frame.
type ExcludeMode int

const (
    ExcludeNoOthers   ExcludeMode = iota  // EXCLUDE NO OTHERS (default)
    ExcludeCurrentRow                      // EXCLUDE CURRENT ROW
    ExcludeGroup                           // EXCLUDE GROUP (current row's peer group)
    ExcludeTies                            // EXCLUDE TIES (peers but not current row)
)
```

### Binder: BoundWindowExpr

```go
// BoundWindowExpr represents a bound window expression.
type BoundWindowExpr struct {
    FunctionName  string             // e.g., "row_number", "sum"
    FunctionType  WindowFunctionType // Ranking, Value, Distribution, Aggregate
    Args          []BoundExpr        // Bound function arguments
    PartitionBy   []BoundExpr        // Bound partition expressions
    OrderBy       []BoundWindowOrder // Bound order expressions with NULLS FIRST/LAST
    Frame         WindowFrame        // Resolved frame (with defaults applied)
    ResultType    dukdb.Type         // Result type of the window function
    IgnoreNulls   bool               // IGNORE NULLS modifier
    Filter        BoundExpr          // Bound FILTER expression (or nil)
    Distinct      bool               // DISTINCT modifier for aggregates
    ResultIndex   int                // Column index in output
}

// BoundWindowOrder represents bound ORDER BY with null ordering.
type BoundWindowOrder struct {
    Expr       BoundExpr
    Desc       bool
    NullsFirst bool
}

type WindowFunctionType int

const (
    WindowFunctionRanking     WindowFunctionType = iota // ROW_NUMBER, RANK, DENSE_RANK, NTILE
    WindowFunctionValue                                  // LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
    WindowFunctionDistribution                           // PERCENT_RANK, CUME_DIST
    WindowFunctionAggregate                              // COUNT, SUM, AVG, MIN, MAX with OVER
)
```

## Window Function Return Types

Each window function has a specific return type that must be correctly inferred:

| Function | Return Type | Notes |
|----------|-------------|-------|
| ROW_NUMBER() | BIGINT | Always sequential integer starting at 1 |
| RANK() | BIGINT | Integer with gaps for ties |
| DENSE_RANK() | BIGINT | Integer without gaps |
| NTILE(n) | BIGINT | Bucket number (1 to n) |
| LAG(expr, ...) | typeof(expr) | Same type as first argument |
| LEAD(expr, ...) | typeof(expr) | Same type as first argument |
| FIRST_VALUE(expr) | typeof(expr) | Same type as argument |
| LAST_VALUE(expr) | typeof(expr) | Same type as argument |
| NTH_VALUE(expr, n) | typeof(expr) | Same type as first argument |
| PERCENT_RANK() | DOUBLE | Value in range [0.0, 1.0] |
| CUME_DIST() | DOUBLE | Value in range (0.0, 1.0] |
| COUNT(*) OVER | BIGINT | Standard aggregate return |
| SUM(expr) OVER | typeof(expr) or promoted | Matches aggregate return type rules |
| AVG(expr) OVER | DOUBLE | Always DOUBLE |
| MIN/MAX(expr) OVER | typeof(expr) | Same type as argument |

## Error Types and Handling

Window functions use the following error types:

| Error Type | Condition | Example |
|------------|-----------|---------|
| ErrorTypeParser | Invalid OVER syntax | `ROW_NUMBER() OVER FROM t` |
| ErrorTypeBinder | Unknown window function | `INVALID_FUNC() OVER ()` |
| ErrorTypeBinder | IGNORE NULLS on non-value function | `RANK() IGNORE NULLS OVER ()` |
| ErrorTypeBinder | FILTER on non-aggregate window | `ROW_NUMBER() FILTER (WHERE x) OVER ()` |
| ErrorTypeBinder | DISTINCT on non-aggregate window | `RANK() DISTINCT OVER ()` |
| ErrorTypeExecutor | NTILE bucket count <= 0 | `NTILE(0) OVER ()` |
| ErrorTypeExecutor | NTH_VALUE index <= 0 | `NTH_VALUE(x, 0) OVER ()` |
| ErrorTypeExecutor | LAG/LEAD offset < 0 | `LAG(x, -1) OVER ()` |
| ErrorTypeExecutor | Frame offset < 0 | `ROWS BETWEEN -1 PRECEDING` |

## Planner Integration Details

### Window Detection in SELECT

The planner must detect window expressions during logical plan construction:

```go
func (p *Planner) planSelect(stmt *parser.SelectStmt) (LogicalPlan, error) {
    // 1. Plan FROM clause
    child := p.planFrom(stmt.From)

    // 2. Plan WHERE clause
    if stmt.Where != nil {
        child = &LogicalFilter{Child: child, Condition: stmt.Where}
    }

    // 3. Detect window expressions in SELECT list
    hasWindowExprs := false
    windowExprs := []*BoundWindowExpr{}
    for _, col := range stmt.Columns {
        if containsWindowExpr(col.Expr) {
            hasWindowExprs = true
            bound := p.bindWindowExpr(col.Expr)
            windowExprs = append(windowExprs, bound)
        }
    }

    // 4. If windows exist, insert LogicalWindow above child
    if hasWindowExprs {
        child = &LogicalWindow{
            Child:       child,
            WindowExprs: windowExprs,
        }
    }

    // 5. Plan projection (window results become available as columns)
    return &LogicalProject{Child: child, Columns: stmt.Columns}, nil
}
```

### Plan Tree Position

```
                LogicalProject
                      │
                      ▼
                LogicalWindow  ◄── Window node ABOVE source, BELOW projection
                      │
                      ▼
                LogicalFilter
                      │
                      ▼
                LogicalScan
```

Window operator receives all columns from child and appends window result columns.
Projection can then reference both original columns and window results.

## Partition Key Computation

Partition keys must handle composite keys and NULL values correctly:

```go
// computePartitionKey generates a unique key for grouping rows.
// NULL values in partition columns form a separate partition where NULL = NULL.
func computePartitionKey(row []any, partitionExprs []BoundExpr) string {
    if len(partitionExprs) == 0 {
        return "" // Single partition for entire dataset
    }

    var buf strings.Builder
    for i, expr := range partitionExprs {
        if i > 0 {
            buf.WriteString("|")
        }
        val := evaluateExpr(expr, row)
        if val == nil {
            buf.WriteString("\x00NULL\x00") // Special NULL marker
        } else {
            // Use type-specific serialization for correct comparison
            buf.WriteString(serializeValue(val))
        }
    }
    return buf.String()
}

// serializeValue produces a comparable string for any value.
func serializeValue(val any) string {
    switch v := val.(type) {
    case int64:
        return fmt.Sprintf("I:%020d", v) // Fixed-width for correct ordering
    case float64:
        return fmt.Sprintf("F:%v", v)
    case string:
        return fmt.Sprintf("S:%s", v)
    case bool:
        if v {
            return "B:T"
        }
        return "B:F"
    case time.Time:
        return fmt.Sprintf("T:%d", v.UnixNano())
    default:
        return fmt.Sprintf("X:%v", v)
    }
}
```

### Executor: WindowState

```go
// WindowState holds state for window function evaluation.
type WindowState struct {
    // Partitions indexed by partition key
    Partitions map[string]*WindowPartition

    // Order of partition keys for deterministic iteration
    PartitionOrder []string
}

// WindowPartition holds rows for a single partition.
type WindowPartition struct {
    // Rows in this partition (sorted by ORDER BY if specified)
    Rows []WindowRow

    // Peer group boundaries (for RANK, DENSE_RANK, RANGE frames)
    // PeerBoundaries[i] is the index where peer group i starts
    PeerBoundaries []int
}

// WindowRow holds a row with its original index and computed values.
type WindowRow struct {
    OriginalIndex int           // Position in original input
    Values        []any         // Column values
    WindowResults []any         // Computed window function results
}
```

## Execution Strategy

### Phase 1: Materialization

The PhysicalWindow operator materializes all child rows into memory, grouped by partition key:

```go
func (w *PhysicalWindow) materialize(child PhysicalOperator) (*WindowState, error) {
    state := &WindowState{
        Partitions:     make(map[string]*WindowPartition),
        PartitionOrder: make([]string, 0),
    }

    for {
        chunk, err := child.Next()
        if err != nil {
            return nil, err
        }
        if chunk == nil {
            break
        }

        for i := 0; i < chunk.Count(); i++ {
            row := extractRow(chunk, i)
            partitionKey := computePartitionKey(row, w.PartitionBy)

            if _, exists := state.Partitions[partitionKey]; !exists {
                state.Partitions[partitionKey] = &WindowPartition{}
                state.PartitionOrder = append(state.PartitionOrder, partitionKey)
            }

            state.Partitions[partitionKey].Rows = append(
                state.Partitions[partitionKey].Rows,
                WindowRow{OriginalIndex: globalIndex, Values: row},
            )
            globalIndex++
        }
    }

    return state, nil
}
```

### Phase 2: Sorting

Each partition is sorted by ORDER BY specification:

```go
func (w *PhysicalWindow) sortPartitions(state *WindowState) {
    for _, partition := range state.Partitions {
        sort.Slice(partition.Rows, func(i, j int) bool {
            return w.compareByOrderBy(partition.Rows[i], partition.Rows[j]) < 0
        })

        // Compute peer group boundaries for RANK/RANGE semantics
        partition.PeerBoundaries = computePeerBoundaries(partition.Rows, w.OrderBy)
    }
}
```

### Phase 3: Evaluation

Window functions are evaluated per row with frame context:

```go
func (w *PhysicalWindow) evaluateWindow(state *WindowState) {
    for _, partitionKey := range state.PartitionOrder {
        partition := state.Partitions[partitionKey]

        for rowIdx := range partition.Rows {
            row := &partition.Rows[rowIdx]

            for _, windowExpr := range w.WindowExprs {
                frame := computeFrameBounds(partition, rowIdx, windowExpr.Frame)
                result := evaluateWindowFunction(windowExpr, partition, rowIdx, frame)
                row.WindowResults = append(row.WindowResults, result)
            }
        }
    }
}
```

### Phase 4: Output

Results are emitted in original row order with window columns appended:

```go
func (w *PhysicalWindow) Next() (*DataChunk, error) {
    // On first call, materialize, sort, and evaluate
    if w.state == nil {
        state, err := w.materialize(w.child)
        if err != nil {
            return nil, err
        }
        w.sortPartitions(state)
        w.evaluateWindow(state)
        w.state = state
        w.outputRows = flattenAndSortByOriginalIndex(state)
    }

    // Emit rows in chunks
    chunk := NewDataChunk(w.outputTypes)
    for w.outputIndex < len(w.outputRows) && chunk.Count() < StandardVectorSize {
        row := w.outputRows[w.outputIndex]
        chunk.AppendRow(append(row.Values, row.WindowResults...))
        w.outputIndex++
    }

    if chunk.Count() == 0 {
        return nil, nil // No more data
    }
    return chunk, nil
}
```

## Window Function Semantics

### Ranking Functions

| Function | Description | Requires ORDER BY |
|----------|-------------|-------------------|
| ROW_NUMBER() | Unique sequential integer per partition | No (but usually used with) |
| RANK() | Rank with gaps for ties | Yes |
| DENSE_RANK() | Rank without gaps for ties | Yes |
| NTILE(n) | Divide into n buckets | No |

**RANK vs DENSE_RANK Example:**
```
Value | RANK | DENSE_RANK
------|------|------------
  10  |  1   |     1
  10  |  1   |     1
  20  |  3   |     2      <- RANK skips 2, DENSE_RANK doesn't
  30  |  4   |     3
```

### Value Functions

| Function | Description | Default Offset | Default Value |
|----------|-------------|----------------|---------------|
| LAG(expr, offset, default) | Value from offset rows before | 1 | NULL |
| LEAD(expr, offset, default) | Value from offset rows after | 1 | NULL |
| FIRST_VALUE(expr) | First value in frame | - | - |
| LAST_VALUE(expr) | Last value in frame | - | - |
| NTH_VALUE(expr, n) | Nth value in frame (1-indexed) | - | NULL if n > frame size |

### Distribution Functions

| Function | Formula |
|----------|---------|
| PERCENT_RANK() | (rank - 1) / (partition_size - 1), or 0 if partition_size = 1 |
| CUME_DIST() | (rows_before_or_equal) / partition_size |

## Frame Semantics

### Default Frame

When no frame is specified:
- If ORDER BY is present: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- If no ORDER BY: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`

### ROWS vs RANGE vs GROUPS

- **ROWS**: Physical row offset (3 PRECEDING = 3 rows back)
- **RANGE**: Logical value offset based on ORDER BY (3 PRECEDING = value - 3)
- **GROUPS**: Peer group offset (3 PRECEDING = 3 peer groups back)

**GROUPS frame semantics:**
Rows with the same ORDER BY values are in the same "peer group". GROUPS frame counts peer groups rather than individual rows.

Example with values [10, 10, 20, 30, 30, 40]:
- Peer groups: {10, 10}, {20}, {30, 30}, {40}
- `GROUPS 1 PRECEDING` at row with value 30 includes groups {20}, {30, 30}

**RANGE frame type constraints:**
- RANGE frame requires exactly one ORDER BY column
- ORDER BY column must be a numeric or temporal type (INT, BIGINT, DECIMAL, FLOAT, DOUBLE, DATE, TIMESTAMP, INTERVAL)
- Using RANGE with multiple ORDER BY columns returns `ErrorTypeBinder`
- Using RANGE with non-numeric/non-temporal types returns `ErrorTypeBinder`
- Use GROUPS frame for non-numeric ordering or multiple columns

### EXCLUDE Clause Semantics

The EXCLUDE clause removes specific rows from the frame:

| Exclude Mode | Behavior |
|--------------|----------|
| EXCLUDE NO OTHERS | Include all rows in frame (default) |
| EXCLUDE CURRENT ROW | Exclude current row from frame |
| EXCLUDE GROUP | Exclude current row's entire peer group |
| EXCLUDE TIES | Exclude peers of current row, but include current row |

Example with values [10, 10, 20] at current row = first 10:
- EXCLUDE NO OTHERS: frame contains [10, 10, 20]
- EXCLUDE CURRENT ROW: frame contains [10, 20] (one 10 removed)
- EXCLUDE GROUP: frame contains [20] (both 10s removed)
- EXCLUDE TIES: frame contains [10, 20] (only one 10, not the peer)

### Frame Boundary Computation

```go
func computeFrameBounds(partition *WindowPartition, rowIdx int, frame WindowFrame) FrameBounds {
    var start, end int

    switch frame.Start.Type {
    case BoundUnboundedPreceding:
        start = 0
    case BoundCurrentRow:
        start = rowIdx
    case BoundPreceding:
        offset := evaluateOffset(frame.Start.Offset)
        if frame.Type == FrameTypeRows {
            start = max(0, rowIdx - offset)
        } else { // RANGE
            start = findRangeStart(partition, rowIdx, offset)
        }
    case BoundFollowing:
        offset := evaluateOffset(frame.Start.Offset)
        if frame.Type == FrameTypeRows {
            start = min(len(partition.Rows)-1, rowIdx + offset)
        } else {
            start = findRangeEnd(partition, rowIdx, offset)
        }
    }

    // Similar logic for end...

    return FrameBounds{Start: start, End: end}
}
```

## NULL Handling

Following DuckDB semantics:

### PARTITION BY

NULL values form their own partition (NULL = NULL for grouping purposes).
Multiple NULL values in partition column(s) are grouped together.

### ORDER BY with NULLS FIRST/LAST

```sql
-- NULLs sorted last (default)
SELECT x, ROW_NUMBER() OVER (ORDER BY x) FROM t;
-- Values: [1, 2, 3, NULL, NULL] → row_numbers: [1, 2, 3, 4, 5]

-- NULLs sorted first
SELECT x, ROW_NUMBER() OVER (ORDER BY x NULLS FIRST) FROM t;
-- Values: [NULL, NULL, 1, 2, 3] → row_numbers: [1, 2, 3, 4, 5]
```

### IGNORE NULLS Modifier

For value functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE), IGNORE NULLS skips NULL values:

```sql
-- Without IGNORE NULLS (default: RESPECT NULLS)
SELECT LAG(x) OVER (ORDER BY id) FROM t;
-- Values: [1, NULL, 3] → LAG: [NULL, 1, NULL]

-- With IGNORE NULLS
SELECT LAG(x) IGNORE NULLS OVER (ORDER BY id) FROM t;
-- Values: [1, NULL, 3] → LAG: [NULL, NULL, 1]
-- LAG at row 3 returns 1 (skips the NULL at row 2)
```

IGNORE NULLS is only valid for: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE.
Using with other functions (RANK, ROW_NUMBER, etc.) returns ErrorTypeBinder.

### LAG/LEAD Boundary Behavior

Return default value (NULL if not specified) when offset goes out of partition bounds.

### Aggregates with NULL

- COUNT(*): Includes NULL rows (counts all rows in frame)
- COUNT(expr): Excludes NULL values
- SUM/AVG/MIN/MAX: Skip NULL values in computation

### FILTER and DISTINCT Interaction

When both FILTER and DISTINCT are used, apply in this order:
1. **FILTER first**: Apply WHERE condition to select rows in frame
2. **DISTINCT second**: Remove duplicate values from filtered rows
3. **Aggregate third**: Compute aggregate on filtered, distinct values

Example:
```sql
-- Data: [(1, 'active'), (1, 'active'), (2, 'inactive'), (2, 'active')]
COUNT(DISTINCT x) FILTER (WHERE status = 'active') OVER ()
-- Step 1: Filter to active rows: [1, 1, 2]
-- Step 2: Distinct: [1, 2]
-- Step 3: Count: 2
```

### NTILE Without ORDER BY

When NTILE(n) is called without ORDER BY in the window specification:
- The partition rows are not sorted
- Bucket assignment is based on the natural row order within partition
- Results are deterministic but may not be meaningful
- Recommended practice: Always use ORDER BY with NTILE for predictable results

### Frame Boundaries with NULL

- ROWS frame: NULL values in ORDER BY are handled like any other value
- RANGE frame: NULL values are excluded from range comparisons
- GROUPS frame: NULL values form their own peer group

## Integration with Existing Code

### Parser Changes (internal/parser/parser.go)

Extend `parseFunctionCall()` to check for OVER keyword:

```go
func (p *Parser) parseFunctionCall(name string) (Expr, error) {
    // ... existing function parsing ...

    // Check for OVER clause
    if p.peek().Type == TokenOVER {
        p.advance() // consume OVER
        return p.parseWindowExpr(&FunctionCall{Name: name, Args: args})
    }

    return &FunctionCall{Name: name, Args: args}, nil
}
```

### Binder Changes (internal/binder/bind_expr.go)

Add window expression binding:

```go
func (b *Binder) bindWindowExpr(expr *parser.WindowExpr) (*BoundWindowExpr, error) {
    // Bind function and validate it's window-capable
    funcInfo, err := b.resolveWindowFunction(expr.Function.Name)
    if err != nil {
        return nil, err
    }

    // Bind partition by expressions
    partitionBy := make([]BoundExpr, len(expr.PartitionBy))
    for i, e := range expr.PartitionBy {
        bound, err := b.bindExpr(e)
        if err != nil {
            return nil, err
        }
        partitionBy[i] = bound
    }

    // Bind order by expressions
    // Apply default frame based on ORDER BY presence
    // ...
}
```

### Planner Changes (internal/planner/physical.go)

Add window operator creation:

```go
func (p *Planner) createPhysicalWindow(logical *LogicalWindow, child PhysicalPlan) PhysicalPlan {
    return &PhysicalWindow{
        Child:       child,
        WindowExprs: logical.WindowExprs,
        PartitionBy: logical.PartitionBy,
        OrderBy:     logical.OrderBy,
    }
}
```

### Executor Changes (internal/executor/)

Create new file `physical_window.go`:

```go
type PhysicalWindow struct {
    child       PhysicalOperator
    windowExprs []*BoundWindowExpr
    state       *WindowState
    outputIndex int
    outputRows  []WindowRow
}

func (w *PhysicalWindow) Next() (*DataChunk, error) {
    // Implementation as described above
}

func (w *PhysicalWindow) GetTypes() []TypeInfo {
    childTypes := w.child.GetTypes()
    windowTypes := make([]TypeInfo, len(w.windowExprs))
    for i, expr := range w.windowExprs {
        windowTypes[i] = expr.ResultType
    }
    return append(childTypes, windowTypes...)
}
```

## Testing Strategy

1. **Unit Tests**: Test each window function in isolation with known inputs/outputs
2. **Partition Tests**: Verify correct partitioning with various PARTITION BY expressions
3. **Order Tests**: Verify ORDER BY with NULL handling (NULLS FIRST/LAST)
4. **Frame Tests**: Test all frame boundary combinations
5. **Compatibility Tests**: Compare results against DuckDB CLI for 100+ queries
6. **Performance Tests**: Benchmark with 100K rows, 100 partitions

## Performance Considerations

### Current Implementation

- O(n) materialization
- O(n log n) per-partition sorting
- O(n²) worst-case frame evaluation (for sliding windows)

### Future Optimizations

1. **Segment Trees**: O(log n) sliding window aggregates
2. **Streaming**: For non-reordering cases (no ORDER BY, full frame)
3. **Parallel Partitions**: Process partitions concurrently
4. **Memory Limits**: Spill large partitions to disk

These optimizations are out of scope for the initial implementation but documented for future work.

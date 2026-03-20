# Window Functions Implementation Design

## Overview

This document captures architectural decisions, trade-offs, and implementation details for completing the window functions implementation in dukdb-go.

## Current Architecture

The existing implementation in `internal/executor/physical_window.go` provides:

```
PhysicalWindowExecutor
├── Phase 1: Materialize (all rows from child into partitions)
├── Phase 2: Sort (each partition by ORDER BY)
├── Phase 3: Compute Peer Boundaries (for RANK/RANGE/GROUPS)
├── Phase 4: Compute Frame Bounds (for each row)
├── Phase 5: Evaluate Window Functions (per row, per partition)
└── Phase 6: Flatten & Restore Order (back to input order)
```

**Key Components:**

- `WindowState`: Holds partitions indexed by PARTITION BY key
- `WindowPartition`: Rows in a partition with peer boundaries
- `WindowRow`: Individual row with original values and computed results
- `FrameBounds`: Start, end, and excluded rows for a row's frame

**Function Dispatcher:**
```go
func (w *PhysicalWindowExecutor) evaluateWindowFunction(...) any {
    switch funcName {
    case "ROW_NUMBER": return w.evaluateRowNumber(...)
    case "RANK": return w.evaluateRank(...)
    // ... etc
    }
}
```

## Implementation Strategy

### 1. Function Categories

Window functions are organized into natural categories based on their semantics and implementation approach:

#### **Phase 1: Ranking Functions** (simplest, build confidence)
- `ROW_NUMBER()` - sequential numbering within partition
- `RANK()` - rank with gaps, based on peer groups
- `DENSE_RANK()` - rank without gaps, based on peer group index
- `NTILE(n)` - distribute into n buckets evenly

**Why Phase 1:** These functions only need the row index and peer group information. No complex frame logic. Good baseline for testing the infrastructure.

#### **Phase 2: Analytic (Value) Functions** (leverage frame computation)
- `LAG(expr, offset, default)` - value from row offset rows before
- `LEAD(expr, offset, default)` - value from row offset rows after
- `FIRST_VALUE(expr)` - first value in frame
- `LAST_VALUE(expr)` - last value in frame
- `NTH_VALUE(expr, n)` - nth value in frame
- All support `IGNORE NULLS` clause

**Why Phase 2:** These use frame boundaries but with value semantics. They require:
- Frame computation (already exists)
- Expression evaluation on target rows
- NULL handling with IGNORE NULLS support

#### **Phase 3: Distribution & Aggregate Functions** (most complex)
- `PERCENT_RANK()` - (rank - 1) / (n - 1)
- `CUME_DIST()` - cumulative distribution using peer groups
- `SUM(expr)` - windowed sum with FILTER/DISTINCT
- `COUNT(expr)`, `COUNT(*)` - windowed count with FILTER/DISTINCT
- `AVG(expr)` - windowed average with FILTER/DISTINCT
- `MIN(expr)` - windowed minimum with FILTER
- `MAX(expr)` - windowed maximum with FILTER

**Why Phase 3:** Distribution functions use mathematical formulas. Aggregate functions require:
- Accumulation across frame (not just single value lookup)
- FILTER clause evaluation per frame row
- DISTINCT tracking (deduplicate within frame)
- Type coercion for aggregation (int → float for AVG)

### 2. Implementation Patterns

#### **Value Function Pattern** (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE)

```go
func (w *PhysicalWindowExecutor) evaluateLag(
    partition *WindowPartition,
    rowIdx int,
    expr *binder.BoundExpr,
    offset int,
    defaultVal any,
    ignoreNulls bool,
) any {
    if ignoreNulls {
        // Skip NULL values when counting backward
        nonNullCount := 0
        for i := rowIdx - 1; i >= 0; i-- {
            val := evaluateExpression(expr, partition.Rows[i])
            if val != nil {
                nonNullCount++
                if nonNullCount == offset {
                    return val
                }
            }
        }
        return defaultVal
    }

    // Standard path: simple offset
    targetIdx := rowIdx - offset
    if targetIdx < 0 {
        return defaultVal
    }

    val := evaluateExpression(expr, partition.Rows[targetIdx])
    if val == nil {
        return defaultVal
    }
    return val
}
```

**Key aspects:**
- Return type is `any` (expression determines concrete type)
- IGNORE NULLS requires scanning backward/forward skipping NULLs
- Respect partition boundaries (no wrap-around)

#### **Ranking Function Pattern** (ROW_NUMBER, RANK, DENSE_RANK, NTILE)

```go
func (w *PhysicalWindowExecutor) evaluateRank(
    partition *WindowPartition,
    rowIdx int,
) int64 {
    peerGroupIdx := w.getPeerGroupForRow(partition, rowIdx)
    return int64(w.getPeerGroupStart(partition, peerGroupIdx) + 1)
}
```

**Key aspects:**
- Return type is always `int64`
- Use peer group boundaries (computed in Phase 3)
- Direct calculation, no frame iteration needed

#### **Aggregate Window Pattern** (SUM, COUNT, AVG, MIN, MAX)

```go
func (w *PhysicalWindowExecutor) evaluateWindowSum(
    partition *WindowPartition,
    frame FrameBounds,
    expr *binder.BoundExpr,
    filter binder.BoundExpr,
    distinct bool,
) any {
    var sum float64
    hasValue := false
    seen := make(map[string]bool)

    for i := frame.Start; i <= frame.End; i++ {
        // Skip excluded rows
        if w.isRowExcluded(frame.ExcludedRows, i) {
            continue
        }

        // Check FILTER condition
        if filter != nil {
            filterVal := evaluateExpression(filter, partition.Rows[i])
            if !toBool(filterVal) {
                continue
            }
        }

        // Evaluate expression
        val := evaluateExpression(expr, partition.Rows[i])
        if val == nil {
            continue
        }

        // Handle DISTINCT
        if distinct {
            key := fmt.Sprintf("%v", val)
            if seen[key] {
                continue
            }
            seen[key] = true
        }

        sum += toFloat64Value(val)
        hasValue = true
    }

    if !hasValue {
        return nil
    }
    return sum
}
```

**Key aspects:**
- Iterate frame range (frame.Start to frame.End inclusive)
- Check excluded rows
- Evaluate FILTER condition if present
- Handle DISTINCT by tracking seen values
- Type conversions for aggregation

### 3. Key Design Decisions

#### **Decision 1: Return Type Handling**

**Question:** How to handle mixed return types (int64 for RANK, float64 for PERCENT_RANK, etc.)?

**Answer:** Use `any` in evaluateWindowFunction, let driver handle type conversion to SQL types. The `ResType` field in BoundWindowExpr specifies expected output type; type coercion happens at conversion to DataChunk.

**Rationale:** Simple, matches existing executor pattern. Type checking is handled at the binder level.

#### **Decision 2: NULL Handling in IGNORE NULLS**

**Question:** For LAG/LEAD/FIRST_VALUE/LAST_VALUE with IGNORE NULLS, should we skip all NULLs or just from the expression?

**Answer:** Skip NULLs from the expression column only. This matches DuckDB behavior:
- `LAG(col) IGNORE NULLS` skips rows where col is NULL
- `LAG(col) FILTER (WHERE cond) IGNORE NULLS` first filters, then ignores NULLs in result

**Rationale:** IGNORE NULLS is a value-level directive, not a row-level filter.

#### **Decision 3: RANGE Frames with Numeric Types**

**Question:** For RANGE frames, how to handle numeric comparisons (int vs float vs decimal)?

**Answer:** Use Go's type coercion system. When comparing ORDER BY values:
1. Convert both to float64 for arithmetic comparison
2. Support int64, int32, float64, float32, and decimal
3. Decimal comparisons use big.Float

**Rationale:** Simplifies implementation while supporting all numeric types. DuckDB also coerces numerically.

#### **Decision 4: FILTER and DISTINCT Interaction**

**Question:** In COUNT(DISTINCT x) FILTER (WHERE cond), is DISTINCT applied before or after FILTER?

**Answer:** FILTER is applied first (evaluates condition), then DISTINCT deduplicates the filtered values.

**Rationale:** Matches SQL standard and DuckDB behavior. The FILTER clause is essentially a WHERE condition within the window frame.

#### **Decision 5: Empty Frames**

**Question:** When frame is empty (e.g., ROWS 5 FOLLOWING on last rows), what should functions return?

**Answer:**
- Ranking functions: Still return rank (not affected by frame)
- Value functions: Return NULL
- Aggregate functions: Return NULL (except COUNT(*) returns 0)

**Rationale:** Matches SQL standard for window functions.

#### **Decision 6: Performance - Frame Computation Caching**

**Question:** Should we cache frame bounds computation across multiple window functions?

**Answer:** Not initially. Each row's frame is computed fresh for each window function. Future optimization can add caching if profiling shows it's a bottleneck.

**Rationale:** Current implementation is already O(n²) with function dispatcher; premature optimization risks added complexity. Validate performance first.

### 4. Type Compatibility Matrix

| Function | INT | FLOAT | DECIMAL | STRING | DATE | TIMESTAMP |
|----------|-----|-------|---------|--------|------|-----------|
| ROW_NUMBER | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| RANK | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| DENSE_RANK | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| NTILE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| LAG | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| LEAD | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| FIRST_VALUE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| LAST_VALUE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| NTH_VALUE | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| PERCENT_RANK | ✓ | ✓ | ✓ | N/A | N/A | N/A |
| CUME_DIST | ✓ | ✓ | ✓ | N/A | N/A | N/A |
| SUM | ✓ | ✓ | ✓ | N/A | N/A | N/A |
| COUNT | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| AVG | ✓ | ✓ | ✓ | N/A | N/A | N/A |
| MIN | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| MAX | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

### 5. Error Handling Strategy

**No new error types needed.** Use existing errors:
- Invalid offset: Return NULL (per SQL standard)
- Invalid n for NTILE/NTH_VALUE: Clamp to 1 or return NULL
- FILTER evaluation error: Skip row (treat as filtered out)
- Expression evaluation error: Treat as NULL
- Type mismatch in aggregation: Rely on existing type system

**Rationale:** Window functions should be forgiving and return NULL rather than error, matching DuckDB behavior.

### 6. Testing Strategy

**Unit Tests:**
- Each function with basic, edge case, and error scenarios
- Isolated to single function evaluation
- Use mock partitions and frames

**Integration Tests:**
- Full queries via SQL parser → binder → executor
- Compare results with DuckDB reference
- Test function combinations
- Test with various data types

**Performance Tests:**
- Benchmark with large partitions (100K+ rows)
- Benchmark with many partitions
- Profiling to identify bottlenecks

### 7. Future Optimization Opportunities

1. **Frame Caching:** Cache computed frames for rows when multiple functions share same spec
2. **Streaming Evaluation:** For certain frames (ROWS unbounded to current), evaluate incrementally
3. **Index Usage:** Leverage indexes for RANGE frames on indexed columns
4. **Parallel Partition Processing:** Evaluate independent partitions in parallel
5. **Vectorized Evaluation:** Process multiple rows in batch for better CPU utilization

*These are documented for future enhancement but not implemented in this change.*

## Files Modified/Created

- **Modified:** `internal/executor/physical_window.go` (add function bodies)
- **Modified:** Test files (add test coverage)
- **No new files** (framework already in place)

## Compatibility Notes

- **API Compatibility:** Window function results match duckdb-go behavior
- **SQL Compatibility:** Follows DuckDB v1.4.3 SQL standard
- **Performance:** Single-pass evaluation (no additional scans)
- **Memory:** O(n) space for partitions (same as current)

## Known Limitations (Out of Scope)

1. **Streaming evaluation:** All window functions require materializing all input (inherent limitation)
2. **User-defined window functions:** Not in scope
3. **Window function predicates:** ORDER BY with function calls in phase detection not supported
4. **ROWS UNBOUNDED PRECEDING with specific numeric types:** Treat same as ROWS
5. **NULL collation in ORDER BY:** Respects NULLS FIRST/LAST but no custom collation

These can be addressed in future changes if needed.

## References

- DuckDB Window Functions: https://duckdb.org/docs/sql/window_functions/index.html (version 1.4.3)
- SQL Standard Window Functions: ISO/IEC 9075 Part 2
- Existing implementation: `internal/executor/physical_window.go`

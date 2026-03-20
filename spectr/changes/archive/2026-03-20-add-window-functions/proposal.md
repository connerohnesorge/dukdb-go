# Change: Complete Window Functions Implementation

## Why

Window functions are a critical SQL feature for analytical queries, enabling calculations across related rows without collapsing them into a single output row. dukdb-go already has foundational infrastructure in place (`internal/executor/physical_window.go`), but needs complete function implementations to reach feature parity with DuckDB v1.4.3 and enable advanced analytical workloads.

The existing implementation provides:
- Window partitioning and ordering
- Frame computation (ROWS, RANGE, GROUPS with BETWEEN syntax)
- EXCLUDE clause support
- Basic architecture for function evaluation

This proposal completes the implementation with all window function categories, enabling users to perform rank analysis, access adjacent rows, compute running aggregates, and calculate statistical distributions.

## What Changes

This implementation is organized into three phases, each delivering complete, independently valuable functionality:

### Phase 1: Ranking Functions
- ROW_NUMBER() - sequential row numbering within partitions
- RANK() - ranking with gaps for ties
- DENSE_RANK() - ranking without gaps
- NTILE(n) - distribution into buckets

### Phase 2: Analytic (Value) Functions
- LAG(expr, offset, default) - access previous rows
- LEAD(expr, offset, default) - access following rows
- FIRST_VALUE(expr) - first value in frame
- LAST_VALUE(expr) - last value in frame
- NTH_VALUE(expr, n) - nth value in frame
- IGNORE NULLS support for all value functions

### Phase 3: Distribution Functions & Aggregate Window Functions
- PERCENT_RANK() - relative rank (0-1)
- CUME_DIST() - cumulative distribution
- SUM(expr) OVER window - running/windowed sum
- COUNT(expr) OVER window - windowed count
- AVG(expr) OVER window - windowed average
- MIN(expr) OVER window - windowed minimum
- MAX(expr) OVER window - windowed maximum
- FILTER clause support for aggregate functions
- DISTINCT keyword support for aggregate functions

## Impact

- **Affected specs**: `window-functions` (new), potential updates to `query-execution` and `execution-engine`
- **Affected code**:
  - `internal/executor/physical_window.go` - extend existing implementation
  - `internal/postgres/functions/aliases_window.go` - window function registry
  - Parser AST structures (if needed for missing syntax)
  - Binder for window expression resolution (if needed)

## Breaking Changes

None. This is a pure feature addition building on existing infrastructure.

## Success Criteria

1. All window functions execute correctly with single partitions
2. Window functions respect PARTITION BY, ORDER BY, and frame clauses
3. IGNORE NULLS works correctly for value functions
4. FILTER and DISTINCT clauses work for aggregate window functions
5. Complex window expressions combine correctly (multiple window functions in same query)
6. Performance is reasonable for analytical workloads (single-pass evaluation where possible)
7. All test scenarios pass against reference DuckDB behavior

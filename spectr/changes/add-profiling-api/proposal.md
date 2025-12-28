# Change: Add Query Profiling API

## Why

Users need visibility into query execution for performance tuning and debugging. The profiling API provides:
- Query plan tree with metrics per operator
- Execution time breakdown
- Row counts and cardinality information

## What Changes

### Core Types

```go
// ProfilingInfo represents a node in the query execution tree
type ProfilingInfo struct {
    // Metrics contains measured values for this node
    Metrics map[string]string
    // Children contains child nodes in the plan
    Children []ProfilingInfo
}
```

### Public API

```go
// GetProfilingInfo retrieves profiling data for the most recent query
func GetProfilingInfo(c *sql.Conn) (ProfilingInfo, error)
```

### Usage Flow

1. Enable profiling via raw SQL: `PRAGMA enable_profiling` or `PRAGMA enable_progress_bar`
   - Note: No wrapper functions provided - use raw SQL execution
2. Execute query
3. Call GetProfilingInfo to retrieve metrics

Note: Memory usage metrics mentioned in goals are NOT currently provided by the implementation. Available metrics are execution time and row counts only.

## Impact

- **Affected specs**: None (standalone feature)
- **Affected code**: New file `profiling.go`
- **Dependencies**: Query execution engine
- **Consumers**: Performance tuning, debugging tools

## Breaking Changes

None. This adds new functionality.

## Metrics Available

- **QUERY_ROOT**: Overall query metrics
  - `TOTAL_TIME`: Total execution time
  - `ROWS_RETURNED`: Number of result rows
- **Operator nodes**: Per-operator metrics
  - `OPERATOR_TYPE`: Type of operator
  - `OPERATOR_TIME`: Time spent in operator
  - `ROWS`: Rows processed
  - `CARDINALITY`: Estimated vs actual cardinality

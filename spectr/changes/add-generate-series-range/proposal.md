# Change: Add generate_series() and range() Table Functions

## Why

DuckDB v1.4.3 provides `generate_series()` and `range()` as fundamental
table-generating functions for producing sequential values (integers, dates,
timestamps). These are heavily used in analytics for generating date
dimensions, filling gaps in time series, and creating test data. dukdb-go
currently has no equivalent, forcing users to create helper tables manually.

## What Changes

- **Executor**: New `table_function_series.go` implementing both functions
  in the existing table function dispatch (`executeTableFunctionScan`)
- **Binder**: Register `generate_series` and `range` as known table functions
  with argument type resolution
- Supports INTEGER, BIGINT, DATE, TIMESTAMP types with INTERVAL steps for
  temporal types
- `generate_series` is inclusive of stop; `range` is exclusive of stop

## Impact

- Affected specs: `execution-engine`, `table-udf`
- Affected code:
  - `internal/executor/table_function_csv.go` — add case entries in dispatch
  - `internal/executor/table_function_series.go` — new file with implementation
  - `internal/binder/bind_stmt.go` — register as known table functions

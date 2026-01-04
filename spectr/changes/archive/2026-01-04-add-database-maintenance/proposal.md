# Change: Database Maintenance Commands Support

## Why

Currently, dukdb-go has limited support for database maintenance commands. While statement type detection exists for `PRAGMA`, `EXPLAIN`, `VACUUM`, and `ANALYZE`, the actual execution is not implemented. This creates gaps in functionality compared to DuckDB:

1. **PRAGMA Support**: DuckDB provides extensive PRAGMA commands for configuration, introspection, and maintenance. Without PRAGMA support, users cannot:
   - Query database size and memory usage (`PRAGMA database_size`)
   - Get table storage information (`PRAGMA storage_info('table')`)
   - List available functions and tables (`PRAGMA functions`, `PRAGMA tables`)
   - Configure runtime settings (`PRAGMA max_memory`, `PRAGMA threads`)
   - Enable/disable query profiling (`PRAGMA enable_profiling`)

2. **EXPLAIN Support**: EXPLAIN and EXPLAIN ANALYZE are essential for query optimization and debugging. Without these, users cannot understand query execution plans.

3. **VACUUM Command**: VACUUM reclaims space from deleted rows and optimizes storage. Without this, databases can grow unnecessarily large over time.

4. **ANALYZE Command**: ANALYZE collects column statistics used by the query optimizer for better query plans. Without statistics, query planning may be suboptimal.

5. **CHECKPOINT Enhancement**: While basic WAL checkpoint exists, full database checkpoint (merging row groups, writing metadata) is not implemented.

6. **System Tables**: DuckDB provides `duckdb_*()` table-valued functions for introspection. Without these, users cannot query metadata programmatically.

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

- **PRAGMA Support**: Parse and execute PRAGMA statements for:
  - Information queries (`database_size`, `table_info`, `storage_info`, `functions`, `tables`)
  - Configuration (`max_memory`, `threads`, `enable_profiling`, `disable_profiling`, `profiling_mode`)
  
- **EXPLAIN / EXPLAIN ANALYZE**: Generate textual query plans showing operator tree and costs
  - `EXPLAIN SELECT ...` - Show estimated query plan
  - `EXPLAIN ANALYZE SELECT ...` - Execute and show actual performance data

- **VACUUM Command**: Reclaim space from deleted rows and optimize table storage

- **ANALYZE Command**: Collect and store column statistics for query optimization

- **Enhanced CHECKPOINT**: Full checkpoint including WAL flush, row group merge, and metadata write

- **System Tables**: Implement `duckdb_tables()`, `duckdb_columns()`, `duckdb_functions()`, `duckdb_settings()`

### Internal Changes

- **Parser**: Add `PragmaStmt`, `ExplainStmt`, `VacuumStmt`, `AnalyzeStmt`, `CheckpointStmt` AST nodes
- **Binder**: Add `BindPragma()`, `BindExplain()`, `BindVacuum()`, `BindAnalyze()`, `BindCheckpoint()`
- **Planner**: Add logical/physical nodes for explain plans
- **Executor**: Add `PhysicalPragma`, `PhysicalExplain`, `PhysicalVacuum`, `PhysicalAnalyze`, `PhysicalCheckpoint` operators
- **Catalog**: Add statistics storage for ANALYZE results

## Impact

### Affected Specs

- `specs/query-execution/spec.md` - Add PRAGMA, EXPLAIN, VACUUM, ANALYZE execution requirements
- `specs/public-api/spec.md` - Add system table function requirements

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/parser/ast.go` | MODIFIED | Add PragmaStmt, ExplainStmt, VacuumStmt, AnalyzeStmt, CheckpointStmt |
| `internal/parser/parser.go` | MODIFIED | Add parsing dispatch for PRAGMA, EXPLAIN, VACUUM, ANALYZE, CHECKPOINT |
| `internal/parser/visitor.go` | MODIFIED | Add visitor methods for new statement types |
| `internal/binder/bind_stmt.go` | MODIFIED | Add bind functions for new statement types |
| `internal/binder/binder.go` | MODIFIED | Add cases for new statement types in Bind() |
| `internal/binder/expressions.go` | MODIFIED | Add bound statement types |
| `internal/planner/logical.go` | MODIFIED | Add LogicalExplain, LogicalPragma, etc. |
| `internal/planner/physical.go` | MODIFIED | Add PhysicalExplain, PhysicalPragma, etc. |
| `internal/executor/executor.go` | MODIFIED | Add execution dispatch for new operators |
| `internal/executor/physical_pragma.go` | ADDED | PRAGMA execution operator |
| `internal/executor/physical_explain.go` | ADDED | EXPLAIN execution operator |
| `internal/executor/physical_vacuum.go` | ADDED | VACUUM execution operator |
| `internal/executor/physical_analyze.go` | ADDED | ANALYZE execution operator |
| `internal/executor/physical_checkpoint.go` | ADDED | CHECKPOINT execution operator |
| `internal/catalog/catalog.go` | MODIFIED | Add statistics storage for ANALYZE |
| `internal/catalog/column.go` | MODIFIED | Add statistics fields to ColumnDef |
| `internal/catalog/statistics.go` | ADDED | Statistics storage and management |
| `internal/storage/storage.go` | MODIFIED | Add VACUUM and checkpoint methods |
| `internal/storage/table.go` | MODIFIED | Add vacuum and optimization methods |

### Dependencies

- This proposal depends on: (none)
- This proposal blocks: (none)

### Compatibility

- Full syntax compatibility with DuckDB PRAGMA statements
- EXPLAIN output format compatible with DuckDB text output
- System table schemas compatible with DuckDB `duckdb_*()` functions

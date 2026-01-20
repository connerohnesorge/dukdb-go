# Tasks: System Functions Implementation

## 1. Infrastructure Setup

- [ ] 1.1 Create `internal/metadata/` package with module structure
- [ ] 1.2 Define metadata data structures (TableMetadata, ColumnMetadata, etc.)
- [ ] 1.3 Implement helper functions in internal/metadata/tables.go
- [ ] 1.4 Implement helper functions in internal/metadata/columns.go
- [ ] 1.5 Implement helper functions in internal/metadata/constraints.go
- [ ] 1.6 Implement helper functions in internal/metadata/views.go
- [ ] 1.7 Implement helper functions in internal/metadata/indexes.go
- [ ] 1.8 Implement helper functions in internal/metadata/sequences.go
- [ ] 1.9 Implement helper functions in internal/metadata/functions.go
- [ ] 1.10 Implement helper functions in internal/metadata/settings.go
- [ ] 1.11 Implement helper functions in internal/metadata/types.go (type string conversion)
- [ ] 1.12 Add comprehensive unit tests for metadata helpers

## 2. System Function Implementations (Part 1 - Core)

- [ ] 2.1 Implement duckdb_settings() table function
- [ ] 2.2 Implement duckdb_tables() table function
- [ ] 2.3 Implement duckdb_columns() table function
- [ ] 2.4 Add tests for 2.1-2.3

## 3. System Function Implementations (Part 2 - Schema)

- [ ] 3.1 Implement duckdb_views() table function
- [ ] 3.2 Implement duckdb_functions() table function
- [ ] 3.3 Implement duckdb_constraints() table function
- [ ] 3.4 Implement duckdb_indexes() table function
- [ ] 3.5 Add tests for 3.1-3.4

## 4. System Function Implementations (Part 3 - Advanced)

- [ ] 4.1 Implement duckdb_databases() table function
- [ ] 4.2 Implement duckdb_sequences() table function
- [ ] 4.3 Implement duckdb_dependencies() table function
- [ ] 4.4 Implement duckdb_optimizers() table function
- [ ] 4.5 Implement duckdb_keywords() table function
- [ ] 4.6 Add tests for 4.1-4.5

## 5. System Function Implementations (Part 4 - Monitoring)

- [ ] 5.1 Implement duckdb_extensions() table function
- [ ] 5.2 Implement duckdb_memory_usage() table function
- [ ] 5.3 Implement duckdb_temp_directory() table function
- [ ] 5.4 Add tests for 5.1-5.3

## 6. Integration and Executor Changes

- [ ] 6.1 Extend internal/executor/table_function_*.go to dispatch system functions
- [ ] 6.2 Update internal/executor/operator.go for table function resolution
- [ ] 6.3 Ensure all 15 functions are registered in dispatcher
- [ ] 6.4 Add end-to-end integration tests

## 7. PRAGMA Statement Implementation

- [ ] 7.1 Add PRAGMA parsing to internal/parser/ (if not exists)
- [ ] 7.2 Implement PRAGMA database_size handler
- [ ] 7.3 Implement PRAGMA table_info handler
- [ ] 7.4 Implement PRAGMA database_list handler
- [ ] 7.5 Implement PRAGMA version handler
- [ ] 7.6 Implement PRAGMA platform handler
- [ ] 7.7 Implement PRAGMA functions handler
- [ ] 7.8 Implement PRAGMA collations handler
- [ ] 7.9 Implement PRAGMA table_storage_info handler
- [ ] 7.10 Implement PRAGMA storage_info handler
- [ ] 7.11 Add tests for all PRAGMA statements

## 8. Testing and Validation

- [ ] 8.1 Run all existing tests to ensure no regressions
- [ ] 8.2 Verify all system functions return correct column schemas
- [ ] 8.3 Verify functions handle empty/special cases (no tables, no views)
- [ ] 8.4 Verify functions respect schema visibility
- [ ] 8.5 Benchmark metadata queries for performance
- [ ] 8.6 Cross-validate against DuckDB v1.4.3 reference implementation
- [ ] 8.7 Test identifier escaping for special characters

## 9. Documentation and Polish

- [ ] 9.1 Add godoc comments to all public metadata functions
- [ ] 9.2 Document column schemas for each system function
- [ ] 9.3 Add examples for common system function queries
- [ ] 9.4 Update CLAUDE.md with system functions documentation

## Dependencies

- **Blocks**: `add-information-schema` proposal (information_schema views depend on metadata infrastructure)
- **Blocks**: `add-postgresql-catalog` proposal (pg_catalog views depend on metadata infrastructure)
- **Independent**: No other internal dependencies

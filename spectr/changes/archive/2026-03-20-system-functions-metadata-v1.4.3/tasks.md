# Tasks: System Functions and Metadata Tables (v1.4.3)

## 1. System Functions Implementation

- [x] 1.1. Implement duckdb_settings() table function
- [x] 1.2. Implement duckdb_tables() table function
- [x] 1.3. Implement duckdb_columns() table function
- [x] 1.4. Implement duckdb_views() table function
- [x] 1.5. Implement duckdb_functions() table function
- [x] 1.6. Implement duckdb_constraints() table function
- [x] 1.7. Implement duckdb_indexes() table function
- [x] 1.8. Implement duckdb_databases() table function
- [x] 1.9. Implement duckdb_sequences() table function
- [x] 1.10. Implement duckdb_dependencies() table function
- [x] 1.11. Implement duckdb_optimizers() table function
- [x] 1.12. Implement duckdb_keywords() table function
- [x] 1.13. Implement duckdb_extensions() table function
- [x] 1.14. Implement duckdb_memory_usage() table function
- [x] 1.15. Implement duckdb_temp_directory() table function

## 2. PRAGMA Support

- [x] 2.1. Implement PRAGMA parser (parser_pragma.go)
- [x] 2.2. Implement PRAGMA binder (bind_pragma.go)
- [x] 2.3. Implement PRAGMA executor framework (physical_maintenance.go)
- [x] 2.4. Implement PRAGMA database_size
- [x] 2.5. Implement PRAGMA table_info
- [x] 2.6. Implement PRAGMA storage_info
- [x] 2.7. Implement PRAGMA version
- [x] 2.8. Implement PRAGMA database_list
- [x] 2.9. Implement PRAGMA functions
- [x] 2.10. Implement PRAGMA collations
- [x] 2.11. Implement PRAGMA memory_limit
- [x] 2.12. Implement PRAGMA threads
- [x] 2.13. Implement PRAGMA enable/disable profiling
- [x] 2.14. Implement PRAGMA checkpoint_threshold

## 3. information_schema Views

- [x] 3.1. Implement information_schema.tables view
- [x] 3.2. Implement information_schema.columns view
- [x] 3.3. Implement information_schema.schemata view
- [x] 3.4. Implement information_schema.views view
- [x] 3.5. Implement information_schema.table_constraints view
- [x] 3.6. Implement information_schema.key_column_usage view
- [x] 3.7. Implement information_schema binder integration

## 4. pg_catalog Tables

- [x] 4.1. Implement pg_catalog.pg_namespace
- [x] 4.2. Implement pg_catalog.pg_class
- [x] 4.3. Implement pg_catalog.pg_attribute
- [x] 4.4. Implement pg_catalog.pg_type
- [x] 4.5. Implement pg_catalog.pg_tables
- [x] 4.6. Implement pg_catalog.pg_views
- [x] 4.7. Implement pg_catalog.pg_index
- [x] 4.8. Implement pg_catalog.pg_constraint
- [x] 4.9. Implement pg_catalog.pg_database
- [x] 4.10. Implement pg_catalog.pg_settings
- [x] 4.11. Implement pg_catalog.pg_roles / pg_user
- [x] 4.12. Implement pg_catalog binder integration

## 5. Integration and Testing

- [x] 5.1. Verify system functions e2e tests pass
- [x] 5.2. Verify information_schema e2e tests pass
- [x] 5.3. Verify pg_catalog e2e tests pass
- [x] 5.4. Verify PRAGMA parser tests pass
- [x] 5.5. Verify metadata retrieval infrastructure works

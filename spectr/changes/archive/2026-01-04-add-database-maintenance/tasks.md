# Tasks: Database Maintenance Commands

## 1. Parser Implementation

- [ ] 1.1 Add AST nodes to `internal/parser/ast.go`
  - [ ] 1.1.1 Add `PragmaStmt` struct with Name, Args, Assignment fields
  - [ ] 1.1.2 Add `ExplainStmt` struct with Query, Analyze fields
  - [ ] 1.1.3 Add `VacuumStmt` struct with TableName field
  - [ ] 1.1.4 Add `AnalyzeStmt` struct with TableName field
  - [ ] 1.1.5 Add `CheckpointStmt` struct (empty for now)

- [ ] 1.2 Add visitor methods to `internal/parser/visitor.go`
  - [ ] 1.2.1 Add `VisitPragmaStmt(stmt *PragmaStmt)`
  - [ ] 1.2.2 Add `VisitExplainStmt(stmt *ExplainStmt)`
  - [ ] 1.2.3 Add `VisitVacuumStmt(stmt *VacuumStmt)`
  - [ ] 1.2.4 Add `VisitAnalyzeStmt(stmt *AnalyzeStmt)`
  - [ ] 1.2.5 Add `VisitCheckpointStmt(stmt *CheckpointStmt)`

- [ ] 1.3 Create `internal/parser/parser_pragma.go` with parsing functions
  - [ ] 1.3.1 Create `parsePragma()` function
  - [ ] 1.3.2 Create `parseExplain()` function
  - [ ] 1.3.3 Create `parseVacuum()` function
  - [ ] 1.3.4 Create `parseAnalyze()` function
  - [ ] 1.3.5 Create `parseCheckpoint()` function

- [ ] 1.4 Update `internal/parser/parser.go` dispatch logic
  - [ ] 1.4.1 Add case for "PRAGMA" keyword in parse()
  - [ ] 1.4.2 Add case for "EXPLAIN" keyword in parse()
  - [ ] 1.4.3 Add case for "EXPLAIN ANALYZE" in parse()
  - [ ] 1.4.4 Add case for "VACUUM" keyword in parse()
  - [ ] 1.4.5 Add case for "ANALYZE" keyword in parse()
  - [ ] 1.4.6 Add case for "CHECKPOINT" keyword in parse()

- [ ] 1.5 Add parser tests for new statements
  - [ ] 1.5.1 Test PRAGMA database_size parsing
  - [ ] 1.5.2 Test PRAGMA table_info('table') parsing
  - [ ] 1.5.3 Test PRAGMA max_memory = value parsing
  - [ ] 1.5.4 Test EXPLAIN SELECT parsing
  - [ ] 1.5.5 Test EXPLAIN ANALYZE SELECT parsing
  - [ ] 1.5.6 Test VACUUM parsing
  - [ ] 1.5.7 Test ANALYZE parsing
  - [ ] 1.5.8 Test CHECKPOINT parsing

## 2. Binder Implementation

- [ ] 2.1 Update `internal/binder/binder.go` Bind() method
  - [ ] 2.1.1 Add case for `*parser.PragmaStmt`
  - [ ] 2.1.2 Add case for `*parser.ExplainStmt`
  - [ ] 2.1.3 Add case for `*parser.VacuumStmt`
  - [ ] 2.1.4 Add case for `*parser.AnalyzeStmt`
  - [ ] 2.1.5 Add case for `*parser.CheckpointStmt`

- [ ] 2.2 Create `internal/binder/bind_pragma.go`
  - [ ] 2.2.1 Create `bindPragma()` function
  - [ ] 2.2.2 Create `bindExplain()` function
  - [ ] 2.2.3 Create `bindVacuum()` function
  - [ ] 2.2.4 Create `bindAnalyze()` function
  - [ ] 2.2.5 Create `bindCheckpoint()` function

- [ ] 2.3 Add bound statement types to `internal/binder/expressions.go`
  - [ ] 2.3.1 Add `BoundPragmaStmt` struct
  - [ ] 2.3.2 Add `BoundExplainStmt` struct
  - [ ] 2.3.3 Add `BoundVacuumStmt` struct
  - [ ] 2.3.4 Add `BoundAnalyzeStmt` struct
  - [ ] 2.3.5 Add `BoundCheckpointStmt` struct

- [ ] 2.4 Add binder tests
  - [ ] 2.4.1 Test PRAGMA binding validation
  - [ ] 2.4.2 Test EXPLAIN binding (child query)
  - [ ] 2.4.3 Test VACUUM table validation
  - [ ] 2.4.4 Test ANALYZE table validation

## 3. Planner Implementation

- [ ] 3.1 Update `internal/planner/logical.go`
  - [ ] 3.1.1 Add `LogicalPragma` node
  - [ ] 3.1.2 Add `LogicalExplain` node with Child and Analyze fields
  - [ ] 3.1.3 Add `LogicalVacuum` node
  - [ ] 3.1.4 Add `LogicalAnalyze` node
  - [ ] 3.1.5 Add `LogicalCheckpoint` node

- [ ] 3.2 Update `internal/planner/physical.go`
  - [ ] 3.2.1 Add `PhysicalPragma` node
  - [ ] 3.2.2 Add `PhysicalExplain` node
  - [ ] 3.2.3 Add `PhysicalVacuum` node
  - [ ] 3.2.4 Add `PhysicalAnalyze` node
  - [ ] 3.2.5 Add `PhysicalCheckpoint` node
  - [ ] 3.2.6 Update `createLogicalPlan()` to handle new statement types
  - [ ] 3.2.7 Update `createPhysicalPlan()` to handle new logical nodes

- [ ] 3.3 Add planner tests
  - [ ] 3.3.1 Test PRAGMA plan generation
  - [ ] 3.3.2 Test EXPLAIN plan generation
  - [ ] 3.3.3 Test VACUUM plan generation
  - [ ] 3.3.4 Test ANALYZE plan generation

## 4. Executor Implementation - PRAGMA

- [ ] 4.1 Create `internal/executor/physical_pragma.go`
  - [ ] 4.1.1 Define `PhysicalPragma` struct
  - [ ] 4.1.2 Implement `GetTypes()` method
  - [ ] 4.1.3 Implement `executePragma()` function
  - [ ] 4.1.4 Implement `executeDatabaseSize()` handler
  - [ ] 4.1.5 Implement `executeTableInfo()` handler
  - [ ] 4.1.6 Implement `executeStorageInfo()` handler
  - [ ] 4.1.7 Implement `executeShowTables()` handler
  - [ ] 4.1.8 Implement `executeFunctions()` handler
  - [ ] 4.1.9 Implement `executeMaxMemory()` handler
  - [ ] 4.1.10 Implement `executeThreads()` handler
  - [ ] 4.1.11 Implement `executeEnableProfiling()` handler
  - [ ] 4.1.12 Implement `executeDisableProfiling()` handler
  - [ ] 4.1.13 Implement `executeProfilingMode()` handler

- [ ] 4.2 Update `internal/executor/executor.go`
  - [ ] 4.2.1 Add case for `*planner.PhysicalPragma` in Execute()
  - [ ] 4.2.2 Add case for `*planner.PhysicalExplain` in Execute()
  - [ ] 4.2.3 Add case for `*planner.PhysicalVacuum` in Execute()
  - [ ] 4.2.4 Add case for `*planner.PhysicalAnalyze` in Execute()
  - [ ] 4.2.5 Add case for `*planner.PhysicalCheckpoint` in Execute()

- [ ] 4.3 Add executor PRAGMA tests
  - [ ] 4.3.1 Test PRAGMA database_size execution
  - [ ] 4.3.2 Test PRAGMA table_info execution
  - [ ] 4.3.3 Test PRAGMA functions execution
  - [ ] 4.3.4 Test PRAGMA max_memory setting

## 5. Executor Implementation - EXPLAIN

- [ ] 5.1 Create `internal/executor/physical_explain.go`
  - [ ] 5.1.1 Define `PhysicalExplain` struct
  - [ ] 5.1.2 Implement `GetTypes()` method
  - [ ] 5.1.3 Implement `executeExplain()` function
  - [ ] 5.1.4 Implement `formatPlan()` function for tree output
  - [ ] 5.1.5 Implement `executeExplainAnalyze()` function with metrics

- [ ] 5.2 Add EXPLAIN tests
  - [ ] 5.2.1 Test EXPLAIN SELECT output format
  - [ ] 5.2.2 Test EXPLAIN with JOIN
  - [ ] 5.2.3 Test EXPLAIN with AGGREGATE
  - [ ] 5.2.4 Test EXPLAIN ANALYZE execution and metrics

## 6. Executor Implementation - VACUUM

- [ ] 6.1 Create `internal/executor/physical_vacuum.go`
  - [ ] 6.1.1 Define `PhysicalVacuum` struct
  - [ ] 6.1.2 Implement `executeVacuum()` function

- [ ] 6.2 Update `internal/storage/table.go`
  - [ ] 6.2.1 Add `DeletedRows()` method to track deleted rows
  - [ ] 6.2.2 Add `Vacuum()` method to reclaim space
  - [ ] 6.2.3 Add `Size()` method for current table size

- [ ] 6.3 Add VACUUM tests
  - [ ] 6.3.1 Test VACUUM reclaims space after DELETE
  - [ ] 6.3.2 Test VACUUM on table with no deleted rows
  - [ ] 6.3.3 Test VACUUM on non-existent table (error)

## 7. Executor Implementation - ANALYZE

- [ ] 7.1 Create `internal/catalog/statistics.go`
  - [ ] 7.1.1 Define `ColumnStatistics` struct
  - [ ] 7.1.2 Define `TableStatistics` struct
  - [ ] 7.1.3 Add `UpdateStatistics()` method

- [ ] 7.2 Update `internal/catalog/column.go`
  - [ ] 7.2.1 Add `Statistics` field to `ColumnDef`
  - [ ] 7.2.2 Add `WithStatistics()` constructor option

- [ ] 7.3 Create `internal/executor/physical_analyze.go`
  - [ ] 7.3.1 Define `PhysicalAnalyze` struct
  - [ ] 7.3.2 Implement `executeAnalyze()` function
  - [ ] 7.3.3 Implement `collectColumnStatistics()` function

- [ ] 7.4 Add ANALYZE tests
  - [ ] 7.4.1 Test ANALYZE collects min/max values
  - [ ] 7.4.2 Test ANALYZE collects null counts
  - [ ] 7.4.3 Test ANALYZE on non-existent table (error)
  - [ ] 7.4.4 Test statistics storage in catalog

## 8. Executor Implementation - CHECKPOINT

- [ ] 8.1 Create `internal/executor/physical_checkpoint.go`
  - [ ] 8.1.1 Define `PhysicalCheckpoint` struct
  - [ ] 8.1.2 Implement `executeCheckpoint()` function

- [ ] 8.2 Update `internal/storage/storage.go`
  - [ ] 8.2.1 Add `Checkpoint()` method for full checkpoint
  - [ ] 8.2.2 Add `MergeRowGroups()` helper
  - [ ] 8.2.3 Add `WriteCheckpointHeader()` helper

- [ ] 8.3 Add CHECKPOINT tests
  - [ ] 8.3.1 Test CHECKPOINT creates valid checkpoint
  - [ ] 8.3.2 Test CHECKPOINT truncates WAL
  - [ ] 8.3.3 Test checkpoint recovery

## 9. System Tables Implementation

- [ ] 9.1 Create `internal/catalog/system_tables.go`
  - [ ] 9.1.1 Define `SystemTable` struct implementing `dukdb.VirtualTable`
  - [ ] 9.1.2 Create `NewSystemTableDef()` function
  - [ ] 9.1.3 Implement `duckdb_tables` virtual table
  - [ ] 9.1.4 Implement `duckdb_columns` virtual table
  - [ ] 9.1.5 Implement `duckdb_functions` virtual table
  - [ ] 9.1.6 Implement `duckdb_settings` virtual table
  - [ ] 9.1.7 Implement `duckdb_types` virtual table
  - [ ] 9.1.8 Implement `duckdb_views` virtual table

- [ ] 9.2 Update `internal/catalog/catalog.go`
  - [ ] 9.2.1 Add `RegisterSystemTables()` function
  - [ ] 9.2.2 Call from `NewCatalog()` to register system tables

- [ ] 9.3 Add system table tests
  - [ ] 9.3.1 Test SELECT * FROM duckdb_tables
  - [ ] 9.3.2 Test SELECT * FROM duckdb_columns
  - [ ] 9.3.3 Test SELECT * FROM duckdb_functions
  - [ ] 9.3.4 Test system table schema matches DuckDB

## 10. Integration Tests

- [ ] 10.1 Create comprehensive integration tests
  - [ ] 10.1.1 Test full PRAGMA workflow
  - [ ] 10.1.2 Test EXPLAIN for complex queries
  - [ ] 10.1.3 Test VACUUM after bulk operations
  - [ ] 10.1.4 Test ANALYZE after data changes
  - [ ] 10.1.5 Test CHECKPOINT and recovery

- [ ] 10.2 Create compatibility tests
  - [ ] 10.2.1 Test PRAGMA output format matches DuckDB
  - [ ] 10.2.2 Test EXPLAIN output format matches DuckDB
  - [ ] 10.2.3 Test system table schemas match DuckDB

## 11. Documentation

- [ ] 11.1 Add PRAGMA documentation
  - [ ] 11.1.1 Document information PRAGMAs (database_size, table_info, etc.)
  - [ ] 11.1.2 Document configuration PRAGMAs (max_memory, threads, etc.)
  - [ ] 11.1.3 Document profiling PRAGMAs

- [ ] 11.2 Add EXPLAIN documentation
  - [ ] 11.2.1 Document EXPLAIN syntax
  - [ ] 11.2.2 Document EXPLAIN ANALYZE syntax
  - [ ] 11.2.3 Document output format

- [ ] 11.3 Add VACUUM documentation
  - [ ] 11.3.1 Document VACUUM syntax
  - [ ] 11.3.2 Document when to use VACUUM

- [ ] 11.4 Add ANALYZE documentation
  - [ ] 11.4.1 Document ANALYZE syntax
  - [ ] 11.4.2 Document statistics usage

- [ ] 11.5 Add system tables documentation
  - [ ] 11.5.1 Document duckdb_tables schema and usage
  - [ ] 11.5.2 Document duckdb_columns schema and usage
  - [ ] 11.5.3 Document other system tables

## 1. Parser: EXPORT/IMPORT DATABASE Statements

- [ ] 1.1 Add `ExportDatabaseStmt` and `ImportDatabaseStmt` AST nodes to `internal/parser/ast.go`
- [ ] 1.2 Implement EXPORT DATABASE parsing with path and optional options list
- [ ] 1.3 Implement IMPORT DATABASE parsing with path
- [ ] 1.4 Add parser unit tests for EXPORT/IMPORT DATABASE syntax variants

## 2. Catalog: DDL Generation Helpers

- [ ] 2.1 Create `internal/catalog/ddl_gen.go` with `TableDef.ToCreateSQL()` method
- [ ] 2.2 Add `ViewDef.ToCreateSQL()` method
- [ ] 2.3 Add `SequenceDef.ToCreateSQL()` method
- [ ] 2.4 Add `IndexDef.ToCreateSQL()` method
- [ ] 2.5 Add unit tests verifying DDL round-trip (generate SQL → parse → compare)

## 3. Planner: Physical Plan Nodes

- [ ] 3.1 Add `PhysicalExportDatabase` and `PhysicalImportDatabase` to `internal/planner/physical.go`
- [ ] 3.2 Wire statement binding and planning for EXPORT/IMPORT DATABASE

## 4. Executor: EXPORT DATABASE

- [ ] 4.1 Create `internal/executor/export_import.go` with `executeExportDatabase`
- [ ] 4.2 Implement catalog object enumeration across all schemas
- [ ] 4.3 Implement dependency ordering (schemas → types → sequences → tables → macros → views → indexes)
- [ ] 4.4 Implement schema.sql generation using DDL helpers (unqualified for main schema, qualified for others)
- [ ] 4.5 Implement table data export via COPY TO (reuse executeCopyTo)
- [ ] 4.6 Implement load.sql generation with filename-only paths (no absolute paths)
- [ ] 4.7 Support FORMAT option (CSV default, PARQUET, JSON)
- [ ] 4.8 Support forwarding COPY options (DELIMITER, HEADER, etc.)
- [ ] 4.9 Add force_not_null option for NOT NULL columns in CSV export

## 5. Executor: IMPORT DATABASE

- [ ] 5.1 Implement `executeImportDatabase` with directory validation
- [ ] 5.2 Implement schema.sql reading and statement-by-statement execution
- [ ] 5.3 Implement load.sql reading with path reconstruction (join import dir + filename)
- [ ] 5.4 Execute COPY FROM statements sequentially
- [ ] 5.5 Add error handling for non-empty database detection

## 6. Integration Tests

- [ ] 6.1 End-to-end test: Export and import single table (CSV round-trip)
- [ ] 6.2 End-to-end test: Export and import with FORMAT PARQUET
- [ ] 6.3 End-to-end test: Export with multiple schemas
- [ ] 6.4 End-to-end test: Export with views, sequences, and indexes
- [ ] 6.5 End-to-end test: Dependency ordering verification
- [ ] 6.6 End-to-end test: Import into non-empty database fails
- [ ] 6.7 End-to-end test: Import from nonexistent directory fails
- [ ] 6.8 End-to-end test: Data integrity verification after round-trip

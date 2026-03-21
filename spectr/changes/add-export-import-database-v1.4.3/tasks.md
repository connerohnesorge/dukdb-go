## 1. Sequence DDL Generation

- [x] 1.1 Implement `generateCreateSequenceSQL(seq *catalog.SequenceDef) string` in `internal/engine/export_import.go`
- [x] 1.2 Add sequence enumeration loop in `handleExportDatabase` calling `schema.ListSequences()` for each schema
- [x] 1.3 Emit CREATE SEQUENCE statements in schema.sql after CREATE SCHEMA and before CREATE TABLE
- [ ] 1.4 Write tests for sequence DDL generation with various options (START WITH, INCREMENT BY, CYCLE, schema-qualified names)

## 2. Index DDL Generation

- [x] 2.1 Implement `generateCreateIndexSQL(idx *catalog.IndexDef) string` in `internal/engine/export_import.go`
- [x] 2.2 Add index enumeration loop in `handleExportDatabase` calling `schema.ListIndexes()` for each schema
- [x] 2.3 Skip indexes where `IsPrimary == true` to avoid duplicate PK declarations
- [x] 2.4 Emit CREATE INDEX statements in schema.sql after CREATE VIEW
- [ ] 2.5 Write tests for index DDL generation (unique, multi-column, schema-qualified, primary key skip)

## 3. FORMAT Option Support

- [x] 3.1 Read and normalize `stmt.Options["FORMAT"]` in `handleExportDatabase` (default to CSV)
- [x] 3.2 Implement `exportTableToParquet(tableDef *catalog.TableDef, path string) error` using `internal/io/parquet` writer
- [x] 3.3 Implement `exportTableToJSON(tableDef *catalog.TableDef, path string) error` using `internal/io/json` writer
- [x] 3.4 Route export to correct method based on FORMAT option (CSV, PARQUET, JSON)
- [x] 3.5 Generate load.sql COPY FROM statements with correct FORMAT clause per format
- [x] 3.6 Return `&dukdb.Error{Msg: "unsupported export format: ..."}` for unknown formats
- [ ] 3.7 Write tests for each format (CSV default, PARQUET, JSON, unknown format error)

## 4. Multi-Schema Data File Naming

- [x] 4.1 Update data file naming in `handleExportDatabase` to use `{schema}_{table}.{ext}` for non-main schemas
- [x] 4.2 Update load.sql generation to reference the correct schema-prefixed filenames
- [ ] 4.3 Write tests for multi-schema file naming with colliding table names across schemas

## 5. Dependency Ordering

- [x] 5.1 Restructure `handleExportDatabase` schema.sql generation into ordered phases: schemas, sequences, tables, views, indexes
- [ ] 5.2 Write test verifying DDL order in schema.sql with all object types present

## 6. DEFAULT Clause Support

- [x] 6.1 Update `generateCreateTableSQL` to emit DEFAULT clause when `col.HasDefault` (not `col.DefaultValue != nil`, since NULL defaults are valid)
- [x] 6.2 Implement `formatDefaultValue(val any) string` helper for proper SQL literal formatting (strings quoted, booleans lowercase, etc.)
- [ ] 6.3 Write tests for DEFAULT clause generation with various types (string, integer, boolean, nil)

## 7. Integration Tests

- [ ] 7.1 Write round-trip test: create database with schemas, sequences, tables, views, indexes, defaults; export; import into fresh database; verify all objects and data match
- [ ] 7.2 Write round-trip test with FORMAT PARQUET
- [ ] 7.3 Write round-trip test with FORMAT JSON

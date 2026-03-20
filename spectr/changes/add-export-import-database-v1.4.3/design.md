## Implementation Details

All changes are in `internal/engine/export_import.go` unless noted otherwise.

### Directory Layout

An exported database directory contains:

```
<export-path>/
  schema.sql    -- DDL to recreate all objects (schemas, sequences, tables, views, indexes)
  load.sql      -- COPY FROM statements to reload data files
  <table>.csv   -- Default CSV data files (one per table)
  <table>.parquet  -- When FORMAT PARQUET
  <table>.json     -- When FORMAT JSON
```

For multi-schema databases, data files for non-main schemas use `{schema}_{table}.{ext}` to avoid name collisions.

### DDL Generation Order in schema.sql

Objects are emitted in strict dependency order:

1. `CREATE SCHEMA` -- non-main schemas
2. `CREATE SEQUENCE` -- sequences (may be referenced by table defaults)
3. `CREATE TABLE` -- tables with columns, types, NOT NULL, DEFAULT, PRIMARY KEY
4. `CREATE VIEW` -- views (may reference tables)
5. `CREATE INDEX` / `CREATE UNIQUE INDEX` -- indexes on tables (skip IsPrimary since PK is inline)

### generateCreateTableSQL Enhancements

Add DEFAULT clause support. The `ColumnDef` struct has both `DefaultValue any` and `HasDefault bool`; we must check `HasDefault` (not just `DefaultValue != nil`) because a NULL default is still a valid default:

```go
if col.HasDefault {
    sb.WriteString(fmt.Sprintf(" DEFAULT %v", formatDefaultValue(col.DefaultValue)))
}
```

### generateCreateSequenceSQL (new function)

```go
func generateCreateSequenceSQL(seq *catalog.SequenceDef) string
```

Produces: `CREATE SEQUENCE [schema.]name START WITH n INCREMENT BY n MINVALUE n MAXVALUE n [CYCLE|NO CYCLE];`

Uses fields: `Name`, `Schema`, `StartWith`, `IncrementBy`, `MinValue`, `MaxValue`, `IsCycle`.

### generateCreateIndexSQL (new function)

```go
func generateCreateIndexSQL(idx *catalog.IndexDef) string
```

Produces: `CREATE [UNIQUE] INDEX name ON [schema.]table (col1, col2, ...);`

Skips indexes where `IsPrimary == true` (already declared inline in CREATE TABLE).

### FORMAT Option Handling

Read `stmt.Options["FORMAT"]` (case-insensitive comparison):

| FORMAT value | Data file extension | Export method | COPY FROM option |
|---|---|---|---|
| `""` or `"CSV"` | `.csv` | `exportTableToCSV` (existing) | `FORMAT CSV, HEADER true` |
| `"PARQUET"` | `.parquet` | new `exportTableToParquet` using `internal/io/parquet` writer | `FORMAT PARQUET` |
| `"JSON"` | `.json` | new `exportTableToJSON` using `internal/io/json` writer | `FORMAT JSON` |

Additional CSV options (DELIMITER, HEADER, NULL) from `stmt.Options` are forwarded to both the CSV writer and the COPY FROM statement in load.sql.

### exportTableToParquet (new function)

```go
func (c *EngineConn) exportTableToParquet(tableDef *catalog.TableDef, path string) error
```

Scans table data and writes a Parquet file using the existing `internal/io/parquet` writer infrastructure.

### exportTableToJSON (new function)

```go
func (c *EngineConn) exportTableToJSON(tableDef *catalog.TableDef, path string) error
```

Scans table data and writes NDJSON format using the existing `internal/io/json` writer infrastructure.

### Error Handling

- Return `&dukdb.Error{Msg: "unsupported export format: ..."}` for unknown FORMAT values
- Return descriptive errors for directory creation failures
- Return errors if export path parent directory does not exist

## Context

The parser already supports `EXPORT DATABASE 'path' (FORMAT PARQUET, DELIMITER '|', ...)` and stores options in `ExportDatabaseStmt.Options`. The import side relies entirely on executing the generated schema.sql and load.sql, so fixing the export automatically fixes the round-trip import.

## Goals / Non-Goals

- Goals: Full round-trip export/import of schemas, sequences, tables (with defaults and PKs), views, and indexes in CSV, Parquet, and JSON formats
- Non-Goals: Foreign key export (not yet implemented in catalog), materialized view export, custom compression options for Parquet export

## Decisions

- Decision: Use NDJSON (not JSON array) for JSON format export because COPY FROM expects NDJSON
- Alternatives considered: JSON array format; rejected because load.sql uses COPY which expects record-per-line

- Decision: Skip IsPrimary indexes in CREATE INDEX since they are already declared inline in CREATE TABLE
- Alternatives considered: Emit CREATE INDEX for all indexes; rejected because it would cause duplicate constraint errors on import

## Risks / Trade-offs

- Large table export may use significant memory for Parquet (Parquet writer may buffer) -- mitigated by using chunk-at-a-time scanning
- JSON export is slower than CSV for large datasets -- acceptable since FORMAT JSON is opt-in

## Open Questions

- None at this time; all required catalog APIs (ListSequences, ListIndexes) and I/O writers (CSV, Parquet, JSON) already exist

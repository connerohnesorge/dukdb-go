# Change: Add ATTACH/DETACH Database Support

## Why

DuckDB v1.4.3 supports attaching multiple databases to a single connection, enabling cross-database queries, data federation, and multi-file workflows. This is a core DuckDB feature used extensively in analytics pipelines to join data across separate database files, attach read-only reference databases, and manage data across environments. Currently dukdb-go has zero multi-database support—no ATTACH, DETACH, USE, CREATE DATABASE, or DROP DATABASE statements—which blocks compatibility with DuckDB v1.4.3 and prevents common analytical workflows.

## What Changes

- **Parser**: Add AST nodes and parsing for `ATTACH [DATABASE] 'path' [AS alias] [(options)]`, `DETACH [DATABASE] name`, `USE database[.schema]`, `CREATE DATABASE name`, `DROP DATABASE name`
- **Engine**: Add `DatabaseManager` that maintains a registry of attached databases, each with their own `Catalog`, `Storage`, and `WAL`
- **Binder**: Extend name resolution to support 3-part qualified names (`database.schema.table`) across all statement types that reference tables
- **Executor**: Add execution operators for ATTACH, DETACH, USE, CREATE DATABASE, DROP DATABASE
- **Planner**: Support cross-database table references in query plans
- **Transaction Manager**: Coordinate transactions across multiple attached databases
- **System Functions**: Update `duckdb_databases()` to reflect attached databases; update `PRAGMA database_list` to show all attached databases

## Impact

- Affected specs: `parser`, `execution-engine`, `catalog`, `persistence`, `storage`, `system-functions`
- Affected code:
  - `internal/parser/ast.go` — new AST node types
  - `internal/parser/parser.go` — parse ATTACH/DETACH/USE/CREATE DATABASE/DROP DATABASE
  - `internal/engine/engine.go` — DatabaseManager integration
  - `internal/engine/conn.go` — connection-level default database tracking
  - `internal/binder/bind_stmt.go` — 3-part name resolution
  - `internal/executor/ddl.go` — execute ATTACH/DETACH/USE
  - `internal/catalog/catalog.go` — per-database catalog instances
  - `internal/wal/` — per-database WAL files
  - `backend.go` — StmtType constants already exist (STATEMENT_TYPE_ATTACH/DETACH)

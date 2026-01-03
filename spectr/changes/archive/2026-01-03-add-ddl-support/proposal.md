# Change: Full DDL Support

## Why

Currently, dukdb-go only supports basic `CREATE TABLE` and `DROP TABLE` operations. DuckDB v1.4.3 provides comprehensive DDL support including views, indexes, sequences, and schemas that are essential for production database workloads. Without full DDL support:

1. **Production Limitations**: Cannot create views for query abstraction or materialized perspectives
2. **Performance Gaps**: Missing index support prevents query optimization
3. **Identity Columns**: No sequence support limits auto-incrementing column implementations
4. **Schema Organization**: Cannot organize tables into namespaces for multi-tenant or complex schemas
5. **Migration Barriers**: Existing DuckDB databases with DDL cannot be fully migrated

The WAL already has placeholder entries (`CreateViewEntry`, `CreateIndexEntry`, `CreateSequenceEntry`, etc.) defined in `internal/wal/entry_catalog.go` that are not yet wired into the execution pipeline.

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

- **CREATE VIEW / DROP VIEW**: Support for named query definitions that can be queried like tables
- **CREATE INDEX / DROP INDEX**: Support for simple hash indexes with column specifications
- **CREATE SEQUENCE / DROP SEQUENCE**: Support for sequence objects with configurable options
- **CREATE SCHEMA / DROP SCHEMA**: Support for schema namespaces with cross-schema table resolution
- **ALTER TABLE Extensions**: Rename table, rename column, drop column, set table options

### Internal Changes

- New AST node types: `CreateViewStmt`, `DropViewStmt`, `CreateIndexStmt`, `DropIndexStmt`, `CreateSequenceStmt`, `DropSequenceStmt`, `CreateSchemaStmt`, `DropSchemaStmt`, `AlterTableStmt`
- New catalog entry types: `ViewDef`, `IndexDef`, `SequenceDef`
- New bound statement types: `BoundCreateViewStmt`, `BoundDropViewStmt`, etc.
- WAL integration: Wire up existing entry types to handlers
- Parser updates: Extended dispatch logic for new statement types
- Binder updates: View resolution (expand view as subquery), schema-aware table lookup

## Impact

### Affected Specs

- `parser`: New DDL statement parsing capability
- `catalog`: View, index, sequence, and schema namespace management
- `binder`: DDL binding and view resolution
- `wal`: WAL entry handling for DDL operations

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/parser/ast.go` | MODIFIED | Add DDL statement AST nodes |
| `internal/parser/parser.go` | MODIFIED | Add DDL parsing functions and dispatch |
| `internal/parser/visitor.go` | MODIFIED | Add visitor methods for new statements |
| `internal/binder/bind_stmt.go` | MODIFIED | Add DDL binding functions |
| `internal/binder/binder.go` | MODIFIED | Add DDL cases to Bind() |
| `internal/catalog/catalog.go` | MODIFIED | Add view, index, sequence management |
| `internal/catalog/schema.go` | MODIFIED | Add views, indexes, sequences to schema |
| `internal/catalog/view.go` | ADDED | ViewDef and view management |
| `internal/catalog/index.go` | ADDED | IndexDef and index management |
| `internal/catalog/sequence.go` | ADDED | SequenceDef and sequence management |
| `internal/wal/entry_handler.go` | ADDED | Handle DDL WAL entries |
| `internal/planner/logical.go` | MODIFIED | Add DDL logical plan nodes |
| `internal/planner/physical.go` | MODIFIED | Add DDL physical plan nodes |
| `internal/executor/ddl.go` | ADDED | DDL execution operators |

### Dependencies

- This proposal depends on: (none)
- This proposal blocks: (none)

### Compatibility

- Full syntax compatibility with DuckDB v1.4.3 DDL statements
- Views behave like DuckDB views (expand at query time)
- Indexes initially as simple hash indexes (ART as future work)
- Sequences compatible with DuckDB sequence semantics

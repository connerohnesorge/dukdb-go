# Change: Wire ATTACH/DETACH/USE Database Execution for DuckDB v1.4.3 Compatibility

**Change ID:** `add-attach-detach-execution-v1.4.3`
**Created:** 2026-03-22
**Status:** PROPOSED
**Scope:** Medium — Planner, executor, engine wiring
**Estimated Complexity:** Medium — Parser and engine infrastructure exist; need planner nodes and executor dispatch
**User-Visible:** Yes — Enables multi-database workflows

## Why

DuckDB v1.4.3 supports ATTACH/DETACH for working with multiple databases simultaneously (e.g., `ATTACH 'analytics.db' AS analytics; SELECT * FROM analytics.sales`). The dukdb-go codebase already has:
- Parser AST nodes: `AttachStmt`, `DetachStmt`, `UseStmt`, `CreateDatabaseStmt`, `DropDatabaseStmt` (ast.go:1575-1650)
- Engine infrastructure: `DatabaseManager` with `Attach()`, `Detach()`, `Get()`, `Use()`, `List()` methods (engine/database_manager.go)
- Catalog per database: Each attached database has its own `Catalog` and `Storage`

What's missing is the **planner physical plan nodes** and **executor dispatch** to connect parsed statements to the engine's DatabaseManager. The SQL statements parse correctly but execution falls through with no handler.

## What Changes

- **Planner**: Add `PhysicalAttach`, `PhysicalDetach`, `PhysicalUse`, `PhysicalCreateDatabase`, `PhysicalDropDatabase` plan nodes in `internal/planner/physical.go`
- **Executor**: Add execution handlers in `internal/executor/ddl.go` that call `engine.DatabaseManager.Attach()`, `.Detach()`, `.Use()` etc.
- **Cross-database queries**: Wire catalog resolution to check attached databases when schema-qualified names include a database prefix (e.g., `analytics.main.sales`)
- **Engine**: Ensure `ATTACH` opens/creates a database file, initializes its own Catalog and Storage, and registers it with DatabaseManager

## Impact

- Affected specs: `multi-database` (new capability)
- Affected code:
  - `internal/planner/physical.go` — new physical plan nodes
  - `internal/executor/ddl.go` — execution handlers for ATTACH/DETACH/USE/CREATE DATABASE/DROP DATABASE
  - `internal/executor/operator.go` — operator type registration
  - `internal/engine/engine.go` — database file open/create during ATTACH
  - `internal/engine/database_manager.go` — already implemented, just needs to be called
  - `internal/binder/bind_expr.go` — cross-database name resolution (db.schema.table)

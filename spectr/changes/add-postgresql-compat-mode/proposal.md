# Change: Add PostgreSQL Compatibility Mode

## Why

PostgreSQL compatibility mode enables users to use DuckDB with tools and ORMs designed for PostgreSQL, reducing friction for migrations and multi-database applications. Key benefits:
1. **ORM Compatibility**: ActiveRecord, Hibernate, SQLAlchemy, TypeORM, etc.
2. **Tooling**: pgAdmin, DBeaver, DataGrip PostgreSQL connections
3. **Migration Path**: Move PostgreSQL workloads to DuckDB with minimal changes
4. **Testing**: Test PostgreSQL applications against DuckDB locally

DuckDB has a PostgreSQL wire protocol compatibility mode that dukdb-go should expose.

## What Changes

- **ADDED**: PostgreSQL wire protocol server mode
- **ADDED**: PostgreSQL type system compatibility layer
- **ADDED**: PostgreSQL function aliases (e.g., `now()` → `current_timestamp`)
- **ADDED**: PostgreSQL syntax support (e.g., `LIMIT ALL`, `DISTINCT ON`)
- **ADDED**: pg_catalog compatibility views
- **ADDED**: Information schema compliance

## Impact

- Affected specs: `specs/sql-dialect/spec.md`
- Affected code:
  - `internal/duckdb/` - PostgreSQL wire protocol server
  - `internal/parser/` - PostgreSQL syntax parsing
  - `internal/types/` - PostgreSQL type aliases
  - `internal/functions/` - PostgreSQL function aliases
- Breaking changes: None
- Dependencies: PostgreSQL wire protocol library (pure Go preferred)

## Priority

**LOW** - PostgreSQL compatibility is a convenience feature, not a core requirement. Lower priority than file formats and production reliability features.

## Note

This is a significant undertaking. The scope should be validated before implementation:
1. Wire protocol compatibility (full PostgreSQL wire protocol)
2. Type system compatibility (PostgreSQL types → DuckDB types)
3. SQL dialect compatibility (PostgreSQL syntax → DuckDB syntax)

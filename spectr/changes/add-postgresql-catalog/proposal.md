# Change: PostgreSQL pg_catalog Compatibility Views

## Why

PostgreSQL's `pg_catalog` schema is the de facto standard for database introspection among database tools and ORMs. Implementing it enables:
- Tool compatibility (DBeaver, pgAdmin, psql all rely heavily on pg_catalog)
- ORM compatibility (SQLAlchemy, Django ORM, Entity Framework, GORM check pg_catalog)
- Migration tooling (flyway, liquibase, alembic expect pg_catalog)
- Broader ecosystem adoption (most mature database tooling assumes pg_catalog exists)
- Client library compatibility (psql driver libraries check pg_catalog)

Combined with `information_schema`, pg_catalog provides complete metadata access for tools that may not understand DuckDB's native system functions.

## What Changes

This proposal implements 13 views in the `pg_catalog` schema providing PostgreSQL-compatible metadata access:

- `pg_catalog.pg_namespace` - Schemas
- `pg_catalog.pg_class` - Tables, views, indexes
- `pg_catalog.pg_attribute` - Columns
- `pg_catalog.pg_type` - Data types
- `pg_catalog.pg_proc` - Functions
- `pg_catalog.pg_index` - Indexes
- `pg_catalog.pg_constraint` - Constraints
- `pg_catalog.pg_database` - Databases
- `pg_catalog.pg_user` / `pg_catalog.pg_roles` - Users/roles
- `pg_catalog.pg_tables` - User tables
- `pg_catalog.pg_views` - User views
- `pg_catalog.pg_settings` - Configuration settings
- `pg_catalog.pg_stat_activity` - Active connections
- `pg_catalog.pg_stat_statements` - Query statistics (via provider interface)

## Architectural Approach

### View Implementation Strategy
- Implement as virtual tables (computed from catalog and monitor providers)
- Each view queries metadata infrastructure from `add-system-functions`
- Views accessible as `SELECT * FROM pg_catalog.pg_class` etc.
- Column schema matches PostgreSQL v14 conventions (with appropriate type mapping)

### OID System
- Implement lightweight OID (object identifier) system for system catalog objects
- OIDs for schemas, tables, columns, types, functions are deterministic
- Enables JOINs across pg_catalog views (e.g., pg_class JOIN pg_namespace)
- OIDs stable across sessions but reset on each database connection

### Registration and Initialization
- Create `internal/postgres/catalog/pg_catalog.go` file (extends existing)
- Add pg_catalog views to virtual table registry
- Initialize at engine startup

### Monitoring Provider Integration
- `pg_stat_activity` uses backend monitoring infrastructure
- `pg_stat_statements` uses query statistics provider (if available)
- Both support lazy initialization (empty if not available)

## Impact

- **Affected specs**: `postgresql-catalog` (new)
- **Affected code**:
  - `internal/postgres/catalog/` - add pg_catalog views
  - May extend `internal/monitor/` for stats providers

- **Dependencies**:
  - **Depends On**: `add-system-functions` (requires metadata infrastructure)
  - **Depends On**: `add-information-schema` (pg_catalog complements information_schema)
  - **Blocks**: Nothing directly

- **Not Breaking**: Pure additions; existing API unchanged

## Success Criteria

1. All 13+ pg_catalog views are queryable
2. Views return correct column schemas matching PostgreSQL v14
3. OID system enables correct JOINs across views
4. pg_namespace, pg_class, pg_attribute work together correctly
5. DBeaver/pgAdmin recognize pg_catalog and database structure
6. Tools can introspect types and functions
7. pg_stat_activity shows current connections
8. NULL values returned for system tables/schemas
9. Performance acceptable for typical workloads
10. All test scenarios pass matching PostgreSQL behavior (where applicable)

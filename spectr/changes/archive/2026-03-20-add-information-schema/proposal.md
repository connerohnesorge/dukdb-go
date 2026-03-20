# Change: SQL Standard information_schema Views

## Why

The `information_schema` is the SQL standard way to query database metadata. Implementing it enables:
- SQL Standard compliance (critical for tool compatibility)
- BI tool integration (Tableau, Power BI, Looker all query information_schema)
- ORM compatibility (SQLAlchemy, Entity Framework expect information_schema)
- Portable SQL scripts (users can write tools that work across PostgreSQL, MySQL, DuckDB)
- Better developer experience (familiar schema names across databases)

This is a prerequisite for broader ecosystem adoption, as most database tools first check for `information_schema` before falling back to system catalogs.

## What Changes

This proposal implements 12 views in the `information_schema` schema providing SQL standard metadata access:

- `information_schema.tables` - All tables and views
- `information_schema.columns` - Column definitions with types
- `information_schema.schemata` - Schema (namespace) definitions
- `information_schema.views` - View definitions and metadata
- `information_schema.table_constraints` - Constraints on tables
- `information_schema.key_column_usage` - Columns used in constraints
- `information_schema.referential_constraints` - Foreign key relationships
- `information_schema.constraint_column_usage` - Columns referenced by constraints
- `information_schema.check_constraints` - Check constraint definitions
- `information_schema.triggers` - Trigger definitions (initially empty, extensible)
- `information_schema.routines` - Functions and procedures
- `information_schema.parameters` - Function parameters

## Architectural Approach

### View Implementation Strategy
- Implement as virtual tables (computed from catalog, not stored)
- Each view queries underlying metadata infrastructure from `add-system-functions`
- Views accessible as `SELECT * FROM information_schema.tables` etc.
- Column schema matches SQL standard (with DuckDB adjustments)

### Registration
- Create `internal/postgres/catalog/information_schema.go` file
- Each view implemented as CatalogProvider query function
- Register in the executor's virtual table resolution

### Column Naming
- Use lowercase with underscores (SQL standard convention)
- Include optional columns for completeness (NULL where not applicable)
- Match ISO/IEC 9075 standard naming conventions

## Impact

- **Affected specs**: `information-schema` (new)
- **Affected code**:
  - `internal/postgres/catalog/` - add information_schema views
  - Existing virtual table machinery (no changes needed)

- **Dependencies**:
  - **DEPENDS ON**: `add-system-functions` (requires metadata infrastructure)
  - **Blocks**: `add-postgresql-catalog` (PostgreSQL views should complement information_schema)

- **Not Breaking**: Pure additions; existing API unchanged

## Success Criteria

1. All 12 information_schema views are queryable
2. Views return correct column schemas per SQL standard
3. Views return data consistent with underlying catalog
4. NULL values returned for optional/not-applicable fields
5. Column order and names match DuckDB v1.4.3 information_schema exactly
6. Views work with JOINs and complex queries (no query restrictions)
7. Tools like DBeaver recognize information_schema
8. Queries perform acceptably on typical database sizes
9. Handle edge cases (no tables, no views, no constraints, etc.)
10. All test scenarios pass and match reference implementation

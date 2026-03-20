## Implementation Details

### AST Changes (`internal/parser/ast.go`)

```go
// ExportDatabaseStmt represents EXPORT DATABASE 'path' (FORMAT fmt).
type ExportDatabaseStmt struct {
    Path    string            // Directory path to export to
    Options map[string]string // FORMAT, DELIMITER, HEADER, etc.
}

// ImportDatabaseStmt represents IMPORT DATABASE 'path'.
type ImportDatabaseStmt struct {
    Path string // Directory path to import from
}
```

### Parser Grammar (`internal/parser/parser.go`)

```
export_database_stmt
    : EXPORT DATABASE string_literal ['(' option_list ')']
    ;

import_database_stmt
    : IMPORT DATABASE string_literal
    ;

option_list
    : option [',' option]*
    ;

option
    : IDENT value
    ;
```

The parser checks for `EXPORT` or `IMPORT` keyword followed by `DATABASE`.
The EXPORT statement accepts an optional parenthesized option list. The FORMAT
option accepts `CSV` (default), `PARQUET`, or `JSON`.

### Export Directory Layout

```
export_dir/
├── schema.sql       # All DDL statements in dependency order
├── load.sql         # COPY FROM statements for each table
├── table1.csv       # Data file for table1 (or .parquet/.json)
├── table2.csv       # Data file for table2
├── main_table1.csv  # Schema-qualified: {schema}_{table}.{ext}
└── ...
```

File naming convention:
- Default schema ("main"): `{table_name}.{ext}`
- Other schemas: `{schema_name}_{table_name}.{ext}`
- Extension based on FORMAT: `.csv` (default), `.parquet`, `.json`

### DDL Generation Algorithm

New file `internal/catalog/ddl_gen.go` provides SQL generation for each
catalog object type:

```go
// ToCreateSQL generates a CREATE TABLE statement for this table definition.
func (t *TableDef) ToCreateSQL() string

// ToCreateSQL generates a CREATE VIEW statement for this view definition.
func (v *ViewDef) ToCreateSQL() string

// ToCreateSQL generates a CREATE SEQUENCE statement.
func (s *SequenceDef) ToCreateSQL() string

// ToCreateSQL generates a CREATE INDEX statement.
func (i *IndexDef) ToCreateSQL() string
```

Each method generates valid SQL that can be parsed back by the dukdb-go parser.
Column types are serialized using their canonical names (e.g., `INTEGER` not
`INT4`, `VARCHAR` not `TEXT`).

For `TableDef.ToCreateSQL()` (main schema — unqualified):
```sql
CREATE TABLE table_name (
    col1 INTEGER NOT NULL,
    col2 VARCHAR DEFAULT 'hello',
    col3 DECIMAL(10, 2),
    PRIMARY KEY (col1)
);
```

For `TableDef.ToCreateSQL()` (non-main schema — qualified):
```sql
CREATE TABLE analytics.table_name (
    col1 INTEGER NOT NULL
);
```

For `ViewDef.ToCreateSQL()`:
```sql
CREATE VIEW view_name AS SELECT ...;
```

The `ToCreateSQL()` method uses the stored `Query` field (original SELECT body)
to reconstruct the view definition. If `ViewDef` gains an `OriginalSQL` field
in the future, prefer that for exact preservation.

For `SequenceDef.ToCreateSQL()`:
```sql
CREATE SEQUENCE seq_name
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    NO CYCLE;
```

For `IndexDef.ToCreateSQL()`:
```sql
CREATE UNIQUE INDEX idx_name ON table_name (col1, col2);
```

### Dependency Resolution Strategy

The `schema.sql` file must contain DDL statements in a valid execution order.
The dependency ordering is:

1. **Schemas** (except "main" which always exists) — `CREATE SCHEMA` first
2. **Custom Types** — user-defined ENUM types before tables that use them
3. **Sequences** — before tables that may reference them via DEFAULT NEXTVAL
4. **Tables** — in dependency order (tables referenced by foreign keys first,
   though FK support is limited; self-referencing tables handled by creating
   table first, adding constraints after)
5. **Macros** — after tables but before views (macros may be used in views);
   ordered by creation order to handle nested macro dependencies
6. **Views** — after all tables they reference; views referencing other views
   are ordered by depth (leaf views first)
7. **Indexes** — after their target tables

For views, dependency depth is determined by parsing the view's SQL body and
finding referenced table/view names. A simple topological sort handles this:

```go
func orderViewsByDependency(views []*ViewDef, tables map[string]bool) []*ViewDef {
    // Build dependency graph: view -> set of referenced views
    // Topological sort: output views whose dependencies are all satisfied
}
```

If circular view dependencies exist (shouldn't be possible in valid SQL),
return an error rather than silently breaking.

### Export Execution (`internal/executor/export_import.go`)

```go
func (e *Executor) executeExportDatabase(
    ctx *ExecutionContext,
    plan *planner.PhysicalExportDatabase,
) (*ExecutionResult, error) {
    // 1. Create export directory (mkdir -p)
    // 2. Collect all catalog objects from all schemas
    // 3. Order objects by dependency
    // 4. Generate schema.sql
    // 5. For each table:
    //    a. Build COPY TO statement for the table
    //    b. Execute via existing executeCopyTo infrastructure
    //    c. Add COPY FROM line to load.sql
    // 6. Write load.sql
    // 7. Return result with count of exported tables
}
```

The export reuses the existing `executeCopyTo` infrastructure by constructing
`PhysicalCopyTo` plans internally. This avoids duplicating CSV/Parquet/JSON
writing logic.

### Import Execution

```go
func (e *Executor) executeImportDatabase(
    ctx *ExecutionContext,
    plan *planner.PhysicalImportDatabase,
) (*ExecutionResult, error) {
    // 1. Verify directory exists and contains schema.sql and load.sql
    // 2. Read schema.sql
    // 3. Split into individual statements (by semicolon)
    // 4. Parse and execute each DDL statement sequentially
    // 5. Read load.sql
    // 6. Split into individual COPY FROM statements
    // 7. Parse and execute each COPY FROM statement sequentially
    // 8. Return result with count of imported tables
}
```

The import executes statements through the normal parse → bind → plan →
execute pipeline, reusing all existing infrastructure. This ensures validation
and error handling are consistent.

### FORMAT Option Handling

| FORMAT | File Extension | Writer Used | Notes |
|--------|---------------|-------------|-------|
| CSV | .csv | existing CSV COPY TO | Default format |
| PARQUET | .parquet | existing Parquet COPY TO | Compact, typed |
| JSON | .json | existing JSON COPY TO | Human-readable |

Additional COPY options (DELIMITER, HEADER, NULL, etc.) passed through the
EXPORT DATABASE option list are forwarded to each COPY TO operation.

The `load.sql` COPY FROM statements include matching options so the import
uses the same settings as the export.

#### load.sql Path Handling

The `load.sql` file stores only **filenames** (not absolute paths) in COPY
FROM statements. At import time, the executor reconstructs full paths by
joining the import directory with each filename. This ensures portability —
an export directory can be moved to a different machine and imported correctly.

Example load.sql content:
```sql
COPY users FROM 'users.csv' (FORMAT CSV, HEADER true);
COPY orders FROM 'orders.csv' (FORMAT CSV, HEADER true);
```

At import time, if importing from `/data/backup/`, these become:
```sql
COPY users FROM '/data/backup/users.csv' (FORMAT CSV, HEADER true);
COPY orders FROM '/data/backup/orders.csv' (FORMAT CSV, HEADER true);
```

#### NOT NULL Column Handling for CSV

When exporting tables with NOT NULL columns to CSV format, the COPY TO
statement includes `force_not_null` option listing all NOT NULL column names.
The matching COPY FROM in load.sql includes the same option so that the import
enforces NOT NULL constraints during loading, matching DuckDB behavior.

#### Schema Qualification

DDL in schema.sql uses unqualified names for objects in the default "main"
schema and qualified names (`schema.table`) for objects in other schemas.
This matches DuckDB's output format.

### Planner Changes (`internal/planner/physical.go`)

```go
type PhysicalExportDatabase struct {
    Path    string
    Options map[string]string
}

type PhysicalImportDatabase struct {
    Path string
}
```

These are simple leaf nodes with no children.

## Context

DuckDB's EXPORT/IMPORT DATABASE is the primary mechanism for database backup,
migration between versions, and sharing datasets. It produces human-readable
output (schema.sql is plain SQL, data files are standard formats) that can be
inspected, modified, or loaded into other systems.

The existing catalog API (`ListSchemas`, `ListTables`, `ListViews`,
`ListSequences`, `ListIndexes`) provides all the enumeration needed. The
existing COPY TO/FROM infrastructure handles data serialization. This proposal
bridges these two capabilities.

## Goals / Non-Goals

**Goals:**
- `EXPORT DATABASE 'path'` with FORMAT CSV (default)
- `EXPORT DATABASE 'path' (FORMAT PARQUET)` for binary format
- `IMPORT DATABASE 'path'` to restore exported database
- Dependency-ordered DDL in schema.sql
- Round-trip correctness: export then import produces equivalent database

**Non-Goals:**
- Incremental/differential export — full export only
- Remote paths (S3, etc.) — local filesystem only initially
- EXPORT DATABASE with encryption or compression
- Cross-version format compatibility with DuckDB's native export format
- EXPORT/IMPORT of user-defined functions or macros (future work)

## Decisions

- **Reuse COPY TO/FROM**: Rather than implementing custom data serialization,
  construct PhysicalCopyTo/From plans internally. This keeps the codebase DRY
  and ensures format consistency.

- **DDL generation as catalog methods**: Each catalog object type gets a
  `ToCreateSQL()` method. This is cleaner than a standalone DDL generator
  because the object itself knows its full definition.

- **Statement-by-statement import**: Import executes each SQL statement
  individually through the full pipeline rather than batch-loading. This is
  slower but simpler and catches errors at the statement level.

- **No transaction wrapping for export**: Export reads a consistent snapshot
  but doesn't wrap in a transaction (the catalog is read-locked during
  enumeration). Import runs in an implicit transaction per statement.

## Risks / Trade-offs

- **Large databases**: Exporting very large tables may be slow. Mitigation:
  the existing COPY TO batching handles this, and FORMAT PARQUET provides
  compression.

- **Concurrent modifications during export**: If another connection modifies
  the database during export, the snapshot may be inconsistent. Mitigation:
  document that export should be run in a quiescent state or under SERIALIZABLE
  isolation.

- **View dependency parsing**: Determining view dependencies requires parsing
  view SQL bodies to find referenced tables/views. This is fragile if views
  use dynamic SQL or complex expressions. Mitigation: use the parser's own
  SELECT parsing to extract table references.

## Open Questions

- Should IMPORT DATABASE fail if the target database is non-empty, or should
  it merge? Decision: Fail if any tables already exist (match DuckDB behavior).
  The user should import into a fresh database.

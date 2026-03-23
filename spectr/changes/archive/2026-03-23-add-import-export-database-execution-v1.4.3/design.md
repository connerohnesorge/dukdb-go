# Design: IMPORT/EXPORT DATABASE Execution

## Implementation Details

### 1. DuckDB EXPORT DATABASE Format

DuckDB's `EXPORT DATABASE 'dir'` creates a directory with:

```
dir/
├── schema.sql      # DDL statements (CREATE SCHEMA, CREATE TABLE, CREATE VIEW, etc.)
├── load.sql        # COPY statements to load data from files
├── table1.csv      # Data file for table1 (format depends on OPTIONS)
├── table2.csv      # Data file for table2
└── ...
```

**schema.sql** contains (in order):
1. `CREATE SCHEMA` statements (excluding 'main')
2. `CREATE TYPE` (ENUM) statements
3. `CREATE SEQUENCE` statements
4. `CREATE TABLE` statements with all constraints
5. `CREATE VIEW` statements
6. `CREATE MACRO` statements

**load.sql** contains:
1. `COPY table FROM 'dir/table.csv' (FORMAT CSV, ...)` for each table

### 2. Physical Plan Nodes (internal/planner/physical.go)

```go
type PhysicalExportDatabase struct {
    Path    string
    Options map[string]string // FORMAT (csv/parquet/json), DELIMITER, HEADER, etc.
}

func (*PhysicalExportDatabase) planNode()             {}
func (*PhysicalExportDatabase) Type() PhysicalPlanType { return PhysicalPlanExportDatabase }

type PhysicalImportDatabase struct {
    Path string
}

func (*PhysicalImportDatabase) planNode()             {}
func (*PhysicalImportDatabase) Type() PhysicalPlanType { return PhysicalPlanImportDatabase }
```

### 3. Executor — EXPORT DATABASE (internal/executor/export_database.go)

New file with the export logic:

```go
func (e *Executor) executeExportDatabase(ctx *ExecContext, plan *planner.PhysicalExportDatabase) error {
    // 1. Create output directory
    if err := os.MkdirAll(plan.Path, 0755); err != nil {
        return fmt.Errorf("cannot create export directory: %w", err)
    }

    // 2. Determine format (default: CSV)
    format := strings.ToUpper(plan.Options["FORMAT"])
    if format == "" {
        format = "CSV"
    }

    // 3. Generate schema.sql
    var schemaBuf bytes.Buffer
    schemas := e.catalog.ListSchemas()
    for _, schema := range schemas {
        if schema != "main" {
            fmt.Fprintf(&schemaBuf, "CREATE SCHEMA %s;\n", quoteIdentifier(schema))
        }
    }
    // Write CREATE TYPE (ENUM) statements
    for _, typ := range e.catalog.ListTypes() {
        schemaBuf.WriteString(typ.ToSQL() + ";\n")
    }
    // Write CREATE SEQUENCE statements
    for _, seq := range e.catalog.ListSequences() {
        schemaBuf.WriteString(seq.ToSQL() + ";\n")
    }
    // Write CREATE TABLE statements (sorted by dependency order for foreign keys)
    tables := e.catalog.ListAllTables()
    for _, table := range e.topologicalSortTables(tables) {
        schemaBuf.WriteString(table.ToCreateSQL() + ";\n\n")
    }
    // Write CREATE VIEW statements
    for _, view := range e.catalog.ListAllViews() {
        schemaBuf.WriteString(view.ToCreateSQL() + ";\n\n")
    }
    // Write CREATE MACRO statements
    for _, macro := range e.catalog.ListMacros() {
        schemaBuf.WriteString(macro.ToSQL() + ";\n\n")
    }
    os.WriteFile(filepath.Join(plan.Path, "schema.sql"), schemaBuf.Bytes(), 0644)

    // 4. Export data files and generate load.sql
    var loadBuf bytes.Buffer
    for _, table := range tables {
        dataFile := tableName + "." + strings.ToLower(format)
        dataPath := filepath.Join(plan.Path, dataFile)

        // Use existing COPY TO infrastructure
        copyStmt := fmt.Sprintf("COPY %s.%s TO '%s' (FORMAT %s)",
            quoteIdentifier(table.Schema),
            quoteIdentifier(table.Name),
            dataPath, format)
        e.executeSQLInternal(ctx, copyStmt)

        // Write COPY FROM to load.sql
        fmt.Fprintf(&loadBuf, "COPY %s.%s FROM '%s' (FORMAT %s);\n",
            quoteIdentifier(table.Schema),
            quoteIdentifier(table.Name),
            dataPath, format)
    }
    os.WriteFile(filepath.Join(plan.Path, "load.sql"), loadBuf.Bytes(), 0644)

    return nil
}
```

### 4. Executor — IMPORT DATABASE (internal/executor/import_database.go)

```go
func (e *Executor) executeImportDatabase(ctx *ExecContext, plan *planner.PhysicalImportDatabase) error {
    // 1. Verify directory exists
    if _, err := os.Stat(plan.Path); os.IsNotExist(err) {
        return fmt.Errorf("import directory does not exist: %s", plan.Path)
    }

    // 2. Read and execute schema.sql
    schemaSQL, err := os.ReadFile(filepath.Join(plan.Path, "schema.sql"))
    if err != nil {
        return fmt.Errorf("cannot read schema.sql: %w", err)
    }
    // Parse and execute each statement in schema.sql
    stmts, err := parser.ParseMultiple(string(schemaSQL))
    if err != nil {
        return fmt.Errorf("parsing schema.sql: %w", err)
    }
    for _, stmt := range stmts {
        if err := e.executeStatement(ctx, stmt); err != nil {
            return fmt.Errorf("executing schema.sql: %w", err)
        }
    }

    // 3. Read and execute load.sql
    loadSQL, err := os.ReadFile(filepath.Join(plan.Path, "load.sql"))
    if err != nil {
        return fmt.Errorf("cannot read load.sql: %w", err)
    }
    stmts, err = parser.ParseMultiple(string(loadSQL))
    if err != nil {
        return fmt.Errorf("parsing load.sql: %w", err)
    }
    for _, stmt := range stmts {
        if err := e.executeStatement(ctx, stmt); err != nil {
            return fmt.Errorf("executing load.sql: %w", err)
        }
    }

    return nil
}
```

### 5. Catalog Helper Methods

Add `ToCreateSQL()` methods to catalog entry types to generate DDL:

```go
func (t *TableDef) ToCreateSQL() string {
    // Generate CREATE TABLE statement with all columns, constraints
}

func (v *ViewEntry) ToCreateSQL() string {
    // Generate CREATE VIEW AS query
}

func (s *SequenceEntry) ToSQL() string {
    // Generate CREATE SEQUENCE with all options
}
```

#### Table Constraint Export

`TableDef.Constraints []any` holds `*UniqueConstraintDef`, `*CheckConstraintDef`, and `*ForeignKeyConstraintDef` entries. The `ToCreateSQL()` method must iterate over these and emit the appropriate SQL for each constraint type as part of the CREATE TABLE body (e.g., `UNIQUE (col1, col2)`, `CHECK (expr)`, `FOREIGN KEY (col) REFERENCES other_table(col)`).

#### Macro SQL Generation

The catalog `Schema` type currently provides `GetMacro`, `CreateMacro`, and `DropMacro` but has no `ListMacros()` method. A `ListMacros() []*MacroDef` method must be added to both `Schema` and `Catalog` to enumerate all macros for export. Additionally, a `generateCreateMacroSQL(*MacroDef) string` helper (or `ToSQL()` method on `MacroDef`) is needed to produce valid `CREATE MACRO` DDL.

### 6. Binder Layer

Like other DDL statements, EXPORT DATABASE and IMPORT DATABASE must pass through the binder before reaching the planner. Add `bindExportDatabase(*parser.ExportDatabaseStmt)` and `bindImportDatabase(*parser.ImportDatabaseStmt)` methods to `internal/binder/bind_stmt.go`. These can perform basic validation (e.g., path is non-empty) and return bound statement representations that the planner consumes.

### 7. Statement Type and Detection Fixes

**ImportDatabaseStmt.Type()**: The current implementation at `internal/parser/ast.go:1346` returns `STATEMENT_TYPE_COPY` which is incorrect. It must return `STATEMENT_TYPE_COPY_DATABASE` (defined at `backend.go:301`) to properly distinguish import-database operations from regular COPY statements.

**stmt_detector.go**: The `keywordToStmtType()` function recognizes "EXPORT" but does not recognize "IMPORT". An `"IMPORT"` case must be added mapping to `STATEMENT_TYPE_COPY_DATABASE`.

### 8. Parser: ParseMultiple

Ensure the parser can handle multiple semicolon-separated statements from a file. This may already exist for batch execution. If not, add a `ParseMultiple(sql string) ([]Statement, error)` function.

## Context

EXPORT/IMPORT DATABASE is used for:
- Database backup and restore
- Migration between DuckDB and dukdb-go instances
- Sharing database contents in human-readable format (SQL + CSV)
- CI/CD pipelines that seed test databases

The format is designed to be DuckDB-compatible, so files exported by dukdb-go can be imported by DuckDB and vice versa.

## Goals / Non-Goals

- **Goals**: Full EXPORT DATABASE with schema.sql + load.sql + data files, IMPORT DATABASE from DuckDB-compatible export directory, CSV/Parquet/JSON format support
- **Non-Goals**: Incremental export, compressed export archives, remote export (S3), streaming export

## Decisions

- **Reuse COPY infrastructure**: Export uses the existing COPY TO mechanism for data files, avoiding code duplication
- **Topological sort**: Tables are exported in dependency order (foreign key references) to ensure schema.sql can be executed in order. The algorithm builds a directed graph where each table is a node and each foreign key creates an edge from the referencing table to the referenced table. A Kahn's algorithm (BFS-based) topological sort processes nodes with zero in-degree first, appending them to the result and decrementing in-degree of their dependents. If a cycle is detected (remaining nodes with no zero in-degree), the sort falls back to alphabetical order for the cycle members and emits a warning
- **Format default**: CSV is the default export format (matching DuckDB behavior)

## Risks / Trade-offs

- **Risk**: Large databases may take significant time/space to export → Mitigation: Export is inherently I/O-bound; parallel table export could be added later
- **Risk**: Schema regeneration may lose formatting or comments → Mitigation: ToCreateSQL() methods should produce clean, canonical SQL
- **Risk**: Import may fail partway through → Mitigation: Run import inside a transaction; rollback on failure

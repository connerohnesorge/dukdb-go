# Design: GENERATED Columns (STORED/VIRTUAL)

## Implementation Details

### 1. AST Changes (internal/parser/ast.go)

Add two fields to `ColumnDefClause`:

```go
type GeneratedKind int

const (
    GeneratedKindStored  GeneratedKind = 0 // Value persisted to storage
    GeneratedKindVirtual GeneratedKind = 1 // Computed on read (default in DuckDB)
)

type ColumnDefClause struct {
    Name          string
    DataType      dukdb.Type
    TypeInfo      dukdb.TypeInfo
    NotNull       bool
    Default       Expr
    PrimaryKey    bool
    Unique        bool
    Check         Expr
    Collation     string
    ForeignKey    *ForeignKeyRef
    GeneratedExpr Expr          // NEW: expression for GENERATED ALWAYS AS
    GeneratedKind GeneratedKind // NEW: STORED or VIRTUAL
    IsGenerated   bool          // NEW: flag to distinguish from default
}
```

### 2. Parser Changes (internal/parser/parser_ddl.go)

In `parseColumnDef()`, after parsing existing constraints, detect the `GENERATED ALWAYS AS` syntax:

```go
// After existing constraint parsing:
if p.isKeyword("GENERATED") {
    p.expect("GENERATED")
    p.expect("ALWAYS")
    p.expect("AS")
    p.expect("(")
    col.GeneratedExpr = p.parseExpr()
    p.expect(")")
    col.IsGenerated = true
    // Default to VIRTUAL (DuckDB default), allow STORED keyword
    if p.isKeyword("STORED") {
        p.advance()
        col.GeneratedKind = GeneratedKindStored
    } else if p.isKeyword("VIRTUAL") {
        p.advance()
        col.GeneratedKind = GeneratedKindVirtual
    } else {
        col.GeneratedKind = GeneratedKindVirtual // DuckDB default
    }
}
```

DuckDB also supports the shorthand `AS (expr) [STORED|VIRTUAL]` without `GENERATED ALWAYS`. This must also be handled — when `AS` appears after a column type (not inside a constraint), it indicates a generated column.

### 3. Catalog Changes (internal/catalog/column.go)

The `ColumnDef` must store generated column metadata so it survives across the lifetime of the table:

```go
type ColumnDef struct {
    Name          string
    Type          dukdb.Type
    TypeInfo      dukdb.TypeInfo
    NotNull       bool
    DefaultExpr   string        // SQL text of DEFAULT expression
    IsGenerated   bool
    GeneratedExpr string        // SQL text of GENERATED expression
    GeneratedKind GeneratedKind // STORED or VIRTUAL
}
```

Store the generated expression as SQL text (not as an AST node) to enable serialization and deserialization without coupling to parser internals.

### 4. Executor: INSERT Path (internal/executor/operator.go — executeInsert())

When inserting into a table with generated columns:

1. **Validate**: Reject explicit values for generated columns (unless DEFAULT keyword is used in the VALUES clause).
2. **Compute**: After binding non-generated column values, evaluate each generated column expression using the current row values.
3. **Order**: Process generated columns in dependency order — a generated column may reference another generated column if that column appears earlier in definition order (DuckDB restriction).

```go
func (e *Executor) computeGeneratedColumns(
    table *catalog.TableDef,
    row []any,        // values for all columns, generated cols have nil
    colIndexMap map[string]int,
) ([]any, error) {
    for i, meta := range table.Columns {
        if !meta.IsGenerated {
            continue
        }
        // Parse and evaluate the expression using current row values
        expr, err := e.parseGeneratedExpr(meta.GeneratedExpr)
        if err != nil {
            return nil, err
        }
        // Create evaluation context with row values bound to column names
        ctx := e.createRowEvalContext(table, row, colIndexMap)
        val, err := e.evaluateExpr(ctx, expr)
        if err != nil {
            return nil, fmt.Errorf("evaluating generated column %q: %w", meta.Name, err)
        }
        row[i] = val
    }
    return row, nil
}
```

### 5. Executor: UPDATE Path (internal/executor/physical_update.go)

When updating a table with generated columns:

1. **Prevent**: Direct updates to generated columns must raise an error.
2. **Recompute**: After updating non-generated columns, re-evaluate all generated columns that depend on any changed column.

### 6. Executor: SELECT Path (VIRTUAL columns)

For VIRTUAL generated columns:
- During table scan, VIRTUAL columns are not read from storage (they don't exist there).
- Instead, after reading base columns, compute VIRTUAL column values on-the-fly.
- For STORED columns, the value is read from storage normally.

**Initial implementation recommendation**: Start with STORED only. DuckDB internally treats all generated columns as VIRTUAL (it doesn't actually persist them), but conceptually STORED is simpler to implement in our architecture since we can simply write the computed value during INSERT/UPDATE. VIRTUAL requires modifying the scan path.

### 7. Binder Validation (internal/binder/binder.go — bindCreateTable())

During CREATE TABLE validation in `bindCreateTable()` (around line 191 of `internal/binder/binder.go`), which already handles `*parser.CreateTableStmt`:
1. Generated column expressions must only reference columns defined BEFORE the generated column (no forward references).
2. Generated columns cannot reference other generated columns (DuckDB restriction — simplified model).
3. Generated columns cannot have DEFAULT values.
4. Generated columns cannot be PRIMARY KEY.
5. Generated column expression must be deterministic (no RANDOM(), UUID(), CURRENT_TIMESTAMP, etc.).

### 8. Storage Serialization (internal/storage/duckdb/)

The DuckDB binary format already defines:
- `PropColumnDefExpression` (uint16 = 102) — for generated column expression
- `ColumnCategoryGenerated` (uint8 = 1) — column category marker

In `catalog_serialize.go`, when writing column definitions:
```go
if col.IsGenerated {
    writeProperty(PropColumnDefCategory, ColumnCategoryGenerated)
    writeProperty(PropColumnDefExpression, col.GeneratedExpr)
}
```

In `catalog_deserialize.go`, when reading column definitions:
```go
case PropColumnDefExpression:
    col.GeneratedExpr = readString(reader)
    col.IsGenerated = true
case PropColumnDefCategory:
    cat := readUint8(reader)
    if cat == ColumnCategoryGenerated {
        col.IsGenerated = true
    }
```

### 9. ALTER TABLE Interactions

- `ALTER TABLE DROP COLUMN` on a generated column: Allowed.
- `ALTER TABLE DROP COLUMN` on a base column referenced by a generated column: Must fail with an error.
- `ALTER TABLE ADD COLUMN` with GENERATED: Allowed.
- `ALTER TABLE ALTER COLUMN TYPE` on a generated column: Must re-validate the expression.

## Context

DuckDB v1.4.3 supports generated columns as part of its SQL standard compliance. The syntax follows PostgreSQL's `GENERATED ALWAYS AS (expr) STORED` pattern, which is also part of the SQL:2003 standard. MySQL and SQLite also support similar syntax.

Generated columns are used for:
- Denormalized computed values (e.g., `full_name` from `first_name || ' ' || last_name`)
- Indexed expressions (create index on generated column for expression indexing)
- Data validation (generated column with CHECK constraint)
- Materialized computations (STORED avoids recomputation)

## Goals / Non-Goals

- **Goals**: Full parser support for GENERATED ALWAYS AS syntax, STORED column execution on INSERT/UPDATE, validation of expressions, binary format compatibility
- **Non-Goals**: VIRTUAL column on-read computation (future work), expression index optimization, generated column statistics collection

## Decisions

- **STORED-first approach**: Implement STORED generated columns first since they integrate cleanly with the existing INSERT/UPDATE path. VIRTUAL columns require scan-path modifications and can be added later.
- **SQL text storage**: Store generated expressions as SQL text strings rather than serialized AST to avoid coupling catalog to parser internals.
- **No forward references**: Generated columns can only reference columns defined before them, matching DuckDB's behavior.
- **Determinism required**: Generated expressions must be deterministic — no volatile functions (RANDOM, UUID, CURRENT_TIMESTAMP).

## Risks / Trade-offs

- **Risk**: Expression evaluation during INSERT/UPDATE adds latency → Mitigation: Only evaluate when generated columns exist; skip for tables without them.
- **Risk**: Expression parsing from SQL text on every INSERT could be slow → Mitigation: Cache parsed expressions per table in the executor.
- **Risk**: ALTER TABLE interactions may have edge cases → Mitigation: Start with clear error messages for unsupported ALTER operations on generated columns.

## Open Questions

- Should we support the shorthand `AS (expr)` without `GENERATED ALWAYS`? DuckDB does support it. **Recommendation**: Yes, add it for compatibility.
- Should VIRTUAL columns be a separate proposal? **Recommendation**: Yes, keep VIRTUAL as future work to limit scope.

# Design: ALTER COLUMN SET/DROP DEFAULT and NOT NULL

## Architecture

Full-stack change: AST → Parser → Executor → Catalog. Extends existing ALTER TABLE infrastructure.

## 1. AST Changes (ast.go:659-668)

Add 4 new AlterTableOp constants:

```go
const (
    AlterTableRenameTo AlterTableOp = iota
    AlterTableRenameColumn
    AlterTableDropColumn
    AlterTableAddColumn
    AlterTableSetOption
    AlterTableAlterColumnType
    AlterTableAddConstraint
    AlterTableDropConstraint
    AlterTableSetColumnDefault    // NEW
    AlterTableDropColumnDefault   // NEW
    AlterTableSetColumnNotNull    // NEW
    AlterTableDropColumnNotNull   // NEW
)
```

Add fields to AlterTableStmt (ast.go:671-687):

```go
type AlterTableStmt struct {
    // ... existing fields ...
    DefaultExpr    Expr   // SET DEFAULT expression
}
```

The AlterColumn field (line 682) already stores the column name, which is reused for all ALTER COLUMN operations.

## 2. Parser Changes (parser_ddl.go:598-631)

The current parser at line 598 handles `ALTER [COLUMN] col_name` but only supports `SET DATA TYPE` / `TYPE`. After parsing the column name at line 606, we need to check for SET DEFAULT, DROP DEFAULT, SET NOT NULL, DROP NOT NULL BEFORE checking for SET DATA TYPE.

### Current flow (lines 598-631):

```
ALTER → COLUMN? → col_name → SET DATA TYPE / TYPE → type_spec
```

### New flow:

```
ALTER → COLUMN? → col_name → {
    SET DEFAULT expr     → AlterTableSetColumnDefault
    DROP DEFAULT         → AlterTableDropColumnDefault
    SET NOT NULL         → AlterTableSetColumnNotNull
    DROP NOT NULL        → AlterTableDropColumnNotNull
    SET DATA TYPE / TYPE → AlterTableAlterColumnType (existing)
}
```

### Implementation (replace lines 608-630):

```go
colName := p.advance().value

if p.isKeyword("SET") {
    p.advance() // consume SET
    if p.isKeyword("DEFAULT") {
        p.advance() // consume DEFAULT
        // Parse the default expression
        expr, err := p.parseExpr()
        if err != nil {
            return nil, err
        }
        stmt.Operation = AlterTableSetColumnDefault
        stmt.AlterColumn = colName
        stmt.DefaultExpr = expr
    } else if p.isKeyword("NOT") {
        p.advance() // consume NOT
        if err := p.expectKeyword("NULL"); err != nil {
            return nil, err
        }
        stmt.Operation = AlterTableSetColumnNotNull
        stmt.AlterColumn = colName
    } else if p.isKeyword("DATA") {
        // Existing: SET DATA TYPE
        if err := p.expectKeyword("TYPE"); err != nil {
            return nil, err
        }
        typeSpec, err := p.collectTypeSpec(
            map[tokenType]bool{tokenSemicolon: true, tokenEOF: true},
            map[string]bool{},
        )
        if err != nil {
            return nil, err
        }
        stmt.Operation = AlterTableAlterColumnType
        stmt.AlterColumn = colName
        stmt.NewTypeRaw = typeSpec
    } else {
        return nil, p.errorf("expected DEFAULT, NOT NULL, or DATA TYPE after SET")
    }
} else if p.isKeyword("DROP") {
    p.advance() // consume DROP
    if p.isKeyword("DEFAULT") {
        p.advance() // consume DEFAULT
        stmt.Operation = AlterTableDropColumnDefault
        stmt.AlterColumn = colName
    } else if p.isKeyword("NOT") {
        p.advance() // consume NOT
        if err := p.expectKeyword("NULL"); err != nil {
            return nil, err
        }
        stmt.Operation = AlterTableDropColumnNotNull
        stmt.AlterColumn = colName
    } else {
        return nil, p.errorf("expected DEFAULT or NOT NULL after DROP")
    }
} else if p.isKeyword("TYPE") {
    // Existing: ALTER COLUMN col TYPE type
    if err := p.expectKeyword("TYPE"); err != nil {
        return nil, err
    }
    typeSpec, err := p.collectTypeSpec(
        map[tokenType]bool{tokenSemicolon: true, tokenEOF: true},
        map[string]bool{},
    )
    if err != nil {
        return nil, err
    }
    stmt.Operation = AlterTableAlterColumnType
    stmt.AlterColumn = colName
    stmt.NewTypeRaw = typeSpec
} else {
    return nil, p.errorf("expected SET, DROP, or TYPE after column name")
}
```

### Also fix the top-level SET handler (line 593-597):

The current code at line 593-597 catches `ALTER TABLE t SET ...` (table-level SET, like SET SCHEMA). This should be preserved but the error message updated:

```go
} else if p.isKeyword("SET") {
    p.advance()
    stmt.Operation = AlterTableSetOption
    return nil, p.errorf("ALTER TABLE SET (table-level) not yet implemented; use ALTER TABLE t ALTER COLUMN c SET DEFAULT/NOT NULL")
}
```

## 3. Executor Changes (ddl.go)

Add cases in the ALTER TABLE execution handler. Need to find the existing ALTER TABLE handler.

```go
case AlterTableSetColumnDefault:
    // Find table, find column, set default expression
    table, err := e.catalog.GetTable(stmt.Schema, stmt.Table)
    if err != nil {
        return nil, err
    }
    col := table.FindColumn(stmt.AlterColumn)
    if col == nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeCatalog,
            Msg:  fmt.Sprintf("column %q not found in table %q", stmt.AlterColumn, stmt.Table),
        }
    }
    col.SetDefault(stmt.DefaultExpr)
    return &ExecutionResult{AffectedRows: 0}, nil

case AlterTableDropColumnDefault:
    table, err := e.catalog.GetTable(stmt.Schema, stmt.Table)
    if err != nil {
        return nil, err
    }
    col := table.FindColumn(stmt.AlterColumn)
    if col == nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeCatalog,
            Msg:  fmt.Sprintf("column %q not found in table %q", stmt.AlterColumn, stmt.Table),
        }
    }
    col.SetDefault(nil) // Remove default
    return &ExecutionResult{AffectedRows: 0}, nil

case AlterTableSetColumnNotNull:
    table, err := e.catalog.GetTable(stmt.Schema, stmt.Table)
    if err != nil {
        return nil, err
    }
    col := table.FindColumn(stmt.AlterColumn)
    if col == nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeCatalog,
            Msg:  fmt.Sprintf("column %q not found in table %q", stmt.AlterColumn, stmt.Table),
        }
    }
    // Check existing data for NULLs before setting NOT NULL
    // (validation needed against stored data)
    col.SetNotNull(true)
    return &ExecutionResult{AffectedRows: 0}, nil

case AlterTableDropColumnNotNull:
    table, err := e.catalog.GetTable(stmt.Schema, stmt.Table)
    if err != nil {
        return nil, err
    }
    col := table.FindColumn(stmt.AlterColumn)
    if col == nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeCatalog,
            Msg:  fmt.Sprintf("column %q not found in table %q", stmt.AlterColumn, stmt.Table),
        }
    }
    col.SetNotNull(false)
    return &ExecutionResult{AffectedRows: 0}, nil
```

Note: The exact method names (FindColumn, SetDefault, SetNotNull) depend on the catalog API. These may need to be created or adapted based on existing ColumnDef/TableEntry structures.

## 4. Catalog Changes

Need to verify whether ColumnDef already has Default and NotNull fields. If not, add:

```go
type ColumnDef struct {
    // ... existing fields ...
    Default   Expr // Default value expression (nil = no default)
    NotNull   bool // NOT NULL constraint
}

func (c *ColumnDef) SetDefault(expr Expr) {
    c.Default = expr
}

func (c *ColumnDef) SetNotNull(v bool) {
    c.NotNull = v
}
```

## Helper Signatures Reference (Verified)

- AlterTableOp constants — ast.go:659-668 — operation type enum
- AlterTableStmt — ast.go:671-687 — ALTER TABLE statement struct
- AlterColumn field — ast.go:682 — column name for ALTER COLUMN operations
- parseAlterTable() — parser_ddl.go:555-636 — ALTER TABLE parser
- ALTER COLUMN branch — parser_ddl.go:598-631 — current ALTER COLUMN handling
- SET handler — parser_ddl.go:593-597 — current "not implemented" error
- `p.parseExpr()` — parser.go — expression parser (for DEFAULT value)
- `p.isKeyword()` — parser helper for keyword checking
- `p.expectKeyword()` — parser helper for required keyword
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeCatalog, Msg: ...}`

## Testing Strategy

1. `ALTER TABLE t ALTER COLUMN c SET DEFAULT 42` → succeeds
2. `INSERT INTO t(other_col) VALUES (1)` → c gets default 42
3. `ALTER TABLE t ALTER COLUMN c DROP DEFAULT` → succeeds
4. `INSERT INTO t(other_col) VALUES (1)` → c gets NULL (no default)
5. `ALTER TABLE t ALTER COLUMN c SET NOT NULL` → succeeds
6. `INSERT INTO t(other_col) VALUES (1)` → error (c is NOT NULL, no default)
7. `ALTER TABLE t ALTER COLUMN c DROP NOT NULL` → succeeds
8. `INSERT INTO t(c) VALUES (NULL)` → succeeds
9. `ALTER TABLE t ALTER COLUMN c SET NOT NULL` with existing NULLs → error
10. Existing ALTER COLUMN TYPE still works

# Design: Table DDL Extensions

## Architecture

All three features extend existing DDL infrastructure. CREATE OR REPLACE and CREATE TEMP thread new flags through the existing parser → binder → planner → executor pipeline. ALTER TABLE ADD/DROP CONSTRAINT adds new operation types to the existing AlterTableStmt dispatch.

## 1. CREATE OR REPLACE TABLE

### 1.1 Parser Changes

**parser.go:2004-2061** — `parseCreate()` already parses `orReplace` (lines 2009-2016) and `temporary` (lines 2025-2027) but doesn't pass them to `parseCreateTable()`.

Change line 2037 from:
```go
stmt, err = p.parseCreateTable()
```
To:
```go
stmt, err = p.parseCreateTable(orReplace, temporary)
```

**parser.go:2063** — Update `parseCreateTable()` signature:
```go
func (p *parser) parseCreateTable(orReplace bool, temporary bool) (*CreateTableStmt, error)
```

Set the fields after creating the stmt:
```go
stmt := &CreateTableStmt{}
stmt.OrReplace = orReplace
stmt.Temporary = temporary
```

### 1.2 AST Changes (ast.go:391-410)

Add two fields to CreateTableStmt:
```go
type CreateTableStmt struct {
    Schema      string
    Table       string
    IfNotExists bool
    OrReplace   bool              // NEW: CREATE OR REPLACE TABLE
    Temporary   bool              // NEW: CREATE TEMP/TEMPORARY TABLE
    Columns     []ColumnDefClause
    PrimaryKey  []string
    Constraints []TableConstraint
    AsSelect    *SelectStmt
}
```

### 1.3 Binder Changes (binder/bind_stmt.go:3193-3302)

Thread through `bindCreateTable()`:
```go
// In BoundCreateTableStmt (binder/statements.go:134-142), add:
type BoundCreateTableStmt struct {
    Schema      string
    Table       string
    IfNotExists bool
    OrReplace   bool  // NEW
    Temporary   bool  // NEW
    Columns     []*catalog.ColumnDef
    PrimaryKey  []string
    Constraints []any
}
```

### 1.4 Planner Changes (planner/physical.go:1901-1911)

Thread through planCreateTable and PhysicalCreateTable (physical.go:600-607):
```go
type PhysicalCreateTable struct {
    Schema      string
    Table       string
    IfNotExists bool
    OrReplace   bool  // NEW
    Temporary   bool  // NEW
    Columns     []*catalog.ColumnDef
    PrimaryKey  []string
    Constraints []any
}
```

### 1.5 Executor Changes (executor/operator.go:2603-2727)

In `executeCreateTable()`, add OR REPLACE logic before the existing "table already exists" check at lines 2607-2620:

```go
func (e *Executor) executeCreateTable(ctx *ExecutionContext, plan *PhysicalCreateTable) (*ExecutionResult, error) {
    // OR REPLACE: drop existing table first
    if plan.OrReplace {
        existing := e.catalog.GetTable(plan.Table, plan.Schema)
        if existing != nil {
            // Drop the existing table (same as DROP TABLE logic)
            if err := e.dropTable(ctx, plan.Schema, plan.Table); err != nil {
                return nil, err
            }
        }
    }

    // TEMPORARY: override schema to temp schema
    schema := plan.Schema
    if plan.Temporary {
        schema = "temp"  // or connection-specific temp schema
    }

    // ... existing creation logic ...
}
```

Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## 2. CREATE TEMP/TEMPORARY TABLE

Temporary tables are stored in a "temp" schema that is scoped to the connection. The parser already recognizes TEMPORARY/TEMP keywords (parser.go:2025-2027).

Implementation notes:
- TEMP tables use the same storage infrastructure but are placed in a connection-scoped schema
- If a "temp" schema doesn't exist, create it automatically
- TEMP tables are dropped when the connection closes
- TEMP tables shadow regular tables with the same name (search temp schema first)

## 3. ALTER TABLE ADD CONSTRAINT

### 3.1 AST Changes (ast.go:654-681)

Add new operation types to AlterTableOp enum:
```go
const (
    AlterTableRenameTo AlterTableOp = iota
    AlterTableRenameColumn
    AlterTableDropColumn
    AlterTableAddColumn
    AlterTableSetOption
    AlterTableAlterColumnType
    AlterTableAddConstraint    // NEW
    AlterTableDropConstraint   // NEW
)
```

Add new fields to AlterTableStmt:
```go
type AlterTableStmt struct {
    // ... existing fields ...

    // ADD/DROP CONSTRAINT:
    ConstraintName string           // for DROP CONSTRAINT name
    Constraint     *TableConstraint // for ADD CONSTRAINT definition
}
```

### 3.2 Parser Changes (parser_ddl.go:470-604)

In `parseAlterTable()`, after the existing `case p.isKeyword("ADD"):` block (around line 550), extend the ADD handling:

```go
case p.isKeyword("ADD"):
    p.advance()
    if p.isKeyword("COLUMN") {
        // ... existing ADD COLUMN logic (lines 550-560)
    } else if p.isKeyword("CONSTRAINT") || p.isKeyword("UNIQUE") || p.isKeyword("CHECK") || p.isKeyword("FOREIGN") {
        // ADD CONSTRAINT — reuse parseTableConstraint() from parser.go:2212
        constraint, err := p.parseTableConstraint()
        if err != nil {
            return nil, err
        }
        stmt.Operation = AlterTableAddConstraint
        stmt.Constraint = &constraint
    } else {
        // Assume ADD COLUMN without explicit COLUMN keyword
        // ... existing implicit ADD COLUMN logic
    }
```

Add DROP CONSTRAINT parsing:
```go
case p.isKeyword("DROP"):
    p.advance()
    if p.isKeyword("COLUMN") {
        // ... existing DROP COLUMN logic (lines 540-549)
    } else if p.isKeyword("CONSTRAINT") {
        p.advance()
        name, err := p.expect(tokenIdent)  // tokenIdent at parser_tokens.go:8
        if err != nil {
            return nil, p.errorf("expected constraint name after DROP CONSTRAINT")
        }
        stmt.Operation = AlterTableDropConstraint
        stmt.ConstraintName = name.value
        // Optional IF EXISTS
        if p.isKeyword("IF") {
            p.advance()
            if err := p.expectKeyword("EXISTS"); err != nil {
                return nil, err
            }
            stmt.IfExists = true
        }
    }
```

### 3.3 Executor Changes (executor/ddl.go:457-576)

In `executeAlterTable()`, add cases for the new operations:

```go
case AlterTableAddConstraint:
    // Validate constraint columns exist in table
    constraint := plan.Constraint
    // Convert parser TableConstraint to catalog constraint (reuse binder logic)
    // Append to tableDef.Constraints
    // For FOREIGN KEY: validate referenced table and columns exist
    // For UNIQUE: optionally create a unique index
    // Update WAL

case AlterTableDropConstraint:
    // Find constraint by name in tableDef.Constraints
    // Remove it from the slice
    // For UNIQUE with associated index: drop the index
    // Update WAL
```

## Helper Signatures Reference (Verified)

- `parseCreate()` — parser.go:2004-2061 — parses OR REPLACE (2009-2016) and TEMPORARY (2025-2027)
- `parseCreateTable()` — parser.go:2063-2210 — currently no parameters for flags
- `CreateTableStmt` — ast.go:391-410 — Schema, Table, IfNotExists, Columns, PrimaryKey, Constraints, AsSelect
- `parseAlterTable()` — parser_ddl.go:470-604 — dispatches on ADD/DROP/RENAME/ALTER keywords
- `AlterTableStmt` — ast.go:654-681 — AlterTableOp enum + operation-specific fields
- `AlterTableOp` — ast.go:655-664 — 6 current operations
- `TableConstraint` — ast.go:379-389 — parsed constraint definition
- `parseTableConstraint()` — parser.go:2212-2299 — parses UNIQUE/CHECK/FOREIGN KEY
- `UniqueConstraintDef` — catalog/constraint.go:15-29 — Name, Columns
- `CheckConstraintDef` — catalog/constraint.go:31-42 — Name, Expression
- `ForeignKeyConstraintDef` — catalog/constraint.go:55-79 — Name, Columns, RefTable, RefColumns, OnDelete, OnUpdate
- `TableDef.Constraints` — catalog/table.go:32-34 — `[]any`
- `bindCreateTable()` — binder/bind_stmt.go:3193-3302 — converts constraints
- `BoundCreateTableStmt` — binder/statements.go:134-142 — bound version
- `PhysicalCreateTable` — planner/physical.go:600-607 — physical plan node
- `executeCreateTable()` — executor/operator.go:2603-2727 — creates table with constraints
- `executeAlterTable()` — executor/ddl.go:457-576 — dispatches ALTER operations
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. CREATE OR REPLACE TABLE: Create table, replace it with different schema, verify old data gone
2. CREATE OR REPLACE with IF NOT EXISTS: Verify error (mutually exclusive)
3. CREATE TEMP TABLE: Create temp table, verify visible in same connection, verify schema is "temp"
4. CREATE TEMP TABLE: Verify temp table shadows regular table with same name
5. ALTER TABLE ADD CONSTRAINT UNIQUE: Add unique constraint, verify violations detected
6. ALTER TABLE ADD CONSTRAINT CHECK: Add check constraint, verify violations detected
7. ALTER TABLE ADD CONSTRAINT FOREIGN KEY: Add FK, verify referential integrity
8. ALTER TABLE DROP CONSTRAINT: Drop named constraint, verify constraint removed
9. ALTER TABLE DROP CONSTRAINT IF EXISTS: Non-existent constraint, no error
10. Error cases: DROP non-existent constraint, ADD constraint on non-existent column

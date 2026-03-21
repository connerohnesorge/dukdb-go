# Design: DDL/DML Extensions

## Architecture

Each feature touches the full stack: Parser → Binder → Planner → Executor → Catalog.

## 1. COMMENT ON Statement

### 1.1 AST (internal/parser/ast.go)

Add after existing DDL statement types (around line 676):

```go
// CommentStmt represents COMMENT ON object IS 'text'
type CommentStmt struct {
    ObjectType string // "TABLE", "COLUMN", "VIEW", "INDEX", "SCHEMA"
    Schema     string // optional schema qualifier
    ObjectName string // table/view/index/schema name
    ColumnName string // for COLUMN comments: column name
    Comment    *string // comment text; nil to drop comment (COMMENT ON ... IS NULL)
}

func (*CommentStmt) stmtNode() {}
```

### 1.2 Parser (internal/parser/parser_ddl.go)

Add `parseComment()` function. Register in parser.go main switch (line 47-100) — add case for "COMMENT" keyword:

```go
case "COMMENT":
    return p.parseComment()
```

Implementation:

```go
func (p *parser) parseComment() (*CommentStmt, error) {
    p.advance() // consume COMMENT
    if err := p.expectKeyword("ON"); err != nil {
        return nil, err
    }

    stmt := &CommentStmt{}

    // Parse object type
    switch {
    case p.isKeyword("TABLE"):
        p.advance()
        stmt.ObjectType = "TABLE"
    case p.isKeyword("COLUMN"):
        p.advance()
        stmt.ObjectType = "COLUMN"
    case p.isKeyword("VIEW"):
        p.advance()
        stmt.ObjectType = "VIEW"
    case p.isKeyword("INDEX"):
        p.advance()
        stmt.ObjectType = "INDEX"
    case p.isKeyword("SCHEMA"):
        p.advance()
        stmt.ObjectType = "SCHEMA"
    default:
        return nil, p.errorf("expected TABLE, COLUMN, VIEW, INDEX, or SCHEMA after COMMENT ON")
    }

    // Parse object name using direct identifier parsing with dot-notation
    // Pattern: [schema.]name[.column]
    // No parseQualifiedName() helper exists — parse identifiers manually
    firstTok, err := p.expect(tokenIdent)
    if err != nil {
        return nil, p.errorf("expected object name")
    }
    firstName := firstTok.value

    if p.current().typ == tokenDot {
        p.advance() // consume dot
        secondTok, err := p.expect(tokenIdent)
        if err != nil {
            return nil, p.errorf("expected name after dot")
        }
        secondName := secondTok.value

        if stmt.ObjectType == "COLUMN" && p.current().typ == tokenDot {
            // schema.table.column
            p.advance()
            thirdTok, err := p.expect(tokenIdent)
            if err != nil {
                return nil, p.errorf("expected column name")
            }
            stmt.Schema = firstName
            stmt.ObjectName = secondName
            stmt.ColumnName = thirdTok.value
        } else if stmt.ObjectType == "COLUMN" {
            // table.column (no schema)
            stmt.ObjectName = firstName
            stmt.ColumnName = secondName
        } else {
            // schema.table (non-column)
            stmt.Schema = firstName
            stmt.ObjectName = secondName
        }
    } else {
        stmt.ObjectName = firstName
    }

    if err := p.expectKeyword("IS"); err != nil {
        return nil, err
    }

    // Parse comment value: string literal or NULL
    if p.isKeyword("NULL") {
        p.advance()
        stmt.Comment = nil // NULL drops the comment
    } else {
        tok, err := p.expect(tokenString)
        if err != nil {
            return nil, p.errorf("expected string literal or NULL after IS")
        }
        stmt.Comment = &tok.value
    }

    return stmt, nil
}
```

### 1.3 Catalog Changes (internal/catalog/)

Add `Comment string` field to:

**table.go** — `TableDef` struct (line 15):
```go
type TableDef struct {
    // ... existing fields ...
    Comment string // NEW: table-level comment
}
```

**column.go** — `ColumnDef` struct (line 7):
```go
type ColumnDef struct {
    // ... existing fields ...
    Comment string // NEW: column-level comment
}
```

Similar additions to index and view definitions if they exist as catalog types.

### 1.4 Binder (internal/binder/)

Add `BoundCommentStmt` to statements.go:
```go
type BoundCommentStmt struct {
    ObjectType string
    Schema     string
    ObjectName string
    ColumnName string
    Comment    *string
}
```

Add `bindComment()` in bind_ddl.go — resolve table/column/view/index references exist.

### 1.5 Planner (internal/planner/physical.go)

Add `PhysicalComment` plan node. Follow pattern of `PhysicalAlterTable` (simple passthrough from bound statement).

### 1.6 Executor (internal/executor/ddl.go)

Add `executeComment()`:
```go
func (e *Executor) executeComment(ctx *ExecutionContext, stmt *binder.BoundCommentStmt) (*ExecutionResult, error) {
    switch stmt.ObjectType {
    case "TABLE":
        tableDef, err := e.catalog.GetTable(stmt.Schema, stmt.ObjectName)
        if err != nil {
            return nil, err
        }
        if stmt.Comment != nil {
            tableDef.Comment = *stmt.Comment
        } else {
            tableDef.Comment = ""
        }
    case "COLUMN":
        tableDef, err := e.catalog.GetTable(stmt.Schema, stmt.ObjectName)
        if err != nil {
            return nil, err
        }
        col := tableDef.GetColumn(stmt.ColumnName)
        if col == nil {
            return nil, &dukdb.Error{
                Type: dukdb.ErrorTypeExecutor,
                Msg:  fmt.Sprintf("column %q not found in table %q", stmt.ColumnName, stmt.ObjectName),
            }
        }
        if stmt.Comment != nil {
            col.Comment = *stmt.Comment
        } else {
            col.Comment = ""
        }
    // ... VIEW, INDEX, SCHEMA cases ...
    }
    return &ExecutionResult{AffectedRows: 0}, nil
}
```

Register in operator.go dispatch (line 364-495).

---

## 2. ALTER TABLE ALTER COLUMN TYPE

### 2.1 AST Changes (internal/parser/ast.go)

Add new operation to AlterTableOp enum (line 653):
```go
const (
    AlterTableRenameTo AlterTableOp = iota
    AlterTableRenameColumn
    AlterTableDropColumn
    AlterTableAddColumn
    AlterTableSetOption
    AlterTableAlterColumnType // NEW
)
```

Add fields to `AlterTableStmt` (line 659):
```go
type AlterTableStmt struct {
    // ... existing fields ...
    AlterColumn    string         // NEW: ALTER COLUMN name
    NewColumnType  dukdb.Type     // NEW: SET DATA TYPE / TYPE
    NewTypeRaw     string         // NEW: raw type string for binder
}
```

### 2.2 Parser (internal/parser/parser_ddl.go)

In `parseAlterTable()` (line 470), add handling for `ALTER COLUMN`:

```go
// After existing RENAME/DROP/ADD handling
case p.isKeyword("ALTER"):
    p.advance() // consume ALTER
    if p.isKeyword("COLUMN") {
        p.advance() // consume COLUMN (optional in some dialects)
    }
    colName, err := p.expect(tokenIdent)
    if err != nil {
        return nil, p.errorf("expected column name after ALTER [COLUMN]")
    }
    // Expect SET DATA TYPE or TYPE
    if p.isKeyword("SET") {
        p.advance()
        if err := p.expectKeyword("DATA"); err != nil {
            return nil, err
        }
    }
    if err := p.expectKeyword("TYPE"); err != nil {
        return nil, p.errorf("expected TYPE or SET DATA TYPE after column name")
    }
    newType, err := p.parseTypeName()
    if err != nil {
        return nil, err
    }
    stmt.Operation = AlterTableAlterColumnType
    stmt.AlterColumn = colName.value
    stmt.NewTypeRaw = newType // pass raw type string to binder
```

**Note:** `parseTypeName()` is at parser.go:6056 — it parses type specifications including size modifiers (e.g., VARCHAR(255), DECIMAL(10,2)) and returns a type representation.

### 2.3 Binder (internal/binder/bind_ddl.go)

In `bindAlterTable()` (line 308), add case for `AlterTableAlterColumnType`:
- Resolve the column exists in the table
- Resolve the new type from raw string to `dukdb.Type`
- Check type conversion is valid (numeric→numeric, string→string, etc.)

### 2.4 Executor (internal/executor/ddl.go)

In `executeAlterTable()` (line 457), add case:
```go
case parser.AlterTableAlterColumnType:
    colIdx := -1
    for i, col := range tableDef.Columns {
        if strings.EqualFold(col.Name, stmt.AlterColumn) {
            colIdx = i
            break
        }
    }
    if colIdx == -1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("column %q not found", stmt.AlterColumn),
        }
    }

    oldType := tableDef.Columns[colIdx].Type
    newType := stmt.NewColumnType

    // Update catalog
    tableDef.Columns[colIdx].Type = newType

    // Convert existing data in storage
    storageTable, err := e.getStorageTable(stmt.Schema, stmt.Table)
    if err != nil {
        return nil, err
    }
    if err := storageTable.ConvertColumnType(colIdx, oldType, newType); err != nil {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ALTER COLUMN TYPE: %v", err),
        }
    }
```

### 2.5 Storage Layer

Add `ConvertColumnType(colIdx int, oldType, newType dukdb.Type) error` to the storage table. This iterates over all RowGroups and converts each column vector's data using existing cast/conversion functions.

---

## 3. DELETE ... USING

### 3.1 AST Changes (internal/parser/ast.go)

Add `Using` field to `DeleteStmt` (line 359):
```go
type DeleteStmt struct {
    Schema    string
    Table     string
    Using     []TableRef  // NEW: USING clause table references
    Where     Expr
    Returning []SelectColumn
}
```

### 3.2 Parser (internal/parser/parser.go)

In `parseDelete()` (line 1879), add USING clause parsing after FROM and before WHERE:

```go
// Parse USING clause (optional)
if p.isKeyword("USING") {
    p.advance()
    // Parse one or more table references separated by commas
    for {
        tableRef, err := p.parseTableRef()
        if err != nil {
            return nil, err
        }
        stmt.Using = append(stmt.Using, tableRef)
        if p.current().typ != tokenComma {
            break
        }
        p.advance() // consume comma
    }
}
```

**Key insight:** `parseTableRef()` already exists (used for FROM clause) — reuse it for USING tables.

### 3.3 Binder (internal/binder/bind_stmt.go)

In `bindDelete()` (line 3086), bind the USING tables:

```go
// Bind USING tables
if len(stmt.Using) > 0 {
    for _, usingRef := range stmt.Using {
        boundRef, err := b.bindTableRef(usingRef)
        if err != nil {
            return nil, err
        }
        bound.Using = append(bound.Using, boundRef)
    }
    // USING tables are available for column resolution in WHERE
    // Push USING table scopes for WHERE clause binding
}
```

Add `Using []BoundTableRef` to `BoundDeleteStmt` in statements.go.

### 3.4 Planner / Executor

The USING clause converts to a cross-join of the USING tables with the target table, filtered by the WHERE condition. The planner should generate:

```
PhysicalDelete
  └── PhysicalFilter (WHERE condition)
       └── PhysicalCrossProduct / PhysicalJoin
            ├── Scan(target_table)
            └── Scan(using_table)
```

The existing delete executor already works with RowIDs collected from a scan. The USING clause simply adds more tables to the scan/filter stage.

### 3.5 Example

```sql
-- Delete orders from customers who are inactive
DELETE FROM orders
USING customers
WHERE orders.customer_id = customers.id AND customers.active = false;
```

This is equivalent to:
```sql
DELETE FROM orders WHERE customer_id IN (
    SELECT id FROM customers WHERE active = false
);
```

---

## Helper Signatures Reference

- `parseTableRef()` — parser.go:773 — parses table reference with optional alias/join
- `parseTypeName()` — parser.go:6056 — parses column type specification (VARCHAR(255), DECIMAL(10,2), etc.)
- `getStorageTable(schema, table string)` — executor — retrieves storage-layer table
- `AlterTableOp` enum — ast.go:653 — current ops: RenameTo, RenameColumn, DropColumn, AddColumn, SetOption
- `AlterTableStmt` — ast.go:659 — existing struct with fields for each op type
- `BoundDeleteStmt` — statements.go:120 — existing bound delete statement type
- `errorf()` — parser method for creating parse errors

## Testing Strategy

### COMMENT ON
1. `COMMENT ON TABLE t IS 'my table'` — sets comment
2. `COMMENT ON COLUMN t.c IS 'my column'` — sets column comment
3. `COMMENT ON TABLE t IS NULL` — drops comment
4. Error: COMMENT ON non-existent table
5. Verify comments persist across transactions

### ALTER TABLE ALTER COLUMN TYPE
1. `ALTER TABLE t ALTER COLUMN c TYPE INTEGER` — change VARCHAR to INTEGER
2. `ALTER TABLE t ALTER COLUMN c SET DATA TYPE VARCHAR` — alternative syntax
3. Error: incompatible type conversion
4. Error: column doesn't exist
5. Verify data is correctly converted

### DELETE ... USING
1. `DELETE FROM t1 USING t2 WHERE t1.id = t2.id` — multi-table delete
2. Multiple USING tables
3. USING with WHERE and RETURNING
4. Error: USING table doesn't exist
5. Verify correct rows deleted, others untouched

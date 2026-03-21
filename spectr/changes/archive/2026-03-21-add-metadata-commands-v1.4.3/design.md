# Design: Metadata Commands

## Architecture

These are all "utility" statements that return result sets from catalog metadata. They follow the pattern used by EXPLAIN (ast.go:1360): a statement type that wraps a reference or query, executed as a metadata lookup.

All new statement types implement `stmtNode()` and `Type()` on the `Statement` interface (ast.go:8-12). SHOW extensions reuse the existing `ShowStmt` (ast.go:1662) with a new `TableName` field. Other statements get new AST types.

## 1. DESCRIBE Statement

### 1.1 AST (internal/parser/ast.go)

```go
// DescribeStmt represents DESCRIBE table or DESCRIBE SELECT ...
type DescribeStmt struct {
    TableName string    // DESCRIBE tablename
    Schema    string    // optional schema qualifier
    Query     Statement // DESCRIBE SELECT ... (wraps a statement)
}

func (*DescribeStmt) stmtNode() {}
func (*DescribeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }
```

### 1.2 Parser (internal/parser/parser.go)

Add to main dispatch at parser.go:47-129 (before `default:` at line 120):
```go
case p.isKeyword("DESCRIBE"), p.isKeyword("DESC"):
    stmt, err = p.parseDescribe()
```

Implementation in parser_pragma.go (add after parseShow at line 289):
```go
func (p *parser) parseDescribe() (*DescribeStmt, error) {
    p.advance() // consume DESCRIBE/DESC
    stmt := &DescribeStmt{}

    // DESCRIBE SELECT ...
    if p.isKeyword("SELECT") || p.isKeyword("WITH") {
        var query Statement
        var err error
        if p.isKeyword("SELECT") {
            query, err = p.parseSelect()
        } else {
            query, err = p.parseWithSelect()
        }
        if err != nil {
            return nil, err
        }
        stmt.Query = query
        return stmt, nil
    }

    // DESCRIBE [schema.]table
    name, err := p.expect(tokenIdent)  // tokenIdent at parser_tokens.go:8
    if err != nil {
        return nil, p.errorf("expected table name or SELECT after DESCRIBE")
    }
    stmt.TableName = name.value
    if p.current().typ == tokenDot {  // tokenDot at parser_tokens.go:17
        p.advance()
        tableName, err := p.expect(tokenIdent)
        if err != nil {
            return nil, err
        }
        stmt.Schema = stmt.TableName
        stmt.TableName = tableName.value
    }
    return stmt, nil
}
```

### 1.3 Executor

DESCRIBE table is handled at the connection level in `handleShow()` (engine/conn.go:626), similar to how SHOW variables are handled. Returns columns from `TableDef.Columns` ([]*ColumnDef at catalog/table.go:24):

```go
func (c *EngineConn) executeDescribe(tableName, schemaName string) ([]map[string]any, []string, error) {
    columns := []string{"column_name", "column_type", "null", "key", "default", "extra"}
    rows := make([]map[string]any, 0)

    tableDef := c.catalog.GetTable(tableName, schemaName)
    if tableDef == nil {
        return nil, nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("table %q not found", tableName),
        }
    }

    for i, col := range tableDef.Columns {
        // ColumnDef fields: Name (line 10), Type (line 13), Nullable (line 20),
        // HasDefault (line 26), DefaultValue (line 23) — all in catalog/column.go
        isPK := "NO"
        for _, pkIdx := range tableDef.PrimaryKey {
            if pkIdx == i {
                isPK = "YES"
                break
            }
        }
        // Note: boolToYesNo() does NOT exist in internal/executor — use inline logic
        nullStr := "YES"
        if !col.Nullable {
            nullStr = "NO"
        }
        defaultVal := interface{}(nil)
        if col.HasDefault {
            defaultVal = col.DefaultValue
        }
        rows = append(rows, map[string]any{
            "column_name": col.Name,
            "column_type": col.Type.String(),
            "null":        nullStr,
            "key":         isPK,
            "default":     defaultVal,
            "extra":       "",
        })
    }
    return rows, columns, nil
}
```

For DESCRIBE SELECT, bind the inner query (don't execute) and return the output columns and types.

## 2. SHOW TABLES / SHOW ALL TABLES

### 2.1 Extend ShowStmt (ast.go:1662) and parseShow() (parser_pragma.go:275)

Add `TableName string` field to the existing ShowStmt at ast.go:1662.

Extend `parseShow()` at parser_pragma.go:275-289 to handle SHOW TABLES:

```go
func (p *parser) parseShow() (*ShowStmt, error) {
    if err := p.expectKeyword("SHOW"); err != nil {
        return nil, err
    }
    stmt := &ShowStmt{}

    // SHOW ALL TABLES
    if p.isKeyword("ALL") {
        p.advance()
        if p.isKeyword("TABLES") {
            p.advance()
            stmt.Variable = "__all_tables"
            return stmt, nil
        }
        // SHOW ALL (settings)
        stmt.Variable = "__all_settings"
        return stmt, nil
    }

    // SHOW TABLES
    if p.isKeyword("TABLES") {
        p.advance()
        stmt.Variable = "__tables"
        return stmt, nil
    }

    // SHOW COLUMNS FROM table
    if p.isKeyword("COLUMNS") {
        p.advance()
        if err := p.expectKeyword("FROM"); err != nil {
            return nil, err
        }
        tableName, err := p.expect(tokenIdent)
        if err != nil {
            return nil, p.errorf("expected table name after SHOW COLUMNS FROM")
        }
        stmt.Variable = "__columns"
        stmt.TableName = tableName.value
        return stmt, nil
    }

    // SHOW variable (existing behavior)
    if p.current().typ != tokenIdent {
        return nil, p.errorf("expected variable name after SHOW")
    }
    stmt.Variable = p.advance().value
    return stmt, nil
}
```

### 2.2 Executor

Handle the special variable names in `handleShow()` at conn.go:626:

```go
case "__tables":
    // List tables in current schema
    return e.executeShowTables(ctx, "main")
case "__all_tables":
    // List tables in all schemas
    return e.executeShowAllTables(ctx)
case "__columns":
    // Show columns for a specific table
    return e.executeShowColumns(ctx, stmt.TableName)
```

`executeShowTables` uses `Schema.ListTables()` (catalog.go:675) and returns name column. Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: ...}`.

## 3. SUMMARIZE Statement

### 3.1 AST

```go
type SummarizeStmt struct {
    TableName string
    Schema    string
    Query     Statement // SUMMARIZE SELECT ...
}
func (*SummarizeStmt) stmtNode() {}
func (*SummarizeStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }
```

### 3.2 Parser

Add to main dispatch at parser.go:47-129:
```go
case p.isKeyword("SUMMARIZE"):
    stmt, err = p.parseSummarize()
```

Implementation in parser_pragma.go follows the same pattern as parseDescribe — parse table name with optional schema qualifier, or wrap a SELECT/WITH query.

### 3.3 Executor

SUMMARIZE executes a `SELECT * FROM table`, then computes per-column statistics:
- column_name, column_type, min, max, unique_count, null_count, avg, std, count

Returns one row per column with these statistics. Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: ...}`.

## 4. CALL Statement

### 4.1 AST

```go
type CallStmt struct {
    FunctionName string
    Args         []Expr
}
func (*CallStmt) stmtNode() {}
func (*CallStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CALL }
// Note: STATEMENT_TYPE_CALL already exists in stmt_type.go:44
```

### 4.2 Parser

Add to main dispatch at parser.go:47-129:
```go
case p.isKeyword("CALL"):
    stmt, err = p.parseCall()
```

Parse function name and arguments using tokenIdent, tokenLeftParen, tokenRightParen, tokenComma (all in parser_tokens.go). Expression parsing via `p.parseExpression()`.

### 4.3 Executor

CALL delegates to table function execution. For table functions like generate_series (bound at binder/bind_stmt.go:1152 via `bindGenerateSeriesTableFunction()`, executed via `executeTableFunctionScan`), convert CALL to equivalent table function invocation. For scalar functions, wrap the result as a single-row, single-column result.

## Helper Signatures Reference (Verified)

- `ShowStmt` — ast.go:1662 — existing SHOW statement AST, has `Variable string`
- `ShowStmt.Type()` — ast.go:1670 — returns `STATEMENT_TYPE_SELECT`
- `handleShow()` — engine/conn.go:626 — existing SHOW handler on EngineConn
- `ExplainStmt` — ast.go:1360 — pattern for wrapping another statement with Query field
- `parseExplain()` — parser_pragma.go:51-90 — pattern for parsing wrapped statements
- `parseShow()` — parser_pragma.go:275-289 — current SHOW parser (only handles variables)
- `TableDef.Columns` — catalog/table.go:24 — `[]*ColumnDef`
- `ColumnDef` — catalog/column.go — Name (10), Type (13), Nullable (20), HasDefault (26), DefaultValue (23), Comment (29)
- `Catalog.ListTables()` — catalog.go:244 — lists all tables across schemas
- `Schema.ListTables()` — catalog.go:675 — lists tables in one schema
- `ExecutionResult` — executor/operator.go:74-78 — has Rows, Columns, RowsAffected
- `STATEMENT_TYPE_CALL` — stmt_type.go:44 — already exists
- Main parser dispatch — parser.go:47-129 — keyword switch
- Token types — parser_tokens.go — tokenIdent (8), tokenDot (17), tokenLeftParen, tokenRightParen, tokenComma
- `boolToYesNo()` — does NOT exist in internal/executor, only in examples — use inline logic

## Testing Strategy

1. DESCRIBE: `DESCRIBE employees` → columns with types, null, key info
2. DESCRIBE SELECT: `DESCRIBE SELECT 1 AS x, 'hello' AS y` → column names and types
3. SHOW TABLES: `SHOW TABLES` → list of tables in current schema
4. SHOW ALL TABLES: `SHOW ALL TABLES` → tables across all schemas
5. SHOW COLUMNS: `SHOW COLUMNS FROM employees` → same as DESCRIBE
6. SUMMARIZE: `SUMMARIZE employees` → per-column statistics
7. CALL: `CALL generate_series(1, 10)` → result set from table function
8. Error cases: DESCRIBE non-existent table, SHOW COLUMNS FROM non-existent table

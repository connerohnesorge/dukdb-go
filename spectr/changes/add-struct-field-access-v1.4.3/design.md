# Design: Struct Dot Notation Field Access

## Architecture

The key challenge is the parser ambiguity between `table.column` and `struct_col.field`. The approach is:
1. Parser continues creating ColumnRef for `a.b` syntax (no parser change needed)
2. Binder resolves the ambiguity: if `a` is a table name, it's table.column; if `a` resolves to a struct-typed column, it becomes a field access
3. A new `BoundFieldAccess` expression type handles the evaluation

This approach is used by PostgreSQL and DuckDB — the binder/resolver disambiguates, not the parser.

## 1. AST Changes

No parser AST changes needed. The existing ColumnRef is sufficient because:
- `struct_col.field` looks syntactically identical to `table.column`
- The parser already creates ColumnRef{Table: "struct_col", Column: "field"}
- The binder resolves the semantic meaning

## 2. Binder Changes (binder/bind_expr.go:113-181)

### 2.1 New Bound Expression (binder/expressions.go)

Add after BoundColumnRef (line 18):

```go
// BoundFieldAccess represents struct.field access
type BoundFieldAccess struct {
    Struct  BoundExpr    // The struct-typed expression
    Field   string       // Field name to extract
    ResType dukdb.Type   // Resolved field type
}

func (*BoundFieldAccess) boundExprNode() {}
func (f *BoundFieldAccess) ResultType() dukdb.Type { return f.ResType }
```

### 2.2 Modify bindColumnRef() (binder/bind_expr.go:113-181)

Current flow at line 115: if `ref.Table != ""`, look up table by name. If not found, error "table not found".

Change to: if table not found, check if `ref.Table` is a column name with struct type:

```go
func (b *Binder) bindColumnRef(ref *parser.ColumnRef) (BoundExpr, error) {
    if ref.Table != "" {
        // First, try as table.column reference (existing behavior)
        tableRef, ok := b.scope.tables[ref.Table]
        if ok {
            // ... existing table.column resolution (lines 117-150) ...
        }

        // Table not found — try as struct_column.field_name
        // Search all tables for a column named ref.Table with struct type
        for _, tableRef := range b.scope.tables {
            for _, col := range tableRef.Columns {
                if strings.EqualFold(col.Column, ref.Table) && col.Type == dukdb.TYPE_STRUCT {
                    // Found a struct column — resolve as field access
                    structRef := &BoundColumnRef{
                        Table:     tableRef.TableName,
                        Column:    col.Column,
                        ColumnIdx: col.ColumnIdx,
                        ColType:   col.Type,
                    }
                    return &BoundFieldAccess{
                        Struct:  structRef,
                        Field:   ref.Column,  // The "column" part is the field name
                        ResType: dukdb.TYPE_ANY,  // Struct field types are dynamic
                    }, nil
                }
            }
        }

        // Neither table nor struct column found
        return nil, b.errorf("table or struct column not found: %s", ref.Table)
    }

    // ... existing unqualified column resolution ...
}
```

### 2.3 Chained Access (table.struct_col.field)

For three-level access like `t.struct_col.field`, the parser would need to handle multi-level dots. Currently parseIdentExpr() only handles one level. To support this:

**Phase 1** (this proposal): Support `struct_col.field` — the common case where struct column is unambiguous.

**Phase 2** (future): Support `table.struct_col.field` by extending the parser to handle chained dots:
```go
// In parseIdentExpr, after parsing a.b, check for another dot:
if p.current().typ == tokenDot {
    p.advance()
    field := p.advance().value
    // Create a FieldAccessExpr wrapping the ColumnRef
}
```

## 3. Executor Changes

### 3.1 Evaluate BoundFieldAccess (executor/expr.go)

Add to the expression evaluation switch (likely in evaluateExpr):

```go
case *binder.BoundFieldAccess:
    // Evaluate the struct expression
    structVal, err := e.evaluateExpr(ctx, expr.Struct, row)
    if err != nil {
        return nil, err
    }
    if structVal == nil {
        return nil, nil  // NULL propagation
    }
    // Extract field from map[string]any (consistent struct representation)
    m, ok := structVal.(map[string]any)
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("expected struct value, got %T", structVal),
        }
    }
    val, exists := m[expr.Field]
    if !exists {
        // Try case-insensitive lookup
        for k, v := range m {
            if strings.EqualFold(k, expr.Field) {
                return v, nil
            }
        }
        return nil, nil  // Field not found returns NULL
    }
    return val, nil
```

This follows the same pattern as STRUCT_EXTRACT (expr.go:2233-2255) but integrates with the expression evaluation pipeline.

## Helper Signatures Reference (Verified)

- `parseIdentExpr()` — parser.go:5033-5113 — identifier expression parsing
- Dot handling — parser.go:5089-5110 — creates ColumnRef{Table, Column}
- `ColumnRef` — ast.go:719-724 — Table and Column fields
- `bindColumnRef()` — binder/bind_expr.go:113-181 — column resolution
- `BoundColumnRef` — binder/expressions.go:8-18 — resolved column reference
- `STRUCT_EXTRACT` — expr.go:2233-2255 — field extraction from map[string]any
- `evaluateExpr()` — executor/expr.go — expression evaluation dispatch
- Struct runtime type — `map[string]any` — consistent throughout codebase
- TYPE_STRUCT — type_enum.go:36 — Type = 26
- tokenDot — parser_tokens.go:17
- Error pattern: `&dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: fmt.Sprintf(...)}`

## Testing Strategy

1. `SELECT struct_pack(a := 1, b := 2).a` → 1 (dot on expression)
2. `CREATE TABLE t(s STRUCT(name VARCHAR, age INTEGER)); SELECT s.name FROM t` → field access
3. `SELECT s.nonexistent FROM t` → NULL (missing field)
4. `SELECT s.name FROM t WHERE s.age > 25` → field access in WHERE
5. `SELECT t.id, s.name FROM t` → mixed table.column and struct.field
6. Ambiguity test: table named "s" AND column named "s" — table reference takes priority
7. NULL struct → NULL (NULL propagation)

# Change: Add Parameter Type Inference

## Why

The dukdb-go driver's `ParamType()` always returns `TYPE_ANY` for all parameters, while duckdb-go (via C API) can infer actual parameter types from context. This limits:

1. **Type validation** - Can't detect type mismatches before execution
2. **Driver clients** - ORMs and query builders can't optimize bindings
3. **Documentation** - Users can't introspect expected parameter types
4. **Compatibility** - duckdb-go returns actual types, dukdb-go returns ANY

**Current State** (from exploration):
- `internal/engine/conn.go`: ParamType() returns TYPE_ANY for all parameters
- `internal/binder/binder.go`: Parameters bound with TYPE_ANY immediately
- No context-based type inference implemented

**duckdb-go Reference**:
- `statement.go`: ParamType(n) calls C API `duckdb_prepared_statement_get_parameter_type()`
- Returns actual type inferred from schema (e.g., column comparison)
- Tests verify: `WHERE baz = ?` → TYPE_INTEGER when baz is INTEGER column

## What Changes

### 1. Expected Type Propagation in Binder (internal/binder/binder.go)

Modify expression binding to accept and propagate expected types:

```go
// Add expectedType parameter to bindExpr
func (b *Binder) bindExpr(expr parser.Expr, expectedType Type) (BoundExpr, error) {
    switch e := expr.(type) {
    case *parser.Parameter:
        return b.bindParameter(e, expectedType)
    case *parser.BinaryExpr:
        return b.bindBinaryExpr(e, expectedType)
    // ... other expression types
    }
}

// BoundParameter now tracks inferred type
type BoundParameter struct {
    Position   int
    ParamType  Type     // Inferred type (no longer always ANY)
    ExpectedBy []string // Debug: what expressions expected this type
}

func (b *Binder) bindParameter(p *parser.Parameter, expectedType Type) (*BoundParameter, error) {
    pos := p.Position
    if pos == 0 {
        pos = b.scope.paramCount + 1
    }
    b.scope.paramCount++

    // Record inferred type
    inferredType := expectedType
    if inferredType == TYPE_UNKNOWN {
        inferredType = TYPE_ANY // fallback if no context
    }

    // Track parameter in scope for later lookup
    b.scope.params[pos] = inferredType

    return &BoundParameter{
        Position:  pos,
        ParamType: inferredType,
    }, nil
}
```

### 2. Binary Expression Type Propagation

```go
func (b *Binder) bindBinaryExpr(e *parser.BinaryExpr, expectedType Type) (*BoundBinaryExpr, error) {
    switch e.Op {
    case parser.EQ, parser.NE, parser.LT, parser.LE, parser.GT, parser.GE:
        // Comparison operators: both sides should have same type

        // Try binding left side first (may resolve column type)
        left, err := b.bindExpr(e.Left, TYPE_UNKNOWN)
        if err != nil {
            return nil, err
        }

        // Use left's type to infer right's type
        leftType := left.Type()
        right, err := b.bindExpr(e.Right, leftType)
        if err != nil {
            return nil, err
        }

        // If left was parameter, try using right's type
        if leftType == TYPE_ANY || leftType == TYPE_UNKNOWN {
            rightType := right.Type()
            if rightType != TYPE_ANY && rightType != TYPE_UNKNOWN {
                b.updateParamType(left, rightType)
            }
        }

        return &BoundBinaryExpr{Left: left, Op: e.Op, Right: right}, nil

    case parser.PLUS, parser.MINUS, parser.MUL, parser.DIV:
        // Arithmetic operators: infer numeric context
        left, _ := b.bindExpr(e.Left, TYPE_DOUBLE)
        right, _ := b.bindExpr(e.Right, TYPE_DOUBLE)
        return &BoundBinaryExpr{Left: left, Op: e.Op, Right: right}, nil
    }
}
```

### 3. Column Reference Type Lookup

```go
// When binding a column reference, look up its type from catalog
func (b *Binder) bindColumnRef(ref *parser.ColumnRef) (*BoundColumnRef, error) {
    // Look up in catalog
    col, err := b.catalog.LookupColumn(ref.Table, ref.Column)
    if err != nil {
        return nil, err
    }

    return &BoundColumnRef{
        Table:     ref.Table,
        Column:    ref.Column,
        ColType:   col.Type,
        TypeInfo:  col.TypeInfo,
    }, nil
}
```

### 4. Parameter Type Storage in Statement (internal/engine/conn.go)

```go
type EngineStmt struct {
    // ... existing fields
    paramTypes map[int]Type // Parameter position → inferred type
}

func (e *Engine) Prepare(ctx context.Context, query string) (BackendStmt, error) {
    // Parse and bind
    parsed, err := e.parser.Parse(query)
    if err != nil {
        return nil, err
    }

    bound, err := e.binder.Bind(parsed)
    if err != nil {
        return nil, err
    }

    // Extract parameter types from binder scope
    paramTypes := make(map[int]Type)
    for pos, typ := range e.binder.scope.params {
        paramTypes[pos] = typ
    }

    return &EngineStmt{
        query:      query,
        parsed:     parsed,
        bound:      bound,
        paramTypes: paramTypes,
        numParams:  len(paramTypes),
    }, nil
}

// ParamType now returns inferred type instead of TYPE_ANY
func (s *EngineStmt) ParamType(index int) Type {
    if index < 1 || index > s.numParams {
        return TYPE_INVALID
    }
    if typ, ok := s.paramTypes[index]; ok {
        return typ
    }
    return TYPE_ANY // fallback for unresolved
}
```

### 5. Function Argument Type Inference

```go
func (b *Binder) bindFunctionCall(fn *parser.FunctionCall) (*BoundFunctionCall, error) {
    // Look up function signature from catalog
    funcDef, err := b.catalog.LookupFunction(fn.Name)
    if err != nil {
        return nil, err
    }

    // Bind each argument with expected type from signature
    boundArgs := make([]BoundExpr, len(fn.Args))
    for i, arg := range fn.Args {
        expectedType := TYPE_ANY
        if i < len(funcDef.ParamTypes) {
            expectedType = funcDef.ParamTypes[i]
        }
        boundArg, err := b.bindExpr(arg, expectedType)
        if err != nil {
            return nil, err
        }
        boundArgs[i] = boundArg
    }

    return &BoundFunctionCall{
        Name:   fn.Name,
        Args:   boundArgs,
        RetType: funcDef.ReturnType,
    }, nil
}
```

### 6. INSERT/UPDATE Value Type Inference

```go
func (b *Binder) bindInsertStmt(ins *parser.InsertStmt) (*BoundInsertStmt, error) {
    // Get table schema
    table, err := b.catalog.LookupTable(ins.Table)
    if err != nil {
        return nil, err
    }

    // Bind values with column types
    for i, row := range ins.Values {
        for j, val := range row {
            // Use column type for parameter inference
            colType := TYPE_ANY
            if j < len(table.Columns) {
                colType = table.Columns[j].Type
            }
            boundVal, err := b.bindExpr(val, colType)
            if err != nil {
                return nil, err
            }
            // ...
        }
    }
}
```

### 7. Binder Scope for Parameter Tracking

```go
type BinderScope struct {
    params     map[int]Type // position → inferred type
    paramCount int
    // ... other scope fields
}

// Update parameter type if we get better information
func (b *Binder) updateParamType(expr BoundExpr, newType Type) {
    if param, ok := expr.(*BoundParameter); ok {
        if param.ParamType == TYPE_ANY || param.ParamType == TYPE_UNKNOWN {
            param.ParamType = newType
            b.scope.params[param.Position] = newType
        }
    }
}
```

### 8. Deterministic Testing

Parameter type inference is purely based on static analysis - no timing dependencies.
Tests should verify:

```go
func TestParameterTypeInference(t *testing.T) {
    db := openTestDB(t)
    defer db.Close()

    // Create table with known schema
    _, err := db.Exec(`CREATE TABLE test (
        id INTEGER,
        name VARCHAR,
        value DOUBLE
    )`)
    require.NoError(t, err)

    testCases := []struct {
        sql           string
        paramIndex    int
        expectedType  Type
    }{
        // Comparison context
        {"SELECT * FROM test WHERE id = ?", 1, TYPE_INTEGER},
        {"SELECT * FROM test WHERE name = $1", 1, TYPE_VARCHAR},
        {"SELECT * FROM test WHERE value > ?", 1, TYPE_DOUBLE},

        // INSERT context
        {"INSERT INTO test (id, name) VALUES (?, ?)", 1, TYPE_INTEGER},
        {"INSERT INTO test (id, name) VALUES (?, ?)", 2, TYPE_VARCHAR},

        // UPDATE context
        {"UPDATE test SET value = ? WHERE id = ?", 1, TYPE_DOUBLE},
        {"UPDATE test SET value = ? WHERE id = ?", 2, TYPE_INTEGER},

        // No context (fallback)
        {"SELECT ? + ?", 1, TYPE_DOUBLE}, // arithmetic context
        {"SELECT ?", 1, TYPE_ANY}, // no context
    }

    for _, tc := range testCases {
        stmt, err := db.Prepare(tc.sql)
        require.NoError(t, err)

        actualType, err := stmt.(*Stmt).ParamType(tc.paramIndex)
        require.NoError(t, err)
        assert.Equal(t, tc.expectedType, actualType, "SQL: %s, param: %d", tc.sql, tc.paramIndex)

        stmt.Close()
    }
}
```

## Impact

- **Affected specs**: parameter-inference (NEW)
- **Affected code**:
  - MODIFIED: `internal/binder/binder.go` (~200 lines - type propagation)
  - MODIFIED: `internal/engine/conn.go` (~50 lines - param type storage)
  - NEW: `internal/binder/type_inference.go` (~150 lines - helpers)

- **Dependencies**:
  - Catalog with table/column schema
  - Function registry with signatures
  - Existing binder infrastructure

## Breaking Changes

None. ParamType() returning actual types instead of TYPE_ANY is an improvement, not a breaking change. Code expecting TYPE_ANY will still work with more specific types.

## Type Inference Coverage

After this change, parameter types will be inferred in these contexts:

| Context | Example | Inference |
|---------|---------|-----------|
| Column comparison | `WHERE col = ?` | Column type |
| INSERT values | `INSERT INTO t(col) VALUES (?)` | Column type |
| UPDATE values | `UPDATE t SET col = ?` | Column type |
| Function arguments | `SELECT abs(?)` | Argument type |
| Arithmetic | `SELECT ? + ?` | DOUBLE |
| BETWEEN | `WHERE col BETWEEN ? AND ?` | Column type |
| IN list | `WHERE col IN (?, ?)` | Column type |
| CASE expression | `CASE WHEN x THEN ? ELSE ? END` | Branch types |
| CAST expression | `CAST(? AS INTEGER)` | Target type |
| UNION branches | Different params in each branch | Each branch context |

Fallback to TYPE_ANY when:
- No context available (standalone expressions)
- Same parameter used in conflicting contexts
- Catalog lookup fails (table/column not found)
- Unknown functions

**Table Alias Resolution**: The binder tracks alias→table mappings. When `FROM t1 AS a`, looking up `a.col` resolves to `t1.col`.

**Error Handling**: Catalog lookup failures (missing table/column) silently return TYPE_ANY rather than propagating errors. This maintains deterministic behavior.

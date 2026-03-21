# Design: Enum Utility Functions

## Architecture

Changes in two files:
1. **Executor** (`internal/executor/expr.go`): Function dispatch and implementation
2. **Binder** (`internal/binder/utils.go`): Type inference entries

## 1. Executor Changes (internal/executor/expr.go)

### Accessing the Catalog

The enum functions need catalog access to look up type definitions. The executor has access via `ExecutionContext`:

```go
type ExecutionContext struct {
    Context          context.Context
    Args             []driver.NamedValue
    CorrelatedValues map[string]any
    conn             ConnectionInterface
}
```

The `conn` field provides `GetSetting()/SetSetting()` but NOT direct catalog access. The executor struct itself (`Executor`) holds the catalog reference. Since `evaluateFunctionCall()` is a method on `*Executor`, it can access `e.catalog` directly.

**Verify:** Check if `Executor` struct has a `catalog` field. The executor is constructed with catalog access (used for table functions, DDL, etc.).

### Implementation

Add cases in `evaluateFunctionCall()` switch (around line 688):

```go
case "ENUM_RANGE":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_RANGE requires exactly 1 argument, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    typeName := toString(args[0])
    typeEntry, ok := e.catalog.GetType(typeName, "")
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_RANGE: type %q not found", typeName),
        }
    }
    if typeEntry.TypeKind != "ENUM" {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_RANGE: type %q is not an ENUM type", typeName),
        }
    }
    // Return all values as []any
    result := make([]any, len(typeEntry.EnumValues))
    for i, v := range typeEntry.EnumValues {
        result[i] = v
    }
    return result, nil

case "ENUM_FIRST":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_FIRST requires exactly 1 argument, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    typeName := toString(args[0])
    typeEntry, ok := e.catalog.GetType(typeName, "")
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_FIRST: type %q not found", typeName),
        }
    }
    if len(typeEntry.EnumValues) == 0 {
        return nil, nil
    }
    return typeEntry.EnumValues[0], nil

case "ENUM_LAST":
    if len(args) != 1 {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_LAST requires exactly 1 argument, got %d", len(args)),
        }
    }
    if args[0] == nil {
        return nil, nil
    }
    typeName := toString(args[0])
    typeEntry, ok := e.catalog.GetType(typeName, "")
    if !ok {
        return nil, &dukdb.Error{
            Type: dukdb.ErrorTypeExecutor,
            Msg:  fmt.Sprintf("ENUM_LAST: type %q not found", typeName),
        }
    }
    if len(typeEntry.EnumValues) == 0 {
        return nil, nil
    }
    return typeEntry.EnumValues[len(typeEntry.EnumValues)-1], nil
```

## 2. Binder Changes (internal/binder/utils.go)

In `inferFunctionResultType()` at line 342, add:

```go
case "ENUM_RANGE":
    return dukdb.TYPE_ANY  // returns list of strings
case "ENUM_FIRST", "ENUM_LAST":
    return dukdb.TYPE_VARCHAR  // returns single enum value string
```

## Helper Signatures Reference

- `TypeEntry` — catalog.go:589 — has Name, Schema, TypeKind, EnumValues fields
- `Catalog.GetType(name, schemaName string) (*TypeEntry, bool)` — catalog.go:1112
- `toString(v any) string` — expr.go:3710
- `evaluateFunctionCall()` — expr.go:630+ — main function dispatch

## Testing Strategy

1. Create enum type: `CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')`
2. `SELECT ENUM_RANGE('mood')` → ['sad', 'ok', 'happy']
3. `SELECT ENUM_FIRST('mood')` → 'sad'
4. `SELECT ENUM_LAST('mood')` → 'happy'
5. Error: `SELECT ENUM_RANGE('nonexistent')` → type not found error
6. Error: `SELECT ENUM_RANGE(NULL)` → NULL
7. Single-value enum: `CREATE TYPE single AS ENUM ('only')` → ENUM_FIRST = ENUM_LAST = 'only'

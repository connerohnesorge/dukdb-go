# Change: Add Scalar User-Defined Functions (UDFs)

## Why

Users need the ability to extend DuckDB's SQL functionality with custom scalar functions written in pure Go. The duckdb-go CGO reference implementation supports scalar UDFs that allow users to define functions like `my_upper(varchar) -> varchar` or `my_add(int, int) -> int` that can be used in SQL queries.

This enables:
- Custom business logic in SQL queries
- Domain-specific transformations
- Integration with external Go libraries
- Extension of DuckDB without modifying the database

## What Changes

### Core Types

1. **ScalarFuncConfig**: Configuration for user-defined scalar functions
   - `InputTypeInfos []TypeInfo` - Input parameter types
   - `ResultTypeInfo TypeInfo` - Return type
   - `VariadicTypeInfo TypeInfo` - Optional variadic parameter type
   - `Volatile bool` - Whether function is non-deterministic
   - `SpecialNullHandling bool` - Custom NULL handling (default: NULL in = NULL out)

2. **ScalarFuncExecutor**: Execution handlers for scalar functions
   - `RowExecutor` - Simple row-based function: `func([]driver.Value) (any, error)`
   - `RowContextExecutor` - Context-aware: `func(ctx context.Context, []driver.Value) (any, error)`
   - `ScalarBinder` - Custom bind phase for constant folding

3. **ScalarFunc Interface**: User-implemented interface
   ```go
   type ScalarFunc interface {
       Config() ScalarFuncConfig
       Executor() ScalarFuncExecutor
   }
   ```

4. **ScalarUDFArg**: Argument metadata during bind phase
   - `Foldable bool` - Whether argument can be constant-folded
   - `Value driver.Value` - Folded value if foldable

### Public API

```go
// Register a single scalar function
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunc) error

// Register function set with overloading
func RegisterScalarUDFSet(c *sql.Conn, name string, functions ...ScalarFunc) error
```

### Execution Model

1. **Registration**: User implements ScalarFunc interface and calls RegisterScalarUDF
2. **Bind Phase**: ScalarBinder (if provided) receives arguments for constant folding
3. **Execution**: For each row, RowExecutor/RowContextExecutor is called with column values
4. **NULL Handling**: Default behavior returns NULL if any input is NULL (unless SpecialNullHandling)

## Impact

- **Affected specs**: Depends on data-chunk-api (required for DataChunk access)
- **Affected code**: New file `scalar_udf.go`
- **Dependencies**: data-chunk-api must be implemented first
- **Consumers**: Users extending DuckDB with custom functions

## Breaking Changes

None. This adds new functionality without modifying existing APIs.

## Implementation Approach

Unlike the CGO reference which uses C callbacks, the pure Go implementation will:
1. Register functions in an internal registry keyed by connection
2. Intercept function calls during query execution in the binder phase
3. Execute the user's Go function directly without CGO callbacks
4. Handle type conversion between SQL values and Go values

### Error Handling Requirements

- **Panic Recovery**: User function calls MUST be wrapped in defer/recover to catch panics and convert them to errors
- **Context Cancellation**: Row iteration loop MUST check context cancellation between rows
- **Error Context**: Errors SHOULD include row index for debugging
- **Type Validation**: Return values SHOULD be validated against declared ResultTypeInfo

### NULL Handling Behavior

With `SpecialNullHandling=false` (default):
- If any input parameter is NULL, the function is NOT called and NULL is returned
- Values array is only populated up to the first NULL found

With `SpecialNullHandling=true`:
- Function is called even if inputs contain NULL
- All parameters are populated before NULL checking

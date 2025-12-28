## Context

Scalar User-Defined Functions (UDFs) allow users to extend SQL with custom single-row functions. Unlike the CGO implementation which relies on C callback mechanisms, the pure Go implementation must handle function registration, binding, and execution entirely within Go.

**Stakeholders**: Users needing custom SQL functions, DuckDB extension developers

**Constraints**:
- No CGO allowed (pure Go requirement)
- Must match duckdb-go public API exactly
- Must handle all 45 DuckDB types as inputs/outputs
- Must support function overloading
- Performance should minimize per-row overhead

## Goals / Non-Goals

### Goals
- API-compatible scalar UDF registration matching duckdb-go
- Support for all DuckDB types as inputs and outputs
- Variadic function support
- Function overloading via RegisterScalarUDFSet
- Context propagation for cancellation and deadlines
- Optional constant folding during bind phase

### Non-Goals
- Aggregate UDFs (separate feature)
- Window functions (separate feature)
- Native SIMD vectorization (Go lacks portable SIMD)
- JIT compilation of user functions

## Decisions

### Decision 1: Function Registry Architecture

**What**: Connection-scoped registry for scalar UDFs

**Why**:
- DuckDB functions are registered per-connection
- Allows different functions in different connections
- Matches CGO implementation semantics

**Implementation**:
```go
type scalarFuncRegistry struct {
    mu        sync.RWMutex
    functions map[string][]registeredScalarFunc
}

type registeredScalarFunc struct {
    name     string
    config   ScalarFuncConfig
    executor ScalarFuncExecutor
}

// Per-connection registry stored in Conn
type Conn struct {
    // ... existing fields
    scalarFuncs *scalarFuncRegistry
}
```

### Decision 2: Execution Integration

**What**: Hook scalar UDF execution into the query binder

**Why**:
- Must resolve function calls during binding
- Type checking happens at bind time
- Constant folding optimization possible

**Implementation**:
```go
// During query binding, when a function call is encountered:
func (b *Binder) resolveFunction(name string, args []Expr) (BoundExpr, error) {
    // Check if it's a registered scalar UDF
    if udf := b.conn.scalarFuncs.lookup(name, argTypes); udf != nil {
        return b.bindScalarUDF(udf, args)
    }
    // Fall back to built-in function resolution
    return b.resolveBuiltinFunction(name, args)
}
```

### Decision 3: Type Matching and Overloading

**What**: Match input types to find correct function overload

**Why**:
- Support multiple implementations of same function name
- Example: `my_length(VARCHAR)` vs `my_length(LIST)`

**Implementation**:
```go
func (r *scalarFuncRegistry) lookup(name string, argTypes []Type) *registeredScalarFunc {
    funcs := r.functions[name]
    for _, f := range funcs {
        if f.matchesTypes(argTypes) {
            return &f
        }
    }
    return nil
}

func (f *registeredScalarFunc) matchesTypes(argTypes []Type) bool {
    config := f.config

    // Check non-variadic parameters
    for i, expected := range config.InputTypeInfos {
        if i >= len(argTypes) {
            return false
        }
        if !typesCompatible(expected.InternalType(), argTypes[i]) {
            return false
        }
    }

    // Check variadic parameters
    if config.VariadicTypeInfo != nil {
        variadicType := config.VariadicTypeInfo.InternalType()
        for i := len(config.InputTypeInfos); i < len(argTypes); i++ {
            if variadicType != TYPE_ANY && !typesCompatible(variadicType, argTypes[i]) {
                return false
            }
        }
    }

    return true
}
```

### Decision 4: Vectorized Execution

**What**: Execute UDFs row-by-row within DataChunk batches

**Why**:
- User functions are inherently row-oriented
- DataChunk provides batch context for efficiency
- Amortizes function call overhead

**Implementation**:
```go
func executeScalarUDF(
    udf *registeredScalarFunc,
    input *DataChunk,
    output *vector,
) error {
    executor := udf.executor
    size := input.GetSize()
    values := make([]driver.Value, len(input.columns))

    for rowIdx := 0; rowIdx < size; rowIdx++ {
        // Check for NULL handling
        if !udf.config.SpecialNullHandling {
            if hasNullInput(input, rowIdx) {
                output.setNull(rowIdx)
                continue
            }
        }

        // Gather input values
        for colIdx := range values {
            val, _ := input.GetValue(colIdx, rowIdx)
            values[colIdx] = val
        }

        // Execute user function
        result, err := executor.RowExecutor(values)
        if err != nil {
            return err
        }

        // Write result
        if err := output.setFn(output, rowIdx, result); err != nil {
            return err
        }
    }
    return nil
}
```

### Decision 5: Constant Folding Support

**What**: Optional ScalarBinder for compile-time optimization

**Why**:
- Some functions can precompute results for constant arguments
- Reduces runtime overhead for static values

**Implementation**:
```go
type ScalarBinderFn func(ctx context.Context, args []ScalarUDFArg) (context.Context, error)

// During bind phase:
func (b *Binder) bindScalarUDF(udf *registeredScalarFunc, args []Expr) (BoundExpr, error) {
    if udf.executor.ScalarBinder != nil {
        udfArgs := make([]ScalarUDFArg, len(args))
        for i, arg := range args {
            udfArgs[i].Foldable = arg.IsConstant()
            if udfArgs[i].Foldable {
                udfArgs[i].Value = evaluateConstant(arg)
            }
        }
        ctx, err := udf.executor.ScalarBinder(context.Background(), udfArgs)
        if err != nil {
            return nil, err
        }
        // Store context for execution phase
    }
    return &BoundScalarUDF{...}, nil
}
```

## Risks / Trade-offs

### Risk 1: Per-Row Function Call Overhead
**Risk**: Go function calls have higher overhead than native DuckDB functions
**Mitigation**:
- Batch processing via DataChunk reduces call frequency
- Encourage users to process data in batches where possible
- Document performance characteristics
**Acceptable**: 5-10x slower than native functions is acceptable for extensibility

### Risk 2: Type Conversion Overhead
**Risk**: Converting between DuckDB types and Go types adds latency
**Mitigation**:
- Use type-specific fast paths
- Avoid allocations in hot path
- Cache type converters

### Risk 3: Error Handling in User Functions
**Risk**: User function panics could crash the driver
**Mitigation**:
- Wrap user function calls in recover()
- Convert panics to errors
- Document that panics are caught

## Migration Plan

This is a new capability with no migration required.

**Rollout steps**:
1. Implement ScalarFuncConfig and ScalarFuncExecutor types
2. Implement function registry with connection scope
3. Hook into binder for function resolution
4. Implement row-by-row execution
5. Add overloading support via RegisterScalarUDFSet
6. Add constant folding support
7. Write comprehensive tests with various type combinations

**Rollback**: Remove new files; no existing functionality affected.

## Open Questions

1. **Thread-safety**: Should UDFs be callable from multiple goroutines simultaneously?
   - Current design: Single-threaded per connection, matches DuckDB semantics

2. **Error context**: Should we provide row index in error messages?
   - Tentative: Yes, include row index for debugging

3. **Memory limits**: Should we limit memory usage per UDF call?
   - Deferred: Rely on Go runtime limits initially

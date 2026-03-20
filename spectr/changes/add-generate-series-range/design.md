## Implementation Details

### Function Signatures

```
generate_series(start, stop)           -- step defaults to 1
generate_series(start, stop, step)     -- explicit step
range(start, stop)                     -- step defaults to 1
range(start, stop, step)               -- explicit step
```

**Key difference**: `generate_series` includes the stop value (inclusive),
`range` excludes it (exclusive). This matches PostgreSQL's `generate_series`
and Python's `range()` respectively.

### Supported Types

| Start/Stop Type | Step Type | Output Type | Default Step |
|----------------|-----------|-------------|--------------|
| INTEGER | INTEGER | INTEGER | 1 |
| BIGINT | BIGINT | BIGINT | 1 |
| DATE | INTERVAL | DATE | INTERVAL '1 day' |
| TIMESTAMP | INTERVAL | TIMESTAMP | INTERVAL '1 day' |

Type resolution:
1. Start and stop must be the same type (or implicitly castable)
2. For numeric types, step is the same type as start/stop
3. For temporal types (DATE, TIMESTAMP), step must be an INTERVAL
4. If start is INTEGER and stop is BIGINT, promote to BIGINT

### Table Function Registration

Add two new cases in `executeTableFunctionScan` switch
(`internal/executor/table_function_csv.go`):

```go
case "generate_series":
    return e.executeGenerateSeries(ctx, plan, true)  // inclusive=true
case "range":
    return e.executeGenerateSeries(ctx, plan, false) // inclusive=false
```

Both dispatch to the same implementation with an `inclusive` flag.

### Implementation (`internal/executor/table_function_series.go`)

```go
func (e *Executor) executeGenerateSeries(
    ctx *ExecutionContext,
    plan *planner.PhysicalTableFunctionScan,
    inclusive bool,
) (*ExecutionResult, error) {
    // 1. Extract bound expressions from plan.Options
    startExpr := plan.Options["start"].(binder.BoundExpr)
    stopExpr := plan.Options["stop"].(binder.BoundExpr)
    var stepExpr binder.BoundExpr
    if s, ok := plan.Options["step"]; ok {
        stepExpr = s.(binder.BoundExpr)
    }

    // 2. Evaluate expressions to concrete values
    startVal := e.evaluateExpr(ctx, startExpr, nil)
    stopVal := e.evaluateExpr(ctx, stopExpr, nil)

    // 3. Handle NULL: any NULL argument → empty result
    if startVal == nil || stopVal == nil {
        return emptyResult(plan.FunctionName), nil
    }

    // 4. Determine type and apply default step if needed
    // 5. Validate: step != 0, step direction matches start→stop
    // 6. Generate rows into DataChunks (batched)
    // 7. Return result with single column named plan.FunctionName
}
```

#### Row Generation Algorithm

```
For INTEGER/BIGINT:
    current = start
    while (inclusive ? current <= stop : current < stop) for positive step
       or (inclusive ? current >= stop : current > stop) for negative step:
        emit current
        current += step

For DATE/TIMESTAMP:
    current = start
    while (inclusive ? current <= stop : current < stop) for positive interval
       or (inclusive ? current >= stop : current > stop) for negative interval:
        emit current
        current = addInterval(current, step)
```

#### Batching

Rows are generated in DataChunk batches of `storage.StandardVectorSize` (2048)
to avoid materializing the entire series in memory at once. Each batch fills
a DataChunk, appends it to results, and continues until the series is
exhausted.

```go
for !done {
    chunk := storage.NewDataChunk([]dukdb.Type{outputType}, batchSize)
    count := 0
    for count < batchSize && !done {
        chunk.SetValue(0, count, current)
        count++
        current = advance(current, step)
        done = pastEnd(current, stop, step, inclusive)
    }
    chunk.SetCount(count)
    results = append(results, chunk)
}
```

### Edge Cases

| Case | Behavior |
|------|----------|
| start == stop, inclusive | Single row with start value |
| start == stop, exclusive | Empty result (range returns nothing) |
| start > stop, positive step | Empty result |
| start < stop, negative step | Empty result |
| step == 0 | Error: step size cannot be zero |
| Very large range (>10M rows) | Works via batching, no memory issue |
| Integer overflow | Stop generation before overflow; do not wrap around |
| NULL start | Return empty result |
| NULL stop | Return empty result |
| NULL step | Return empty result |
| Type mismatch (e.g., INTEGER start, DATE stop) | Binding error at binder stage |
| Temporal start/stop with non-INTERVAL step | Binding error at binder stage |

### Output Column

The output column name matches the function name:
- `generate_series(1, 5)` → column named `generate_series`
- `range(1, 5)` → column named `range`

Users can alias: `SELECT * FROM generate_series(1, 5) AS t(n)`

### Binder: Dedicated Binding Function

The binder must recognize `generate_series` and `range` as table functions
(not regular scalar functions) when used in FROM clauses. Following the
UNNEST pattern (`bindUnnestTableFunction` in `bind_stmt.go`), add a dedicated
binding function:

```go
func (b *Binder) bindGenerateSeriesTableFunction(
    ref parser.TableRef,
    funcName string, // "generate_series" or "range"
) (*BoundTableRef, error) {
    // 1. Validate argument count: 2 or 3
    // 2. Bind each argument as a BoundExpr via b.bindExpr()
    // 3. Determine output type from start/stop expression types:
    //    - Both INTEGER → output INTEGER
    //    - Either BIGINT → promote to BIGINT
    //    - Both DATE → output DATE, validate step is INTERVAL
    //    - Both TIMESTAMP → output TIMESTAMP, validate step is INTERVAL
    //    - Type mismatch → error
    // 4. Store bound expressions in Options map:
    //    options["start"] = boundStartExpr
    //    options["stop"] = boundStopExpr
    //    options["step"] = boundStepExpr (if 3 args)
    // 5. Create output column definition with resolved type
    // 6. Return BoundTableRef with columns and PhysicalTableFunctionScan info
}
```

Add dispatch in `bindTableFunction()` (near the UNNEST check):
```go
if funcNameLower == "generate_series" || funcNameLower == "range" {
    return b.bindGenerateSeriesTableFunction(ref, funcNameLower)
}
```

Arguments are passed as `BoundExpr` values in the `Options` map (same pattern
as UNNEST which stores `"array_expr"` in Options). The executor evaluates
these at runtime via `e.evaluateExpr()`.

#### PostgreSQL Compatibility

The existing PostgreSQL alias in `internal/postgres/functions/aliases_transform.go`
transforms `generate_series(start, stop)` → `range(start, stop+1)`. With
native `generate_series` support, this transformer should be updated to
pass through to the native `generate_series` function instead of
transforming to `range`. The native implementation handles inclusive
semantics directly.

For the scalar variant (`SELECT generate_series(1, 3)` returning a LIST),
this is deferred to future work. The initial implementation covers only the
table function form (`FROM generate_series(...)`).

## Context

The existing table function infrastructure (`PhysicalTableFunctionScan`,
`executeTableFunctionScan` dispatch, `plan.Options` for bound expressions)
provides everything needed. Arguments are stored as `BoundExpr` entries in
the `Options` map (like UNNEST stores `"array_expr"`), NOT in a separate
`Args` field. This proposal adds a new case to the dispatch switch, a
dedicated binder function, and a new implementation file.

## Goals / Non-Goals

**Goals:**
- `generate_series(start, stop)` and `generate_series(start, stop, step)`
  as table functions
- `range(start, stop)` and `range(start, stop, step)` as table functions
- INTEGER, BIGINT, DATE, TIMESTAMP type support
- INTERVAL step for temporal types
- Correct inclusive/exclusive behavior
- Efficient batched generation

**Non-Goals:**
- Scalar form returning LIST (`SELECT generate_series(1, 3)`) — future work
- TIMESTAMP WITH TIME ZONE support — future work
- Parallel generation — unnecessary for sequential data

## Decisions

- **Single implementation for both functions**: The only difference is the
  inclusive flag, so both share `executeGenerateSeries`. This avoids code
  duplication.

- **Batch generation**: Generate in StandardVectorSize chunks rather than
  materializing all values upfront. This handles very large ranges efficiently.

- **Column naming**: Use function name as default column name. This matches
  DuckDB behavior and allows standard aliasing.

## Risks / Trade-offs

- **Very large ranges**: A query like `generate_series(1, 1000000000)` will
  produce 1B rows. This is correct behavior but may consume significant memory
  if the downstream query materializes all rows. Mitigation: batching limits
  per-batch memory; the query planner can potentially short-circuit with LIMIT.

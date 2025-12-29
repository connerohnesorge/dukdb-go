# Change: Add Aggregate UDF API

## Why

The current dukdb-go implementation has a robust scalar UDF system (`scalar_udf.go`, ~400 lines) but lacks aggregate function support. Aggregate UDFs are essential for:

1. **Custom analytics** - Implementing domain-specific aggregations (median, mode, percentiles)
2. **Data processing pipelines** - Creating specialized reducers for ETL workflows
3. **API parity with duckdb-go** - Matching the reference implementation's aggregate UDF capabilities

**Current State** (from exploration):
- `scalar_udf.go`: Complete scalar UDF with Config, Executor, Registry, clock integration
- `table_udf.go`: Table functions with ChunkTableSource and RowTableSource interfaces
- `type_info.go`: Full TypeInfo system for all 37 DuckDB types
- **Missing**: Aggregate UDF registration, state management, combine/finalize operations

**duckdb-go Reference** (from DuckDB C API):
- `duckdb_create_aggregate_function` - Create aggregate function
- `duckdb_aggregate_function_set_*` - Configure callbacks
- State management callbacks: `init_t`, `update_t`, `combine_t`, `finalize_t`, `destroy_t`
- Bind-time type resolution

## What Changes

### 1. Aggregate Function Types (aggregate_udf.go - NEW)

```go
// AggregateFuncConfig defines aggregate function signature and behavior
type AggregateFuncConfig struct {
    InputTypeInfos      []TypeInfo  // Input parameter types (cannot be empty, each cannot be nil)
    ResultTypeInfo      TypeInfo    // Final result type (required, cannot be nil or TYPE_ANY)
    VariadicTypeInfo    TypeInfo    // Optional variadic parameter type
    Volatile            bool        // If true, skip result caching
    SpecialNullHandling bool        // If true, NULLs passed to Update; if false, NULLs skipped
}
// Note: DISTINCT and ORDER BY are handled by the engine, not the UDF
// Note: IntermediateTypeInfo removed - serialization deferred to future work

// AggregateFuncState is the user-defined state type
type AggregateFuncState interface{}

// State lifecycle functions
type StateInitFn func() AggregateFuncState
type StateDestroyFn func(state AggregateFuncState)

// Core aggregate operations
type UpdateFn func(state AggregateFuncState, args ...driver.Value) error
type CombineFn func(target, source AggregateFuncState) error
type FinalizeFn func(state AggregateFuncState) (driver.Value, error)

// Context-aware versions for clock integration
type UpdateContextFn func(ctx *AggregateFuncContext, state AggregateFuncState, args ...driver.Value) error
type FinalizeContextFn func(ctx *AggregateFuncContext, state AggregateFuncState) (driver.Value, error)

// Complete aggregate executor
type AggregateFuncExecutor struct {
    // State management (Init required, Destroy optional for cleanup)
    Init        StateInitFn       // Create initial state (REQUIRED)
    Destroy     StateDestroyFn    // Cleanup state (optional, called during Finalize cleanup)

    // Core operations (one of Update or UpdateCtx REQUIRED)
    Update      UpdateFn          // Process input value (simple variant)
    UpdateCtx   UpdateContextFn   // Process with context (context-aware variant)

    // Combine partial states (REQUIRED for parallel execution)
    Combine     CombineFn

    // Produce final result (one of Finalize or FinalizeCtx REQUIRED)
    Finalize    FinalizeFn        // Return final value (simple variant)
    FinalizeCtx FinalizeContextFn // Return final value (context-aware variant)
}
// Note: All callbacks wrapped with panic recovery (safeAggregate* functions)
// Note: Serialization deferred to future work (not in DuckDB C API)

// AggregateFunc combines config and executor
type AggregateFunc struct {
    Config   AggregateFuncConfig
    Executor AggregateFuncExecutor
}
```

### 2. Aggregate Function Context (aggregate_udf.go)

```go
// AggregateFuncContext provides runtime context with clock for determinism
// Matches ScalarFuncContext pattern from scalar_udf.go
type AggregateFuncContext struct {
    ctx      context.Context
    clock    quartz.Clock
    groupKey []any  // Current group key for grouped aggregations
}

// NewAggregateFuncContext creates context with nil-clock defaulting (matches scalar UDF)
func NewAggregateFuncContext(ctx context.Context, clock quartz.Clock) *AggregateFuncContext {
    if clock == nil {
        clock = quartz.NewReal()  // Default to real clock if nil (matches scalar UDF)
    }
    return &AggregateFuncContext{
        ctx:   ctx,
        clock: clock,
    }
}

// WithClock returns new context with specified clock (matches ScalarFuncContext)
func (c *AggregateFuncContext) WithClock(clock quartz.Clock) *AggregateFuncContext {
    return &AggregateFuncContext{
        ctx:      c.ctx,
        clock:    clock,
        groupKey: c.groupKey,
    }
}

func (c *AggregateFuncContext) Context() context.Context {
    return c.ctx
}

func (c *AggregateFuncContext) Clock() quartz.Clock {
    return c.clock
}

func (c *AggregateFuncContext) GroupKey() []any {
    return c.groupKey
}

// Now returns current time for deterministic trapping
func (c *AggregateFuncContext) Now() time.Time {
    return c.clock.Now()  // Tag: "aggregate", "context", "now"
}

// checkTimeout checks for deadline exceeded using clock.Until() (matches scalar UDF pattern)
func (c *AggregateFuncContext) checkTimeout() error {
    deadline, ok := c.ctx.Deadline()
    if ok && c.clock.Until(deadline) <= 0 {
        return context.DeadlineExceeded
    }
    select {
    case <-c.ctx.Done():
        return c.ctx.Err()
    default:
        return nil
    }
}
```

### 3. Aggregate Function Registry (aggregate_udf.go)

```go
type registeredAggregateFunc struct {
    name     string
    config   AggregateFuncConfig
    executor AggregateFuncExecutor
}

type aggregateFuncRegistry struct {
    mu        sync.RWMutex
    functions map[string][]*registeredAggregateFunc  // name -> overloads
}

func newAggregateFuncRegistry() *aggregateFuncRegistry {
    return &aggregateFuncRegistry{
        functions: make(map[string][]*registeredAggregateFunc),
    }
}

func (r *aggregateFuncRegistry) register(name string, f AggregateFunc) error {
    // Validation matching scalar UDF error message patterns
    if name == "" {
        return errors.New("aggregate UDF name cannot be empty")
    }
    if f.Executor.Init == nil {
        return errors.New("aggregate UDF must have Init callback")
    }
    if f.Executor.Update == nil && f.Executor.UpdateCtx == nil {
        return errors.New("aggregate UDF must have either Update or UpdateCtx callback")
    }
    if f.Executor.Finalize == nil && f.Executor.FinalizeCtx == nil {
        return errors.New("aggregate UDF must have either Finalize or FinalizeCtx callback")
    }
    if f.Executor.Combine == nil {
        return errors.New("aggregate UDF must have Combine callback for parallel execution")
    }
    // Type validation (matches scalar UDF pattern)
    if f.Config.ResultTypeInfo == nil {
        return errors.New("aggregate UDF result type cannot be nil")
    }
    if f.Config.ResultTypeInfo.Type() == TYPE_ANY {
        return errors.New("aggregate UDF result type cannot be TYPE_ANY")
    }
    if len(f.Config.InputTypeInfos) == 0 {
        return errors.New("aggregate UDF must have at least one input type")
    }
    for i, info := range f.Config.InputTypeInfos {
        if info == nil {
            return fmt.Errorf("aggregate UDF input type at index %d cannot be nil", i)
        }
    }

    r.mu.Lock()
    defer r.mu.Unlock()

    reg := &registeredAggregateFunc{
        name:     name,
        config:   f.Config,
        executor: f.Executor,
    }
    r.functions[name] = append(r.functions[name], reg)
    return nil
}

func (r *aggregateFuncRegistry) lookup(name string, argTypes []Type) *registeredAggregateFunc {
    r.mu.RLock()
    defer r.mu.RUnlock()

    overloads, ok := r.functions[name]
    if !ok {
        return nil
    }

    for _, fn := range overloads {
        // matchesTypes reused from scalar_udf.go - handles exact match and variadic
        if matchesTypes(fn.config.InputTypeInfos, argTypes, fn.config.VariadicTypeInfo) {
            return fn
        }
    }
    return nil
}

// LookupAggregateUDF implements AggregateUDFResolver interface for binder integration
func (r *aggregateFuncRegistry) LookupAggregateUDF(name string, argTypes []Type) (udfInfo any, resultType Type, found bool) {
    fn := r.lookup(name, argTypes)
    if fn == nil {
        return nil, 0, false
    }
    return fn, fn.config.ResultTypeInfo.Type(), true
}

// IsVolatile returns true if function is marked volatile (skip caching)
func (r *aggregateFuncRegistry) IsVolatile(udfInfo any) bool {
    if fn, ok := udfInfo.(*registeredAggregateFunc); ok {
        return fn.config.Volatile
    }
    return false
}
```

### 4. Aggregate Execution Engine (aggregate_udf.go)

```go
// GroupState manages state for each group in GROUP BY
type GroupState struct {
    key   []any
    state AggregateFuncState
}

// AggregateExecutionState manages all groups
// Clock is injected from Conn → Engine → ExecutionState for deterministic testing
type AggregateExecutionState struct {
    udf        *registeredAggregateFunc
    groups     map[string]*GroupState  // Serialized key -> state
    noGroupKey *GroupState             // For ungrouped aggregations
    clock      quartz.Clock            // Injected from Conn.clock via query execution
    ctx        context.Context
}

// NewAggregateExecutionState creates execution state with injected clock
// Clock flow: Conn.clock → Engine.execute() → NewAggregateExecutionState(clock)
func NewAggregateExecutionState(udf *registeredAggregateFunc, ctx context.Context, clock quartz.Clock) *AggregateExecutionState {
    if clock == nil {
        clock = quartz.NewReal()  // Default to real clock (matches scalar UDF)
    }
    return &AggregateExecutionState{
        udf:    udf,
        groups: make(map[string]*GroupState),
        clock:  clock,
        ctx:    ctx,
    }
}

// ProcessChunk updates state for all rows in a DataChunk
func (s *AggregateExecutionState) ProcessChunk(chunk *DataChunk, groupCols []int, valueCols []int) error {
    _ = s.clock.Now()  // Tag: "aggregate", "process", "chunk_start"

    for rowIdx := 0; rowIdx < chunk.GetSize(); rowIdx++ {
        // Extract group key
        groupKey := s.extractGroupKey(chunk, rowIdx, groupCols)
        state := s.getOrCreateState(groupKey)

        // Extract values
        values := s.extractValues(chunk, rowIdx, valueCols)

        // Handle NULLs
        if !s.udf.config.SpecialNullHandling && s.hasNull(values) {
            continue  // Skip NULL values by default
        }

        // Update state
        if err := s.updateState(state, values); err != nil {
            return err
        }
    }

    _ = s.clock.Now()  // Tag: "aggregate", "process", "chunk_end"
    return nil
}

func (s *AggregateExecutionState) updateState(gs *GroupState, values []driver.Value) error {
    if s.udf.executor.UpdateCtx != nil {
        ctx := &AggregateFuncContext{
            ctx:      s.ctx,
            clock:    s.clock,
            groupKey: gs.key,
        }
        return safeAggregateUpdate(s.udf.executor.UpdateCtx, ctx, gs.state, values)
    }
    return safeAggregateUpdateSimple(s.udf.executor.Update, gs.state, values)
}

// CombineWith merges another execution state (for parallel aggregation)
func (s *AggregateExecutionState) CombineWith(other *AggregateExecutionState) error {
    _ = s.clock.Now()  // Tag: "aggregate", "combine", "start"

    for key, otherGs := range other.groups {
        if gs, ok := s.groups[key]; ok {
            // Use safe wrapper for panic recovery
            if err := safeAggregateCombine(s.udf.executor.Combine, gs.state, otherGs.state); err != nil {
                return err
            }
        } else {
            s.groups[key] = otherGs
        }
    }

    _ = s.clock.Now()  // Tag: "aggregate", "combine", "end"
    return nil
}

// Finalize produces final results for all groups
func (s *AggregateExecutionState) Finalize() ([]GroupResult, error) {
    _ = s.clock.Now()  // Tag: "aggregate", "finalize", "start"

    var results []GroupResult
    for _, gs := range s.groups {
        result, err := s.finalizeGroup(gs)
        if err != nil {
            return nil, err
        }
        results = append(results, GroupResult{Key: gs.key, Value: result})
    }

    _ = s.clock.Now()  // Tag: "aggregate", "finalize", "end"
    return results, nil
}

// finalizeGroup produces result for a single group (with panic recovery)
func (s *AggregateExecutionState) finalizeGroup(gs *GroupState) (driver.Value, error) {
    if s.udf.executor.FinalizeCtx != nil {
        ctx := &AggregateFuncContext{
            ctx:      s.ctx,
            clock:    s.clock,
            groupKey: gs.key,
        }
        return safeAggregateFinalize(s.udf.executor.FinalizeCtx, ctx, gs.state)
    }
    return safeAggregateFinalizeSimple(s.udf.executor.Finalize, gs.state)
}

// Cleanup destroys all group states (called after results are collected)
func (s *AggregateExecutionState) Cleanup() {
    _ = s.clock.Now()  // Tag: "aggregate", "cleanup", "start"

    if s.udf.executor.Destroy != nil {
        for _, gs := range s.groups {
            safeAggregateDestroy(s.udf.executor.Destroy, gs.state)
        }
    }
    s.groups = nil  // Release map for GC

    _ = s.clock.Now()  // Tag: "aggregate", "cleanup", "end"
}

// GroupResult holds key and final value for a group
type GroupResult struct {
    Key   []any
    Value driver.Value
}

// Helper methods for group key management
func (s *AggregateExecutionState) extractGroupKey(chunk *DataChunk, rowIdx int, groupCols []int) []any {
    key := make([]any, len(groupCols))
    for i, col := range groupCols {
        key[i] = chunk.GetValue(col, rowIdx)
    }
    return key
}

func (s *AggregateExecutionState) getOrCreateState(groupKey []any) *GroupState {
    keyStr := serializeGroupKey(groupKey)
    if gs, ok := s.groups[keyStr]; ok {
        return gs
    }
    gs := &GroupState{
        key:   groupKey,
        state: s.udf.executor.Init(),
    }
    s.groups[keyStr] = gs
    return gs
}

func serializeGroupKey(key []any) string {
    // Simple serialization for map lookup (matches design doc approach)
    var buf bytes.Buffer
    for i, v := range key {
        if i > 0 {
            buf.WriteByte('|')
        }
        fmt.Fprintf(&buf, "%v", v)
    }
    return buf.String()
}

func (s *AggregateExecutionState) extractValues(chunk *DataChunk, rowIdx int, valueCols []int) []driver.Value {
    values := make([]driver.Value, len(valueCols))
    for i, col := range valueCols {
        values[i] = chunk.GetValue(col, rowIdx)
    }
    return values
}

func (s *AggregateExecutionState) hasNull(values []driver.Value) bool {
    for _, v := range values {
        if v == nil {
            return true
        }
    }
    return false
}
```

### 5. Safe Execution Wrappers (aggregate_udf.go)

All user callbacks are wrapped with panic recovery (matching scalar UDF safeExecute pattern).

```go
// Update wrappers
func safeAggregateUpdate(fn UpdateContextFn, ctx *AggregateFuncContext, state AggregateFuncState, values []driver.Value) (err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in aggregate UDF Update: %v", r)
        }
    }()
    return fn(ctx, state, values...)
}

func safeAggregateUpdateSimple(fn UpdateFn, state AggregateFuncState, values []driver.Value) (err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in aggregate UDF Update: %v", r)
        }
    }()
    return fn(state, values...)
}

// Combine wrapper (CRITICAL - was missing from original proposal)
func safeAggregateCombine(fn CombineFn, target, source AggregateFuncState) (err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in aggregate UDF Combine: %v", r)
        }
    }()
    return fn(target, source)
}

// Finalize wrappers
func safeAggregateFinalize(fn FinalizeContextFn, ctx *AggregateFuncContext, state AggregateFuncState) (result driver.Value, err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in aggregate UDF Finalize: %v", r)
        }
    }()
    return fn(ctx, state)
}

func safeAggregateFinalizeSimple(fn FinalizeFn, state AggregateFuncState) (result driver.Value, err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in aggregate UDF Finalize: %v", r)
        }
    }()
    return fn(state)
}

// Destroy wrapper (called during Cleanup)
func safeAggregateDestroy(fn StateDestroyFn, state AggregateFuncState) {
    defer func() {
        if r := recover(); r != nil {
            // Log but don't propagate - cleanup should not fail
            // In production: log.Printf("panic in aggregate UDF Destroy: %v", r)
        }
    }()
    fn(state)
}
```

### 6. Public Registration API (aggregate_udf.go)

```go
// RegisterAggregateUDF registers an aggregate function on a connection
// The Conn's clock will be used for deterministic execution in tests
func RegisterAggregateUDF(c *sql.Conn, name string, f AggregateFunc) error {
    return c.Raw(func(driverConn any) error {
        conn, ok := driverConn.(*Conn)
        if !ok {
            return fmt.Errorf("invalid connection type: %T", driverConn)
        }

        if conn.aggregateFuncs == nil {
            conn.aggregateFuncs = newAggregateFuncRegistry()
        }

        return conn.aggregateFuncs.register(name, f)
    })
}

// RegisterAggregateUDFSet registers multiple overloads of an aggregate function
func RegisterAggregateUDFSet(c *sql.Conn, name string, functions ...AggregateFunc) error {
    for _, f := range functions {
        if err := RegisterAggregateUDF(c, name, f); err != nil {
            return fmt.Errorf("registering aggregate UDF %s: %w", name, err)
        }
    }
    return nil
}
```

### 6.1 Conn Integration (conn.go - MODIFIED)

```go
// Add to Conn struct
type Conn struct {
    // ... existing fields
    clock          quartz.Clock           // Injected clock for deterministic testing
    aggregateFuncs *aggregateFuncRegistry // Aggregate UDF registry
}

// WithClock option for Engine/Conn (enables deterministic testing)
func WithClock(clock quartz.Clock) Option {
    return func(c *config) {
        c.clock = clock
    }
}

// Clock flow during query execution:
// 1. Conn.clock is set via WithClock option during connection creation
// 2. When aggregate UDF is called, engine passes conn.clock to execution:
//    exec := NewAggregateExecutionState(udf, ctx, conn.clock)
// 3. All clock.Now() calls in execution use the injected clock
```

### 7. Binder Integration (internal/binder/binder.go - MODIFIED)

```go
// Add to Binder struct
type Binder struct {
    // ... existing fields
    aggregateFuncs AggregateUDFResolver
}

// AggregateUDFResolver interface for aggregate UDF lookup
type AggregateUDFResolver interface {
    LookupAggregateUDF(name string, argTypes []dukdb.Type) (
        udfInfo any, resultType dukdb.Type, found bool)
    IsVolatile(udfInfo any) bool
}

// Modify bindAggregateCall to check UDFs
func (b *Binder) bindAggregateCall(call *parser.FunctionCall) (BoundExpr, error) {
    // 1. Check built-in aggregates (SUM, COUNT, AVG, etc.)
    if builtin := b.lookupBuiltinAggregate(call.Name); builtin != nil {
        return b.bindBuiltinAggregate(builtin, call)
    }

    // 2. Check user-defined aggregates
    if b.aggregateFuncs != nil {
        argTypes := b.extractArgTypes(call.Args)
        if udfInfo, resultType, found := b.aggregateFuncs.LookupAggregateUDF(call.Name, argTypes); found {
            return b.bindUDFAggregate(udfInfo, resultType, call)
        }
    }

    return nil, fmt.Errorf("unknown aggregate function: %s", call.Name)
}
```

### 8. Deterministic Testing Support

#### 8.1 Clock Tags Documentation

All clock.Now() calls are tagged for deterministic trapping:

| Operation | Tag Prefix | Tags |
|-----------|------------|------|
| ProcessChunk | "aggregate", "process" | "chunk_start", "chunk_end" |
| CombineWith | "aggregate", "combine" | "start", "end" |
| Finalize | "aggregate", "finalize" | "start", "end" |
| Cleanup | "aggregate", "cleanup" | "start", "end" |
| Context.Now | "aggregate", "context" | "now" |

#### 8.2 Basic Test with Mock Clock

```go
func TestAggregateUDFDeterministic(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

    // Define a custom running average aggregate
    runningAvg := AggregateFunc{
        Config: AggregateFuncConfig{
            InputTypeInfos: []TypeInfo{NewTypeInfo(TYPE_DOUBLE)},
            ResultTypeInfo: NewTypeInfo(TYPE_DOUBLE),
        },
        Executor: AggregateFuncExecutor{
            Init: func() AggregateFuncState {
                return &struct{ sum, count float64 }{0, 0}
            },
            Update: func(state AggregateFuncState, args ...driver.Value) error {
                s := state.(*struct{ sum, count float64 })
                if v, ok := args[0].(float64); ok {
                    s.sum += v
                    s.count++
                }
                return nil
            },
            Combine: func(target, source AggregateFuncState) error {
                t := target.(*struct{ sum, count float64 })
                s := source.(*struct{ sum, count float64 })
                t.sum += s.sum
                t.count += s.count
                return nil
            },
            Finalize: func(state AggregateFuncState) (driver.Value, error) {
                s := state.(*struct{ sum, count float64 })
                if s.count == 0 {
                    return nil, nil
                }
                return s.sum / s.count, nil
            },
        },
    }

    // Register and test with clock injection
    db := openTestDB(t, WithClock(mClock))
    conn, err := db.Conn(context.Background())
    require.NoError(t, err)
    defer conn.Close()

    err = RegisterAggregateUDF(conn, "running_avg", runningAvg)
    require.NoError(t, err)

    // Create test data
    _, err = db.Exec("CREATE TABLE data (value DOUBLE)")
    require.NoError(t, err)
    _, err = db.Exec("INSERT INTO data VALUES (1.0), (2.0), (3.0), (4.0), (5.0)")
    require.NoError(t, err)

    // Execute and verify
    var avg float64
    err = db.QueryRow("SELECT running_avg(value) FROM data").Scan(&avg)
    require.NoError(t, err)
    assert.Equal(t, 3.0, avg)
}
```

#### 8.3 Trap Example for Verifying Clock Usage

```go
func TestAggregateUDFClockTrapping(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

    // Set up trap to verify ProcessChunk clock calls
    trap := mClock.Trap().Now()
    defer trap.Close()

    db := openTestDB(t, WithClock(mClock))
    conn, _ := db.Conn(context.Background())

    // Register simple count aggregate
    countAgg := AggregateFunc{
        Config: AggregateFuncConfig{
            InputTypeInfos: []TypeInfo{NewTypeInfo(TYPE_INTEGER)},
            ResultTypeInfo: NewTypeInfo(TYPE_BIGINT),
        },
        Executor: AggregateFuncExecutor{
            Init:     func() AggregateFuncState { return new(int64) },
            Update:   func(s AggregateFuncState, args ...driver.Value) error { *s.(*int64)++; return nil },
            Combine:  func(t, s AggregateFuncState) error { *t.(*int64) += *s.(*int64); return nil },
            Finalize: func(s AggregateFuncState) (driver.Value, error) { return *s.(*int64), nil },
        },
    }
    RegisterAggregateUDF(conn, "my_count", countAgg)

    // Run query in background
    ctx := context.Background()
    go func() {
        db.QueryRowContext(ctx, "SELECT my_count(1) FROM generate_series(1, 10)")
    }()

    // Verify clock is called at ProcessChunk start
    call := trap.Wait(ctx)
    // call.Tags would contain ["aggregate", "process", "chunk_start"]
    call.Release()

    // Verify clock is called at ProcessChunk end
    call = trap.Wait(ctx)
    // call.Tags would contain ["aggregate", "process", "chunk_end"]
    call.Release()
}
```

#### 8.4 Timeout Test with clock.Until()

```go
func TestAggregateUDFTimeout(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

    db := openTestDB(t, WithClock(mClock))
    conn, _ := db.Conn(context.Background())

    // Register slow aggregate that checks timeout
    slowAgg := AggregateFunc{
        Config: AggregateFuncConfig{
            InputTypeInfos: []TypeInfo{NewTypeInfo(TYPE_INTEGER)},
            ResultTypeInfo: NewTypeInfo(TYPE_INTEGER),
        },
        Executor: AggregateFuncExecutor{
            Init: func() AggregateFuncState { return new(int) },
            UpdateCtx: func(ctx *AggregateFuncContext, s AggregateFuncState, args ...driver.Value) error {
                // Check timeout using clock.Until (deterministic)
                if err := ctx.checkTimeout(); err != nil {
                    return err
                }
                return nil
            },
            Combine:  func(t, s AggregateFuncState) error { return nil },
            Finalize: func(s AggregateFuncState) (driver.Value, error) { return 0, nil },
        },
    }
    RegisterAggregateUDF(conn, "slow_agg", slowAgg)

    // Set deadline that will be exceeded
    ctx, cancel := context.WithDeadline(context.Background(), mClock.Now().Add(100*time.Millisecond))
    defer cancel()

    // Advance clock past deadline
    mClock.Advance(200 * time.Millisecond)

    // Query should fail with deadline exceeded
    _, err := db.QueryContext(ctx, "SELECT slow_agg(1)")
    assert.ErrorIs(t, err, context.DeadlineExceeded)
}
```

## Impact

- **Affected specs**: execution-engine (MODIFIED for aggregate binding)
- **Affected code**:
  - NEW: `aggregate_udf.go` (~700 lines)
  - NEW: `aggregate_udf_test.go` (~1000 lines)
  - MODIFIED: `conn.go` (~25 lines - add aggregateFuncs field, clock field)
  - MODIFIED: `internal/binder/binder.go` (~60 lines - add AggregateUDFResolver)

- **Dependencies**:
  - `scalar_udf.go` - Reuse `matchesTypes()` function, NULL handling patterns
  - `type_info.go` - TypeInfo for signature definition
  - `quartz.Clock` - Deterministic timing via clock injection

## Breaking Changes

None. All changes are additive. Existing scalar UDF and table UDF APIs unchanged.

## Deterministic Testing Requirements

All aggregate operations use quartz clock injection with tagged calls:

| Operation | Clock Call Sites | Tags |
|-----------|------------------|------|
| ProcessChunk | Start, End | "aggregate", "process", "chunk_start/end" |
| CombineWith | Start, End | "aggregate", "combine", "start/end" |
| Finalize | Start, End | "aggregate", "finalize", "start/end" |
| Cleanup | Start, End | "aggregate", "cleanup", "start/end" |
| Context.Now | Each call | "aggregate", "context", "now" |

**Clock Flow**:
1. `Conn` receives clock via `WithClock(clock)` option
2. Engine passes `conn.clock` to `NewAggregateExecutionState(udf, ctx, clock)`
3. Execution state creates `AggregateFuncContext` with clock for context-aware callbacks
4. All `clock.Now()` and `clock.Until()` calls use injected clock

**Timeout Checking**: Uses `clock.Until(deadline)` pattern (matching scalar UDF) for deterministic deadline checking.

## Notes on API Choices

1. **Serialization deferred**: `Serialize`/`Deserialize` callbacks removed - not in DuckDB C API
2. **DISTINCT/ORDER BY**: Handled by engine, not UDF (config flags removed)
3. **State cleanup**: `Cleanup()` method calls `Destroy` callback for all group states
4. **Panic recovery**: All callbacks wrapped with `safeAggregate*` functions
5. **Type matching**: Reuses `matchesTypes()` from `scalar_udf.go`

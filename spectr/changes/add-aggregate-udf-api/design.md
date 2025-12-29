# Design: Aggregate UDF API

## Context

The dukdb-go project has complete scalar UDF support (`scalar_udf.go`) but lacks aggregate function capability. This design adds aggregate UDFs following the same patterns for consistency.

**Stakeholders**:
- Users needing custom aggregation (median, percentile, custom statistics)
- Data processing applications requiring specialized reducers
- Users migrating from duckdb-go expecting aggregate UDF support

**Constraints**:
- Must remain pure Go (no CGO)
- Must integrate with existing type system (TypeInfo)
- Must support quartz clock for deterministic testing
- Must match scalar UDF patterns for API consistency
- Must support parallel aggregation via Combine

## Goals / Non-Goals

**Goals**:
1. Register custom aggregate functions by name
2. Support Init/Update/Combine/Finalize lifecycle
3. Support grouped aggregations (GROUP BY)
4. Full quartz clock integration for deterministic testing
5. NULL handling matching scalar UDF patterns
6. Overloading by parameter types

**Non-Goals**:
1. Window function support (separate proposal)
2. Distributed aggregation across nodes (state serialization is optional)
3. DISTINCT optimization (handled by engine, not UDF)
4. ORDER BY within groups (future enhancement)

## Decisions

### Decision 1: State Management Model

**Options**:
A. Interface-based state - State must implement interface
B. Generic state - `any` type with reflection
C. Type-parameterized - Go generics for type safety
D. Pointer-based - User manages memory directly

**Choice**: B - Generic state (`any` type)

**Rationale**:
- Matches scalar UDF pattern (uses `driver.Value`)
- Maximum flexibility for user implementations
- No interface implementation burden
- Works with all Go types

```go
type AggregateFuncState interface{}  // Actually just `any`

type StateInitFn func() AggregateFuncState

// Example usage:
Init: func() AggregateFuncState {
    return &MyState{sum: 0, count: 0}
}
```

### Decision 2: Callback Organization

**Options**:
A. Single interface with all methods - Forces implementation of unused methods
B. Separate function types - Maximum flexibility
C. Combined config + executor - Matches scalar UDF pattern
D. Builder pattern - Fluent API

**Choice**: C - Combined config + executor (matches scalar UDF)

**Rationale**:
- Consistent with `ScalarFuncConfig` + `ScalarFuncExecutor` pattern
- Clear separation of signature (config) from behavior (executor)
- Familiar to existing scalar UDF users

```go
type AggregateFunc struct {
    Config   AggregateFuncConfig   // What: types, volatility
    Executor AggregateFuncExecutor // How: callbacks
}
```

### Decision 3: Context-Aware vs Simple Callbacks

**Options**:
A. All callbacks take context - Consistent but verbose
B. Simple callbacks only - Simpler but no timeout/clock
C. Both variants - User choice (matches scalar UDF)

**Choice**: C - Both variants available

**Rationale**:
- Simple callbacks for trivial aggregates (sum, count)
- Context callbacks for time-aware operations
- Matches `RowExecutorFn` vs `RowContextExecutorFn` pattern

```go
type AggregateFuncExecutor struct {
    // Simple variant
    Update   UpdateFn
    Finalize FinalizeFn

    // Context-aware variant
    UpdateCtx   UpdateContextFn
    FinalizeCtx FinalizeContextFn
}
```

### Decision 4: Combine Requirement

**Options**:
A. Optional Combine - Single-threaded only without it
B. Required Combine - Always parallel-ready
C. Combine with fallback - Default to merge states

**Choice**: B - Required Combine

**Rationale**:
- DuckDB always executes aggregates in parallel when possible
- Combine is essential for correctness in parallel execution
- Forces users to think about aggregation semantics
- Can always implement trivial Combine (copy source to target)

```go
// Required in executor
type CombineFn func(target, source AggregateFuncState) error

// Validation at registration
if f.Executor.Combine == nil {
    return errors.New("aggregate function must have Combine callback")
}
```

### Decision 5: NULL Handling Strategy

**Options**:
A. Always skip NULLs - Simplest, matches SQL standard
B. Always pass NULLs - User handles all cases
C. Configurable via flag - Matches scalar UDF

**Choice**: C - Configurable via `SpecialNullHandling` flag

**Rationale**:
- Matches `ScalarFuncConfig.SpecialNullHandling` pattern
- Default (false): Skip rows with NULL values
- Special (true): Pass NULLs to Update for custom handling
- Consistent API across UDF types

```go
type AggregateFuncConfig struct {
    SpecialNullHandling bool  // If true, NULLs passed to Update
}

// In execution:
if !config.SpecialNullHandling && hasNull(values) {
    continue  // Skip this row
}
```

### Decision 6: Group State Management

**Options**:
A. Map by serialized key - Simple, works with any key type
B. Hash map with custom hasher - More efficient
C. Tree-based - Ordered iteration
D. External state management - User provides storage

**Choice**: A - Map by serialized key

**Rationale**:
- Simple implementation using `map[string]*GroupState`
- Group key serialized to string for map lookup
- Works with any combination of group columns
- No custom hash function needed

```go
type AggregateExecutionState struct {
    groups map[string]*GroupState  // Serialized key -> state
}

func (s *AggregateExecutionState) getOrCreateState(key []any) *GroupState {
    keyStr := serializeGroupKey(key)
    if gs, ok := s.groups[keyStr]; ok {
        return gs
    }
    gs := &GroupState{
        key:   key,
        state: s.udf.executor.Init(),
    }
    s.groups[keyStr] = gs
    return gs
}
```

### Decision 7: Clock Integration Points

**Options**:
A. Clock at execution state level - Fewer calls
B. Clock at every operation - Maximum observability
C. Tagged clock calls for selective trapping

**Choice**: C - Tagged clock calls for selective trapping

**Rationale**:
- Matches scalar UDF pattern
- Allows tests to trap specific operations
- Tags identify operation type for debugging

```go
// ProcessChunk
_ = s.clock.Now()  // Tag: "aggregate", "process", "chunk_start"
// ... process rows ...
_ = s.clock.Now()  // Tag: "aggregate", "process", "chunk_end"

// CombineWith
_ = s.clock.Now()  // Tag: "aggregate", "combine", "start"
// ... combine states ...
_ = s.clock.Now()  // Tag: "aggregate", "combine", "end"

// Finalize
_ = s.clock.Now()  // Tag: "aggregate", "finalize", "start"
// ... produce results ...
_ = s.clock.Now()  // Tag: "aggregate", "finalize", "end"
```

### Decision 8: Panic Recovery

**Options**:
A. No recovery - Let panics propagate
B. Recover and return error - Safe execution
C. Recover with state cleanup - Full safety

**Choice**: B - Recover and return error (matches scalar UDF)

**Rationale**:
- Prevents user bugs from crashing the database
- Converts panics to errors for graceful handling
- Matches `safeExecute` pattern from scalar UDF

```go
func safeAggregateUpdate(fn UpdateContextFn, ctx *AggregateFuncContext,
                         state AggregateFuncState, values []driver.Value) (err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in aggregate Update: %v", r)
        }
    }()
    return fn(ctx, state, values...)
}
```

### Decision 9: Serialization for Distributed Aggregation

**Options**:
A. Required serialization - Always distributed-ready
B. Optional serialization - Only for distributed scenarios
C. Deferred - Not in DuckDB C API, implement later
D. Automatic serialization via reflection - Magic but fragile

**Choice**: C - Deferred (not in DuckDB C API)

**Rationale**:
- DuckDB C API does NOT include serialize/deserialize callbacks
- Adding features not in C API creates maintenance burden
- Can be added as pure-Go extension in future work
- Keeps initial implementation simpler and aligned with reference

```go
// NOT included in initial implementation
// Future: If distributed aggregation needed, add:
// type StateSerializeFn func(state AggregateFuncState) ([]byte, error)
// type StateDeserializeFn func(data []byte) (AggregateFuncState, error)
```

### Decision 10: Registry Thread Safety

**Options**:
A. No locking - Single-threaded registration only
B. RWMutex - Concurrent reads, exclusive writes
C. Sync.Map - Lock-free reads

**Choice**: B - RWMutex (matches scalar UDF)

**Rationale**:
- Consistent with `scalarFuncRegistry` pattern
- Registration is rare, lookup is frequent
- RWMutex allows concurrent lookups

```go
type aggregateFuncRegistry struct {
    mu        sync.RWMutex
    functions map[string][]*registeredAggregateFunc
}

func (r *aggregateFuncRegistry) lookup(name string, argTypes []Type) *registeredAggregateFunc {
    r.mu.RLock()
    defer r.mu.RUnlock()
    // ... lookup logic
}
```

### Decision 11: Clock Injection Flow

**Options**:
A. Clock only in AggregateFuncContext - Minimal changes
B. Clock in ExecutionState, passed to Context - Full tracing
C. Clock at connection level, propagated through execution

**Choice**: C - Clock at connection level, propagated through execution

**Rationale**:
- Matches scalar UDF pattern where clock is set via WithClock option
- Enables deterministic testing of entire query execution
- Clock flows: `Conn.clock` → `Engine.execute()` → `AggregateExecutionState.clock` → `AggregateFuncContext.clock`
- All clock.Now() and clock.Until() calls use injected clock

```go
// Clock injection flow:
// 1. Test sets up mock clock
mClock := quartz.NewMock(t)

// 2. Clock passed to connection via option
db := openTestDB(t, WithClock(mClock))

// 3. Engine receives clock from Conn
conn.clock // set from WithClock option

// 4. Execution state receives clock
exec := NewAggregateExecutionState(udf, ctx, conn.clock)

// 5. Context receives clock for callbacks
ctx := &AggregateFuncContext{clock: exec.clock}

// 6. checkTimeout uses clock.Until for deterministic deadline
func (c *AggregateFuncContext) checkTimeout() error {
    deadline, ok := c.ctx.Deadline()
    if ok && c.clock.Until(deadline) <= 0 {
        return context.DeadlineExceeded
    }
    // ...
}
```

### Decision 12: Complete Panic Recovery Coverage

**Options**:
A. Recover only Update/Finalize - Minimum coverage
B. Recover all callbacks including Combine and Destroy
C. No recovery - Let panics propagate

**Choice**: B - Recover all callbacks including Combine and Destroy

**Rationale**:
- User code can panic in any callback
- Combine is called during parallel merge - must not crash
- Destroy is called during cleanup - must complete
- Consistent error handling across all callbacks

```go
// All user callbacks wrapped:
safeAggregateUpdate(fn, ctx, state, values)
safeAggregateUpdateSimple(fn, state, values)
safeAggregateCombine(fn, target, source)  // Added
safeAggregateFinalize(fn, ctx, state)
safeAggregateFinalizeSimple(fn, state)
safeAggregateDestroy(fn, state)  // Added - logs but doesn't propagate
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| State memory growth | Medium | Document cleanup responsibility |
| Combine semantics bugs | High | Clear documentation, examples |
| NULL handling confusion | Low | Match scalar UDF behavior |
| Parallel execution races | High | State isolation per group |
| Type mismatch errors | Medium | Validate at registration |

## Migration Plan

### Phase 1: Core Types (Day 1)
1. Define config and executor types
2. Define state lifecycle callbacks
3. Add context type with clock

### Phase 2: Registry (Day 2)
1. Implement aggregateFuncRegistry
2. Add lookup with overloading
3. Add validation at registration

### Phase 3: Execution (Days 3-4)
1. Implement AggregateExecutionState
2. Add ProcessChunk with NULL handling
3. Add CombineWith for parallel
4. Add Finalize for results

### Phase 4: Integration (Days 5-6)
1. Add aggregateFuncs to Conn
2. Add AggregateUDFResolver to binder
3. Connect to query execution

### Phase 5: Testing (Days 7-8)
1. Unit tests for all callbacks
2. Deterministic tests with mock clock
3. NULL handling tests
4. Parallel aggregation tests

## Open Questions (Resolved)

1. **Window function syntax?**
   - Answer: Separate proposal, different execution model

2. **DISTINCT handling?**
   - Answer: Engine handles DISTINCT completely
   - DistinctAware/OrderAware config flags removed - engine responsibility

3. **Memory limits for state?**
   - Answer: Not enforced, user responsibility
   - Cleanup() method added to destroy states after Finalize
   - Future: Add optional memory callback

4. **Error recovery in Combine?**
   - Answer: Return error via safeAggregateCombine wrapper
   - Panic recovery converts to error, query fails gracefully

5. **State initialization per group?**
   - Answer: Init called lazily in getOrCreateState
   - One state per group key via serialized key map

6. **Clock flow for testing?**
   - Answer: Conn.clock → Engine → ExecutionState → Context
   - WithClock option sets clock at connection level
   - All clock.Now() and clock.Until() use injected clock

7. **Serialization for distributed?**
   - Answer: Deferred to future work
   - Not in DuckDB C API, avoids scope creep

# Tasks: Add Aggregate UDF API

## Phase 1: Core Types and Registry

- [ ] **1.1** Create `aggregate_udf.go` with core type definitions
  - AggregateFuncConfig struct with InputTypeInfos, ResultTypeInfo, SpecialNullHandling
  - AggregateFuncState type alias (any)
  - StateInitFn, StateDestroyFn function types
  - UpdateFn, CombineFn, FinalizeFn function types
  - UpdateContextFn, FinalizeContextFn context-aware variants
  - StateSerializeFn, StateDeserializeFn for distributed (optional)
  - AggregateFuncExecutor combining all callbacks
  - AggregateFunc combining Config and Executor

- [ ] **1.2** Implement AggregateFuncContext
  - Context field with context.Context
  - Clock field with quartz.Clock
  - GroupKey field for current group
  - Context() method returning context.Context
  - Clock() method returning quartz.Clock
  - Now() method with tagged clock call
  - checkTimeout() method for deadline checking

- [ ] **1.3** Implement aggregateFuncRegistry
  - registeredAggregateFunc struct (name, config, executor)
  - aggregateFuncRegistry with sync.RWMutex
  - functions map[string][]*registeredAggregateFunc
  - newAggregateFuncRegistry() constructor
  - register(name, f) with validation
  - lookup(name, argTypes) with type matching

- [ ] **1.4** Add registration validation
  - Reject empty function name
  - Require Init callback
  - Require Update or UpdateCtx callback
  - Require Finalize or FinalizeCtx callback
  - Require Combine callback (parallel support)
  - Validate InputTypeInfos not empty
  - Validate ResultTypeInfo set

## Phase 2: Execution Engine

- [ ] **2.1** Implement GroupState
  - key field ([]any) for group key
  - state field (AggregateFuncState) for user state

- [ ] **2.2** Implement AggregateExecutionState
  - udf field (*registeredAggregateFunc)
  - groups field (map[string]*GroupState)
  - noGroupKey field (*GroupState) for ungrouped
  - clock field (quartz.Clock)
  - ctx field (context.Context)
  - NewAggregateExecutionState constructor

- [ ] **2.3** Implement group key management
  - extractGroupKey(chunk, rowIdx, groupCols) method
  - serializeGroupKey(key []any) string helper
  - getOrCreateState(groupKey) method with Init call

- [ ] **2.4** Implement ProcessChunk
  - Clock tag at chunk_start
  - Iterate rows in chunk
  - Extract group key for each row
  - Extract values from value columns
  - NULL handling based on SpecialNullHandling
  - Call updateState for each row
  - Clock tag at chunk_end

- [ ] **2.5** Implement CombineWith
  - Clock tag at combine_start
  - Iterate other.groups
  - For existing keys: call Combine
  - For new keys: copy GroupState
  - Clock tag at combine_end

- [ ] **2.6** Implement Finalize
  - Clock tag at finalize_start
  - Iterate all groups
  - Call finalizeGroup for each
  - Return []GroupResult with key and value
  - Clock tag at finalize_end

## Phase 3: Safe Execution Wrappers

- [ ] **3.1** Implement safeAggregateUpdate
  - defer/recover for panic handling
  - Convert panic to error
  - Support both Update and UpdateCtx variants

- [ ] **3.2** Implement safeAggregateFinalize
  - defer/recover for panic handling
  - Convert panic to error
  - Support both Finalize and FinalizeCtx variants

- [ ] **3.3** Implement safeAggregateCombine
  - defer/recover for panic handling
  - Convert panic to error

## Phase 4: Public API and Integration

- [ ] **4.1** Add aggregateFuncs field to Conn
  - *aggregateFuncRegistry field
  - Lazy initialization on first registration

- [ ] **4.2** Implement RegisterAggregateUDF
  - Extract *Conn from sql.Conn via Raw()
  - Initialize registry if nil
  - Call registry.register()

- [ ] **4.3** Implement RegisterAggregateUDFSet
  - Accept variadic AggregateFuncs
  - Register each overload
  - Return first error encountered

- [ ] **4.4** Add AggregateUDFResolver interface to binder
  - LookupAggregateUDF(name, argTypes) method
  - Returns udfInfo, resultType, found

- [ ] **4.5** Modify binder to check aggregate UDFs
  - bindAggregateCall checks built-in first
  - Then checks aggregateFuncs resolver
  - bindUDFAggregate for UDF aggregates

## Phase 5: Testing

- [ ] **5.1** Unit tests for core types
  - Test AggregateFuncConfig construction
  - Test AggregateFuncExecutor validation
  - Test AggregateFunc complete setup

- [ ] **5.2** Unit tests for registry
  - Test successful registration
  - Test validation errors (empty name, missing callbacks)
  - Test lookup with exact type match
  - Test lookup with overloading
  - Test thread safety with concurrent access

- [ ] **5.3** Unit tests for execution engine
  - Test ProcessChunk with single group
  - Test ProcessChunk with multiple groups
  - Test NULL handling (skip vs pass)
  - Test CombineWith state merging
  - Test Finalize result generation

- [ ] **5.4** Integration tests with queries
  - Test simple aggregate: SELECT my_avg(value) FROM t
  - Test grouped aggregate: SELECT g, my_avg(value) FROM t GROUP BY g
  - Test multiple aggregates: SELECT my_sum(a), my_avg(b) FROM t
  - Test NULL values in input
  - Test empty result set

- [ ] **5.5** Deterministic tests with mock clock
  - Test with quartz.NewMock(t)
  - Verify clock tags at each phase
  - Test with Trap().Now() for specific operations
  - Verify deterministic behavior across runs

- [ ] **5.6** Panic recovery tests
  - Test panic in Init
  - Test panic in Update
  - Test panic in Combine
  - Test panic in Finalize
  - Verify errors returned, not panics

- [ ] **5.7** Context timeout tests
  - Test with cancelled context
  - Test with deadline exceeded
  - Verify graceful handling

## Validation Criteria

- [ ] All 7 callback types work correctly
- [ ] Registry supports type-based overloading
- [ ] NULL handling matches scalar UDF behavior
- [ ] Combine enables parallel aggregation
- [ ] All tests pass with mock clock injection
- [ ] Panic recovery converts to errors
- [ ] API matches duckdb-go patterns where applicable

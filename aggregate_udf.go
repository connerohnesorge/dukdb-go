package dukdb

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"time"

	"github.com/coder/quartz"
)

// =============================================================================
// Core Types (Phase 1.1)
// =============================================================================

// AggregateFuncConfig defines aggregate function signature and behavior.
type AggregateFuncConfig struct {
	// InputTypeInfos contains Type information for each input parameter of the aggregate function.
	// Cannot be empty, and each element cannot be nil.
	InputTypeInfos []TypeInfo

	// ResultTypeInfo holds the Type information of the aggregate function's result type.
	// Required, cannot be nil or TYPE_ANY.
	ResultTypeInfo TypeInfo

	// VariadicTypeInfo configures the number of input parameters.
	// If this field is nil, then the input parameters match InputTypeInfos.
	// Otherwise, the aggregate function's input parameters are set to variadic.
	VariadicTypeInfo TypeInfo

	// Volatile sets the stability of the aggregate function to volatile, if true.
	// Volatile aggregate functions skip result caching.
	Volatile bool

	// SpecialNullHandling disables the default NULL handling of aggregate functions, if true.
	// If false (default), NULL values are skipped in Update.
	// If true, NULL values are passed to Update.
	SpecialNullHandling bool
}

// AggregateFuncState is the user-defined state type for aggregate functions.
// Users implement their own state type that will be used during aggregation.
type AggregateFuncState interface{}

// StateInitFn creates initial aggregate state.
type StateInitFn func() AggregateFuncState

// StateDestroyFn cleans up aggregate state.
type StateDestroyFn func(state AggregateFuncState)

// UpdateFn processes input value and updates state (simple variant).
type UpdateFn func(state AggregateFuncState, args ...driver.Value) error

// UpdateContextFn processes input value with context (context-aware variant).
type UpdateContextFn func(ctx *AggregateFuncContext, state AggregateFuncState, args ...driver.Value) error

// CombineFn merges source state into target state (for parallel execution).
type CombineFn func(target, source AggregateFuncState) error

// FinalizeFn produces final result from state (simple variant).
type FinalizeFn func(state AggregateFuncState) (driver.Value, error)

// FinalizeContextFn produces final result with context (context-aware variant).
type FinalizeContextFn func(ctx *AggregateFuncContext, state AggregateFuncState) (driver.Value, error)

// AggregateFuncExecutor contains the callbacks to execute a user-defined aggregate function.
type AggregateFuncExecutor struct {
	// Init creates initial state (REQUIRED).
	Init StateInitFn

	// Destroy cleans up state (optional, called during Cleanup).
	Destroy StateDestroyFn

	// Update processes input value (simple variant).
	// One of Update or UpdateCtx is REQUIRED.
	Update UpdateFn

	// UpdateCtx processes input value with context (context-aware variant).
	// One of Update or UpdateCtx is REQUIRED.
	UpdateCtx UpdateContextFn

	// Combine merges partial states (REQUIRED for parallel execution).
	Combine CombineFn

	// Finalize produces final result (simple variant).
	// One of Finalize or FinalizeCtx is REQUIRED.
	Finalize FinalizeFn

	// FinalizeCtx produces final result with context (context-aware variant).
	// One of Finalize or FinalizeCtx is REQUIRED.
	FinalizeCtx FinalizeContextFn
}

// AggregateFunc combines config and executor for a complete aggregate function definition.
type AggregateFunc struct {
	Config   AggregateFuncConfig
	Executor AggregateFuncExecutor
}

// =============================================================================
// Aggregate Function Context (Phase 1.2)
// =============================================================================

// AggregateFuncContext provides runtime context with clock for deterministic execution.
// Matches ScalarFuncContext pattern from scalar_udf.go.
type AggregateFuncContext struct {
	ctx      context.Context
	clock    quartz.Clock
	groupKey []any // Current group key for grouped aggregations
}

// NewAggregateFuncContext creates a new AggregateFuncContext with the given context and clock.
func NewAggregateFuncContext(ctx context.Context, clock quartz.Clock) *AggregateFuncContext {
	if clock == nil {
		clock = quartz.NewReal() // Default to real clock if nil (matches scalar UDF)
	}
	return &AggregateFuncContext{
		ctx:   ctx,
		clock: clock,
	}
}

// WithClock returns a new context with the specified clock.
func (c *AggregateFuncContext) WithClock(clock quartz.Clock) *AggregateFuncContext {
	return &AggregateFuncContext{
		ctx:      c.ctx,
		clock:    clock,
		groupKey: c.groupKey,
	}
}

// Context returns the underlying context.
func (c *AggregateFuncContext) Context() context.Context {
	return c.ctx
}

// Clock returns the clock used for timeout checking.
func (c *AggregateFuncContext) Clock() quartz.Clock {
	return c.clock
}

// GroupKey returns the current group key for grouped aggregations.
func (c *AggregateFuncContext) GroupKey() []any {
	return c.groupKey
}

// Now returns current time for deterministic testing.
func (c *AggregateFuncContext) Now() time.Time {
	return c.clock.Now() // Tag: "aggregate", "context", "now"
}

// checkTimeout checks if the context deadline has been exceeded using the injected clock.
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

// =============================================================================
// Aggregate Function Registry (Phase 1.3 & 1.4)
// =============================================================================

// registeredAggregateFunc holds a registered aggregate function with its metadata.
type registeredAggregateFunc struct {
	name     string
	config   AggregateFuncConfig
	executor AggregateFuncExecutor
}

// aggregateFuncRegistry holds registered aggregate functions per connection.
type aggregateFuncRegistry struct {
	mu        sync.RWMutex
	functions map[string][]*registeredAggregateFunc // name -> overloads
}

// newAggregateFuncRegistry creates a new aggregate function registry.
func newAggregateFuncRegistry() *aggregateFuncRegistry {
	return &aggregateFuncRegistry{
		functions: make(map[string][]*registeredAggregateFunc),
	}
}

// register adds an aggregate function to the registry.
func (r *aggregateFuncRegistry) register(name string, f AggregateFunc) error {
	// Validation matching scalar UDF error message patterns
	if name == "" {
		return fmt.Errorf("aggregate UDF name cannot be empty")
	}
	if f.Executor.Init == nil {
		return fmt.Errorf("aggregate UDF must have Init callback")
	}
	if f.Executor.Update == nil && f.Executor.UpdateCtx == nil {
		return fmt.Errorf("aggregate UDF must have either Update or UpdateCtx callback")
	}
	if f.Executor.Finalize == nil && f.Executor.FinalizeCtx == nil {
		return fmt.Errorf("aggregate UDF must have either Finalize or FinalizeCtx callback")
	}
	if f.Executor.Combine == nil {
		return fmt.Errorf("aggregate UDF must have Combine callback for parallel execution")
	}

	// Type validation (matches scalar UDF pattern)
	if f.Config.ResultTypeInfo == nil {
		return fmt.Errorf("aggregate UDF result type cannot be nil")
	}
	if f.Config.ResultTypeInfo.InternalType() == TYPE_ANY {
		return fmt.Errorf("aggregate UDF result type cannot be TYPE_ANY")
	}
	if len(f.Config.InputTypeInfos) == 0 {
		return fmt.Errorf("aggregate UDF must have at least one input type")
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

// lookup finds an aggregate function by name and argument types.
func (r *aggregateFuncRegistry) lookup(name string, argTypes []Type) *registeredAggregateFunc {
	r.mu.RLock()
	defer r.mu.RUnlock()

	overloads, ok := r.functions[name]
	if !ok {
		return nil
	}

	for _, fn := range overloads {
		if matchesTypesForAggregate(fn.config.InputTypeInfos, argTypes, fn.config.VariadicTypeInfo) {
			return fn
		}
	}
	return nil
}

// matchesTypesForAggregate checks if the function matches the given argument types.
// Reuses the same logic as scalar UDF type matching.
func matchesTypesForAggregate(inputTypes []TypeInfo, argTypes []Type, variadicType TypeInfo) bool {
	// Check non-variadic parameters
	for i, expected := range inputTypes {
		if i >= len(argTypes) {
			return false
		}
		if !typesCompatible(expected.InternalType(), argTypes[i]) {
			return false
		}
	}

	// Check if we have exactly the right number of args (non-variadic case)
	if variadicType == nil {
		return len(argTypes) == len(inputTypes)
	}

	// Check variadic parameters
	varType := variadicType.InternalType()
	for i := len(inputTypes); i < len(argTypes); i++ {
		if varType != TYPE_ANY && !typesCompatible(varType, argTypes[i]) {
			return false
		}
	}

	return true
}

// LookupAggregateUDF implements AggregateUDFResolver interface for binder integration.
func (r *aggregateFuncRegistry) LookupAggregateUDF(name string, argTypes []Type) (udfInfo any, resultType Type, found bool) {
	fn := r.lookup(name, argTypes)
	if fn == nil {
		return nil, TYPE_INVALID, false
	}
	return fn, fn.config.ResultTypeInfo.InternalType(), true
}

// IsVolatile returns true if function is marked volatile (skip caching).
func (r *aggregateFuncRegistry) IsVolatile(udfInfo any) bool {
	if fn, ok := udfInfo.(*registeredAggregateFunc); ok {
		return fn.config.Volatile
	}
	return false
}

// =============================================================================
// Group State and Execution Engine (Phase 2)
// =============================================================================

// GroupState manages state for each group in GROUP BY.
type GroupState struct {
	key   []any
	state AggregateFuncState
}

// GroupResult holds key and final value for a group.
type GroupResult struct {
	Key   []any
	Value driver.Value
}

// AggregateExecutionState manages all groups for aggregate execution.
// Clock is injected from Conn -> Engine -> ExecutionState for deterministic testing.
type AggregateExecutionState struct {
	udf        *registeredAggregateFunc
	groups     map[string]*GroupState // Serialized key -> state
	noGroupKey *GroupState            // For ungrouped aggregations
	clock      quartz.Clock           // Injected from Conn.clock via query execution
	ctx        context.Context
}

// NewAggregateExecutionState creates execution state with injected clock.
// Clock flow: Conn.clock -> Engine.execute() -> NewAggregateExecutionState(clock)
func NewAggregateExecutionState(udf *registeredAggregateFunc, ctx context.Context, clock quartz.Clock) *AggregateExecutionState {
	if clock == nil {
		clock = quartz.NewReal() // Default to real clock (matches scalar UDF)
	}
	return &AggregateExecutionState{
		udf:    udf,
		groups: make(map[string]*GroupState),
		clock:  clock,
		ctx:    ctx,
	}
}

// ProcessChunk updates state for all rows in a DataChunk.
func (s *AggregateExecutionState) ProcessChunk(chunk *DataChunk, groupCols []int, valueCols []int) error {
	_ = s.clock.Now() // Tag: "aggregate", "process", "chunk_start"

	for rowIdx := 0; rowIdx < chunk.GetSize(); rowIdx++ {
		// Extract group key
		groupKey := s.extractGroupKey(chunk, rowIdx, groupCols)
		gs := s.getOrCreateState(groupKey)

		// Extract values
		values := s.extractValues(chunk, rowIdx, valueCols)

		// Handle NULLs
		if !s.udf.config.SpecialNullHandling && s.hasNull(values) {
			continue // Skip NULL values by default
		}

		// Update state
		if err := s.updateState(gs, values); err != nil {
			return err
		}
	}

	_ = s.clock.Now() // Tag: "aggregate", "process", "chunk_end"
	return nil
}

// updateState calls the appropriate Update callback with panic recovery.
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

// CombineWith merges another execution state (for parallel aggregation).
func (s *AggregateExecutionState) CombineWith(other *AggregateExecutionState) error {
	_ = s.clock.Now() // Tag: "aggregate", "combine", "start"

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

	// Handle ungrouped aggregation
	if other.noGroupKey != nil {
		if s.noGroupKey != nil {
			if err := safeAggregateCombine(s.udf.executor.Combine, s.noGroupKey.state, other.noGroupKey.state); err != nil {
				return err
			}
		} else {
			s.noGroupKey = other.noGroupKey
		}
	}

	_ = s.clock.Now() // Tag: "aggregate", "combine", "end"
	return nil
}

// Finalize produces final results for all groups.
func (s *AggregateExecutionState) Finalize() ([]GroupResult, error) {
	_ = s.clock.Now() // Tag: "aggregate", "finalize", "start"

	var results []GroupResult

	// Handle ungrouped aggregation first
	if s.noGroupKey != nil {
		result, err := s.finalizeGroup(s.noGroupKey)
		if err != nil {
			return nil, err
		}
		results = append(results, GroupResult{Key: nil, Value: result})
	}

	// Handle grouped aggregations
	for _, gs := range s.groups {
		result, err := s.finalizeGroup(gs)
		if err != nil {
			return nil, err
		}
		results = append(results, GroupResult{Key: gs.key, Value: result})
	}

	_ = s.clock.Now() // Tag: "aggregate", "finalize", "end"
	return results, nil
}

// finalizeGroup produces result for a single group (with panic recovery).
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

// Cleanup destroys all group states (called after results are collected).
func (s *AggregateExecutionState) Cleanup() {
	_ = s.clock.Now() // Tag: "aggregate", "cleanup", "start"

	if s.udf.executor.Destroy != nil {
		for _, gs := range s.groups {
			safeAggregateDestroy(s.udf.executor.Destroy, gs.state)
		}
		if s.noGroupKey != nil {
			safeAggregateDestroy(s.udf.executor.Destroy, s.noGroupKey.state)
		}
	}
	s.groups = nil // Release map for GC
	s.noGroupKey = nil

	_ = s.clock.Now() // Tag: "aggregate", "cleanup", "end"
}

// =============================================================================
// Group Key Management Helpers (Phase 2.3)
// =============================================================================

// extractGroupKey extracts the group key from a chunk row.
func (s *AggregateExecutionState) extractGroupKey(chunk *DataChunk, rowIdx int, groupCols []int) []any {
	if len(groupCols) == 0 {
		return nil // Ungrouped aggregation
	}
	key := make([]any, len(groupCols))
	for i, col := range groupCols {
		val, _ := chunk.GetValue(col, rowIdx)
		key[i] = val
	}
	return key
}

// getOrCreateState gets existing state for a group key or creates new state.
func (s *AggregateExecutionState) getOrCreateState(groupKey []any) *GroupState {
	if groupKey == nil {
		// Ungrouped aggregation
		if s.noGroupKey == nil {
			s.noGroupKey = &GroupState{
				key:   nil,
				state: s.udf.executor.Init(),
			}
		}
		return s.noGroupKey
	}

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

// serializeGroupKey converts a group key to a string for map lookup.
func serializeGroupKey(key []any) string {
	var buf bytes.Buffer
	for i, v := range key {
		if i > 0 {
			buf.WriteByte('|')
		}
		fmt.Fprintf(&buf, "%v", v)
	}
	return buf.String()
}

// extractValues extracts values from specified columns in a chunk row.
func (s *AggregateExecutionState) extractValues(chunk *DataChunk, rowIdx int, valueCols []int) []driver.Value {
	values := make([]driver.Value, len(valueCols))
	for i, col := range valueCols {
		val, _ := chunk.GetValue(col, rowIdx)
		values[i] = val
	}
	return values
}

// hasNull checks if any value in the slice is NULL.
func (s *AggregateExecutionState) hasNull(values []driver.Value) bool {
	for _, v := range values {
		if v == nil {
			return true
		}
	}
	return false
}

// =============================================================================
// Safe Execution Wrappers (Phase 3)
// =============================================================================

// safeAggregateUpdate wraps UpdateContextFn with panic recovery.
func safeAggregateUpdate(fn UpdateContextFn, ctx *AggregateFuncContext, state AggregateFuncState, values []driver.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in aggregate UDF Update: %v", r)
		}
	}()
	return fn(ctx, state, values...)
}

// safeAggregateUpdateSimple wraps UpdateFn with panic recovery.
func safeAggregateUpdateSimple(fn UpdateFn, state AggregateFuncState, values []driver.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in aggregate UDF Update: %v", r)
		}
	}()
	return fn(state, values...)
}

// safeAggregateCombine wraps CombineFn with panic recovery.
func safeAggregateCombine(fn CombineFn, target, source AggregateFuncState) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in aggregate UDF Combine: %v", r)
		}
	}()
	return fn(target, source)
}

// safeAggregateFinalize wraps FinalizeContextFn with panic recovery.
func safeAggregateFinalize(fn FinalizeContextFn, ctx *AggregateFuncContext, state AggregateFuncState) (result driver.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in aggregate UDF Finalize: %v", r)
		}
	}()
	return fn(ctx, state)
}

// safeAggregateFinalizeSimple wraps FinalizeFn with panic recovery.
func safeAggregateFinalizeSimple(fn FinalizeFn, state AggregateFuncState) (result driver.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in aggregate UDF Finalize: %v", r)
		}
	}()
	return fn(state)
}

// safeAggregateDestroy wraps StateDestroyFn with panic recovery.
// Panics are caught but not propagated - cleanup should not fail.
func safeAggregateDestroy(fn StateDestroyFn, state AggregateFuncState) {
	defer func() {
		if r := recover(); r != nil {
			// Log but don't propagate - cleanup should not fail
			// In production: log.Printf("panic in aggregate UDF Destroy: %v", r)
			_ = r // Suppress unused warning
		}
	}()
	fn(state)
}

// =============================================================================
// Public Registration API (Phase 4.2 & 4.3)
// =============================================================================

// RegisterAggregateUDF registers an aggregate function on a connection.
// The Conn's clock will be used for deterministic execution in tests.
func RegisterAggregateUDF(c *sql.Conn, name string, f AggregateFunc) error {
	return c.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("invalid connection type: expected *Conn, got %T", driverConn)
		}

		if conn.aggregateFuncs == nil {
			conn.aggregateFuncs = newAggregateFuncRegistry()
		}

		return conn.aggregateFuncs.register(name, f)
	})
}

// RegisterAggregateUDFSet registers multiple overloads of an aggregate function.
func RegisterAggregateUDFSet(c *sql.Conn, name string, functions ...AggregateFunc) error {
	for i, f := range functions {
		if err := RegisterAggregateUDF(c, name, f); err != nil {
			return fmt.Errorf("registering aggregate UDF %s overload %d: %w", name, i, err)
		}
	}
	return nil
}

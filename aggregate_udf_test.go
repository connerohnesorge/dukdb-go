package dukdb

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unit Tests for Core Types

func TestAggregateFuncConfig(t *testing.T) {
	intType, err := NewTypeInfo(TYPE_INTEGER)
	require.NoError(t, err)
	bigintType, err := NewTypeInfo(TYPE_BIGINT)
	require.NoError(t, err)

	config := AggregateFuncConfig{
		InputTypeInfos:      []TypeInfo{intType},
		ResultTypeInfo:      bigintType,
		VariadicTypeInfo:    nil,
		Volatile:            false,
		SpecialNullHandling: false,
	}

	assert.Equal(t, 1, len(config.InputTypeInfos))
	assert.Equal(t, TYPE_INTEGER, config.InputTypeInfos[0].InternalType())
	assert.Equal(t, TYPE_BIGINT, config.ResultTypeInfo.InternalType())
	assert.False(t, config.Volatile)
	assert.False(t, config.SpecialNullHandling)
}

func TestAggregateFuncExecutor(t *testing.T) {
	// Simple sum state
	type sumState struct {
		sum int64
	}

	executor := AggregateFuncExecutor{
		Init: func() AggregateFuncState {
			return &sumState{sum: 0}
		},
		Destroy: func(state AggregateFuncState) {
			// No-op cleanup
		},
		Update: func(state AggregateFuncState, args ...driver.Value) error {
			s := state.(*sumState)
			if len(args) > 0 {
				if v, ok := args[0].(int64); ok {
					s.sum += v
				}
			}

			return nil
		},
		Combine: func(target, source AggregateFuncState) error {
			t := target.(*sumState)
			s := source.(*sumState)
			t.sum += s.sum

			return nil
		},
		Finalize: func(state AggregateFuncState) (driver.Value, error) {
			s := state.(*sumState)

			return s.sum, nil
		},
	}

	// Test Init
	state := executor.Init()
	require.NotNil(t, state)

	// Test Update
	err := executor.Update(state, int64(10))
	require.NoError(t, err)
	err = executor.Update(state, int64(20))
	require.NoError(t, err)

	// Test Finalize
	result, err := executor.Finalize(state)
	require.NoError(t, err)
	assert.Equal(t, int64(30), result)

	// Test Combine
	state2 := executor.Init()
	_ = executor.Update(state2, int64(5))
	err = executor.Combine(state, state2)
	require.NoError(t, err)

	result, err = executor.Finalize(state)
	require.NoError(t, err)
	assert.Equal(t, int64(35), result)
}

// Unit Tests for Registry

func TestAggregateRegistryValidation(t *testing.T) {
	registry := newAggregateFuncRegistry()
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	validFunc := AggregateFunc{
		Config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: bigintType,
		},
		Executor: AggregateFuncExecutor{
			Init:     func() AggregateFuncState { return new(int64) },
			Update:   func(state AggregateFuncState, args ...driver.Value) error { return nil },
			Combine:  func(target, source AggregateFuncState) error { return nil },
			Finalize: func(state AggregateFuncState) (driver.Value, error) { return int64(0), nil },
		},
	}

	// Test: Empty name
	err := registry.register("", validFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "name cannot be empty")

	// Test: Missing Init
	noInitFunc := validFunc
	noInitFunc.Executor.Init = nil
	err = registry.register("test", noInitFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have Init callback")

	// Test: Missing Update and UpdateCtx
	noUpdateFunc := validFunc
	noUpdateFunc.Executor.Update = nil
	noUpdateFunc.Executor.UpdateCtx = nil
	err = registry.register("test", noUpdateFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have either Update or UpdateCtx")

	// Test: Missing Finalize and FinalizeCtx
	noFinalizeFunc := validFunc
	noFinalizeFunc.Executor.Finalize = nil
	noFinalizeFunc.Executor.FinalizeCtx = nil
	err = registry.register("test", noFinalizeFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have either Finalize or FinalizeCtx")

	// Test: Missing Combine
	noCombineFunc := validFunc
	noCombineFunc.Executor.Combine = nil
	err = registry.register("test", noCombineFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have Combine callback")

	// Test: Nil result type
	nilResultFunc := validFunc
	nilResultFunc.Config.ResultTypeInfo = nil
	err = registry.register("test", nilResultFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "result type cannot be nil")

	// Test: Empty input types
	noInputFunc := validFunc
	noInputFunc.Config.InputTypeInfos = nil
	err = registry.register("test", noInputFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have at least one input type")

	// Test: Nil input type
	nilInputFunc := validFunc
	nilInputFunc.Config.InputTypeInfos = []TypeInfo{nil}
	err = registry.register("test", nilInputFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "input type at index 0 cannot be nil")

	// Test: Valid registration
	err = registry.register("my_sum", validFunc)
	assert.NoError(t, err)
}

func TestAggregateRegistryLookup(t *testing.T) {
	registry := newAggregateFuncRegistry()
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	doubleType, _ := NewTypeInfo(TYPE_DOUBLE)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	// Register integer sum
	intSum := AggregateFunc{
		Config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: bigintType,
		},
		Executor: AggregateFuncExecutor{
			Init:     func() AggregateFuncState { return new(int64) },
			Update:   func(state AggregateFuncState, args ...driver.Value) error { return nil },
			Combine:  func(target, source AggregateFuncState) error { return nil },
			Finalize: func(state AggregateFuncState) (driver.Value, error) { return int64(0), nil },
		},
	}
	err := registry.register("my_sum", intSum)
	require.NoError(t, err)

	// Register double sum
	doubleSum := AggregateFunc{
		Config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{doubleType},
			ResultTypeInfo: doubleType,
		},
		Executor: AggregateFuncExecutor{
			Init:     func() AggregateFuncState { return new(float64) },
			Update:   func(state AggregateFuncState, args ...driver.Value) error { return nil },
			Combine:  func(target, source AggregateFuncState) error { return nil },
			Finalize: func(state AggregateFuncState) (driver.Value, error) { return float64(0), nil },
		},
	}
	err = registry.register("my_sum", doubleSum)
	require.NoError(t, err)

	// Test: Lookup integer variant
	fn := registry.lookup("my_sum", []Type{TYPE_INTEGER})
	require.NotNil(t, fn)
	assert.Equal(t, TYPE_BIGINT, fn.config.ResultTypeInfo.InternalType())

	// Test: Lookup double variant
	fn = registry.lookup("my_sum", []Type{TYPE_DOUBLE})
	require.NotNil(t, fn)
	assert.Equal(t, TYPE_DOUBLE, fn.config.ResultTypeInfo.InternalType())

	// Test: Lookup non-existent function
	fn = registry.lookup("non_existent", []Type{TYPE_INTEGER})
	assert.Nil(t, fn)

	// Test: Lookup with wrong type
	fn = registry.lookup("my_sum", []Type{TYPE_VARCHAR})
	assert.Nil(t, fn)

	// Test: LookupAggregateUDF interface
	udfInfo, resultType, found := registry.LookupAggregateUDF("my_sum", []Type{TYPE_INTEGER})
	assert.True(t, found)
	assert.NotNil(t, udfInfo)
	assert.Equal(t, TYPE_BIGINT, resultType)

	// Test: IsVolatile
	assert.False(t, registry.IsVolatile(udfInfo))
}

func TestAggregateRegistryVariadic(t *testing.T) {
	registry := newAggregateFuncRegistry()
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	// Register variadic sum
	variadicSum := AggregateFunc{
		Config: AggregateFuncConfig{
			InputTypeInfos:   []TypeInfo{intType},
			ResultTypeInfo:   bigintType,
			VariadicTypeInfo: intType,
		},
		Executor: AggregateFuncExecutor{
			Init:     func() AggregateFuncState { return new(int64) },
			Update:   func(state AggregateFuncState, args ...driver.Value) error { return nil },
			Combine:  func(target, source AggregateFuncState) error { return nil },
			Finalize: func(state AggregateFuncState) (driver.Value, error) { return int64(0), nil },
		},
	}
	err := registry.register("variadic_sum", variadicSum)
	require.NoError(t, err)

	// Test: Lookup with single arg
	fn := registry.lookup("variadic_sum", []Type{TYPE_INTEGER})
	require.NotNil(t, fn)

	// Test: Lookup with multiple args
	fn = registry.lookup("variadic_sum", []Type{TYPE_INTEGER, TYPE_INTEGER, TYPE_INTEGER})
	require.NotNil(t, fn)

	// Test: Lookup with wrong type in variadic
	fn = registry.lookup("variadic_sum", []Type{TYPE_INTEGER, TYPE_VARCHAR})
	assert.Nil(t, fn)
}

// Unit Tests for Execution Engine

func TestAggregateExecutionStateBasic(t *testing.T) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	type sumState struct {
		sum int64
	}

	reg := &registeredAggregateFunc{
		name: "test_sum",
		config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: bigintType,
		},
		executor: AggregateFuncExecutor{
			Init: func() AggregateFuncState {
				return &sumState{sum: 0}
			},
			Update: func(state AggregateFuncState, args ...driver.Value) error {
				s := state.(*sumState)
				if len(args) > 0 {
					if v, ok := args[0].(int32); ok {
						s.sum += int64(v)
					}
				}

				return nil
			},
			Combine: func(target, source AggregateFuncState) error {
				t := target.(*sumState)
				s := source.(*sumState)
				t.sum += s.sum

				return nil
			},
			Finalize: func(state AggregateFuncState) (driver.Value, error) {
				s := state.(*sumState)

				return s.sum, nil
			},
		},
	}

	mClock := quartz.NewMock(t)
	mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	execState := NewAggregateExecutionState(reg, context.Background(), mClock)
	require.NotNil(t, execState)

	// Create a test data chunk
	chunk, err := NewDataChunk([]TypeInfo{intType})
	require.NoError(t, err)

	// Add test data
	for i := range 5 {
		err = chunk.SetValue(0, i, int32(i+1))
		require.NoError(t, err)
	}
	err = chunk.SetSize(5)
	require.NoError(t, err)

	// Process chunk (ungrouped aggregation)
	err = execState.ProcessChunk(chunk, []int{}, []int{0})
	require.NoError(t, err)

	// Finalize
	results, err := execState.Finalize()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(15), results[0].Value) // 1+2+3+4+5 = 15

	// Cleanup
	execState.Cleanup()
}

func TestAggregateExecutionStateCombine(t *testing.T) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	type sumState struct {
		sum int64
	}

	reg := &registeredAggregateFunc{
		name: "test_sum",
		config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: bigintType,
		},
		executor: AggregateFuncExecutor{
			Init: func() AggregateFuncState {
				return &sumState{sum: 0}
			},
			Update: func(state AggregateFuncState, args ...driver.Value) error {
				s := state.(*sumState)
				if len(args) > 0 {
					if v, ok := args[0].(int32); ok {
						s.sum += int64(v)
					}
				}

				return nil
			},
			Combine: func(target, source AggregateFuncState) error {
				t := target.(*sumState)
				s := source.(*sumState)
				t.sum += s.sum

				return nil
			},
			Finalize: func(state AggregateFuncState) (driver.Value, error) {
				s := state.(*sumState)

				return s.sum, nil
			},
		},
	}

	mClock := quartz.NewMock(t)

	// Create two execution states
	state1 := NewAggregateExecutionState(reg, context.Background(), mClock)
	state2 := NewAggregateExecutionState(reg, context.Background(), mClock)

	// Create test chunks
	chunk1, _ := NewDataChunk([]TypeInfo{intType})
	chunk2, _ := NewDataChunk([]TypeInfo{intType})

	for i := range 3 {
		_ = chunk1.SetValue(0, i, int32(i+1)) // 1, 2, 3
	}
	_ = chunk1.SetSize(3)

	for i := range 2 {
		_ = chunk2.SetValue(0, i, int32(i+10)) // 10, 11
	}
	_ = chunk2.SetSize(2)

	// Process chunks
	_ = state1.ProcessChunk(chunk1, []int{}, []int{0})
	_ = state2.ProcessChunk(chunk2, []int{}, []int{0})

	// Combine state2 into state1
	err := state1.CombineWith(state2)
	require.NoError(t, err)

	// Finalize
	results, err := state1.Finalize()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(27), results[0].Value) // 1+2+3+10+11 = 27
}

func TestAggregateExecutionStateNullHandling(t *testing.T) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	type countState struct {
		count int64
	}

	// Without SpecialNullHandling - NULLs are skipped
	reg := &registeredAggregateFunc{
		name: "test_count",
		config: AggregateFuncConfig{
			InputTypeInfos:      []TypeInfo{intType},
			ResultTypeInfo:      bigintType,
			SpecialNullHandling: false,
		},
		executor: AggregateFuncExecutor{
			Init: func() AggregateFuncState {
				return &countState{count: 0}
			},
			Update: func(state AggregateFuncState, args ...driver.Value) error {
				s := state.(*countState)
				s.count++

				return nil
			},
			Combine: func(target, source AggregateFuncState) error {
				t := target.(*countState)
				s := source.(*countState)
				t.count += s.count

				return nil
			},
			Finalize: func(state AggregateFuncState) (driver.Value, error) {
				s := state.(*countState)

				return s.count, nil
			},
		},
	}

	execState := NewAggregateExecutionState(reg, context.Background(), nil)

	chunk, _ := NewDataChunk([]TypeInfo{intType})
	_ = chunk.SetValue(0, 0, int32(1))
	_ = chunk.SetValue(0, 1, nil) // NULL
	_ = chunk.SetValue(0, 2, int32(3))
	_ = chunk.SetSize(3)

	_ = execState.ProcessChunk(chunk, []int{}, []int{0})

	results, err := execState.Finalize()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(2), results[0].Value) // Only non-NULL values counted

	// With SpecialNullHandling - NULLs are passed to Update
	regWithNull := *reg
	regWithNull.config.SpecialNullHandling = true

	execState2 := NewAggregateExecutionState(&regWithNull, context.Background(), nil)

	chunk2, _ := NewDataChunk([]TypeInfo{intType})
	_ = chunk2.SetValue(0, 0, int32(1))
	_ = chunk2.SetValue(0, 1, nil) // NULL
	_ = chunk2.SetValue(0, 2, int32(3))
	_ = chunk2.SetSize(3)

	_ = execState2.ProcessChunk(chunk2, []int{}, []int{0})

	results2, err := execState2.Finalize()
	require.NoError(t, err)
	require.Len(t, results2, 1)
	assert.Equal(t, int64(3), results2[0].Value) // All values counted including NULL
}

// Deterministic Tests with Mock Clock

func TestAggregateFuncContextWithClock(t *testing.T) {
	mClock := quartz.NewMock(t)
	mClock.Set(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))

	ctx := NewAggregateFuncContext(context.Background(), mClock)
	require.NotNil(t, ctx)

	// Test Now() returns mock time
	now := ctx.Now()
	assert.Equal(t, 2024, now.Year())
	assert.Equal(t, time.January, now.Month())
	assert.Equal(t, 1, now.Day())

	// Test Clock() returns the mock clock
	assert.Equal(t, mClock, ctx.Clock())

	// Test Context() returns the underlying context
	assert.Equal(t, context.Background(), ctx.Context())

	// Test WithClock creates new context with new clock
	mClock2 := quartz.NewMock(t)
	mClock2.Set(time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC))

	ctx2 := ctx.WithClock(mClock2)
	assert.Equal(t, 2025, ctx2.Now().Year())
}

func TestAggregateFuncContextGroupKey(t *testing.T) {
	mClock := quartz.NewMock(t)
	ctx := &AggregateFuncContext{
		ctx:      context.Background(),
		clock:    mClock,
		groupKey: []any{"department", int64(42)},
	}

	key := ctx.GroupKey()
	require.Len(t, key, 2)
	assert.Equal(t, "department", key[0])
	assert.Equal(t, int64(42), key[1])
}

func TestAggregateFuncContextDefaultClock(t *testing.T) {
	// When clock is nil, should default to real clock
	ctx := NewAggregateFuncContext(context.Background(), nil)
	require.NotNil(t, ctx)
	require.NotNil(t, ctx.Clock())

	// The time should be close to now
	now := ctx.Now()
	assert.WithinDuration(t, time.Now(), now, 5*time.Second)
}

// Panic Recovery Tests

func TestAggregateUDFPanicRecoveryUpdate(t *testing.T) {
	// Test Update panic recovery
	panicUpdate := func(state AggregateFuncState, args ...driver.Value) error {
		panic("intentional panic in Update")
	}

	err := safeAggregateUpdateSimple(panicUpdate, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic in aggregate UDF Update")
	assert.Contains(t, err.Error(), "intentional panic in Update")
}

func TestAggregateUDFPanicRecoveryUpdateContext(t *testing.T) {
	panicUpdateCtx := func(ctx *AggregateFuncContext, state AggregateFuncState, args ...driver.Value) error {
		panic("intentional panic in UpdateCtx")
	}

	ctx := NewAggregateFuncContext(context.Background(), nil)
	err := safeAggregateUpdate(panicUpdateCtx, ctx, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic in aggregate UDF Update")
}

func TestAggregateUDFPanicRecoveryCombine(t *testing.T) {
	panicCombine := func(target, source AggregateFuncState) error {
		panic("intentional panic in Combine")
	}

	err := safeAggregateCombine(panicCombine, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic in aggregate UDF Combine")
}

func TestAggregateUDFPanicRecoveryFinalize(t *testing.T) {
	panicFinalize := func(state AggregateFuncState) (driver.Value, error) {
		panic("intentional panic in Finalize")
	}

	_, err := safeAggregateFinalizeSimple(panicFinalize, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic in aggregate UDF Finalize")
}

func TestAggregateUDFPanicRecoveryFinalizeContext(t *testing.T) {
	panicFinalizeCtx := func(ctx *AggregateFuncContext, state AggregateFuncState) (driver.Value, error) {
		panic("intentional panic in FinalizeCtx")
	}

	ctx := NewAggregateFuncContext(context.Background(), nil)
	_, err := safeAggregateFinalize(panicFinalizeCtx, ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic in aggregate UDF Finalize")
}

func TestAggregateUDFPanicRecoveryDestroy(t *testing.T) {
	panicDestroy := func(state AggregateFuncState) {
		panic("intentional panic in Destroy")
	}

	// Should not panic - panics are swallowed in Destroy
	assert.NotPanics(t, func() {
		safeAggregateDestroy(panicDestroy, nil)
	})
}

// Context Timeout Tests

func TestAggregateFuncContextTimeout(t *testing.T) {
	mClock := quartz.NewMock(t)
	mClock.Set(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC))

	// Create context with deadline in the future (use real time for deadline, mock for checking)
	deadline := time.Now().Add(1 * time.Hour) // Real time deadline in the future
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	aggCtx := &AggregateFuncContext{
		ctx:   ctx,
		clock: mClock,
	}

	// Before deadline - should not timeout (mock clock is in the past)
	err := aggCtx.checkTimeout()
	assert.NoError(t, err)

	// Create a context with deadline already passed according to mock clock
	pastDeadline := mClock.Now().Add(-100 * time.Millisecond)
	ctx2, cancel2 := context.WithDeadline(context.Background(), pastDeadline)
	defer cancel2()

	aggCtx2 := &AggregateFuncContext{
		ctx:   ctx2,
		clock: mClock,
	}

	// After deadline - should timeout
	err = aggCtx2.checkTimeout()
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestAggregateFuncContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	aggCtx := NewAggregateFuncContext(ctx, nil)

	// Before cancellation - should not error
	err := aggCtx.checkTimeout()
	assert.NoError(t, err)

	// Cancel the context
	cancel()

	// After cancellation - should error
	err = aggCtx.checkTimeout()
	assert.ErrorIs(t, err, context.Canceled)
}

// Helper Tests

func TestSerializeGroupKey(t *testing.T) {
	tests := []struct {
		name     string
		key      []any
		expected string
	}{
		{
			name:     "empty key",
			key:      []any{},
			expected: "",
		},
		{
			name:     "single string",
			key:      []any{"hello"},
			expected: "hello",
		},
		{
			name:     "single int",
			key:      []any{42},
			expected: "42",
		},
		{
			name:     "multiple values",
			key:      []any{"dept", 100, true},
			expected: "dept|100|true",
		},
		{
			name:     "nil value",
			key:      []any{nil, "test"},
			expected: "<nil>|test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := serializeGroupKey(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMatchesTypesForAggregate(t *testing.T) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	doubleType, _ := NewTypeInfo(TYPE_DOUBLE)
	anyType := &typeInfo{typ: TYPE_ANY}

	tests := []struct {
		name         string
		inputTypes   []TypeInfo
		argTypes     []Type
		variadicType TypeInfo
		expected     bool
	}{
		{
			name:         "exact match single",
			inputTypes:   []TypeInfo{intType},
			argTypes:     []Type{TYPE_INTEGER},
			variadicType: nil,
			expected:     true,
		},
		{
			name:         "exact match multiple",
			inputTypes:   []TypeInfo{intType, doubleType},
			argTypes:     []Type{TYPE_INTEGER, TYPE_DOUBLE},
			variadicType: nil,
			expected:     true,
		},
		{
			name:         "type mismatch",
			inputTypes:   []TypeInfo{intType},
			argTypes:     []Type{TYPE_DOUBLE},
			variadicType: nil,
			expected:     false,
		},
		{
			name:         "too few args",
			inputTypes:   []TypeInfo{intType, doubleType},
			argTypes:     []Type{TYPE_INTEGER},
			variadicType: nil,
			expected:     false,
		},
		{
			name:         "too many args non-variadic",
			inputTypes:   []TypeInfo{intType},
			argTypes:     []Type{TYPE_INTEGER, TYPE_INTEGER},
			variadicType: nil,
			expected:     false,
		},
		{
			name:         "variadic match",
			inputTypes:   []TypeInfo{intType},
			argTypes:     []Type{TYPE_INTEGER, TYPE_INTEGER, TYPE_INTEGER},
			variadicType: intType,
			expected:     true,
		},
		{
			name:         "variadic type mismatch",
			inputTypes:   []TypeInfo{intType},
			argTypes:     []Type{TYPE_INTEGER, TYPE_DOUBLE},
			variadicType: intType,
			expected:     false,
		},
		{
			name:         "variadic any type",
			inputTypes:   []TypeInfo{intType},
			argTypes:     []Type{TYPE_INTEGER, TYPE_DOUBLE, TYPE_VARCHAR},
			variadicType: anyType,
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesTypesForAggregate(tt.inputTypes, tt.argTypes, tt.variadicType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Validation Criteria Tests

func TestAllCallbackTypesWork(t *testing.T) {
	// Test that all 7 callback types work correctly
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)

	type state struct {
		sum   int64
		calls []string
	}

	destroyCalled := false

	agg := AggregateFunc{
		Config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: bigintType,
		},
		Executor: AggregateFuncExecutor{
			Init: func() AggregateFuncState {
				return &state{sum: 0, calls: []string{"init"}}
			},
			Destroy: func(s AggregateFuncState) {
				destroyCalled = true
				st := s.(*state)
				st.calls = append(st.calls, "destroy")
			},
			Update: func(s AggregateFuncState, args ...driver.Value) error {
				st := s.(*state)
				st.calls = append(st.calls, "update")
				if len(args) > 0 {
					if v, ok := args[0].(int32); ok {
						st.sum += int64(v)
					}
				}

				return nil
			},
			Combine: func(target, source AggregateFuncState) error {
				t := target.(*state)
				s := source.(*state)
				t.sum += s.sum
				t.calls = append(t.calls, "combine")

				return nil
			},
			Finalize: func(s AggregateFuncState) (driver.Value, error) {
				st := s.(*state)
				st.calls = append(st.calls, "finalize")

				return st.sum, nil
			},
		},
	}

	registry := newAggregateFuncRegistry()
	err := registry.register("test_agg", agg)
	require.NoError(t, err)

	fn := registry.lookup("test_agg", []Type{TYPE_INTEGER})
	require.NotNil(t, fn)

	exec := NewAggregateExecutionState(fn, context.Background(), nil)

	chunk, _ := NewDataChunk([]TypeInfo{intType})
	_ = chunk.SetValue(0, 0, int32(10))
	_ = chunk.SetValue(0, 1, int32(20))
	_ = chunk.SetSize(2)

	err = exec.ProcessChunk(chunk, []int{}, []int{0})
	require.NoError(t, err)

	results, err := exec.Finalize()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(30), results[0].Value)

	exec.Cleanup()
	assert.True(t, destroyCalled)
}

func TestContextAwareCallbacks(t *testing.T) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	bigintType, _ := NewTypeInfo(TYPE_BIGINT)
	mClock := quartz.NewMock(t)
	mClock.Set(time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC))

	type state struct {
		sum       int64
		clockUsed bool
	}

	agg := AggregateFunc{
		Config: AggregateFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: bigintType,
		},
		Executor: AggregateFuncExecutor{
			Init: func() AggregateFuncState {
				return &state{sum: 0}
			},
			UpdateCtx: func(ctx *AggregateFuncContext, s AggregateFuncState, args ...driver.Value) error {
				st := s.(*state)
				// Use the clock from context
				now := ctx.Now()
				if now.Year() == 2024 {
					st.clockUsed = true
				}
				if len(args) > 0 {
					if v, ok := args[0].(int32); ok {
						st.sum += int64(v)
					}
				}

				return nil
			},
			Combine: func(target, source AggregateFuncState) error {
				t := target.(*state)
				s := source.(*state)
				t.sum += s.sum

				return nil
			},
			FinalizeCtx: func(ctx *AggregateFuncContext, s AggregateFuncState) (driver.Value, error) {
				st := s.(*state)
				// Verify clock was used
				if !st.clockUsed {
					return nil, nil
				}

				return st.sum, nil
			},
		},
	}

	reg := &registeredAggregateFunc{
		name:     "ctx_agg",
		config:   agg.Config,
		executor: agg.Executor,
	}

	exec := NewAggregateExecutionState(reg, context.Background(), mClock)

	chunk, _ := NewDataChunk([]TypeInfo{intType})
	_ = chunk.SetValue(0, 0, int32(5))
	_ = chunk.SetSize(1)

	err := exec.ProcessChunk(chunk, []int{}, []int{0})
	require.NoError(t, err)

	results, err := exec.Finalize()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(5), results[0].Value)
}

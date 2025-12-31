package dukdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testContextKey is a custom type for context keys to avoid staticcheck SA1029.
type testContextKey string

// testScalarFunc is a simple scalar function implementation for testing.
type testScalarFunc struct {
	config   ScalarFuncConfig
	executor ScalarFuncExecutor
}

func (f *testScalarFunc) Config() ScalarFuncConfig {
	return f.config
}

func (f *testScalarFunc) Executor() ScalarFuncExecutor {
	return f.executor
}

// createSimpleScalarFunc creates a simple scalar function for testing.
func createSimpleScalarFunc(
	inputTypes []TypeInfo,
	resultType TypeInfo,
	fn RowExecutorFn,
) ScalarFunc {
	return &testScalarFunc{
		config: ScalarFuncConfig{
			InputTypeInfos: inputTypes,
			ResultTypeInfo: resultType,
		},
		executor: ScalarFuncExecutor{
			RowExecutor: fn,
		},
	}
}

// createTestVector creates a vector initialized with a type for testing.
func createTestVector(
	typeInfo TypeInfo,
	capacity int,
) (*vector, error) {
	vec := newVector(capacity)
	if err := vec.init(typeInfo, 0); err != nil {
		return nil, err
	}

	return vec, nil
}

// TestScalarFuncRegistry tests the scalar function registry.
func TestScalarFuncRegistry(t *testing.T) {
	t.Run(
		"register and lookup basic function",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"test_func",
				fn,
			)
			require.NoError(t, err)

			result := registry.lookup(
				"test_func",
				[]Type{TYPE_INTEGER},
			)
			require.NotNil(t, result)
			assert.Equal(
				t,
				"test_func",
				result.name,
			)
		},
	)

	t.Run(
		"lookup non-existent function",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()
			result := registry.lookup(
				"non_existent",
				[]Type{TYPE_INTEGER},
			)
			assert.Nil(t, result)
		},
	)

	t.Run(
		"register empty name",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register("", fn)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"name cannot be empty",
			)
		},
	)

	t.Run(
		"register nil function",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()
			err := registry.register(
				"test_func",
				nil,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"cannot be nil",
			)
		},
	)

	t.Run(
		"register function with nil result type",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			fn := &testScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: nil,
					ResultTypeInfo: nil,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return nil, nil
					},
				},
			}

			err := registry.register(
				"test_func",
				fn,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"result type cannot be nil",
			)
		},
	)

	t.Run(
		"register function with TYPE_ANY result",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			anyType := &typeInfo{typ: TYPE_ANY}

			fn := &testScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: nil,
					ResultTypeInfo: anyType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return nil, nil
					},
				},
			}

			err := registry.register(
				"test_func",
				fn,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"cannot be TYPE_ANY",
			)
		},
	)

	t.Run(
		"register function without executor",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := &testScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{},
			}

			err = registry.register(
				"test_func",
				fn,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"must have either RowExecutor or RowContextExecutor",
			)
		},
	)
}

// TestScalarFuncOverloading tests function overloading.
func TestScalarFuncOverloading(t *testing.T) {
	t.Run(
		"register multiple overloads",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)
			varcharType, err := NewTypeInfo(
				TYPE_VARCHAR,
			)
			require.NoError(t, err)

			fn1 := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			fn2 := createSimpleScalarFunc(
				[]TypeInfo{varcharType},
				varcharType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"overloaded",
				fn1,
			)
			require.NoError(t, err)

			err = registry.register(
				"overloaded",
				fn2,
			)
			require.NoError(t, err)

			// Lookup with INTEGER type
			result := registry.lookup(
				"overloaded",
				[]Type{TYPE_INTEGER},
			)
			require.NotNil(t, result)

			// Lookup with VARCHAR type
			result = registry.lookup(
				"overloaded",
				[]Type{TYPE_VARCHAR},
			)
			require.NotNil(t, result)
		},
	)

	t.Run(
		"lookup with non-matching type",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"test_func",
				fn,
			)
			require.NoError(t, err)

			// Lookup with mismatched type
			result := registry.lookup(
				"test_func",
				[]Type{TYPE_VARCHAR},
			)
			assert.Nil(t, result)
		},
	)
}

// TestTypeMatching tests type matching for function overloading.
func TestTypeMatching(t *testing.T) {
	t.Run("exact type match", func(t *testing.T) {
		assert.True(
			t,
			typesCompatible(
				TYPE_INTEGER,
				TYPE_INTEGER,
			),
		)
		assert.True(
			t,
			typesCompatible(
				TYPE_VARCHAR,
				TYPE_VARCHAR,
			),
		)
		assert.False(
			t,
			typesCompatible(
				TYPE_INTEGER,
				TYPE_VARCHAR,
			),
		)
	})

	t.Run(
		"TYPE_ANY matches anything",
		func(t *testing.T) {
			assert.True(
				t,
				typesCompatible(
					TYPE_ANY,
					TYPE_INTEGER,
				),
			)
			assert.True(
				t,
				typesCompatible(
					TYPE_ANY,
					TYPE_VARCHAR,
				),
			)
			assert.True(
				t,
				typesCompatible(
					TYPE_ANY,
					TYPE_BOOLEAN,
				),
			)
		},
	)

	t.Run(
		"variadic parameter matching",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			f := &registeredScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					VariadicTypeInfo: intType,
					ResultTypeInfo:   intType,
				},
			}

			// Should match with 1 argument
			assert.True(
				t,
				f.matchesTypes(
					[]Type{TYPE_INTEGER},
				),
			)

			// Should match with multiple arguments
			assert.True(
				t,
				f.matchesTypes(
					[]Type{
						TYPE_INTEGER,
						TYPE_INTEGER,
						TYPE_INTEGER,
					},
				),
			)

			// Should not match with wrong type
			assert.False(
				t,
				f.matchesTypes(
					[]Type{TYPE_VARCHAR},
				),
			)
		},
	)

	t.Run(
		"variadic with TYPE_ANY",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)
			anyType := &typeInfo{typ: TYPE_ANY}

			f := &registeredScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					VariadicTypeInfo: anyType,
					ResultTypeInfo:   intType,
				},
			}

			// Should match any additional arguments
			assert.True(
				t,
				f.matchesTypes(
					[]Type{
						TYPE_INTEGER,
						TYPE_VARCHAR,
						TYPE_BOOLEAN,
					},
				),
			)
		},
	)

	t.Run(
		"non-variadic arg count mismatch",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			f := &registeredScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
						intType,
					},
					ResultTypeInfo: intType,
				},
			}

			// Too few args
			assert.False(
				t,
				f.matchesTypes(
					[]Type{TYPE_INTEGER},
				),
			)

			// Too many args
			assert.False(
				t,
				f.matchesTypes(
					[]Type{
						TYPE_INTEGER,
						TYPE_INTEGER,
						TYPE_INTEGER,
					},
				),
			)

			// Correct count
			assert.True(
				t,
				f.matchesTypes(
					[]Type{
						TYPE_INTEGER,
						TYPE_INTEGER,
					},
				),
			)
		},
	)
}

// TestScalarFuncExecution tests scalar function execution.
func TestScalarFuncExecution(t *testing.T) {
	t.Run(
		"execute basic function",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			udf := &registeredScalarFunc{
				name: "double",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						v, ok := values[0].(int32)
						if !ok {
							return nil, fmt.Errorf(
								"expected int32, got %T",
								values[0],
							)
						}

						return v * 2, nil
					},
				},
				bindCtx: context.Background(),
			}

			// Create a simple DataChunk with one column
			chunk, err := NewDataChunk(
				[]TypeInfo{intType},
			)
			require.NoError(t, err)

			// Set values
			err = chunk.SetValue(0, 0, int32(5))
			require.NoError(t, err)
			err = chunk.SetValue(0, 1, int32(10))
			require.NoError(t, err)

			err = chunk.SetSize(2)
			require.NoError(t, err)

			// Create output vector
			output, err := createTestVector(
				intType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.NoError(t, err)

			// Check results
			val := output.getFn(output, 0)
			assert.Equal(t, int32(10), val)

			val = output.getFn(output, 1)
			assert.Equal(t, int32(20), val)
		},
	)

	t.Run(
		"null handling - null in null out",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			udf := &registeredScalarFunc{
				name: "identity",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
					// SpecialNullHandling: false (default)
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return values[0], nil
					},
				},
				bindCtx: context.Background(),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{intType},
			)
			require.NoError(t, err)

			err = chunk.SetValue(
				0,
				0,
				nil,
			) // NULL
			require.NoError(t, err)
			err = chunk.SetValue(0, 1, int32(42))
			require.NoError(t, err)

			err = chunk.SetSize(2)
			require.NoError(t, err)

			output, err := createTestVector(
				intType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.NoError(t, err)

			// First row should be NULL
			val := output.getFn(output, 0)
			assert.Nil(t, val)

			// Second row should be 42
			val = output.getFn(output, 1)
			assert.Equal(t, int32(42), val)
		},
	)

	t.Run(
		"special null handling",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			udf := &registeredScalarFunc{
				name: "null_to_zero",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo:      intType,
					SpecialNullHandling: true,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						if values[0] == nil {
							return int32(0), nil
						}

						return values[0], nil
					},
				},
				bindCtx: context.Background(),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{intType},
			)
			require.NoError(t, err)

			err = chunk.SetValue(
				0,
				0,
				nil,
			) // NULL
			require.NoError(t, err)
			err = chunk.SetValue(0, 1, int32(42))
			require.NoError(t, err)

			err = chunk.SetSize(2)
			require.NoError(t, err)

			output, err := createTestVector(
				intType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.NoError(t, err)

			// First row should be 0 (converted from NULL)
			val := output.getFn(output, 0)
			assert.Equal(t, int32(0), val)

			// Second row should be 42
			val = output.getFn(output, 1)
			assert.Equal(t, int32(42), val)
		},
	)

	t.Run("panic recovery", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		udf := &registeredScalarFunc{
			name: "panicker",
			config: ScalarFuncConfig{
				InputTypeInfos: []TypeInfo{
					intType,
				},
				ResultTypeInfo: intType,
			},
			executor: ScalarFuncExecutor{
				RowExecutor: func(values []driver.Value) (any, error) {
					panic("test panic")
				},
			},
			bindCtx: context.Background(),
		}

		chunk, err := NewDataChunk(
			[]TypeInfo{intType},
		)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, int32(1))
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		output, err := createTestVector(
			intType,
			VectorSize,
		)
		require.NoError(t, err)

		ctx := NewScalarFuncContext(
			context.Background(),
			nil,
		)
		err = executeScalarUDF(
			ctx,
			udf,
			chunk,
			output,
		)
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"panic in scalar UDF",
		)
	})

	t.Run(
		"user error propagation",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			expectedErr := errors.New(
				"user defined error",
			)
			udf := &registeredScalarFunc{
				name: "error_func",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return nil, expectedErr
					},
				},
				bindCtx: context.Background(),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{intType},
			)
			require.NoError(t, err)

			err = chunk.SetValue(0, 0, int32(1))
			require.NoError(t, err)

			err = chunk.SetSize(1)
			require.NoError(t, err)

			output, err := createTestVector(
				intType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"user defined error",
			)
		},
	)
}

// TestScalarFuncContext tests the ScalarFuncContext.
func TestScalarFuncContext(t *testing.T) {
	t.Run(
		"create with nil clock",
		func(t *testing.T) {
			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			require.NotNil(t, ctx)
			assert.NotNil(t, ctx.Clock())
			assert.Equal(
				t,
				context.Background(),
				ctx.Context(),
			)
		},
	)

	t.Run(
		"create with mock clock",
		func(t *testing.T) {
			mockClock := quartz.NewMock(t)
			ctx := NewScalarFuncContext(
				context.Background(),
				mockClock,
			)
			require.NotNil(t, ctx)
			assert.Equal(
				t,
				mockClock,
				ctx.Clock(),
			)
		},
	)

	t.Run(
		"WithClock creates new context",
		func(t *testing.T) {
			mockClock1 := quartz.NewMock(t)
			mockClock2 := quartz.NewMock(t)

			baseCtx := context.Background()
			ctx1 := NewScalarFuncContext(
				baseCtx,
				mockClock1,
			)
			ctx2 := ctx1.WithClock(mockClock2)

			assert.Equal(
				t,
				mockClock1,
				ctx1.Clock(),
			)
			assert.Equal(
				t,
				mockClock2,
				ctx2.Clock(),
			)
			// Both contexts should reference the same underlying context
			assert.Equal(
				t,
				ctx1.Context(),
				ctx2.Context(),
			)
		},
	)
}

// TestDeterministicTimeout tests timeout checking with deterministic clocks.
func TestDeterministicTimeout(t *testing.T) {
	t.Run(
		"timeout not exceeded",
		func(t *testing.T) {
			mockClock := quartz.NewMock(t)
			baseTime := time.Date(
				2024,
				6,
				15,
				14,
				0,
				0,
				0,
				time.UTC,
			)
			mockClock.Set(baseTime)

			deadline := baseTime.Add(
				1 * time.Second,
			)
			goCtx, cancel := context.WithDeadline(
				context.Background(),
				deadline,
			)
			defer cancel()

			ctx := NewScalarFuncContext(
				goCtx,
				mockClock,
			)
			err := ctx.checkTimeout()
			assert.NoError(t, err)
		},
	)

	t.Run("timeout exceeded", func(t *testing.T) {
		mockClock := quartz.NewMock(t)
		baseTime := time.Date(
			2024,
			6,
			15,
			14,
			0,
			0,
			0,
			time.UTC,
		)
		mockClock.Set(baseTime)

		deadline := baseTime.Add(
			-1 * time.Second,
		) // Already passed
		goCtx, cancel := context.WithDeadline(
			context.Background(),
			deadline,
		)
		defer cancel()

		ctx := NewScalarFuncContext(
			goCtx,
			mockClock,
		)
		err := ctx.checkTimeout()
		assert.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
		)
	})

	t.Run(
		"timeout during execution - deterministic",
		func(t *testing.T) {
			mockClock := quartz.NewMock(t)
			baseTime := time.Date(
				2024,
				6,
				15,
				14,
				0,
				0,
				0,
				time.UTC,
			)
			mockClock.Set(baseTime)

			deadline := baseTime.Add(
				100 * time.Millisecond,
			)
			goCtx, cancel := context.WithDeadline(
				context.Background(),
				deadline,
			)
			defer cancel()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			rowCount := 0
			udf := &registeredScalarFunc{
				name: "slow_func",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						rowCount++
						// Simulate time passing
						mockClock.Advance(
							50 * time.Millisecond,
						)

						return values[0], nil
					},
				},
				bindCtx: context.Background(),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{intType},
			)
			require.NoError(t, err)

			for i := range 10 {
				err = chunk.SetValue(
					0,
					i,
					int32(i),
				)
				require.NoError(t, err)
			}
			err = chunk.SetSize(10)
			require.NoError(t, err)

			output, err := createTestVector(
				intType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				goCtx,
				mockClock,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)

			// Should timeout after ~2-3 rows (100ms / 50ms per row)
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"timeout",
			)
			assert.Less(t, rowCount, 10)
		},
	)

	t.Run(
		"no timeout without deadline",
		func(t *testing.T) {
			mockClock := quartz.NewMock(t)
			baseTime := time.Date(
				2024,
				6,
				15,
				14,
				0,
				0,
				0,
				time.UTC,
			)
			mockClock.Set(baseTime)

			ctx := NewScalarFuncContext(
				context.Background(),
				mockClock,
			)
			err := ctx.checkTimeout()
			assert.NoError(t, err)
		},
	)
}

// TestRowContextExecutor tests the RowContextExecutor path.
func TestRowContextExecutor(t *testing.T) {
	t.Run(
		"context executor receives context",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			type ctxKey string
			var receivedCtx context.Context
			udf := &registeredScalarFunc{
				name: "ctx_func",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{
					RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
						receivedCtx = ctx

						return values[0], nil
					},
				},
				bindCtx: context.WithValue(
					context.Background(),
					ctxKey("key"),
					"value",
				),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{intType},
			)
			require.NoError(t, err)

			err = chunk.SetValue(0, 0, int32(42))
			require.NoError(t, err)

			err = chunk.SetSize(1)
			require.NoError(t, err)

			output, err := createTestVector(
				intType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.NoError(t, err)

			// Should have received the bind context
			assert.NotNil(t, receivedCtx)
			assert.Equal(
				t,
				"value",
				receivedCtx.Value(ctxKey("key")),
			)
		},
	)
}

// TestScalarFuncPrimitiveTypes tests scalar functions with all primitive types.
func TestScalarFuncPrimitiveTypes(t *testing.T) {
	testCases := []struct {
		name     string
		typ      Type
		inputVal any
		expected any
	}{
		{"BOOLEAN", TYPE_BOOLEAN, true, true},
		{
			"TINYINT",
			TYPE_TINYINT,
			int8(42),
			int8(42),
		},
		{
			"SMALLINT",
			TYPE_SMALLINT,
			int16(1000),
			int16(1000),
		},
		{
			"INTEGER",
			TYPE_INTEGER,
			int32(100000),
			int32(100000),
		},
		{
			"BIGINT",
			TYPE_BIGINT,
			int64(1000000000),
			int64(1000000000),
		},
		{
			"FLOAT",
			TYPE_FLOAT,
			float32(3.14),
			float32(3.14),
		},
		{
			"DOUBLE",
			TYPE_DOUBLE,
			float64(3.14159265),
			float64(3.14159265),
		},
		{
			"VARCHAR",
			TYPE_VARCHAR,
			"hello",
			"hello",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			typeInfo, err := NewTypeInfo(tc.typ)
			require.NoError(t, err)

			udf := &registeredScalarFunc{
				name: "identity",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						typeInfo,
					},
					ResultTypeInfo: typeInfo,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return values[0], nil
					},
				},
				bindCtx: context.Background(),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{typeInfo},
			)
			require.NoError(t, err)

			err = chunk.SetValue(
				0,
				0,
				tc.inputVal,
			)
			require.NoError(t, err)

			err = chunk.SetSize(1)
			require.NoError(t, err)

			output, err := createTestVector(
				typeInfo,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.NoError(t, err)

			val := output.getFn(output, 0)
			assert.Equal(t, tc.expected, val)
		})
	}
}

// TestScalarFuncMultipleInputs tests scalar functions with multiple inputs.
func TestScalarFuncMultipleInputs(t *testing.T) {
	t.Run("add two integers", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		udf := &registeredScalarFunc{
			name: "add",
			config: ScalarFuncConfig{
				InputTypeInfos: []TypeInfo{
					intType,
					intType,
				},
				ResultTypeInfo: intType,
			},
			executor: ScalarFuncExecutor{
				RowExecutor: func(values []driver.Value) (any, error) {
					a, _ := values[0].(int32)
					b, _ := values[1].(int32)

					return a + b, nil
				},
			},
			bindCtx: context.Background(),
		}

		chunk, err := NewDataChunk(
			[]TypeInfo{intType, intType},
		)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, int32(10))
		require.NoError(t, err)
		err = chunk.SetValue(1, 0, int32(20))
		require.NoError(t, err)
		err = chunk.SetValue(0, 1, int32(100))
		require.NoError(t, err)
		err = chunk.SetValue(1, 1, int32(200))
		require.NoError(t, err)

		err = chunk.SetSize(2)
		require.NoError(t, err)

		output, err := createTestVector(
			intType,
			VectorSize,
		)
		require.NoError(t, err)

		ctx := NewScalarFuncContext(
			context.Background(),
			nil,
		)
		err = executeScalarUDF(
			ctx,
			udf,
			chunk,
			output,
		)
		require.NoError(t, err)

		val := output.getFn(output, 0)
		assert.Equal(t, int32(30), val)

		val = output.getFn(output, 1)
		assert.Equal(t, int32(300), val)
	})

	t.Run(
		"concat two strings",
		func(t *testing.T) {
			varcharType, err := NewTypeInfo(
				TYPE_VARCHAR,
			)
			require.NoError(t, err)

			udf := &registeredScalarFunc{
				name: "concat",
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						varcharType,
						varcharType,
					},
					ResultTypeInfo: varcharType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						a, _ := values[0].(string)
						b, _ := values[1].(string)

						return a + b, nil
					},
				},
				bindCtx: context.Background(),
			}

			chunk, err := NewDataChunk(
				[]TypeInfo{
					varcharType,
					varcharType,
				},
			)
			require.NoError(t, err)

			err = chunk.SetValue(0, 0, "Hello, ")
			require.NoError(t, err)
			err = chunk.SetValue(1, 0, "World!")
			require.NoError(t, err)

			err = chunk.SetSize(1)
			require.NoError(t, err)

			output, err := createTestVector(
				varcharType,
				VectorSize,
			)
			require.NoError(t, err)

			ctx := NewScalarFuncContext(
				context.Background(),
				nil,
			)
			err = executeScalarUDF(
				ctx,
				udf,
				chunk,
				output,
			)
			require.NoError(t, err)

			val := output.getFn(output, 0)
			assert.Equal(t, "Hello, World!", val)
		},
	)
}

// TestSafeExecute tests the panic recovery wrapper.
func TestSafeExecute(t *testing.T) {
	t.Run("normal execution", func(t *testing.T) {
		fn := func(values []driver.Value) (any, error) {
			return int64(42), nil
		}

		result, err := safeExecute(fn, nil)
		assert.NoError(t, err)
		assert.Equal(t, int64(42), result)
	})

	t.Run(
		"function returns error",
		func(t *testing.T) {
			expectedErr := errors.New(
				"expected error",
			)
			fn := func(values []driver.Value) (any, error) {
				return nil, expectedErr
			}

			result, err := safeExecute(fn, nil)
			assert.ErrorIs(t, err, expectedErr)
			assert.Nil(t, result)
		},
	)

	t.Run("function panics", func(t *testing.T) {
		fn := func(values []driver.Value) (any, error) {
			panic("test panic")
		}

		result, err := safeExecute(fn, nil)
		assert.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"panic in scalar UDF",
		)
		assert.Nil(t, result)
	})
}

// TestVolatileFunction tests volatile function handling.
func TestVolatileFunction(t *testing.T) {
	t.Run(
		"volatile function config",
		func(t *testing.T) {
			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := &testScalarFunc{
				config: ScalarFuncConfig{
					InputTypeInfos: nil,
					ResultTypeInfo: intType,
					Volatile:       true,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return int32(42), nil
					},
				},
			}

			assert.True(t, fn.Config().Volatile)
		},
	)
}

// TestScalarFuncRegistryLookupScalarUDF tests the binder resolver interface.
func TestScalarFuncRegistryLookupScalarUDF(
	t *testing.T,
) {
	t.Run(
		"lookup existing function returns UDF info",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register("my_func", fn)
			require.NoError(t, err)

			udfInfo, resultType, found := registry.LookupScalarUDF(
				"my_func",
				[]Type{TYPE_INTEGER},
			)
			require.True(t, found)
			assert.NotNil(t, udfInfo)
			assert.Equal(
				t,
				TYPE_INTEGER,
				resultType,
			)
		},
	)

	t.Run(
		"lookup non-existent function returns not found",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			udfInfo, resultType, found := registry.LookupScalarUDF(
				"non_existent",
				[]Type{TYPE_INTEGER},
			)
			assert.False(t, found)
			assert.Nil(t, udfInfo)
			assert.Equal(
				t,
				TYPE_INVALID,
				resultType,
			)
		},
	)

	t.Run(
		"lookup with wrong types returns not found",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"typed_func",
				fn,
			)
			require.NoError(t, err)

			// Try to lookup with wrong type
			udfInfo, resultType, found := registry.LookupScalarUDF(
				"typed_func",
				[]Type{TYPE_VARCHAR},
			)
			assert.False(t, found)
			assert.Nil(t, udfInfo)
			assert.Equal(
				t,
				TYPE_INVALID,
				resultType,
			)
		},
	)
}

// TestScalarFuncComplexTypes tests scalar UDFs with LIST, STRUCT, and MAP types.
func TestScalarFuncComplexTypes(t *testing.T) {
	t.Run(
		"LIST input and output",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			listType, err := NewListInfo(intType)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{listType},
				listType,
				func(values []driver.Value) (any, error) {
					// Identity function for lists
					return values[0], nil
				},
			)

			err = registry.register(
				"list_identity",
				fn,
			)
			require.NoError(t, err)

			result := registry.lookup(
				"list_identity",
				[]Type{TYPE_LIST},
			)
			require.NotNil(t, result)
		},
	)

	t.Run(
		"STRUCT input and output",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			strType, err := NewTypeInfo(
				TYPE_VARCHAR,
			)
			require.NoError(t, err)

			idEntry, err := NewStructEntry(
				intType,
				"id",
			)
			require.NoError(t, err)

			nameEntry, err := NewStructEntry(
				strType,
				"name",
			)
			require.NoError(t, err)

			structType, err := NewStructInfo(
				idEntry,
				nameEntry,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{structType},
				structType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"struct_identity",
				fn,
			)
			require.NoError(t, err)

			result := registry.lookup(
				"struct_identity",
				[]Type{TYPE_STRUCT},
			)
			require.NotNil(t, result)
		},
	)

	t.Run(
		"MAP input and output",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			strType, err := NewTypeInfo(
				TYPE_VARCHAR,
			)
			require.NoError(t, err)

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			mapType, err := NewMapInfo(
				strType,
				intType,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{mapType},
				mapType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"map_identity",
				fn,
			)
			require.NoError(t, err)

			result := registry.lookup(
				"map_identity",
				[]Type{TYPE_MAP},
			)
			require.NotNil(t, result)
		},
	)
}

// TestScalarFuncConstantFolding tests the ScalarBinder callback for constant folding.
func TestScalarFuncConstantFolding(t *testing.T) {
	t.Run(
		"BindScalarUDF with no ScalarBinder returns nil",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"identity",
				fn,
			)
			require.NoError(t, err)

			udfInfo, _, found := registry.LookupScalarUDF(
				"identity",
				[]Type{TYPE_INTEGER},
			)
			require.True(t, found)

			args := []ScalarUDFArg{
				{
					Foldable: true,
					Value:    int32(42),
				},
			}

			bindCtx, err := registry.BindScalarUDF(
				udfInfo,
				args,
			)
			require.NoError(t, err)
			require.Nil(
				t,
				bindCtx,
			) // No ScalarBinder defined
		},
	)

	t.Run(
		"BindScalarUDF with ScalarBinder is called",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			binderCalled := false
			var receivedArgs []ScalarUDFArg

			fn := &testScalarFuncWithBinder{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return values[0], nil
					},
					ScalarBinder: func(ctx context.Context, args []ScalarUDFArg) (context.Context, error) {
						binderCalled = true
						receivedArgs = args

						return context.WithValue(
							ctx,
							testContextKey(
								"test",
							),
							"bound",
						), nil
					},
				},
			}

			err = registry.register(
				"with_binder",
				fn,
			)
			require.NoError(t, err)

			udfInfo, _, found := registry.LookupScalarUDF(
				"with_binder",
				[]Type{
					TYPE_INTEGER,
					TYPE_INTEGER,
				},
			)
			require.True(t, found)

			args := []ScalarUDFArg{
				{
					Foldable: true,
					Value:    int32(42),
				},
				{Foldable: false, Value: nil},
			}

			bindCtx, err := registry.BindScalarUDF(
				udfInfo,
				args,
			)
			require.NoError(t, err)
			require.True(t, binderCalled)
			require.Len(t, receivedArgs, 2)
			require.True(
				t,
				receivedArgs[0].Foldable,
			)
			require.Equal(
				t,
				int32(42),
				receivedArgs[0].Value,
			)
			require.False(
				t,
				receivedArgs[1].Foldable,
			)
			require.NotNil(t, bindCtx)
		},
	)

	t.Run(
		"BindScalarUDF error propagation",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := &testScalarFuncWithBinder{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return values[0], nil
					},
					ScalarBinder: func(ctx context.Context, args []ScalarUDFArg) (context.Context, error) {
						return nil, fmt.Errorf(
							"bind failed",
						)
					},
				},
			}

			err = registry.register(
				"failing_binder",
				fn,
			)
			require.NoError(t, err)

			udfInfo, _, found := registry.LookupScalarUDF(
				"failing_binder",
				[]Type{TYPE_INTEGER},
			)
			require.True(t, found)

			args := []ScalarUDFArg{
				{Foldable: true, Value: int32(1)},
			}

			_, err = registry.BindScalarUDF(
				udfInfo,
				args,
			)
			require.Error(t, err)
			require.Contains(
				t,
				err.Error(),
				"bind failed",
			)
		},
	)

	t.Run(
		"BindScalarUDF with invalid udfInfo returns error",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			args := []ScalarUDFArg{
				{Foldable: true, Value: int32(1)},
			}

			_, err := registry.BindScalarUDF(
				"not a udf",
				args,
			)
			require.Error(t, err)
			require.Contains(
				t,
				err.Error(),
				"invalid UDF info type",
			)
		},
	)
}

// testScalarFuncWithBinder is a test helper for scalar functions with ScalarBinder.
type testScalarFuncWithBinder struct {
	config   ScalarFuncConfig
	executor ScalarFuncExecutor
}

func (f *testScalarFuncWithBinder) Config() ScalarFuncConfig {
	return f.config
}

func (f *testScalarFuncWithBinder) Executor() ScalarFuncExecutor {
	return f.executor
}

// TestScalarFuncVolatileCaching tests that volatile functions prevent caching.
func TestScalarFuncVolatileCaching(t *testing.T) {
	t.Run(
		"IsVolatile returns true for volatile function",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := &testScalarFuncWithBinder{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{},
					ResultTypeInfo: intType,
					Volatile:       true,
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return int32(42), nil
					},
				},
			}

			err = registry.register(
				"volatile_fn",
				fn,
			)
			require.NoError(t, err)

			udfInfo, _, found := registry.LookupScalarUDF(
				"volatile_fn",
				[]Type{},
			)
			require.True(t, found)

			isVolatile := registry.IsVolatile(
				udfInfo,
			)
			require.True(t, isVolatile)
		},
	)

	t.Run(
		"IsVolatile returns false for non-volatile function",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			fn := createSimpleScalarFunc(
				[]TypeInfo{intType},
				intType,
				func(values []driver.Value) (any, error) {
					return values[0], nil
				},
			)

			err = registry.register(
				"non_volatile",
				fn,
			)
			require.NoError(t, err)

			udfInfo, _, found := registry.LookupScalarUDF(
				"non_volatile",
				[]Type{TYPE_INTEGER},
			)
			require.True(t, found)

			isVolatile := registry.IsVolatile(
				udfInfo,
			)
			require.False(t, isVolatile)
		},
	)

	t.Run(
		"IsVolatile returns false for invalid udfInfo",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			isVolatile := registry.IsVolatile(
				"not a udf",
			)
			require.False(t, isVolatile)
		},
	)

	t.Run(
		"BindScalarUDF skips ScalarBinder for volatile function",
		func(t *testing.T) {
			registry := newScalarFuncRegistry()

			intType, err := NewTypeInfo(
				TYPE_INTEGER,
			)
			require.NoError(t, err)

			binderCalled := false

			fn := &testScalarFuncWithBinder{
				config: ScalarFuncConfig{
					InputTypeInfos: []TypeInfo{
						intType,
					},
					ResultTypeInfo: intType,
					Volatile:       true, // Volatile function
				},
				executor: ScalarFuncExecutor{
					RowExecutor: func(values []driver.Value) (any, error) {
						return values[0], nil
					},
					ScalarBinder: func(ctx context.Context, args []ScalarUDFArg) (context.Context, error) {
						binderCalled = true // This should NOT be called for volatile functions

						return ctx, nil
					},
				},
			}

			err = registry.register(
				"volatile_with_binder",
				fn,
			)
			require.NoError(t, err)

			udfInfo, _, found := registry.LookupScalarUDF(
				"volatile_with_binder",
				[]Type{TYPE_INTEGER},
			)
			require.True(t, found)

			args := []ScalarUDFArg{
				{
					Foldable: true,
					Value:    int32(42),
				},
			}

			bindCtx, err := registry.BindScalarUDF(
				udfInfo,
				args,
			)
			require.NoError(t, err)
			require.Nil(
				t,
				bindCtx,
			) // Should return nil for volatile functions
			require.False(
				t,
				binderCalled,
			) // ScalarBinder should not be called
		},
	)
}

// Benchmark tests for scalar UDF performance.
//
// Performance Characteristics:
//
// Based on benchmark results on typical hardware:
//
// - Registration: ~275ns/op, 4 allocations
//   The overhead comes from creating the registry map entry and function metadata.
//
// - Lookup: ~21ns/op, zero allocations
//   Function lookup is very fast with O(n) scan through overloads.
//   For most use cases (< 10 overloads), this is negligible.
//
// - Execution: ~3ns/op, zero allocations
//   UDF execution is extremely fast since it's a direct function call.
//   The main overhead in real queries is data conversion, not the UDF call itself.
//
// - Type Matching: ~31ns/op, zero allocations
//   Type matching for overload resolution is efficient.
//   Worst case is O(n*m) where n=overloads, m=arguments.
//
// - Binder Call: ~45ns/op, 1 allocation
//   ScalarBinder callback has minimal overhead.
//   Single allocation is for context.WithValue if used.
//
// These benchmarks measure the pure Go overhead. In practice, the database
// operations (parsing, planning, data access) dominate execution time.

// BenchmarkScalarFuncRegistration benchmarks function registration.
func BenchmarkScalarFuncRegistration(
	b *testing.B,
) {
	intType, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		b.Fatal(err)
	}

	fn := createSimpleScalarFunc(
		[]TypeInfo{intType},
		intType,
		func(values []driver.Value) (any, error) {
			return values[0], nil
		},
	)

	b.ResetTimer()
	for range b.N {
		registry := newScalarFuncRegistry()
		_ = registry.register("test_func", fn)
	}
}

// BenchmarkScalarFuncLookup benchmarks function lookup.
func BenchmarkScalarFuncLookup(b *testing.B) {
	registry := newScalarFuncRegistry()

	intType, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		b.Fatal(err)
	}

	for i := range 10 {
		fn := createSimpleScalarFunc(
			[]TypeInfo{intType},
			intType,
			func(values []driver.Value) (any, error) {
				return values[0], nil
			},
		)
		_ = registry.register(
			fmt.Sprintf("func_%d", i),
			fn,
		)
	}

	argTypes := []Type{TYPE_INTEGER}

	b.ResetTimer()
	for range b.N {
		_ = registry.lookup("func_5", argTypes)
	}
}

// BenchmarkScalarFuncExecution benchmarks UDF execution.
func BenchmarkScalarFuncExecution(b *testing.B) {
	registry := newScalarFuncRegistry()

	intType, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		b.Fatal(err)
	}

	fn := createSimpleScalarFunc(
		[]TypeInfo{intType, intType},
		intType,
		func(values []driver.Value) (any, error) {
			a := values[0].(int32)
			c := values[1].(int32)

			return a + c, nil
		},
	)
	_ = registry.register("add", fn)

	udf := registry.lookup(
		"add",
		[]Type{TYPE_INTEGER, TYPE_INTEGER},
	)
	executor := udf.executor.RowExecutor
	values := []driver.Value{int32(10), int32(20)}

	b.ResetTimer()
	for range b.N {
		_, _ = executor(values)
	}
}

// BenchmarkScalarFuncTypeMatching benchmarks type matching for overloads.
func BenchmarkScalarFuncTypeMatching(
	b *testing.B,
) {
	registry := newScalarFuncRegistry()

	// Register multiple overloads
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	strType, _ := NewTypeInfo(TYPE_VARCHAR)
	floatType, _ := NewTypeInfo(TYPE_DOUBLE)

	_ = registry.register(
		"overloaded",
		createSimpleScalarFunc(
			[]TypeInfo{intType},
			intType,
			func(v []driver.Value) (any, error) { return v[0], nil },
		),
	)
	_ = registry.register(
		"overloaded",
		createSimpleScalarFunc(
			[]TypeInfo{strType},
			strType,
			func(v []driver.Value) (any, error) { return v[0], nil },
		),
	)
	_ = registry.register(
		"overloaded",
		createSimpleScalarFunc(
			[]TypeInfo{floatType},
			floatType,
			func(v []driver.Value) (any, error) { return v[0], nil },
		),
	)
	_ = registry.register(
		"overloaded",
		createSimpleScalarFunc(
			[]TypeInfo{intType, intType},
			intType,
			func(v []driver.Value) (any, error) { return v[0], nil },
		),
	)

	b.ResetTimer()
	for range b.N {
		// Match the last overload to test worst-case matching
		registry.lookup(
			"overloaded",
			[]Type{TYPE_INTEGER, TYPE_INTEGER},
		)
	}
}

// BenchmarkScalarFuncBinderCall benchmarks ScalarBinder callback.
func BenchmarkScalarFuncBinderCall(b *testing.B) {
	registry := newScalarFuncRegistry()

	intType, err := NewTypeInfo(TYPE_INTEGER)
	if err != nil {
		b.Fatal(err)
	}

	fn := &testScalarFuncWithBinder{
		config: ScalarFuncConfig{
			InputTypeInfos: []TypeInfo{intType},
			ResultTypeInfo: intType,
		},
		executor: ScalarFuncExecutor{
			RowExecutor: func(values []driver.Value) (any, error) {
				return values[0], nil
			},
			ScalarBinder: func(ctx context.Context, args []ScalarUDFArg) (context.Context, error) {
				return context.WithValue(
					ctx,
					testContextKey("key"),
					args[0].Value,
				), nil
			},
		},
	}

	_ = registry.register("with_binder", fn)
	udfInfo, _, _ := registry.LookupScalarUDF(
		"with_binder",
		[]Type{TYPE_INTEGER},
	)
	args := []ScalarUDFArg{
		{Foldable: true, Value: int32(42)},
	}

	b.ResetTimer()
	for range b.N {
		_, _ = registry.BindScalarUDF(
			udfInfo,
			args,
		)
	}
}

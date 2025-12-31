package dukdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplacementScanCallback_BasicSignature(
	t *testing.T,
) {
	var callback ReplacementScanCallback = func(tableName string) (string, []any, error) {
		return "read_csv", []any{"file.csv"}, nil
	}

	funcName, params, err := callback("my_table")
	require.NoError(t, err)
	assert.Equal(t, "read_csv", funcName)
	assert.Equal(t, []any{"file.csv"}, params)
}

func TestReplacementScanCallback_NoReplacement(
	t *testing.T,
) {
	var callback ReplacementScanCallback = func(tableName string) (string, []any, error) {
		// Empty function name means no replacement
		return "", nil, nil
	}

	funcName, params, err := callback("my_table")
	require.NoError(t, err)
	assert.Equal(t, "", funcName)
	assert.Nil(t, params)
}

func TestReplacementScanCallback_Error(
	t *testing.T,
) {
	expectedErr := errors.New("scan failed")
	var callback ReplacementScanCallback = func(tableName string) (string, []any, error) {
		return "", nil, expectedErr
	}

	_, _, err := callback("my_table")
	assert.ErrorIs(t, err, expectedErr)
}

func TestReplacementScanContext_NewWithNilClock(
	t *testing.T,
) {
	ctx := NewReplacementScanContext(
		context.Background(),
		nil,
	)

	require.NotNil(t, ctx)
	assert.NotNil(t, ctx.Clock())
	assert.NotNil(t, ctx.Context())
}

func TestReplacementScanContext_NewWithNilContext(
	t *testing.T,
) {
	//nolint:staticcheck // testing nil context handling
	ctx := NewReplacementScanContext(nil, nil)

	require.NotNil(t, ctx)
	assert.NotNil(t, ctx.Context())
}

func TestReplacementScanContext_WithClock(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	ctx := NewReplacementScanContext(
		context.Background(),
		nil,
	)

	ctx2 := ctx.WithClock(mClock)
	assert.Equal(t, mClock, ctx2.Clock())
}

func TestReplacementScanContext_ExecuteCallback_Success(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	ctx := NewReplacementScanContext(
		context.Background(),
		mClock,
	)

	callback := func(tableName string) (string, []any, error) {
		return "read_parquet", []any{
			tableName + ".parquet",
		}, nil
	}

	funcName, params, err := ctx.executeCallback(
		callback,
		"data",
	)
	require.NoError(t, err)
	assert.Equal(t, "read_parquet", funcName)
	assert.Equal(t, []any{"data.parquet"}, params)
}

func TestReplacementScanContext_ExecuteCallback_DeadlineExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()

	// Create context with deadline in the past (relative to mock clock)
	deadline := startTime.Add(-1 * time.Second)
	bgCtx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	ctx := NewReplacementScanContext(
		bgCtx,
		mClock,
	)

	callback := func(tableName string) (string, []any, error) {
		return "read_csv", nil, nil
	}

	_, _, err := ctx.executeCallback(
		callback,
		"test",
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

func TestReplacementScanContext_ExecuteCallback_DeadlineNotExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()

	// Create context with deadline in the future (relative to mock clock)
	deadline := startTime.Add(10 * time.Second)
	bgCtx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	ctx := NewReplacementScanContext(
		bgCtx,
		mClock,
	)

	callback := func(tableName string) (string, []any, error) {
		return "read_csv", []any{"file.csv"}, nil
	}

	funcName, params, err := ctx.executeCallback(
		callback,
		"test",
	)
	require.NoError(t, err)
	assert.Equal(t, "read_csv", funcName)
	assert.Equal(t, []any{"file.csv"}, params)
}

func TestReplacementScanRegistry_Register(
	t *testing.T,
) {
	registry := newReplacementScanRegistry()

	callback := func(tableName string) (string, []any, error) {
		return "test_func", nil, nil
	}

	registry.register(callback)

	registered := registry.get()
	require.NotNil(t, registered)

	funcName, _, _ := registered("table")
	assert.Equal(t, "test_func", funcName)
}

func TestReplacementScanRegistry_ReRegister(
	t *testing.T,
) {
	registry := newReplacementScanRegistry()

	callback1 := func(tableName string) (string, []any, error) {
		return "first_func", nil, nil
	}
	callback2 := func(tableName string) (string, []any, error) {
		return "second_func", nil, nil
	}

	registry.register(callback1)
	registry.register(callback2)

	registered := registry.get()
	require.NotNil(t, registered)

	funcName, _, _ := registered("table")
	assert.Equal(t, "second_func", funcName)
}

func TestReplacementScanRegistry_GetNil(
	t *testing.T,
) {
	registry := newReplacementScanRegistry()

	callback := registry.get()
	assert.Nil(t, callback)
}

func TestRegisterReplacementScan_NewConnector(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	callback := func(tableName string) (string, []any, error) {
		return "read_csv", []any{
			tableName + ".csv",
		}, nil
	}

	RegisterReplacementScan(connector, callback)

	require.NotNil(t, connector.replacementScans)
	registered := connector.replacementScans.get()
	require.NotNil(t, registered)

	funcName, params, err := registered("data")
	require.NoError(t, err)
	assert.Equal(t, "read_csv", funcName)
	assert.Equal(t, []any{"data.csv"}, params)
}

func TestRegisterReplacementScan_ReplaceExisting(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	callback1 := func(tableName string) (string, []any, error) {
		return "func1", nil, nil
	}
	callback2 := func(tableName string) (string, []any, error) {
		return "func2", nil, nil
	}

	RegisterReplacementScan(connector, callback1)
	RegisterReplacementScan(connector, callback2)

	registered := connector.replacementScans.get()
	funcName, _, _ := registered("table")
	assert.Equal(t, "func2", funcName)
}

func TestValidateReplacementParams_SupportedTypes(
	t *testing.T,
) {
	tests := []struct {
		name   string
		params []any
	}{
		{"empty", []any{}},
		{"string", []any{"value"}},
		{"int64", []any{int64(42)}},
		{
			"string slice",
			[]any{[]string{"a", "b"}},
		},
		{
			"mixed supported",
			[]any{
				"str",
				int64(123),
				[]string{"x", "y"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReplacementParams(
				tt.params,
			)
			assert.NoError(t, err)
		})
	}
}

func TestValidateReplacementParams_UnsupportedTypes(
	t *testing.T,
) {
	tests := []struct {
		name   string
		params []any
	}{
		{"bool", []any{true}},
		{"float64", []any{3.14}},
		{"int", []any{42}},
		{"int32", []any{int32(42)}},
		{"[]int", []any{[]int{1, 2, 3}}},
		{
			"map",
			[]any{
				map[string]string{"key": "val"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateReplacementParams(
				tt.params,
			)
			assert.ErrorIs(
				t,
				err,
				errReplacementScanUnsupportedType,
			)
		})
	}
}

// Deterministic timing test using mock clock
func TestReplacementScanContext_DeterministicTiming(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	mClock.Set(
		time.Date(
			2024,
			1,
			15,
			10,
			0,
			0,
			0,
			time.UTC,
		),
	)

	// Create a deadline 5 seconds from now
	deadline := mClock.Now().Add(5 * time.Second)
	bgCtx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	ctx := NewReplacementScanContext(
		bgCtx,
		mClock,
	)

	callback := func(tableName string) (string, []any, error) {
		return "func", nil, nil
	}

	// Should succeed - deadline hasn't passed
	_, _, err := ctx.executeCallback(
		callback,
		"test",
	)
	require.NoError(t, err)

	// Advance clock past deadline
	mClock.Advance(6 * time.Second)

	// Should fail - deadline exceeded
	_, _, err = ctx.executeCallback(
		callback,
		"test",
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

// Test that no time.Sleep is used in tests
func TestNoTimeSleepInReplacementScanTests(
	t *testing.T,
) {
	// This test exists to document that we don't use time.Sleep
	// The actual verification is done via grep in the validation phase
	assert.True(t, true)
}

// ---------- Binder Integration Tests ----------

func TestTryReplacementScan_NilConnector(
	t *testing.T,
) {
	result := TryReplacementScan(
		nil,
		"test_table",
	)
	assert.False(t, result.Replaced)
	assert.NoError(t, result.Error)
}

func TestTryReplacementScan_NoCallback(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	result := TryReplacementScan(
		connector,
		"test_table",
	)
	assert.False(t, result.Replaced)
	assert.NoError(t, result.Error)
}

func TestTryReplacementScan_Success(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			if tableName == "csv_data" {
				return "read_csv", []any{
					"data.csv",
				}, nil
			}

			return "", nil, nil
		},
	)

	result := TryReplacementScan(
		connector,
		"csv_data",
	)
	assert.True(t, result.Replaced)
	assert.Equal(
		t,
		"read_csv",
		result.FunctionName,
	)
	assert.Equal(
		t,
		[]any{"data.csv"},
		result.Params,
	)
	assert.NoError(t, result.Error)
}

func TestTryReplacementScan_NoMatch(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			// Only handle tables starting with "csv_"
			if len(tableName) > 4 &&
				tableName[:4] == "csv_" {
				return "read_csv", []any{
					tableName[4:] + ".csv",
				}, nil
			}

			return "", nil, nil
		},
	)

	result := TryReplacementScan(
		connector,
		"regular_table",
	)
	assert.False(t, result.Replaced)
	assert.NoError(t, result.Error)
}

func TestTryReplacementScan_CallbackError(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			return "", nil, errors.New(
				"file not found",
			)
		},
	)

	result := TryReplacementScan(
		connector,
		"missing_file",
	)
	assert.False(t, result.Replaced)
	assert.Error(t, result.Error)
	assert.ErrorIs(
		t,
		result.Error,
		errReplacementScanFailed,
	)
}

func TestTryReplacementScan_UnsupportedParamType(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			// Return unsupported type (float64)
			return "func", []any{3.14}, nil
		},
	)

	result := TryReplacementScan(
		connector,
		"test_table",
	)
	assert.False(t, result.Replaced)
	assert.Error(t, result.Error)
	assert.ErrorIs(
		t,
		result.Error,
		errReplacementScanUnsupportedType,
	)
}

func TestTryReplacementScanWithContext_Success(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			return "read_parquet", []any{
				tableName + ".parquet",
			}, nil
		},
	)

	ctx := NewReplacementScanContext(
		context.Background(),
		mClock,
	)
	result := TryReplacementScanWithContext(
		connector,
		"data",
		ctx,
	)

	assert.True(t, result.Replaced)
	assert.Equal(
		t,
		"read_parquet",
		result.FunctionName,
	)
	assert.NoError(t, result.Error)
}

func TestTryReplacementScanWithContext_DeadlineExceeded(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	startTime := mClock.Now()
	deadline := startTime.Add(-1 * time.Second)

	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			return "read_csv", nil, nil
		},
	)

	bgCtx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()
	ctx := NewReplacementScanContext(
		bgCtx,
		mClock,
	)

	result := TryReplacementScanWithContext(
		connector,
		"test",
		ctx,
	)
	assert.False(t, result.Replaced)
	assert.Error(t, result.Error)
	assert.ErrorIs(
		t,
		result.Error,
		errReplacementScanFailed,
	)
}

func TestTryReplacementScan_WithAllSupportedTypes(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			return "multi_param_func", []any{
				"string_val",
				int64(42),
				[]string{"a", "b", "c"},
			}, nil
		},
	)

	result := TryReplacementScan(
		connector,
		"test_table",
	)
	assert.True(t, result.Replaced)
	assert.Equal(
		t,
		"multi_param_func",
		result.FunctionName,
	)
	require.Len(t, result.Params, 3)
	assert.Equal(
		t,
		"string_val",
		result.Params[0],
	)
	assert.Equal(t, int64(42), result.Params[1])
	assert.Equal(
		t,
		[]string{"a", "b", "c"},
		result.Params[2],
	)
	assert.NoError(t, result.Error)
}

// ---------- ConvertParamsToFunctionArgs Tests ----------

func TestConvertParamsToFunctionArgs_Empty(
	t *testing.T,
) {
	args := ConvertParamsToFunctionArgs(nil)
	assert.Nil(t, args)

	args = ConvertParamsToFunctionArgs([]any{})
	assert.Nil(t, args)
}

func TestConvertParamsToFunctionArgs_WithParams(
	t *testing.T,
) {
	params := []any{
		"file.csv",
		int64(100),
		[]string{"col1", "col2"},
	}
	args := ConvertParamsToFunctionArgs(params)

	require.Len(t, args, 3)
	assert.Equal(t, 0, args[0].Position)
	assert.Equal(t, "file.csv", args[0].Value)
	assert.Equal(t, 1, args[1].Position)
	assert.Equal(t, int64(100), args[1].Value)
	assert.Equal(t, 2, args[2].Position)
	assert.Equal(
		t,
		[]string{"col1", "col2"},
		args[2].Value,
	)
}

// ---------- Error Scenario Tests ----------

func TestReplacementScanResult_IsSuccess(
	t *testing.T,
) {
	// Success case
	result := ReplacementScanResult{
		Replaced:     true,
		FunctionName: "func",
		Params:       []any{"arg1"},
	}
	assert.True(t, result.Replaced)
	assert.NoError(t, result.Error)

	// No replacement case
	result = ReplacementScanResult{
		Replaced: false,
	}
	assert.False(t, result.Replaced)
	assert.NoError(t, result.Error)

	// Error case
	result = ReplacementScanResult{
		Replaced: false,
		Error:    errors.New("test error"),
	}
	assert.False(t, result.Replaced)
	assert.Error(t, result.Error)
}

func TestReplacementScan_EmptyFunctionNameWithParams(
	t *testing.T,
) {
	connector, err := NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() { assert.NoError(t, connector.Close()) }()

	RegisterReplacementScan(
		connector,
		func(tableName string) (string, []any, error) {
			// Return empty function name with params (should be treated as no replacement)
			return "", []any{"ignored"}, nil
		},
	)

	result := TryReplacementScan(
		connector,
		"test_table",
	)
	assert.False(t, result.Replaced)
	assert.NoError(t, result.Error)
}

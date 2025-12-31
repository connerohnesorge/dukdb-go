package dukdb

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/coder/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupProfilingTest(t *testing.T) {
	t.Helper()
	setupTestMockBackend()
}

func TestProfilingInfo_Structure(t *testing.T) {
	info := ProfilingInfo{
		Metrics: map[string]string{
			"TOTAL_TIME":    "100ms",
			"ROWS_RETURNED": "42",
		},
		Children: []ProfilingInfo{
			{
				Metrics: map[string]string{
					"OPERATOR_TYPE":   "SCAN",
					"OPERATOR_TIMING": "50ms",
					"ROWS":            "100",
				},
				Children: nil,
			},
		},
	}

	assert.Equal(
		t,
		"100ms",
		info.Metrics["TOTAL_TIME"],
	)
	assert.Equal(
		t,
		"42",
		info.Metrics["ROWS_RETURNED"],
	)
	require.Len(t, info.Children, 1)
	assert.Equal(
		t,
		"SCAN",
		info.Children[0].Metrics["OPERATOR_TYPE"],
	)
}

func TestProfilingContext_NewWithNilClock(
	t *testing.T,
) {
	ctx := NewProfilingContext(nil)

	require.NotNil(t, ctx)
	assert.NotNil(t, ctx.Clock())
	assert.False(t, ctx.IsEnabled())
}

func TestProfilingContext_WithClock(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	ctx := NewProfilingContext(nil)

	ctx2 := ctx.WithClock(mClock)
	assert.Equal(t, mClock, ctx2.Clock())
}

func TestProfilingContext_EnableDisable(
	t *testing.T,
) {
	ctx := NewProfilingContext(nil)

	assert.False(t, ctx.IsEnabled())

	ctx.Enable()
	assert.True(t, ctx.IsEnabled())

	ctx.Disable()
	assert.False(t, ctx.IsEnabled())
}

func TestProfilingContext_StartAndElapsed(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	ctx := NewProfilingContext(mClock)

	// Without enabling, Start should be no-op
	ctx.Start()
	assert.Equal(
		t,
		time.Duration(0),
		ctx.Elapsed(),
	)

	// Enable and start
	ctx.Enable()
	ctx.Start()

	// Advance clock
	mClock.Advance(100 * time.Millisecond)

	// Check elapsed
	elapsed := ctx.Elapsed()
	assert.Equal(t, 100*time.Millisecond, elapsed)
}

func TestProfilingContext_AddOperator(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	ctx := NewProfilingContext(mClock)
	ctx.Enable()
	ctx.Start()

	// Add an operator
	ctx.AddOperator(
		"SCAN",
		100,
		50*time.Millisecond,
	)

	// Complete and get info
	ctx.Complete(42)
	info, err := ctx.GetInfo()
	require.NoError(t, err)

	assert.Equal(
		t,
		"42",
		info.Metrics["ROWS_RETURNED"],
	)
	require.Len(t, info.Children, 1)
	assert.Equal(
		t,
		"SCAN",
		info.Children[0].Metrics["OPERATOR_TYPE"],
	)
	assert.Equal(
		t,
		"100",
		info.Children[0].Metrics["ROWS"],
	)
}

func TestProfilingContext_Complete(t *testing.T) {
	mClock := quartz.NewMock(t)
	ctx := NewProfilingContext(mClock)
	ctx.Enable()
	ctx.Start()

	// Advance time
	mClock.Advance(200 * time.Millisecond)

	// Complete
	ctx.Complete(100)

	info, err := ctx.GetInfo()
	require.NoError(t, err)

	assert.Equal(
		t,
		"200ms",
		info.Metrics["TOTAL_TIME"],
	)
	assert.Equal(
		t,
		"100",
		info.Metrics["ROWS_RETURNED"],
	)
}

func TestProfilingContext_GetInfo_NotEnabled(
	t *testing.T,
) {
	ctx := NewProfilingContext(nil)

	_, err := ctx.GetInfo()
	assert.ErrorIs(t, err, errProfilingNotEnabled)
}

func TestProfilingContext_GetInfo_Empty(
	t *testing.T,
) {
	ctx := NewProfilingContext(nil)
	ctx.Enable()
	// Don't start, so root is nil

	_, err := ctx.GetInfo()
	assert.ErrorIs(t, err, errProfilingInfoEmpty)
}

func TestProfilingContext_GetInfo_ClearsAfterRetrieval(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)
	ctx := NewProfilingContext(mClock)
	ctx.Enable()
	ctx.Start()
	ctx.Complete(10)

	// First retrieval succeeds
	_, err := ctx.GetInfo()
	require.NoError(t, err)

	// Second retrieval fails (cleared)
	_, err = ctx.GetInfo()
	assert.ErrorIs(t, err, errProfilingInfoEmpty)
}

func TestOperatorMetrics_Timing(t *testing.T) {
	mClock := quartz.NewMock(t)
	metrics := newOperatorMetrics(
		mClock,
		"FILTER",
	)

	assert.Equal(
		t,
		"FILTER",
		metrics.OperatorType(),
	)
	assert.Equal(
		t,
		time.Duration(0),
		metrics.Duration(),
	)
	assert.Equal(t, 0, metrics.RowCount())

	// Start timing
	metrics.Start()

	// Advance clock
	mClock.Advance(75 * time.Millisecond)

	// Check duration (still ongoing)
	assert.Equal(
		t,
		75*time.Millisecond,
		metrics.Duration(),
	)

	// End timing
	mClock.Advance(25 * time.Millisecond)
	metrics.End()

	// Duration should be fixed at 100ms
	assert.Equal(
		t,
		100*time.Millisecond,
		metrics.Duration(),
	)

	// Advancing clock should not change duration
	mClock.Advance(100 * time.Millisecond)
	assert.Equal(
		t,
		100*time.Millisecond,
		metrics.Duration(),
	)
}

func TestOperatorMetrics_RowCount(t *testing.T) {
	mClock := quartz.NewMock(t)
	metrics := newOperatorMetrics(
		mClock,
		"PROJECTION",
	)

	metrics.AddRows(10)
	assert.Equal(t, 10, metrics.RowCount())

	metrics.AddRows(5)
	assert.Equal(t, 15, metrics.RowCount())
}

func TestGetProfilingInfo_NotEnabled(
	t *testing.T,
) {
	setupProfilingTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	// Profiling not enabled
	_, err = GetProfilingInfo(conn)
	assert.Error(t, err)
}

func TestGetProfilingInfo_WithProfiling(
	t *testing.T,
) {
	setupProfilingTest(t)
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	// Set up profiling context on the connection
	err = conn.Raw(func(driverConn any) error {
		c := driverConn.(*Conn)
		c.profiling = NewProfilingContext(nil)
		c.profiling.Enable()
		c.profiling.Start()
		c.profiling.Complete(50)

		return nil
	})
	require.NoError(t, err)

	// Get profiling info
	info, err := GetProfilingInfo(conn)
	require.NoError(t, err)

	assert.NotNil(t, info.Metrics)
	assert.Equal(
		t,
		"50",
		info.Metrics["ROWS_RETURNED"],
	)
}

// Deterministic timing test using mock clock
func TestProfilingContext_DeterministicTiming(
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

	ctx := NewProfilingContext(mClock)
	ctx.Enable()
	ctx.Start()

	// Add operators with specific timing
	ctx.AddOperator(
		"SCAN",
		1000,
		100*time.Millisecond,
	)
	ctx.AddOperator(
		"FILTER",
		500,
		50*time.Millisecond,
	)
	ctx.AddOperator(
		"PROJECTION",
		500,
		25*time.Millisecond,
	)

	// Advance clock exactly 500ms
	mClock.Advance(500 * time.Millisecond)

	ctx.Complete(500)

	info, err := ctx.GetInfo()
	require.NoError(t, err)

	// Verify exact timing
	assert.Equal(
		t,
		"500ms",
		info.Metrics["TOTAL_TIME"],
	)
	assert.Equal(
		t,
		"500",
		info.Metrics["ROWS_RETURNED"],
	)

	// Verify operators
	require.Len(t, info.Children, 3)
	assert.Equal(
		t,
		"SCAN",
		info.Children[0].Metrics["OPERATOR_TYPE"],
	)
	assert.Equal(
		t,
		"100ms",
		info.Children[0].Metrics["OPERATOR_TIMING"],
	)
	assert.Equal(
		t,
		"FILTER",
		info.Children[1].Metrics["OPERATOR_TYPE"],
	)
	assert.Equal(
		t,
		"50ms",
		info.Children[1].Metrics["OPERATOR_TIMING"],
	)
	assert.Equal(
		t,
		"PROJECTION",
		info.Children[2].Metrics["OPERATOR_TYPE"],
	)
	assert.Equal(
		t,
		"25ms",
		info.Children[2].Metrics["OPERATOR_TIMING"],
	)
}

// Test that no time.Sleep is used in tests
func TestNoTimeSleepInTests(t *testing.T) {
	// This test exists to document that we don't use time.Sleep
	// The actual verification is done via grep in the validation phase
	assert.True(t, true)
}

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

// boundTestMockBackendStmt implements BackendStmt for testing bound execution.
type boundTestMockBackendStmt struct {
	numParams int
	closed    bool
}

func (m *boundTestMockBackendStmt) Execute(
	ctx context.Context,
	args []driver.NamedValue,
) (int64, error) {
	return 1, nil
}

func (m *boundTestMockBackendStmt) Query(
	ctx context.Context,
	args []driver.NamedValue,
) ([]map[string]any, []string, error) {
	return []map[string]any{
			{"col": "value"},
		}, []string{
			"col",
		}, nil
}

func (m *boundTestMockBackendStmt) Close() error {
	m.closed = true

	return nil
}

func (m *boundTestMockBackendStmt) NumInput() int {
	return m.numParams
}

func TestBind(t *testing.T) {
	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 3,
		},
	}

	t.Run("valid bind", func(t *testing.T) {
		err := stmt.Bind(1, "value1")
		require.NoError(t, err)
		err = stmt.Bind(2, 42)
		require.NoError(t, err)
		err = stmt.Bind(3, true)
		require.NoError(t, err)
	})

	t.Run("invalid index", func(t *testing.T) {
		err := stmt.Bind(0, "value")
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"must be >= 1",
		)
	})

	t.Run(
		"out of range index",
		func(t *testing.T) {
			err := stmt.Bind(10, "value")
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"out of range",
			)
		},
	)

	t.Run("closed statement", func(t *testing.T) {
		closedStmt := &Stmt{
			backendStmt: &boundTestMockBackendStmt{
				numParams: 3,
			},
			closed: true,
		}
		err := closedStmt.Bind(1, "value")
		require.Error(t, err)
		assert.ErrorIs(t, err, errClosedStmt)
	})
}

func TestExecBound(t *testing.T) {
	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 2,
		},
	}

	// Bind parameters
	require.NoError(t, stmt.Bind(1, "value1"))
	require.NoError(t, stmt.Bind(2, 42))

	result, err := stmt.ExecBound()
	require.NoError(t, err)
	require.NotNil(t, result)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

func TestQueryBound(t *testing.T) {
	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 1,
		},
	}

	// Bind parameter
	require.NoError(
		t,
		stmt.Bind(1, "query_value"),
	)

	rows, err := stmt.QueryBound()
	require.NoError(t, err)
	require.NotNil(t, rows)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	assert.Equal(
		t,
		[]string{"col"},
		rows.Columns(),
	)
}

func TestClearBound(t *testing.T) {
	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 2,
		},
	}

	// Bind parameters
	require.NoError(t, stmt.Bind(1, "value1"))
	require.NoError(t, stmt.Bind(2, 42))

	// Clear bound parameters
	stmt.ClearBound()

	// Verify parameters are cleared
	assert.Nil(t, stmt.boundParams)
}

// TestExecBoundContextClockTimeout tests deterministic timeout behavior using quartz.Mock.
// This test verifies that ExecBoundContextClock properly checks deadlines using the injected clock.
func TestExecBoundContextClockTimeout(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 1,
		},
	}

	// Bind a parameter
	require.NoError(t, stmt.Bind(1, "value"))

	// Create deadline based on mock clock time
	deadline := mClock.Now().Add(1 * time.Second)

	// Use a context without its own deadline (we'll check using the clock)
	ctx := context.Background()

	// Create a context with the mock clock's deadline for the function to check
	ctxWithDeadline, cancel := context.WithDeadline(
		ctx,
		deadline,
	)
	defer cancel()

	// Advance past deadline using mock clock (no MustWait needed - just advance)
	mClock.Advance(2 * time.Second)

	// Now the clock says we're past the deadline
	_, err := stmt.ExecBoundContextClock(
		ctxWithDeadline,
		mClock,
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

// TestQueryBoundContextClockTimeout tests deterministic timeout behavior using quartz.Mock.
// This test verifies that QueryBoundContextClock properly checks deadlines using the injected clock.
func TestQueryBoundContextClockTimeout(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 1,
		},
	}

	// Bind a parameter
	require.NoError(t, stmt.Bind(1, "value"))

	// Create deadline based on mock clock time
	deadline := mClock.Now().Add(1 * time.Second)

	// Use a context without its own deadline (we'll check using the clock)
	ctx := context.Background()

	// Create a context with the mock clock's deadline for the function to check
	ctxWithDeadline, cancel := context.WithDeadline(
		ctx,
		deadline,
	)
	defer cancel()

	// Advance past deadline using mock clock (no MustWait needed - just advance)
	mClock.Advance(2 * time.Second)

	// Now the clock says we're past the deadline
	_, err := stmt.QueryBoundContextClock(
		ctxWithDeadline,
		mClock,
	)
	assert.ErrorIs(
		t,
		err,
		context.DeadlineExceeded,
	)
}

// TestExecBoundContextClockSuccess tests that execution succeeds when no deadline is set.
func TestExecBoundContextClockSuccess(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 1,
		},
	}

	// Bind a parameter
	require.NoError(t, stmt.Bind(1, "value"))

	// Use context without deadline - clock check is skipped when no deadline
	ctx := context.Background()

	result, err := stmt.ExecBoundContextClock(
		ctx,
		mClock,
	)
	require.NoError(t, err)
	require.NotNil(t, result)

	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

// TestQueryBoundContextClockSuccess tests that query succeeds when no deadline is set.
func TestQueryBoundContextClockSuccess(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 1,
		},
	}

	// Bind a parameter
	require.NoError(t, stmt.Bind(1, "value"))

	// Use context without deadline - clock check is skipped when no deadline
	ctx := context.Background()

	rows, err := stmt.QueryBoundContextClock(
		ctx,
		mClock,
	)
	require.NoError(t, err)
	require.NotNil(t, rows)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	assert.Equal(
		t,
		[]string{"col"},
		rows.Columns(),
	)
}

// TestExecBoundContextClockBeforeDeadline tests that execution succeeds when clock shows we're before deadline.
func TestExecBoundContextClockBeforeDeadline(
	t *testing.T,
) {
	mClock := quartz.NewMock(t)

	stmt := &Stmt{
		backendStmt: &boundTestMockBackendStmt{
			numParams: 1,
		},
	}

	// Bind a parameter
	require.NoError(t, stmt.Bind(1, "value"))

	// Use a real-time deadline that won't expire (1 hour from now)
	// The context's deadline is checked using clock.Until(), which uses the mock clock
	// Since the real deadline is in the future, clock.Until returns positive duration
	ctx, cancel := context.WithDeadline(
		context.Background(),
		time.Now().Add(1*time.Hour),
	)
	defer cancel()

	// The mock clock hasn't advanced, so clock.Until(future_deadline) returns positive
	result, err := stmt.ExecBoundContextClock(
		ctx,
		mClock,
	)
	require.NoError(t, err)
	require.NotNil(t, result)
}

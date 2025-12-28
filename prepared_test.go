package dukdb

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCountPlaceholders tests the countPlaceholders function
func TestCountPlaceholders(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected int
	}{
		// No placeholders
		{
			name:     "no placeholders",
			query:    "SELECT * FROM users",
			expected: 0,
		},

		// Positional placeholders - simple cases
		{
			name:     "single positional $1",
			query:    "SELECT * FROM users WHERE id = $1",
			expected: 1,
		},
		{
			name:     "sequential positional $1, $2",
			query:    "SELECT * FROM users WHERE id = $1 AND name = $2",
			expected: 2,
		},
		{
			name:     "gap in sequence $1, $3 returns max 3",
			query:    "SELECT * FROM users WHERE id = $1 AND status = $3",
			expected: 3,
		},
		{
			name:     "duplicate positional $1, $1 returns 1",
			query:    "SELECT * FROM users WHERE id = $1 OR parent_id = $1",
			expected: 1,
		},
		{
			name:     "large gap $1, $10 returns 10",
			query:    "SELECT $1, $10",
			expected: 10,
		},

		// Named placeholders
		{
			name:     "single named @foo",
			query:    "SELECT * FROM users WHERE id = @foo",
			expected: 1,
		},
		{
			name:     "two unique named @foo, @bar",
			query:    "SELECT * FROM users WHERE id = @foo AND name = @bar",
			expected: 2,
		},
		{
			name:     "duplicate named @foo, @foo returns 1",
			query:    "SELECT * FROM users WHERE id = @foo OR parent_id = @foo",
			expected: 1,
		},
		{
			name:     "three unique named @a, @b, @c",
			query:    "SELECT @a, @b, @c",
			expected: 3,
		},

		// Mixed placeholders
		{
			name:     "mixed $1, @foo returns sum 2",
			query:    "SELECT * FROM users WHERE id = $1 AND name = @foo",
			expected: 2,
		},
		{
			name:     "mixed $2, @bar returns sum 3",
			query:    "SELECT $2, @bar",
			expected: 3,
		},

		// Placeholders inside string literals (should be ignored)
		{
			name:     "positional inside string literal ignored",
			query:    "SELECT * FROM users WHERE name = 'test$1'",
			expected: 0,
		},
		{
			name:     "named inside string literal ignored",
			query:    "SELECT * FROM users WHERE email = 'user@example.com'",
			expected: 0,
		},
		{
			name:     "placeholder outside string counted",
			query:    "SELECT * FROM users WHERE name = 'hello' AND id = $1",
			expected: 1,
		},
		{
			name:     "escaped quote with placeholder outside",
			query:    "SELECT * FROM users WHERE name = 'it''s ok' AND id = $1",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countPlaceholders(tt.query)
			assert.Equal(
				t,
				tt.expected,
				result,
				"countPlaceholders(%q)",
				tt.query,
			)
		})
	}
}

// TestPreparedStmtNumInput tests the NumInput method
func TestPreparedStmtNumInput(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{"no params", "SELECT 1", 0},
		{"one positional", "SELECT $1", 1},
		{"two positional", "SELECT $1, $2", 2},
		{"gap in positional", "SELECT $1, $3", 3},
		{"one named", "SELECT @foo", 1},
		{"two named", "SELECT @foo, @bar", 2},
		{
			"duplicate named",
			"SELECT @foo, @foo",
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				tt.query,
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				tt.expected,
				stmt.NumInput(),
			)
		})
	}
}

// TestPreparedStmtClose tests the Close method
func TestPreparedStmtClose(t *testing.T) {
	t.Run("close once", func(t *testing.T) {
		conn := &Conn{closed: false}
		stmt, err := conn.PreparePreparedStmt(
			"SELECT $1",
		)
		require.NoError(t, err)

		// First close should succeed
		err = stmt.Close()
		assert.NoError(t, err)
	})

	t.Run(
		"close is idempotent",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Multiple closes should all succeed (idempotent)
			err = stmt.Close()
			assert.NoError(t, err)

			err = stmt.Close()
			assert.NoError(t, err)

			err = stmt.Close()
			assert.NoError(t, err)
		},
	)
}

// TestPreparedStmtClosedError tests that closed statement returns proper errors
func TestPreparedStmtClosedError(t *testing.T) {
	t.Run(
		"ExecContext on closed statement",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			err = stmt.Close()
			require.NoError(t, err)

			_, err = stmt.ExecContext(
				context.Background(),
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeClosed,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"statement is closed",
			)
		},
	)

	t.Run(
		"QueryContext on closed statement",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			err = stmt.Close()
			require.NoError(t, err)

			_, err = stmt.QueryContext(
				context.Background(),
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeClosed,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"statement is closed",
			)
		},
	)
}

// TestPreparedStmtConnectionClosed tests connection closed error handling
func TestPreparedStmtConnectionClosed(
	t *testing.T,
) {
	t.Run(
		"Prepare on closed connection",
		func(t *testing.T) {
			conn := &Conn{closed: true}
			_, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeConnection,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"connection is closed",
			)
		},
	)

	t.Run(
		"ExecContext with closed connection",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Close the connection after preparing
			conn.closed = true

			_, err = stmt.ExecContext(
				context.Background(),
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeConnection,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"connection is closed",
			)
		},
	)

	t.Run(
		"QueryContext with closed connection",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Close the connection after preparing
			conn.closed = true

			_, err = stmt.QueryContext(
				context.Background(),
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeConnection,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"connection is closed",
			)
		},
	)
}

// TestPreparedStmtParameterValidation tests parameter count validation
func TestPreparedStmtParameterValidation(
	t *testing.T,
) {
	t.Run(
		"too few parameters for ExecContext",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1, $2, $3",
			)
			require.NoError(t, err)
			assert.Equal(t, 3, stmt.NumInput())

			// Provide only 2 parameters when 3 are expected
			_, err = stmt.ExecContext(
				context.Background(),
				[]driver.NamedValue{
					{Ordinal: 1, Value: 1},
					{Ordinal: 2, Value: 2},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeInvalid,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"expected 3 parameter(s), got 2",
			)
		},
	)

	t.Run(
		"too many parameters for ExecContext",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, stmt.NumInput())

			// Provide 3 parameters when only 1 is expected
			_, err = stmt.ExecContext(
				context.Background(),
				[]driver.NamedValue{
					{Ordinal: 1, Value: 1},
					{Ordinal: 2, Value: 2},
					{Ordinal: 3, Value: 3},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeInvalid,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"expected 1 parameter(s), got 3",
			)
		},
	)

	t.Run(
		"too few parameters for QueryContext",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1, $2",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, stmt.NumInput())

			// Provide 0 parameters when 2 are expected
			_, err = stmt.QueryContext(
				context.Background(),
				[]driver.NamedValue{},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeInvalid,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"expected 2 parameter(s), got 0",
			)
		},
	)

	t.Run(
		"zero parameters when none expected",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT 1",
			)
			require.NoError(t, err)
			assert.Equal(t, 0, stmt.NumInput())

			// We can only verify parameter count validation here.
			// Without a mock backend, we can't test actual execution.
			// The fact that NumInput() returns 0 and we create the statement
			// successfully means parameter counting works for zero params.
		},
	)

	t.Run(
		"correct parameters for named params",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT @foo, @bar",
			)
			require.NoError(t, err)
			assert.Equal(t, 2, stmt.NumInput())

			// Verify that the statement expects 2 parameters
			// and test with wrong count to verify validation
			_, err = stmt.QueryContext(
				context.Background(),
				[]driver.NamedValue{
					{Name: "foo", Value: 1},
				},
			)
			require.Error(t, err)

			duckErr, ok := err.(*Error)
			require.True(
				t,
				ok,
				"expected *Error type",
			)
			assert.Equal(
				t,
				ErrorTypeInvalid,
				duckErr.Type,
			)
			assert.Contains(
				t,
				duckErr.Msg,
				"expected 2 parameter(s), got 1",
			)
		},
	)
}

// TestPreparedStmtContextCancellation tests context cancellation handling
func TestPreparedStmtContextCancellation(
	t *testing.T,
) {
	t.Run(
		"ExecContext with cancelled context",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Create an already-cancelled context
			ctx, cancel := context.WithCancel(
				context.Background(),
			)
			cancel()

			_, err = stmt.ExecContext(
				ctx,
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)
			assert.Equal(t, context.Canceled, err)
		},
	)

	t.Run(
		"QueryContext with cancelled context",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Create an already-cancelled context
			ctx, cancel := context.WithCancel(
				context.Background(),
			)
			cancel()

			_, err = stmt.QueryContext(
				ctx,
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)
			assert.Equal(t, context.Canceled, err)
		},
	)

	t.Run(
		"ExecContext with deadline exceeded",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Create a context that's already past its deadline
			ctx, cancel := context.WithDeadline(
				context.Background(),
				time.Now().Add(-time.Second),
			)
			defer cancel()

			_, err = stmt.ExecContext(
				ctx,
				[]driver.NamedValue{
					{Ordinal: 1, Value: 42},
				},
			)
			require.Error(t, err)
			assert.Equal(
				t,
				context.DeadlineExceeded,
				err,
			)
		},
	)
}

// TestPreparedStmtConcurrentExecution tests thread safety
func TestPreparedStmtConcurrentExecution(
	t *testing.T,
) {
	t.Run(
		"concurrent NumInput calls",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1, $2, $3",
			)
			require.NoError(t, err)

			var wg sync.WaitGroup
			results := make([]int, 100)

			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					results[idx] = stmt.NumInput()
				}(i)
			}

			wg.Wait()

			// All results should be 3
			for i, r := range results {
				assert.Equal(
					t,
					3,
					r,
					"NumInput call %d returned wrong value",
					i,
				)
			}
		},
	)

	t.Run(
		"concurrent Close calls",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			var wg sync.WaitGroup
			errors := make([]error, 100)

			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					errors[idx] = stmt.Close()
				}(i)
			}

			wg.Wait()

			// All Close calls should succeed (idempotent)
			for i, e := range errors {
				assert.NoError(
					t,
					e,
					"Close call %d returned error",
					i,
				)
			}
		},
	)

	t.Run(
		"concurrent ExecContext after close",
		func(t *testing.T) {
			conn := &Conn{closed: false}
			stmt, err := conn.PreparePreparedStmt(
				"SELECT $1",
			)
			require.NoError(t, err)

			// Close the statement first
			err = stmt.Close()
			require.NoError(t, err)

			var wg sync.WaitGroup
			errors := make([]error, 100)

			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					_, errors[idx] = stmt.ExecContext(
						context.Background(),
						[]driver.NamedValue{
							{
								Ordinal: 1,
								Value:   42,
							},
						},
					)
				}(i)
			}

			wg.Wait()

			// All ExecContext calls should return "statement is closed" error
			for i, e := range errors {
				require.Error(
					t,
					e,
					"ExecContext call %d should have errored",
					i,
				)
				duckErr, ok := e.(*Error)
				require.True(
					t,
					ok,
					"expected *Error type for call %d",
					i,
				)
				assert.Equal(
					t,
					ErrorTypeClosed,
					duckErr.Type,
				)
			}
		},
	)
}

// TestPreparedStmtDriverInterfaces verifies interface compliance
func TestPreparedStmtDriverInterfaces(
	t *testing.T,
) {
	// These compile-time checks verify interface implementation
	var _ driver.Stmt = (*PreparedStmt)(nil)
	var _ driver.StmtExecContext = (*PreparedStmt)(nil)
	var _ driver.StmtQueryContext = (*PreparedStmt)(nil)
}

// TestPreparedStmtExecDeprecated tests the deprecated Exec method
func TestPreparedStmtExecDeprecated(
	t *testing.T,
) {
	conn := &Conn{closed: false}
	stmt, err := conn.PreparePreparedStmt(
		"SELECT $1",
	)
	require.NoError(t, err)

	// Close immediately to test error handling through deprecated method
	err = stmt.Close()
	require.NoError(t, err)

	_, err = stmt.Exec([]driver.Value{42})
	require.Error(t, err)

	duckErr, ok := err.(*Error)
	require.True(t, ok, "expected *Error type")
	assert.Equal(t, ErrorTypeClosed, duckErr.Type)
}

// TestPreparedStmtQueryDeprecated tests the deprecated Query method
func TestPreparedStmtQueryDeprecated(
	t *testing.T,
) {
	conn := &Conn{closed: false}
	stmt, err := conn.PreparePreparedStmt(
		"SELECT $1",
	)
	require.NoError(t, err)

	// Close immediately to test error handling through deprecated method
	err = stmt.Close()
	require.NoError(t, err)

	_, err = stmt.Query([]driver.Value{42})
	require.Error(t, err)

	duckErr, ok := err.(*Error)
	require.True(t, ok, "expected *Error type")
	assert.Equal(t, ErrorTypeClosed, duckErr.Type)
}

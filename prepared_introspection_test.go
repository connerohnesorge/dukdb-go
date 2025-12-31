package dukdb

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPreparedStmtStatementType tests the StatementType() method
func TestPreparedStmtStatementType(t *testing.T) {
	conn := &Conn{} // Mock connection for testing

	tests := []struct {
		name     string
		query    string
		expected StmtType
	}{
		{"SELECT", "SELECT * FROM users", STATEMENT_TYPE_SELECT},
		{"INSERT", "INSERT INTO users (name) VALUES ($1)", STATEMENT_TYPE_INSERT},
		{"UPDATE", "UPDATE users SET name = $1", STATEMENT_TYPE_UPDATE},
		{"DELETE", "DELETE FROM users WHERE id = $1", STATEMENT_TYPE_DELETE},
		{"CREATE", "CREATE TABLE test (id INT)", STATEMENT_TYPE_CREATE},
		{"DROP", "DROP TABLE test", STATEMENT_TYPE_DROP},
		{"WITH CTE", "WITH cte AS (SELECT 1) SELECT * FROM cte", STATEMENT_TYPE_SELECT},
		{"EXPLAIN", "EXPLAIN SELECT * FROM users", STATEMENT_TYPE_EXPLAIN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := conn.PreparePreparedStmt(tt.query)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, stmt.StatementType())
		})
	}
}

// TestPreparedStmtParamCount tests the ParamCount() method
func TestPreparedStmtParamCount(t *testing.T) {
	conn := &Conn{}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{"no params", "SELECT * FROM users", 0},
		{"single positional", "SELECT * FROM users WHERE id = $1", 1},
		{"multiple positional", "SELECT * FROM users WHERE id = $1 AND name = $2", 2},
		{"gap in positional", "SELECT * FROM users WHERE id = $1 AND name = $3", 3},
		{"single named", "SELECT * FROM users WHERE id = @userId", 1},
		{"multiple named", "SELECT * FROM users WHERE id = @userId AND name = @userName", 2},
		{"repeated named", "SELECT * FROM users WHERE id = @userId OR parent_id = @userId", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := conn.PreparePreparedStmt(tt.query)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, stmt.ParamCount())
			assert.Equal(t, stmt.NumInput(), stmt.ParamCount()) // Should be aliases
		})
	}
}

// TestPreparedStmtParamName tests the ParamName() method
func TestPreparedStmtParamName(t *testing.T) {
	conn := &Conn{}

	t.Run("positional parameters", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1, $2, $3")
		require.NoError(t, err)

		name0, err := stmt.ParamName(0)
		require.NoError(t, err)
		assert.Equal(t, "1", name0)

		name1, err := stmt.ParamName(1)
		require.NoError(t, err)
		assert.Equal(t, "2", name1)

		name2, err := stmt.ParamName(2)
		require.NoError(t, err)
		assert.Equal(t, "3", name2)
	})

	t.Run("named parameters", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT * FROM users WHERE id = @userId AND name = @userName")
		require.NoError(t, err)

		name0, err := stmt.ParamName(0)
		require.NoError(t, err)
		assert.Equal(t, "userId", name0)

		name1, err := stmt.ParamName(1)
		require.NoError(t, err)
		assert.Equal(t, "userName", name1)
	})

	t.Run("out of range error", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1")
		require.NoError(t, err)

		_, err = stmt.ParamName(-1)
		assert.Error(t, err)

		_, err = stmt.ParamName(1) // Only index 0 exists
		assert.Error(t, err)
	})

	t.Run("no params returns error for any index", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT 1")
		require.NoError(t, err)

		_, err = stmt.ParamName(0)
		assert.Error(t, err)
	})
}

// TestPreparedStmtBind tests the Bind() method
func TestPreparedStmtBind(t *testing.T) {
	conn := &Conn{}

	t.Run("bind positional parameters", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1, $2")
		require.NoError(t, err)

		err = stmt.Bind(0, 42)
		require.NoError(t, err)

		err = stmt.Bind(1, "hello")
		require.NoError(t, err)

		// Verify bound values
		assert.Equal(t, 1, stmt.boundParams[0].Ordinal)
		assert.Equal(t, 42, stmt.boundParams[0].Value)
		assert.Equal(t, 2, stmt.boundParams[1].Ordinal)
		assert.Equal(t, "hello", stmt.boundParams[1].Value)
	})

	t.Run("bind named parameters", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT @userId, @userName")
		require.NoError(t, err)

		err = stmt.Bind(0, 42)
		require.NoError(t, err)

		err = stmt.Bind(1, "alice")
		require.NoError(t, err)

		// Verify bound values - named params use Name instead of Ordinal
		assert.Equal(t, "userId", stmt.boundParams[0].Name)
		assert.Equal(t, 42, stmt.boundParams[0].Value)
		assert.Equal(t, "userName", stmt.boundParams[1].Name)
		assert.Equal(t, "alice", stmt.boundParams[1].Value)
	})

	t.Run("bind out of range returns error", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1")
		require.NoError(t, err)

		err = stmt.Bind(-1, 42)
		assert.Error(t, err)

		err = stmt.Bind(1, 42) // Only index 0 is valid
		assert.Error(t, err)
	})

	t.Run("bind closed statement returns error", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1")
		require.NoError(t, err)

		err = stmt.Close()
		require.NoError(t, err)

		err = stmt.Bind(0, 42)
		assert.Error(t, err)
	})

	t.Run("bind nil value", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1")
		require.NoError(t, err)

		err = stmt.Bind(0, nil)
		require.NoError(t, err)
		assert.Nil(t, stmt.boundParams[0].Value)
	})
}

// TestPreparedStmtClearBindings tests the ClearBindings() method
func TestPreparedStmtClearBindings(t *testing.T) {
	conn := &Conn{}

	stmt, err := conn.PreparePreparedStmt("SELECT $1, $2")
	require.NoError(t, err)

	// Bind values
	err = stmt.Bind(0, 42)
	require.NoError(t, err)
	err = stmt.Bind(1, "hello")
	require.NoError(t, err)

	// Clear bindings
	stmt.ClearBindings()

	// Verify bindings are cleared
	assert.Equal(t, 0, stmt.boundParams[0].Ordinal)
	assert.Equal(t, "", stmt.boundParams[0].Name)
	assert.Nil(t, stmt.boundParams[0].Value)
	assert.Equal(t, 0, stmt.boundParams[1].Ordinal)
	assert.Equal(t, "", stmt.boundParams[1].Name)
	assert.Nil(t, stmt.boundParams[1].Value)
}

// TestPreparedStmtAllParamsBound tests the internal allParamsBound() method
func TestPreparedStmtAllParamsBound(t *testing.T) {
	conn := &Conn{}

	t.Run("no params returns true", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT 1")
		require.NoError(t, err)
		assert.True(t, stmt.allParamsBound())
	})

	t.Run("unbound params returns false", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1, $2")
		require.NoError(t, err)
		assert.False(t, stmt.allParamsBound())
	})

	t.Run("partially bound returns false", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1, $2")
		require.NoError(t, err)

		err = stmt.Bind(0, 42)
		require.NoError(t, err)

		assert.False(t, stmt.allParamsBound())
	})

	t.Run("all bound returns true", func(t *testing.T) {
		stmt, err := conn.PreparePreparedStmt("SELECT $1, $2")
		require.NoError(t, err)

		err = stmt.Bind(0, 42)
		require.NoError(t, err)
		err = stmt.Bind(1, "hello")
		require.NoError(t, err)

		assert.True(t, stmt.allParamsBound())
	})
}

// TestExecBoundWithoutParams tests ExecBound with no parameters
// NOTE: This test requires the real execution engine which can't be imported
// in the root package due to import cycles. See internal/engine/engine_test.go
// for comprehensive integration tests with TestBoundExecution.
func TestExecBoundWithoutParams(t *testing.T) {
	t.Skip("integration test - see internal/engine/engine_test.go TestBoundExecution")

	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)
	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	dukConn := conn.(*Conn)
	ctx := context.Background()

	// Create table
	_, err = dukConn.ExecContext(ctx, "CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Prepare and execute without params
	stmt, err := dukConn.PreparePreparedStmt("INSERT INTO test VALUES (1, 'alice')")
	require.NoError(t, err)
	defer stmt.Close()

	result, err := stmt.ExecBound(ctx)
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

// TestExecBoundWithParams tests ExecBound with parameters
// NOTE: This test requires the real execution engine which can't be imported
// in the root package due to import cycles. See internal/engine/engine_test.go
// for comprehensive integration tests with TestBoundExecution.
func TestExecBoundWithParams(t *testing.T) {
	t.Skip("integration test - see internal/engine/engine_test.go TestBoundExecution")

	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)
	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	dukConn := conn.(*Conn)
	ctx := context.Background()

	// Create table
	_, err = dukConn.ExecContext(ctx, "CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Prepare with parameters
	stmt, err := dukConn.PreparePreparedStmt("INSERT INTO test VALUES ($1, $2)")
	require.NoError(t, err)
	defer stmt.Close()

	// Bind parameters
	err = stmt.Bind(0, 1)
	require.NoError(t, err)
	err = stmt.Bind(1, "alice")
	require.NoError(t, err)

	// Execute
	result, err := stmt.ExecBound(ctx)
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

// TestQueryBoundWithParams tests QueryBound with parameters
// NOTE: This test requires the real execution engine which can't be imported
// in the root package due to import cycles. See internal/engine/engine_test.go
// for comprehensive integration tests with TestBoundExecution.
func TestQueryBoundWithParams(t *testing.T) {
	t.Skip("integration test - see internal/engine/engine_test.go TestBoundExecution")

	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)
	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	dukConn := conn.(*Conn)
	ctx := context.Background()

	// Create and populate table
	_, err = dukConn.ExecContext(ctx, "CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = dukConn.ExecContext(ctx, "INSERT INTO test VALUES (1, 'alice'), (2, 'bob')", nil)
	require.NoError(t, err)

	// Prepare with parameter
	stmt, err := dukConn.PreparePreparedStmt("SELECT name FROM test WHERE id = $1")
	require.NoError(t, err)
	defer stmt.Close()

	// Bind and query
	err = stmt.Bind(0, 1)
	require.NoError(t, err)

	rows, err := stmt.QueryBound(ctx)
	require.NoError(t, err)
	defer rows.Close()

	// Verify result
	cols := rows.Columns()
	assert.Equal(t, []string{"name"}, cols)

	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "alice", dest[0])
}

// TestExecBoundUnboundParams tests that ExecBound returns error if params not bound
func TestExecBoundUnboundParams(t *testing.T) {
	// This test doesn't need a real backend - just uses mock conn
	conn := &Conn{}

	stmt, err := conn.PreparePreparedStmt("SELECT $1")
	require.NoError(t, err)
	defer stmt.Close()

	// Try to execute without binding - should error before hitting backend
	_, err = stmt.ExecBound(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not all parameters have been bound")
}

// TestQueryBoundUnboundParams tests that QueryBound returns error if params not bound
func TestQueryBoundUnboundParams(t *testing.T) {
	// This test doesn't need a real backend - just uses mock conn
	conn := &Conn{}

	stmt, err := conn.PreparePreparedStmt("SELECT $1")
	require.NoError(t, err)
	defer stmt.Close()

	// Try to query without binding - should error before hitting backend
	_, err = stmt.QueryBound(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not all parameters have been bound")
}

// TestRebindParameters tests rebinding parameters between executions
// NOTE: This test requires the real execution engine which can't be imported
// in the root package due to import cycles. See internal/engine/engine_test.go
// for comprehensive integration tests with TestBoundExecution.
func TestRebindParameters(t *testing.T) {
	t.Skip("integration test - see internal/engine/engine_test.go TestBoundExecution")

	connector, err := NewConnector(":memory:", nil)
	require.NoError(t, err)
	defer connector.Close()

	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	dukConn := conn.(*Conn)
	ctx := context.Background()

	// Create and populate table
	_, err = dukConn.ExecContext(ctx, "CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = dukConn.ExecContext(ctx, "INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')", nil)
	require.NoError(t, err)

	// Prepare a SELECT with parameter
	stmt, err := dukConn.PreparePreparedStmt("SELECT name FROM test WHERE id = $1")
	require.NoError(t, err)
	defer stmt.Close()

	// First execution - find alice
	err = stmt.Bind(0, 1)
	require.NoError(t, err)

	rows, err := stmt.QueryBound(ctx)
	require.NoError(t, err)

	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "alice", dest[0])
	rows.Close()

	// Clear and rebind for second execution - find bob
	stmt.ClearBindings()
	err = stmt.Bind(0, 2)
	require.NoError(t, err)

	rows, err = stmt.QueryBound(ctx)
	require.NoError(t, err)

	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "bob", dest[0])
	rows.Close()

	// Third execution without clearing - just rebind - find charlie
	err = stmt.Bind(0, 3)
	require.NoError(t, err)

	rows, err = stmt.QueryBound(ctx)
	require.NoError(t, err)

	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "charlie", dest[0])
	rows.Close()
}

// TestStatementTypeHelperMethods tests StmtType helper methods work correctly
func TestStatementTypeHelperMethods(t *testing.T) {
	conn := &Conn{}

	tests := []struct {
		query     string
		isDML     bool
		isDDL     bool
		isQuery   bool
		modifies  bool
		isTxn     bool
	}{
		{"SELECT * FROM t", false, false, true, false, false},
		{"INSERT INTO t VALUES (1)", true, false, false, true, false},
		{"UPDATE t SET x = 1", true, false, false, true, false},
		{"DELETE FROM t", true, false, false, true, false},
		{"CREATE TABLE t (id INT)", false, true, false, true, false},
		{"DROP TABLE t", false, true, false, true, false},
		{"BEGIN", false, false, false, false, true},
		{"COMMIT", false, false, false, false, true},
		{"ROLLBACK", false, false, false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			stmt, err := conn.PreparePreparedStmt(tt.query)
			require.NoError(t, err)

			stmtType := stmt.StatementType()
			assert.Equal(t, tt.isDML, stmtType.IsDML(), "IsDML")
			assert.Equal(t, tt.isDDL, stmtType.IsDDL(), "IsDDL")
			assert.Equal(t, tt.isQuery, stmtType.IsQuery(), "IsQuery")
			assert.Equal(t, tt.modifies, stmtType.ModifiesData(), "ModifiesData")
			assert.Equal(t, tt.isTxn, stmtType.IsTransaction(), "IsTransaction")
		})
	}
}

// TestExtractOrderedParams tests the extractOrderedParams function
func TestExtractOrderedParams(t *testing.T) {
	t.Run("positional params are ordered by index", func(t *testing.T) {
		// Even if used out of order, they should be ordered 1, 2, 3
		params := extractOrderedParams("SELECT $3, $1, $2")
		require.Len(t, params, 3)
		assert.Equal(t, "1", params[0].name)
		assert.Equal(t, "2", params[1].name)
		assert.Equal(t, "3", params[2].name)
		assert.True(t, params[0].isPositional)
	})

	t.Run("named params are in first-occurrence order", func(t *testing.T) {
		params := extractOrderedParams("SELECT @c, @a, @b, @a")
		require.Len(t, params, 3)
		assert.Equal(t, "c", params[0].name)
		assert.Equal(t, "a", params[1].name)
		assert.Equal(t, "b", params[2].name)
		assert.False(t, params[0].isPositional)
	})

	t.Run("no params returns nil", func(t *testing.T) {
		params := extractOrderedParams("SELECT 1")
		assert.Nil(t, params)
	})

	t.Run("mixed params returns nil", func(t *testing.T) {
		params := extractOrderedParams("SELECT $1, @name")
		assert.Nil(t, params)
	})
}

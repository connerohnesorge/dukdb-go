// Package tests provides integration tests for dukdb-go that require the full backend engine.
// These tests are in a separate package to avoid import cycles.
package tests

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// TestPreparedStmtExecBoundWithoutParams tests ExecBound with no parameters
func TestPreparedStmtExecBoundWithoutParams(t *testing.T) {
	connector, err := dukdb.NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connector.Close())
	}()

	conn, err := connector.Connect(
		context.Background(),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	dukConn := conn.(*dukdb.Conn)
	ctx := context.Background()

	// Create table
	_, err = dukConn.ExecContext(
		ctx,
		"CREATE TABLE test (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)

	// Prepare and execute without params
	stmt, err := dukConn.PreparePreparedStmt(
		"INSERT INTO test VALUES (1, 'alice')",
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, stmt.Close())
	}()

	result, err := stmt.ExecBound(ctx)
	require.NoError(t, err)

	affected, err := result.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
}

// TestPreparedStmtExecBoundWithParams tests ExecBound with parameters
func TestPreparedStmtExecBoundWithParams(t *testing.T) {
	connector, err := dukdb.NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connector.Close())
	}()

	conn, err := connector.Connect(
		context.Background(),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	dukConn := conn.(*dukdb.Conn)
	ctx := context.Background()

	// Create table
	_, err = dukConn.ExecContext(
		ctx,
		"CREATE TABLE test (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)

	// Prepare with parameters
	stmt, err := dukConn.PreparePreparedStmt(
		"INSERT INTO test VALUES ($1, $2)",
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, stmt.Close())
	}()

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

// TestPreparedStmtQueryBoundWithParams tests QueryBound with parameters
func TestPreparedStmtQueryBoundWithParams(t *testing.T) {
	connector, err := dukdb.NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connector.Close())
	}()

	conn, err := connector.Connect(
		context.Background(),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	dukConn := conn.(*dukdb.Conn)
	ctx := context.Background()

	// Create and populate table
	_, err = dukConn.ExecContext(
		ctx,
		"CREATE TABLE test (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)
	_, err = dukConn.ExecContext(
		ctx,
		"INSERT INTO test VALUES (1, 'alice'), (2, 'bob')",
		nil,
	)
	require.NoError(t, err)

	// Prepare with parameter
	stmt, err := dukConn.PreparePreparedStmt(
		"SELECT name FROM test WHERE id = $1",
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, stmt.Close())
	}()

	// Bind and query
	err = stmt.Bind(0, 1)
	require.NoError(t, err)

	rows, err := stmt.QueryBound(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Verify result
	cols := rows.Columns()
	assert.Equal(t, []string{"name"}, cols)

	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "alice", dest[0])
}

// TestPreparedStmtRebindParameters tests rebinding parameters between executions
func TestPreparedStmtRebindParameters(t *testing.T) {
	connector, err := dukdb.NewConnector(
		":memory:",
		nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, connector.Close())
	}()

	conn, err := connector.Connect(
		context.Background(),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	dukConn := conn.(*dukdb.Conn)
	ctx := context.Background()

	// Create and populate table
	_, err = dukConn.ExecContext(
		ctx,
		"CREATE TABLE test (id INTEGER, name VARCHAR)",
		nil,
	)
	require.NoError(t, err)
	_, err = dukConn.ExecContext(
		ctx,
		"INSERT INTO test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
		nil,
	)
	require.NoError(t, err)

	// Prepare a SELECT with parameter
	stmt, err := dukConn.PreparePreparedStmt(
		"SELECT name FROM test WHERE id = $1",
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, stmt.Close())
	}()

	// First execution - find alice
	err = stmt.Bind(0, 1)
	require.NoError(t, err)

	rows, err := stmt.QueryBound(ctx)
	require.NoError(t, err)

	dest := make([]driver.Value, 1)
	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "alice", dest[0])
	require.NoError(t, rows.Close())

	// Clear and rebind for second execution - find bob
	stmt.ClearBindings()
	err = stmt.Bind(0, 2)
	require.NoError(t, err)

	rows, err = stmt.QueryBound(ctx)
	require.NoError(t, err)

	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "bob", dest[0])
	require.NoError(t, rows.Close())

	// Third execution without clearing - just rebind - find charlie
	err = stmt.Bind(0, 3)
	require.NoError(t, err)

	rows, err = stmt.QueryBound(ctx)
	require.NoError(t, err)

	err = rows.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, "charlie", dest[0])
	require.NoError(t, rows.Close())
}

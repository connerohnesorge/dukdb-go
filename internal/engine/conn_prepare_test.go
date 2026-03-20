package engine

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestConn(t *testing.T) *EngineConn {
	t.Helper()
	engine := NewEngine()
	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	ec := conn.(*EngineConn)
	t.Cleanup(func() { _ = ec.Close() })
	return ec
}

func TestPrepareExecuteSelect(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	// Create table and insert data
	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')", nil)
	require.NoError(t, err)

	// PREPARE a SELECT with a parameter
	_, err = c.Execute(ctx, "PREPARE q AS SELECT id, name FROM t WHERE id = $1", nil)
	require.NoError(t, err)

	// EXECUTE with parameter
	rows, cols, err := c.Query(ctx, "EXECUTE q(2)", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, cols)
	require.Len(t, rows, 1)
	assert.Equal(t, int32(2), rows[0]["id"])
	assert.Equal(t, "bob", rows[0]["name"])
}

func TestPrepareExecuteInsert(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER, val VARCHAR)", nil)
	require.NoError(t, err)

	// PREPARE an INSERT
	_, err = c.Execute(ctx, "PREPARE ins AS INSERT INTO t VALUES ($1, $2)", nil)
	require.NoError(t, err)

	// EXECUTE multiple times
	_, err = c.Execute(ctx, "EXECUTE ins(1, 'a')", nil)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "EXECUTE ins(2, 'b')", nil)
	require.NoError(t, err)

	// Verify
	rows, _, err := c.Query(ctx, "SELECT id, val FROM t ORDER BY id", nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int32(1), rows[0]["id"])
	assert.Equal(t, "a", rows[0]["val"])
	assert.Equal(t, int32(2), rows[1]["id"])
	assert.Equal(t, "b", rows[1]["val"])
}

func TestDeallocate(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	// PREPARE
	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// DEALLOCATE
	_, err = c.Execute(ctx, "DEALLOCATE q", nil)
	require.NoError(t, err)

	// EXECUTE after DEALLOCATE should fail
	_, err = c.Execute(ctx, "EXECUTE q", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestDeallocateAll(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE q1 AS SELECT * FROM t", nil)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "PREPARE q2 AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// DEALLOCATE ALL
	_, err = c.Execute(ctx, "DEALLOCATE ALL", nil)
	require.NoError(t, err)

	// Both should be gone
	_, err = c.Execute(ctx, "EXECUTE q1", nil)
	require.Error(t, err)
	_, err = c.Execute(ctx, "EXECUTE q2", nil)
	require.Error(t, err)
}

func TestDeallocatePrepareKeyword(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// DEALLOCATE PREPARE name syntax
	_, err = c.Execute(ctx, "DEALLOCATE PREPARE q", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "EXECUTE q", nil)
	require.Error(t, err)
}

func TestDuplicatePrepareName(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// Duplicate name should fail
	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestExecuteWrongParamCount(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t WHERE id = $1", nil)
	require.NoError(t, err)

	// No params when 1 expected
	_, err = c.Execute(ctx, "EXECUTE q", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 1 parameters, got 0")

	// Too many params
	_, err = c.Execute(ctx, "EXECUTE q(1, 2)", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected 1 parameters, got 2")
}

func TestExecuteNonExistent(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "EXECUTE nonexistent", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestDeallocateNonExistent(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "DEALLOCATE nonexistent", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestConnectionScopedIsolation(t *testing.T) {
	engine := NewEngine()

	conn1, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	c1 := conn1.(*EngineConn)
	defer c1.Close()

	conn2, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	c2 := conn2.(*EngineConn)
	defer c2.Close()

	ctx := context.Background()

	_, err = c1.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	// PREPARE on connection 1
	_, err = c1.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// EXECUTE on connection 2 should fail (not visible)
	_, err = c2.Execute(ctx, "EXECUTE q", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestConnectionCloseCleanup(t *testing.T) {
	engine := NewEngine()
	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	c := conn.(*EngineConn)

	ctx := context.Background()

	_, err = c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// Close should clean up
	err = c.Close()
	require.NoError(t, err)

	assert.Nil(t, c.sqlPrepared)
}

func TestPrepareViaQuery(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "INSERT INTO t VALUES (42)", nil)
	require.NoError(t, err)

	// PREPARE via Query path
	rows, cols, err := c.Query(ctx, "PREPARE q AS SELECT * FROM t WHERE id = $1", nil)
	require.NoError(t, err)
	assert.Empty(t, rows)
	assert.Empty(t, cols)

	// EXECUTE via Query path
	rows, cols, err = c.Query(ctx, "EXECUTE q(42)", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int32(42), rows[0]["id"])
}

func TestPrepareNoParams(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "INSERT INTO t VALUES (1), (2)", nil)
	require.NoError(t, err)

	// PREPARE with no parameters
	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t", nil)
	require.NoError(t, err)

	// EXECUTE with no parameters
	rows, _, err := c.Query(ctx, "EXECUTE q", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestExecuteWithNegativeParam(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER)", nil)
	require.NoError(t, err)
	_, err = c.Execute(ctx, "INSERT INTO t VALUES (-5), (0), (5)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE q AS SELECT * FROM t WHERE id = $1", nil)
	require.NoError(t, err)

	rows, _, err := c.Query(ctx, "EXECUTE q(-5)", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int32(-5), rows[0]["id"])
}

// Verify the Execute (Exec) path uses driver.NamedValue args properly.
func TestExecuteExecPathWithDriverArgs(t *testing.T) {
	c := setupTestConn(t)
	ctx := context.Background()

	_, err := c.Execute(ctx, "CREATE TABLE t (id INTEGER, val VARCHAR)", nil)
	require.NoError(t, err)

	_, err = c.Execute(ctx, "PREPARE ins AS INSERT INTO t VALUES ($1, $2)", nil)
	require.NoError(t, err)

	// Use driver args
	_, err = c.Execute(ctx, "EXECUTE ins(10, 'hello')", []driver.NamedValue{})
	require.NoError(t, err)

	rows, _, err := c.Query(ctx, "SELECT * FROM t", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int32(10), rows[0]["id"])
	assert.Equal(t, "hello", rows[0]["val"])
}

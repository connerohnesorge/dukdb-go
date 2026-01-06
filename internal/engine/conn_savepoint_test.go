package engine

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineConn_Savepoint_OutsideTransaction(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// SAVEPOINT outside transaction should fail
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can only be used in transaction block")
}

func TestEngineConn_RollbackToSavepoint_OutsideTransaction(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// ROLLBACK TO SAVEPOINT outside transaction should fail
	_, err = engineConn.Execute(ctx, "ROLLBACK TO SAVEPOINT sp1", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can only be used in transaction block")
}

func TestEngineConn_ReleaseSavepoint_OutsideTransaction(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// RELEASE SAVEPOINT outside transaction should fail
	_, err = engineConn.Execute(ctx, "RELEASE SAVEPOINT sp1", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can only be used in transaction block")
}

func TestEngineConn_Savepoint_InTransaction(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Create savepoint should succeed
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Verify savepoint was created
	sp, found := engineConn.txn.GetSavepoint("sp1")
	require.True(t, found)
	assert.Equal(t, "sp1", sp.Name)

	// Commit to clean up
	_, err = engineConn.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestEngineConn_ReleaseSavepoint_InTransaction(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Create savepoint
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Verify savepoint exists
	_, found := engineConn.txn.GetSavepoint("sp1")
	require.True(t, found)

	// Release savepoint
	_, err = engineConn.Execute(ctx, "RELEASE SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Verify savepoint is gone
	_, found = engineConn.txn.GetSavepoint("sp1")
	assert.False(t, found)

	// Commit to clean up
	_, err = engineConn.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestEngineConn_RollbackToSavepoint_UndoesInserts(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Create table
	_, err = engineConn.Execute(ctx, "CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Insert initial data
	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (1, 'one')", nil)
	require.NoError(t, err)

	// Create savepoint
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Insert more data after savepoint
	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (2, 'two')", nil)
	require.NoError(t, err)

	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (3, 'three')", nil)
	require.NoError(t, err)

	// Verify all three rows exist
	rows, _, err := engineConn.Query(ctx, "SELECT COUNT(*) FROM test", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(3), rows[0]["count"])

	// Rollback to savepoint
	_, err = engineConn.Execute(ctx, "ROLLBACK TO SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Verify only the first row remains (rows 2 and 3 were undone)
	rows, _, err = engineConn.Query(ctx, "SELECT COUNT(*) FROM test", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(1), rows[0]["count"])

	// Verify it's the correct row
	rows, _, err = engineConn.Query(ctx, "SELECT * FROM test", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int32(1), rows[0]["id"])
	assert.Equal(t, "one", rows[0]["name"])

	// Commit to clean up
	_, err = engineConn.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestEngineConn_RollbackToSavepoint_NonExistent(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Try to rollback to non-existent savepoint
	_, err = engineConn.Execute(ctx, "ROLLBACK TO SAVEPOINT nonexistent", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Rollback to clean up
	_, err = engineConn.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

func TestEngineConn_ReleaseSavepoint_NonExistent(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Try to release non-existent savepoint
	_, err = engineConn.Execute(ctx, "RELEASE SAVEPOINT nonexistent", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Commit to clean up
	_, err = engineConn.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestEngineConn_MultipleSavepoints(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Create table
	_, err = engineConn.Execute(ctx, "CREATE TABLE test (id INTEGER)", nil)
	require.NoError(t, err)

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Insert and create savepoint sp1
	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (1)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.NoError(t, err)

	// Insert and create savepoint sp2
	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (2)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp2", nil)
	require.NoError(t, err)

	// Insert and create savepoint sp3
	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (3)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp3", nil)
	require.NoError(t, err)

	// Insert more data
	_, err = engineConn.Execute(ctx, "INSERT INTO test VALUES (4)", nil)
	require.NoError(t, err)

	// Verify all four rows exist
	rows, _, err := engineConn.Query(ctx, "SELECT COUNT(*) FROM test", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(4), rows[0]["count"])

	// Rollback to sp2 (should undo inserts 3 and 4)
	_, err = engineConn.Execute(ctx, "ROLLBACK TO SAVEPOINT sp2", nil)
	require.NoError(t, err)

	// Verify only two rows remain
	rows, _, err = engineConn.Query(ctx, "SELECT COUNT(*) FROM test", nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), rows[0]["count"])

	// sp1 should still exist
	_, found := engineConn.txn.GetSavepoint("sp1")
	assert.True(t, found)

	// sp2 and sp3 should be gone
	_, found = engineConn.txn.GetSavepoint("sp2")
	assert.False(t, found)
	_, found = engineConn.txn.GetSavepoint("sp3")
	assert.False(t, found)

	// Commit to clean up
	_, err = engineConn.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestEngineConn_Savepoint_ParseVariations(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	tests := []struct {
		name  string
		sql   string
		spKey string
	}{
		{
			name:  "simple savepoint",
			sql:   "SAVEPOINT my_savepoint",
			spKey: "my_savepoint",
		},
		{
			name:  "savepoint with underscore",
			sql:   "SAVEPOINT my_long_savepoint_name",
			spKey: "my_long_savepoint_name",
		},
		{
			name:  "savepoint with numbers",
			sql:   "SAVEPOINT sp123",
			spKey: "sp123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Start transaction
			_, err = engineConn.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)

			// Create savepoint
			_, err = engineConn.Execute(ctx, tc.sql, nil)
			require.NoError(t, err)

			// Verify savepoint exists
			_, found := engineConn.txn.GetSavepoint(tc.spKey)
			assert.True(t, found, "savepoint %q should exist", tc.spKey)

			// Clean up
			_, err = engineConn.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		})
	}
}

func TestEngineConn_Savepoint_CommitClearsSavepoints(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Create savepoints
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp2", nil)
	require.NoError(t, err)

	// Verify savepoints exist
	assert.Equal(t, 2, engineConn.txn.SavepointCount())

	// Commit
	_, err = engineConn.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	// Savepoints should be cleared
	assert.Equal(t, 0, engineConn.txn.SavepointCount())
}

func TestEngineConn_Savepoint_RollbackClearsSavepoints(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	// Create savepoints
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp2", nil)
	require.NoError(t, err)

	// Verify savepoints exist
	assert.Equal(t, 2, engineConn.txn.SavepointCount())

	// Rollback entire transaction
	_, err = engineConn.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	// Savepoints should be cleared
	assert.Equal(t, 0, engineConn.txn.SavepointCount())
}

func TestEngineConn_Savepoint_WithDriverNamedValue(t *testing.T) {
	engine := NewEngine()
	defer func() { _ = engine.Close() }()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)
	ctx := context.Background()

	// Start transaction
	_, err = engineConn.Execute(ctx, "BEGIN", []driver.NamedValue{})
	require.NoError(t, err)

	// Create savepoint with empty args (shouldn't matter)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp1", []driver.NamedValue{})
	require.NoError(t, err)

	// Verify savepoint exists
	_, found := engineConn.txn.GetSavepoint("sp1")
	assert.True(t, found)

	// Rollback to savepoint
	_, err = engineConn.Execute(ctx, "ROLLBACK TO SAVEPOINT sp1", []driver.NamedValue{})
	require.NoError(t, err)

	// Release savepoint (create a new one first)
	_, err = engineConn.Execute(ctx, "SAVEPOINT sp2", []driver.NamedValue{})
	require.NoError(t, err)
	_, err = engineConn.Execute(ctx, "RELEASE SAVEPOINT sp2", []driver.NamedValue{})
	require.NoError(t, err)

	// Commit to clean up
	_, err = engineConn.Execute(ctx, "COMMIT", []driver.NamedValue{})
	require.NoError(t, err)
}

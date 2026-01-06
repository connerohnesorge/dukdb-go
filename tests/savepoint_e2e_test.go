package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Basic SAVEPOINT Operations
// =============================================================================

func TestSavepoint_E2E_BasicSavepoint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT, value TEXT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (1, 'one')")
	require.NoError(t, err)

	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data
	_, err = tx.Exec("INSERT INTO test VALUES (2, 'two')")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state - both rows should exist
	rows, err := db.Query("SELECT id FROM test ORDER BY id")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1, 2}, ids)
}

func TestSavepoint_E2E_MultipleSavepoints(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Create multiple savepoints
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp2")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp3")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify all rows exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestSavepoint_E2E_DuplicateSavepointNameReplaces(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint sp1
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more and create sp1 again (should replace)
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data
	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)

	// Rollback to sp1 - should only undo the insert of 3
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify only rows 1 and 2 exist (since sp1 was replaced at row 2)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// =============================================================================
// ROLLBACK TO SAVEPOINT
// =============================================================================

func TestSavepoint_E2E_RollbackUndoesInsert(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT, value TEXT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert initial data
	_, err = tx.Exec("INSERT INTO test VALUES (1, 'one')")
	require.NoError(t, err)

	// Create savepoint
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data after savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (2, 'two')")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (3, 'three')")
	require.NoError(t, err)

	// Verify 3 rows exist within transaction
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Verify only 1 row remains within transaction
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	var id int
	var value string
	err = db.QueryRow("SELECT id, value FROM test").Scan(&id, &value)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "one", value)
}

func TestSavepoint_E2E_RollbackUndoesUpdate(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create and populate test table
	_, err = db.Exec("CREATE TABLE test (id INT, value TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO test VALUES (1, 'original')")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Create savepoint
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Update data after savepoint
	_, err = tx.Exec("UPDATE test SET value = 'modified' WHERE id = 1")
	require.NoError(t, err)

	// Verify update within transaction
	var value string
	err = tx.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, "modified", value)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Verify value is restored within transaction
	err = tx.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, "original", value)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state
	err = db.QueryRow("SELECT value FROM test WHERE id = 1").Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, "original", value)
}

func TestSavepoint_E2E_RollbackUndoesDelete(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create and populate test table
	_, err = db.Exec("CREATE TABLE test (id INT, value TEXT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO test VALUES (1, 'one'), (2, 'two')")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Create savepoint
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Delete data after savepoint
	_, err = tx.Exec("DELETE FROM test WHERE id = 2")
	require.NoError(t, err)

	// Verify delete within transaction
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Verify deleted row is restored within transaction
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestSavepoint_E2E_TransactionContinuesAfterRollbackToSavepoint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Continue with more operations
	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (4)")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state - should have rows 1, 3, 4 (not 2)
	rows, err := db.Query("SELECT id FROM test ORDER BY id")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1, 3, 4}, ids)
}

func TestSavepoint_E2E_NestedSavepointsRollbackRemovesInner(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Create nested savepoints
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp2")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp3")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (4)")
	require.NoError(t, err)

	// Rollback to sp1 - should undo rows 2, 3, 4 and remove sp2, sp3
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Verify only row 1 remains
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Attempting to rollback to sp2 or sp3 should fail (they were removed)
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp3")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Rollback to clean up
	err = tx.Rollback()
	require.NoError(t, err)
}

// =============================================================================
// RELEASE SAVEPOINT
// =============================================================================

func TestSavepoint_E2E_ReleaseRemovesSavepoint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)

	// Release savepoint
	_, err = tx.Exec("RELEASE SAVEPOINT sp1")
	require.NoError(t, err)

	// Attempting to rollback to released savepoint should fail
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state - both rows should exist (release doesn't undo)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestSavepoint_E2E_ReleaseRemovesNestedSavepoints(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Create nested savepoints
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp2")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp3")
	require.NoError(t, err)

	// Release sp1 - should remove sp1, sp2, and sp3
	_, err = tx.Exec("RELEASE SAVEPOINT sp1")
	require.NoError(t, err)

	// All savepoints should be gone
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.Error(t, err)

	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp2")
	require.Error(t, err)

	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp3")
	require.Error(t, err)

	// Rollback to clean up
	err = tx.Rollback()
	require.NoError(t, err)
}

func TestSavepoint_E2E_ReleaseDoesNotUndoOperations(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)

	// Release savepoint - data should remain
	_, err = tx.Exec("RELEASE SAVEPOINT sp1")
	require.NoError(t, err)

	// Verify all data still exists
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

// =============================================================================
// Error Cases
// =============================================================================

func TestSavepoint_E2E_SavepointOutsideTransactionFails(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// SAVEPOINT outside transaction should fail
	_, err = db.Exec("SAVEPOINT sp1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can only be used in transaction block")
}

func TestSavepoint_E2E_RollbackToSavepointOutsideTransactionFails(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// ROLLBACK TO SAVEPOINT outside transaction should fail
	_, err = db.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can only be used in transaction block")
}

func TestSavepoint_E2E_ReleaseSavepointOutsideTransactionFails(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// RELEASE SAVEPOINT outside transaction should fail
	_, err = db.Exec("RELEASE SAVEPOINT sp1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can only be used in transaction block")
}

func TestSavepoint_E2E_RollbackToNonExistentSavepointFails(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// ROLLBACK TO non-existent savepoint should fail
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestSavepoint_E2E_ReleaseNonExistentSavepointFails(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// RELEASE non-existent savepoint should fail
	_, err = tx.Exec("RELEASE SAVEPOINT nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

// =============================================================================
// Complex Scenarios
// =============================================================================

func TestSavepoint_E2E_NestedSavepointsWithPartialRollbacks(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Layer 1: Insert and savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Layer 2: Insert and savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp2")
	require.NoError(t, err)

	// Layer 3: Insert and savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp3")
	require.NoError(t, err)

	// More inserts
	_, err = tx.Exec("INSERT INTO test VALUES (4)")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (5)")
	require.NoError(t, err)

	// Rollback to sp3 - undo rows 4, 5
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp3")
	require.NoError(t, err)

	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Insert new data
	_, err = tx.Exec("INSERT INTO test VALUES (6)")
	require.NoError(t, err)

	// Rollback to sp2 - undo rows 3, 6
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp2")
	require.NoError(t, err)

	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state - rows 1, 2
	rows, err := db.Query("SELECT id FROM test ORDER BY id")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1, 2}, ids)
}

func TestSavepoint_E2E_CreateRollbackDoMoreCommit(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT, status TEXT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert initial record
	_, err = tx.Exec("INSERT INTO test VALUES (1, 'pending')")
	require.NoError(t, err)

	// Create savepoint before risky operation
	_, err = tx.Exec("SAVEPOINT before_risky")
	require.NoError(t, err)

	// Simulate risky operation that we want to undo
	_, err = tx.Exec("UPDATE test SET status = 'failed' WHERE id = 1")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (2, 'error_data')")
	require.NoError(t, err)

	// Realize we need to undo - rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT before_risky")
	require.NoError(t, err)

	// Do correct operations instead
	_, err = tx.Exec("UPDATE test SET status = 'success' WHERE id = 1")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (3, 'valid_data')")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state
	rows, err := db.Query("SELECT id, status FROM test ORDER BY id")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	type record struct {
		id     int
		status string
	}
	var records []record
	for rows.Next() {
		var r record
		require.NoError(t, rows.Scan(&r.id, &r.status))
		records = append(records, r)
	}
	require.NoError(t, rows.Err())

	assert.Len(t, records, 2)
	assert.Equal(t, record{id: 1, status: "success"}, records[0])
	assert.Equal(t, record{id: 3, status: "valid_data"}, records[1])
}

func TestSavepoint_E2E_MultipleRollbacksToSameSavepoint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT checkpoint")
	require.NoError(t, err)

	// First iteration: insert and rollback
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)

	// Note: After rollback, the savepoint is removed
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT checkpoint")
	require.NoError(t, err)

	// Create checkpoint again
	_, err = tx.Exec("SAVEPOINT checkpoint")
	require.NoError(t, err)

	// Second iteration: insert and rollback
	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT checkpoint")
	require.NoError(t, err)

	// Create checkpoint again
	_, err = tx.Exec("SAVEPOINT checkpoint")
	require.NoError(t, err)

	// Third iteration: insert and commit
	_, err = tx.Exec("INSERT INTO test VALUES (4)")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state - only rows 1 and 4
	rows, err := db.Query("SELECT id FROM test ORDER BY id")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var ids []int
	for rows.Next() {
		var id int
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int{1, 4}, ids)
}

func TestSavepoint_E2E_RollbackToMiddleSavepoint(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoint sp1
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert and create savepoint sp2
	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp2")
	require.NoError(t, err)

	// Insert and create savepoint sp3
	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp3")
	require.NoError(t, err)

	// Insert more
	_, err = tx.Exec("INSERT INTO test VALUES (4)")
	require.NoError(t, err)

	// Rollback to sp2 (middle savepoint)
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp2")
	require.NoError(t, err)

	// sp1 should still exist
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Verify only row 1 remains
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)
}

func TestSavepoint_E2E_MixedOperationsWithSavepoints(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE accounts (id INT, balance INT)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO accounts VALUES (1, 1000), (2, 500)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Create savepoint before transfer
	_, err = tx.Exec("SAVEPOINT before_transfer")
	require.NoError(t, err)

	// Transfer 200 from account 1 to account 2
	_, err = tx.Exec("UPDATE accounts SET balance = balance - 200 WHERE id = 1")
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE accounts SET balance = balance + 200 WHERE id = 2")
	require.NoError(t, err)

	// Verify intermediate state
	var balance1, balance2 int
	err = tx.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&balance1)
	require.NoError(t, err)
	err = tx.QueryRow("SELECT balance FROM accounts WHERE id = 2").Scan(&balance2)
	require.NoError(t, err)
	assert.Equal(t, 800, balance1)
	assert.Equal(t, 700, balance2)

	// Create another savepoint
	_, err = tx.Exec("SAVEPOINT after_transfer")
	require.NoError(t, err)

	// Try another transfer that we'll undo
	_, err = tx.Exec("UPDATE accounts SET balance = balance - 500 WHERE id = 1")
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE accounts SET balance = balance + 500 WHERE id = 2")
	require.NoError(t, err)

	// Undo the second transfer
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT after_transfer")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state (only first transfer applied)
	err = db.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&balance1)
	require.NoError(t, err)
	err = db.QueryRow("SELECT balance FROM accounts WHERE id = 2").Scan(&balance2)
	require.NoError(t, err)
	assert.Equal(t, 800, balance1)
	assert.Equal(t, 700, balance2)
}

func TestSavepoint_E2E_TransactionRollbackAfterSavepoints(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table
	_, err = db.Exec("CREATE TABLE test (id INT)")
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert and create savepoints
	_, err = tx.Exec("INSERT INTO test VALUES (1)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (2)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp2")
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test VALUES (3)")
	require.NoError(t, err)

	// Rollback entire transaction (not savepoint)
	err = tx.Rollback()
	require.NoError(t, err)

	// Verify no data exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestSavepoint_E2E_SavepointWithDifferentDataTypes(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Create test table with various data types
	_, err = db.Exec(`
		CREATE TABLE test (
			id INT,
			name VARCHAR,
			amount DOUBLE,
			active BOOLEAN
		)
	`)
	require.NoError(t, err)

	// Begin transaction
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert initial data
	_, err = tx.Exec("INSERT INTO test VALUES (1, 'Alice', 100.50, true)")
	require.NoError(t, err)
	_, err = tx.Exec("SAVEPOINT sp1")
	require.NoError(t, err)

	// Insert more data
	_, err = tx.Exec("INSERT INTO test VALUES (2, 'Bob', 200.75, false)")
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO test VALUES (3, 'Charlie', 300.25, true)")
	require.NoError(t, err)

	// Rollback to savepoint
	_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err)

	// Commit
	err = tx.Commit()
	require.NoError(t, err)

	// Verify final state
	var id int
	var name string
	var amount float64
	var active bool
	err = db.QueryRow("SELECT id, name, amount, active FROM test").Scan(&id, &name, &amount, &active)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "Alice", name)
	assert.Equal(t, 100.50, amount)
	assert.Equal(t, true, active)
}

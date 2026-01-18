package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetDefaultIsolationLevel tests SET default_transaction_isolation.
func TestSetDefaultIsolationLevel(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Initial default should be SERIALIZABLE (zero value)
	assert.Equal(t, parser.IsolationLevelSerializable, engineConn.defaultIsolationLevel)

	tests := []struct {
		name          string
		sql           string
		expectedLevel parser.IsolationLevel
	}{
		{
			name:          "SET to READ UNCOMMITTED",
			sql:           "SET default_transaction_isolation = 'READ UNCOMMITTED'",
			expectedLevel: parser.IsolationLevelReadUncommitted,
		},
		{
			name:          "SET to READ COMMITTED",
			sql:           "SET default_transaction_isolation = 'READ COMMITTED'",
			expectedLevel: parser.IsolationLevelReadCommitted,
		},
		{
			name:          "SET to REPEATABLE READ",
			sql:           "SET default_transaction_isolation = 'REPEATABLE READ'",
			expectedLevel: parser.IsolationLevelRepeatableRead,
		},
		{
			name:          "SET to SERIALIZABLE",
			sql:           "SET default_transaction_isolation = 'SERIALIZABLE'",
			expectedLevel: parser.IsolationLevelSerializable,
		},
		{
			name:          "SET with lowercase value",
			sql:           "SET default_transaction_isolation = 'read committed'",
			expectedLevel: parser.IsolationLevelReadCommitted,
		},
		{
			name:          "SET using TO syntax",
			sql:           "SET default_transaction_isolation TO 'REPEATABLE READ'",
			expectedLevel: parser.IsolationLevelRepeatableRead,
		},
		{
			name:          "SET using transaction_isolation (synonym)",
			sql:           "SET transaction_isolation = 'READ UNCOMMITTED'",
			expectedLevel: parser.IsolationLevelReadUncommitted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := engineConn.Execute(context.Background(), tt.sql, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedLevel, engineConn.defaultIsolationLevel)
		})
	}
}

// TestShowTransactionIsolation tests SHOW transaction_isolation.
func TestShowTransactionIsolation(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Test SHOW default_transaction_isolation
	t.Run("SHOW default_transaction_isolation", func(t *testing.T) {
		rows, columns, err := engineConn.Query(
			context.Background(),
			"SHOW default_transaction_isolation",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, columns, 1)
		assert.Equal(t, "default_transaction_isolation", columns[0])
		require.Len(t, rows, 1)
		assert.Equal(t, "SERIALIZABLE", rows[0]["default_transaction_isolation"])
	})

	// Test SHOW transaction_isolation outside of transaction
	t.Run("SHOW transaction_isolation outside transaction", func(t *testing.T) {
		rows, columns, err := engineConn.Query(
			context.Background(),
			"SHOW transaction_isolation",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, columns, 1)
		assert.Equal(t, "transaction_isolation", columns[0])
		require.Len(t, rows, 1)
		// Should return default isolation level when not in transaction
		assert.Equal(t, "SERIALIZABLE", rows[0]["transaction_isolation"])
	})

	// Change default and verify
	t.Run("SHOW after SET", func(t *testing.T) {
		_, err := engineConn.Execute(
			context.Background(),
			"SET default_transaction_isolation = 'READ COMMITTED'",
			nil,
		)
		require.NoError(t, err)

		rows, _, err := engineConn.Query(
			context.Background(),
			"SHOW default_transaction_isolation",
			nil,
		)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "READ COMMITTED", rows[0]["default_transaction_isolation"])
	})
}

// TestBeginWithIsolationLevel tests BEGIN TRANSACTION ISOLATION LEVEL.
func TestBeginWithIsolationLevel(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Begin transaction with explicit isolation level
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
		nil,
	)
	require.NoError(t, err)

	// Check that transaction isolation level is set
	assert.True(t, engineConn.inTxn)
	assert.Equal(t, parser.IsolationLevelReadCommitted, engineConn.currentIsolationLevel)

	// SHOW transaction_isolation should return the transaction's level
	rows, _, err := engineConn.Query(context.Background(), "SHOW transaction_isolation", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "READ COMMITTED", rows[0]["transaction_isolation"])

	// Rollback
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestBeginUsesDefaultIsolationLevel tests that BEGIN uses the connection's default.
func TestBeginUsesDefaultIsolationLevel(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Set default isolation level
	_, err = engineConn.Execute(
		context.Background(),
		"SET default_transaction_isolation = 'READ UNCOMMITTED'",
		nil,
	)
	require.NoError(t, err)

	// Begin transaction without explicit isolation level
	_, err = engineConn.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)

	// Transaction should use the default isolation level
	assert.True(t, engineConn.inTxn)
	assert.Equal(t, parser.IsolationLevelReadUncommitted, engineConn.currentIsolationLevel)

	// SHOW should return the transaction's level
	rows, _, err := engineConn.Query(context.Background(), "SHOW transaction_isolation", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "READ UNCOMMITTED", rows[0]["transaction_isolation"])

	// Rollback
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestSetInvalidIsolationLevel tests that invalid isolation levels return an error.
func TestSetInvalidIsolationLevel(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Try to set an invalid isolation level
	_, err = engineConn.Execute(
		context.Background(),
		"SET default_transaction_isolation = 'INVALID'",
		nil,
	)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid isolation level")
}

// TestParseIsolationLevelString tests the parseIsolationLevelString function.
func TestParseIsolationLevelString(t *testing.T) {
	tests := []struct {
		input    string
		expected parser.IsolationLevel
		wantErr  bool
	}{
		{"READ UNCOMMITTED", parser.IsolationLevelReadUncommitted, false},
		{"read uncommitted", parser.IsolationLevelReadUncommitted, false},
		{"READ COMMITTED", parser.IsolationLevelReadCommitted, false},
		{"read committed", parser.IsolationLevelReadCommitted, false},
		{"REPEATABLE READ", parser.IsolationLevelRepeatableRead, false},
		{"repeatable read", parser.IsolationLevelRepeatableRead, false},
		{"SERIALIZABLE", parser.IsolationLevelSerializable, false},
		{"serializable", parser.IsolationLevelSerializable, false},
		{"INVALID", parser.IsolationLevelSerializable, true},
		{"", parser.IsolationLevelSerializable, true},
		{"READ", parser.IsolationLevelSerializable, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			level, err := parseIsolationLevelString(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, level)
			}
		})
	}
}

// TestTransactionManagerBeginWithIsolation tests BeginWithIsolation on TransactionManager.
func TestTransactionManagerBeginWithIsolation(t *testing.T) {
	tm := NewTransactionManager()

	tests := []struct {
		name  string
		level parser.IsolationLevel
	}{
		{"SERIALIZABLE", parser.IsolationLevelSerializable},
		{"REPEATABLE READ", parser.IsolationLevelRepeatableRead},
		{"READ COMMITTED", parser.IsolationLevelReadCommitted},
		{"READ UNCOMMITTED", parser.IsolationLevelReadUncommitted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := tm.BeginWithIsolation(tt.level)
			require.NotNil(t, txn)
			assert.True(t, txn.IsActive())
			assert.Equal(t, tt.level, txn.GetIsolationLevel())

			// Cleanup: rollback the transaction
			err := tm.Rollback(txn)
			require.NoError(t, err)
		})
	}
}

// TestTransactionManagerBeginUsesDefaultIsolation tests that Begin uses SERIALIZABLE.
func TestTransactionManagerBeginUsesDefaultIsolation(t *testing.T) {
	tm := NewTransactionManager()

	txn := tm.Begin()
	require.NotNil(t, txn)
	assert.True(t, txn.IsActive())
	// Begin() should use SERIALIZABLE as the default
	assert.Equal(t, parser.IsolationLevelSerializable, txn.GetIsolationLevel())

	// Cleanup
	err := tm.Rollback(txn)
	require.NoError(t, err)
}

// TestTransactionGetIsolationLevel tests GetIsolationLevel on Transaction.
func TestTransactionGetIsolationLevel(t *testing.T) {
	tm := NewTransactionManager()

	// Test with different isolation levels
	levels := []parser.IsolationLevel{
		parser.IsolationLevelSerializable,
		parser.IsolationLevelRepeatableRead,
		parser.IsolationLevelReadCommitted,
		parser.IsolationLevelReadUncommitted,
	}

	for _, level := range levels {
		txn := tm.BeginWithIsolation(level)
		assert.Equal(t, level, txn.GetIsolationLevel())
		_ = tm.Rollback(txn)
	}
}

// TestTransactionSetIsolationLevel tests SetIsolationLevel on Transaction.
func TestTransactionSetIsolationLevel(t *testing.T) {
	tm := NewTransactionManager()

	// Create transaction with SERIALIZABLE
	txn := tm.BeginWithIsolation(parser.IsolationLevelSerializable)
	assert.Equal(t, parser.IsolationLevelSerializable, txn.GetIsolationLevel())

	// Change isolation level
	txn.SetIsolationLevel(parser.IsolationLevelReadCommitted)
	assert.Equal(t, parser.IsolationLevelReadCommitted, txn.GetIsolationLevel())

	// Cleanup
	_ = tm.Rollback(txn)
}

// TestTransactionIsolationLevelPreservedAfterCommit tests that creating new transactions
// after commit gets fresh isolation levels.
func TestTransactionIsolationLevelPreservedAfterCommit(t *testing.T) {
	tm := NewTransactionManager()

	// Create first transaction with READ COMMITTED
	txn1 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)
	assert.Equal(t, parser.IsolationLevelReadCommitted, txn1.GetIsolationLevel())
	err := tm.Commit(txn1)
	require.NoError(t, err)

	// Create second transaction - should get its own isolation level
	txn2 := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)
	assert.Equal(t, parser.IsolationLevelRepeatableRead, txn2.GetIsolationLevel())

	// First transaction's level should not affect second
	assert.NotEqual(t, txn1.GetIsolationLevel(), txn2.GetIsolationLevel())

	// Cleanup
	_ = tm.Rollback(txn2)
}

// TestConnectionTransactionIsolationLevelIntegration tests that the connection
// properly sets the transaction's isolation level.
func TestConnectionTransactionIsolationLevelIntegration(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Set default isolation level
	_, err = engineConn.Execute(
		context.Background(),
		"SET default_transaction_isolation = 'REPEATABLE READ'",
		nil,
	)
	require.NoError(t, err)

	// Begin transaction without explicit isolation level
	_, err = engineConn.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)

	// Both connection and transaction should have REPEATABLE READ
	assert.Equal(t, parser.IsolationLevelRepeatableRead, engineConn.currentIsolationLevel)
	if engineConn.txn != nil {
		assert.Equal(t, parser.IsolationLevelRepeatableRead, engineConn.txn.GetIsolationLevel())
	}

	// Rollback
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestConnectionTransactionExplicitIsolationLevel tests that explicit isolation
// levels in BEGIN override the default.
func TestConnectionTransactionExplicitIsolationLevel(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Set default isolation level to SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"SET default_transaction_isolation = 'SERIALIZABLE'",
		nil,
	)
	require.NoError(t, err)

	// Begin transaction with explicit READ UNCOMMITTED (overrides default)
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
		nil,
	)
	require.NoError(t, err)

	// Transaction should have READ UNCOMMITTED, not the default SERIALIZABLE
	assert.Equal(t, parser.IsolationLevelReadUncommitted, engineConn.currentIsolationLevel)
	if engineConn.txn != nil {
		assert.Equal(t, parser.IsolationLevelReadUncommitted, engineConn.txn.GetIsolationLevel())
	}

	// Rollback
	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestTransactionBeginStatement tests that BeginStatement updates the statement time.
func TestTransactionBeginStatement(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()
	defer func() { _ = tm.Rollback(txn) }()

	// Initially, statement time should be zero
	assert.True(t, txn.GetStatementTime().IsZero())

	// Call BeginStatement
	txn.BeginStatement()

	// Statement time should now be set to approximately now
	stmtTime := txn.GetStatementTime()
	assert.False(t, stmtTime.IsZero())
	assert.WithinDuration(t, time.Now(), stmtTime, time.Second)
}

// TestTransactionBeginStatementUpdates tests that multiple BeginStatement calls update the time.
func TestTransactionBeginStatementUpdates(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()
	defer func() { _ = tm.Rollback(txn) }()

	// First statement
	txn.BeginStatement()
	firstTime := txn.GetStatementTime()
	assert.False(t, firstTime.IsZero())

	// Wait a small amount to ensure time changes
	time.Sleep(10 * time.Millisecond)

	// Second statement
	txn.BeginStatement()
	secondTime := txn.GetStatementTime()
	assert.False(t, secondTime.IsZero())

	// Second time should be after (or equal to) first time
	assert.True(t, secondTime.After(firstTime) || secondTime.Equal(firstTime))
}

// TestTransactionContextAdapterGetStatementTime tests that the adapter returns the statement time.
func TestTransactionContextAdapterGetStatementTime(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()
	defer func() { _ = tm.Rollback(txn) }()

	adapter := NewTransactionContextAdapter(txn, tm)

	// Initially, statement time should be zero
	assert.True(t, adapter.GetStatementTime().IsZero())

	// Call BeginStatement on the transaction
	txn.BeginStatement()

	// Adapter should return the updated statement time
	stmtTime := adapter.GetStatementTime()
	assert.False(t, stmtTime.IsZero())
	assert.Equal(t, txn.GetStatementTime(), stmtTime)
}

// TestConnectionExecuteUpdatesStatementTime tests that Execute updates the statement time.
func TestConnectionExecuteUpdatesStatementTime(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Initially, statement time should be zero (no statements executed yet)
	initialTime := engineConn.txn.GetStatementTime()
	assert.True(t, initialTime.IsZero())

	// Execute a statement
	_, err = engineConn.Execute(context.Background(), "CREATE TABLE test (id INTEGER)", nil)
	require.NoError(t, err)

	// Statement time should now be set
	afterCreate := engineConn.txn.GetStatementTime()
	assert.False(t, afterCreate.IsZero())
	assert.WithinDuration(t, time.Now(), afterCreate, time.Second)

	// Wait and execute another statement
	time.Sleep(10 * time.Millisecond)

	_, err = engineConn.Execute(context.Background(), "INSERT INTO test VALUES (1)", nil)
	require.NoError(t, err)

	// Statement time should be updated
	afterInsert := engineConn.txn.GetStatementTime()
	assert.False(t, afterInsert.IsZero())
	assert.True(t, afterInsert.After(afterCreate) || afterInsert.Equal(afterCreate))
}

// TestConnectionQueryUpdatesStatementTime tests that Query updates the statement time.
func TestConnectionQueryUpdatesStatementTime(t *testing.T) {
	// Create a new in-memory engine
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	// Create a connection
	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Get engine connection
	engineConn := conn.(*EngineConn)

	// Create a table first
	_, err = engineConn.Execute(context.Background(), "CREATE TABLE test (id INTEGER)", nil)
	require.NoError(t, err)

	timeAfterCreate := engineConn.txn.GetStatementTime()

	// Wait a small amount
	time.Sleep(10 * time.Millisecond)

	// Execute a query
	_, _, err = engineConn.Query(context.Background(), "SELECT * FROM test", nil)
	require.NoError(t, err)

	// Statement time should be updated
	afterSelect := engineConn.txn.GetStatementTime()
	assert.False(t, afterSelect.IsZero())
	assert.True(t, afterSelect.After(timeAfterCreate) || afterSelect.Equal(timeAfterCreate))
}

// TestStatementTimeConcurrency tests that BeginStatement is thread-safe.
func TestStatementTimeConcurrency(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()
	defer func() { _ = tm.Rollback(txn) }()

	// Run multiple goroutines updating statement time
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				txn.BeginStatement()
				// Read the statement time to ensure no race
				_ = txn.GetStatementTime()
			}
		}()
	}
	wg.Wait()

	// Should complete without race conditions
	finalTime := txn.GetStatementTime()
	assert.False(t, finalTime.IsZero())
}

// =============================================================================
// Transaction Snapshot Tests (Phase 4: REPEATABLE READ)
// =============================================================================

// TestTransactionSnapshotCreatedForRepeatableRead tests that a snapshot is created
// when a REPEATABLE READ transaction begins.
func TestTransactionSnapshotCreatedForRepeatableRead(t *testing.T) {
	tm := NewTransactionManager()

	// Create a REPEATABLE READ transaction
	txn := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)
	defer func() { _ = tm.Rollback(txn) }()

	// Transaction should have a snapshot
	assert.True(t, txn.HasSnapshot())
	assert.NotNil(t, txn.GetSnapshot())

	// Snapshot timestamp should be approximately now
	snapshot := txn.GetSnapshot()
	assert.WithinDuration(t, time.Now(), snapshot.GetTimestamp(), time.Second)
}

// TestTransactionSnapshotCreatedForSerializable tests that a snapshot is created
// when a SERIALIZABLE transaction begins.
func TestTransactionSnapshotCreatedForSerializable(t *testing.T) {
	tm := NewTransactionManager()

	// Create a SERIALIZABLE transaction
	txn := tm.BeginWithIsolation(parser.IsolationLevelSerializable)
	defer func() { _ = tm.Rollback(txn) }()

	// Transaction should have a snapshot
	assert.True(t, txn.HasSnapshot())
	assert.NotNil(t, txn.GetSnapshot())

	// Snapshot timestamp should be approximately now
	snapshot := txn.GetSnapshot()
	assert.WithinDuration(t, time.Now(), snapshot.GetTimestamp(), time.Second)
}

// TestTransactionNoSnapshotForReadCommitted tests that no snapshot is created
// for READ COMMITTED transactions.
func TestTransactionNoSnapshotForReadCommitted(t *testing.T) {
	tm := NewTransactionManager()

	// Create a READ COMMITTED transaction
	txn := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)
	defer func() { _ = tm.Rollback(txn) }()

	// Transaction should NOT have a snapshot
	assert.False(t, txn.HasSnapshot())
	assert.Nil(t, txn.GetSnapshot())
}

// TestTransactionNoSnapshotForReadUncommitted tests that no snapshot is created
// for READ UNCOMMITTED transactions.
func TestTransactionNoSnapshotForReadUncommitted(t *testing.T) {
	tm := NewTransactionManager()

	// Create a READ UNCOMMITTED transaction
	txn := tm.BeginWithIsolation(parser.IsolationLevelReadUncommitted)
	defer func() { _ = tm.Rollback(txn) }()

	// Transaction should NOT have a snapshot
	assert.False(t, txn.HasSnapshot())
	assert.Nil(t, txn.GetSnapshot())
}

// TestTransactionSnapshotCapturesActiveTransactions tests that the snapshot
// captures which transactions are active when it's taken.
func TestTransactionSnapshotCapturesActiveTransactions(t *testing.T) {
	tm := NewTransactionManager()

	// Start two transactions (T1 and T2) before T3
	t1 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)
	t2 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)

	// Now start T3 with REPEATABLE READ - it should capture T1 and T2 as active
	t3 := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)

	// T3's snapshot should show T1 and T2 as active
	assert.True(t, t3.WasActiveAtSnapshot(t1.ID()),
		"T1 should be marked as active at T3's snapshot")
	assert.True(t, t3.WasActiveAtSnapshot(t2.ID()),
		"T2 should be marked as active at T3's snapshot")

	// T3 should NOT see itself as active (it's the current transaction)
	// Note: The transaction is added to active map after snapshot is taken
	// so it shouldn't be in the snapshot
	assert.False(t, t3.WasActiveAtSnapshot(t3.ID()),
		"T3 should not be in its own snapshot")

	// Cleanup
	_ = tm.Rollback(t1)
	_ = tm.Rollback(t2)
	_ = tm.Rollback(t3)
}

// TestTransactionSnapshotDoesNotIncludeCommittedTransactions tests that
// committed transactions are not in the active list.
func TestTransactionSnapshotDoesNotIncludeCommittedTransactions(t *testing.T) {
	tm := NewTransactionManager()

	// Start T1 and commit it
	t1 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)
	t1ID := t1.ID()
	err := tm.Commit(t1)
	require.NoError(t, err)

	// Start T2 with REPEATABLE READ
	t2 := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)
	defer func() { _ = tm.Rollback(t2) }()

	// T2's snapshot should NOT include T1 (it was committed)
	assert.False(t, t2.WasActiveAtSnapshot(t1ID),
		"Committed T1 should not be in T2's snapshot")
}

// TestTransactionStartTime tests that transactions have a start time.
func TestTransactionStartTime(t *testing.T) {
	tm := NewTransactionManager()

	beforeStart := time.Now()
	txn := tm.Begin()
	afterStart := time.Now()

	defer func() { _ = tm.Rollback(txn) }()

	startTime := txn.GetStartTime()
	assert.False(t, startTime.IsZero())
	assert.True(t, startTime.After(beforeStart) || startTime.Equal(beforeStart))
	assert.True(t, startTime.Before(afterStart) || startTime.Equal(afterStart))
}

// TestTransactionContextAdapterGetStartTime tests that the adapter returns start time.
func TestTransactionContextAdapterGetStartTime(t *testing.T) {
	tm := NewTransactionManager()
	txn := tm.Begin()
	defer func() { _ = tm.Rollback(txn) }()

	adapter := NewTransactionContextAdapter(txn, tm)

	// Adapter should return the transaction's start time
	assert.Equal(t, txn.GetStartTime(), adapter.GetStartTime())
	assert.False(t, adapter.GetStartTime().IsZero())
}

// TestTransactionContextAdapterGetSnapshot tests that the adapter exposes snapshot.
func TestTransactionContextAdapterGetSnapshot(t *testing.T) {
	tm := NewTransactionManager()

	// For REPEATABLE READ, snapshot should exist
	txn1 := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)
	adapter1 := NewTransactionContextAdapter(txn1, tm)
	assert.NotNil(t, adapter1.GetSnapshot())
	_ = tm.Rollback(txn1)

	// For READ COMMITTED, snapshot should be nil
	txn2 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)
	adapter2 := NewTransactionContextAdapter(txn2, tm)
	assert.Nil(t, adapter2.GetSnapshot())
	_ = tm.Rollback(txn2)
}

// TestTransactionContextAdapterWasActiveAtSnapshot tests the adapter's method.
func TestTransactionContextAdapterWasActiveAtSnapshot(t *testing.T) {
	tm := NewTransactionManager()

	// Start T1 (will be active when T2 starts)
	t1 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)

	// Start T2 with REPEATABLE READ
	t2 := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)
	adapter := NewTransactionContextAdapter(t2, tm)

	// T1 should be marked as active at T2's snapshot
	assert.True(t, adapter.WasActiveAtSnapshot(t1.ID()))

	// Non-existent transaction should not be active
	assert.False(t, adapter.WasActiveAtSnapshot(99999))

	// Cleanup
	_ = tm.Rollback(t1)
	_ = tm.Rollback(t2)
}

// TestTransactionManagerTakeSnapshot tests the public TakeSnapshot method.
func TestTransactionManagerTakeSnapshot(t *testing.T) {
	tm := NewTransactionManager()

	// Start some transactions
	t1 := tm.Begin()
	t2 := tm.Begin()

	// Take a snapshot
	snapshot := tm.TakeSnapshot()
	require.NotNil(t, snapshot)

	// Snapshot should include T1 and T2 as active
	assert.True(t, snapshot.WasActiveAtSnapshot(t1.ID()))
	assert.True(t, snapshot.WasActiveAtSnapshot(t2.ID()))

	// Snapshot timestamp should be approximately now
	assert.WithinDuration(t, time.Now(), snapshot.GetTimestamp(), time.Second)

	// Cleanup
	_ = tm.Rollback(t1)
	_ = tm.Rollback(t2)
}

// TestTransactionManagerGetActiveTransactionIDs tests the helper method.
func TestTransactionManagerGetActiveTransactionIDs(t *testing.T) {
	tm := NewTransactionManager()

	// Initially no active transactions
	ids := tm.GetActiveTransactionIDs()
	assert.Empty(t, ids)

	// Start some transactions
	t1 := tm.Begin()
	t2 := tm.Begin()

	// Now should have 2 active transactions
	ids = tm.GetActiveTransactionIDs()
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, t1.ID())
	assert.Contains(t, ids, t2.ID())

	// Commit T1
	_ = tm.Commit(t1)

	// Now should have 1 active transaction
	ids = tm.GetActiveTransactionIDs()
	assert.Len(t, ids, 1)
	assert.Contains(t, ids, t2.ID())
	assert.NotContains(t, ids, t1.ID())

	// Cleanup
	_ = tm.Rollback(t2)
}

// TestSnapshotImmutability tests that snapshots are immutable after creation.
func TestSnapshotImmutability(t *testing.T) {
	tm := NewTransactionManager()

	// Start T1
	t1 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)

	// Start T2 with REPEATABLE READ - captures T1 as active
	t2 := tm.BeginWithIsolation(parser.IsolationLevelRepeatableRead)
	snapshot := t2.GetSnapshot()

	// Verify T1 is in snapshot
	assert.True(t, snapshot.WasActiveAtSnapshot(t1.ID()))

	// Commit T1
	_ = tm.Commit(t1)

	// T2's snapshot should STILL show T1 as "was active at snapshot time"
	// The snapshot is a point-in-time capture and doesn't change
	assert.True(t, snapshot.WasActiveAtSnapshot(t1.ID()),
		"Snapshot should be immutable - T1 was active when snapshot was taken")

	// Start T3 after T1 committed
	t3 := tm.BeginWithIsolation(parser.IsolationLevelReadCommitted)

	// T3 should NOT be in T2's snapshot (started after snapshot was taken)
	assert.False(t, snapshot.WasActiveAtSnapshot(t3.ID()),
		"T3 started after snapshot, should not be in snapshot")

	// Cleanup
	_ = tm.Rollback(t2)
	_ = tm.Rollback(t3)
}

// TestConnectionRepeatableReadCreatesSnapshot tests that BEGIN with REPEATABLE READ
// creates a snapshot through the connection interface.
func TestConnectionRepeatableReadCreatesSnapshot(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Begin transaction with REPEATABLE READ
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ",
		nil,
	)
	require.NoError(t, err)

	// Transaction should have a snapshot
	assert.NotNil(t, engineConn.txn)
	assert.True(t, engineConn.txn.HasSnapshot())
	assert.NotNil(t, engineConn.txn.GetSnapshot())

	// Cleanup
	_, _ = engineConn.Execute(context.Background(), "ROLLBACK", nil)
}

// TestConnectionSerializableCreatesSnapshot tests that BEGIN with SERIALIZABLE
// creates a snapshot through the connection interface.
func TestConnectionSerializableCreatesSnapshot(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Begin transaction with SERIALIZABLE
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
		nil,
	)
	require.NoError(t, err)

	// Transaction should have a snapshot
	assert.NotNil(t, engineConn.txn)
	assert.True(t, engineConn.txn.HasSnapshot())
	assert.NotNil(t, engineConn.txn.GetSnapshot())

	// Cleanup
	_, _ = engineConn.Execute(context.Background(), "ROLLBACK", nil)
}

// TestConnectionReadCommittedNoSnapshot tests that BEGIN with READ COMMITTED
// does NOT create a snapshot.
func TestConnectionReadCommittedNoSnapshot(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Begin transaction with READ COMMITTED
	_, err = engineConn.Execute(
		context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED",
		nil,
	)
	require.NoError(t, err)

	// Transaction should NOT have a snapshot
	assert.NotNil(t, engineConn.txn)
	assert.False(t, engineConn.txn.HasSnapshot())
	assert.Nil(t, engineConn.txn.GetSnapshot())

	// Cleanup
	_, _ = engineConn.Execute(context.Background(), "ROLLBACK", nil)
}

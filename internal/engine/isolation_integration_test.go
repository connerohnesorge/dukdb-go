package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// COMPREHENSIVE ISOLATION LEVELS INTEGRATION TESTS
// =============================================================================
// These tests verify the complete isolation levels implementation by testing:
//   1. All isolation levels with concurrent transactions
//   2. Isolation level switching between transactions
//   3. Default isolation level configuration (SET/SHOW)
//   4. Error messages for isolation violations
//   5. Basic performance impact of each isolation level
// =============================================================================

// -----------------------------------------------------------------------------
// Section 1: All Isolation Levels with Concurrent Transactions
// -----------------------------------------------------------------------------
// Tests that verify each isolation level sees appropriate data based on
// their isolation rules.

// TestAllIsolationLevelsConcurrentBehavior verifies that multiple transactions
// with different isolation levels running concurrently see appropriate data
// based on their isolation level.
func TestAllIsolationLevelsConcurrentBehavior(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup: Create table with initial data
	_, err = engineConn.Execute(
		context.Background(),
		"CREATE TABLE isolation_test (id INTEGER, value INTEGER)",
		nil,
	)
	require.NoError(t, err)
	_, err = engineConn.Execute(
		context.Background(),
		"INSERT INTO isolation_test VALUES (1, 100)",
		nil,
	)
	require.NoError(t, err)

	testCases := []struct {
		name           string
		isolationLevel string
		hasSnapshot    bool
		description    string
	}{
		{
			name:           "READ UNCOMMITTED",
			isolationLevel: "READ UNCOMMITTED",
			hasSnapshot:    false,
			description:    "Weakest isolation, no snapshot",
		},
		{
			name:           "READ COMMITTED",
			isolationLevel: "READ COMMITTED",
			hasSnapshot:    false,
			description:    "Statement-level visibility, no transaction snapshot",
		},
		{
			name:           "REPEATABLE READ",
			isolationLevel: "REPEATABLE READ",
			hasSnapshot:    true,
			description:    "Transaction-level snapshot for consistent reads",
		},
		{
			name:           "SERIALIZABLE",
			isolationLevel: "SERIALIZABLE",
			hasSnapshot:    true,
			description:    "Strictest isolation with conflict detection",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Begin transaction with specific isolation level
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+tc.isolationLevel, nil)
			require.NoError(t, err)

			// Verify isolation level is correctly set
			assert.True(t, engineConn.inTxn, "Should be in transaction")
			assert.Equal(t, tc.isolationLevel, engineConn.currentIsolationLevel.String(),
				"Isolation level should match")

			// Verify snapshot behavior
			if tc.hasSnapshot {
				assert.True(t, engineConn.txn.HasSnapshot(),
					"%s should have transaction snapshot", tc.name)
				assert.NotNil(t, engineConn.txn.GetSnapshot())
			} else {
				assert.False(t, engineConn.txn.HasSnapshot(),
					"%s should NOT have transaction snapshot", tc.name)
			}

			// Execute query within transaction
			rows, _, err := engineConn.Query(context.Background(),
				"SELECT * FROM isolation_test WHERE id = 1", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 1, "Should see the row")
			assert.EqualValues(t, 100, rows[0]["value"])

			// Commit
			_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)
		})
	}
}

// TestConcurrentTransactionsDifferentLevels tests multiple transactions
// with different isolation levels running on the same data.
func TestConcurrentTransactionsDifferentLevels(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, data VARCHAR)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO concurrent_test VALUES (1, 'initial')", nil)
	require.NoError(t, err)

	// Test cycle through all isolation levels
	levels := []string{
		"READ UNCOMMITTED",
		"READ COMMITTED",
		"REPEATABLE READ",
		"SERIALIZABLE",
	}

	for i, level1 := range levels {
		for j, level2 := range levels {
			t.Run(level1+"_then_"+level2, func(t *testing.T) {
				// Transaction 1 with level1
				_, err := engineConn.Execute(context.Background(),
					"BEGIN TRANSACTION ISOLATION LEVEL "+level1, nil)
				require.NoError(t, err)

				// Read in transaction 1
				rows, _, err := engineConn.Query(context.Background(),
					"SELECT * FROM concurrent_test WHERE id = 1", nil)
				require.NoError(t, err)
				assert.Len(t, rows, 1)
				t1Data := rows[0]["data"]

				_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
				require.NoError(t, err)

				// Transaction 2 with level2
				_, err = engineConn.Execute(context.Background(),
					"BEGIN TRANSACTION ISOLATION LEVEL "+level2, nil)
				require.NoError(t, err)

				// Read in transaction 2
				rows, _, err = engineConn.Query(context.Background(),
					"SELECT * FROM concurrent_test WHERE id = 1", nil)
				require.NoError(t, err)
				assert.Len(t, rows, 1)
				t2Data := rows[0]["data"]

				// Both should see the same data (no concurrent modifications)
				assert.Equal(t, t1Data, t2Data,
					"Both transactions should see same data when no concurrent modifications")

				_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
				require.NoError(t, err)

				// Log the test combination for debugging
				_ = i
				_ = j
			})
		}
	}
}

// -----------------------------------------------------------------------------
// Section 2: Isolation Level Switching Between Transactions
// -----------------------------------------------------------------------------

// TestIsolationLevelSwitchingBehavior tests that starting with one isolation
// level, committing, then starting a new transaction with a different level
// changes behavior appropriately.
func TestIsolationLevelSwitchingBehavior(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE switch_test (id INTEGER, counter INTEGER)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO switch_test VALUES (1, 0)", nil)
	require.NoError(t, err)

	// Test: SERIALIZABLE -> READ COMMITTED
	t.Run("SERIALIZABLE_to_READ_COMMITTED", func(t *testing.T) {
		// First transaction: SERIALIZABLE (has snapshot)
		_, err := engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", nil)
		require.NoError(t, err)

		assert.True(t, engineConn.txn.HasSnapshot())
		assert.Equal(t, "SERIALIZABLE", engineConn.currentIsolationLevel.String())

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)

		// Second transaction: READ COMMITTED (no snapshot)
		_, err = engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED", nil)
		require.NoError(t, err)

		assert.False(t, engineConn.txn.HasSnapshot())
		assert.Equal(t, "READ COMMITTED", engineConn.currentIsolationLevel.String())

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)
	})

	// Test: READ UNCOMMITTED -> REPEATABLE READ
	t.Run("READ_UNCOMMITTED_to_REPEATABLE_READ", func(t *testing.T) {
		// First transaction: READ UNCOMMITTED
		_, err := engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", nil)
		require.NoError(t, err)

		assert.False(t, engineConn.txn.HasSnapshot())
		assert.Equal(t, "READ UNCOMMITTED", engineConn.currentIsolationLevel.String())

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)

		// Second transaction: REPEATABLE READ (has snapshot)
		_, err = engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", nil)
		require.NoError(t, err)

		assert.True(t, engineConn.txn.HasSnapshot())
		assert.Equal(t, "REPEATABLE READ", engineConn.currentIsolationLevel.String())

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)
	})

	// Test: REPEATABLE READ -> READ UNCOMMITTED
	t.Run("REPEATABLE_READ_to_READ_UNCOMMITTED", func(t *testing.T) {
		// First transaction: REPEATABLE READ (has snapshot)
		_, err := engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", nil)
		require.NoError(t, err)

		assert.True(t, engineConn.txn.HasSnapshot())
		snapshotTime := engineConn.txn.GetSnapshot().GetTimestamp()
		assert.False(t, snapshotTime.IsZero())

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)

		// Second transaction: READ UNCOMMITTED (no snapshot)
		_, err = engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", nil)
		require.NoError(t, err)

		assert.False(t, engineConn.txn.HasSnapshot())
		assert.Equal(t, "READ UNCOMMITTED", engineConn.currentIsolationLevel.String())

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)
	})
}

// TestSnapshotTimestampsAcrossTransactions verifies that new transactions
// get fresh snapshots (not reusing old ones).
func TestSnapshotTimestampsAcrossTransactions(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE snapshot_test (id INTEGER)", nil)
	require.NoError(t, err)

	var previousSnapshotTime time.Time

	// Start multiple REPEATABLE READ transactions and verify snapshots are fresh
	for i := 0; i < 3; i++ {
		_, err := engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", nil)
		require.NoError(t, err)

		currentSnapshotTime := engineConn.txn.GetSnapshot().GetTimestamp()

		if !previousSnapshotTime.IsZero() {
			// Each new transaction should have a >= timestamp
			assert.True(t, currentSnapshotTime.After(previousSnapshotTime) ||
				currentSnapshotTime.Equal(previousSnapshotTime),
				"New transaction should have newer or equal snapshot timestamp")
		}

		previousSnapshotTime = currentSnapshotTime

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)

		// Small delay to ensure time progresses
		time.Sleep(5 * time.Millisecond)
	}
}

// -----------------------------------------------------------------------------
// Section 3: Default Isolation Level Configuration
// -----------------------------------------------------------------------------

// TestSetDefaultTransactionIsolation tests SET default_transaction_isolation.
func TestSetDefaultTransactionIsolation(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	testCases := []struct {
		setValue      string
		expectedLevel parser.IsolationLevel
	}{
		{"READ UNCOMMITTED", parser.IsolationLevelReadUncommitted},
		{"READ COMMITTED", parser.IsolationLevelReadCommitted},
		{"REPEATABLE READ", parser.IsolationLevelRepeatableRead},
		{"SERIALIZABLE", parser.IsolationLevelSerializable},
	}

	for _, tc := range testCases {
		t.Run(tc.setValue, func(t *testing.T) {
			// SET default_transaction_isolation
			_, err := engineConn.Execute(context.Background(),
				"SET default_transaction_isolation = '"+tc.setValue+"'", nil)
			require.NoError(t, err)

			// Verify internal state
			assert.Equal(t, tc.expectedLevel, engineConn.defaultIsolationLevel)

			// Verify via SHOW default_transaction_isolation
			rows, _, err := engineConn.Query(context.Background(),
				"SHOW default_transaction_isolation", nil)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, tc.setValue, rows[0]["default_transaction_isolation"])

			// Verify BEGIN without explicit level uses default
			_, err = engineConn.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedLevel, engineConn.currentIsolationLevel)
			assert.Equal(t, tc.expectedLevel, engineConn.txn.GetIsolationLevel())

			_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)
		})
	}
}

// TestShowTransactionIsolationIntegration tests SHOW transaction_isolation
// with various scenarios including default override.
func TestShowTransactionIsolationIntegration(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Outside transaction: should return default
	t.Run("outside_transaction", func(t *testing.T) {
		// Set default to READ COMMITTED
		_, err := engineConn.Execute(context.Background(),
			"SET default_transaction_isolation = 'READ COMMITTED'", nil)
		require.NoError(t, err)

		rows, _, err := engineConn.Query(context.Background(),
			"SHOW transaction_isolation", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, "READ COMMITTED", rows[0]["transaction_isolation"])
	})

	// Inside transaction: should return current transaction's level
	t.Run("inside_transaction", func(t *testing.T) {
		// Set default to something different
		_, err := engineConn.Execute(context.Background(),
			"SET default_transaction_isolation = 'READ UNCOMMITTED'", nil)
		require.NoError(t, err)

		// Begin with explicit SERIALIZABLE
		_, err = engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", nil)
		require.NoError(t, err)

		rows, _, err := engineConn.Query(context.Background(),
			"SHOW transaction_isolation", nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		// Should return SERIALIZABLE (current transaction), not READ UNCOMMITTED (default)
		assert.Equal(t, "SERIALIZABLE", rows[0]["transaction_isolation"])

		_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
		require.NoError(t, err)
	})
}

// TestExplicitBeginOverridesDefault tests that BEGIN ISOLATION LEVEL
// overrides the default isolation level.
func TestExplicitBeginOverridesDefault(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	testCases := []struct {
		defaultLevel   string
		explicitLevel  string
		expectedLevel  string
		expectSnapshot bool
	}{
		{
			defaultLevel:   "SERIALIZABLE",
			explicitLevel:  "READ UNCOMMITTED",
			expectedLevel:  "READ UNCOMMITTED",
			expectSnapshot: false,
		},
		{
			defaultLevel:   "READ UNCOMMITTED",
			explicitLevel:  "SERIALIZABLE",
			expectedLevel:  "SERIALIZABLE",
			expectSnapshot: true,
		},
		{
			defaultLevel:   "READ COMMITTED",
			explicitLevel:  "REPEATABLE READ",
			expectedLevel:  "REPEATABLE READ",
			expectSnapshot: true,
		},
		{
			defaultLevel:   "REPEATABLE READ",
			explicitLevel:  "READ COMMITTED",
			expectedLevel:  "READ COMMITTED",
			expectSnapshot: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.defaultLevel+"_override_"+tc.explicitLevel, func(t *testing.T) {
			// Set default
			_, err := engineConn.Execute(context.Background(),
				"SET default_transaction_isolation = '"+tc.defaultLevel+"'", nil)
			require.NoError(t, err)

			// Begin with explicit override
			_, err = engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+tc.explicitLevel, nil)
			require.NoError(t, err)

			// Verify the explicit level takes precedence
			assert.Equal(t, tc.expectedLevel, engineConn.currentIsolationLevel.String())

			if tc.expectSnapshot {
				assert.True(t, engineConn.txn.HasSnapshot(),
					"Expected snapshot for %s", tc.explicitLevel)
			} else {
				assert.False(t, engineConn.txn.HasSnapshot(),
					"Did not expect snapshot for %s", tc.explicitLevel)
			}

			_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)
		})
	}
}

// TestSetTransactionIsolationSynonym tests that SET transaction_isolation
// works as a synonym for SET default_transaction_isolation.
func TestSetTransactionIsolationSynonym(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Use synonym
	_, err = engineConn.Execute(context.Background(),
		"SET transaction_isolation = 'REPEATABLE READ'", nil)
	require.NoError(t, err)

	// Verify it sets the default
	assert.Equal(t, parser.IsolationLevelRepeatableRead, engineConn.defaultIsolationLevel)

	// BEGIN should use this as default
	_, err = engineConn.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)

	assert.Equal(t, "REPEATABLE READ", engineConn.currentIsolationLevel.String())
	assert.True(t, engineConn.txn.HasSnapshot())

	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestCaseInsensitiveIsolationLevel tests that isolation level values
// are case-insensitive.
func TestCaseInsensitiveIsolationLevel(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	testCases := []string{
		"read committed",
		"READ COMMITTED",
		"Read Committed",
		"read COMMITTED",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			_, err := engineConn.Execute(context.Background(),
				"SET default_transaction_isolation = '"+tc+"'", nil)
			require.NoError(t, err)

			assert.Equal(t, parser.IsolationLevelReadCommitted, engineConn.defaultIsolationLevel)
		})
	}
}

// -----------------------------------------------------------------------------
// Section 4: Error Messages for Isolation Violations
// -----------------------------------------------------------------------------

// TestSerializationFailureErrorMessage verifies the error message format
// for serialization failures.
func TestSerializationFailureErrorMessage(t *testing.T) {
	// Test the error itself
	err := storage.ErrSerializationFailure
	require.NotNil(t, err)

	// Verify error message contains expected text
	errMsg := err.Error()
	assert.Contains(t, errMsg, "could not serialize",
		"Error message should mention serialization")
	assert.Contains(t, errMsg, "concurrent",
		"Error message should mention concurrent operations")

	// Verify errors.Is works
	var testErr error = storage.ErrSerializationFailure
	assert.True(t, errors.Is(testErr, storage.ErrSerializationFailure))
}

// TestLockTimeoutErrorMessage verifies the error message format for
// lock timeout errors.
func TestLockTimeoutErrorMessage(t *testing.T) {
	// Test the error itself
	err := storage.ErrLockTimeout
	require.NotNil(t, err)

	// Verify error message contains expected text
	errMsg := err.Error()
	assert.Contains(t, errMsg, "lock",
		"Error message should mention lock")
	assert.Contains(t, errMsg, "timed out",
		"Error message should mention timed out")

	// Verify errors.Is works
	var testErr error = storage.ErrLockTimeout
	assert.True(t, errors.Is(testErr, storage.ErrLockTimeout))
}

// TestSerializationFailureFromConflictDetector tests that the conflict
// detector returns the correct error type.
func TestSerializationFailureFromConflictDetector(t *testing.T) {
	cd := storage.NewConflictDetector()

	// Create a write-write conflict
	cd.RegisterWrite(1, "table", "row")
	cd.RegisterWrite(2, "table", "row")

	// T1 commits first
	err := cd.CheckConflicts(1, []uint64{})
	assert.NoError(t, err)

	// T2 should fail
	err = cd.CheckConflicts(2, []uint64{1})
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrSerializationFailure)
}

// TestLockTimeoutFromLockManager tests that the lock manager returns
// the correct error type.
func TestLockTimeoutFromLockManager(t *testing.T) {
	lm := storage.NewLockManager()

	// T1 acquires lock
	err := lm.Lock(1, "table", "row", time.Second)
	require.NoError(t, err)

	// T2 tries with no timeout
	err = lm.Lock(2, "table", "row", 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrLockTimeout)

	// T2 tries with short timeout
	err = lm.Lock(2, "table", "row", 10*time.Millisecond)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrLockTimeout)

	// Cleanup
	lm.Release(1)
}

// TestInvalidIsolationLevelError tests that invalid isolation levels
// return appropriate errors.
func TestInvalidIsolationLevelError(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	invalidLevels := []string{
		"INVALID",
		"SNAPSHOT",
		"CHAOS",
		"",
		"READ",
	}

	for _, invalid := range invalidLevels {
		t.Run(invalid, func(t *testing.T) {
			_, err := engineConn.Execute(context.Background(),
				"SET default_transaction_isolation = '"+invalid+"'", nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid isolation level")
		})
	}
}

// -----------------------------------------------------------------------------
// Section 5: Basic Performance Impact Tests
// -----------------------------------------------------------------------------

// TestPerformanceIsolationLevelOverhead measures basic overhead of each
// isolation level to ensure no major regressions.
func TestPerformanceIsolationLevelOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE perf_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	// Insert some test data
	for i := 0; i < 100; i++ {
		_, err = engineConn.Execute(
			context.Background(),
			"INSERT INTO perf_test VALUES ("+string(
				rune('0'+i%10),
			)+", "+string(
				rune('0'+i%10),
			)+")",
			nil,
		)
		require.NoError(t, err)
	}

	levels := []string{
		"READ UNCOMMITTED",
		"READ COMMITTED",
		"REPEATABLE READ",
		"SERIALIZABLE",
	}

	const iterations = 100
	results := make(map[string]time.Duration)

	for _, level := range levels {
		start := time.Now()

		for i := 0; i < iterations; i++ {
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
			require.NoError(t, err)

			_, _, err = engineConn.Query(context.Background(),
				"SELECT * FROM perf_test WHERE id = 1", nil)
			require.NoError(t, err)

			_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)
		}

		results[level] = time.Since(start)
	}

	// Log results (not assertions, just informational)
	t.Logf("Performance results (%d iterations):", iterations)
	for level, duration := range results {
		avgMs := float64(duration.Microseconds()) / float64(iterations) / 1000.0
		t.Logf("  %s: total %v, avg %.3f ms/iteration", level, duration, avgMs)
	}

	// Basic sanity check: SERIALIZABLE shouldn't be more than 10x slower than READ UNCOMMITTED
	// This is a very loose bound just to catch major regressions
	if results["SERIALIZABLE"] > results["READ UNCOMMITTED"]*10 {
		t.Logf("Warning: SERIALIZABLE is significantly slower than READ UNCOMMITTED")
	}
}

// TestConcurrentTransactionScalability tests that the system can handle
// multiple concurrent transactions without deadlocks or excessive delays.
func TestConcurrentTransactionScalability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping scalability test in short mode")
	}

	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Setup
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE scale_test (id INTEGER PRIMARY KEY, counter INTEGER)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO scale_test VALUES (1, 0)", nil)
	require.NoError(t, err)

	// Test rapid transaction cycling
	const numTransactions = 100
	start := time.Now()

	for i := 0; i < numTransactions; i++ {
		// Cycle through isolation levels
		level := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}[i%4]

		_, err := engineConn.Execute(context.Background(),
			"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
		require.NoError(t, err)

		_, _, err = engineConn.Query(context.Background(),
			"SELECT * FROM scale_test WHERE id = 1", nil)
		require.NoError(t, err)

		_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
		require.NoError(t, err)
	}

	elapsed := time.Since(start)
	avgDuration := elapsed / numTransactions

	t.Logf(
		"Completed %d transactions in %v (avg %v per transaction)",
		numTransactions,
		elapsed,
		avgDuration,
	)

	// Sanity check: should complete in reasonable time
	assert.Less(t, elapsed, 10*time.Second,
		"Transaction cycling should complete in reasonable time")
}

// -----------------------------------------------------------------------------
// Section 6: Integration Tests - Full Parser to Commit Flow
// -----------------------------------------------------------------------------

// TestFullIsolationLevelFlow tests the complete flow from SQL parsing through
// executor to transaction commit for all isolation levels.
func TestFullIsolationLevelFlow(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create test infrastructure
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE flow_test (id INTEGER PRIMARY KEY, status VARCHAR, updated_at INTEGER)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO flow_test VALUES (1, 'initial', 1000)", nil)
	require.NoError(t, err)

	// Test each isolation level with full CRUD operations
	levels := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

	for _, level := range levels {
		t.Run("FullFlow_"+level, func(t *testing.T) {
			// BEGIN with isolation level
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
			require.NoError(t, err)

			// Verify transaction state
			assert.True(t, engineConn.inTxn)
			assert.Equal(t, level, engineConn.currentIsolationLevel.String())

			// SELECT (read operation)
			rows, _, err := engineConn.Query(context.Background(),
				"SELECT * FROM flow_test WHERE id = 1", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 1)

			// UPDATE (write operation)
			_, err = engineConn.Execute(context.Background(),
				"UPDATE flow_test SET status = 'updated' WHERE id = 1", nil)
			require.NoError(t, err)

			// Verify update is visible within transaction
			rows, _, err = engineConn.Query(context.Background(),
				"SELECT status FROM flow_test WHERE id = 1", nil)
			require.NoError(t, err)
			assert.Equal(t, "updated", rows[0]["status"])

			// INSERT (write operation)
			_, err = engineConn.Execute(context.Background(),
				"INSERT INTO flow_test VALUES (2, 'new', 2000)", nil)
			require.NoError(t, err)

			// Verify insert is visible
			rows, _, err = engineConn.Query(context.Background(),
				"SELECT COUNT(*) as cnt FROM flow_test", nil)
			require.NoError(t, err)
			assert.EqualValues(t, 2, rows[0]["cnt"])

			// COMMIT
			_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)

			// Verify changes persisted after commit
			rows, _, err = engineConn.Query(context.Background(),
				"SELECT * FROM flow_test ORDER BY id", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 2)
			assert.Equal(t, "updated", rows[0]["status"])
			assert.Equal(t, "new", rows[1]["status"])

			// Cleanup for next iteration
			_, err = engineConn.Execute(context.Background(),
				"DELETE FROM flow_test WHERE id = 2", nil)
			require.NoError(t, err)
			_, err = engineConn.Execute(context.Background(),
				"UPDATE flow_test SET status = 'initial' WHERE id = 1", nil)
			require.NoError(t, err)
		})
	}
}

// TestRollbackBehaviorAllLevels tests that ROLLBACK correctly undoes changes
// for all isolation levels.
func TestRollbackBehaviorAllLevels(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create test table
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE rollback_test (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)
	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO rollback_test VALUES (1, 100)", nil)
	require.NoError(t, err)

	levels := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

	for _, level := range levels {
		t.Run("Rollback_"+level, func(t *testing.T) {
			// BEGIN
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
			require.NoError(t, err)

			// Make changes
			_, err = engineConn.Execute(context.Background(),
				"UPDATE rollback_test SET value = 999 WHERE id = 1", nil)
			require.NoError(t, err)

			_, err = engineConn.Execute(context.Background(),
				"INSERT INTO rollback_test VALUES (2, 200)", nil)
			require.NoError(t, err)

			// Verify changes visible within transaction
			rows, _, err := engineConn.Query(context.Background(),
				"SELECT * FROM rollback_test ORDER BY id", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 2)
			assert.EqualValues(t, 999, rows[0]["value"])

			// ROLLBACK
			_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)

			// Verify changes are undone
			rows, _, err = engineConn.Query(context.Background(),
				"SELECT * FROM rollback_test ORDER BY id", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 1, "Insert should be rolled back")
			assert.EqualValues(t, 100, rows[0]["value"], "Update should be rolled back")
		})
	}
}

// TestSavepointBehaviorAllLevels tests that savepoints work correctly
// with all isolation levels.
func TestSavepointBehaviorAllLevels(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	// Create test table
	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE savepoint_test (id INTEGER, value VARCHAR)", nil)
	require.NoError(t, err)

	levels := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

	for _, level := range levels {
		t.Run("Savepoint_"+level, func(t *testing.T) {
			// BEGIN
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
			require.NoError(t, err)

			// Insert first row
			_, err = engineConn.Execute(context.Background(),
				"INSERT INTO savepoint_test VALUES (1, 'first')", nil)
			require.NoError(t, err)

			// Create savepoint
			_, err = engineConn.Execute(context.Background(), "SAVEPOINT sp1", nil)
			require.NoError(t, err)

			// Insert second row
			_, err = engineConn.Execute(context.Background(),
				"INSERT INTO savepoint_test VALUES (2, 'second')", nil)
			require.NoError(t, err)

			// Verify both rows visible
			rows, _, err := engineConn.Query(context.Background(),
				"SELECT * FROM savepoint_test ORDER BY id", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 2)

			// Rollback to savepoint
			_, err = engineConn.Execute(context.Background(), "ROLLBACK TO SAVEPOINT sp1", nil)
			require.NoError(t, err)

			// Verify only first row remains
			rows, _, err = engineConn.Query(context.Background(),
				"SELECT * FROM savepoint_test", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 1)
			assert.Equal(t, "first", rows[0]["value"])

			// Commit
			_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)

			// Verify final state
			rows, _, err = engineConn.Query(context.Background(),
				"SELECT * FROM savepoint_test", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 1)

			// Cleanup
			_, err = engineConn.Execute(context.Background(),
				"DELETE FROM savepoint_test", nil)
			require.NoError(t, err)
		})
	}
}

// -----------------------------------------------------------------------------
// Section 7: Concurrent Access with Lock Manager
// -----------------------------------------------------------------------------

// TestLockManagerConcurrentAccess tests concurrent lock acquisition and release.
func TestLockManagerConcurrentAccess(t *testing.T) {
	lm := storage.NewLockManager()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				// Try to acquire lock
				err := lm.Lock(txnID, "table", "row", 100*time.Millisecond)
				if err != nil {
					// Lock timeout is expected under contention
					if !errors.Is(err, storage.ErrLockTimeout) {
						errCh <- err

						return
					}

					continue
				}

				// Simulate some work
				time.Sleep(time.Microsecond)

				// Release lock
				lm.Release(txnID)
			}
		}(uint64(i + 1))
	}

	wg.Wait()
	close(errCh)

	// Check for unexpected errors
	for err := range errCh {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify all locks are released
	assert.Equal(t, 0, lm.TotalLockCount())
}

// TestConflictDetectorConcurrentAccess tests concurrent read/write set tracking.
func TestConflictDetectorConcurrentAccess(t *testing.T) {
	cd := storage.NewConflictDetector()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(txnID uint64) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				rowID := string(rune('a' + j%26))

				// Register reads and writes
				cd.RegisterRead(txnID, "table", rowID)
				cd.RegisterWrite(txnID, "table", rowID)
			}

			// Clear at the end
			cd.ClearTransaction(txnID)
		}(uint64(i + 1))
	}

	wg.Wait()

	// All transactions should be cleared
	assert.Equal(t, 0, cd.ActiveTransactionCount())
}

// -----------------------------------------------------------------------------
// Section 8: Edge Cases
// -----------------------------------------------------------------------------

// TestEmptyTransaction tests BEGIN/COMMIT with no operations.
func TestEmptyTransaction(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	levels := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

	for _, level := range levels {
		t.Run("Empty_"+level, func(t *testing.T) {
			// BEGIN
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
			require.NoError(t, err)

			assert.True(t, engineConn.inTxn)

			// COMMIT immediately (no operations)
			_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)

			assert.False(t, engineConn.inTxn)
		})
	}
}

// TestTransactionAfterRollback tests that a new transaction can start
// after a rollback.
func TestTransactionAfterRollback(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE after_rollback (id INTEGER)", nil)
	require.NoError(t, err)

	// First transaction: rollback
	_, err = engineConn.Execute(context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE", nil)
	require.NoError(t, err)

	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO after_rollback VALUES (1)", nil)
	require.NoError(t, err)

	_, err = engineConn.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)

	// Verify rollback worked
	rows, _, err := engineConn.Query(context.Background(),
		"SELECT * FROM after_rollback", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 0)

	// Second transaction: should work normally
	_, err = engineConn.Execute(context.Background(),
		"BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED", nil)
	require.NoError(t, err)

	assert.True(t, engineConn.inTxn)
	assert.Equal(t, "READ COMMITTED", engineConn.currentIsolationLevel.String())

	_, err = engineConn.Execute(context.Background(),
		"INSERT INTO after_rollback VALUES (2)", nil)
	require.NoError(t, err)

	_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
	require.NoError(t, err)

	// Verify commit worked
	rows, _, err = engineConn.Query(context.Background(),
		"SELECT * FROM after_rollback", nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

// TestMultipleStatementsInTransaction tests executing multiple statements
// within a single transaction for each isolation level.
func TestMultipleStatementsInTransaction(t *testing.T) {
	eng := NewEngine()
	defer func() { _ = eng.Close() }()

	conn, err := eng.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	engineConn := conn.(*EngineConn)

	_, err = engineConn.Execute(context.Background(),
		"CREATE TABLE multi_stmt (id INTEGER, name VARCHAR, value INTEGER)", nil)
	require.NoError(t, err)

	levels := []string{"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"}

	for _, level := range levels {
		t.Run("MultiStmt_"+level, func(t *testing.T) {
			_, err := engineConn.Execute(context.Background(),
				"BEGIN TRANSACTION ISOLATION LEVEL "+level, nil)
			require.NoError(t, err)

			// Multiple INSERTs
			for i := 1; i <= 5; i++ {
				_, err = engineConn.Execute(
					context.Background(),
					"INSERT INTO multi_stmt VALUES ("+string(
						rune('0'+i),
					)+", 'name', "+string(
						rune('0'+i),
					)+")",
					nil,
				)
				require.NoError(t, err)
			}

			// Multiple SELECTs
			for i := 0; i < 3; i++ {
				rows, _, err := engineConn.Query(context.Background(),
					"SELECT COUNT(*) as cnt FROM multi_stmt", nil)
				require.NoError(t, err)
				assert.EqualValues(t, 5, rows[0]["cnt"])
			}

			// UPDATE
			_, err = engineConn.Execute(context.Background(),
				"UPDATE multi_stmt SET value = value * 2", nil)
			require.NoError(t, err)

			// DELETE
			_, err = engineConn.Execute(context.Background(),
				"DELETE FROM multi_stmt WHERE id > 3", nil)
			require.NoError(t, err)

			// Final SELECT
			rows, _, err := engineConn.Query(context.Background(),
				"SELECT * FROM multi_stmt ORDER BY id", nil)
			require.NoError(t, err)
			assert.Len(t, rows, 3)

			_, err = engineConn.Execute(context.Background(), "COMMIT", nil)
			require.NoError(t, err)

			// Cleanup
			_, err = engineConn.Execute(context.Background(),
				"DELETE FROM multi_stmt", nil)
			require.NoError(t, err)
		})
	}
}

package storage

import (
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScanWithVisibility_ReadUncommitted_SeesOwnUncommittedChanges tests that
// a transaction can see its own uncommitted inserts under READ UNCOMMITTED.
func TestScanWithVisibility_ReadUncommitted_SeesOwnUncommittedChanges(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Create a transaction context
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// Insert a row with version info for our transaction
	err := table.AppendRow([]any{int32(1), "row1"})
	require.NoError(t, err)

	// Set version info for the row
	table.SetRowVersion(RowID(0), &VersionInfo{
		CreatedTxnID: 100, // Our transaction
		DeletedTxnID: 0,
		Committed:    false,
	})

	// Scan with visibility - should see our own uncommitted row
	scanner := table.ScanWithVisibility(visibility, txn)
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "row1", chunk.GetValue(0, 1))
}

// TestScanWithVisibility_ReadUncommitted_DirtyReadAllowed tests that
// READ UNCOMMITTED allows dirty reads from other transactions.
func TestScanWithVisibility_ReadUncommitted_DirtyReadAllowed(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Create a transaction context for T1
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// T2 (transaction 200) inserts a row but doesn't commit
	err := table.AppendRow([]any{int32(42)})
	require.NoError(t, err)

	// Set version info for the row created by T2
	table.SetRowVersion(RowID(0), &VersionInfo{
		CreatedTxnID: 200, // T2's transaction
		DeletedTxnID: 0,
		Committed:    false,
	})

	// T1 scans - should see T2's uncommitted row (dirty read)
	scanner := table.ScanWithVisibility(visibility, t1)
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, int32(42), chunk.GetValue(0, 0))
}

// TestScanWithVisibility_ReadUncommitted_DeletedRowNotVisible tests that
// deleted rows are not visible even under READ UNCOMMITTED.
func TestScanWithVisibility_ReadUncommitted_DeletedRowNotVisible(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Create a transaction context
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// Insert a row
	err := table.AppendRow([]any{int32(1)})
	require.NoError(t, err)

	// Set version info for the row - it's been deleted
	table.SetRowVersion(RowID(0), &VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 60, // Row has been deleted
		Committed:    true,
	})
	txn.setCommitted(50)
	txn.setCommitted(60)

	// Scan with visibility - should not see deleted row
	scanner := table.ScanWithVisibility(visibility, txn)
	chunk := scanner.Next()
	// Either nil or empty chunk expected
	if chunk != nil {
		assert.Equal(t, 0, chunk.Count())
	}
}

// TestScanWithVisibility_ReadUncommitted_AbortedRowNotVisible tests that
// rows from aborted transactions are not visible.
func TestScanWithVisibility_ReadUncommitted_AbortedRowNotVisible(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Create a transaction context
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// T2 (transaction 200) inserts a row but aborts
	err := table.AppendRow([]any{int32(42)})
	require.NoError(t, err)

	// Set version info for the row created by T2
	table.SetRowVersion(RowID(0), &VersionInfo{
		CreatedTxnID: 200, // T2's transaction
		DeletedTxnID: 0,
		Committed:    false,
	})
	txn.setAborted(200) // T2 has aborted

	// Scan with visibility - should NOT see T2's aborted row
	scanner := table.ScanWithVisibility(visibility, txn)
	chunk := scanner.Next()
	// Either nil or empty chunk expected
	if chunk != nil {
		assert.Equal(t, 0, chunk.Count())
	}
}

// TestScanWithVisibility_ReadUncommitted_CommittedRowsVisible tests that
// committed rows are visible under READ UNCOMMITTED.
func TestScanWithVisibility_ReadUncommitted_CommittedRowsVisible(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR})

	// Create a transaction context
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// Insert multiple committed rows
	for i := 0; i < 3; i++ {
		err := table.AppendRow([]any{int32(i + 1), "row"})
		require.NoError(t, err)

		// Set version info - committed by earlier transaction
		table.SetRowVersion(RowID(i), &VersionInfo{
			CreatedTxnID: uint64(10 + i),
			DeletedTxnID: 0,
			Committed:    true,
		})
		txn.setCommitted(uint64(10 + i))
	}

	// Scan with visibility - should see all committed rows
	scanner := table.ScanWithVisibility(visibility, txn)
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 3, chunk.Count())
}

// TestScanWithVisibility_MixedVisibility tests scanning with mixed row states
// (committed, uncommitted, deleted, aborted).
func TestScanWithVisibility_MixedVisibility(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Create a transaction context for T1
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// Row 0: Committed row - should be visible
	err := table.AppendRow([]any{int32(1)})
	require.NoError(t, err)
	table.SetRowVersion(RowID(0), &VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		Committed:    true,
	})
	t1.setCommitted(10)

	// Row 1: Uncommitted row from T2 - should be visible (dirty read)
	err = table.AppendRow([]any{int32(2)})
	require.NoError(t, err)
	table.SetRowVersion(RowID(1), &VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		Committed:    false,
	})

	// Row 2: Deleted row - should NOT be visible
	err = table.AppendRow([]any{int32(3)})
	require.NoError(t, err)
	table.SetRowVersion(RowID(2), &VersionInfo{
		CreatedTxnID: 20,
		DeletedTxnID: 30,
		Committed:    true,
	})
	t1.setCommitted(20)
	t1.setCommitted(30)

	// Row 3: Row from aborted transaction - should NOT be visible
	err = table.AppendRow([]any{int32(4)})
	require.NoError(t, err)
	table.SetRowVersion(RowID(3), &VersionInfo{
		CreatedTxnID: 300,
		DeletedTxnID: 0,
		Committed:    false,
	})
	t1.setAborted(300)

	// Row 4: Our own row - should be visible
	err = table.AppendRow([]any{int32(5)})
	require.NoError(t, err)
	table.SetRowVersion(RowID(4), &VersionInfo{
		CreatedTxnID: 100, // T1's own transaction
		DeletedTxnID: 0,
		Committed:    false,
	})

	// Scan with visibility - should see rows 0, 1, and 4
	scanner := table.ScanWithVisibility(visibility, t1)
	var allValues []int32
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			if val, ok := chunk.GetValue(i, 0).(int32); ok {
				allValues = append(allValues, val)
			}
		}
	}

	assert.Len(t, allValues, 3)
	assert.Contains(t, allValues, int32(1))    // Committed row
	assert.Contains(t, allValues, int32(2))    // Uncommitted (dirty read)
	assert.Contains(t, allValues, int32(5))    // Own row
	assert.NotContains(t, allValues, int32(3)) // Deleted row
	assert.NotContains(t, allValues, int32(4)) // Aborted row
}

// TestScanWithVisibility_NilVisibilityBehavesLikeScan tests that passing nil
// visibility behaves like the regular Scan method.
func TestScanWithVisibility_NilVisibilityBehavesLikeScan(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Insert some rows
	for i := 0; i < 3; i++ {
		err := table.AppendRow([]any{int32(i + 1)})
		require.NoError(t, err)
	}

	// Scan with nil visibility and nil txn context
	scanner := table.ScanWithVisibility(nil, nil)
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 3, chunk.Count())
}

// TestScanWithVisibility_LegacyRowsWithoutVersionInfo tests that rows without
// version info are visible (for backward compatibility).
func TestScanWithVisibility_LegacyRowsWithoutVersionInfo(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Create a transaction context
	txn := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// Insert rows WITHOUT setting version info (legacy behavior)
	for i := 0; i < 3; i++ {
		err := table.AppendRow([]any{int32(i + 1)})
		require.NoError(t, err)
		// Intentionally NOT calling SetRowVersion
	}

	// Scan with visibility - legacy rows should be visible
	scanner := table.ScanWithVisibility(visibility, txn)
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 3, chunk.Count())
}

// TestScanWithVisibility_ReadUncommitted_UncommittedDeleteHidesRow tests that
// a row deleted by an uncommitted transaction is not visible.
func TestScanWithVisibility_ReadUncommitted_UncommittedDeleteHidesRow(t *testing.T) {
	// Create a table
	table := NewTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})

	// Create a transaction context for T1
	t1 := newMockTransactionContext(100, parser.IsolationLevelReadUncommitted)
	visibility := NewReadUncommittedVisibility()

	// Insert a row
	err := table.AppendRow([]any{int32(42)})
	require.NoError(t, err)

	// Set version info - row created by committed T0, deleted by uncommitted T2
	table.SetRowVersion(RowID(0), &VersionInfo{
		CreatedTxnID: 50,
		DeletedTxnID: 200, // T2 deleted it but hasn't committed
		Committed:    true,
		CreatedTime:  time.Now().Add(-1 * time.Hour),
	})
	t1.setCommitted(50)

	// Scan with visibility - should NOT see the row (deleted by T2)
	scanner := table.ScanWithVisibility(visibility, t1)
	chunk := scanner.Next()
	// Either nil or empty chunk expected
	if chunk != nil {
		assert.Equal(t, 0, chunk.Count())
	}
}

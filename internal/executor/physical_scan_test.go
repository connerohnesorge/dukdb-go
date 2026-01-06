package executor

import (
	"testing"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhysicalScanOperator_Basic(
	t *testing.T,
) {
	// Create a table with some test data
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable(
		"test_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert some test data
	require.NoError(
		t,
		table.AppendRow([]any{int32(1), "Alice"}),
	)
	require.NoError(
		t,
		table.AppendRow([]any{int32(2), "Bob"}),
	)
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(3), "Charlie"},
		),
	)

	// Create table definition for catalog
	tableDef := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef(
				"name",
				dukdb.TYPE_VARCHAR,
			),
		},
	)

	// Create a PhysicalScan plan
	plan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}

	// Create the scan operator
	scanOp, err := NewPhysicalScanOperator(
		plan,
		stor,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Test GetTypes()
	types := scanOp.GetTypes()
	assert.Equal(t, 2, len(types))
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		types[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		types[1].InternalType(),
	)

	// Test Next() - should return chunks with data
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(
		t,
		chunk,
		"First chunk should not be nil",
	)
	assert.Equal(
		t,
		3,
		chunk.Count(),
		"Should have 3 rows",
	)
	assert.Equal(
		t,
		2,
		chunk.ColumnCount(),
		"Should have 2 columns",
	)

	// Verify data
	assert.Equal(
		t,
		int32(1),
		chunk.GetValue(0, 0),
	)
	assert.Equal(t, "Alice", chunk.GetValue(0, 1))
	assert.Equal(
		t,
		int32(2),
		chunk.GetValue(1, 0),
	)
	assert.Equal(t, "Bob", chunk.GetValue(1, 1))
	assert.Equal(
		t,
		int32(3),
		chunk.GetValue(2, 0),
	)
	assert.Equal(
		t,
		"Charlie",
		chunk.GetValue(2, 1),
	)

	// Test Next() again - should return nil (no more data)
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(
		t,
		chunk,
		"Second chunk should be nil (no more data)",
	)
}

func TestPhysicalScanOperator_WithProjections(
	t *testing.T,
) {
	// Create a table with some test data
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BOOLEAN,
	}
	table, err := stor.CreateTable(
		"test_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert some test data
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(1), "Alice", true},
		),
	)
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(2), "Bob", false},
		),
	)

	// Create table definition for catalog
	tableDef := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef(
				"name",
				dukdb.TYPE_VARCHAR,
			),
			catalog.NewColumnDef(
				"active",
				dukdb.TYPE_BOOLEAN,
			),
		},
	)

	// Create a PhysicalScan plan with projections (only columns 0 and 2)
	plan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
		Projections: []int{
			0,
			2,
		}, // Select id and active, skip name
	}

	// Create the scan operator
	scanOp, err := NewPhysicalScanOperator(
		plan,
		stor,
	)
	require.NoError(t, err)

	// Test GetTypes() - should only have 2 types
	types := scanOp.GetTypes()
	assert.Equal(t, 2, len(types))
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		types[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_BOOLEAN,
		types[1].InternalType(),
	)

	// Test Next() - should return projected columns only
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(
		t,
		2,
		chunk.ColumnCount(),
		"Should have 2 columns (projected)",
	)

	// Verify projected data
	assert.Equal(
		t,
		int32(1),
		chunk.GetValue(0, 0),
	)
	assert.Equal(t, true, chunk.GetValue(0, 1))
	assert.Equal(
		t,
		int32(2),
		chunk.GetValue(1, 0),
	)
	assert.Equal(t, false, chunk.GetValue(1, 1))
}

func TestPhysicalScanOperator_EmptyTable(
	t *testing.T,
) {
	// Create an empty table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
	}
	_, err := stor.CreateTable(
		"empty_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Create table definition for catalog
	tableDef := catalog.NewTableDef(
		"empty_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
		},
	)

	// Create a PhysicalScan plan
	plan := &planner.PhysicalScan{
		TableName: "empty_table",
		TableDef:  tableDef,
	}

	// Create the scan operator
	scanOp, err := NewPhysicalScanOperator(
		plan,
		stor,
	)
	require.NoError(t, err)

	// Test Next() - should return nil for empty table
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	assert.Nil(
		t,
		chunk,
		"Should return nil for empty table",
	)
}

func TestPhysicalScanOperator_MultipleChunks(
	t *testing.T,
) {
	// Create a table and insert enough data to span multiple chunks
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
	}
	table, err := stor.CreateTable(
		"large_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert more rows than StandardVectorSize to ensure multiple chunks
	numRows := storage.StandardVectorSize + 100
	for i := range numRows {
		require.NoError(
			t,
			table.AppendRow([]any{int32(i)}),
		)
	}

	// Create table definition for catalog
	tableDef := catalog.NewTableDef(
		"large_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
		},
	)

	// Create a PhysicalScan plan
	plan := &planner.PhysicalScan{
		TableName: "large_table",
		TableDef:  tableDef,
	}

	// Create the scan operator
	scanOp, err := NewPhysicalScanOperator(
		plan,
		stor,
	)
	require.NoError(t, err)

	// Read all chunks and count total rows
	totalRows := 0
	chunkCount := 0
	for {
		chunk, err := scanOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunkCount++
		totalRows += chunk.Count()
	}

	assert.Equal(
		t,
		numRows,
		totalRows,
		"Should read all rows",
	)
	assert.GreaterOrEqual(
		t,
		chunkCount,
		2,
		"Should have multiple chunks",
	)
}

func TestPhysicalScanOperator_TableNotFound(
	t *testing.T,
) {
	stor := storage.NewStorage()

	// Create table definition for catalog
	tableDef := catalog.NewTableDef(
		"nonexistent_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
		},
	)

	// Create a PhysicalScan plan for a table that doesn't exist
	plan := &planner.PhysicalScan{
		TableName: "nonexistent_table",
		TableDef:  tableDef,
	}

	// Creating the scan operator should fail
	_, err := NewPhysicalScanOperator(plan, stor)
	assert.ErrorIs(t, err, dukdb.ErrTableNotFound)
}

func TestPhysicalScanOperator_WithVisibility(t *testing.T) {
	// Create a table with some test data
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable(
		"test_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert rows with version info
	// Row 0: Committed row - should be visible
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice"}))
	table.SetRowVersion(storage.RowID(0), &storage.VersionInfo{
		CreatedTxnID: 10,
		DeletedTxnID: 0,
		Committed:    true,
	})

	// Row 1: Uncommitted row from another transaction - should be visible with dirty read
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob"}))
	table.SetRowVersion(storage.RowID(1), &storage.VersionInfo{
		CreatedTxnID: 200,
		DeletedTxnID: 0,
		Committed:    false,
	})

	// Row 2: Deleted row - should NOT be visible
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie"}))
	table.SetRowVersion(storage.RowID(2), &storage.VersionInfo{
		CreatedTxnID: 20,
		DeletedTxnID: 30,
		Committed:    true,
	})

	// Create table definition for catalog
	tableDef := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		},
	)

	// Create a PhysicalScan plan
	plan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}

	// Create visibility checker and mock transaction context
	visibility := storage.NewReadUncommittedVisibility()
	txnCtx := newMockTxnContext(100)
	txnCtx.committed[10] = true
	txnCtx.committed[20] = true
	txnCtx.committed[30] = true

	// Create the scan operator with visibility
	scanOp, err := NewPhysicalScanOperatorWithVisibility(
		plan,
		stor,
		visibility,
		txnCtx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Test Next() - should return chunks with visible data only
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Chunk should not be nil")

	// Should have 2 visible rows (row 0 and row 1, but not row 2 which is deleted)
	assert.Equal(t, 2, chunk.Count(), "Should have 2 visible rows")

	// Verify data
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "Alice", chunk.GetValue(0, 1))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "Bob", chunk.GetValue(1, 1))

	// Test Next() again - should return nil (no more data)
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Second chunk should be nil (no more data)")
}

func TestPhysicalScanOperator_WithVisibility_NilParameters(t *testing.T) {
	// Test that nil visibility parameters behave like regular scan
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	table, err := stor.CreateTable("test_table", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(1)}))
	require.NoError(t, table.AppendRow([]any{int32(2)}))

	tableDef := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		},
	)

	plan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}

	// Create with nil visibility - should work like regular scan
	scanOp, err := NewPhysicalScanOperatorWithVisibility(plan, stor, nil, nil)
	require.NoError(t, err)

	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.Count())
}

// mockTxnContext implements storage.TransactionContext for testing.
type mockTxnContext struct {
	txnID          uint64
	isolationLevel parser.IsolationLevel
	startTime      time.Time
	statementTime  time.Time
	committed      map[uint64]bool
	aborted        map[uint64]bool
}

func newMockTxnContext(txnID uint64) *mockTxnContext {
	now := time.Now()
	return &mockTxnContext{
		txnID:          txnID,
		isolationLevel: parser.IsolationLevelReadUncommitted,
		startTime:      now,
		statementTime:  now,
		committed:      make(map[uint64]bool),
		aborted:        make(map[uint64]bool),
	}
}

func (m *mockTxnContext) GetTxnID() uint64 {
	return m.txnID
}

func (m *mockTxnContext) GetIsolationLevel() parser.IsolationLevel {
	return m.isolationLevel
}

func (m *mockTxnContext) GetStartTime() time.Time {
	return m.startTime
}

func (m *mockTxnContext) GetStatementTime() time.Time {
	return m.statementTime
}

func (m *mockTxnContext) IsCommitted(txnID uint64) bool {
	return m.committed[txnID]
}

func (m *mockTxnContext) IsAborted(txnID uint64) bool {
	return m.aborted[txnID]
}

func (m *mockTxnContext) GetSnapshot() *storage.Snapshot {
	return nil // Mock returns nil for basic testing
}

func (m *mockTxnContext) WasActiveAtSnapshot(txnID uint64) bool {
	return false // Mock returns false for basic testing
}

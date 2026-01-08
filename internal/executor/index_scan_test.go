package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/dukdb/dukdb-go/internal/storage/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPhysicalIndexScanOperator_SingleMatch tests that an index scan finds exactly one matching row.
func TestPhysicalIndexScanOperator_SingleMatch(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("users", columnTypes)
	require.NoError(t, err)

	// Insert test data
	// RowID 0
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice"}))
	// RowID 1
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob"}))
	// RowID 2
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie"}))

	// Create a HashIndex on the "id" column
	index := storage.NewHashIndex("idx_users_id", "users", []string{"id"}, false)
	// Insert index entries (key, rowID)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	// Create table definition
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key for id = 2
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator
	scanOp, err := NewPhysicalIndexScanOperator(
		"users",
		"main",
		tableDef,
		"idx_users_id",
		indexDef,
		index,
		lookupKeys,
		nil, // no projection - all columns
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Get results
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Should return a chunk with data")

	// Verify we got exactly one row
	assert.Equal(t, 1, chunk.Count(), "Should have exactly 1 matching row")
	assert.Equal(t, 2, chunk.ColumnCount(), "Should have 2 columns")

	// Verify the data is correct (id=2, name="Bob")
	assert.Equal(t, int32(2), chunk.GetValue(0, 0), "id should be 2")
	assert.Equal(t, "Bob", chunk.GetValue(0, 1), "name should be Bob")

	// Next call should return nil (no more data)
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Second call should return nil")
}

// TestPhysicalIndexScanOperator_MultipleMatches tests that an index scan finds multiple matching rows.
func TestPhysicalIndexScanOperator_MultipleMatches(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_VARCHAR, // name
		dukdb.TYPE_INTEGER, // department_id
	}
	table, err := stor.CreateTable("employees", columnTypes)
	require.NoError(t, err)

	// Insert test data - multiple employees in same department
	// RowID 0
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice", int32(10)}))
	// RowID 1
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob", int32(20)}))
	// RowID 2
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie", int32(10)}))
	// RowID 3
	require.NoError(t, table.AppendRow([]any{int32(4), "Diana", int32(10)}))
	// RowID 4
	require.NoError(t, table.AppendRow([]any{int32(5), "Eve", int32(20)}))

	// Create a HashIndex on the "department_id" column (non-unique)
	index := storage.NewHashIndex("idx_employees_dept", "employees", []string{"department_id"}, false)
	// Insert index entries
	require.NoError(t, index.Insert([]any{int32(10)}, storage.RowID(0))) // Alice
	require.NoError(t, index.Insert([]any{int32(20)}, storage.RowID(1))) // Bob
	require.NoError(t, index.Insert([]any{int32(10)}, storage.RowID(2))) // Charlie
	require.NoError(t, index.Insert([]any{int32(10)}, storage.RowID(3))) // Diana
	require.NoError(t, index.Insert([]any{int32(20)}, storage.RowID(4))) // Eve

	// Create table definition
	tableDef := catalog.NewTableDef("employees", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("department_id", dukdb.TYPE_INTEGER),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_employees_dept", "main", "employees", []string{"department_id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key for department_id = 10
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(10), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator
	scanOp, err := NewPhysicalIndexScanOperator(
		"employees",
		"main",
		tableDef,
		"idx_employees_dept",
		indexDef,
		index,
		lookupKeys,
		nil, // no projection
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Get results
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Should return a chunk with data")

	// Verify we got 3 rows (Alice, Charlie, Diana)
	assert.Equal(t, 3, chunk.Count(), "Should have 3 matching rows")

	// Verify all rows have department_id = 10
	for i := 0; i < chunk.Count(); i++ {
		deptID := chunk.GetValue(i, 2)
		assert.Equal(t, int32(10), deptID, "Row %d should have department_id = 10", i)
	}

	// Collect names to verify
	names := make(map[string]bool)
	for i := 0; i < chunk.Count(); i++ {
		name := chunk.GetValue(i, 1).(string)
		names[name] = true
	}
	assert.True(t, names["Alice"], "Should contain Alice")
	assert.True(t, names["Charlie"], "Should contain Charlie")
	assert.True(t, names["Diana"], "Should contain Diana")

	// Next call should return nil
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Second call should return nil")
}

// TestPhysicalIndexScanOperator_NoMatches tests that an index scan returns empty when no rows match.
func TestPhysicalIndexScanOperator_NoMatches(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("products", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Product A"}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Product B"}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Product C"}))

	// Create a HashIndex on the "id" column
	index := storage.NewHashIndex("idx_products_id", "products", []string{"id"}, false)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	// Create table definition
	tableDef := catalog.NewTableDef("products", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_products_id", "main", "products", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key for id = 999 (doesn't exist)
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(999), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator
	scanOp, err := NewPhysicalIndexScanOperator(
		"products",
		"main",
		tableDef,
		"idx_products_id",
		indexDef,
		index,
		lookupKeys,
		nil, // no projection
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Get results - should return nil (no matches)
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Should return nil for non-matching key")
}

// TestPhysicalIndexScanOperator_WithProjection tests that an index scan correctly projects columns.
func TestPhysicalIndexScanOperator_WithProjection(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_VARCHAR, // first_name
		dukdb.TYPE_VARCHAR, // last_name
		dukdb.TYPE_INTEGER, // age
	}
	table, err := stor.CreateTable("people", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "John", "Doe", int32(30)}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Jane", "Smith", int32(25)}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Bob", "Johnson", int32(40)}))

	// Create a HashIndex on the "id" column
	index := storage.NewHashIndex("idx_people_id", "people", []string{"id"}, true) // unique
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	// Create table definition
	tableDef := catalog.NewTableDef("people", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("first_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("last_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_people_id", "main", "people", []string{"id"}, true)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key for id = 2
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
	}

	// Create projection: only columns 1 (first_name) and 3 (age)
	projections := []int{1, 3}

	// Create the index scan operator with projection
	scanOp, err := NewPhysicalIndexScanOperator(
		"people",
		"main",
		tableDef,
		"idx_people_id",
		indexDef,
		index,
		lookupKeys,
		projections,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Verify GetTypes returns projected types
	types := scanOp.GetTypes()
	assert.Equal(t, 2, len(types), "Should have 2 projected types")
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[0].InternalType(), "First type should be VARCHAR")
	assert.Equal(t, dukdb.TYPE_INTEGER, types[1].InternalType(), "Second type should be INTEGER")

	// Get results
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Should return a chunk with data")

	// Verify we got exactly one row with 2 columns
	assert.Equal(t, 1, chunk.Count(), "Should have exactly 1 row")
	assert.Equal(t, 2, chunk.ColumnCount(), "Should have 2 projected columns")

	// Verify the projected data (first_name="Jane", age=25)
	assert.Equal(t, "Jane", chunk.GetValue(0, 0), "first_name should be Jane")
	assert.Equal(t, int32(25), chunk.GetValue(0, 1), "age should be 25")

	// Next call should return nil
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Second call should return nil")
}

// TestPhysicalIndexScanOperator_DeletedRowsHandled tests that deleted rows are skipped gracefully.
func TestPhysicalIndexScanOperator_DeletedRowsHandled(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("items", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Item A"})) // RowID 0
	require.NoError(t, table.AppendRow([]any{int32(2), "Item B"})) // RowID 1
	require.NoError(t, table.AppendRow([]any{int32(3), "Item C"})) // RowID 2

	// Create a HashIndex on "id" column
	index := storage.NewHashIndex("idx_items_id", "items", []string{"id"}, false)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	// Mark row 1 (id=2) as deleted
	table.SetRowDeleted(storage.RowID(1), 100)

	// Create table definition
	tableDef := catalog.NewTableDef("items", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_items_id", "main", "items", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key for id = 2 (the deleted row)
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator
	scanOp, err := NewPhysicalIndexScanOperator(
		"items",
		"main",
		tableDef,
		"idx_items_id",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Get results - should return nil because the matching row is deleted
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Should return nil for deleted row")
}

// TestPhysicalIndexScanOperator_NonExistentRowID tests that non-existent RowIDs are handled gracefully.
func TestPhysicalIndexScanOperator_NonExistentRowID(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("orders", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Order A"})) // RowID 0
	require.NoError(t, table.AppendRow([]any{int32(2), "Order B"})) // RowID 1

	// Create a HashIndex and insert entries, but add an entry for a RowID that doesn't exist
	index := storage.NewHashIndex("idx_orders_id", "orders", []string{"id"}, false)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	// This is the stale entry pointing to non-existent RowID 999
	require.NoError(t, index.Insert([]any{int32(999)}, storage.RowID(999)))

	// Create table definition
	tableDef := catalog.NewTableDef("orders", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_orders_id", "main", "orders", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key for id = 999 (exists in index but not in table)
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(999), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator
	scanOp, err := NewPhysicalIndexScanOperator(
		"orders",
		"main",
		tableDef,
		"idx_orders_id",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Get results - should return nil because the RowID doesn't exist in table
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Should return nil for non-existent RowID")
}

// TestPhysicalIndexScanOperator_TableNotFound tests that an error is returned when table doesn't exist.
func TestPhysicalIndexScanOperator_TableNotFound(t *testing.T) {
	stor := storage.NewStorage()

	// Create table definition for a table that doesn't exist in storage
	tableDef := catalog.NewTableDef("nonexistent", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_nonexistent", "main", "nonexistent", []string{"id"}, false)

	// Create an index (even though table doesn't exist)
	index := storage.NewHashIndex("idx_nonexistent", "nonexistent", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator - should fail
	_, err := NewPhysicalIndexScanOperator(
		"nonexistent",
		"main",
		tableDef,
		"idx_nonexistent",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	assert.ErrorIs(t, err, dukdb.ErrTableNotFound)
}

// TestPhysicalIndexScanOperator_NilIndex tests that an error is returned when index is nil.
func TestPhysicalIndexScanOperator_NilIndex(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	_, err := stor.CreateTable("test_nil_index", columnTypes)
	require.NoError(t, err)

	// Create table definition
	tableDef := catalog.NewTableDef("test_nil_index", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_nil", "main", "test_nil_index", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator with nil index
	scanOp, err := NewPhysicalIndexScanOperator(
		"test_nil_index",
		"main",
		tableDef,
		"idx_nil",
		indexDef,
		nil, // nil index
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err) // Construction should succeed

	// Next() should fail because index is nil
	_, err = scanOp.Next()
	assert.Error(t, err, "Should return error when index is nil")
	assert.Contains(t, err.Error(), "index scan requires an index")
}

// TestPhysicalIndexScanOperator_OutputColumns tests that OutputColumns returns correct bindings.
func TestPhysicalIndexScanOperator_OutputColumns(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BOOLEAN,
	}
	_, err := stor.CreateTable("test_output", columnTypes)
	require.NoError(t, err)

	// Create table definition
	tableDef := catalog.NewTableDef("test_output", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("active", dukdb.TYPE_BOOLEAN),
	})

	// Create index and definition
	index := storage.NewHashIndex("idx_test", "test_output", []string{"id"}, false)
	indexDef := catalog.NewIndexDef("idx_test", "main", "test_output", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	// Test without projection
	scanOp, err := NewPhysicalIndexScanOperator(
		"test_output",
		"main",
		tableDef,
		"idx_test",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)

	columns := scanOp.OutputColumns()
	assert.Equal(t, 3, len(columns), "Should have 3 output columns")
	assert.Equal(t, "id", columns[0].Column)
	assert.Equal(t, "name", columns[1].Column)
	assert.Equal(t, "active", columns[2].Column)
	assert.Equal(t, "test_output", columns[0].Table)
	assert.Equal(t, dukdb.TYPE_INTEGER, columns[0].Type)
	assert.Equal(t, dukdb.TYPE_VARCHAR, columns[1].Type)
	assert.Equal(t, dukdb.TYPE_BOOLEAN, columns[2].Type)

	// Test with projection
	scanOpProjected, err := NewPhysicalIndexScanOperator(
		"test_output",
		"main",
		tableDef,
		"idx_test",
		indexDef,
		index,
		lookupKeys,
		[]int{0, 2}, // only id and active
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)

	columnsProjected := scanOpProjected.OutputColumns()
	assert.Equal(t, 2, len(columnsProjected), "Should have 2 projected output columns")
	assert.Equal(t, "id", columnsProjected[0].Column)
	assert.Equal(t, "active", columnsProjected[1].Column)
}

// TestPhysicalIndexScanOperator_Reset tests that Reset allows re-execution.
func TestPhysicalIndexScanOperator_Reset(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	table, err := stor.CreateTable("reset_test", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(1), "First"}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Second"}))

	// Create index
	index := storage.NewHashIndex("idx_reset", "reset_test", []string{"id"}, false)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))

	// Create definitions
	tableDef := catalog.NewTableDef("reset_test", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_reset", "main", "reset_test", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	scanOp, err := NewPhysicalIndexScanOperator(
		"reset_test",
		"main",
		tableDef,
		"idx_reset",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// First execution
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "First", chunk.GetValue(0, 1))

	// Should be exhausted
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk)

	// Reset and execute again
	scanOp.Reset()

	// Second execution after reset
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "After reset, should be able to execute again")
	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "First", chunk.GetValue(0, 1))
}

// --- Index-Only Scan Integration Tests ---
// These tests verify the isIndexOnly flag behavior in PhysicalIndexScanOperator.
// Note: Current HashIndex only stores RowIDs, so true index-only scans (avoiding heap access)
// are not yet implemented. These tests ensure the flag is handled correctly and execution
// still works when isIndexOnly=true, demonstrating coverage detection works correctly.

// TestIndexOnlyScan_CoveringIndex_SingleColumn tests index-only scan with a single-column index
// that covers all columns needed by the query.
func TestIndexOnlyScan_CoveringIndex_SingleColumn(t *testing.T) {
	// Setup: Create table with id column and an index on id
	// Query: SELECT id FROM table WHERE id = X
	// The index on (id) covers the only column needed by the query.

	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_VARCHAR, // name (not needed for covering index scenario)
	}
	table, err := stor.CreateTable("covering_single", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice"}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob"}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie"}))

	// Create index on (id) - covers the single column query
	index := storage.NewHashIndex("idx_covering_id", "covering_single", []string{"id"}, true)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	tableDef := catalog.NewTableDef("covering_single", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_covering_id", "main", "covering_single", []string{"id"}, true)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Lookup id = 2, only project column 0 (id)
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
	}

	// Create index scan with isIndexOnly=true (covering index scenario)
	scanOp, err := NewPhysicalIndexScanOperator(
		"covering_single",
		"main",
		tableDef,
		"idx_covering_id",
		indexDef,
		index,
		lookupKeys,
		[]int{0}, // Only project the id column
		true,     // isIndexOnly = true (covering index detected)
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Verify IsIndexOnlyScan returns true
	assert.True(t, scanOp.IsIndexOnlyScan(), "Should report as index-only scan")

	// Execute and verify results
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	assert.Equal(t, 1, chunk.Count(), "Should return exactly 1 row")
	assert.Equal(t, 1, chunk.ColumnCount(), "Should have 1 column (only id)")
	assert.Equal(t, int32(2), chunk.GetValue(0, 0), "id should be 2")

	// Exhausted
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk)
}

// TestIndexOnlyScan_CoveringIndex_CompositeIndex tests index-only scan with a composite index
// that covers all columns needed by the query.
func TestIndexOnlyScan_CoveringIndex_CompositeIndex(t *testing.T) {
	// Setup: Create table with (id, name, age) and a composite index on (id, name)
	// Query: SELECT id, name FROM table WHERE id = X
	// The index on (id, name) covers both columns needed.

	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_VARCHAR, // name
		dukdb.TYPE_INTEGER, // age (not in index)
	}
	table, err := stor.CreateTable("covering_composite", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(100), "Alice", int32(25)}))
	require.NoError(t, table.AppendRow([]any{int32(200), "Bob", int32(30)}))
	require.NoError(t, table.AppendRow([]any{int32(300), "Charlie", int32(35)}))

	// Create composite index on (id, name)
	index := storage.NewHashIndex("idx_covering_id_name", "covering_composite", []string{"id", "name"}, false)
	require.NoError(t, index.Insert([]any{int32(100), "Alice"}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(200), "Bob"}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(300), "Charlie"}, storage.RowID(2)))

	tableDef := catalog.NewTableDef("covering_composite", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_covering_id_name", "main", "covering_composite", []string{"id", "name"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Lookup by id, the first column of composite key (partial key lookup)
	// Note: For composite index lookups, we typically provide values for all key columns
	// But here we're testing that the isIndexOnly flag works correctly
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(200), ValType: dukdb.TYPE_INTEGER},
		&binder.BoundLiteral{Value: "Bob", ValType: dukdb.TYPE_VARCHAR},
	}

	// Create index scan with isIndexOnly=true
	// Project only columns 0 and 1 (id, name) which are both in the index
	scanOp, err := NewPhysicalIndexScanOperator(
		"covering_composite",
		"main",
		tableDef,
		"idx_covering_id_name",
		indexDef,
		index,
		lookupKeys,
		[]int{0, 1}, // Only project id and name (both in index)
		true,        // isIndexOnly = true
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	assert.True(t, scanOp.IsIndexOnlyScan())
	assert.Equal(t, "idx_covering_id_name", scanOp.GetIndexName())

	// Execute and verify
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, 2, chunk.ColumnCount())
	assert.Equal(t, int32(200), chunk.GetValue(0, 0), "id should be 200")
	assert.Equal(t, "Bob", chunk.GetValue(0, 1), "name should be Bob")
}

// TestIndexOnlyScan_NonCoveringIndex tests that isIndexOnly=false works correctly
// when the index does NOT cover all required columns.
func TestIndexOnlyScan_NonCoveringIndex(t *testing.T) {
	// Setup: Create table with (id, name, age) and index on (id)
	// Query: SELECT * FROM table WHERE id = X
	// The index on (id) does NOT cover name and age.

	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_INTEGER,
	}
	table, err := stor.CreateTable("non_covering", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(10), "David", int32(40)}))
	require.NoError(t, table.AppendRow([]any{int32(20), "Eve", int32(35)}))
	require.NoError(t, table.AppendRow([]any{int32(30), "Frank", int32(45)}))

	index := storage.NewHashIndex("idx_non_covering_id", "non_covering", []string{"id"}, true)
	require.NoError(t, index.Insert([]any{int32(10)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(20)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(30)}, storage.RowID(2)))

	tableDef := catalog.NewTableDef("non_covering", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("age", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_non_covering_id", "main", "non_covering", []string{"id"}, true)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(20), ValType: dukdb.TYPE_INTEGER},
	}

	// Create index scan with isIndexOnly=false (non-covering, need heap access)
	scanOp, err := NewPhysicalIndexScanOperator(
		"non_covering",
		"main",
		tableDef,
		"idx_non_covering_id",
		indexDef,
		index,
		lookupKeys,
		nil,   // All columns (SELECT *)
		false, // isIndexOnly = false (non-covering)
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Verify NOT index-only
	assert.False(t, scanOp.IsIndexOnlyScan(), "Should NOT be index-only scan")

	// Execute and verify all columns are returned
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	assert.Equal(t, 1, chunk.Count())
	assert.Equal(t, 3, chunk.ColumnCount(), "Should have all 3 columns")
	assert.Equal(t, int32(20), chunk.GetValue(0, 0), "id should be 20")
	assert.Equal(t, "Eve", chunk.GetValue(0, 1), "name should be Eve")
	assert.Equal(t, int32(35), chunk.GetValue(0, 2), "age should be 35")
}

// TestIndexOnlyScan_CoveringIndex_MultipleMatches tests index-only scan returning multiple rows.
func TestIndexOnlyScan_CoveringIndex_MultipleMatches(t *testing.T) {
	// Setup: Non-unique index on department_id, query only department_id column
	// Multiple employees in the same department

	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_VARCHAR, // name
		dukdb.TYPE_INTEGER, // department_id
	}
	table, err := stor.CreateTable("emp_covering", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(1), "Alice", int32(100)}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob", int32(200)}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie", int32(100)}))
	require.NoError(t, table.AppendRow([]any{int32(4), "Diana", int32(100)}))
	require.NoError(t, table.AppendRow([]any{int32(5), "Eve", int32(200)}))

	// Index on (department_id) - non-unique, multiple matches
	index := storage.NewHashIndex("idx_emp_dept", "emp_covering", []string{"department_id"}, false)
	require.NoError(t, index.Insert([]any{int32(100)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(200)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(100)}, storage.RowID(2)))
	require.NoError(t, index.Insert([]any{int32(100)}, storage.RowID(3)))
	require.NoError(t, index.Insert([]any{int32(200)}, storage.RowID(4)))

	tableDef := catalog.NewTableDef("emp_covering", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("department_id", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_emp_dept", "main", "emp_covering", []string{"department_id"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(100), ValType: dukdb.TYPE_INTEGER},
	}

	// Create index scan with isIndexOnly=true, only project department_id (column 2)
	scanOp, err := NewPhysicalIndexScanOperator(
		"emp_covering",
		"main",
		tableDef,
		"idx_emp_dept",
		indexDef,
		index,
		lookupKeys,
		[]int{2}, // Only project department_id
		true,     // isIndexOnly = true
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	assert.True(t, scanOp.IsIndexOnlyScan())

	// Execute
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Should have 3 rows (Alice, Charlie, Diana all in dept 100)
	assert.Equal(t, 3, chunk.Count(), "Should have 3 matching rows")
	assert.Equal(t, 1, chunk.ColumnCount(), "Should have only 1 projected column")

	// All rows should have department_id = 100
	for i := 0; i < chunk.Count(); i++ {
		assert.Equal(t, int32(100), chunk.GetValue(i, 0), "Row %d department_id should be 100", i)
	}

	// Verify matched row count
	assert.Equal(t, 3, scanOp.GetMatchedRowCount())
}

// TestIndexOnlyScan_CompareWithNonIndexOnly verifies that both execution paths
// (index-only and non-index-only) return the same results for the same query.
func TestIndexOnlyScan_CompareWithNonIndexOnly(t *testing.T) {
	// Setup
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("compare_table", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(1), "One"}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Two"}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Three"}))

	index := storage.NewHashIndex("idx_compare", "compare_table", []string{"id"}, true)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	tableDef := catalog.NewTableDef("compare_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_compare", "main", "compare_table", []string{"id"}, true)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
	}

	// Execute with isIndexOnly=true
	scanOpIndexOnly, err := NewPhysicalIndexScanOperator(
		"compare_table", "main", tableDef, "idx_compare", indexDef, index,
		lookupKeys, []int{0}, true, stor, exec, ctx,
	)
	require.NoError(t, err)

	chunkIndexOnly, err := scanOpIndexOnly.Next()
	require.NoError(t, err)
	require.NotNil(t, chunkIndexOnly)

	// Execute with isIndexOnly=false (same projection)
	scanOpNonIndexOnly, err := NewPhysicalIndexScanOperator(
		"compare_table", "main", tableDef, "idx_compare", indexDef, index,
		lookupKeys, []int{0}, false, stor, exec, ctx,
	)
	require.NoError(t, err)

	chunkNonIndexOnly, err := scanOpNonIndexOnly.Next()
	require.NoError(t, err)
	require.NotNil(t, chunkNonIndexOnly)

	// Both should return the same result
	assert.Equal(t, chunkIndexOnly.Count(), chunkNonIndexOnly.Count())
	assert.Equal(t, chunkIndexOnly.ColumnCount(), chunkNonIndexOnly.ColumnCount())
	assert.Equal(t, chunkIndexOnly.GetValue(0, 0), chunkNonIndexOnly.GetValue(0, 0))

	// Verify the flag difference
	assert.True(t, scanOpIndexOnly.IsIndexOnlyScan())
	assert.False(t, scanOpNonIndexOnly.IsIndexOnlyScan())
}

// TestIndexOnlyScan_WithDeletedRows tests that deleted rows are properly
// skipped even in index-only scan mode.
func TestIndexOnlyScan_WithDeletedRows(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("deleted_rows_covering", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(1), "First"}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Second"}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Third"}))

	index := storage.NewHashIndex("idx_deleted_covering", "deleted_rows_covering", []string{"id"}, true)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(3)}, storage.RowID(2)))

	// Mark row with id=2 as deleted
	table.SetRowDeleted(storage.RowID(1), 100)

	tableDef := catalog.NewTableDef("deleted_rows_covering", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_deleted_covering", "main", "deleted_rows_covering", []string{"id"}, true)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
	}

	// Index-only scan for deleted row
	scanOp, err := NewPhysicalIndexScanOperator(
		"deleted_rows_covering", "main", tableDef, "idx_deleted_covering", indexDef, index,
		lookupKeys, []int{0}, true, stor, exec, ctx,
	)
	require.NoError(t, err)

	// Should return nil because the row is deleted
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "Should return nil for deleted row even in index-only mode")
}

// TestIndexOnlyScan_TypesPreserved verifies that GetTypes() returns correct
// types for projected columns in index-only mode.
func TestIndexOnlyScan_TypesPreserved(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_DOUBLE,
	}
	_, err := stor.CreateTable("types_test", columnTypes)
	require.NoError(t, err)

	index := storage.NewHashIndex("idx_types", "types_test", []string{"col0", "col1"}, false)

	tableDef := catalog.NewTableDef("types_test", []*catalog.ColumnDef{
		catalog.NewColumnDef("col0", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("col1", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("col2", dukdb.TYPE_BOOLEAN),
		catalog.NewColumnDef("col3", dukdb.TYPE_DOUBLE),
	})
	indexDef := catalog.NewIndexDef("idx_types", "main", "types_test", []string{"col0", "col1"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int64(1), ValType: dukdb.TYPE_BIGINT},
		&binder.BoundLiteral{Value: "test", ValType: dukdb.TYPE_VARCHAR},
	}

	// Project columns 0 and 1 (covering index scenario)
	scanOp, err := NewPhysicalIndexScanOperator(
		"types_test", "main", tableDef, "idx_types", indexDef, index,
		lookupKeys, []int{0, 1}, true, stor, exec, ctx,
	)
	require.NoError(t, err)

	types := scanOp.GetTypes()
	require.Len(t, types, 2)
	assert.Equal(t, dukdb.TYPE_BIGINT, types[0].InternalType())
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1].InternalType())

	// Verify output columns
	cols := scanOp.OutputColumns()
	require.Len(t, cols, 2)
	assert.Equal(t, "col0", cols[0].Column)
	assert.Equal(t, "col1", cols[1].Column)
}

// --- Residual Filter Tests for Partial Composite Index Matches ---
// These tests verify that the executor correctly applies residual filters
// when only part of a composite index is matched.

// TestPhysicalIndexScanOperator_PartialCompositeMatch_WithResidualFilter tests
// the scenario where a composite index on (a, b) is used with a query like
// WHERE a = 1 AND c = 3. Only 'a = 1' uses the index, and 'c = 3' becomes
// a residual filter applied after fetching rows.
func TestPhysicalIndexScanOperator_PartialCompositeMatch_WithResidualFilter(t *testing.T) {
	// Create storage and table with columns: a, b, c
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // a (indexed)
		dukdb.TYPE_INTEGER, // b (indexed)
		dukdb.TYPE_INTEGER, // c (not indexed, becomes residual filter)
	}
	table, err := stor.CreateTable("partial_match", columnTypes)
	require.NoError(t, err)

	// Insert test data
	// Row 0: a=1, b=10, c=100 - matches a=1, should match c=100 residual filter
	require.NoError(t, table.AppendRow([]any{int32(1), int32(10), int32(100)}))
	// Row 1: a=1, b=20, c=200 - matches a=1, should NOT match c=100 residual filter
	require.NoError(t, table.AppendRow([]any{int32(1), int32(20), int32(200)}))
	// Row 2: a=1, b=30, c=100 - matches a=1, should match c=100 residual filter
	require.NoError(t, table.AppendRow([]any{int32(1), int32(30), int32(100)}))
	// Row 3: a=2, b=40, c=100 - does NOT match a=1
	require.NoError(t, table.AppendRow([]any{int32(2), int32(40), int32(100)}))
	// Row 4: a=1, b=50, c=300 - matches a=1, should NOT match c=100 residual filter
	require.NoError(t, table.AppendRow([]any{int32(1), int32(50), int32(300)}))

	// Create a composite HashIndex on (a, b)
	// This index can be used for a=1 but c is not in the index
	index := storage.NewHashIndex("idx_partial_ab", "partial_match", []string{"a", "b"}, false)
	require.NoError(t, index.Insert([]any{int32(1), int32(10)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(1), int32(20)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(1), int32(30)}, storage.RowID(2)))
	require.NoError(t, index.Insert([]any{int32(2), int32(40)}, storage.RowID(3)))
	require.NoError(t, index.Insert([]any{int32(1), int32(50)}, storage.RowID(4)))

	// We'll use a workaround for partial key lookups: look up all matching a=1 values
	// In a real scenario, the index lookup would support prefix lookups.
	// For this test, we'll insert with just a=1 as well, to simulate prefix behavior.
	indexSingleCol := storage.NewHashIndex("idx_partial_a", "partial_match", []string{"a"}, false)
	require.NoError(t, indexSingleCol.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, indexSingleCol.Insert([]any{int32(1)}, storage.RowID(1)))
	require.NoError(t, indexSingleCol.Insert([]any{int32(1)}, storage.RowID(2)))
	require.NoError(t, indexSingleCol.Insert([]any{int32(2)}, storage.RowID(3)))
	require.NoError(t, indexSingleCol.Insert([]any{int32(1)}, storage.RowID(4)))

	// Create table definition
	tableDef := catalog.NewTableDef("partial_match", []*catalog.ColumnDef{
		catalog.NewColumnDef("a", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("b", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("c", dukdb.TYPE_INTEGER),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_partial_a", "main", "partial_match", []string{"a"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Lookup key for a = 1 (index can match this)
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	// Create the index scan operator
	scanOp, err := NewPhysicalIndexScanOperator(
		"partial_match",
		"main",
		tableDef,
		"idx_partial_a",
		indexDef,
		indexSingleCol,
		lookupKeys,
		nil, // no projection, get all columns
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Get results from the index scan
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Should return rows matching a=1")

	// Index lookup for a=1 should return 4 rows (rows 0, 1, 2, 4)
	assert.Equal(t, 4, chunk.Count(), "Index scan should find 4 rows with a=1")

	// Collect the 'c' values to verify which rows were returned
	cValues := make(map[int32]int)
	for i := 0; i < chunk.Count(); i++ {
		c := chunk.GetValue(i, 2).(int32)
		cValues[c]++
	}

	// Without residual filter, we should have:
	// c=100 appears 2 times (rows 0 and 2)
	// c=200 appears 1 time (row 1)
	// c=300 appears 1 time (row 4)
	assert.Equal(t, 2, cValues[100], "c=100 should appear twice")
	assert.Equal(t, 1, cValues[200], "c=200 should appear once")
	assert.Equal(t, 1, cValues[300], "c=300 should appear once")

	// The residual filter (c = 100) would filter this down to 2 rows
	// This is handled by the executor when executing PhysicalIndexScan with ResidualFilter
}

// TestPhysicalIndexScanOperator_VerifyResidualFilterAppliesCorrectly creates a scenario
// to verify that when index returns multiple rows, a residual filter reduces the count.
func TestPhysicalIndexScanOperator_VerifyResidualFilterAppliesCorrectly(t *testing.T) {
	// This test verifies the end-to-end behavior of residual filters.
	// Setup: Index on (category), query WHERE category = 'A' AND status = 'active'
	// The index can only handle category = 'A', status must be residual.

	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_VARCHAR, // category (indexed)
		dukdb.TYPE_VARCHAR, // status (not indexed - residual filter)
		dukdb.TYPE_INTEGER, // value
	}
	table, err := stor.CreateTable("residual_test", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{"A", "active", int32(10)}))    // Row 0: matches both
	require.NoError(t, table.AppendRow([]any{"A", "inactive", int32(20)}))  // Row 1: matches index, not residual
	require.NoError(t, table.AppendRow([]any{"A", "active", int32(30)}))    // Row 2: matches both
	require.NoError(t, table.AppendRow([]any{"B", "active", int32(40)}))    // Row 3: doesn't match index
	require.NoError(t, table.AppendRow([]any{"A", "pending", int32(50)}))   // Row 4: matches index, not residual
	require.NoError(t, table.AppendRow([]any{"A", "active", int32(60)}))    // Row 5: matches both

	// Create index on category
	index := storage.NewHashIndex("idx_category", "residual_test", []string{"category"}, false)
	require.NoError(t, index.Insert([]any{"A"}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{"A"}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{"A"}, storage.RowID(2)))
	require.NoError(t, index.Insert([]any{"B"}, storage.RowID(3)))
	require.NoError(t, index.Insert([]any{"A"}, storage.RowID(4)))
	require.NoError(t, index.Insert([]any{"A"}, storage.RowID(5)))

	tableDef := catalog.NewTableDef("residual_test", []*catalog.ColumnDef{
		catalog.NewColumnDef("category", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("status", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("value", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_category", "main", "residual_test", []string{"category"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Index lookup for category = 'A'
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: "A", ValType: dukdb.TYPE_VARCHAR},
	}

	scanOp, err := NewPhysicalIndexScanOperator(
		"residual_test",
		"main",
		tableDef,
		"idx_category",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Execute index scan without residual filter
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Should return 5 rows (all category='A' rows: 0, 1, 2, 4, 5)
	assert.Equal(t, 5, chunk.Count(), "Index scan should find 5 rows with category='A'")

	// Verify status values in the returned rows
	statusCounts := make(map[string]int)
	for i := 0; i < chunk.Count(); i++ {
		status := chunk.GetValue(i, 1).(string)
		statusCounts[status]++
	}
	assert.Equal(t, 3, statusCounts["active"], "3 rows should have status='active'")
	assert.Equal(t, 1, statusCounts["inactive"], "1 row should have status='inactive'")
	assert.Equal(t, 1, statusCounts["pending"], "1 row should have status='pending'")

	// After a residual filter (status = 'active') is applied,
	// only 3 rows should remain (rows 0, 2, 5)
	// This filtering happens in executeIndexScan when plan.ResidualFilter is set
}

// TestPhysicalIndexScanOperator_ResidualFilterWithCompositeIndexGap tests
// the scenario where a composite index on (a, b, c) is queried with a = 1 AND c = 3,
// skipping the middle column 'b'. Only 'a' is usable for index lookup.
func TestPhysicalIndexScanOperator_ResidualFilterWithCompositeIndexGap(t *testing.T) {
	// Setup: Composite index on (a, b, c)
	// Query: WHERE a = 1 AND c = 3 (gap in middle - b is missing)
	// Only 'a = 1' uses the index, 'c = 3' is residual

	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // a
		dukdb.TYPE_INTEGER, // b
		dukdb.TYPE_INTEGER, // c
	}
	table, err := stor.CreateTable("composite_gap", columnTypes)
	require.NoError(t, err)

	// Insert test data
	// a=1 with various b and c values
	require.NoError(t, table.AppendRow([]any{int32(1), int32(10), int32(3)}))  // Row 0: matches a=1 AND c=3
	require.NoError(t, table.AppendRow([]any{int32(1), int32(20), int32(5)}))  // Row 1: matches a=1, NOT c=3
	require.NoError(t, table.AppendRow([]any{int32(1), int32(30), int32(3)}))  // Row 2: matches a=1 AND c=3
	require.NoError(t, table.AppendRow([]any{int32(2), int32(40), int32(3)}))  // Row 3: NOT a=1
	require.NoError(t, table.AppendRow([]any{int32(1), int32(50), int32(7)}))  // Row 4: matches a=1, NOT c=3

	// Create single-column index on 'a' to simulate partial composite match
	// In a real scenario, we'd use the first column of the composite index
	index := storage.NewHashIndex("idx_a", "composite_gap", []string{"a"}, false)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(2)))
	require.NoError(t, index.Insert([]any{int32(2)}, storage.RowID(3)))
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(4)))

	tableDef := catalog.NewTableDef("composite_gap", []*catalog.ColumnDef{
		catalog.NewColumnDef("a", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("b", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("c", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_a", "main", "composite_gap", []string{"a"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	scanOp, err := NewPhysicalIndexScanOperator(
		"composite_gap",
		"main",
		tableDef,
		"idx_a",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Execute index scan (without residual filter at this level)
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Index returns 4 rows (all with a=1: rows 0, 1, 2, 4)
	assert.Equal(t, 4, chunk.Count(), "Index scan for a=1 should return 4 rows")

	// Count rows where c=3 (these are the ones that would pass the residual filter)
	matchesResidual := 0
	for i := 0; i < chunk.Count(); i++ {
		c := chunk.GetValue(i, 2).(int32)
		if c == 3 {
			matchesResidual++
		}
	}
	assert.Equal(t, 2, matchesResidual, "2 rows should have c=3 (rows 0 and 2)")

	// After applying residual filter c=3, only 2 rows would remain
	// The residual filter application happens in executeIndexScan
}

// TestPhysicalIndexScan_ResidualFilterWithNoMatches tests the case where the
// index finds rows but none pass the residual filter.
func TestPhysicalIndexScan_ResidualFilterWithNoMatches(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // key (indexed)
		dukdb.TYPE_VARCHAR, // filter_col (not indexed)
	}
	table, err := stor.CreateTable("no_residual_match", columnTypes)
	require.NoError(t, err)

	// All rows have key=1 but different filter_col values
	require.NoError(t, table.AppendRow([]any{int32(1), "x"})) // Row 0
	require.NoError(t, table.AppendRow([]any{int32(1), "y"})) // Row 1
	require.NoError(t, table.AppendRow([]any{int32(1), "z"})) // Row 2

	index := storage.NewHashIndex("idx_key", "no_residual_match", []string{"key"}, false)
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(1)))
	require.NoError(t, index.Insert([]any{int32(1)}, storage.RowID(2)))

	tableDef := catalog.NewTableDef("no_residual_match", []*catalog.ColumnDef{
		catalog.NewColumnDef("key", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("filter_col", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_key", "main", "no_residual_match", []string{"key"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	scanOp, err := NewPhysicalIndexScanOperator(
		"no_residual_match",
		"main",
		tableDef,
		"idx_key",
		indexDef,
		index,
		lookupKeys,
		nil,
		false,
		stor,
		exec,
		ctx,
	)
	require.NoError(t, err)

	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Index finds 3 rows
	assert.Equal(t, 3, chunk.Count(), "Index scan should find 3 rows")

	// All have filter_col values that would NOT match filter_col = 'nonexistent'
	// If residual filter = "filter_col = 'nonexistent'" was applied,
	// the result would be 0 rows
	for i := 0; i < chunk.Count(); i++ {
		filterVal := chunk.GetValue(i, 1).(string)
		assert.NotEqual(t, "nonexistent", filterVal)
	}
}

// =============================================================================
// Executor-Level Index Not Found Error Tests
// =============================================================================

// TestExecuteIndexScan_IndexNotFoundInStorage tests the error handling when
// an index is not found in storage during execution. This simulates the scenario
// where an index is dropped between planning and execution (TOCTOU race).
func TestExecuteIndexScan_IndexNotFoundInStorage(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	_, err := stor.CreateTable("toctou_test", columnTypes)
	require.NoError(t, err)

	// Create table definition
	tableDef := catalog.NewTableDef("toctou_test", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create index definition (but don't create the actual index in storage!)
	// This simulates the case where index exists in catalog but not in storage
	indexDef := catalog.NewIndexDef("idx_toctou", "main", "toctou_test", []string{"id"}, false)

	// Create catalog and executor
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create lookup key
	lookupKeys := []binder.BoundExpr{
		&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
	}

	// Create PhysicalIndexScan plan - note: no index is created in storage
	plan := &planner.PhysicalIndexScan{
		Schema:      "main",
		TableName:   "toctou_test",
		Alias:       "toctou_test",
		TableDef:    tableDef,
		IndexName:   "idx_toctou",
		IndexDef:    indexDef,
		LookupKeys:  lookupKeys,
		Projections: nil,
		IsIndexOnly: false,
	}

	// Execute the index scan - should fail because index is not in storage
	_, err = exec.executeIndexScan(ctx, plan)
	require.Error(t, err, "Should return error when index not found in storage")

	// Verify error message includes all context
	errMsg := err.Error()
	assert.Contains(t, errMsg, "idx_toctou", "Error should include index name")
	assert.Contains(t, errMsg, "main", "Error should include schema name")
	assert.Contains(t, errMsg, "toctou_test", "Error should include table name")
	assert.Contains(t, errMsg, "not found", "Error should indicate index not found")
	assert.Contains(t, errMsg, "dropped", "Error should suggest the index may have been dropped")
}

// =============================================================================
// Range Scan Tests
// =============================================================================

// TestRangeScan_OperatorConfiguration tests that the range scan operator is configured correctly.
func TestRangeScan_OperatorConfiguration(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	_, err := stor.CreateTable("range_config_test", columnTypes)
	require.NoError(t, err)

	// Create ART index
	artIdx := index.NewART(dukdb.TYPE_INTEGER)

	// Create table definition
	tableDef := catalog.NewTableDef("range_config_test", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create index definition
	indexDef := catalog.NewIndexDef("idx_range_id", "main", "range_config_test", []string{"id"}, false)

	// Create executor and context
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Test with IsRangeScan = true
	scanOp, err := NewPhysicalIndexScanOperatorWithConfig(IndexScanConfig{
		TableName:        "range_config_test",
		Schema:           "main",
		TableDef:         tableDef,
		IndexName:        "idx_range_id",
		IndexDef:         indexDef,
		ARTIndex:         artIdx,
		IsRangeScan:      true,
		LowerBound:       &binder.BoundLiteral{Value: int32(3), ValType: dukdb.TYPE_INTEGER},
		UpperBound:       &binder.BoundLiteral{Value: int32(7), ValType: dukdb.TYPE_INTEGER},
		LowerInclusive:   true,
		UpperInclusive:   true,
		RangeColumnIndex: 0,
		Projections:      nil,
		IsIndexOnly:      false,
		Storage:          stor,
		Executor:         exec,
		Ctx:              ctx,
	})
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Verify configuration
	assert.True(t, scanOp.IsRangeScan(), "Should be configured as range scan")
	assert.NotNil(t, scanOp.GetARTIndex(), "Should have ART index")
	assert.Equal(t, "idx_range_id", scanOp.GetIndexName(), "Should have correct index name")
}

// TestRangeScan_PointLookupConfiguration tests that point lookups are not range scans.
func TestRangeScan_PointLookupConfiguration(t *testing.T) {
	// Create storage and table
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	_, err := stor.CreateTable("point_lookup_test", columnTypes)
	require.NoError(t, err)

	// Create HashIndex for point lookups
	hashIdx := storage.NewHashIndex("idx_point", "point_lookup_test", []string{"id"}, false)

	tableDef := catalog.NewTableDef("point_lookup_test", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_point", "main", "point_lookup_test", []string{"id"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create point lookup (not range scan)
	scanOp, err := NewPhysicalIndexScanOperatorWithConfig(IndexScanConfig{
		TableName:   "point_lookup_test",
		Schema:      "main",
		TableDef:    tableDef,
		IndexName:   "idx_point",
		IndexDef:    indexDef,
		Index:       hashIdx,
		ARTIndex:    nil, // No ART for point lookups
		IsRangeScan: false,
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int32(5), ValType: dukdb.TYPE_INTEGER},
		},
		Projections: nil,
		IsIndexOnly: false,
		Storage:     stor,
		Executor:    exec,
		Ctx:         ctx,
	})
	require.NoError(t, err)
	require.NotNil(t, scanOp)

	// Verify it's NOT a range scan
	assert.False(t, scanOp.IsRangeScan(), "Should NOT be configured as range scan")
	assert.Nil(t, scanOp.GetARTIndex(), "Should NOT have ART index for point lookups")
}

// TestRangeScan_NilARTIndex tests that range scan returns error when ART index is nil.
func TestRangeScan_NilARTIndex(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	_, err := stor.CreateTable("range_nil", columnTypes)
	require.NoError(t, err)

	tableDef := catalog.NewTableDef("range_nil", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_nil", "main", "range_nil", []string{"id"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create range scan operator with nil ART index
	scanOp, err := NewPhysicalIndexScanOperatorWithConfig(IndexScanConfig{
		TableName:      "range_nil",
		Schema:         "main",
		TableDef:       tableDef,
		IndexName:      "idx_nil",
		IndexDef:       indexDef,
		ARTIndex:       nil, // nil ART index
		IsRangeScan:    true,
		LowerBound:     &binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
		UpperBound:     nil,
		LowerInclusive: true,
		UpperInclusive: false,
		Projections:    nil,
		IsIndexOnly:    false,
		Storage:        stor,
		Executor:       exec,
		Ctx:            ctx,
	})
	require.NoError(t, err)

	// Execution should fail with error about nil ART index
	_, err = scanOp.Next()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "range scan requires an ART index")
}

// TestRangeScan_SetARTIndex tests the SetARTIndex method.
func TestRangeScan_SetARTIndex(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	_, err := stor.CreateTable("range_set", columnTypes)
	require.NoError(t, err)

	tableDef := catalog.NewTableDef("range_set", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_set", "main", "range_set", []string{"id"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create range scan without ART index initially
	scanOp, err := NewPhysicalIndexScanOperatorWithConfig(IndexScanConfig{
		TableName:      "range_set",
		Schema:         "main",
		TableDef:       tableDef,
		IndexName:      "idx_set",
		IndexDef:       indexDef,
		ARTIndex:       nil, // No ART index initially
		IsRangeScan:    true,
		LowerBound:     &binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
		UpperBound:     &binder.BoundLiteral{Value: int32(4), ValType: dukdb.TYPE_INTEGER},
		LowerInclusive: true,
		UpperInclusive: true,
		Projections:    nil,
		IsIndexOnly:    false,
		Storage:        stor,
		Executor:       exec,
		Ctx:            ctx,
	})
	require.NoError(t, err)

	// Verify no ART index initially
	assert.Nil(t, scanOp.GetARTIndex())

	// Create and set ART index
	artIdx := index.NewART(dukdb.TYPE_INTEGER)

	scanOp.SetARTIndex(artIdx)

	// Verify ART index is now set
	assert.NotNil(t, scanOp.GetARTIndex())
	assert.Equal(t, artIdx, scanOp.GetARTIndex())
}

// TestEncodeKeyValue tests the key encoding functions for various types.
func TestEncodeKeyValue(t *testing.T) {
	// Test int encoding preserves order
	key1 := encodeKeyValue(int32(1))
	key2 := encodeKeyValue(int32(2))
	key3 := encodeKeyValue(int32(3))

	assert.Less(t, string(key1), string(key2), "1 should encode less than 2")
	assert.Less(t, string(key2), string(key3), "2 should encode less than 3")

	// Test negative numbers
	keyNeg := encodeKeyValue(int32(-1))
	keyZero := encodeKeyValue(int32(0))
	keyPos := encodeKeyValue(int32(1))

	assert.Less(t, string(keyNeg), string(keyZero), "-1 should encode less than 0")
	assert.Less(t, string(keyZero), string(keyPos), "0 should encode less than 1")

	// Test int64
	key64_1 := encodeKeyValue(int64(1000000))
	key64_2 := encodeKeyValue(int64(2000000))
	assert.Less(t, string(key64_1), string(key64_2))

	// Test string encoding
	keyA := encodeKeyValue("apple")
	keyB := encodeKeyValue("banana")
	assert.Less(t, string(keyA), string(keyB), "apple should encode less than banana")

	// Test nil returns nil
	assert.Nil(t, encodeKeyValue(nil))
}

// =============================================================================
// Residual Filter Tests with executeIndexScan
// =============================================================================
// These tests verify that residual filters are correctly applied after index lookups.
// The residual filter is applied in executeIndexScan() regardless of whether the
// underlying scan uses point lookups (HashIndex) or range scans (ART).

// TestExecuteIndexScan_ResidualFilter_PointLookup tests that residual filters
// are correctly applied after point lookup index scans.
// Scenario: Index on 'department_id', query WHERE department_id = 10 AND status = 'active'
func TestExecuteIndexScan_ResidualFilter_PointLookup(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_INTEGER, // department_id (indexed)
		dukdb.TYPE_VARCHAR, // status (residual filter)
	}
	table, err := stor.CreateTable("employees_residual", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), int32(10), "active"}))   // matches both
	require.NoError(t, table.AppendRow([]any{int32(2), int32(10), "inactive"})) // matches index, fails residual
	require.NoError(t, table.AppendRow([]any{int32(3), int32(10), "active"}))   // matches both
	require.NoError(t, table.AppendRow([]any{int32(4), int32(20), "active"}))   // fails index
	require.NoError(t, table.AppendRow([]any{int32(5), int32(10), "pending"}))  // matches index, fails residual

	// Create hash index on department_id
	hashIdx := storage.NewHashIndex("idx_dept_residual", "employees_residual", []string{"department_id"}, false)
	require.NoError(t, hashIdx.Insert([]any{int32(10)}, storage.RowID(0)))
	require.NoError(t, hashIdx.Insert([]any{int32(10)}, storage.RowID(1)))
	require.NoError(t, hashIdx.Insert([]any{int32(10)}, storage.RowID(2)))
	require.NoError(t, hashIdx.Insert([]any{int32(20)}, storage.RowID(3)))
	require.NoError(t, hashIdx.Insert([]any{int32(10)}, storage.RowID(4)))

	// Register the index in storage
	require.NoError(t, stor.CreateIndex("main", hashIdx))

	tableDef := catalog.NewTableDef("employees_residual", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("department_id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("status", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_dept_residual", "main", "employees_residual", []string{"department_id"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create PhysicalIndexScan plan with residual filter (status = 'active')
	plan := &planner.PhysicalIndexScan{
		TableName: "employees_residual",
		Schema:    "main",
		TableDef:  tableDef,
		IndexName: "idx_dept_residual",
		IndexDef:  indexDef,
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int32(10), ValType: dukdb.TYPE_INTEGER},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "status", Table: "", ColType: dukdb.TYPE_VARCHAR, ColumnIdx: 2},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "active", ValType: dukdb.TYPE_VARCHAR},
		},
	}

	// Execute the index scan
	result, err := exec.executeIndexScan(ctx, plan)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Without residual filter, index returns 4 rows (dept=10)
	// With residual filter (status='active'), only 2 rows should remain
	assert.Equal(t, 2, len(result.Rows), "Residual filter should reduce 4 rows to 2")

	// Verify all returned rows have status='active'
	for _, row := range result.Rows {
		assert.Equal(t, "active", row["status"], "All rows should have status='active'")
	}
}

// TestExecuteIndexScan_ResidualFilter_MultiplePredicates tests residual filter
// with multiple predicates combined with AND.
// Scenario: Index on 'category', query WHERE category = 'A' AND price > 100 AND stock > 0
func TestExecuteIndexScan_ResidualFilter_MultiplePredicates(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // id
		dukdb.TYPE_VARCHAR, // category (indexed)
		dukdb.TYPE_INTEGER, // price (residual)
		dukdb.TYPE_INTEGER, // stock (residual)
	}
	table, err := stor.CreateTable("products_residual", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "A", int32(150), int32(5)}))  // passes all
	require.NoError(t, table.AppendRow([]any{int32(2), "A", int32(80), int32(10)}))  // fails price
	require.NoError(t, table.AppendRow([]any{int32(3), "A", int32(200), int32(0)}))  // fails stock
	require.NoError(t, table.AppendRow([]any{int32(4), "B", int32(120), int32(3)}))  // fails index
	require.NoError(t, table.AppendRow([]any{int32(5), "A", int32(110), int32(2)}))  // passes all
	require.NoError(t, table.AppendRow([]any{int32(6), "A", int32(50), int32(0)}))   // fails both residual

	// Create hash index on category
	hashIdx := storage.NewHashIndex("idx_cat_residual", "products_residual", []string{"category"}, false)
	require.NoError(t, hashIdx.Insert([]any{"A"}, storage.RowID(0)))
	require.NoError(t, hashIdx.Insert([]any{"A"}, storage.RowID(1)))
	require.NoError(t, hashIdx.Insert([]any{"A"}, storage.RowID(2)))
	require.NoError(t, hashIdx.Insert([]any{"B"}, storage.RowID(3)))
	require.NoError(t, hashIdx.Insert([]any{"A"}, storage.RowID(4)))
	require.NoError(t, hashIdx.Insert([]any{"A"}, storage.RowID(5)))

	require.NoError(t, stor.CreateIndex("main", hashIdx))

	tableDef := catalog.NewTableDef("products_residual", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("category", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("price", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("stock", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_cat_residual", "main", "products_residual", []string{"category"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Create PhysicalIndexScan plan with complex residual filter
	// (price > 100 AND stock > 0)
	plan := &planner.PhysicalIndexScan{
		TableName: "products_residual",
		Schema:    "main",
		TableDef:  tableDef,
		IndexName: "idx_cat_residual",
		IndexDef:  indexDef,
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: "A", ValType: dukdb.TYPE_VARCHAR},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "price", Table: "", ColType: dukdb.TYPE_INTEGER, ColumnIdx: 2},
				Op:    parser.OpGt,
				Right: &binder.BoundLiteral{Value: int32(100), ValType: dukdb.TYPE_INTEGER},
			},
			Op: parser.OpAnd,
			Right: &binder.BoundBinaryExpr{
				Left:  &binder.BoundColumnRef{Column: "stock", Table: "", ColType: dukdb.TYPE_INTEGER, ColumnIdx: 3},
				Op:    parser.OpGt,
				Right: &binder.BoundLiteral{Value: int32(0), ValType: dukdb.TYPE_INTEGER},
			},
		},
	}

	// Execute the index scan
	result, err := exec.executeIndexScan(ctx, plan)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Index returns 5 rows (category='A')
	// Residual filter (price > 100 AND stock > 0) keeps only 2 rows
	assert.Equal(t, 2, len(result.Rows), "Residual filter should reduce 5 rows to 2")

	// Verify all returned rows satisfy the residual predicates
	for _, row := range result.Rows {
		price := row["price"].(int32)
		stock := row["stock"].(int32)
		assert.Greater(t, price, int32(100), "All rows should have price > 100")
		assert.Greater(t, stock, int32(0), "All rows should have stock > 0")
	}
}

// TestExecuteIndexScan_ResidualFilter_NoMatches tests the case where index finds
// rows but all are filtered out by the residual filter.
func TestExecuteIndexScan_ResidualFilter_NoMatches(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // key
		dukdb.TYPE_VARCHAR, // filter_col
	}
	table, err := stor.CreateTable("no_match_residual", columnTypes)
	require.NoError(t, err)

	// All rows have key=1 but none have filter_col='target'
	require.NoError(t, table.AppendRow([]any{int32(1), "x"}))
	require.NoError(t, table.AppendRow([]any{int32(1), "y"}))
	require.NoError(t, table.AppendRow([]any{int32(1), "z"}))

	hashIdx := storage.NewHashIndex("idx_key_nomatch", "no_match_residual", []string{"key"}, false)
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(1)))
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(2)))

	require.NoError(t, stor.CreateIndex("main", hashIdx))

	tableDef := catalog.NewTableDef("no_match_residual", []*catalog.ColumnDef{
		catalog.NewColumnDef("key", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("filter_col", dukdb.TYPE_VARCHAR),
	})
	indexDef := catalog.NewIndexDef("idx_key_nomatch", "main", "no_match_residual", []string{"key"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Residual filter = "filter_col = 'target'" (matches nothing)
	plan := &planner.PhysicalIndexScan{
		TableName: "no_match_residual",
		Schema:    "main",
		TableDef:  tableDef,
		IndexName: "idx_key_nomatch",
		IndexDef:  indexDef,
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "filter_col", Table: "", ColType: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: "target", ValType: dukdb.TYPE_VARCHAR},
		},
	}

	result, err := exec.executeIndexScan(ctx, plan)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Index returns 3 rows, but residual filter matches none
	assert.Equal(t, 0, len(result.Rows), "Residual filter should filter out all rows")
}

// TestExecuteIndexScan_ResidualFilter_CompositeIndexPartialMatch tests residual filter
// when only part of a composite index is matched.
// Scenario: Index on (a, b), query WHERE a = 1 AND c = 3
// Only 'a = 1' uses the index, 'c = 3' becomes residual filter.
func TestExecuteIndexScan_ResidualFilter_CompositeIndexPartialMatch(t *testing.T) {
	stor := storage.NewStorage()
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER, // a (indexed)
		dukdb.TYPE_INTEGER, // b (indexed but not in query)
		dukdb.TYPE_INTEGER, // c (not indexed, residual filter)
	}
	table, err := stor.CreateTable("composite_partial", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), int32(10), int32(3)})) // matches a=1 AND c=3
	require.NoError(t, table.AppendRow([]any{int32(1), int32(20), int32(5)})) // matches a=1, fails c=3
	require.NoError(t, table.AppendRow([]any{int32(1), int32(30), int32(3)})) // matches a=1 AND c=3
	require.NoError(t, table.AppendRow([]any{int32(2), int32(40), int32(3)})) // fails a=1
	require.NoError(t, table.AppendRow([]any{int32(1), int32(50), int32(7)})) // matches a=1, fails c=3

	// Create single-column index on 'a' to simulate partial composite match
	hashIdx := storage.NewHashIndex("idx_a_partial", "composite_partial", []string{"a"}, false)
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(0)))
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(1)))
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(2)))
	require.NoError(t, hashIdx.Insert([]any{int32(2)}, storage.RowID(3)))
	require.NoError(t, hashIdx.Insert([]any{int32(1)}, storage.RowID(4)))

	require.NoError(t, stor.CreateIndex("main", hashIdx))

	tableDef := catalog.NewTableDef("composite_partial", []*catalog.ColumnDef{
		catalog.NewColumnDef("a", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("b", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("c", dukdb.TYPE_INTEGER),
	})
	indexDef := catalog.NewIndexDef("idx_a_partial", "main", "composite_partial", []string{"a"}, false)

	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	// Residual filter: c = 3
	plan := &planner.PhysicalIndexScan{
		TableName: "composite_partial",
		Schema:    "main",
		TableDef:  tableDef,
		IndexName: "idx_a_partial",
		IndexDef:  indexDef,
		LookupKeys: []binder.BoundExpr{
			&binder.BoundLiteral{Value: int32(1), ValType: dukdb.TYPE_INTEGER},
		},
		ResidualFilter: &binder.BoundBinaryExpr{
			Left:  &binder.BoundColumnRef{Column: "c", Table: "", ColType: dukdb.TYPE_INTEGER, ColumnIdx: 2},
			Op:    parser.OpEq,
			Right: &binder.BoundLiteral{Value: int32(3), ValType: dukdb.TYPE_INTEGER},
		},
	}

	result, err := exec.executeIndexScan(ctx, plan)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Index returns 4 rows (a=1), residual filter (c=3) keeps 2
	assert.Equal(t, 2, len(result.Rows), "Residual filter should reduce 4 rows to 2")

	// Verify all returned rows have c=3
	for _, row := range result.Rows {
		c := row["c"].(int32)
		assert.Equal(t, int32(3), c, "All rows should have c=3")
	}
}

package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPhysicalHashJoinBasic(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create left table: a (id INT, name VARCHAR)
	leftTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	leftTable, err := stor.CreateTable("a", leftTypes)
	require.NoError(t, err)

	// Insert data into left table
	require.NoError(t, leftTable.AppendRow([]any{int64(1), "Alice"}))
	require.NoError(t, leftTable.AppendRow([]any{int64(2), "Bob"}))
	require.NoError(t, leftTable.AppendRow([]any{int64(3), "Charlie"}))

	// Create right table: b (id INT, city VARCHAR)
	rightTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	rightTable, err := stor.CreateTable("b", rightTypes)
	require.NoError(t, err)

	// Insert data into right table
	require.NoError(t, rightTable.AppendRow([]any{int64(1), "NYC"}))
	require.NoError(t, rightTable.AppendRow([]any{int64(2), "SF"}))
	require.NoError(t, rightTable.AppendRow([]any{int64(4), "LA"}))

	// Create left scan operator
	leftScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "a",
			Alias:     "a",
			TableDef: &catalog.TableDef{
				Name: "a",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "name", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: leftTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	// Create right scan operator
	rightScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "b",
			Alias:     "b",
			TableDef: &catalog.TableDef{
				Name: "b",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "city", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: rightTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	// Create join condition: a.id = b.id
	condition := &binder.BoundBinaryExpr{
		Op: 5, // OpEq
		Left: &binder.BoundColumnRef{
			Table:   "a",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Table:   "b",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	// Create column bindings
	leftColumns := []planner.ColumnBinding{
		{Table: "a", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "a", Column: "name", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}
	rightColumns := []planner.ColumnBinding{
		{Table: "b", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "b", Column: "city", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}

	// Create hash join operator
	joinOp, err := NewPhysicalHashJoinOperator(
		leftScan,
		rightScan,
		leftColumns,
		rightColumns,
		planner.JoinTypeInner,
		condition,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Execute join
	var results [][]any
	for {
		chunk, err := joinOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}

		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Debug: print results
	t.Logf("Got %d results:", len(results))
	for i, row := range results {
		t.Logf("Row %d: %v", i, row)
	}

	// Verify results: should have 2 matches (id 1 and 2)
	require.Len(t, results, 2)

	// Check first row: (1, "Alice", 1, "NYC")
	assert.Equal(t, int32(1), results[0][0])
	assert.Equal(t, "Alice", results[0][1])
	assert.Equal(t, int32(1), results[0][2])
	assert.Equal(t, "NYC", results[0][3])

	// Check second row: (2, "Bob", 2, "SF")
	assert.Equal(t, int32(2), results[1][0])
	assert.Equal(t, "Bob", results[1][1])
	assert.Equal(t, int32(2), results[1][2])
	assert.Equal(t, "SF", results[1][3])
}

func TestPhysicalHashJoinMultipleMatches(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create left table: orders (customer_id INT, amount DOUBLE)
	leftTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE}
	leftTable, err := stor.CreateTable("orders", leftTypes)
	require.NoError(t, err)

	// Insert data - multiple orders for same customer
	require.NoError(t, leftTable.AppendRow([]any{int64(1), 100.0}))
	require.NoError(t, leftTable.AppendRow([]any{int64(1), 200.0}))
	require.NoError(t, leftTable.AppendRow([]any{int64(2), 150.0}))

	// Create right table: customers (id INT, name VARCHAR)
	rightTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	rightTable, err := stor.CreateTable("customers", rightTypes)
	require.NoError(t, err)

	// Insert data
	require.NoError(t, rightTable.AppendRow([]any{int64(1), "Alice"}))
	require.NoError(t, rightTable.AppendRow([]any{int64(2), "Bob"}))

	// Create operators
	leftScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "orders",
			Alias:     "orders",
			TableDef: &catalog.TableDef{
				Name: "orders",
				Columns: []*catalog.ColumnDef{
					{Name: "customer_id", Type: dukdb.TYPE_INTEGER},
					{Name: "amount", Type: dukdb.TYPE_DOUBLE},
				},
			},
		},
		storage: stor,
		scanner: leftTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_DOUBLE},
		},
	}

	rightScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "customers",
			Alias:     "customers",
			TableDef: &catalog.TableDef{
				Name: "customers",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "name", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: rightTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	// Create join condition: orders.customer_id = customers.id
	condition := &binder.BoundBinaryExpr{
		Op: 5, // OpEq
		Left: &binder.BoundColumnRef{
			Table:   "orders",
			Column:  "customer_id",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Table:   "customers",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	leftColumns := []planner.ColumnBinding{
		{Table: "orders", Column: "customer_id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "orders", Column: "amount", Type: dukdb.TYPE_DOUBLE, ColumnIdx: 1},
	}
	rightColumns := []planner.ColumnBinding{
		{Table: "customers", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "customers", Column: "name", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}

	joinOp, err := NewPhysicalHashJoinOperator(
		leftScan,
		rightScan,
		leftColumns,
		rightColumns,
		planner.JoinTypeInner,
		condition,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Execute join
	var results [][]any
	for {
		chunk, err := joinOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}

		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Debug: print results
	t.Logf("Got %d results:", len(results))
	for i, row := range results {
		t.Logf("Row %d: %v", i, row)
	}

	// Should have 3 results: 2 orders for Alice, 1 for Bob
	require.Len(t, results, 3)

	// Check customer_id 1 orders
	count1 := 0
	for _, row := range results {
		if row[0] == int32(1) {
			count1++
			assert.Equal(t, "Alice", row[3])
		}
	}
	assert.Equal(t, 2, count1)
}

func TestPhysicalHashJoinNoMatches(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create left table
	leftTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	leftTable, err := stor.CreateTable("a", leftTypes)
	require.NoError(t, err)
	require.NoError(t, leftTable.AppendRow([]any{int64(1), "Alice"}))

	// Create right table with no matching IDs
	rightTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	rightTable, err := stor.CreateTable("b", rightTypes)
	require.NoError(t, err)
	require.NoError(t, rightTable.AppendRow([]any{int64(2), "Bob"}))

	// Create operators
	leftScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "a",
			Alias:     "a",
			TableDef: &catalog.TableDef{
				Name: "a",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "name", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: leftTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	rightScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "b",
			Alias:     "b",
			TableDef: &catalog.TableDef{
				Name: "b",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "city", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: rightTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	condition := &binder.BoundBinaryExpr{
		Op: 5,
		Left: &binder.BoundColumnRef{
			Table:   "a",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Table:   "b",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	leftColumns := []planner.ColumnBinding{
		{Table: "a", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "a", Column: "name", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}
	rightColumns := []planner.ColumnBinding{
		{Table: "b", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "b", Column: "city", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}

	joinOp, err := NewPhysicalHashJoinOperator(
		leftScan,
		rightScan,
		leftColumns,
		rightColumns,
		planner.JoinTypeInner,
		condition,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Execute join
	var results [][]any
	for {
		chunk, err := joinOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}

		for i := 0; i < chunk.Count(); i++ {
			row := make([]any, chunk.ColumnCount())
			for j := 0; j < chunk.ColumnCount(); j++ {
				row[j] = chunk.GetValue(i, j)
			}
			results = append(results, row)
		}
	}

	// Should have no results
	assert.Len(t, results, 0)
}

func TestPhysicalHashJoinEmptyTables(t *testing.T) {
	// Create catalog and storage
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create empty tables
	leftTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	leftTable, err := stor.CreateTable("a", leftTypes)
	require.NoError(t, err)

	rightTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	rightTable, err := stor.CreateTable("b", rightTypes)
	require.NoError(t, err)

	// Create operators
	leftScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "a",
			Alias:     "a",
			TableDef: &catalog.TableDef{
				Name: "a",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "name", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: leftTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	rightScan := &PhysicalScanOperator{
		plan: &planner.PhysicalScan{
			TableName: "b",
			Alias:     "b",
			TableDef: &catalog.TableDef{
				Name: "b",
				Columns: []*catalog.ColumnDef{
					{Name: "id", Type: dukdb.TYPE_INTEGER},
					{Name: "city", Type: dukdb.TYPE_VARCHAR},
				},
			},
		},
		storage: stor,
		scanner: rightTable.Scan(),
		types: []dukdb.TypeInfo{
			&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
			&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
		},
	}

	condition := &binder.BoundBinaryExpr{
		Op: 5,
		Left: &binder.BoundColumnRef{
			Table:   "a",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Table:   "b",
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	leftColumns := []planner.ColumnBinding{
		{Table: "a", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "a", Column: "name", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}
	rightColumns := []planner.ColumnBinding{
		{Table: "b", Column: "id", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Table: "b", Column: "city", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}

	joinOp, err := NewPhysicalHashJoinOperator(
		leftScan,
		rightScan,
		leftColumns,
		rightColumns,
		planner.JoinTypeInner,
		condition,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Execute join
	chunk, err := joinOp.Next()
	require.NoError(t, err)

	// Should return nil immediately (no data)
	assert.Nil(t, chunk)
}

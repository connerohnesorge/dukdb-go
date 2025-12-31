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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPhaseA_E2E_BasicSelectStar tests that SELECT * FROM t returns a DataChunk
// with all columns and rows. (Task 1.28)
func TestPhaseA_E2E_BasicSelectStar(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table with multiple types
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable("test_table", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice", 10.5}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob", 20.0}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie", 30.5}))

	// Create table definition with TypeInfo
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	varcharInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
	doubleInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_DOUBLE)

	col1 := catalog.NewColumnDef("id", dukdb.TYPE_INTEGER)
	col1.TypeInfo = intInfo
	col2 := catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR)
	col2.TypeInfo = varcharInfo
	col3 := catalog.NewColumnDef("score", dukdb.TYPE_DOUBLE)
	col3.TypeInfo = doubleInfo

	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		col1, col2, col3,
	})

	// Create scan operator (simulates SELECT * FROM test_table)
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	// Execute and get DataChunk
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "SELECT * should return DataChunk")

	// Verify DataChunk structure
	assert.Equal(t, 3, chunk.ColumnCount(), "Should have 3 columns")
	assert.Equal(t, 3, chunk.Count(), "Should have 3 rows")

	// Verify types
	types := chunk.Types()
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])
	assert.Equal(t, dukdb.TYPE_DOUBLE, types[2])

	// Verify data values
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "Alice", chunk.GetValue(0, 1))
	assert.InDelta(t, 10.5, chunk.GetValue(0, 2), 0.001)

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "Bob", chunk.GetValue(1, 1))
	assert.InDelta(t, 20.0, chunk.GetValue(1, 2), 0.001)

	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
	assert.Equal(t, "Charlie", chunk.GetValue(2, 1))
	assert.InDelta(t, 30.5, chunk.GetValue(2, 2), 0.001)

	// No more data
	chunk, err = scanOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "No more chunks should be available")
}

// TestPhaseA_E2E_ColumnProjection tests that SELECT a, b FROM t returns a
// 2-column DataChunk with only the projected columns. (Task 1.29)
func TestPhaseA_E2E_ColumnProjection(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table with 4 columns
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}
	table, err := stor.CreateTable("test_table", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice", 10.5, true}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob", 20.0, false}))

	// Create table definition
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("score", dukdb.TYPE_DOUBLE),
		catalog.NewColumnDef("active", dukdb.TYPE_BOOLEAN),
	})

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	// Create project operator that selects only columns 0 and 1 (id, name)
	childColumns := []planner.ColumnBinding{
		{Table: "test_table", Column: "id", Type: dukdb.TYPE_INTEGER},
		{Table: "test_table", Column: "name", Type: dukdb.TYPE_VARCHAR},
		{Table: "test_table", Column: "score", Type: dukdb.TYPE_DOUBLE},
		{Table: "test_table", Column: "active", Type: dukdb.TYPE_BOOLEAN},
	}

	projectExprs := []binder.BoundExpr{
		&binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
		&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	projectOp, err := NewPhysicalProjectOperator(scanOp, childColumns, projectExprs, exec, execCtx)
	require.NoError(t, err)

	// Execute and get DataChunk
	chunk, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "SELECT a, b should return DataChunk")

	// Verify DataChunk structure - should only have 2 columns
	assert.Equal(t, 2, chunk.ColumnCount(), "Should have 2 columns (projected)")
	assert.Equal(t, 2, chunk.Count(), "Should have 2 rows")

	// Verify types
	types := chunk.Types()
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])

	// Verify data values - only id and name
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "Alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "Bob", chunk.GetValue(1, 1))
}

// TestPhaseA_E2E_FilteredQuery tests that SELECT * FROM t WHERE x > 5 filters
// rows correctly and returns DataChunk with only matching rows. (Task 1.30)
func TestPhaseA_E2E_FilteredQuery(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable("test_table", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(3), "Alice"}))
	require.NoError(t, table.AppendRow([]any{int32(7), "Bob"}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Charlie"}))
	require.NoError(t, table.AppendRow([]any{int32(10), "David"}))
	require.NoError(t, table.AppendRow([]any{int32(5), "Eve"}))

	// Create table definition
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
	})

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	// Create filter operator: WHERE id > 5
	filterPredicate := &binder.BoundBinaryExpr{
		Op:      parser.OpGt,
		Left:    &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
		Right:   &binder.BoundLiteral{Value: int32(5), ValType: dukdb.TYPE_INTEGER},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{Table: "test_table", Column: "id", Type: dukdb.TYPE_INTEGER},
		{Table: "test_table", Column: "name", Type: dukdb.TYPE_VARCHAR},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(scanOp, childColumns, filterPredicate, exec, execCtx)

	// Execute and get DataChunk
	chunk, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Filtered query should return DataChunk")

	// Verify DataChunk structure
	assert.Equal(t, 2, chunk.ColumnCount(), "Should have 2 columns")
	assert.Equal(t, 2, chunk.Count(), "Should have 2 rows (id > 5: Bob=7, David=10)")

	// Verify filtered data - should only have rows where id > 5
	// Bob (id=7) and David (id=10)
	val1 := chunk.GetValue(0, 0)
	val2 := chunk.GetValue(1, 0)

	// Check that we got the correct rows (order might vary)
	ids := []int32{val1.(int32), val2.(int32)}
	assert.Contains(t, ids, int32(7), "Should contain Bob's id (7)")
	assert.Contains(t, ids, int32(10), "Should contain David's id (10)")

	// No more data
	chunk, err = filterOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "No more chunks should be available")
}

// TestPhaseA_E2E_NullHandling tests that NULL values are handled correctly
// throughout the operator pipeline. (Task 1.31)
func TestPhaseA_E2E_NullHandling(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable("test_table", columnTypes)
	require.NoError(t, err)

	// Insert test data with NULLs
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice", 10.5}))
	require.NoError(t, table.AppendRow([]any{int32(2), nil, 20.0}))      // NULL name
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie", nil})) // NULL score
	require.NoError(t, table.AppendRow([]any{nil, "David", 40.0}))       // NULL id

	// Create table definition
	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("score", dukdb.TYPE_DOUBLE),
	})

	// Test 1: Scan with NULLs
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Verify NULLs are preserved in scan
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "Alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Nil(t, chunk.GetValue(1, 1), "NULL name should be preserved")

	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
	assert.Nil(t, chunk.GetValue(2, 2), "NULL score should be preserved")

	assert.Nil(t, chunk.GetValue(3, 0), "NULL id should be preserved")
	assert.Equal(t, "David", chunk.GetValue(3, 1))

	// Test 2: Filter with IS NULL predicate
	scanPlan2 := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp2, err := NewPhysicalScanOperator(scanPlan2, stor)
	require.NoError(t, err)

	filterPredicate := &binder.BoundUnaryExpr{
		Op:      parser.OpIsNull,
		Expr:    &binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{Table: "test_table", Column: "id", Type: dukdb.TYPE_INTEGER},
		{Table: "test_table", Column: "name", Type: dukdb.TYPE_VARCHAR},
		{Table: "test_table", Column: "score", Type: dukdb.TYPE_DOUBLE},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(scanOp2, childColumns, filterPredicate, exec, execCtx)

	chunk, err = filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Should only return row with NULL name (id=2)
	assert.Equal(t, 1, chunk.Count(), "Should have 1 row with NULL name")
	assert.Equal(t, int32(2), chunk.GetValue(0, 0))
	assert.Nil(t, chunk.GetValue(0, 1))

	// Test 3: Filter with IS NOT NULL predicate
	scanPlan3 := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp3, err := NewPhysicalScanOperator(scanPlan3, stor)
	require.NoError(t, err)

	filterPredicate2 := &binder.BoundUnaryExpr{
		Op:      parser.OpIsNotNull,
		Expr:    &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	filterOp2 := NewPhysicalFilterOperator(scanOp3, childColumns, filterPredicate2, exec, execCtx)

	chunk, err = filterOp2.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Should return all rows with non-NULL id (3 rows)
	assert.Equal(t, 3, chunk.Count(), "Should have 3 rows with non-NULL id")

	// Verify no NULL ids in results
	for i := 0; i < chunk.Count(); i++ {
		assert.NotNil(t, chunk.GetValue(i, 0), "All ids should be non-NULL")
	}
}

// TestPhaseA_E2E_ScanFilterProjectPipeline tests the complete pipeline:
// Scan -> Filter -> Project. (Task 1.32 - Validation)
func TestPhaseA_E2E_ScanFilterProjectPipeline(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}
	table, err := stor.CreateTable("employees", columnTypes)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{int32(1), "Alice", 50000.0, true}))
	require.NoError(t, table.AppendRow([]any{int32(2), "Bob", 60000.0, false}))
	require.NoError(t, table.AppendRow([]any{int32(3), "Charlie", 75000.0, true}))
	require.NoError(t, table.AppendRow([]any{int32(4), "David", 45000.0, true}))
	require.NoError(t, table.AppendRow([]any{int32(5), "Eve", 80000.0, false}))

	// Create table definition
	tableDef := catalog.NewTableDef("employees", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("salary", dukdb.TYPE_DOUBLE),
		catalog.NewColumnDef("active", dukdb.TYPE_BOOLEAN),
	})

	// Step 1: Create Scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "employees",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	// Step 2: Create Filter operator: WHERE salary > 55000 AND active = true
	// First filter: salary > 55000
	salaryFilter := &binder.BoundBinaryExpr{
		Op:      parser.OpGt,
		Left:    &binder.BoundColumnRef{Column: "salary", ColType: dukdb.TYPE_DOUBLE},
		Right:   &binder.BoundLiteral{Value: 55000.0, ValType: dukdb.TYPE_DOUBLE},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	// Second filter: active = true
	activeFilter := &binder.BoundBinaryExpr{
		Op:      parser.OpEq,
		Left:    &binder.BoundColumnRef{Column: "active", ColType: dukdb.TYPE_BOOLEAN},
		Right:   &binder.BoundLiteral{Value: true, ValType: dukdb.TYPE_BOOLEAN},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	// Combined filter: salary > 55000 AND active = true
	combinedFilter := &binder.BoundBinaryExpr{
		Op:      parser.OpAnd,
		Left:    salaryFilter,
		Right:   activeFilter,
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{Table: "employees", Column: "id", Type: dukdb.TYPE_INTEGER},
		{Table: "employees", Column: "name", Type: dukdb.TYPE_VARCHAR},
		{Table: "employees", Column: "salary", Type: dukdb.TYPE_DOUBLE},
		{Table: "employees", Column: "active", Type: dukdb.TYPE_BOOLEAN},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(scanOp, childColumns, combinedFilter, exec, execCtx)

	// Step 3: Create Project operator: SELECT name, salary
	projectExprs := []binder.BoundExpr{
		&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
		&binder.BoundColumnRef{Column: "salary", ColType: dukdb.TYPE_DOUBLE},
	}

	projectOp, err := NewPhysicalProjectOperator(filterOp, childColumns, projectExprs, exec, execCtx)
	require.NoError(t, err)

	// Execute the complete pipeline
	chunk, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk, "Pipeline should return DataChunk")

	// Verify results
	// Should only have Charlie (salary=75000, active=true)
	assert.Equal(t, 2, chunk.ColumnCount(), "Should have 2 columns (projected)")
	assert.Equal(t, 1, chunk.Count(), "Should have 1 row (Charlie only)")

	// Verify projected data
	assert.Equal(t, "Charlie", chunk.GetValue(0, 0))
	assert.InDelta(t, 75000.0, chunk.GetValue(0, 1), 0.001)

	// Verify types are correct
	types := chunk.Types()
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[0])
	assert.Equal(t, dukdb.TYPE_DOUBLE, types[1])

	// Verify TypeInfo from operator
	typeInfos := projectOp.GetTypes()
	assert.Equal(t, 2, len(typeInfos))
	assert.Equal(t, dukdb.TYPE_VARCHAR, typeInfos[0].InternalType())
	assert.Equal(t, dukdb.TYPE_DOUBLE, typeInfos[1].InternalType())

	// No more data
	chunk, err = projectOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk, "No more chunks should be available")
}

// TestPhaseA_E2E_MultipleChunks tests that operators correctly handle
// data that spans multiple DataChunks.
func TestPhaseA_E2E_MultipleChunks(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table
	columnTypes := []dukdb.Type{dukdb.TYPE_INTEGER}
	table, err := stor.CreateTable("large_table", columnTypes)
	require.NoError(t, err)

	// Insert enough rows to span multiple chunks
	numRows := storage.StandardVectorSize + 500
	for i := 0; i < numRows; i++ {
		require.NoError(t, table.AppendRow([]any{int32(i)}))
	}

	// Create table definition
	tableDef := catalog.NewTableDef("large_table", []*catalog.ColumnDef{
		catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
	})

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "large_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	// Create filter operator: WHERE id % 2 = 0 (even numbers only)
	// This will test that filtering works across multiple chunks
	filterPredicate := &binder.BoundBinaryExpr{
		Op: parser.OpEq,
		Left: &binder.BoundBinaryExpr{
			Op:      parser.OpMod,
			Left:    &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
			Right:   &binder.BoundLiteral{Value: int32(2), ValType: dukdb.TYPE_INTEGER},
			ResType: dukdb.TYPE_INTEGER,
		},
		Right:   &binder.BoundLiteral{Value: int32(0), ValType: dukdb.TYPE_INTEGER},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{Table: "large_table", Column: "id", Type: dukdb.TYPE_INTEGER},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(scanOp, childColumns, filterPredicate, exec, execCtx)

	// Collect all chunks
	var totalRows int
	var chunkCount int
	for {
		chunk, err := filterOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunkCount++
		totalRows += chunk.Count()

		// Verify all values in this chunk are even
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0).(int32)
			assert.Equal(t, int32(0), val%2, "All filtered values should be even")
		}
	}

	// Verify we got approximately half the rows (even numbers)
	expectedRows := numRows / 2
	assert.Equal(t, expectedRows, totalRows, "Should have filtered to even numbers only")
	assert.GreaterOrEqual(t, chunkCount, 2, "Should have multiple chunks")
}

// TestPhaseA_E2E_TypeInfoPropagation tests that TypeInfo is correctly
// propagated through the operator pipeline.
func TestPhaseA_E2E_TypeInfoPropagation(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // Used in child operators

	// Create table with complex types
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable("test_table", columnTypes)
	require.NoError(t, err)

	require.NoError(t, table.AppendRow([]any{int32(1), "test", 1.5}))

	// Create table definition with explicit TypeInfo
	intInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	varcharInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
	doubleInfo, _ := dukdb.NewTypeInfo(dukdb.TYPE_DOUBLE)

	col1 := catalog.NewColumnDef("id", dukdb.TYPE_INTEGER)
	col1.TypeInfo = intInfo
	col2 := catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR)
	col2.TypeInfo = varcharInfo
	col3 := catalog.NewColumnDef("value", dukdb.TYPE_DOUBLE)
	col3.TypeInfo = doubleInfo

	tableDef := catalog.NewTableDef("test_table", []*catalog.ColumnDef{
		col1, col2, col3,
	})

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(scanPlan, stor)
	require.NoError(t, err)

	// Verify TypeInfo from scan
	scanTypes := scanOp.GetTypes()
	assert.Equal(t, 3, len(scanTypes))
	assert.Equal(t, dukdb.TYPE_INTEGER, scanTypes[0].InternalType())
	assert.Equal(t, "INTEGER", scanTypes[0].SQLType())
	assert.Equal(t, dukdb.TYPE_VARCHAR, scanTypes[1].InternalType())
	assert.Equal(t, "VARCHAR", scanTypes[1].SQLType())
	assert.Equal(t, dukdb.TYPE_DOUBLE, scanTypes[2].InternalType())
	assert.Equal(t, "DOUBLE", scanTypes[2].SQLType())

	// Create filter operator
	filterPredicate := &binder.BoundBinaryExpr{
		Op:      parser.OpGt,
		Left:    &binder.BoundColumnRef{Column: "id", ColType: dukdb.TYPE_INTEGER},
		Right:   &binder.BoundLiteral{Value: int32(0), ValType: dukdb.TYPE_INTEGER},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{Table: "test_table", Column: "id", Type: dukdb.TYPE_INTEGER},
		{Table: "test_table", Column: "name", Type: dukdb.TYPE_VARCHAR},
		{Table: "test_table", Column: "value", Type: dukdb.TYPE_DOUBLE},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(scanOp, childColumns, filterPredicate, exec, execCtx)

	// Verify TypeInfo propagates through filter
	filterTypes := filterOp.GetTypes()
	assert.Equal(t, 3, len(filterTypes))
	assert.Equal(t, dukdb.TYPE_INTEGER, filterTypes[0].InternalType())
	assert.Equal(t, dukdb.TYPE_VARCHAR, filterTypes[1].InternalType())
	assert.Equal(t, dukdb.TYPE_DOUBLE, filterTypes[2].InternalType())

	// Create project operator
	projectExprs := []binder.BoundExpr{
		&binder.BoundColumnRef{Column: "name", ColType: dukdb.TYPE_VARCHAR},
	}

	projectOp, err := NewPhysicalProjectOperator(filterOp, childColumns, projectExprs, exec, execCtx)
	require.NoError(t, err)

	// Verify TypeInfo from project
	projectTypes := projectOp.GetTypes()
	assert.Equal(t, 1, len(projectTypes))
	assert.Equal(t, dukdb.TYPE_VARCHAR, projectTypes[0].InternalType())
	assert.Equal(t, "VARCHAR", projectTypes[0].SQLType())
}

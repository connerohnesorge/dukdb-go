package executor

import (
	"context"
	"database/sql/driver"
	"io"
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

// TestPhaseB_E2E_ResultSetFromScan tests ResultSet created from PhysicalScan operator output.
// Validates that ResultSet can iterate over all rows from a table scan.
// (Tasks 2.15-2.18, 2.23)
func TestPhaseB_E2E_ResultSetFromScan(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	_ = cat // catalog used implicitly by executor

	// Create table with multiple types
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable(
		"employees",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert test data
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(1), "Alice", 50000.0},
		),
	)
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(2), "Bob", 60000.0},
		),
	)
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(3), "Charlie", 75000.0},
		),
	)

	// Create table definition with TypeInfo
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	doubleInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_DOUBLE,
	)

	col1 := catalog.NewColumnDef(
		"id",
		dukdb.TYPE_INTEGER,
	)
	col1.TypeInfo = intInfo
	col2 := catalog.NewColumnDef(
		"name",
		dukdb.TYPE_VARCHAR,
	)
	col2.TypeInfo = varcharInfo
	col3 := catalog.NewColumnDef(
		"salary",
		dukdb.TYPE_DOUBLE,
	)
	col3.TypeInfo = doubleInfo

	tableDef := catalog.NewTableDef(
		"employees",
		[]*catalog.ColumnDef{
			col1, col2, col3,
		},
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "employees",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Collect all DataChunks from scan
	var chunks []*storage.DataChunk
	for {
		chunk, err := scanOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Create ResultSet from chunks
	columnNames := []string{
		"id",
		"name",
		"salary",
	}
	typeInfos := scanOp.GetTypes()
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Test Columns() method (Task 2.23)
	columns := rs.Columns()
	assert.Equal(
		t,
		columnNames,
		columns,
		"Columns() should return correct column names",
	)

	// Test Next() iteration (Task 2.24)
	dest := make([]driver.Value, 3)

	// First row
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dest[0])
	assert.Equal(t, "Alice", dest[1])
	assert.Equal(t, float64(50000.0), dest[2])

	// Second row
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(2), dest[0])
	assert.Equal(t, "Bob", dest[1])
	assert.Equal(t, float64(60000.0), dest[2])

	// Third row
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(3), dest[0])
	assert.Equal(t, "Charlie", dest[1])
	assert.Equal(t, float64(75000.0), dest[2])

	// Should return io.EOF when exhausted (Task 2.17)
	err = rs.Next(dest)
	assert.Equal(
		t,
		io.EOF,
		err,
		"Next() should return io.EOF when exhausted",
	)

	// Test Close()
	err = rs.Close()
	require.NoError(t, err)
}

// TestPhaseB_E2E_ResultSetAcrossChunkBoundaries tests ResultSet iteration
// when data spans multiple chunks. Validates chunk boundary handling.
// (Tasks 2.15-2.18)
func TestPhaseB_E2E_ResultSetAcrossChunkBoundaries(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	_ = cat // catalog used implicitly by executor

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
	}
	table, err := stor.CreateTable(
		"numbers",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert enough rows to create multiple chunks (StandardVectorSize + extra)
	numRows := storage.StandardVectorSize + 100
	for i := range numRows {
		require.NoError(
			t,
			table.AppendRow([]any{int32(i)}),
		)
	}

	// Create table definition
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	col1 := catalog.NewColumnDef(
		"value",
		dukdb.TYPE_INTEGER,
	)
	col1.TypeInfo = intInfo
	tableDef := catalog.NewTableDef(
		"numbers",
		[]*catalog.ColumnDef{col1},
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "numbers",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Collect all chunks
	var chunks []*storage.DataChunk
	for {
		chunk, err := scanOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Should have multiple chunks
	require.GreaterOrEqual(
		t,
		len(chunks),
		2,
		"Should have multiple chunks for this test",
	)

	// Create ResultSet
	columnNames := []string{"value"}
	typeInfos := scanOp.GetTypes()
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Iterate through all rows and verify correct handling of chunk boundaries
	dest := make([]driver.Value, 1)
	rowCount := 0
	for {
		err := rs.Next(dest)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Verify value is correct
		assert.Equal(
			t,
			int64(rowCount),
			dest[0],
			"Row %d should have correct value",
			rowCount,
		)
		rowCount++
	}

	// Verify we got all rows
	assert.Equal(
		t,
		numRows,
		rowCount,
		"Should iterate through all rows across chunk boundaries",
	)

	err = rs.Close()
	require.NoError(t, err)
}

// TestPhaseB_E2E_ResultSetTypeConversion tests that ResultSet correctly
// converts all DuckDB types to driver.Value types.
// (Tasks 2.19-2.20)
func TestPhaseB_E2E_ResultSetTypeConversion(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	_ = cat // catalog used implicitly by executor

	// Create table with various types
	columnTypes := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable(
		"types_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert test data
	require.NoError(t, table.AppendRow([]any{
		true,
		int8(10),
		int16(1000),
		int32(100000),
		int64(10000000),
		float32(3.14),
		float64(2.71828),
		"test string",
	}))

	// Create table definition with TypeInfo
	boolInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_BOOLEAN,
	)
	tinyIntInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_TINYINT,
	)
	smallIntInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_SMALLINT,
	)
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	bigIntInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_BIGINT,
	)
	floatInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_FLOAT,
	)
	doubleInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_DOUBLE,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)

	columns := []*catalog.ColumnDef{
		{
			Name:     "bool_col",
			Type:     dukdb.TYPE_BOOLEAN,
			TypeInfo: boolInfo,
		},
		{
			Name:     "tiny_col",
			Type:     dukdb.TYPE_TINYINT,
			TypeInfo: tinyIntInfo,
		},
		{
			Name:     "small_col",
			Type:     dukdb.TYPE_SMALLINT,
			TypeInfo: smallIntInfo,
		},
		{
			Name:     "int_col",
			Type:     dukdb.TYPE_INTEGER,
			TypeInfo: intInfo,
		},
		{
			Name:     "big_col",
			Type:     dukdb.TYPE_BIGINT,
			TypeInfo: bigIntInfo,
		},
		{
			Name:     "float_col",
			Type:     dukdb.TYPE_FLOAT,
			TypeInfo: floatInfo,
		},
		{
			Name:     "double_col",
			Type:     dukdb.TYPE_DOUBLE,
			TypeInfo: doubleInfo,
		},
		{
			Name:     "varchar_col",
			Type:     dukdb.TYPE_VARCHAR,
			TypeInfo: varcharInfo,
		},
	}
	tableDef := catalog.NewTableDef(
		"types_table",
		columns,
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "types_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Get chunk
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Create ResultSet
	columnNames := []string{
		"bool_col",
		"tiny_col",
		"small_col",
		"int_col",
		"big_col",
		"float_col",
		"double_col",
		"varchar_col",
	}
	typeInfos := scanOp.GetTypes()
	rs := NewResultSet(
		[]*storage.DataChunk{chunk},
		typeInfos,
		columnNames,
	)

	// Test type conversion (Task 2.25)
	dest := make([]driver.Value, 8)
	err = rs.Next(dest)
	require.NoError(t, err)

	// Verify conversions to driver.Value types
	assert.Equal(
		t,
		true,
		dest[0],
		"BOOLEAN should be bool",
	)
	assert.Equal(
		t,
		int64(10),
		dest[1],
		"TINYINT should be int64",
	)
	assert.Equal(
		t,
		int64(1000),
		dest[2],
		"SMALLINT should be int64",
	)
	assert.Equal(
		t,
		int64(100000),
		dest[3],
		"INTEGER should be int64",
	)
	assert.Equal(
		t,
		int64(10000000),
		dest[4],
		"BIGINT should be int64",
	)
	assert.InDelta(
		t,
		3.14,
		dest[5],
		0.001,
		"FLOAT should be float64",
	)
	assert.InDelta(
		t,
		2.71828,
		dest[6],
		0.00001,
		"DOUBLE should be float64",
	)
	assert.Equal(
		t,
		"test string",
		dest[7],
		"VARCHAR should be string",
	)

	err = rs.Close()
	require.NoError(t, err)
}

// TestPhaseB_E2E_ResultSetNullHandling tests that ResultSet correctly
// handles NULL values from the operator pipeline.
// (Tasks 2.21-2.22)
func TestPhaseB_E2E_ResultSetNullHandling(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	_ = cat // catalog used implicitly by executor

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable(
		"nullable_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert rows with various NULL patterns
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(1), "Alice", 50000.0},
		),
	) // No NULLs
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(2), nil, 60000.0},
		),
	) // NULL name
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(3), "Charlie", nil},
		),
	) // NULL salary
	require.NoError(
		t,
		table.AppendRow(
			[]any{nil, "David", 70000.0},
		),
	) // NULL id
	require.NoError(
		t,
		table.AppendRow([]any{nil, nil, nil}),
	) // All NULLs
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(6), "Frank", 80000.0},
		),
	) // No NULLs

	// Create table definition
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	doubleInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_DOUBLE,
	)

	columns := []*catalog.ColumnDef{
		{
			Name:     "id",
			Type:     dukdb.TYPE_INTEGER,
			TypeInfo: intInfo,
		},
		{
			Name:     "name",
			Type:     dukdb.TYPE_VARCHAR,
			TypeInfo: varcharInfo,
		},
		{
			Name:     "salary",
			Type:     dukdb.TYPE_DOUBLE,
			TypeInfo: doubleInfo,
		},
	}
	tableDef := catalog.NewTableDef(
		"nullable_table",
		columns,
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "nullable_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Collect chunks
	var chunks []*storage.DataChunk
	for {
		chunk, err := scanOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Create ResultSet
	columnNames := []string{
		"id",
		"name",
		"salary",
	}
	typeInfos := scanOp.GetTypes()
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	dest := make([]driver.Value, 3)

	// Row 1: No NULLs
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(1), dest[0])
	assert.Equal(t, "Alice", dest[1])
	assert.Equal(t, float64(50000.0), dest[2])

	// Row 2: NULL name
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(2), dest[0])
	assert.Nil(
		t,
		dest[1],
		"NULL name should be nil",
	)
	assert.Equal(t, float64(60000.0), dest[2])

	// Row 3: NULL salary
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(3), dest[0])
	assert.Equal(t, "Charlie", dest[1])
	assert.Nil(
		t,
		dest[2],
		"NULL salary should be nil",
	)

	// Row 4: NULL id
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Nil(
		t,
		dest[0],
		"NULL id should be nil",
	)
	assert.Equal(t, "David", dest[1])
	assert.Equal(t, float64(70000.0), dest[2])

	// Row 5: All NULLs
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Nil(
		t,
		dest[0],
		"All values should be nil",
	)
	assert.Nil(
		t,
		dest[1],
		"All values should be nil",
	)
	assert.Nil(
		t,
		dest[2],
		"All values should be nil",
	)

	// Row 6: No NULLs
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(6), dest[0])
	assert.Equal(t, "Frank", dest[1])
	assert.Equal(t, float64(80000.0), dest[2])

	// Should be exhausted
	err = rs.Next(dest)
	assert.Equal(t, io.EOF, err)

	err = rs.Close()
	require.NoError(t, err)
}

// TestPhaseB_E2E_ResultSetColumnMetadata tests that ResultSet provides
// correct column type metadata through ColumnTypeDatabaseTypeName.
// (Task 2.26)
func TestPhaseB_E2E_ResultSetColumnMetadata(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	_ = cat // catalog used implicitly by executor

	// Create table with various types including DECIMAL
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DECIMAL,
	}
	table, err := stor.CreateTable(
		"products",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert one row
	require.NoError(
		t,
		table.AppendRow(
			[]any{
				int32(1),
				"Widget",
				int64(1250),
			},
		),
	) // 12.50 as decimal

	// Create table definition with TypeInfo including DECIMAL details
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	decimalInfo, _ := dukdb.NewDecimalInfo(10, 2)

	columns := []*catalog.ColumnDef{
		{
			Name:     "id",
			Type:     dukdb.TYPE_INTEGER,
			TypeInfo: intInfo,
		},
		{
			Name:     "name",
			Type:     dukdb.TYPE_VARCHAR,
			TypeInfo: varcharInfo,
		},
		{
			Name:     "price",
			Type:     dukdb.TYPE_DECIMAL,
			TypeInfo: decimalInfo,
		},
	}
	tableDef := catalog.NewTableDef(
		"products",
		columns,
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "products",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Get chunk
	chunk, err := scanOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Create ResultSet
	columnNames := []string{"id", "name", "price"}
	typeInfos := scanOp.GetTypes()
	rs := NewResultSet(
		[]*storage.DataChunk{chunk},
		typeInfos,
		columnNames,
	)

	// Test ColumnTypeDatabaseTypeName (Task 2.26)
	assert.Equal(
		t,
		"INTEGER",
		rs.ColumnTypeDatabaseTypeName(0),
	)
	assert.Equal(
		t,
		"VARCHAR",
		rs.ColumnTypeDatabaseTypeName(1),
	)
	assert.Equal(
		t,
		"DECIMAL(10,2)",
		rs.ColumnTypeDatabaseTypeName(2),
	)

	// Test out of bounds
	assert.Equal(
		t,
		"",
		rs.ColumnTypeDatabaseTypeName(-1),
	)
	assert.Equal(
		t,
		"",
		rs.ColumnTypeDatabaseTypeName(3),
	)

	// Test ColumnTypeInfo
	typeInfo0 := rs.ColumnTypeInfo(0)
	require.NotNil(t, typeInfo0)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		typeInfo0.InternalType(),
	)

	typeInfo2 := rs.ColumnTypeInfo(2)
	require.NotNil(t, typeInfo2)
	assert.Equal(
		t,
		dukdb.TYPE_DECIMAL,
		typeInfo2.InternalType(),
	)
	details := typeInfo2.Details()
	require.NotNil(t, details)
	decimalDetails, ok := details.(*dukdb.DecimalDetails)
	require.True(t, ok)
	assert.Equal(
		t,
		uint8(10),
		decimalDetails.Width,
	)
	assert.Equal(
		t,
		uint8(2),
		decimalDetails.Scale,
	)

	err = rs.Close()
	require.NoError(t, err)
}

// TestPhaseB_E2E_CompleteQueryPipeline tests the complete integration:
// Create table → Insert data → Query with operators → Iterate with ResultSet
// This validates the entire Phase B functionality end-to-end.
// (Task 2.27 - Validation)
func TestPhaseB_E2E_CompleteQueryPipeline(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}
	table, err := stor.CreateTable(
		"employees",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert test data
	testData := []struct {
		id     int32
		name   string
		salary float64
		active bool
	}{
		{1, "Alice", 50000.0, true},
		{2, "Bob", 60000.0, false},
		{3, "Charlie", 75000.0, true},
		{4, "David", 45000.0, true},
		{5, "Eve", 80000.0, false},
		{6, "Frank", 90000.0, true},
	}

	for _, row := range testData {
		require.NoError(
			t,
			table.AppendRow(
				[]any{
					row.id,
					row.name,
					row.salary,
					row.active,
				},
			),
		)
	}

	// Create table definition
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	doubleInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_DOUBLE,
	)
	boolInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_BOOLEAN,
	)

	columns := []*catalog.ColumnDef{
		{
			Name:     "id",
			Type:     dukdb.TYPE_INTEGER,
			TypeInfo: intInfo,
		},
		{
			Name:     "name",
			Type:     dukdb.TYPE_VARCHAR,
			TypeInfo: varcharInfo,
		},
		{
			Name:     "salary",
			Type:     dukdb.TYPE_DOUBLE,
			TypeInfo: doubleInfo,
		},
		{
			Name:     "active",
			Type:     dukdb.TYPE_BOOLEAN,
			TypeInfo: boolInfo,
		},
	}
	tableDef := catalog.NewTableDef(
		"employees",
		columns,
	)

	// Build operator pipeline: SELECT name, salary FROM employees WHERE salary > 55000 AND active = true

	// Step 1: Scan
	scanPlan := &planner.PhysicalScan{
		TableName: "employees",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Step 2: Filter (salary > 55000 AND active = true)
	salaryFilter := &binder.BoundBinaryExpr{
		Op: parser.OpGt,
		Left: &binder.BoundColumnRef{
			Column:  "salary",
			ColType: dukdb.TYPE_DOUBLE,
		},
		Right: &binder.BoundLiteral{
			Value:   55000.0,
			ValType: dukdb.TYPE_DOUBLE,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	activeFilter := &binder.BoundBinaryExpr{
		Op: parser.OpEq,
		Left: &binder.BoundColumnRef{
			Column:  "active",
			ColType: dukdb.TYPE_BOOLEAN,
		},
		Right: &binder.BoundLiteral{
			Value:   true,
			ValType: dukdb.TYPE_BOOLEAN,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	combinedFilter := &binder.BoundBinaryExpr{
		Op:      parser.OpAnd,
		Left:    salaryFilter,
		Right:   activeFilter,
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{
			Table:  "employees",
			Column: "id",
			Type:   dukdb.TYPE_INTEGER,
		},
		{
			Table:  "employees",
			Column: "name",
			Type:   dukdb.TYPE_VARCHAR,
		},
		{
			Table:  "employees",
			Column: "salary",
			Type:   dukdb.TYPE_DOUBLE,
		},
		{
			Table:  "employees",
			Column: "active",
			Type:   dukdb.TYPE_BOOLEAN,
		},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(
		scanOp,
		childColumns,
		combinedFilter,
		exec,
		execCtx,
	)

	// Step 3: Project (name, salary)
	projectExprs := []binder.BoundExpr{
		&binder.BoundColumnRef{
			Column:  "name",
			ColType: dukdb.TYPE_VARCHAR,
		},
		&binder.BoundColumnRef{
			Column:  "salary",
			ColType: dukdb.TYPE_DOUBLE,
		},
	}

	projectOp, err := NewPhysicalProjectOperator(
		filterOp,
		childColumns,
		projectExprs,
		exec,
		execCtx,
	)
	require.NoError(t, err)

	// Step 4: Collect all chunks from pipeline
	var chunks []*storage.DataChunk
	for {
		chunk, err := projectOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Step 5: Create ResultSet
	columnNames := []string{"name", "salary"}
	typeInfos := projectOp.GetTypes()
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Step 6: Verify ResultSet functionality

	// Test Columns()
	cols := rs.Columns()
	assert.Equal(t, columnNames, cols)

	// Test ColumnTypeDatabaseTypeName()
	assert.Equal(
		t,
		"VARCHAR",
		rs.ColumnTypeDatabaseTypeName(0),
	)
	assert.Equal(
		t,
		"DOUBLE",
		rs.ColumnTypeDatabaseTypeName(1),
	)

	// Test iteration and data retrieval
	dest := make([]driver.Value, 2)

	// Expected results: Charlie (75000, active), Frank (90000, active)
	expectedResults := []struct {
		name   string
		salary float64
	}{
		{"Charlie", 75000.0},
		{"Frank", 90000.0},
	}

	rowCount := 0
	for {
		err := rs.Next(dest)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Verify we're within expected results
		require.Less(
			t,
			rowCount,
			len(expectedResults),
			"More rows than expected",
		)

		// Check if this row matches any expected result (order may vary)
		name, ok := dest[0].(string)
		require.True(
			t,
			ok,
			"dest[0] should be a string",
		)
		salary, ok := dest[1].(float64)
		require.True(
			t,
			ok,
			"dest[1] should be a float64",
		)

		found := false
		for _, expected := range expectedResults {
			if expected.name == name &&
				expected.salary == salary {
				found = true

				break
			}
		}
		assert.True(
			t,
			found,
			"Row (%s, %f) should match an expected result",
			name,
			salary,
		)

		rowCount++
	}

	// Verify we got the expected number of rows
	assert.Equal(
		t,
		len(expectedResults),
		rowCount,
		"Should have correct number of result rows",
	)

	// Test that subsequent calls still return EOF
	err = rs.Next(dest)
	assert.Equal(t, io.EOF, err)

	// Test Close
	err = rs.Close()
	require.NoError(t, err)

	// Verify closed state prevents further iteration
	err = rs.Next(dest)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// TestPhaseB_E2E_EmptyResultSet tests ResultSet behavior with no results.
func TestPhaseB_E2E_EmptyResultSet(t *testing.T) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	_ = cat // catalog used implicitly by executor

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	_, err := stor.CreateTable(
		"empty_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Don't insert any data

	// Create table definition
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)

	columns := []*catalog.ColumnDef{
		{
			Name:     "id",
			Type:     dukdb.TYPE_INTEGER,
			TypeInfo: intInfo,
		},
		{
			Name:     "name",
			Type:     dukdb.TYPE_VARCHAR,
			TypeInfo: varcharInfo,
		},
	}
	tableDef := catalog.NewTableDef(
		"empty_table",
		columns,
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "empty_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Collect chunks (should be empty or contain empty chunks)
	var chunks []*storage.DataChunk
	for {
		chunk, err := scanOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Create ResultSet
	columnNames := []string{"id", "name"}
	typeInfos := scanOp.GetTypes()
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Should immediately return EOF
	dest := make([]driver.Value, 2)
	err = rs.Next(dest)
	assert.Equal(
		t,
		io.EOF,
		err,
		"Empty result set should return EOF immediately",
	)

	// Columns should still work
	assert.Equal(t, columnNames, rs.Columns())

	err = rs.Close()
	require.NoError(t, err)
}

// TestPhaseB_E2E_ResultSetWithFilteredData tests ResultSet when filter
// reduces result set significantly (tests sparse chunk handling).
func TestPhaseB_E2E_ResultSetWithFilteredData(
	t *testing.T,
) {
	// Create storage and catalog
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)
	_ = exec // used for filter operator

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
	}
	table, err := stor.CreateTable(
		"numbers",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert 100 rows
	for i := range 100 {
		require.NoError(
			t,
			table.AppendRow([]any{int32(i)}),
		)
	}

	// Create table definition
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	columns := []*catalog.ColumnDef{
		{
			Name:     "value",
			Type:     dukdb.TYPE_INTEGER,
			TypeInfo: intInfo,
		},
	}
	tableDef := catalog.NewTableDef(
		"numbers",
		columns,
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "numbers",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Filter: value = 42 (only one row should match)
	filterPredicate := &binder.BoundBinaryExpr{
		Op: parser.OpEq,
		Left: &binder.BoundColumnRef{
			Column:  "value",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(42),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{
			Table:  "numbers",
			Column: "value",
			Type:   dukdb.TYPE_INTEGER,
		},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	filterOp := NewPhysicalFilterOperator(
		scanOp,
		childColumns,
		filterPredicate,
		exec,
		execCtx,
	)

	// Collect chunks
	var chunks []*storage.DataChunk
	for {
		chunk, err := filterOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Create ResultSet
	columnNames := []string{"value"}
	typeInfos := filterOp.GetTypes()
	rs := NewResultSet(
		chunks,
		typeInfos,
		columnNames,
	)

	// Should have exactly one row
	dest := make([]driver.Value, 1)
	err = rs.Next(dest)
	require.NoError(t, err)
	assert.Equal(t, int64(42), dest[0])

	// Should be exhausted
	err = rs.Next(dest)
	assert.Equal(t, io.EOF, err)

	err = rs.Close()
	require.NoError(t, err)
}

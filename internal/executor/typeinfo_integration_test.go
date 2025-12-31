package executor

import (
	"context"
	"database/sql/driver"
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

// TestTypeInfoPropagation_ScanToFilter tests that TypeInfo flows correctly
// from storage through scan to filter operators.
func TestTypeInfoPropagation_ScanToFilter(
	t *testing.T,
) {
	// Setup: Create table with various types
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)

	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_BOOLEAN,
	}
	table, err := stor.CreateTable(
		"test_table",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert test data with NULLs
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(1), "Alice", 10.5, true},
		),
	)
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(2), nil, 20.0, false},
		),
	)
	require.NoError(
		t,
		table.AppendRow(
			[]any{int32(3), "Charlie", nil, true},
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
	boolInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_BOOLEAN,
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
		"score",
		dukdb.TYPE_DOUBLE,
	)
	col3.TypeInfo = doubleInfo
	col4 := catalog.NewColumnDef(
		"active",
		dukdb.TYPE_BOOLEAN,
	)
	col4.TypeInfo = boolInfo

	tableDef := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			col1, col2, col3, col4,
		},
	)

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Verify TypeInfo from scan
	scanTypes := scanOp.GetTypes()
	assert.Equal(t, 4, len(scanTypes))
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		scanTypes[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		scanTypes[1].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_DOUBLE,
		scanTypes[2].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_BOOLEAN,
		scanTypes[3].InternalType(),
	)

	// Verify SQLType() works correctly
	assert.Equal(
		t,
		"INTEGER",
		scanTypes[0].SQLType(),
	)
	assert.Equal(
		t,
		"VARCHAR",
		scanTypes[1].SQLType(),
	)
	assert.Equal(
		t,
		"DOUBLE",
		scanTypes[2].SQLType(),
	)
	assert.Equal(
		t,
		"BOOLEAN",
		scanTypes[3].SQLType(),
	)

	// Create filter operator on top of scan
	filterPredicate := &binder.BoundBinaryExpr{
		Op: parser.OpGt,
		Left: &binder.BoundColumnRef{
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(1),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{
			Table:  "test_table",
			Column: "id",
			Type:   dukdb.TYPE_INTEGER,
		},
		{
			Table:  "test_table",
			Column: "name",
			Type:   dukdb.TYPE_VARCHAR,
		},
		{
			Table:  "test_table",
			Column: "score",
			Type:   dukdb.TYPE_DOUBLE,
		},
		{
			Table:  "test_table",
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
		filterPredicate,
		exec,
		execCtx,
	)

	// Verify TypeInfo propagates through filter (should be same as scan)
	filterTypes := filterOp.GetTypes()
	assert.Equal(t, 4, len(filterTypes))
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		filterTypes[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		filterTypes[1].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_DOUBLE,
		filterTypes[2].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_BOOLEAN,
		filterTypes[3].InternalType(),
	)

	// Verify filter produces correct data
	chunk, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(
		t,
		2,
		chunk.Count(),
		"Should filter out row with id=1",
	)

	// No more data
	chunk, err = filterOp.Next()
	require.NoError(t, err)
	assert.Nil(t, chunk)
}

// TestTypeInfoPropagation_ScanToProject tests that TypeInfo flows correctly
// through projection operators.
func TestTypeInfoPropagation_ScanToProject(
	t *testing.T,
) {
	// Setup
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)

	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable(
		"test_table",
		columnTypes,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		table.AppendRow([]any{int32(1), "Alice"}),
	)
	require.NoError(
		t,
		table.AppendRow([]any{int32(2), "Bob"}),
	)

	// Create table definition
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

	// Create scan operator
	scanPlan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Create project operator that produces different types
	childColumns := []planner.ColumnBinding{
		{
			Table:  "test_table",
			Column: "id",
			Type:   dukdb.TYPE_INTEGER,
		},
		{
			Table:  "test_table",
			Column: "name",
			Type:   dukdb.TYPE_VARCHAR,
		},
	}

	projectExprs := []binder.BoundExpr{
		// id + 10 -> INTEGER
		&binder.BoundBinaryExpr{
			Op: parser.OpAdd,
			Left: &binder.BoundColumnRef{
				Column:  "id",
				ColType: dukdb.TYPE_INTEGER,
			},
			Right: &binder.BoundLiteral{
				Value:   int32(10),
				ValType: dukdb.TYPE_INTEGER,
			},
			ResType: dukdb.TYPE_BIGINT, // Arithmetic promotes to larger type
		},
		// UPPER(name) -> VARCHAR
		&binder.BoundFunctionCall{
			Name: "UPPER",
			Args: []binder.BoundExpr{
				&binder.BoundColumnRef{
					Column:  "name",
					ColType: dukdb.TYPE_VARCHAR,
				},
			},
			ResType: dukdb.TYPE_VARCHAR,
		},
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	projectOp, err := NewPhysicalProjectOperator(
		scanOp,
		childColumns,
		projectExprs,
		exec,
		execCtx,
	)
	require.NoError(t, err)

	// Verify TypeInfo from project reflects projection types
	projectTypes := projectOp.GetTypes()
	assert.Equal(t, 2, len(projectTypes))
	assert.Equal(
		t,
		dukdb.TYPE_BIGINT,
		projectTypes[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		projectTypes[1].InternalType(),
	)

	// Verify projected data
	chunk, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, 2, chunk.ColumnCount())

	// Check values
	val0 := chunk.GetValue(0, 0)
	val1 := chunk.GetValue(0, 1)
	assert.NotNil(t, val0)
	assert.Equal(t, "ALICE", val1)
}

// TestExpressionEvaluation_WithDataChunk tests that expression evaluation
// works correctly when extracting row data from DataChunk.
func TestExpressionEvaluation_WithDataChunk(
	t *testing.T,
) {
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)

	// Create table with numeric data
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable(
		"numbers",
		columnTypes,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		table.AppendRow([]any{int32(10), 5.5}),
	)
	require.NoError(
		t,
		table.AppendRow([]any{int32(20), 10.0}),
	)
	require.NoError(
		t,
		table.AppendRow([]any{int32(30), nil}),
	) // NULL value

	tableDef := catalog.NewTableDef(
		"numbers",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"a",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef(
				"b",
				dukdb.TYPE_DOUBLE,
			),
		},
	)

	scanPlan := &planner.PhysicalScan{
		TableName: "numbers",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Test arithmetic expression: a + b
	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER},
		{Column: "b", Type: dukdb.TYPE_DOUBLE},
	}

	addExpr := &binder.BoundBinaryExpr{
		Op: parser.OpAdd,
		Left: &binder.BoundColumnRef{
			Column:  "a",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Column:  "b",
			ColType: dukdb.TYPE_DOUBLE,
		},
		ResType: dukdb.TYPE_DOUBLE,
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	projectOp, err := NewPhysicalProjectOperator(
		scanOp,
		childColumns,
		[]binder.BoundExpr{addExpr},
		exec,
		execCtx,
	)
	require.NoError(t, err)

	// Get results
	chunk, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)
	assert.Equal(t, 3, chunk.Count())

	// Verify arithmetic results
	// Row 0: 10 + 5.5 = 15.5
	val0 := chunk.GetValue(0, 0)
	assert.InDelta(t, 15.5, val0, 0.001)

	// Row 1: 20 + 10.0 = 30.0
	val1 := chunk.GetValue(1, 0)
	assert.InDelta(t, 30.0, val1, 0.001)

	// Row 2: 30 + NULL = NULL (NULL propagation)
	val2 := chunk.GetValue(2, 0)
	assert.Nil(
		t,
		val2,
		"NULL propagation should result in NULL",
	)
}

// TestNullPropagation_ThroughOperators tests NULL handling through
// scan -> filter -> project operator chain.
func TestNullPropagation_ThroughOperators(
	t *testing.T,
) {
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)

	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	table, err := stor.CreateTable(
		"nulls",
		columnTypes,
	)
	require.NoError(t, err)

	// Insert rows with NULLs
	require.NoError(
		t,
		table.AppendRow([]any{int32(1), "Alice"}),
	)
	require.NoError(
		t,
		table.AppendRow([]any{nil, "Bob"}),
	) // NULL id
	require.NoError(
		t,
		table.AppendRow([]any{int32(3), nil}),
	) // NULL name
	require.NoError(
		t,
		table.AppendRow([]any{nil, nil}),
	) // All NULL
	require.NoError(
		t,
		table.AppendRow([]any{int32(5), "Eve"}),
	)

	tableDef := catalog.NewTableDef(
		"nulls",
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

	scanPlan := &planner.PhysicalScan{
		TableName: "nulls",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Filter: id IS NOT NULL
	filterPredicate := &binder.BoundUnaryExpr{
		Op: parser.OpIsNotNull,
		Expr: &binder.BoundColumnRef{
			Column:  "id",
			ColType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	childColumns := []planner.ColumnBinding{
		{
			Table:  "nulls",
			Column: "id",
			Type:   dukdb.TYPE_INTEGER,
		},
		{
			Table:  "nulls",
			Column: "name",
			Type:   dukdb.TYPE_VARCHAR,
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

	// Collect all filtered results
	var chunks []*storage.DataChunk
	for {
		chunk, err := filterOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		chunks = append(chunks, chunk)
	}

	// Count total rows (should exclude rows with NULL id)
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += chunk.Count()
	}
	assert.Equal(
		t,
		3,
		totalRows,
		"Should have 3 rows with non-NULL id",
	)

	// Verify no NULL ids in results
	for _, chunk := range chunks {
		for i := 0; i < chunk.Count(); i++ {
			id := chunk.GetValue(i, 0)
			assert.NotNil(
				t,
				id,
				"Filtered results should not have NULL ids",
			)
		}
	}
}

// TestTypeCoercion_InExpressions tests type coercion during expression evaluation.
func TestTypeCoercion_InExpressions(
	t *testing.T,
) {
	stor := storage.NewStorage()
	cat := catalog.NewCatalog()
	exec := NewExecutor(cat, stor)

	// Create table with mixed types
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_DOUBLE,
	}
	table, err := stor.CreateTable(
		"mixed",
		columnTypes,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		table.AppendRow([]any{int32(10), 2.5}),
	)

	tableDef := catalog.NewTableDef(
		"mixed",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"int_col",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef(
				"dbl_col",
				dukdb.TYPE_DOUBLE,
			),
		},
	)

	scanPlan := &planner.PhysicalScan{
		TableName: "mixed",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	// Test: int_col * dbl_col (should coerce int to double)
	childColumns := []planner.ColumnBinding{
		{
			Column: "int_col",
			Type:   dukdb.TYPE_INTEGER,
		},
		{
			Column: "dbl_col",
			Type:   dukdb.TYPE_DOUBLE,
		},
	}

	mulExpr := &binder.BoundBinaryExpr{
		Op: parser.OpMul,
		Left: &binder.BoundColumnRef{
			Column:  "int_col",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundColumnRef{
			Column:  "dbl_col",
			ColType: dukdb.TYPE_DOUBLE,
		},
		ResType: dukdb.TYPE_DOUBLE,
	}

	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args:    nil,
	}
	projectOp, err := NewPhysicalProjectOperator(
		scanOp,
		childColumns,
		[]binder.BoundExpr{mulExpr},
		exec,
		execCtx,
	)
	require.NoError(t, err)

	chunk, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, chunk)

	// Verify result: 10 * 2.5 = 25.0
	result := chunk.GetValue(0, 0)
	assert.InDelta(t, 25.0, result, 0.001)
}

// TestComplexTypeInfo_NestedTypes tests TypeInfo with complex nested types.
func TestComplexTypeInfo_NestedTypes(
	t *testing.T,
) {
	// Test DECIMAL type
	decimalInfo, err := dukdb.NewDecimalInfo(
		10,
		2,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		dukdb.TYPE_DECIMAL,
		decimalInfo.InternalType(),
	)
	assert.Equal(
		t,
		"DECIMAL(10,2)",
		decimalInfo.SQLType(),
	)

	details := decimalInfo.Details()
	require.NotNil(t, details)
	decDetails, ok := details.(*dukdb.DecimalDetails)
	require.True(t, ok)
	assert.Equal(t, uint8(10), decDetails.Width)
	assert.Equal(t, uint8(2), decDetails.Scale)

	// Test LIST type
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	listInfo, err := dukdb.NewListInfo(intInfo)
	require.NoError(t, err)
	assert.Equal(
		t,
		dukdb.TYPE_LIST,
		listInfo.InternalType(),
	)
	assert.Equal(
		t,
		"INTEGER[]",
		listInfo.SQLType(),
	)

	listDetails := listInfo.Details()
	require.NotNil(t, listDetails)
	listDet, ok := listDetails.(*dukdb.ListDetails)
	require.True(t, ok)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		listDet.Child.InternalType(),
	)

	// Test ARRAY type
	arrayInfo, err := dukdb.NewArrayInfo(
		intInfo,
		5,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		dukdb.TYPE_ARRAY,
		arrayInfo.InternalType(),
	)
	assert.Equal(
		t,
		"INTEGER[5]",
		arrayInfo.SQLType(),
	)

	// Test STRUCT type
	entry1, _ := dukdb.NewStructEntry(
		intInfo,
		"id",
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	entry2, _ := dukdb.NewStructEntry(
		varcharInfo,
		"name",
	)
	structInfo, err := dukdb.NewStructInfo(
		entry1,
		entry2,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		dukdb.TYPE_STRUCT,
		structInfo.InternalType(),
	)
	assert.Contains(
		t,
		structInfo.SQLType(),
		"STRUCT",
	)
	assert.Contains(t, structInfo.SQLType(), "id")
	assert.Contains(
		t,
		structInfo.SQLType(),
		"name",
	)
}

// TestTypeInfo_ColumnDefinition tests that TypeInfo is properly stored
// and retrieved from column definitions.
func TestTypeInfo_ColumnDefinition(t *testing.T) {
	// Create column with TypeInfo
	decimalInfo, _ := dukdb.NewDecimalInfo(18, 4)
	col := catalog.NewColumnDef(
		"price",
		dukdb.TYPE_DECIMAL,
	)
	col.TypeInfo = decimalInfo

	// Verify GetTypeInfo returns the set TypeInfo
	retrievedInfo := col.GetTypeInfo()
	require.NotNil(t, retrievedInfo)
	assert.Equal(
		t,
		dukdb.TYPE_DECIMAL,
		retrievedInfo.InternalType(),
	)
	assert.Equal(
		t,
		"DECIMAL(18,4)",
		retrievedInfo.SQLType(),
	)

	// Test column without explicit TypeInfo (should auto-create)
	col2 := catalog.NewColumnDef(
		"count",
		dukdb.TYPE_INTEGER,
	)
	retrievedInfo2 := col2.GetTypeInfo()
	require.NotNil(t, retrievedInfo2)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		retrievedInfo2.InternalType(),
	)
	assert.Equal(
		t,
		"INTEGER",
		retrievedInfo2.SQLType(),
	)
}

// TestTypeInfo_TableDefinition tests that TypeInfo flows from table
// definition to scan operators.
func TestTypeInfo_TableDefinition(t *testing.T) {
	stor := storage.NewStorage()

	// Create table
	columnTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	_, err := stor.CreateTable(
		"types_test",
		columnTypes,
	)
	require.NoError(t, err)

	// Create table definition with TypeInfo
	intInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharInfo, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
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

	tableDef := catalog.NewTableDef(
		"types_test",
		[]*catalog.ColumnDef{col1, col2},
	)

	// Get TypeInfos from table definition
	typeInfos := tableDef.ColumnTypeInfos()
	assert.Equal(t, 2, len(typeInfos))
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		typeInfos[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		typeInfos[1].InternalType(),
	)

	// Create scan and verify TypeInfo propagates
	scanPlan := &planner.PhysicalScan{
		TableName: "types_test",
		TableDef:  tableDef,
	}
	scanOp, err := NewPhysicalScanOperator(
		scanPlan,
		stor,
	)
	require.NoError(t, err)

	scanTypes := scanOp.GetTypes()
	assert.Equal(t, 2, len(scanTypes))
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		scanTypes[0].InternalType(),
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		scanTypes[1].InternalType(),
	)
}

// TestExpressionEvaluation_NullHandling tests NULL handling in various
// expression types.
func TestExpressionEvaluation_NullHandling(
	t *testing.T,
) {
	exec := NewExecutor(nil, nil)
	execCtx := &ExecutionContext{
		Context: context.Background(),
		Args: []driver.NamedValue{
			{Ordinal: 1, Value: int32(42)},
		},
	}

	tests := []struct {
		name     string
		expr     binder.BoundExpr
		row      map[string]any
		expected any
	}{
		{
			name: "NULL + 5 = NULL",
			expr: &binder.BoundBinaryExpr{
				Op: parser.OpAdd,
				Left: &binder.BoundColumnRef{
					Column:  "a",
					ColType: dukdb.TYPE_INTEGER,
				},
				Right: &binder.BoundLiteral{
					Value:   int32(5),
					ValType: dukdb.TYPE_INTEGER,
				},
				ResType: dukdb.TYPE_INTEGER,
			},
			row:      map[string]any{"a": nil},
			expected: nil,
		},
		{
			name: "NULL AND TRUE = NULL",
			expr: &binder.BoundBinaryExpr{
				Op: parser.OpAnd,
				Left: &binder.BoundColumnRef{
					Column:  "a",
					ColType: dukdb.TYPE_BOOLEAN,
				},
				Right: &binder.BoundLiteral{
					Value:   true,
					ValType: dukdb.TYPE_BOOLEAN,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			},
			row:      map[string]any{"a": nil},
			expected: nil,
		},
		{
			name: "NULL AND FALSE = FALSE",
			expr: &binder.BoundBinaryExpr{
				Op: parser.OpAnd,
				Left: &binder.BoundColumnRef{
					Column:  "a",
					ColType: dukdb.TYPE_BOOLEAN,
				},
				Right: &binder.BoundLiteral{
					Value:   false,
					ValType: dukdb.TYPE_BOOLEAN,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			},
			row:      map[string]any{"a": nil},
			expected: false,
		},
		{
			name: "NULL OR TRUE = TRUE",
			expr: &binder.BoundBinaryExpr{
				Op: parser.OpOr,
				Left: &binder.BoundColumnRef{
					Column:  "a",
					ColType: dukdb.TYPE_BOOLEAN,
				},
				Right: &binder.BoundLiteral{
					Value:   true,
					ValType: dukdb.TYPE_BOOLEAN,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			},
			row:      map[string]any{"a": nil},
			expected: true,
		},
		{
			name: "NULL IS NULL = TRUE",
			expr: &binder.BoundUnaryExpr{
				Op: parser.OpIsNull,
				Expr: &binder.BoundColumnRef{
					Column:  "a",
					ColType: dukdb.TYPE_INTEGER,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			},
			row:      map[string]any{"a": nil},
			expected: true,
		},
		{
			name: "5 IS NOT NULL = TRUE",
			expr: &binder.BoundUnaryExpr{
				Op: parser.OpIsNotNull,
				Expr: &binder.BoundColumnRef{
					Column:  "a",
					ColType: dukdb.TYPE_INTEGER,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			},
			row: map[string]any{
				"a": int32(5),
			},
			expected: true,
		},
		{
			name: "COALESCE(NULL, NULL, 42) = 42",
			expr: &binder.BoundFunctionCall{
				Name: "COALESCE",
				Args: []binder.BoundExpr{
					&binder.BoundColumnRef{
						Column:  "a",
						ColType: dukdb.TYPE_INTEGER,
					},
					&binder.BoundColumnRef{
						Column:  "b",
						ColType: dukdb.TYPE_INTEGER,
					},
					&binder.BoundLiteral{
						Value:   int32(42),
						ValType: dukdb.TYPE_INTEGER,
					},
				},
				ResType: dukdb.TYPE_INTEGER,
			},
			row: map[string]any{
				"a": nil,
				"b": nil,
			},
			expected: int32(42),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exec.evaluateExpr(
				execCtx,
				tt.expr,
				tt.row,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

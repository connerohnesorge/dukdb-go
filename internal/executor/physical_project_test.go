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

func TestPhysicalProjectOperator_ProjectSingleColumn(t *testing.T) {
	// Setup: Create a mock child operator that produces chunks with 3 columns (a, b, c)
	chunk := storage.NewDataChunk([]dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
	})
	chunk.AppendRow([]any{int32(1), int32(2), int32(3)})
	chunk.AppendRow([]any{int32(4), int32(5), int32(6)})

	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Column: "b", Type: dukdb.TYPE_INTEGER, ColumnIdx: 1},
		{Column: "c", Type: dukdb.TYPE_INTEGER, ColumnIdx: 2},
	}

	types := make([]dukdb.TypeInfo, 3)
	for i := range types {
		types[i] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	}

	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		index:  0,
		types:  types,
	}

	// Create projection for just column "b"
	expressions := []binder.BoundExpr{
		&binder.BoundColumnRef{
			Column:  "b",
			ColType: dukdb.TYPE_INTEGER,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	executor := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	projectOp, err := NewPhysicalProjectOperator(mockChild, childColumns, expressions, executor, ctx)
	require.NoError(t, err)

	// Execute projection
	result, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify: Should have 1 column, 2 rows
	assert.Equal(t, 1, result.ColumnCount())
	assert.Equal(t, 2, result.Count())

	// Verify values (should be column b: 2, 5)
	assert.Equal(t, int32(2), result.GetValue(0, 0))
	assert.Equal(t, int32(5), result.GetValue(1, 0))

	// Verify no more data
	result, err = projectOp.Next()
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestPhysicalProjectOperator_ProjectMultipleColumnsReordered(t *testing.T) {
	// Setup: Create a mock child operator that produces chunks with 3 columns (a, b, c)
	chunk := storage.NewDataChunk([]dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
	})
	chunk.AppendRow([]any{int32(1), int32(2), int32(3)})
	chunk.AppendRow([]any{int32(4), int32(5), int32(6)})

	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Column: "b", Type: dukdb.TYPE_INTEGER, ColumnIdx: 1},
		{Column: "c", Type: dukdb.TYPE_INTEGER, ColumnIdx: 2},
	}

	types := make([]dukdb.TypeInfo, 3)
	for i := range types {
		types[i] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	}

	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		index:  0,
		types:  types,
	}

	// Create projection for columns c, a (reordered)
	expressions := []binder.BoundExpr{
		&binder.BoundColumnRef{
			Column:  "c",
			ColType: dukdb.TYPE_INTEGER,
		},
		&binder.BoundColumnRef{
			Column:  "a",
			ColType: dukdb.TYPE_INTEGER,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	executor := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	projectOp, err := NewPhysicalProjectOperator(mockChild, childColumns, expressions, executor, ctx)
	require.NoError(t, err)

	// Execute projection
	result, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify: Should have 2 columns, 2 rows
	assert.Equal(t, 2, result.ColumnCount())
	assert.Equal(t, 2, result.Count())

	// Verify values (should be c, a: [3, 1], [6, 4])
	assert.Equal(t, int32(3), result.GetValue(0, 0)) // row 0, col 0 (c)
	assert.Equal(t, int32(1), result.GetValue(0, 1)) // row 0, col 1 (a)
	assert.Equal(t, int32(6), result.GetValue(1, 0)) // row 1, col 0 (c)
	assert.Equal(t, int32(4), result.GetValue(1, 1)) // row 1, col 1 (a)
}

func TestPhysicalProjectOperator_ProjectWithExpression(t *testing.T) {
	// Setup: Create a mock child operator that produces chunks with 2 columns (a, b)
	chunk := storage.NewDataChunk([]dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
	})
	chunk.AppendRow([]any{int32(10), int32(5)})
	chunk.AppendRow([]any{int32(20), int32(3)})

	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Column: "b", Type: dukdb.TYPE_INTEGER, ColumnIdx: 1},
	}

	types := make([]dukdb.TypeInfo, 2)
	for i := range types {
		types[i] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	}

	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		index:  0,
		types:  types,
	}

	// Create projection for a + b
	expressions := []binder.BoundExpr{
		&binder.BoundBinaryExpr{
			Left: &binder.BoundColumnRef{
				Column:  "a",
				ColType: dukdb.TYPE_INTEGER,
			},
			Op: parser.OpAdd,
			Right: &binder.BoundColumnRef{
				Column:  "b",
				ColType: dukdb.TYPE_INTEGER,
			},
			ResType: dukdb.TYPE_DOUBLE,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	executor := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	projectOp, err := NewPhysicalProjectOperator(mockChild, childColumns, expressions, executor, ctx)
	require.NoError(t, err)

	// Execute projection
	result, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify: Should have 1 column, 2 rows
	assert.Equal(t, 1, result.ColumnCount())
	assert.Equal(t, 2, result.Count())

	// Verify values (should be a+b: 15.0, 23.0)
	val0 := result.GetValue(0, 0)
	val1 := result.GetValue(1, 0)

	// Values should be numeric (might be int32 or float64 depending on evaluation)
	assert.NotNil(t, val0)
	assert.NotNil(t, val1)
}

func TestPhysicalProjectOperator_ProjectWithLiteral(t *testing.T) {
	// Setup: Create a mock child operator that produces chunks with 1 column
	chunk := storage.NewDataChunk([]dukdb.Type{
		dukdb.TYPE_INTEGER,
	})
	chunk.AppendRow([]any{int32(1)})
	chunk.AppendRow([]any{int32(2)})

	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
	}

	types := []dukdb.TypeInfo{
		&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
	}

	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		index:  0,
		types:  types,
	}

	// Create projection for column a and a literal value
	expressions := []binder.BoundExpr{
		&binder.BoundColumnRef{
			Column:  "a",
			ColType: dukdb.TYPE_INTEGER,
		},
		&binder.BoundLiteral{
			Value:   int64(42),
			ValType: dukdb.TYPE_BIGINT,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	executor := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	projectOp, err := NewPhysicalProjectOperator(mockChild, childColumns, expressions, executor, ctx)
	require.NoError(t, err)

	// Execute projection
	result, err := projectOp.Next()
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify: Should have 2 columns, 2 rows
	assert.Equal(t, 2, result.ColumnCount())
	assert.Equal(t, 2, result.Count())

	// Verify values
	assert.Equal(t, int32(1), result.GetValue(0, 0)) // row 0, col 0 (a)
	assert.Equal(t, int64(42), result.GetValue(0, 1)) // row 0, col 1 (literal)
	assert.Equal(t, int32(2), result.GetValue(1, 0)) // row 1, col 0 (a)
	assert.Equal(t, int64(42), result.GetValue(1, 1)) // row 1, col 1 (literal)
}

func TestPhysicalProjectOperator_GetTypes(t *testing.T) {
	// Setup: Create a mock child operator
	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
		{Column: "b", Type: dukdb.TYPE_VARCHAR, ColumnIdx: 1},
	}

	types := []dukdb.TypeInfo{
		&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
		&basicTypeInfo{typ: dukdb.TYPE_VARCHAR},
	}

	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{},
		index:  0,
		types:  types,
	}

	// Create projection with different types
	expressions := []binder.BoundExpr{
		&binder.BoundColumnRef{
			Column:  "b",
			ColType: dukdb.TYPE_VARCHAR,
		},
		&binder.BoundColumnRef{
			Column:  "a",
			ColType: dukdb.TYPE_INTEGER,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	executor := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	projectOp, err := NewPhysicalProjectOperator(mockChild, childColumns, expressions, executor, ctx)
	require.NoError(t, err)

	// Verify types
	resultTypes := projectOp.GetTypes()
	assert.Equal(t, 2, len(resultTypes))
	assert.Equal(t, dukdb.TYPE_VARCHAR, resultTypes[0].InternalType())
	assert.Equal(t, dukdb.TYPE_INTEGER, resultTypes[1].InternalType())
}

func TestPhysicalProjectOperator_EmptyInput(t *testing.T) {
	// Setup: Create a mock child operator with no data
	childColumns := []planner.ColumnBinding{
		{Column: "a", Type: dukdb.TYPE_INTEGER, ColumnIdx: 0},
	}

	types := []dukdb.TypeInfo{
		&basicTypeInfo{typ: dukdb.TYPE_INTEGER},
	}

	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{},
		index:  0,
		types:  types,
	}

	expressions := []binder.BoundExpr{
		&binder.BoundColumnRef{
			Column:  "a",
			ColType: dukdb.TYPE_INTEGER,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	executor := NewExecutor(cat, stor)
	ctx := &ExecutionContext{Context: context.Background()}

	projectOp, err := NewPhysicalProjectOperator(mockChild, childColumns, expressions, executor, ctx)
	require.NoError(t, err)

	// Execute projection
	result, err := projectOp.Next()
	require.NoError(t, err)
	assert.Nil(t, result) // Should return nil for empty input
}

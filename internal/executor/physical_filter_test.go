package executor

import (
	"context"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPhysicalOperator is a simple mock operator that returns predefined chunks.
type mockPhysicalOperator struct {
	chunks []*storage.DataChunk
	index  int
	types  []dukdb.TypeInfo
}

func (m *mockPhysicalOperator) Next() (*storage.DataChunk, error) {
	if m.index >= len(m.chunks) {
		return nil, nil
	}
	chunk := m.chunks[m.index]
	m.index++

	return chunk, nil
}

func (m *mockPhysicalOperator) GetTypes() []dukdb.TypeInfo {
	return m.types
}

func TestPhysicalFilter_BasicFilter(t *testing.T) {
	// Create a simple table with integers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	for i := 1; i <= 10; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	// Create mock child operator
	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// Create a filter predicate: x > 5
	// For testing, we'll use a BoundBinaryExpr
	predicate := &binder.BoundBinaryExpr{
		Op: 9, // OpGt (greater than)
		Left: &binder.BoundColumnRef{
			Column:  "0", // Using index as column name for simplicity
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(5),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	// Create executor
	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	// Create filter operator
	filterOp := NewPhysicalFilterOperator(mockChild, nil, predicate, exec, ctx)

	// Get filtered results
	resultChunk, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// Should have 5 rows (6, 7, 8, 9, 10)
	assert.Equal(t, 5, resultChunk.Count())

	// Verify the values
	for i := 0; i < resultChunk.Count(); i++ {
		val := resultChunk.GetValue(i, 0)
		expected := int32(6 + i)
		assert.Equal(t, expected, val, "Row %d should have value %d", i, expected)
	}

	// Next call should return nil (no more data)
	nextChunk, err := filterOp.Next()
	require.NoError(t, err)
	assert.Nil(t, nextChunk)
}

func TestPhysicalFilter_WithNulls(t *testing.T) {
	// Create a table with some NULL values: [1, NULL, 3, NULL, 5]
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{int32(1)})
	chunk.AppendRow([]any{nil})
	chunk.AppendRow([]any{int32(3)})
	chunk.AppendRow([]any{nil})
	chunk.AppendRow([]any{int32(5)})

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// Filter: x > 2
	predicate := &binder.BoundBinaryExpr{
		Op: 9, // OpGt
		Left: &binder.BoundColumnRef{
			Column:  "0",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(2),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	filterOp := NewPhysicalFilterOperator(mockChild, nil, predicate, exec, ctx)

	resultChunk, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// Should have 2 rows (3, 5) - NULLs are filtered out
	assert.Equal(t, 2, resultChunk.Count())
	assert.Equal(t, int32(3), resultChunk.GetValue(0, 0))
	assert.Equal(t, int32(5), resultChunk.GetValue(1, 0))
}

func TestPhysicalFilter_AllRowsMatch(t *testing.T) {
	// Create table: [10, 20, 30, 40, 50]
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	for i := 1; i <= 5; i++ {
		chunk.AppendRow([]any{int32(i * 10)})
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// Filter: x > 0 (all rows should match)
	predicate := &binder.BoundBinaryExpr{
		Op: 9, // OpGt
		Left: &binder.BoundColumnRef{
			Column:  "0",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(0),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	filterOp := NewPhysicalFilterOperator(mockChild, nil, predicate, exec, ctx)

	resultChunk, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// All 5 rows should match
	assert.Equal(t, 5, resultChunk.Count())
	for i := 0; i < resultChunk.Count(); i++ {
		expected := int32((i + 1) * 10)
		assert.Equal(t, expected, resultChunk.GetValue(i, 0))
	}
}

func TestPhysicalFilter_NoRowsMatch(t *testing.T) {
	// Create table: [1, 2, 3, 4, 5]
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	for i := 1; i <= 5; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// Filter: x > 100 (no rows should match)
	predicate := &binder.BoundBinaryExpr{
		Op: 9, // OpGt
		Left: &binder.BoundColumnRef{
			Column:  "0",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(100),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	filterOp := NewPhysicalFilterOperator(mockChild, nil, predicate, exec, ctx)

	// Should return nil (no matching rows in any chunk)
	resultChunk, err := filterOp.Next()
	require.NoError(t, err)
	assert.Nil(t, resultChunk)
}

func TestPhysicalFilter_MultipleChunks(t *testing.T) {
	// Create two chunks
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	chunk1 := storage.NewDataChunk(types)
	for i := 1; i <= 5; i++ {
		chunk1.AppendRow([]any{int32(i)})
	}

	chunk2 := storage.NewDataChunk(types)
	for i := 6; i <= 10; i++ {
		chunk2.AppendRow([]any{int32(i)})
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk1, chunk2},
		types:  typeInfos,
	}

	// Filter: x > 3 (should match 4,5 from chunk1 and 6,7,8,9,10 from chunk2)
	predicate := &binder.BoundBinaryExpr{
		Op: 9, // OpGt
		Left: &binder.BoundColumnRef{
			Column:  "0",
			ColType: dukdb.TYPE_INTEGER,
		},
		Right: &binder.BoundLiteral{
			Value:   int32(3),
			ValType: dukdb.TYPE_INTEGER,
		},
		ResType: dukdb.TYPE_BOOLEAN,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	filterOp := NewPhysicalFilterOperator(mockChild, nil, predicate, exec, ctx)

	// Get first result chunk (from chunk1)
	resultChunk1, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk1)
	assert.Equal(t, 2, resultChunk1.Count()) // 4, 5

	// Get second result chunk (from chunk2)
	resultChunk2, err := filterOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk2)
	assert.Equal(t, 5, resultChunk2.Count()) // 6, 7, 8, 9, 10

	// No more chunks
	resultChunk3, err := filterOp.Next()
	require.NoError(t, err)
	assert.Nil(t, resultChunk3)
}

func TestPhysicalFilter_GetTypes(t *testing.T) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunk(types)

	typeInfos := make([]dukdb.TypeInfo, 2)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	typeInfos[1] = &basicTypeInfo{typ: dukdb.TYPE_VARCHAR}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// Dummy predicate
	predicate := &binder.BoundLiteral{
		Value:   true,
		ValType: dukdb.TYPE_BOOLEAN,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	filterOp := NewPhysicalFilterOperator(mockChild, nil, predicate, exec, ctx)

	// GetTypes should return same types as child
	resultTypes := filterOp.GetTypes()
	require.Len(t, resultTypes, 2)
	assert.Equal(t, dukdb.TYPE_INTEGER, resultTypes[0].InternalType())
	assert.Equal(t, dukdb.TYPE_VARCHAR, resultTypes[1].InternalType())
}

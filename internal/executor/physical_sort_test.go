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

func TestPhysicalSort_SingleColumnAsc(
	t *testing.T,
) {
	// Test sorting a single integer column in ascending order
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	// Add values out of order: 5, 2, 8, 1, 3
	chunk.AppendRow([]any{int32(5)})
	chunk.AppendRow([]any{int32(2)})
	chunk.AppendRow([]any{int32(8)})
	chunk.AppendRow([]any{int32(1)})
	chunk.AppendRow([]any{int32(3)})

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{
		typ: dukdb.TYPE_INTEGER,
	}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// ORDER BY column 0 ASC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
			Desc: false,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	sortOp, err := NewPhysicalSortOperator(
		mockChild,
		nil,
		orderBy,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Collect all results
	var allValues []int32
	for {
		resultChunk, err := sortOp.Next()
		require.NoError(t, err)
		if resultChunk == nil {
			break
		}
		for i := 0; i < resultChunk.Count(); i++ {
			allValues = append(
				allValues,
				resultChunk.GetValue(i, 0).(int32),
			)
		}
	}

	// Should be sorted: 1, 2, 3, 5, 8
	expected := []int32{1, 2, 3, 5, 8}
	assert.Equal(t, expected, allValues)
}

func TestPhysicalSort_SingleColumnDesc(
	t *testing.T,
) {
	// Test sorting a single integer column in descending order
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	// Add values out of order: 5, 2, 8, 1, 3
	chunk.AppendRow([]any{int32(5)})
	chunk.AppendRow([]any{int32(2)})
	chunk.AppendRow([]any{int32(8)})
	chunk.AppendRow([]any{int32(1)})
	chunk.AppendRow([]any{int32(3)})

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{
		typ: dukdb.TYPE_INTEGER,
	}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// ORDER BY column 0 DESC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
			Desc: true,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	sortOp, err := NewPhysicalSortOperator(
		mockChild,
		nil,
		orderBy,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Collect all results
	var allValues []int32
	for {
		resultChunk, err := sortOp.Next()
		require.NoError(t, err)
		if resultChunk == nil {
			break
		}
		for i := 0; i < resultChunk.Count(); i++ {
			allValues = append(
				allValues,
				resultChunk.GetValue(i, 0).(int32),
			)
		}
	}

	// Should be sorted descending: 8, 5, 3, 2, 1
	expected := []int32{8, 5, 3, 2, 1}
	assert.Equal(t, expected, allValues)
}

func TestPhysicalSort_MultipleColumns(
	t *testing.T,
) {
	// Test sorting by multiple columns: ORDER BY a DESC, b ASC
	types := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	}
	chunk := storage.NewDataChunk(types)
	// Add rows: (a, b)
	chunk.AppendRow([]any{int32(2), "beta"})
	chunk.AppendRow([]any{int32(1), "gamma"})
	chunk.AppendRow([]any{int32(2), "alpha"})
	chunk.AppendRow([]any{int32(3), "delta"})
	chunk.AppendRow([]any{int32(1), "alpha"})

	typeInfos := make([]dukdb.TypeInfo, 2)
	typeInfos[0] = &basicTypeInfo{
		typ: dukdb.TYPE_INTEGER,
	}
	typeInfos[1] = &basicTypeInfo{
		typ: dukdb.TYPE_VARCHAR,
	}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// ORDER BY column 0 DESC, column 1 ASC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
			Desc: true,
		},
		{
			Expr: &binder.BoundColumnRef{
				Column:  "1",
				ColType: dukdb.TYPE_VARCHAR,
			},
			Desc: false,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	sortOp, err := NewPhysicalSortOperator(
		mockChild,
		nil,
		orderBy,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Collect all results
	type row struct {
		a int32
		b string
	}
	var allRows []row
	for {
		resultChunk, err := sortOp.Next()
		require.NoError(t, err)
		if resultChunk == nil {
			break
		}
		for i := 0; i < resultChunk.Count(); i++ {
			allRows = append(allRows, row{
				a: resultChunk.GetValue(i, 0).(int32),
				b: resultChunk.GetValue(i, 1).(string),
			})
		}
	}

	// Expected order: (3, delta), (2, alpha), (2, beta), (1, alpha), (1, gamma)
	// a DESC (3 before 2 before 1), then b ASC within each group
	expected := []row{
		{3, "delta"},
		{2, "alpha"},
		{2, "beta"},
		{1, "alpha"},
		{1, "gamma"},
	}
	assert.Equal(t, expected, allRows)
}

func TestPhysicalSort_WithNulls(t *testing.T) {
	// Test sorting with NULL values
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	// Add values including NULLs: 5, NULL, 2, NULL, 1
	chunk.AppendRow([]any{int32(5)})
	chunk.AppendRow([]any{nil})
	chunk.AppendRow([]any{int32(2)})
	chunk.AppendRow([]any{nil})
	chunk.AppendRow([]any{int32(1)})

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{
		typ: dukdb.TYPE_INTEGER,
	}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// ORDER BY column 0 ASC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
			Desc: false,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	sortOp, err := NewPhysicalSortOperator(
		mockChild,
		nil,
		orderBy,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Collect all results
	var allValues []any
	for {
		resultChunk, err := sortOp.Next()
		require.NoError(t, err)
		if resultChunk == nil {
			break
		}
		for i := 0; i < resultChunk.Count(); i++ {
			allValues = append(
				allValues,
				resultChunk.GetValue(i, 0),
			)
		}
	}

	// NULLs should come last for ASC (matching PostgreSQL/DuckDB default behavior)
	// Expected: 1, 2, 5, NULL, NULL
	assert.Equal(t, 5, len(allValues))
	assert.Equal(t, int32(1), allValues[0])
	assert.Equal(t, int32(2), allValues[1])
	assert.Equal(t, int32(5), allValues[2])
	assert.Nil(t, allValues[3])
	assert.Nil(t, allValues[4])
}

func TestPhysicalSort_EmptyInput(t *testing.T) {
	// Test sorting with empty input
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	// No rows added

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{
		typ: dukdb.TYPE_INTEGER,
	}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// ORDER BY column 0 ASC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
			Desc: false,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	sortOp, err := NewPhysicalSortOperator(
		mockChild,
		nil,
		orderBy,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Should return nil immediately
	resultChunk, err := sortOp.Next()
	require.NoError(t, err)
	assert.Nil(t, resultChunk)
}

func TestPhysicalSort_StringComparison(
	t *testing.T,
) {
	// Test sorting strings
	types := []dukdb.Type{dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{"zebra"})
	chunk.AppendRow([]any{"apple"})
	chunk.AppendRow([]any{"mango"})
	chunk.AppendRow([]any{"banana"})

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{
		typ: dukdb.TYPE_VARCHAR,
	}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// ORDER BY column 0 ASC
	orderBy := []*binder.BoundOrderBy{
		{
			Expr: &binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_VARCHAR,
			},
			Desc: false,
		},
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	sortOp, err := NewPhysicalSortOperator(
		mockChild,
		nil,
		orderBy,
		exec,
		ctx,
	)
	require.NoError(t, err)

	// Collect all results
	var allValues []string
	for {
		resultChunk, err := sortOp.Next()
		require.NoError(t, err)
		if resultChunk == nil {
			break
		}
		for i := 0; i < resultChunk.Count(); i++ {
			allValues = append(
				allValues,
				resultChunk.GetValue(i, 0).(string),
			)
		}
	}

	// Should be sorted alphabetically: apple, banana, mango, zebra
	expected := []string{
		"apple",
		"banana",
		"mango",
		"zebra",
	}
	assert.Equal(t, expected, allValues)
}

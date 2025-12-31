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

func TestPhysicalAggregate_SumNoGroupBy(t *testing.T) {
	// SELECT SUM(x) FROM table with x = [1, 2, 3, 4, 5]
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

	// SUM(x) where x is column 0
	sumExpr := &binder.BoundFunctionCall{
		Name: "SUM",
		Args: []binder.BoundExpr{
			&binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
		},
		ResType: dukdb.TYPE_DOUBLE,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{}, // No GROUP BY
		[]binder.BoundExpr{sumExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// Should have 1 row with SUM = 15.0
	assert.Equal(t, 1, resultChunk.Count())
	assert.Equal(t, 1, resultChunk.ColumnCount())
	sum := resultChunk.GetValue(0, 0)
	assert.Equal(t, float64(15), sum)

	// Next call should return nil
	nextChunk, err := aggOp.Next()
	require.NoError(t, err)
	assert.Nil(t, nextChunk)
}

func TestPhysicalAggregate_CountStar(t *testing.T) {
	// SELECT COUNT(*) FROM table with 10 rows
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	for i := 1; i <= 10; i++ {
		chunk.AppendRow([]any{int32(i)})
	}

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// COUNT(*)
	countExpr := &binder.BoundFunctionCall{
		Name:    "COUNT",
		Star:    true,
		Args:    []binder.BoundExpr{},
		ResType: dukdb.TYPE_BIGINT,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{},
		[]binder.BoundExpr{countExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	assert.Equal(t, 1, resultChunk.Count())
	count := resultChunk.GetValue(0, 0)
	assert.Equal(t, int64(10), count)
}

func TestPhysicalAggregate_CountWithNulls(t *testing.T) {
	// SELECT COUNT(x) FROM table with x = [1, NULL, 3, NULL, 5]
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

	// COUNT(x)
	countExpr := &binder.BoundFunctionCall{
		Name: "COUNT",
		Args: []binder.BoundExpr{
			&binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
		},
		ResType: dukdb.TYPE_BIGINT,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{},
		[]binder.BoundExpr{countExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// COUNT should be 3 (excludes NULLs)
	assert.Equal(t, 1, resultChunk.Count())
	count := resultChunk.GetValue(0, 0)
	assert.Equal(t, int64(3), count)
}

func TestPhysicalAggregate_AvgMinMax(t *testing.T) {
	// SELECT AVG(x), MIN(x), MAX(x) FROM table with x = [10, 20, 30, 40, 50]
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

	colRef := &binder.BoundColumnRef{
		Column:  "0",
		ColType: dukdb.TYPE_INTEGER,
	}

	avgExpr := &binder.BoundFunctionCall{
		Name:    "AVG",
		Args:    []binder.BoundExpr{colRef},
		ResType: dukdb.TYPE_DOUBLE,
	}
	minExpr := &binder.BoundFunctionCall{
		Name:    "MIN",
		Args:    []binder.BoundExpr{colRef},
		ResType: dukdb.TYPE_INTEGER,
	}
	maxExpr := &binder.BoundFunctionCall{
		Name:    "MAX",
		Args:    []binder.BoundExpr{colRef},
		ResType: dukdb.TYPE_INTEGER,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{},
		[]binder.BoundExpr{avgExpr, minExpr, maxExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	assert.Equal(t, 1, resultChunk.Count())
	assert.Equal(t, 3, resultChunk.ColumnCount())

	// AVG = 30.0
	avg := resultChunk.GetValue(0, 0)
	assert.Equal(t, float64(30), avg)

	// MIN = 10
	min := resultChunk.GetValue(0, 1)
	assert.Equal(t, int32(10), min)

	// MAX = 50
	max := resultChunk.GetValue(0, 2)
	assert.Equal(t, int32(50), max)
}

func TestPhysicalAggregate_EmptyInput(t *testing.T) {
	// SELECT COUNT(*) FROM empty table
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	// No rows appended

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	countExpr := &binder.BoundFunctionCall{
		Name:    "COUNT",
		Star:    true,
		ResType: dukdb.TYPE_BIGINT,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{},
		[]binder.BoundExpr{countExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// Should still return 1 row with COUNT = 0
	assert.Equal(t, 1, resultChunk.Count())
	count := resultChunk.GetValue(0, 0)
	assert.Equal(t, int64(0), count)
}

func TestPhysicalAggregate_GroupBySingleColumn(t *testing.T) {
	// SELECT category, COUNT(*) FROM table GROUP BY category
	// Input: [(A, 1), (B, 2), (A, 3), (B, 4), (A, 5)]
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{"A", int32(1)})
	chunk.AppendRow([]any{"B", int32(2)})
	chunk.AppendRow([]any{"A", int32(3)})
	chunk.AppendRow([]any{"B", int32(4)})
	chunk.AppendRow([]any{"A", int32(5)})

	typeInfos := make([]dukdb.TypeInfo, 2)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_VARCHAR}
	typeInfos[1] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// GROUP BY column 0 (category)
	groupByExpr := &binder.BoundColumnRef{
		Column:  "0",
		ColType: dukdb.TYPE_VARCHAR,
	}

	// COUNT(*)
	countExpr := &binder.BoundFunctionCall{
		Name:    "COUNT",
		Star:    true,
		ResType: dukdb.TYPE_BIGINT,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{groupByExpr},
		[]binder.BoundExpr{countExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// Should have 2 groups
	assert.Equal(t, 2, resultChunk.Count())
	assert.Equal(t, 2, resultChunk.ColumnCount())

	// Check groups (order may vary)
	groupA := -1
	groupB := -1
	for i := 0; i < resultChunk.Count(); i++ {
		category := resultChunk.GetValue(i, 0)
		switch category {
		case "A":
			groupA = i
		case "B":
			groupB = i
		}
	}

	require.NotEqual(t, -1, groupA, "Group A not found")
	require.NotEqual(t, -1, groupB, "Group B not found")

	// Group A should have count 3
	assert.Equal(t, int64(3), resultChunk.GetValue(groupA, 1))

	// Group B should have count 2
	assert.Equal(t, int64(2), resultChunk.GetValue(groupB, 1))
}

func TestPhysicalAggregate_GroupByWithSum(t *testing.T) {
	// SELECT category, SUM(value) FROM table GROUP BY category
	// Input: [(A, 10), (B, 20), (A, 30), (B, 40)]
	types := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)
	chunk.AppendRow([]any{"A", int32(10)})
	chunk.AppendRow([]any{"B", int32(20)})
	chunk.AppendRow([]any{"A", int32(30)})
	chunk.AppendRow([]any{"B", int32(40)})

	typeInfos := make([]dukdb.TypeInfo, 2)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_VARCHAR}
	typeInfos[1] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// GROUP BY column 0
	groupByExpr := &binder.BoundColumnRef{
		Column:  "0",
		ColType: dukdb.TYPE_VARCHAR,
	}

	// SUM(column 1)
	sumExpr := &binder.BoundFunctionCall{
		Name: "SUM",
		Args: []binder.BoundExpr{
			&binder.BoundColumnRef{
				Column:  "1",
				ColType: dukdb.TYPE_INTEGER,
			},
		},
		ResType: dukdb.TYPE_DOUBLE,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{groupByExpr},
		[]binder.BoundExpr{sumExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	assert.Equal(t, 2, resultChunk.Count())
	assert.Equal(t, 2, resultChunk.ColumnCount())

	// Find groups
	groupA := -1
	groupB := -1
	for i := 0; i < resultChunk.Count(); i++ {
		category := resultChunk.GetValue(i, 0)
		switch category {
		case "A":
			groupA = i
		case "B":
			groupB = i
		}
	}

	require.NotEqual(t, -1, groupA)
	require.NotEqual(t, -1, groupB)

	// Group A SUM = 40.0
	assert.Equal(t, float64(40), resultChunk.GetValue(groupA, 1))

	// Group B SUM = 60.0
	assert.Equal(t, float64(60), resultChunk.GetValue(groupB, 1))
}

func TestPhysicalAggregate_MultipleChunks(t *testing.T) {
	// Test aggregation across multiple input chunks
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

	// SUM(x)
	sumExpr := &binder.BoundFunctionCall{
		Name: "SUM",
		Args: []binder.BoundExpr{
			&binder.BoundColumnRef{
				Column:  "0",
				ColType: dukdb.TYPE_INTEGER,
			},
		},
		ResType: dukdb.TYPE_DOUBLE,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{},
		[]binder.BoundExpr{sumExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultChunk, err := aggOp.Next()
	require.NoError(t, err)
	require.NotNil(t, resultChunk)

	// SUM of 1..10 = 55
	assert.Equal(t, 1, resultChunk.Count())
	sum := resultChunk.GetValue(0, 0)
	assert.Equal(t, float64(55), sum)
}

func TestPhysicalAggregate_GetTypes(t *testing.T) {
	// Test that GetTypes returns correct types
	types := []dukdb.Type{dukdb.TYPE_INTEGER}
	chunk := storage.NewDataChunk(types)

	typeInfos := make([]dukdb.TypeInfo, 1)
	typeInfos[0] = &basicTypeInfo{typ: dukdb.TYPE_INTEGER}
	mockChild := &mockPhysicalOperator{
		chunks: []*storage.DataChunk{chunk},
		types:  typeInfos,
	}

	// GROUP BY integer, COUNT(*) -> [INTEGER, BIGINT]
	groupByExpr := &binder.BoundColumnRef{
		Column:  "0",
		ColType: dukdb.TYPE_INTEGER,
	}

	countExpr := &binder.BoundFunctionCall{
		Name:    "COUNT",
		Star:    true,
		ResType: dukdb.TYPE_BIGINT,
	}

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)
	ctx := &ExecutionContext{
		Context: context.Background(),
	}

	aggOp, err := NewPhysicalAggregateOperator(
		mockChild,
		nil,
		[]binder.BoundExpr{groupByExpr},
		[]binder.BoundExpr{countExpr},
		exec,
		ctx,
	)
	require.NoError(t, err)

	resultTypes := aggOp.GetTypes()
	require.Len(t, resultTypes, 2)
	assert.Equal(t, dukdb.TYPE_INTEGER, resultTypes[0].InternalType())
	assert.Equal(t, dukdb.TYPE_BIGINT, resultTypes[1].InternalType())
}

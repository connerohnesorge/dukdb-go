package executor

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockOperator is a simple mock operator for testing
type mockOperator struct {
	chunks []*storage.DataChunk
	idx    int
}

func (m *mockOperator) Next() (*storage.DataChunk, error) {
	if m.idx >= len(m.chunks) {
		return nil, nil
	}
	chunk := m.chunks[m.idx]
	m.idx++

	return chunk, nil
}

func (m *mockOperator) GetTypes() []dukdb.TypeInfo {
	if len(m.chunks) > 0 &&
		m.chunks[0].ColumnCount() > 0 {
		types := make(
			[]dukdb.TypeInfo,
			m.chunks[0].ColumnCount(),
		)
		for i := range types {
			types[i] = &basicTypeInfo{
				typ: dukdb.TYPE_BIGINT,
			}
		}

		return types
	}

	return nil
}

func TestPhysicalLimitOperator_LimitOnly(
	t *testing.T,
) {
	// Create mock data: 20 rows total
	chunk1 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 10 {
		chunk1.AppendRow([]any{int64(i)})
	}

	chunk2 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := 10; i < 20; i++ {
		chunk2.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{
			chunk1,
			chunk2,
		},
	}

	// Create limit operator with LIMIT 5
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		5, // limit
		0, // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should only get first 5 rows
	assert.Equal(
		t,
		[]int64{0, 1, 2, 3, 4},
		results,
	)
}

func TestPhysicalLimitOperator_OffsetOnly(
	t *testing.T,
) {
	// Create mock data: 20 rows total
	chunk1 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 10 {
		chunk1.AppendRow([]any{int64(i)})
	}

	chunk2 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := 10; i < 20; i++ {
		chunk2.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{
			chunk1,
			chunk2,
		},
	}

	// Create limit operator with OFFSET 15 (no limit)
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		-1, // no limit
		15, // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should skip first 15 rows, get rows 15-19
	assert.Equal(
		t,
		[]int64{15, 16, 17, 18, 19},
		results,
	)
}

func TestPhysicalLimitOperator_LimitAndOffset(
	t *testing.T,
) {
	// Create mock data: 20 rows total
	chunk1 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 10 {
		chunk1.AppendRow([]any{int64(i)})
	}

	chunk2 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := 10; i < 20; i++ {
		chunk2.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{
			chunk1,
			chunk2,
		},
	}

	// Create limit operator with LIMIT 5 OFFSET 10
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		5,  // limit
		10, // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should skip first 10 rows, then get next 5 (rows 10-14)
	assert.Equal(
		t,
		[]int64{10, 11, 12, 13, 14},
		results,
	)
}

func TestPhysicalLimitOperator_LimitLargerThanData(
	t *testing.T,
) {
	// Create mock data: 5 rows total
	chunk := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 5 {
		chunk.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{chunk},
	}

	// Create limit operator with LIMIT 100 (more than available)
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		100, // limit
		0,   // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should get all 5 rows
	assert.Equal(
		t,
		[]int64{0, 1, 2, 3, 4},
		results,
	)
}

func TestPhysicalLimitOperator_OffsetLargerThanData(
	t *testing.T,
) {
	// Create mock data: 5 rows total
	chunk := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 5 {
		chunk.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{chunk},
	}

	// Create limit operator with OFFSET 100 (more than available)
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		-1,  // no limit
		100, // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should get no rows
	assert.Empty(t, results)
}

func TestPhysicalLimitOperator_PartialChunk(
	t *testing.T,
) {
	// Create mock data: chunk boundaries don't align with limit
	chunk1 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 7 {
		chunk1.AppendRow([]any{int64(i)})
	}

	chunk2 := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := 7; i < 15; i++ {
		chunk2.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{
			chunk1,
			chunk2,
		},
	}

	// Create limit operator with LIMIT 10 OFFSET 3
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		10, // limit
		3,  // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should skip first 3 rows (0,1,2), then get next 10 (3-12)
	assert.Equal(
		t,
		[]int64{3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		results,
	)
}

func TestPhysicalLimitOperator_ZeroLimit(
	t *testing.T,
) {
	// Create mock data
	chunk := storage.NewDataChunk(
		[]dukdb.Type{dukdb.TYPE_BIGINT},
	)
	for i := range 5 {
		chunk.AppendRow([]any{int64(i)})
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{chunk},
	}

	// Create limit operator with LIMIT 0
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
		},
		0, // limit 0
		0, // offset
	)
	require.NoError(t, err)

	// Collect all results
	var results []int64
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			val := chunk.GetValue(i, 0)
			results = append(results, val.(int64))
		}
	}

	// Should get no rows
	assert.Empty(t, results)
}

func TestPhysicalLimitOperator_MultipleColumns(
	t *testing.T,
) {
	// Create mock data with multiple columns
	chunk := storage.NewDataChunk(
		[]dukdb.Type{
			dukdb.TYPE_BIGINT,
			dukdb.TYPE_VARCHAR,
		},
	)
	for i := range 10 {
		chunk.AppendRow(
			[]any{
				int64(i),
				"value" + string(rune('0'+i)),
			},
		)
	}

	mockChild := &mockOperator{
		chunks: []*storage.DataChunk{chunk},
	}

	// Create limit operator with LIMIT 3 OFFSET 2
	limitOp, err := NewPhysicalLimitOperator(
		mockChild,
		[]planner.ColumnBinding{
			{
				Column: "id",
				Type:   dukdb.TYPE_BIGINT,
			},
			{
				Column: "name",
				Type:   dukdb.TYPE_VARCHAR,
			},
		},
		3, // limit
		2, // offset
	)
	require.NoError(t, err)

	// Collect all results
	type row struct {
		id   int64
		name string
	}
	var results []row
	for {
		chunk, err := limitOp.Next()
		require.NoError(t, err)
		if chunk == nil {
			break
		}
		for i := 0; i < chunk.Count(); i++ {
			results = append(results, row{
				id:   chunk.GetValue(i, 0).(int64),
				name: chunk.GetValue(i, 1).(string),
			})
		}
	}

	// Should skip first 2 rows, then get next 3
	expected := []row{
		{2, "value2"},
		{3, "value3"},
		{4, "value4"},
	}
	assert.Equal(t, expected, results)
}

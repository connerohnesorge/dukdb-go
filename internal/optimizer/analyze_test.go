package optimizer

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatisticsCollectorBasic(t *testing.T) {
	collector := NewStatisticsCollector()

	// Test basic integer column
	columnNames := []string{"id", "name", "value"}
	columnTypes := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}

	// Create sample data
	idData := []any{int64(1), int64(2), int64(3), int64(4), int64(5)}
	nameData := []any{"Alice", "Bob", "Charlie", "Alice", nil}
	valueData := []any{float64(10.5), float64(20.3), float64(15.7), float64(10.5), float64(25.0)}

	allData := [][]any{idData, nameData, valueData}

	dataReader := func(columnIndex int) ([]any, error) {
		return allData[columnIndex], nil
	}

	stats, err := collector.CollectTableStats(columnNames, columnTypes, 5, dataReader)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Check table-level stats
	assert.Equal(t, int64(5), stats.RowCount)
	assert.True(t, stats.IsAnalyzed())
	assert.Equal(t, 3, len(stats.Columns))

	// Check ID column stats
	idStats := stats.GetColumnStats("id")
	require.NotNil(t, idStats)
	assert.Equal(t, "id", idStats.ColumnName)
	assert.Equal(t, dukdb.TYPE_BIGINT, idStats.ColumnType)
	assert.Equal(t, float64(0), idStats.NullFraction) // No nulls
	assert.Equal(t, int64(5), idStats.DistinctCount)  // All unique
	assert.Equal(t, int64(1), idStats.MinValue)
	assert.Equal(t, int64(5), idStats.MaxValue)

	// Check name column stats
	nameStats := stats.GetColumnStats("name")
	require.NotNil(t, nameStats)
	assert.Equal(t, "name", nameStats.ColumnName)
	assert.Equal(t, dukdb.TYPE_VARCHAR, nameStats.ColumnType)
	assert.Equal(t, 0.2, nameStats.NullFraction) // 1 out of 5 is null
	assert.Equal(t, int64(3), nameStats.DistinctCount)
	assert.Equal(t, "Alice", nameStats.MinValue)
	assert.Equal(t, "Charlie", nameStats.MaxValue)

	// Check value column stats
	valueStats := stats.GetColumnStats("value")
	require.NotNil(t, valueStats)
	assert.Equal(t, "value", valueStats.ColumnName)
	assert.Equal(t, dukdb.TYPE_DOUBLE, valueStats.ColumnType)
	assert.Equal(t, float64(0), valueStats.NullFraction)
	assert.Equal(t, int64(4), valueStats.DistinctCount)
	assert.Equal(t, float64(10.5), valueStats.MinValue)
	assert.Equal(t, float64(25.0), valueStats.MaxValue)
}

func TestStatisticsCollectorEmptyTable(t *testing.T) {
	collector := NewStatisticsCollector()

	columnNames := []string{"id"}
	columnTypes := []dukdb.Type{dukdb.TYPE_BIGINT}

	dataReader := func(columnIndex int) ([]any, error) {
		return []any{}, nil
	}

	stats, err := collector.CollectTableStats(columnNames, columnTypes, 0, dataReader)
	require.NoError(t, err)
	require.NotNil(t, stats)

	assert.Equal(t, int64(0), stats.RowCount)
	assert.True(t, stats.IsAnalyzed())

	idStats := stats.GetColumnStats("id")
	require.NotNil(t, idStats)
	assert.Equal(t, int64(0), idStats.DistinctCount)
	assert.Nil(t, idStats.MinValue)
	assert.Nil(t, idStats.MaxValue)
}

func TestStatisticsCollectorAllNulls(t *testing.T) {
	collector := NewStatisticsCollector()

	columnNames := []string{"nullable_col"}
	columnTypes := []dukdb.Type{dukdb.TYPE_VARCHAR}

	data := []any{nil, nil, nil, nil, nil}

	dataReader := func(columnIndex int) ([]any, error) {
		return data, nil
	}

	stats, err := collector.CollectTableStats(columnNames, columnTypes, 5, dataReader)
	require.NoError(t, err)
	require.NotNil(t, stats)

	colStats := stats.GetColumnStats("nullable_col")
	require.NotNil(t, colStats)
	assert.Equal(t, float64(1), colStats.NullFraction) // All nulls
	assert.Equal(t, int64(0), colStats.DistinctCount)
	assert.Nil(t, colStats.MinValue)
	assert.Nil(t, colStats.MaxValue)
}

func TestReservoirSampling(t *testing.T) {
	// Create a large array
	values := make([]any, 10000)
	for i := range values {
		values[i] = i
	}

	// Sample down to 100
	sample := reservoirSample(values, 100)

	assert.Equal(t, 100, len(sample))

	// Verify all sampled values are from the original array
	for _, v := range sample {
		val := v.(int)
		assert.True(t, val >= 0 && val < 10000)
	}
}

func TestReservoirSamplingSmallerThanK(t *testing.T) {
	values := []any{1, 2, 3}

	sample := reservoirSample(values, 100)

	assert.Equal(t, 3, len(sample))
	assert.Equal(t, values, sample)
}

func TestHistogramBuilding(t *testing.T) {
	collector := NewStatisticsCollector()
	collector.SetHistogramBuckets(10)

	// Create enough data for histogram
	values := make([]any, 100)
	for i := range values {
		values[i] = int64(i)
	}

	columnNames := []string{"col"}
	columnTypes := []dukdb.Type{dukdb.TYPE_BIGINT}

	dataReader := func(columnIndex int) ([]any, error) {
		return values, nil
	}

	stats, err := collector.CollectTableStats(columnNames, columnTypes, 100, dataReader)
	require.NoError(t, err)
	require.NotNil(t, stats)

	colStats := stats.GetColumnStats("col")
	require.NotNil(t, colStats)
	require.NotNil(t, colStats.Histogram)

	hist := colStats.Histogram
	assert.Equal(t, 10, hist.NumBuckets)
	assert.Equal(t, 10, len(hist.Buckets))

	// Check that buckets are roughly equi-depth
	for _, bucket := range hist.Buckets {
		// Each bucket should have ~10% of the data
		assert.InDelta(t, 0.1, bucket.Frequency, 0.02)
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		a, b     any
		colType  dukdb.Type
		expected int
	}{
		{"int64 less", int64(1), int64(2), dukdb.TYPE_BIGINT, -1},
		{"int64 equal", int64(5), int64(5), dukdb.TYPE_BIGINT, 0},
		{"int64 greater", int64(10), int64(3), dukdb.TYPE_BIGINT, 1},
		{"float64 less", float64(1.5), float64(2.5), dukdb.TYPE_DOUBLE, -1},
		{"string less", "apple", "banana", dukdb.TYPE_VARCHAR, -1},
		{"string equal", "test", "test", dukdb.TYPE_VARCHAR, 0},
		{"bool false < true", false, true, dukdb.TYPE_BOOLEAN, -1},
		{"nil nil", nil, nil, dukdb.TYPE_BIGINT, 0},
		{"nil less", nil, int64(1), dukdb.TYPE_BIGINT, -1},
		{"nil greater", int64(1), nil, dukdb.TYPE_BIGINT, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b, tt.colType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDistinctCount(t *testing.T) {
	collector := NewStatisticsCollector()

	tests := []struct {
		name     string
		values   []any
		expected int64
	}{
		{"all unique", []any{1, 2, 3, 4, 5}, 5},
		{"some duplicates", []any{1, 2, 2, 3, 3, 3}, 3},
		{"all same", []any{1, 1, 1, 1, 1}, 1},
		{"with nils", []any{1, nil, 2, nil, 3}, 3},
		{"empty", []any{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := collector.countDistinct(tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFindMinMax(t *testing.T) {
	collector := NewStatisticsCollector()

	tests := []struct {
		name        string
		values      []any
		colType     dukdb.Type
		expectedMin any
		expectedMax any
	}{
		{
			"integers",
			[]any{int64(5), int64(2), int64(8), int64(1)},
			dukdb.TYPE_BIGINT,
			int64(1),
			int64(8),
		},
		{
			"floats",
			[]any{float64(3.14), float64(1.41), float64(2.72)},
			dukdb.TYPE_DOUBLE,
			float64(1.41),
			float64(3.14),
		},
		{
			"strings",
			[]any{"banana", "apple", "cherry"},
			dukdb.TYPE_VARCHAR,
			"apple",
			"cherry",
		},
		{
			"single value",
			[]any{int64(42)},
			dukdb.TYPE_BIGINT,
			int64(42),
			int64(42),
		},
		{
			"empty",
			[]any{},
			dukdb.TYPE_BIGINT,
			nil,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			min, max := collector.findMinMax(tt.values, tt.colType)
			assert.Equal(t, tt.expectedMin, min)
			assert.Equal(t, tt.expectedMax, max)
		})
	}
}

func TestIsHistogramType(t *testing.T) {
	histogramTypes := []dukdb.Type{
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, typ := range histogramTypes {
		assert.True(t, isHistogramType(typ), "expected histogram support for %v", typ)
	}

	nonHistogramTypes := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_LIST,
		dukdb.TYPE_STRUCT,
		dukdb.TYPE_MAP,
	}

	for _, typ := range nonHistogramTypes {
		assert.False(t, isHistogramType(typ), "expected no histogram support for %v", typ)
	}
}

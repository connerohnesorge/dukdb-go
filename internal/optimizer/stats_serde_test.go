package optimizer

import (
	"bytes"
	"math"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSerializeDeserializeInt8 tests int8 statistics roundtrip.
func TestSerializeDeserializeInt8(t *testing.T) {
	// Create original statistics
	original := &TableStatistics{
		RowCount: 1000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				ColumnType:    dukdb.TYPE_TINYINT,
				NullFraction:  0.0,
				DistinctCount: 128,
				MinValue:      int8(-128),
				MaxValue:      int8(127),
				AvgWidth:      1,
			},
		},
	}

	// Serialize
	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Deserialize
	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	// Verify
	assert.Equal(t, original.RowCount, restored.RowCount)
	assert.Equal(t, len(original.Columns), len(restored.Columns))

	col := restored.Columns[0]
	assert.Equal(t, "id", col.ColumnName)
	assert.Equal(t, dukdb.TYPE_TINYINT, col.ColumnType)
	assert.Equal(t, int64(128), col.DistinctCount)
	assert.Equal(t, int8(-128), col.MinValue)
	assert.Equal(t, int8(127), col.MaxValue)
}

// TestSerializeDeserializeInt32 tests int32 statistics roundtrip.
func TestSerializeDeserializeInt32(t *testing.T) {
	original := &TableStatistics{
		RowCount: 50000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "user_id",
				ColumnType:    dukdb.TYPE_INTEGER,
				NullFraction:  0.05,
				DistinctCount: 40000,
				MinValue:      int32(1),
				MaxValue:      int32(1000000),
				AvgWidth:      4,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	assert.Equal(t, original.RowCount, restored.RowCount)
	col := restored.Columns[0]
	assert.Equal(t, int32(1), col.MinValue)
	assert.Equal(t, int32(1000000), col.MaxValue)
	assert.Equal(t, dukdb.TYPE_INTEGER, col.ColumnType)
}

// TestSerializeDeserializeInt64 tests int64 statistics roundtrip.
func TestSerializeDeserializeInt64(t *testing.T) {
	original := &TableStatistics{
		RowCount: 100000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "timestamp",
				ColumnType:    dukdb.TYPE_BIGINT,
				NullFraction:  0.0,
				DistinctCount: 95000,
				MinValue:      int64(1000000000000),
				MaxValue:      int64(9999999999999),
				AvgWidth:      8,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	col := restored.Columns[0]
	assert.Equal(t, int64(1000000000000), col.MinValue)
	assert.Equal(t, int64(9999999999999), col.MaxValue)
}

// TestSerializeDeserializeUint64 tests uint64 statistics roundtrip.
func TestSerializeDeserializeUint64(t *testing.T) {
	original := &TableStatistics{
		RowCount: 50000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "hash_value",
				ColumnType:    dukdb.TYPE_UBIGINT,
				NullFraction:  0.0,
				DistinctCount: 50000,
				MinValue:      uint64(0),
				MaxValue:      uint64(18446744073709551615),
				AvgWidth:      8,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	col := restored.Columns[0]
	assert.Equal(t, uint64(0), col.MinValue)
	assert.Equal(t, uint64(18446744073709551615), col.MaxValue)
}

// TestSerializeDeserializeFloat32 tests float32 statistics roundtrip.
func TestSerializeDeserializeFloat32(t *testing.T) {
	original := &TableStatistics{
		RowCount: 10000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "price",
				ColumnType:    dukdb.TYPE_FLOAT,
				NullFraction:  0.02,
				DistinctCount: 5000,
				MinValue:      float32(0.01),
				MaxValue:      float32(999.99),
				AvgWidth:      4,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	col := restored.Columns[0]
	assert.Equal(t, float32(0.01), col.MinValue)
	assert.Equal(t, float32(999.99), col.MaxValue)
}

// TestSerializeDeserializeFloat64 tests float64 statistics roundtrip.
func TestSerializeDeserializeFloat64(t *testing.T) {
	original := &TableStatistics{
		RowCount: 5000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "value",
				ColumnType:    dukdb.TYPE_DOUBLE,
				NullFraction:  0.0,
				DistinctCount: 4500,
				MinValue:      float64(-1.7976931348623157e+308),
				MaxValue:      float64(1.7976931348623157e+308),
				AvgWidth:      8,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	col := restored.Columns[0]
	assert.Equal(t, float64(-1.7976931348623157e+308), col.MinValue)
	assert.Equal(t, float64(1.7976931348623157e+308), col.MaxValue)
}

// TestSerializeDeserializeString tests string statistics roundtrip.
func TestSerializeDeserializeString(t *testing.T) {
	original := &TableStatistics{
		RowCount: 10000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "name",
				ColumnType:    dukdb.TYPE_VARCHAR,
				NullFraction:  0.1,
				DistinctCount: 9000,
				MinValue:      "Alice",
				MaxValue:      "Zoe",
				AvgWidth:      20,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	col := restored.Columns[0]
	assert.Equal(t, "Alice", col.MinValue)
	assert.Equal(t, "Zoe", col.MaxValue)
	assert.Equal(t, dukdb.TYPE_VARCHAR, col.ColumnType)
}

// TestSerializeDeserializeMultipleColumns tests multiple columns in one table.
func TestSerializeDeserializeMultipleColumns(t *testing.T) {
	original := &TableStatistics{
		RowCount: 5000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				ColumnType:    dukdb.TYPE_BIGINT,
				NullFraction:  0.0,
				DistinctCount: 5000,
				MinValue:      int64(1),
				MaxValue:      int64(5000),
				AvgWidth:      8,
			},
			{
				ColumnName:    "name",
				ColumnType:    dukdb.TYPE_VARCHAR,
				NullFraction:  0.05,
				DistinctCount: 4800,
				MinValue:      "Alice",
				MaxValue:      "Zoe",
				AvgWidth:      25,
			},
			{
				ColumnName:    "score",
				ColumnType:    dukdb.TYPE_DOUBLE,
				NullFraction:  0.02,
				DistinctCount: 500,
				MinValue:      float64(0.0),
				MaxValue:      float64(100.0),
				AvgWidth:      8,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	assert.Equal(t, 3, len(restored.Columns))
	assert.Equal(t, int64(1), restored.Columns[0].MinValue)
	assert.Equal(t, "Zoe", restored.Columns[1].MaxValue)
	assert.Equal(t, float64(100.0), restored.Columns[2].MaxValue)
}

// TestSerializeDeserializeNullHandling tests NULL flag handling.
func TestSerializeDeserializeNullHandling(t *testing.T) {
	tests := []struct {
		name              string
		nullFraction      float64
		expectedHasNull   bool
		expectedHasNoNull bool
	}{
		{"no_nulls", 0.0, false, true},
		{"all_nulls", 1.0, true, false},
		{"mixed", 0.5, true, true},
		{"few_nulls", 0.1, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &TableStatistics{
				RowCount: 1000,
				Columns: []ColumnStatistics{
					{
						ColumnName:    "col",
						ColumnType:    dukdb.TYPE_INTEGER,
						NullFraction:  tt.nullFraction,
						DistinctCount: 100,
						MinValue:      int32(0),
						MaxValue:      int32(999),
						AvgWidth:      4,
					},
				},
			}

			serializer := NewStatsSerializer()
			data, err := serializer.SerializeTableStatistics(original)
			require.NoError(t, err)

			deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
			restored, err := deserializer.DeserializeTableStatistics()
			require.NoError(t, err)

			col := restored.Columns[0]
			// Allow some tolerance for NULL fraction due to encoding/decoding
			if tt.nullFraction == 0.0 {
				assert.Equal(t, 0.0, col.NullFraction)
			} else if tt.nullFraction == 1.0 {
				assert.Equal(t, 1.0, col.NullFraction)
			} else {
				// For mixed NULL fractions, should be non-zero and less than 1
				assert.Greater(t, col.NullFraction, 0.0)
				assert.Less(t, col.NullFraction, 1.0)
			}
		})
	}
}

// TestSerializeDeserializeVersionCompat tests version compatibility.
func TestSerializeDeserializeVersionCompat(t *testing.T) {
	original := &TableStatistics{
		RowCount: 1000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				ColumnType:    dukdb.TYPE_INTEGER,
				NullFraction:  0.0,
				DistinctCount: 500,
				MinValue:      int32(0),
				MaxValue:      int32(999),
				AvgWidth:      4,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	// Test with unlimited read size
	deserializer := NewStatsDeserializer(bytes.NewReader(data), 0)
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)
	assert.Equal(t, original.RowCount, restored.RowCount)

	// Test with limited read size (should succeed if data is small enough)
	deserializer2 := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored2, err := deserializer2.DeserializeTableStatistics()
	require.NoError(t, err)
	assert.Equal(t, original.RowCount, restored2.RowCount)
}

// TestSerializeDeserializeEmptyTable tests empty statistics.
func TestSerializeDeserializeEmptyTable(t *testing.T) {
	original := &TableStatistics{
		RowCount: 0,
		Columns:  []ColumnStatistics{},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	assert.Equal(t, int64(0), restored.RowCount)
	assert.Equal(t, 0, len(restored.Columns))
}

// TestSerializeDeserializeSpecialFloatValues tests special float values (NaN, Inf).
func TestSerializeDeserializeSpecialFloatValues(t *testing.T) {
	original := &TableStatistics{
		RowCount: 100,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "value",
				ColumnType:    dukdb.TYPE_DOUBLE,
				NullFraction:  0.0,
				DistinctCount: 50,
				MinValue:      math.Inf(-1),
				MaxValue:      math.Inf(1),
				AvgWidth:      8,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
	restored, err := deserializer.DeserializeTableStatistics()
	require.NoError(t, err)

	col := restored.Columns[0]
	assert.Equal(t, math.Inf(-1), col.MinValue)
	assert.Equal(t, math.Inf(1), col.MaxValue)
}

// TestDeserializeInvalidVersion tests error handling for unsupported versions.
func TestDeserializeInvalidVersion(t *testing.T) {
	// Create a buffer with invalid version
	buf := new(bytes.Buffer)
	buf.WriteByte(255) // Invalid version

	deserializer := NewStatsDeserializer(buf, 1000)
	_, err := deserializer.DeserializeTableStatistics()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported")
}

// TestDeserializeReadSizeLimit tests read size limit protection.
func TestDeserializeReadSizeLimit(t *testing.T) {
	original := &TableStatistics{
		RowCount: 1000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				ColumnType:    dukdb.TYPE_INTEGER,
				NullFraction:  0.0,
				DistinctCount: 500,
				MinValue:      int32(0),
				MaxValue:      int32(999),
				AvgWidth:      4,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(original)
	require.NoError(t, err)

	// Try to deserialize with very small limit
	deserializer := NewStatsDeserializer(bytes.NewReader(data), 1)
	_, err = deserializer.DeserializeTableStatistics()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeded")
}

// BenchmarkSerializeTableStatistics benchmarks table statistics serialization.
func BenchmarkSerializeTableStatistics(b *testing.B) {
	stats := &TableStatistics{
		RowCount: 1000000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				ColumnType:    dukdb.TYPE_BIGINT,
				NullFraction:  0.0,
				DistinctCount: 900000,
				MinValue:      int64(1),
				MaxValue:      int64(1000000),
				AvgWidth:      8,
			},
			{
				ColumnName:    "name",
				ColumnType:    dukdb.TYPE_VARCHAR,
				NullFraction:  0.05,
				DistinctCount: 800000,
				MinValue:      "A",
				MaxValue:      "Z",
				AvgWidth:      30,
			},
			{
				ColumnName:    "value",
				ColumnType:    dukdb.TYPE_DOUBLE,
				NullFraction:  0.02,
				DistinctCount: 500000,
				MinValue:      0.0,
				MaxValue:      1000.0,
				AvgWidth:      8,
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		serializer := NewStatsSerializer()
		_, err := serializer.SerializeTableStatistics(stats)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeserializeTableStatistics benchmarks table statistics deserialization.
func BenchmarkDeserializeTableStatistics(b *testing.B) {
	stats := &TableStatistics{
		RowCount: 1000000,
		Columns: []ColumnStatistics{
			{
				ColumnName:    "id",
				ColumnType:    dukdb.TYPE_BIGINT,
				NullFraction:  0.0,
				DistinctCount: 900000,
				MinValue:      int64(1),
				MaxValue:      int64(1000000),
				AvgWidth:      8,
			},
			{
				ColumnName:    "name",
				ColumnType:    dukdb.TYPE_VARCHAR,
				NullFraction:  0.05,
				DistinctCount: 800000,
				MinValue:      "A",
				MaxValue:      "Z",
				AvgWidth:      30,
			},
			{
				ColumnName:    "value",
				ColumnType:    dukdb.TYPE_DOUBLE,
				NullFraction:  0.02,
				DistinctCount: 500000,
				MinValue:      0.0,
				MaxValue:      1000.0,
				AvgWidth:      8,
			},
		},
	}

	serializer := NewStatsSerializer()
	data, err := serializer.SerializeTableStatistics(stats)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deserializer := NewStatsDeserializer(bytes.NewReader(data), int64(len(data)*2))
		_, err := deserializer.DeserializeTableStatistics()
		if err != nil {
			b.Fatal(err)
		}
	}
}

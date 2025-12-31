package dukdb

import (
	"testing"
)

// BenchmarkComparison_DataChunkOps benchmarks core DataChunk operations.
// These benchmarks establish baseline performance for comparison with DuckDB C++.
//
// DuckDB C++ typical performance (from documentation and benchmarks):
// - Vector operations: ~1-10ns per element
// - Scan throughput: ~100M+ rows/second for simple types
// - Validity mask operations: ~1ns per bit
//
// dukdb-go targets: within 2x of C++ performance for these operations.

func BenchmarkComparison_ValidityMaskOps(b *testing.B) {
	// DuckDB C++ validity operations: ~1ns per operation
	// Target: <2ns per operation
	b.Run("IsValid", func(b *testing.B) {
		intType, _ := NewTypeInfo(TYPE_INTEGER)
		chunk, _ := NewDataChunk([]TypeInfo{intType})
		_ = chunk.SetSize(GetDataChunkCapacity())

		for i := 0; i < GetDataChunkCapacity(); i++ {
			_ = chunk.SetValue(0, i, int32(i))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for row := 0; row < GetDataChunkCapacity(); row++ {
				_, _ = chunk.GetValue(0, row)
			}
		}

		opsPerIter := float64(GetDataChunkCapacity())
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})
}

func BenchmarkComparison_IntegerOps(b *testing.B) {
	// DuckDB C++ integer operations: ~5-10ns per value
	// Target: <20ns per value
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	chunk, _ := NewDataChunk([]TypeInfo{intType})
	_ = chunk.SetSize(GetDataChunkCapacity())

	b.Run("SetValue", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for row := 0; row < GetDataChunkCapacity(); row++ {
				_ = chunk.SetValue(0, row, int32(row))
			}
		}

		opsPerIter := float64(GetDataChunkCapacity())
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})

	b.Run("GetValue", func(b *testing.B) {
		// Pre-fill
		for row := 0; row < GetDataChunkCapacity(); row++ {
			_ = chunk.SetValue(0, row, int32(row))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for row := 0; row < GetDataChunkCapacity(); row++ {
				_, _ = chunk.GetValue(0, row)
			}
		}

		opsPerIter := float64(GetDataChunkCapacity())
		nsPerOp := float64(b.Elapsed().Nanoseconds()) / float64(b.N) / opsPerIter
		b.ReportMetric(nsPerOp, "ns/value")
	})
}

func BenchmarkComparison_BulkAppend(b *testing.B) {
	// DuckDB C++ bulk append: ~10M rows/second for simple types
	// Target: >5M rows/second (within 2x)
	mock, state := newAppenderMock()
	state.setTableColumns([]string{"id"}, []string{"INTEGER"})
	conn := createAppenderTestConn(mock)

	const rowCount = 100000

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appender, _ := NewAppenderWithThreshold(conn, "", "main", "test", rowCount)
		for j := 0; j < rowCount; j++ {
			_ = appender.AppendRow(j)
		}
		_ = appender.Flush()
		_ = appender.Close()

		b.StopTimer()
		state.execCount = 0
		b.StartTimer()
	}

	rowsPerSec := float64(rowCount) / (float64(b.Elapsed().Nanoseconds()) / float64(b.N) / 1e9)
	b.ReportMetric(rowsPerSec, "rows/sec")
}

package arrow

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Benchmark configuration constants.
const (
	benchmarkSmallRows   = 100
	benchmarkMediumRows  = 1000
	benchmarkLargeRows   = 10000
	benchmarkColumnCount = 10
)

// createBenchmarkSchema creates a schema with multiple column types for benchmarking.
func createBenchmarkSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "bool_col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "int32_col", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "int64_col", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "float32_col", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
			{Name: "float64_col", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "string_col", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "binary_col", Type: arrow.BinaryTypes.Binary, Nullable: true},
			{Name: "date_col", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
			{
				Name:     "timestamp_col",
				Type:     &arrow.TimestampType{Unit: arrow.Microsecond},
				Nullable: true,
			},
		},
		nil,
	)
}

// createBenchmarkRecord creates a record batch with the given number of rows.
func createBenchmarkRecord(
	alloc memory.Allocator,
	schema *arrow.Schema,
	numRows int,
) arrow.RecordBatch {
	bldr := array.NewRecordBuilder(alloc, schema)
	defer bldr.Release()

	for i := 0; i < numRows; i++ {
		bldr.Field(0).(*array.Int64Builder).Append(int64(i))
		bldr.Field(1).(*array.BooleanBuilder).Append(i%2 == 0)
		bldr.Field(2).(*array.Int32Builder).Append(int32(i * 100))
		bldr.Field(3).(*array.Int64Builder).Append(int64(i * 1000))
		bldr.Field(4).(*array.Float32Builder).Append(float32(i) * 1.5)
		bldr.Field(5).(*array.Float64Builder).Append(float64(i) * 2.5)
		bldr.Field(6).(*array.StringBuilder).Append("string value " + string(rune('A'+i%26)))
		bldr.Field(7).(*array.BinaryBuilder).Append([]byte{byte(i % 256), byte((i + 1) % 256)})
		bldr.Field(8).(*array.Date32Builder).Append(arrow.Date32(19000 + i))
		bldr.Field(9).(*array.TimestampBuilder).Append(arrow.Timestamp(int64(i) * 1000000))
	}

	return bldr.NewRecord()
}

// createBenchmarkArrowBuffer creates an Arrow IPC file in memory with the given number of rows.
func createBenchmarkArrowBuffer(numRows int) *bytes.Buffer {
	alloc := memory.NewGoAllocator()
	schema := createBenchmarkSchema()

	var buf bytes.Buffer
	writer, err := ipc.NewFileWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err != nil {
		panic(err)
	}

	record := createBenchmarkRecord(alloc, schema, numRows)
	defer record.Release()

	if err := writer.Write(record); err != nil {
		panic(err)
	}

	if err := writer.Close(); err != nil {
		panic(err)
	}

	return &buf
}

// createBenchmarkStreamBuffer creates an Arrow IPC stream in memory with the given number of rows.
func createBenchmarkStreamBuffer(numRows int) *bytes.Buffer {
	alloc := memory.NewGoAllocator()
	schema := createBenchmarkSchema()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))

	record := createBenchmarkRecord(alloc, schema, numRows)
	defer record.Release()

	if err := writer.Write(record); err != nil {
		panic(err)
	}

	if err := writer.Close(); err != nil {
		panic(err)
	}

	return &buf
}

// createBenchmarkDataChunk creates a DataChunk for write benchmarking.
func createBenchmarkDataChunk(numRows int) *storage.DataChunk {
	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	chunk := storage.NewDataChunkWithCapacity(types, numRows)
	chunk.SetCount(numRows)

	// Base date for benchmarks
	baseDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < numRows; i++ {
		chunk.GetVector(0).SetValue(i, int64(i))
		chunk.GetVector(1).SetValue(i, i%2 == 0)
		chunk.GetVector(2).SetValue(i, int32(i*100))
		chunk.GetVector(3).SetValue(i, int64(i*1000))
		chunk.GetVector(4).SetValue(i, float32(i)*1.5)
		chunk.GetVector(5).SetValue(i, float64(i)*2.5)
		chunk.GetVector(6).SetValue(i, "string value "+string(rune('A'+i%26)))
		chunk.GetVector(7).SetValue(i, []byte{byte(i % 256), byte((i + 1) % 256)})
		// Date and timestamp as time.Time
		chunk.GetVector(8).SetValue(i, baseDate.AddDate(0, 0, i))
		chunk.GetVector(9).SetValue(i, baseDate.Add(time.Duration(i)*time.Hour))
	}

	return chunk
}

// BenchmarkReadArrowFile benchmarks reading Arrow IPC files.
func BenchmarkReadArrowFile(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	for _, size := range sizes {
		buf := createBenchmarkArrowBuffer(size.rows)
		data := buf.Bytes()

		b.Run(size.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader, err := NewReader(bytes.NewReader(data), nil)
				if err != nil {
					b.Fatal(err)
				}

				// Read all chunks
				for {
					_, err := reader.ReadChunk()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}

				_ = reader.Close()
			}
		})
	}
}

// BenchmarkWriteArrowFile benchmarks writing Arrow IPC files.
func BenchmarkWriteArrowFile(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	names := []string{"id", "bool_col", "int32_col", "int64_col", "float32_col",
		"float64_col", "string_col", "binary_col", "date_col", "timestamp_col"}
	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, size := range sizes {
		chunk := createBenchmarkDataChunk(size.rows)

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer

				writer, err := NewWriter(&buf, nil)
				if err != nil {
					b.Fatal(err)
				}

				if err := writer.SetSchema(names); err != nil {
					b.Fatal(err)
				}

				if err := writer.SetTypes(types); err != nil {
					b.Fatal(err)
				}

				if err := writer.WriteChunk(chunk); err != nil {
					b.Fatal(err)
				}

				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}

				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}

// BenchmarkReadArrowStream benchmarks reading Arrow IPC streams.
func BenchmarkReadArrowStream(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	for _, size := range sizes {
		buf := createBenchmarkStreamBuffer(size.rows)
		data := buf.Bytes()

		b.Run(size.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader, err := NewStreamReader(bytes.NewReader(data), nil)
				if err != nil {
					b.Fatal(err)
				}

				// Read all chunks
				for {
					_, err := reader.ReadChunk()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}

				_ = reader.Close()
			}
		})
	}
}

// BenchmarkWriteArrowStream benchmarks writing Arrow IPC streams.
func BenchmarkWriteArrowStream(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	names := []string{"id", "bool_col", "int32_col", "int64_col", "float32_col",
		"float64_col", "string_col", "binary_col", "date_col", "timestamp_col"}
	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, size := range sizes {
		chunk := createBenchmarkDataChunk(size.rows)

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer

				writer, err := NewStreamWriter(&buf, nil)
				if err != nil {
					b.Fatal(err)
				}

				if err := writer.SetSchema(names); err != nil {
					b.Fatal(err)
				}

				if err := writer.SetTypes(types); err != nil {
					b.Fatal(err)
				}

				if err := writer.WriteChunk(chunk); err != nil {
					b.Fatal(err)
				}

				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}

				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}

// BenchmarkRecordBatchToDataChunk benchmarks conversion from Arrow RecordBatch to DataChunk.
func BenchmarkRecordBatchToDataChunk(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	alloc := memory.NewGoAllocator()
	schema := createBenchmarkSchema()

	for _, size := range sizes {
		record := createBenchmarkRecord(alloc, schema, size.rows)
		defer record.Release()

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				chunk, err := RecordBatchToDataChunk(record)
				if err != nil {
					b.Fatal(err)
				}
				_ = chunk
			}
		})
	}
}

// BenchmarkDataChunkToRecordBatch benchmarks conversion from DataChunk to Arrow RecordBatch.
func BenchmarkDataChunkToRecordBatch(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	alloc := memory.NewGoAllocator()
	schema := createBenchmarkSchema()

	for _, size := range sizes {
		chunk := createBenchmarkDataChunk(size.rows)

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				record, err := DataChunkToRecordBatch(chunk, schema, alloc)
				if err != nil {
					b.Fatal(err)
				}
				record.Release()
			}
		})
	}
}

// BenchmarkRoundTrip benchmarks a complete write-then-read round trip.
func BenchmarkRoundTrip(b *testing.B) {
	sizes := []struct {
		name string
		rows int
	}{
		{"100rows", benchmarkSmallRows},
		{"1000rows", benchmarkMediumRows},
		{"10000rows", benchmarkLargeRows},
	}

	names := []string{"id", "bool_col", "int32_col", "int64_col", "float32_col",
		"float64_col", "string_col", "binary_col", "date_col", "timestamp_col"}
	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, size := range sizes {
		chunk := createBenchmarkDataChunk(size.rows)

		b.Run(size.name, func(b *testing.B) {
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Write
				var buf bytes.Buffer
				writer, err := NewWriter(&buf, nil)
				if err != nil {
					b.Fatal(err)
				}

				if err := writer.SetSchema(names); err != nil {
					b.Fatal(err)
				}

				if err := writer.SetTypes(types); err != nil {
					b.Fatal(err)
				}

				if err := writer.WriteChunk(chunk); err != nil {
					b.Fatal(err)
				}

				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}

				// Read
				reader, err := NewReader(bytes.NewReader(buf.Bytes()), nil)
				if err != nil {
					b.Fatal(err)
				}

				for {
					_, err := reader.ReadChunk()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}

				_ = reader.Close()
			}
		})
	}
}

// BenchmarkReadArrowFile_DiskIO benchmarks reading Arrow files from disk.
// This measures actual I/O performance.
func BenchmarkReadArrowFile_DiskIO(b *testing.B) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "bench_arrow_*.arrow")
	if err != nil {
		b.Fatal(err)
	}
	path := tmpFile.Name()
	defer func() { _ = os.Remove(path) }()

	// Write benchmark data
	alloc := memory.NewGoAllocator()
	schema := createBenchmarkSchema()
	writer, err := ipc.NewFileWriter(tmpFile, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err != nil {
		b.Fatal(err)
	}

	record := createBenchmarkRecord(alloc, schema, benchmarkLargeRows)
	defer record.Release()

	if err := writer.Write(record); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	_ = tmpFile.Close()

	// Get file size for throughput calculation
	info, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	fileSize := info.Size()

	b.ResetTimer()
	b.SetBytes(fileSize)

	for i := 0; i < b.N; i++ {
		reader, err := NewReaderFromPath(path, nil)
		if err != nil {
			b.Fatal(err)
		}

		for {
			_, err := reader.ReadChunk()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}

		_ = reader.Close()
	}
}

// BenchmarkWriteArrowFile_DiskIO benchmarks writing Arrow files to disk.
// This measures actual I/O performance.
func BenchmarkWriteArrowFile_DiskIO(b *testing.B) {
	chunk := createBenchmarkDataChunk(benchmarkLargeRows)

	names := []string{"id", "bool_col", "int32_col", "int64_col", "float32_col",
		"float64_col", "string_col", "binary_col", "date_col", "timestamp_col"}
	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a temporary file
		tmpFile, err := os.CreateTemp("", "bench_arrow_*.arrow")
		if err != nil {
			b.Fatal(err)
		}
		path := tmpFile.Name()
		_ = tmpFile.Close()
		_ = os.Remove(path) // Remove so writer can create

		writer, err := NewWriterToPath(path, nil)
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.SetSchema(names); err != nil {
			b.Fatal(err)
		}

		if err := writer.SetTypes(types); err != nil {
			b.Fatal(err)
		}

		if err := writer.WriteChunk(chunk); err != nil {
			b.Fatal(err)
		}

		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}

		// Get file size for throughput
		info, err := os.Stat(path)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(info.Size())

		_ = os.Remove(path)
	}
}

// BenchmarkWriteWithCompression benchmarks writing with different compression options.
func BenchmarkWriteWithCompression(b *testing.B) {
	compressions := []struct {
		name string
		comp Compression
	}{
		{"none", CompressionNone},
		{"lz4", CompressionLZ4},
		{"zstd", CompressionZSTD},
	}

	chunk := createBenchmarkDataChunk(benchmarkMediumRows)

	names := []string{"id", "bool_col", "int32_col", "int64_col", "float32_col",
		"float64_col", "string_col", "binary_col", "date_col", "timestamp_col"}
	types := []dukdb.Type{
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BLOB,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, comp := range compressions {
		b.Run(comp.name, func(b *testing.B) {
			opts := &WriterOptions{Compression: comp.comp}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				var buf bytes.Buffer

				writer, err := NewWriter(&buf, opts)
				if err != nil {
					b.Fatal(err)
				}

				if err := writer.SetSchema(names); err != nil {
					b.Fatal(err)
				}

				if err := writer.SetTypes(types); err != nil {
					b.Fatal(err)
				}

				if err := writer.WriteChunk(chunk); err != nil {
					b.Fatal(err)
				}

				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}

				b.SetBytes(int64(buf.Len()))
			}
		})
	}
}

// BenchmarkIterator benchmarks the batch iterator pattern.
func BenchmarkIterator(b *testing.B) {
	alloc := memory.NewGoAllocator()
	schema := createBenchmarkSchema()

	// Create a file with multiple batches
	var buf bytes.Buffer
	writer, err := ipc.NewFileWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err != nil {
		b.Fatal(err)
	}

	for batch := 0; batch < 10; batch++ {
		record := createBenchmarkRecord(alloc, schema, benchmarkMediumRows)
		if err := writer.Write(record); err != nil {
			record.Release()
			b.Fatal(err)
		}
		record.Release()
	}

	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		reader, err := NewReader(bytes.NewReader(data), nil)
		if err != nil {
			b.Fatal(err)
		}

		iter := reader.Iterator()
		for iter.Next() {
			_ = iter.Record()
		}
		if iter.Err() != nil {
			b.Fatal(iter.Err())
		}

		_ = reader.Close()
	}
}

// BenchmarkColumnProjection benchmarks reading with column projection.
func BenchmarkColumnProjection(b *testing.B) {
	buf := createBenchmarkArrowBuffer(benchmarkLargeRows)
	data := buf.Bytes()

	benchCases := []struct {
		name    string
		columns []string
	}{
		{"all_columns", nil},
		{"single_column", []string{"id"}},
		{"three_columns", []string{"id", "string_col", "float64_col"}},
		{"half_columns", []string{"id", "bool_col", "int32_col", "int64_col", "float32_col"}},
	}

	for _, bc := range benchCases {
		opts := &ReaderOptions{Columns: bc.columns}

		b.Run(bc.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader, err := NewReader(bytes.NewReader(data), opts)
				if err != nil {
					b.Fatal(err)
				}

				for {
					_, err := reader.ReadChunk()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}

				_ = reader.Close()
			}
		})
	}
}

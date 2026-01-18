package io

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// generateFileSchemasVarying creates n file schemas with varying columns for benchmarking.
func generateFileSchemasVarying(n int) []FileSchema {
	schemas := make([]FileSchema, n)

	// Base columns
	baseColumns := []ColumnInfo{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
		{Name: "value", Type: dukdb.TYPE_DOUBLE},
		{Name: "timestamp", Type: dukdb.TYPE_TIMESTAMP},
	}

	for i := range n {
		path := replacePatternBench("file_%d.csv", i)

		// Create varying schemas by adding/removing columns
		// Pre-calculate capacity for extra columns
		extraCols := 0
		if i%3 == 0 {
			extraCols++
		}
		if i%5 == 0 {
			extraCols++
		}

		cols := make([]ColumnInfo, 0, len(baseColumns)+extraCols)
		cols = append(cols, baseColumns...)

		// Some files have extra columns
		if i%3 == 0 {
			cols = append(cols, ColumnInfo{Name: "extra_a", Type: dukdb.TYPE_VARCHAR})
		}
		if i%5 == 0 {
			cols = append(cols, ColumnInfo{Name: "extra_b", Type: dukdb.TYPE_INTEGER})
		}
		if i%7 == 0 && len(cols) > 3 {
			// Skip the 'value' column (index 2)
			cols = append(cols[:2], cols[3:]...)
		}

		schemas[i] = FileSchema{Path: path, Columns: cols}
	}

	return schemas
}

// generateFileSchemasIdentical creates n file schemas with identical columns for benchmarking.
func generateFileSchemasIdentical(n int) []FileSchema {
	schemas := make([]FileSchema, n)

	// Base columns
	baseColumns := []ColumnInfo{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
		{Name: "value", Type: dukdb.TYPE_DOUBLE},
		{Name: "timestamp", Type: dukdb.TYPE_TIMESTAMP},
	}

	for i := range n {
		path := replacePatternBench("file_%d.csv", i)
		schemas[i] = FileSchema{Path: path, Columns: baseColumns}
	}

	return schemas
}

// generateTypeMismatchSchemas creates schemas with type widening requirements.
func generateTypeMismatchSchemas(n int) []FileSchema {
	schemas := make([]FileSchema, n)

	for i := range n {
		path := replacePatternBench("file_%d.csv", i)

		// Alternate between INTEGER and BIGINT to require widening
		idType := dukdb.TYPE_INTEGER
		if i%2 == 0 {
			idType = dukdb.TYPE_BIGINT
		}

		// Alternate between FLOAT and DOUBLE
		valueType := dukdb.TYPE_FLOAT
		if i%3 == 0 {
			valueType = dukdb.TYPE_DOUBLE
		}

		cols := []ColumnInfo{
			{Name: "id", Type: idType},
			{Name: "name", Type: dukdb.TYPE_VARCHAR},
			{Name: "value", Type: valueType},
		}

		schemas[i] = FileSchema{Path: path, Columns: cols}
	}

	return schemas
}

// replacePatternBench replaces %d with the index value in a pattern string.
func replacePatternBench(pattern string, idx int) string {
	result := make([]byte, 0, len(pattern)+10)
	for i := 0; i < len(pattern); i++ {
		if i < len(pattern)-1 && pattern[i] == '%' && pattern[i+1] == 'd' {
			// Replace %d with the index
			result = appendIntDigits(result, idx)
			i++ // Skip the 'd'
		} else {
			result = append(result, pattern[i])
		}
	}
	return string(result)
}

// appendIntDigits appends the digits of a non-negative integer to a byte slice.
func appendIntDigits(buf []byte, n int) []byte {
	if n == 0 {
		return append(buf, '0')
	}
	digits := make([]byte, 0, 10)
	val := n
	for val > 0 {
		digits = append(digits, byte('0'+val%10))
		val /= 10
	}
	// Reverse digits and append
	result := buf
	for j := len(digits) - 1; j >= 0; j-- {
		result = append(result, digits[j])
	}
	return result
}

// BenchmarkSchemaMerge10Files benchmarks schema merging with 10 files with different schemas.
func BenchmarkSchemaMerge10Files(b *testing.B) {
	schemas := generateFileSchemasVarying(10)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := MergeSchemas(schemas, true)
		if err != nil {
			b.Fatalf("merge failed: %v", err)
		}
	}

	b.ReportMetric(float64(10), "schemas/op")
}

// BenchmarkSchemaMerge100Files benchmarks schema merging with 100 files.
func BenchmarkSchemaMerge100Files(b *testing.B) {
	schemas := generateFileSchemasVarying(100)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := MergeSchemas(schemas, true)
		if err != nil {
			b.Fatalf("merge failed: %v", err)
		}
	}

	b.ReportMetric(float64(100), "schemas/op")
}

// BenchmarkSchemaMergeIdentical benchmarks merging identical schemas (fast path).
func BenchmarkSchemaMergeIdentical(b *testing.B) {
	schemas := generateFileSchemasIdentical(100)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := MergeSchemas(schemas, true)
		if err != nil {
			b.Fatalf("merge failed: %v", err)
		}
	}

	b.ReportMetric(float64(100), "schemas/op")
}

// BenchmarkSchemaMergeByPosition benchmarks position-based schema merging.
func BenchmarkSchemaMergeByPosition(b *testing.B) {
	schemas := generateFileSchemasIdentical(100)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := MergeSchemas(schemas, false)
		if err != nil {
			b.Fatalf("merge failed: %v", err)
		}
	}

	b.ReportMetric(float64(100), "schemas/op")
}

// BenchmarkAlignChunk benchmarks chunk alignment with various scenarios.
func BenchmarkAlignChunk(b *testing.B) {
	// Create a source chunk
	sourceTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(sourceTypes, 1000)

	for row := range 1000 {
		chunk.SetValue(row, 0, int32(row))
		chunk.SetValue(row, 1, "test_value")
	}
	chunk.SetCount(1000)

	// Target has reordered columns
	mapping := []int{1, 0}
	targetTypes := []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_INTEGER}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = AlignChunk(chunk, mapping, targetTypes)
	}

	b.ReportMetric(float64(1000), "rows/op")
}

// BenchmarkAlignChunkWithMissing benchmarks chunk alignment with missing columns.
func BenchmarkAlignChunkWithMissing(b *testing.B) {
	// Create a source chunk with 2 columns
	sourceTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(sourceTypes, 1000)

	for row := range 1000 {
		chunk.SetValue(row, 0, int32(row))
		chunk.SetValue(row, 1, "test_value")
	}
	chunk.SetCount(1000)

	// Target has 4 columns, 2 missing from source
	mapping := []int{0, -1, 1, -1}
	targetTypes := []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BIGINT,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = AlignChunk(chunk, mapping, targetTypes)
	}

	b.ReportMetric(float64(1000), "rows/op")
}

// BenchmarkAlignChunkWithWidening benchmarks chunk alignment with type widening.
func BenchmarkAlignChunkWithWidening(b *testing.B) {
	// Create a source chunk with INTEGER
	sourceTypes := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_FLOAT}
	chunk := storage.NewDataChunkWithCapacity(sourceTypes, 1000)

	for row := range 1000 {
		chunk.SetValue(row, 0, int32(row))
		chunk.SetValue(row, 1, float32(row)*1.5)
	}
	chunk.SetCount(1000)

	// Target requires widening to BIGINT and DOUBLE
	mapping := []int{0, 1}
	targetTypes := []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_DOUBLE}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = AlignChunk(chunk, mapping, targetTypes)
	}

	b.ReportMetric(float64(1000), "rows/op")
}

// BenchmarkTypeWidening benchmarks type compatibility validation.
func BenchmarkTypeWidening(b *testing.B) {
	testCases := []struct {
		t1 dukdb.Type
		t2 dukdb.Type
	}{
		{dukdb.TYPE_INTEGER, dukdb.TYPE_INTEGER},
		{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
		{dukdb.TYPE_TINYINT, dukdb.TYPE_HUGEINT},
		{dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE},
		{dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE},
		{dukdb.TYPE_UTINYINT, dukdb.TYPE_INTEGER},
		{dukdb.TYPE_TIMESTAMP, dukdb.TYPE_TIMESTAMP_NS},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, tc := range testCases {
			_, _ = ValidateTypeCompatibility(tc.t1, tc.t2)
		}
	}

	b.ReportMetric(float64(len(testCases)), "types/op")
}

// BenchmarkTypeWideningManyTypes benchmarks type widening with many type combinations.
func BenchmarkTypeWideningManyTypes(b *testing.B) {
	schemas := generateTypeMismatchSchemas(100)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := MergeSchemas(schemas, true)
		if err != nil {
			b.Fatalf("merge failed: %v", err)
		}
	}

	b.ReportMetric(float64(100), "schemas/op")
}

// BenchmarkWidenType benchmarks the WidenType function directly.
func BenchmarkWidenType(b *testing.B) {
	typePairs := []struct {
		t1 dukdb.Type
		t2 dukdb.Type
	}{
		{dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT},
		{dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER},
		{dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
		{dukdb.TYPE_BIGINT, dukdb.TYPE_HUGEINT},
		{dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE},
		{dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT},
		{dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER},
		{dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, tp := range typePairs {
			WidenType(tp.t1, tp.t2)
		}
	}

	b.ReportMetric(float64(len(typePairs)), "pairs/op")
}

// BenchmarkConcatenateChunks benchmarks chunk concatenation.
func BenchmarkConcatenateChunks(b *testing.B) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE}

	// Create 10 chunks with 100 rows each
	chunks := make([]*storage.DataChunk, 10)
	for i := range 10 {
		chunk := storage.NewDataChunkWithCapacity(types, 100)
		for row := range 100 {
			chunk.SetValue(row, 0, int32(i*100+row))
			chunk.SetValue(row, 1, "test")
			chunk.SetValue(row, 2, float64(row)*1.5)
		}
		chunk.SetCount(100)
		chunks[i] = chunk
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = ConcatenateChunks(chunks)
	}

	b.ReportMetric(float64(1000), "rows/op")
}

// BenchmarkAddMetadataColumns benchmarks adding metadata columns.
func BenchmarkAddMetadataColumns(b *testing.B) {
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}
	chunk := storage.NewDataChunkWithCapacity(types, 1000)
	for row := range 1000 {
		chunk.SetValue(row, 0, int32(row))
		chunk.SetValue(row, 1, "data")
	}
	chunk.SetCount(1000)

	opts := &MultiFileOptions{
		Filename:      true,
		FileRowNumber: true,
		FileIndex:     true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = AddMetadataColumns(chunk, "/path/to/file.csv", 5, 0, opts)
	}

	b.ReportMetric(float64(1000), "rows/op")
}

// BenchmarkIsGlobPattern benchmarks glob pattern detection.
func BenchmarkIsGlobPattern(b *testing.B) {
	paths := []string{
		"/path/to/file.csv",
		"/path/to/*.csv",
		"/path/**/file.csv",
		"data/file[0-9].csv",
		"data/file?.csv",
		"simple.txt",
		"s3://bucket/prefix/data.parquet",
		"gs://bucket/path/to/files/**/*.json",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, p := range paths {
			IsGlobPattern(p)
		}
	}

	b.ReportMetric(float64(len(paths)), "paths/op")
}

// BenchmarkValidateMaxFiles benchmarks max files validation.
func BenchmarkValidateMaxFiles(b *testing.B) {
	counts := []int{100, 1000, 5000, 9999, 10000}
	opts := &MultiFileOptions{MaxFiles: 10000}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, count := range counts {
			_ = ValidateMaxFiles(count, opts)
		}
	}

	b.ReportMetric(float64(len(counts)), "checks/op")
}

// BenchmarkConvertValue benchmarks value conversion for type widening.
func BenchmarkConvertValue(b *testing.B) {
	testCases := []struct {
		val        any
		srcType    dukdb.Type
		targetType dukdb.Type
	}{
		{int32(42), dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT},
		{int8(42), dukdb.TYPE_TINYINT, dukdb.TYPE_BIGINT},
		{int32(42), dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE},
		{float32(3.14), dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE},
		{uint8(42), dukdb.TYPE_UTINYINT, dukdb.TYPE_UBIGINT},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for _, tc := range testCases {
			convertValue(tc.val, tc.srcType, tc.targetType)
		}
	}

	b.ReportMetric(float64(len(testCases)), "conversions/op")
}

// BenchmarkMultiFileReaderCreation benchmarks MultiFileReader creation.
func BenchmarkMultiFileReaderCreation(b *testing.B) {
	paths := make([]string, 1000)
	for i := range 1000 {
		paths[i] = replacePatternBench("file_%d.csv", i)
	}

	opts := DefaultMultiFileOptions()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := NewMultiFileReader(paths, opts, nil)
		if err != nil {
			b.Fatalf("creation failed: %v", err)
		}
	}

	b.ReportMetric(float64(1000), "paths/op")
}

// BenchmarkSchemaMergeResultTypes benchmarks getting types from merge result.
func BenchmarkSchemaMergeResultTypes(b *testing.B) {
	schemas := generateFileSchemasVarying(50)
	result, err := MergeSchemas(schemas, true)
	if err != nil {
		b.Fatalf("merge failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = result.Types()
	}

	b.ReportMetric(float64(len(result.Columns)), "columns/op")
}

// BenchmarkSchemaMergeResultNames benchmarks getting names from merge result.
func BenchmarkSchemaMergeResultNames(b *testing.B) {
	schemas := generateFileSchemasVarying(50)
	result, err := MergeSchemas(schemas, true)
	if err != nil {
		b.Fatalf("merge failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_ = result.Names()
	}

	b.ReportMetric(float64(len(result.Columns)), "columns/op")
}

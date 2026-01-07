// Package xlsx provides Excel XLSX file reading and writing capabilities for dukdb-go.
// This file contains performance benchmarks for the XLSX reader and writer.
package xlsx

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/storage"
	"github.com/xuri/excelize/v2"
)

// BenchmarkReader_SmallFile benchmarks reading a small XLSX file.
func BenchmarkReader_SmallFile(b *testing.B) {
	// Create a small test file: 100 rows x 5 columns
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "small.xlsx")

	createBenchmarkFile(b, path, 100, 5)

	b.ResetTimer()
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

		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReader_MediumFile benchmarks reading a medium XLSX file.
func BenchmarkReader_MediumFile(b *testing.B) {
	// Create a medium test file: 1000 rows x 10 columns
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "medium.xlsx")

	createBenchmarkFile(b, path, 1000, 10)

	b.ResetTimer()
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

		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReader_LargeFile benchmarks reading a large XLSX file.
func BenchmarkReader_LargeFile(b *testing.B) {
	// Create a large test file: 10000 rows x 20 columns
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "large.xlsx")

	createBenchmarkFile(b, path, 10000, 20)

	b.ResetTimer()
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

		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReader_FromMemory benchmarks reading XLSX from memory.
func BenchmarkReader_FromMemory(b *testing.B) {
	// Create test file in memory
	data := createBenchmarkFileInMemory(b, 1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, err := NewReader(bytes.NewReader(data), nil)
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

		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReader_TypeInference benchmarks type inference during reading.
func BenchmarkReader_TypeInference(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "typed.xlsx")

	// Create file with mixed types
	createMixedTypeBenchmarkFile(b, path, 1000)

	b.Run("with_inference", func(b *testing.B) {
		opts := DefaultReaderOptions()
		opts.InferTypes = true

		for i := 0; i < b.N; i++ {
			reader, err := NewReaderFromPath(path, opts)
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

			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("without_inference", func(b *testing.B) {
		opts := DefaultReaderOptions()
		opts.InferTypes = false

		for i := 0; i < b.N; i++ {
			reader, err := NewReaderFromPath(path, opts)
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

			if err := reader.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWriter_SmallFile benchmarks writing a small XLSX file.
func BenchmarkWriter_SmallFile(b *testing.B) {
	chunks := generateBenchmarkChunks(100, 5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.SetSchema(generateBenchmarkColumnNames(5)); err != nil {
			b.Fatal(err)
		}

		for _, chunk := range chunks {
			if err := writer.WriteChunk(chunk); err != nil {
				b.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriter_MediumFile benchmarks writing a medium XLSX file.
func BenchmarkWriter_MediumFile(b *testing.B) {
	chunks := generateBenchmarkChunks(1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.SetSchema(generateBenchmarkColumnNames(10)); err != nil {
			b.Fatal(err)
		}

		for _, chunk := range chunks {
			if err := writer.WriteChunk(chunk); err != nil {
				b.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriter_LargeFile benchmarks writing a large XLSX file.
func BenchmarkWriter_LargeFile(b *testing.B) {
	chunks := generateBenchmarkChunks(10000, 20)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.SetSchema(generateBenchmarkColumnNames(20)); err != nil {
			b.Fatal(err)
		}

		for _, chunk := range chunks {
			if err := writer.WriteChunk(chunk); err != nil {
				b.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriter_ToFile benchmarks writing XLSX directly to file.
func BenchmarkWriter_ToFile(b *testing.B) {
	chunks := generateBenchmarkChunks(1000, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmpFile, err := os.CreateTemp("", "bench_*.xlsx")
		if err != nil {
			b.Fatal(err)
		}
		path := tmpFile.Name()
		_ = tmpFile.Close()

		writer, err := NewWriterToPath(path, nil)
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.SetSchema(generateBenchmarkColumnNames(10)); err != nil {
			_ = os.Remove(path)
			b.Fatal(err)
		}

		for _, chunk := range chunks {
			if err := writer.WriteChunk(chunk); err != nil {
				_ = os.Remove(path)
				b.Fatal(err)
			}
		}

		if err := writer.Close(); err != nil {
			_ = os.Remove(path)
			b.Fatal(err)
		}

		_ = os.Remove(path)
	}
}

// BenchmarkWriter_AutoWidth benchmarks auto-width calculation impact.
func BenchmarkWriter_AutoWidth(b *testing.B) {
	chunks := generateBenchmarkChunks(1000, 10)

	b.Run("with_autowidth", func(b *testing.B) {
		opts := DefaultWriterOptions()
		opts.AutoWidth = true

		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			writer, err := NewWriter(&buf, opts)
			if err != nil {
				b.Fatal(err)
			}

			if err := writer.SetSchema(generateBenchmarkColumnNames(10)); err != nil {
				b.Fatal(err)
			}

			for _, chunk := range chunks {
				if err := writer.WriteChunk(chunk); err != nil {
					b.Fatal(err)
				}
			}

			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("without_autowidth", func(b *testing.B) {
		opts := DefaultWriterOptions()
		opts.AutoWidth = false

		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			writer, err := NewWriter(&buf, opts)
			if err != nil {
				b.Fatal(err)
			}

			if err := writer.SetSchema(generateBenchmarkColumnNames(10)); err != nil {
				b.Fatal(err)
			}

			for _, chunk := range chunks {
				if err := writer.WriteChunk(chunk); err != nil {
					b.Fatal(err)
				}
			}

			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRoundTrip benchmarks full read-write-read cycle.
func BenchmarkRoundTrip(b *testing.B) {
	chunks := generateBenchmarkChunks(1000, 10)

	for i := 0; i < b.N; i++ {
		// Write
		var buf bytes.Buffer
		writer, err := NewWriter(&buf, nil)
		if err != nil {
			b.Fatal(err)
		}

		if err := writer.SetSchema(generateBenchmarkColumnNames(10)); err != nil {
			b.Fatal(err)
		}

		for _, chunk := range chunks {
			if err := writer.WriteChunk(chunk); err != nil {
				b.Fatal(err)
			}
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

		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// Helper functions for creating benchmark data

func createBenchmarkFile(b *testing.B, path string, rows, cols int) {
	b.Helper()

	f := excelize.NewFile()

	// Header row
	for col := 0; col < cols; col++ {
		cell := CellAddress(col, 1)
		if err := f.SetCellValue("Sheet1", cell, IndexToColumnLetters(col)); err != nil {
			b.Fatal(err)
		}
	}

	// Data rows
	for row := 2; row <= rows+1; row++ {
		for col := 0; col < cols; col++ {
			cell := CellAddress(col, row)
			if err := f.SetCellValue("Sheet1", cell, (row-1)*cols+col); err != nil {
				b.Fatal(err)
			}
		}
	}

	if err := f.SaveAs(path); err != nil {
		b.Fatal(err)
	}

	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
}

func createBenchmarkFileInMemory(b *testing.B, rows, cols int) []byte {
	b.Helper()

	f := excelize.NewFile()

	// Header row
	for col := 0; col < cols; col++ {
		cell := CellAddress(col, 1)
		if err := f.SetCellValue("Sheet1", cell, IndexToColumnLetters(col)); err != nil {
			b.Fatal(err)
		}
	}

	// Data rows
	for row := 2; row <= rows+1; row++ {
		for col := 0; col < cols; col++ {
			cell := CellAddress(col, row)
			if err := f.SetCellValue("Sheet1", cell, (row-1)*cols+col); err != nil {
				b.Fatal(err)
			}
		}
	}

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		b.Fatal(err)
	}

	if err := f.Close(); err != nil {
		b.Fatal(err)
	}

	return buf.Bytes()
}

func createMixedTypeBenchmarkFile(b *testing.B, path string, rows int) {
	b.Helper()

	f := excelize.NewFile()

	// Headers
	if err := f.SetCellValue("Sheet1", "A1", "StringCol"); err != nil {
		b.Fatal(err)
	}
	if err := f.SetCellValue("Sheet1", "B1", "IntCol"); err != nil {
		b.Fatal(err)
	}
	if err := f.SetCellValue("Sheet1", "C1", "FloatCol"); err != nil {
		b.Fatal(err)
	}
	if err := f.SetCellValue("Sheet1", "D1", "BoolCol"); err != nil {
		b.Fatal(err)
	}

	// Data
	for row := 2; row <= rows+1; row++ {
		if err := f.SetCellValue("Sheet1", CellAddress(0, row), "text"+string(rune('A'+row%26))); err != nil {
			b.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", CellAddress(1, row), row*10); err != nil {
			b.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", CellAddress(2, row), float64(row)*1.5); err != nil {
			b.Fatal(err)
		}
		if err := f.SetCellValue("Sheet1", CellAddress(3, row), row%2 == 0); err != nil {
			b.Fatal(err)
		}
	}

	if err := f.SaveAs(path); err != nil {
		b.Fatal(err)
	}

	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
}

func generateBenchmarkChunks(totalRows, cols int) []*storage.DataChunk {
	types := make([]dukdb.Type, cols)
	for i := range types {
		types[i] = dukdb.TYPE_VARCHAR
	}

	chunks := make([]*storage.DataChunk, 0)
	rowsRemaining := totalRows
	chunkSize := storage.StandardVectorSize

	for rowsRemaining > 0 {
		size := chunkSize
		if rowsRemaining < size {
			size = rowsRemaining
		}

		chunk := storage.NewDataChunkWithCapacity(types, size)

		// Fill the chunk with dummy data
		for rowIdx := 0; rowIdx < size; rowIdx++ {
			for colIdx := 0; colIdx < cols; colIdx++ {
				vec := chunk.GetVector(colIdx)
				if vec != nil {
					vec.SetValue(rowIdx, "data")
				}
			}
		}
		chunk.SetCount(size)

		chunks = append(chunks, chunk)
		rowsRemaining -= size
	}

	return chunks
}

func generateBenchmarkColumnNames(cols int) []string {
	names := make([]string, cols)
	for i := range names {
		names[i] = IndexToColumnLetters(i)
	}
	return names
}

package parquet

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

// bufferReaderAt wraps a bytes.Reader to implement io.ReaderAt with size
type bufferReaderAt struct {
	*bytes.Reader
	size int64
}

func newBufferReaderAt(data []byte) *bufferReaderAt {
	return &bufferReaderAt{
		Reader: bytes.NewReader(data),
		size:   int64(len(data)),
	}
}

func (b *bufferReaderAt) Size() int64 {
	return b.size
}

// TestParquetGoLibraryAccessible verifies that the parquet-go library is properly
// imported and accessible. This is a basic smoke test to confirm the dependency works.
func TestParquetGoLibraryAccessible(t *testing.T) {
	// Define a simple test schema using a struct
	type TestRecord struct {
		ID   int64  `parquet:"id"`
		Name string `parquet:"name"`
	}

	// Create a buffer to write parquet data
	buf := new(bytes.Buffer)

	// Create a parquet writer
	writer := parquet.NewGenericWriter[TestRecord](buf)

	// Write some test records
	records := []TestRecord{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	n, err := writer.Write(records)
	require.NoError(t, err)
	require.Equal(t, 3, n)

	// Close the writer to flush the data
	err = writer.Close()
	require.NoError(t, err)

	// Verify we have parquet data written
	require.Greater(t, buf.Len(), 0, "expected parquet data to be written")

	// Create a parquet reader from the buffer
	data := buf.Bytes()
	reader := parquet.NewGenericReader[TestRecord](newBufferReaderAt(data))
	defer func() {
		_ = reader.Close()
	}()

	// Read back the records
	readRecords := make([]TestRecord, 3)
	n, err = reader.Read(readRecords)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 3, n)

	// Verify the data matches
	require.Equal(t, records, readRecords)
}

// TestParquetGoSchemaCreation verifies that we can create schemas programmatically
func TestParquetGoSchemaCreation(t *testing.T) {
	// Create a simple schema using the parquet-go schema builder
	type SchemaTestRecord struct {
		IntCol    int32   `parquet:"int_col"`
		FloatCol  float64 `parquet:"float_col"`
		StringCol string  `parquet:"string_col"`
		BoolCol   bool    `parquet:"bool_col"`
	}

	// Get the schema from the struct
	schema := parquet.SchemaOf(new(SchemaTestRecord))
	require.NotNil(t, schema)

	// Verify we have the expected number of fields
	fields := schema.Fields()
	require.Len(t, fields, 4)

	// Verify field names
	fieldNames := make([]string, len(fields))
	for i, f := range fields {
		fieldNames[i] = f.Name()
	}
	require.Contains(t, fieldNames, "int_col")
	require.Contains(t, fieldNames, "float_col")
	require.Contains(t, fieldNames, "string_col")
	require.Contains(t, fieldNames, "bool_col")
}

// TestParquetGoCompressionSupport verifies that compression options are available
func TestParquetGoCompressionSupport(t *testing.T) {
	type Record struct {
		Value int64 `parquet:"value"`
	}

	// Test with different compression codecs using their names
	testCases := []struct {
		name  string
		codec parquet.WriterOption
	}{
		{"Uncompressed", parquet.Compression(&parquet.Uncompressed)},
		{"Snappy", parquet.Compression(&parquet.Snappy)},
		{"Gzip", parquet.Compression(&parquet.Gzip)},
		{"Zstd", parquet.Compression(&parquet.Zstd)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Create writer with compression
			writer := parquet.NewGenericWriter[Record](buf, tc.codec)

			records := []Record{{Value: 42}, {Value: 100}}
			_, err := writer.Write(records)
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Verify data was written
			require.Greater(t, buf.Len(), 0)

			// Read back and verify
			data := buf.Bytes()
			reader := parquet.NewGenericReader[Record](newBufferReaderAt(data))
			defer func() {
				_ = reader.Close()
			}()

			readRecords := make([]Record, 2)
			n, err := reader.Read(readRecords)
			if err != nil && err != io.EOF {
				require.NoError(t, err)
			}
			require.Equal(t, 2, n)
			require.Equal(t, records, readRecords)
		})
	}
}

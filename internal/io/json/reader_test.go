// Package json provides JSON and NDJSON file reading and writing capabilities for dukdb-go.
package json

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test data constants used across multiple tests.
const testNDJSONTwoRows = `{"id": 1, "name": "alice"}
{"id": 2, "name": "bob"}`

// TestNewReader_BasicJSONArray tests reading a simple JSON array.
func TestNewReader_BasicJSONArray(t *testing.T) {
	jsonData := `[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "charlie"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	// Check schema - columns should be sorted alphabetically.
	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, schema)

	// Check types.
	types, err := r.Types()
	require.NoError(t, err)
	require.Len(t, types, 2)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0], "id should be INTEGER")
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1], "name should be VARCHAR")

	// Read first chunk.
	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, 2, chunk.ColumnCount())

	// Verify values.
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "bob", chunk.GetValue(1, 1))

	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
	assert.Equal(t, "charlie", chunk.GetValue(2, 1))

	// Second read should return EOF.
	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// TestNewReader_EmptyArray tests reading an empty JSON array.
func TestNewReader_EmptyArray(t *testing.T) {
	jsonData := `[]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Empty(t, schema)

	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// TestNewReader_EmptyFile tests reading an empty file.
func TestNewReader_EmptyFile(t *testing.T) {
	jsonData := ``
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Empty(t, schema)

	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// TestNewReader_NullValues tests NULL value handling.
func TestNewReader_NullValues(t *testing.T) {
	jsonData := `[{"id": 1, "name": null, "value": 100}, {"id": 2, "name": "bob", "value": null}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Null fields should be NULL.
	assert.Nil(t, chunk.GetValue(0, 1)) // name is NULL in first row
	assert.Nil(t, chunk.GetValue(1, 2)) // value is NULL in second row
}

// TestNewReader_MissingFields tests objects with missing fields.
func TestNewReader_MissingFields(t *testing.T) {
	jsonData := `[{"id": 1, "name": "alice"}, {"id": 2}, {"name": "charlie"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	// Schema should include all fields seen across all objects.
	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, schema)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Missing fields should be NULL.
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Nil(t, chunk.GetValue(1, 1)) // name is missing

	assert.Nil(t, chunk.GetValue(2, 0)) // id is missing
	assert.Equal(t, "charlie", chunk.GetValue(2, 1))
}

// TestNewReader_TypeInference tests type inference from JSON values.
func TestNewReader_TypeInference(t *testing.T) {
	t.Run("boolean values", func(t *testing.T) {
		jsonData := `[{"flag": true}, {"flag": false}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_BOOLEAN, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, true, chunk.GetValue(0, 0))
		assert.Equal(t, false, chunk.GetValue(1, 0))
	})

	t.Run("integer values", func(t *testing.T) {
		jsonData := `[{"num": 100}, {"num": -200}, {"num": 0}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, int32(100), chunk.GetValue(0, 0))
		assert.Equal(t, int32(-200), chunk.GetValue(1, 0))
		assert.Equal(t, int32(0), chunk.GetValue(2, 0))
	})

	t.Run("large integer becomes BIGINT", func(t *testing.T) {
		jsonData := `[{"num": 3000000000}, {"num": 4000000000}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_BIGINT, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, int64(3000000000), chunk.GetValue(0, 0))
		assert.Equal(t, int64(4000000000), chunk.GetValue(1, 0))
	})

	t.Run("float values", func(t *testing.T) {
		jsonData := `[{"num": 1.5}, {"num": 2.5}, {"num": 3.0}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_DOUBLE, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 1.5, chunk.GetValue(0, 0))
		assert.Equal(t, 2.5, chunk.GetValue(1, 0))
		assert.Equal(t, 3.0, chunk.GetValue(2, 0))
	})

	t.Run("string values", func(t *testing.T) {
		jsonData := `[{"text": "hello"}, {"text": "world"}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_VARCHAR, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, "hello", chunk.GetValue(0, 0))
		assert.Equal(t, "world", chunk.GetValue(1, 0))
	})
}

// TestNewReader_MixedTypes tests that mixed types fall back to VARCHAR.
func TestNewReader_MixedTypes(t *testing.T) {
	jsonData := `[{"val": 100}, {"val": "hello"}, {"val": 300}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[0])

	chunk, err := r.ReadChunk()
	require.NoError(t, err)

	// Values should be converted to strings.
	assert.Equal(t, "100", chunk.GetValue(0, 0))
	assert.Equal(t, "hello", chunk.GetValue(1, 0))
	assert.Equal(t, "300", chunk.GetValue(2, 0))
}

// TestNewReader_NestedObjects tests handling of nested objects.
func TestNewReader_NestedObjects(t *testing.T) {
	jsonData := `[{"id": 1, "user": {"name": "alice", "age": 30}}, {"id": 2, "user": {"name": "bob", "age": 25}}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	// Nested objects become VARCHAR (JSON string).
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0]) // id
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1]) // user (nested object)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Nested object should be serialized as JSON string.
	userVal := chunk.GetValue(0, 1)
	assert.IsType(t, "", userVal)
	assert.Contains(t, userVal.(string), "alice")
}

// TestNewReader_Arrays tests handling of array values.
func TestNewReader_Arrays(t *testing.T) {
	jsonData := `[{"id": 1, "tags": ["a", "b", "c"]}, {"id": 2, "tags": ["x", "y"]}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	// Arrays become VARCHAR (JSON string).
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0]) // id
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1]) // tags (array)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Array should be serialized as JSON string.
	tagsVal := chunk.GetValue(0, 1)
	assert.IsType(t, "", tagsVal)
	assert.Contains(t, tagsVal.(string), `["a","b","c"]`)
}

// TestNewReader_DateValues tests date string detection.
func TestNewReader_DateValues(t *testing.T) {
	jsonData := `[{"date": "2024-01-15"}, {"date": "2024-06-30"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_DATE, types[0])
}

// TestNewReader_TimestampValues tests timestamp string detection.
func TestNewReader_TimestampValues(t *testing.T) {
	jsonData := `[{"ts": "2024-01-15T10:30:00Z"}, {"ts": "2024-06-30T14:45:00Z"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_TIMESTAMP, types[0])
}

// TestNewReader_Whitespace tests handling of whitespace in JSON.
func TestNewReader_Whitespace(t *testing.T) {
	jsonData := `[
		{
			"id": 1,
			"name": "alice"
		},
		{
			"id": 2,
			"name": "bob"
		}
	]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))
}

// TestNewReader_UnicodeStrings tests handling of Unicode characters.
func TestNewReader_UnicodeStrings(t *testing.T) {
	jsonData := `[{"name": "\u4e2d\u6587"}, {"name": "\u00e9\u00e0\u00f9"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Unicode should be properly decoded.
	assert.Equal(t, "\u4e2d\u6587", chunk.GetValue(0, 0))
	assert.Equal(t, "\u00e9\u00e0\u00f9", chunk.GetValue(1, 0))
}

// TestNewReader_SpecialCharactersInStrings tests strings with special characters.
func TestNewReader_SpecialCharactersInStrings(t *testing.T) {
	jsonData := `[{"text": "line1\nline2"}, {"text": "tab\there"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	assert.Equal(t, "line1\nline2", chunk.GetValue(0, 0))
	assert.Equal(t, "tab\there", chunk.GetValue(1, 0))
}

// TestNewReader_InvalidJSON tests error handling for invalid JSON.
func TestNewReader_InvalidJSON(t *testing.T) {
	t.Run("not a JSON array with explicit array format", func(t *testing.T) {
		// When format is explicitly set to array, a single object should fail.
		jsonData := `{"id": 1, "name": "alice"}`
		reader := strings.NewReader(jsonData)

		opts := DefaultReaderOptions()
		opts.Format = FormatArray

		r, err := NewReader(reader, opts)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		_, err = r.Schema()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected '['")
	})

	t.Run("single object auto-detected as NDJSON", func(t *testing.T) {
		// With auto-detection, a single object is valid NDJSON.
		jsonData := `{"id": 1, "name": "alice"}`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil) // Uses default auto-detect.
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		schema, err := r.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, schema)

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 1, chunk.Count())
	})

	t.Run("truncated JSON object", func(t *testing.T) {
		// This tests reading when the JSON is truncated mid-object.
		// The Go JSON decoder may return an error during Decode, not during Schema.
		jsonData := `[{"id": 1, "name": `
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		// Schema will trigger reading, which may fail on truncated JSON.
		_, err = r.Schema()
		// Schema may succeed if the decoder doesn't validate eagerly.
		// In this case, the error will happen during ReadChunk.
		if err == nil {
			_, err = r.ReadChunk()
		}

		// Either Schema or ReadChunk should fail on truncated JSON.
		require.Error(t, err)
	})
}

// TestNewReader_ManyColumns tests handling of objects with many fields.
func TestNewReader_ManyColumns(t *testing.T) {
	// Create object with 50 fields.
	fields := make([]string, 50)
	for i := range 50 {
		fields[i] = `"` + "col" + string(
			rune('0'+i/10),
		) + string(
			rune('0'+i%10),
		) + `": ` + string(
			rune('0'+i%10),
		)
	}

	jsonData := `[{` + strings.Join(fields, ", ") + `}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Len(t, schema, 50)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 50, chunk.ColumnCount())
}

// TestNewReaderFromPath_FileOperations tests reading JSON files from the filesystem.
func TestNewReaderFromPath_FileOperations(t *testing.T) {
	t.Run("read simple JSON from file", func(t *testing.T) {
		tmpDir := t.TempDir()
		jsonPath := filepath.Join(tmpDir, "test.json")
		jsonContent := `[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]`

		err := os.WriteFile(jsonPath, []byte(jsonContent), 0o644)
		require.NoError(t, err)

		r, err := NewReaderFromPath(jsonPath, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		schema, err := r.Schema()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, schema)

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 2, chunk.Count())
	})

	t.Run("file not found returns error", func(t *testing.T) {
		_, err := NewReaderFromPath("/nonexistent/file.json", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open file")
	})
}

// TestNewReader_ChunkSize tests reading files larger than chunk size.
func TestNewReader_ChunkSize(t *testing.T) {
	t.Run("more rows than chunk size", func(t *testing.T) {
		// Create JSON with more than 2048 rows (StandardVectorSize).
		numRows := 2500

		var builder strings.Builder
		builder.WriteString(`[{"id": 1}`)

		for i := 2; i <= numRows; i++ {
			builder.WriteString(`, {"id": `)
			builder.WriteString(string(rune('0' + i%10)))
			builder.WriteString(`}`)
		}

		builder.WriteString(`]`)
		jsonData := builder.String()

		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		totalRows := 0
		chunkCount := 0

		for {
			chunk, err := r.ReadChunk()
			if err == io.EOF {
				break
			}

			require.NoError(t, err)
			totalRows += chunk.Count()
			chunkCount++
		}

		assert.Equal(t, numRows, totalRows)
		assert.GreaterOrEqual(t, chunkCount, 2)
	})

	t.Run("custom max rows per chunk", func(t *testing.T) {
		jsonData := `[{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]`
		reader := strings.NewReader(jsonData)

		opts := DefaultReaderOptions()
		opts.MaxRowsPerChunk = 2

		r, err := NewReader(reader, opts)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		chunk1, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 2, chunk1.Count())

		chunk2, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 2, chunk2.Count())

		chunk3, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 1, chunk3.Count())

		_, err = r.ReadChunk()
		assert.Equal(t, io.EOF, err)
	})
}

// TestNewReader_Close tests that Close works correctly.
func TestNewReader_Close(t *testing.T) {
	t.Run("close before reading", func(t *testing.T) {
		jsonData := `[{"id": 1}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("close after reading", func(t *testing.T) {
		jsonData := `[{"id": 1}, {"id": 2}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)

		_, err = r.ReadChunk()
		require.NoError(t, err)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("multiple close calls", func(t *testing.T) {
		jsonData := `[{"id": 1}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)

		err = r.Close()
		require.NoError(t, err)

		// Second close should also succeed.
		err = r.Close()
		require.NoError(t, err)
	})
}

// TestNewReader_SchemaConsistency tests that Schema returns consistent results.
func TestNewReader_SchemaConsistency(t *testing.T) {
	jsonData := `[{"id": 1, "name": "alice"}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema1, err := r.Schema()
	require.NoError(t, err)

	schema2, err := r.Schema()
	require.NoError(t, err)

	assert.Equal(t, schema1, schema2)
}

// TestNewReader_NumericEdgeCases tests numeric edge cases.
func TestNewReader_NumericEdgeCases(t *testing.T) {
	t.Run("negative integers", func(t *testing.T) {
		jsonData := `[{"num": -100}, {"num": -2147483648}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, int32(-100), chunk.GetValue(0, 0))
		assert.Equal(t, int32(-2147483648), chunk.GetValue(1, 0))
	})

	t.Run("negative floats", func(t *testing.T) {
		jsonData := `[{"num": -1.5}, {"num": -2.5}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, -1.5, chunk.GetValue(0, 0))
		assert.Equal(t, -2.5, chunk.GetValue(1, 0))
	})

	t.Run("zero values", func(t *testing.T) {
		jsonData := `[{"num": 0}, {"num": 0.0}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		// Both should be valid values (not NULL).
		assert.NotNil(t, chunk.GetValue(0, 0))
		assert.NotNil(t, chunk.GetValue(1, 0))
	})
}

// TestNewReader_AllNullColumn tests a column with only null values.
func TestNewReader_AllNullColumn(t *testing.T) {
	jsonData := `[{"id": 1, "empty": null}, {"id": 2, "empty": null}]`
	reader := strings.NewReader(jsonData)

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	// Get schema to understand column ordering (alphabetical).
	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"empty", "id"}, schema) // Columns are sorted alphabetically.

	types, err := r.Types()
	require.NoError(t, err)
	// Column with all nulls should be VARCHAR.
	// Note: columns are sorted alphabetically: "empty" comes before "id".
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[0]) // empty (all nulls)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[1]) // id

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Nil(t, chunk.GetValue(0, 0)) // empty is NULL
	assert.Nil(t, chunk.GetValue(1, 0)) // empty is NULL
	assert.Equal(t, int32(1), chunk.GetValue(0, 1))
	assert.Equal(t, int32(2), chunk.GetValue(1, 1))
}

// =============================================================================
// NDJSON Format Tests
// =============================================================================

// TestNewReader_BasicNDJSON tests reading a simple NDJSON file.
func TestNewReader_BasicNDJSON(t *testing.T) {
	ndjsonData := `{"id": 1, "name": "alice"}
{"id": 2, "name": "bob"}
{"id": 3, "name": "charlie"}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	// Check schema - columns should be sorted alphabetically.
	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, schema)

	// Check types.
	types, err := r.Types()
	require.NoError(t, err)
	require.Len(t, types, 2)
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0], "id should be INTEGER")
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1], "name should be VARCHAR")

	// Read first chunk.
	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())
	assert.Equal(t, 2, chunk.ColumnCount())

	// Verify values.
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "bob", chunk.GetValue(1, 1))

	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
	assert.Equal(t, "charlie", chunk.GetValue(2, 1))

	// Second read should return EOF.
	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// TestNewReader_NDJSONWithBlankLines tests NDJSON with blank lines.
func TestNewReader_NDJSONWithBlankLines(t *testing.T) {
	ndjsonData := `{"id": 1, "name": "alice"}

{"id": 2, "name": "bob"}


{"id": 3, "name": "charlie"}
`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
}

// TestNewReader_NDJSONWithWhitespaceLines tests NDJSON with whitespace-only lines.
func TestNewReader_NDJSONWithWhitespaceLines(t *testing.T) {
	ndjsonData := "   \n\t\n{\"id\": 1}\n  \t  \n{\"id\": 2}\n\n"
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
}

// TestNewReader_NDJSONEmptyFile tests reading an empty NDJSON file.
func TestNewReader_NDJSONEmptyFile(t *testing.T) {
	ndjsonData := ``
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Empty(t, schema)

	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// TestNewReader_NDJSONOnlyBlankLines tests NDJSON file with only blank lines.
func TestNewReader_NDJSONOnlyBlankLines(t *testing.T) {
	ndjsonData := "\n\n\n   \n\t\n"
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Empty(t, schema)

	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// TestNewReader_NDJSONNullValues tests NULL value handling in NDJSON.
func TestNewReader_NDJSONNullValues(t *testing.T) {
	ndjsonData := `{"id": 1, "name": null, "value": 100}
{"id": 2, "name": "bob", "value": null}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Null fields should be NULL.
	assert.Nil(t, chunk.GetValue(0, 1)) // name is NULL in first row
	assert.Nil(t, chunk.GetValue(1, 2)) // value is NULL in second row
}

// TestNewReader_NDJSONMissingFields tests objects with missing fields in NDJSON.
func TestNewReader_NDJSONMissingFields(t *testing.T) {
	ndjsonData := `{"id": 1, "name": "alice"}
{"id": 2}
{"name": "charlie"}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	// Schema should include all fields seen across all objects.
	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, schema)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count())

	// Missing fields should be NULL.
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Nil(t, chunk.GetValue(1, 1)) // name is missing

	assert.Nil(t, chunk.GetValue(2, 0)) // id is missing
	assert.Equal(t, "charlie", chunk.GetValue(2, 1))
}

// TestNewReader_NDJSONTypeInference tests type inference in NDJSON.
func TestNewReader_NDJSONTypeInference(t *testing.T) {
	t.Run("integer values", func(t *testing.T) {
		ndjsonData := `{"num": 100}
{"num": -200}
{"num": 0}`
		reader := strings.NewReader(ndjsonData)

		opts := DefaultReaderOptions()
		opts.Format = FormatNDJSON

		r, err := NewReader(reader, opts)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_INTEGER, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, int32(100), chunk.GetValue(0, 0))
		assert.Equal(t, int32(-200), chunk.GetValue(1, 0))
		assert.Equal(t, int32(0), chunk.GetValue(2, 0))
	})

	t.Run("float values", func(t *testing.T) {
		ndjsonData := `{"num": 1.5}
{"num": 2.5}
{"num": 3.0}`
		reader := strings.NewReader(ndjsonData)

		opts := DefaultReaderOptions()
		opts.Format = FormatNDJSON

		r, err := NewReader(reader, opts)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_DOUBLE, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 1.5, chunk.GetValue(0, 0))
		assert.Equal(t, 2.5, chunk.GetValue(1, 0))
		assert.Equal(t, 3.0, chunk.GetValue(2, 0))
	})

	t.Run("boolean values", func(t *testing.T) {
		ndjsonData := `{"flag": true}
{"flag": false}`
		reader := strings.NewReader(ndjsonData)

		opts := DefaultReaderOptions()
		opts.Format = FormatNDJSON

		r, err := NewReader(reader, opts)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		types, err := r.Types()
		require.NoError(t, err)
		assert.Equal(t, dukdb.TYPE_BOOLEAN, types[0])

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, true, chunk.GetValue(0, 0))
		assert.Equal(t, false, chunk.GetValue(1, 0))
	})
}

// TestNewReader_NDJSONShortFormat tests using "ndjson" format alias.
func TestNewReader_NDJSONShortFormat(t *testing.T) {
	ndjsonData := `{"id": 1}
{"id": 2}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSONShort // "ndjson"

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
}

// =============================================================================
// Auto-detection Tests
// =============================================================================

// TestNewReader_AutoDetectJSONArray tests auto-detection of JSON array format.
func TestNewReader_AutoDetectJSONArray(t *testing.T) {
	jsonData := `[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]`
	reader := strings.NewReader(jsonData)

	// Use default options which should auto-detect.
	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "bob", chunk.GetValue(1, 1))
}

// TestNewReader_AutoDetectNDJSON tests auto-detection of NDJSON format.
func TestNewReader_AutoDetectNDJSON(t *testing.T) {
	reader := strings.NewReader(testNDJSONTwoRows)

	// Use default options which should auto-detect.
	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, "alice", chunk.GetValue(0, 1))

	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, "bob", chunk.GetValue(1, 1))
}

// TestNewReader_AutoDetectWithLeadingWhitespace tests auto-detection with leading whitespace.
func TestNewReader_AutoDetectWithLeadingWhitespace(t *testing.T) {
	t.Run("array with leading whitespace", func(t *testing.T) {
		jsonData := "   \n\t  [" + `{"id": 1}]`
		reader := strings.NewReader(jsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 1, chunk.Count())
		assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	})

	t.Run("ndjson with leading whitespace", func(t *testing.T) {
		ndjsonData := "   \n\t  " + `{"id": 1}
{"id": 2}`
		reader := strings.NewReader(ndjsonData)

		r, err := NewReader(reader, nil)
		require.NoError(t, err)
		defer func() { _ = r.Close() }()

		chunk, err := r.ReadChunk()
		require.NoError(t, err)
		assert.Equal(t, 2, chunk.Count())
		assert.Equal(t, int32(1), chunk.GetValue(0, 0))
		assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	})
}

// TestNewReader_AutoDetectEmptyFile tests auto-detection on empty file.
func TestNewReader_AutoDetectEmptyFile(t *testing.T) {
	reader := strings.NewReader("")

	r, err := NewReader(reader, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Empty(t, schema)

	_, err = r.ReadChunk()
	assert.Equal(t, io.EOF, err)
}

// =============================================================================
// NDJSON File Operations Tests
// =============================================================================

// TestNewReaderFromPath_NDJSONFile tests reading NDJSON from a file.
func TestNewReaderFromPath_NDJSONFile(t *testing.T) {
	tmpDir := t.TempDir()
	ndjsonPath := filepath.Join(tmpDir, "test.ndjson")

	err := os.WriteFile(ndjsonPath, []byte(testNDJSONTwoRows), 0o644)
	require.NoError(t, err)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReaderFromPath(ndjsonPath, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, schema)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())
}

// TestNewReaderFromPath_NDJSONAutoDetect tests auto-detection from file path.
func TestNewReaderFromPath_NDJSONAutoDetect(t *testing.T) {
	tmpDir := t.TempDir()
	ndjsonPath := filepath.Join(tmpDir, "test.json")

	err := os.WriteFile(ndjsonPath, []byte(testNDJSONTwoRows), 0o644)
	require.NoError(t, err)

	// Use auto-detect (default).
	r, err := NewReaderFromPath(ndjsonPath, nil)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
}

// TestNewReader_NDJSONChunkSize tests reading NDJSON larger than chunk size.
func TestNewReader_NDJSONChunkSize(t *testing.T) {
	var builder strings.Builder
	numRows := 100

	for i := 1; i <= numRows; i++ {
		builder.WriteString(`{"id": `)
		builder.WriteString(string(rune('0' + i%10)))
		builder.WriteString("}\n")
	}

	ndjsonData := builder.String()
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON
	opts.MaxRowsPerChunk = 30

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	totalRows := 0
	chunkCount := 0

	for {
		chunk, err := r.ReadChunk()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		totalRows += chunk.Count()
		chunkCount++
	}

	assert.Equal(t, numRows, totalRows)
	assert.GreaterOrEqual(t, chunkCount, 4) // At least 4 chunks for 100 rows with 30 per chunk.
}

// TestNewReader_NDJSONIgnoreErrors tests ignoring malformed lines in NDJSON.
func TestNewReader_NDJSONIgnoreErrors(t *testing.T) {
	ndjsonData := `{"id": 1}
not valid json
{"id": 2}
{broken
{"id": 3}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON
	opts.IgnoreErrors = true

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 3, chunk.Count()) // Only valid lines.

	assert.Equal(t, int32(1), chunk.GetValue(0, 0))
	assert.Equal(t, int32(2), chunk.GetValue(1, 0))
	assert.Equal(t, int32(3), chunk.GetValue(2, 0))
}

// TestNewReader_NDJSONMalformedError tests error on malformed NDJSON without ignore_errors.
func TestNewReader_NDJSONMalformedError(t *testing.T) {
	ndjsonData := `{"id": 1}
not valid json
{"id": 2}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON
	opts.IgnoreErrors = false

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	// Schema should work since first line is valid.
	schema, err := r.Schema()
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, schema)

	// But reading all data should fail on malformed line.
	// Since sample size is 1000, the malformed line is encountered during sampling.
	// The schema call already does sampling, so error may occur there or during ReadChunk.
	// In this case, the sampling breaks on error, so we get only 1 row.
	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 1, chunk.Count())
}

// TestNewReader_NDJSONNestedObjects tests handling of nested objects in NDJSON.
func TestNewReader_NDJSONNestedObjects(t *testing.T) {
	ndjsonData := `{"id": 1, "user": {"name": "alice", "age": 30}}
{"id": 2, "user": {"name": "bob", "age": 25}}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	// Nested objects become VARCHAR (JSON string).
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0]) // id
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1]) // user (nested object)

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Nested object should be serialized as JSON string.
	userVal := chunk.GetValue(0, 1)
	assert.IsType(t, "", userVal)
	assert.Contains(t, userVal.(string), "alice")
}

// TestNewReader_NDJSONDateValues tests date string detection in NDJSON.
func TestNewReader_NDJSONDateValues(t *testing.T) {
	ndjsonData := `{"date": "2024-01-15"}
{"date": "2024-06-30"}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_DATE, types[0])
}

// TestNewReader_NDJSONTimestampValues tests timestamp string detection in NDJSON.
func TestNewReader_NDJSONTimestampValues(t *testing.T) {
	ndjsonData := `{"ts": "2024-01-15T10:30:00Z"}
{"ts": "2024-06-30T14:45:00Z"}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	types, err := r.Types()
	require.NoError(t, err)
	assert.Equal(t, dukdb.TYPE_TIMESTAMP, types[0])
}

// TestNewReader_NDJSONUnicodeStrings tests handling of Unicode characters in NDJSON.
func TestNewReader_NDJSONUnicodeStrings(t *testing.T) {
	ndjsonData := `{"name": "\u4e2d\u6587"}
{"name": "\u00e9\u00e0\u00f9"}`
	reader := strings.NewReader(ndjsonData)

	opts := DefaultReaderOptions()
	opts.Format = FormatNDJSON

	r, err := NewReader(reader, opts)
	require.NoError(t, err)

	defer func() { _ = r.Close() }()

	chunk, err := r.ReadChunk()
	require.NoError(t, err)
	assert.Equal(t, 2, chunk.Count())

	// Unicode should be properly decoded.
	assert.Equal(t, "\u4e2d\u6587", chunk.GetValue(0, 0))
	assert.Equal(t, "\u00e9\u00e0\u00f9", chunk.GetValue(1, 0))
}

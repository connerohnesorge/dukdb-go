package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/arrow"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// writeArrow creates an Arrow IPC file with the given columns and data.
func writeArrow(t *testing.T, path string, columns []string, types []dukdb.Type, data [][]any) {
	t.Helper()

	// Create directory if needed
	dir := filepath.Dir(path)
	require.NoError(t, os.MkdirAll(dir, 0755))

	opts := arrow.DefaultWriterOptions()

	writer, err := arrow.NewWriterToPathOverwrite(path, opts)
	require.NoError(t, err)

	// Set schema
	err = writer.SetSchema(columns)
	require.NoError(t, err)
	err = writer.SetTypes(types)
	require.NoError(t, err)

	// Create a DataChunk with the data
	chunk := storage.NewDataChunkWithCapacity(types, len(data))
	for i, row := range data {
		for j, val := range row {
			chunk.SetValue(i, j, val)
		}
	}
	chunk.SetCount(len(data))

	// Write the chunk
	err = writer.WriteChunk(chunk)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)
}

// TestArrowGlobPatterns tests glob pattern expansion for Arrow files.
func TestArrowGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test Arrow files
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeArrow(t, filepath.Join(tmpDir, "data1.arrow"), columns, types, [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})
	writeArrow(t, filepath.Join(tmpDir, "data2.arrow"), columns, types, [][]any{
		{int32(3), "Charlie"},
		{int32(4), "Diana"},
	})
	// Create a non-arrow file to verify it's excluded
	err := os.WriteFile(filepath.Join(tmpDir, "other.txt"), []byte("not an arrow file"), 0644)
	require.NoError(t, err)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("glob_star_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.arrow")
		sql := `SELECT * FROM read_arrow('` + escapeForSQL(pattern) + `') ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// Check first and last row
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int32(4), result.Rows[3]["id"])
		assert.Equal(t, "Diana", result.Rows[3]["name"])
	})

	t.Run("glob_question_mark_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data?.arrow")
		sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("glob_bracket_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data[12].arrow")
		sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})
}

// TestArrowFilenameColumn tests the filename metadata column.
func TestArrowFilenameColumn(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeArrow(t, filepath.Join(tmpDir, "file1.arrow"), columns, types, [][]any{
		{int32(1), "a"},
		{int32(2), "b"},
	})
	writeArrow(t, filepath.Join(tmpDir, "file2.arrow"), columns, types, [][]any{
		{int32(3), "c"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT id, value, filename FROM read_arrow('` + escapeForSQL(
		pattern,
	) + `', filename=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// Files are sorted alphabetically, so file1 comes before file2
	assert.Contains(t, result.Rows[0]["filename"], "file1.arrow")
	assert.Contains(t, result.Rows[1]["filename"], "file1.arrow")
	assert.Contains(t, result.Rows[2]["filename"], "file2.arrow")
}

// TestArrowFileRowNumber tests the file_row_number metadata column.
func TestArrowFileRowNumber(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeArrow(t, filepath.Join(tmpDir, "file1.arrow"), columns, types, [][]any{
		{int32(1), "a"},
		{int32(2), "b"},
		{int32(3), "c"},
	})
	writeArrow(t, filepath.Join(tmpDir, "file2.arrow"), columns, types, [][]any{
		{int32(4), "d"},
		{int32(5), "e"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT id, file_row_number FROM read_arrow('` + escapeForSQL(
		pattern,
	) + `', file_row_number=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 5)
	// Row numbers are 1-indexed within each file
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"]) // file1 row 1
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"]) // file1 row 2
	assert.Equal(t, int64(3), result.Rows[2]["file_row_number"]) // file1 row 3
	assert.Equal(t, int64(1), result.Rows[3]["file_row_number"]) // file2 row 1
	assert.Equal(t, int64(2), result.Rows[4]["file_row_number"]) // file2 row 2
}

// TestArrowFileIndex tests the file_index metadata column.
func TestArrowFileIndex(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeArrow(t, filepath.Join(tmpDir, "a_file.arrow"), columns, types, [][]any{
		{int32(1)},
	})
	writeArrow(t, filepath.Join(tmpDir, "b_file.arrow"), columns, types, [][]any{
		{int32(2)},
	})
	writeArrow(t, filepath.Join(tmpDir, "c_file.arrow"), columns, types, [][]any{
		{int32(3)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT id, file_index FROM read_arrow('` + escapeForSQL(
		pattern,
	) + `', file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// File indexes are 0-indexed based on file order (alphabetical)
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"])) // a_file.arrow
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"])) // b_file.arrow
	assert.Equal(t, int32(2), testToInt32(result.Rows[2]["file_index"])) // c_file.arrow
}

// TestArrowFileGlobBehavior tests the file_glob_behavior option.
func TestArrowFileGlobBehavior(t *testing.T) {
	tmpDir := t.TempDir()

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("disallow_empty_default", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.arrow")
		sql := `SELECT * FROM read_arrow('` + escapeForSQL(pattern) + `')`

		_, err := execQueryWithErr(cat, exec, sql)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no files")
	})

	t.Run("allow_empty", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.arrow")
		sql := `SELECT * FROM read_arrow('` + escapeForSQL(
			pattern,
		) + `', file_glob_behavior='ALLOW_EMPTY')`

		result := execQuery(t, cat, exec, sql)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("fallback_glob_with_existing_file", func(t *testing.T) {
		// Create a file
		columns := []string{"id"}
		types := []dukdb.Type{dukdb.TYPE_INTEGER}
		writeArrow(t, filepath.Join(tmpDir, "data.arrow"), columns, types, [][]any{
			{int32(1)},
		})

		path := filepath.Join(tmpDir, "data.arrow")
		sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(
			path,
		) + `', file_glob_behavior='FALLBACK_GLOB')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(1), cnt)
	})
}

// TestArrowUnionByName tests union-by-name schema merging.
func TestArrowUnionByName(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with same schema for basic union_by_name test
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeArrow(t, filepath.Join(tmpDir, "data1.arrow"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeArrow(t, filepath.Join(tmpDir, "data2.arrow"), columns, types, [][]any{
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(
		pattern,
	) + `', union_by_name=true)`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestArrowRecursiveGlob tests recursive glob patterns.
func TestArrowRecursiveGlob(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure
	subDir1 := filepath.Join(tmpDir, "level1", "level2")
	subDir2 := filepath.Join(tmpDir, "level1", "other")

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeArrow(t, filepath.Join(tmpDir, "root.arrow"), columns, types, [][]any{
		{int32(1)},
	})
	writeArrow(t, filepath.Join(tmpDir, "level1", "l1.arrow"), columns, types, [][]any{
		{int32(2)},
	})
	writeArrow(t, filepath.Join(subDir1, "l2.arrow"), columns, types, [][]any{
		{int32(3)},
	})
	writeArrow(t, filepath.Join(subDir2, "other.arrow"), columns, types, [][]any{
		{int32(4)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("recursive_all", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.arrow")
		sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("recursive_specific_dir", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "level1", "**/*.arrow")
		sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(3), cnt) // l1.arrow, l2.arrow, other.arrow
	})
}

// TestArrowAllMetadataColumns tests using all metadata columns together.
func TestArrowAllMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeArrow(t, filepath.Join(tmpDir, "file1.arrow"), columns, types, [][]any{
		{int32(1)},
		{int32(2)},
	})
	writeArrow(t, filepath.Join(tmpDir, "file2.arrow"), columns, types, [][]any{
		{int32(3)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_arrow('` + escapeForSQL(
		pattern,
	) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index, file_row_number`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)

	// First row from file1
	assert.Equal(t, int32(1), result.Rows[0]["id"])
	assert.Contains(t, result.Rows[0]["filename"], "file1.arrow")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))

	// Second row from file1
	assert.Equal(t, int32(2), result.Rows[1]["id"])
	assert.Contains(t, result.Rows[1]["filename"], "file1.arrow")
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[1]["file_index"]))

	// First row from file2
	assert.Equal(t, int32(3), result.Rows[2]["id"])
	assert.Contains(t, result.Rows[2]["filename"], "file2.arrow")
	assert.Equal(t, int64(1), result.Rows[2]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[2]["file_index"]))
}

// TestArrowFilesToSniff tests the files_to_sniff option.
func TestArrowFilesToSniff(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with consistent schema
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeArrow(t, filepath.Join(tmpDir, "data1.arrow"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeArrow(t, filepath.Join(tmpDir, "data2.arrow"), columns, types, [][]any{
		{int32(2), "Bob"},
	})
	writeArrow(t, filepath.Join(tmpDir, "data3.arrow"), columns, types, [][]any{
		{int32(3), "Charlie"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")

	// Test that files_to_sniff option is recognized and files are read
	sql := `SELECT COUNT(*) as cnt FROM read_arrow('` + escapeForSQL(
		pattern,
	) + `', files_to_sniff=1)`
	result := execQuery(t, cat, exec, sql)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(3), cnt)
}

// TestArrowSingleFileNoGlob tests single file without glob patterns.
func TestArrowSingleFileNoGlob(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	filePath := filepath.Join(tmpDir, "data.arrow")
	writeArrow(t, filePath, columns, types, [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	sql := `SELECT * FROM read_arrow('` + escapeForSQL(filePath) + `') ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 2)
	assert.Equal(t, int32(1), result.Rows[0]["id"])
	assert.Equal(t, "Alice", result.Rows[0]["name"])
	assert.Equal(t, int32(2), result.Rows[1]["id"])
	assert.Equal(t, "Bob", result.Rows[1]["name"])
}

// TestArrowAutoGlobPatterns tests glob pattern expansion for read_arrow_auto.
func TestArrowAutoGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test Arrow files
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeArrow(t, filepath.Join(tmpDir, "data1.arrow"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeArrow(t, filepath.Join(tmpDir, "data2.arrow"), columns, types, [][]any{
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT COUNT(*) as cnt FROM read_arrow_auto('` + escapeForSQL(pattern) + `')`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestArrowAutoMetadataColumns tests metadata columns for read_arrow_auto.
func TestArrowAutoMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeArrow(t, filepath.Join(tmpDir, "file1.arrow"), columns, types, [][]any{
		{int32(1)},
	})
	writeArrow(t, filepath.Join(tmpDir, "file2.arrow"), columns, types, [][]any{
		{int32(2)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.arrow")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_arrow_auto('` + escapeForSQL(
		pattern,
	) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 2)
	assert.Contains(t, result.Rows[0]["filename"], "file1.arrow")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))
	assert.Contains(t, result.Rows[1]["filename"], "file2.arrow")
	assert.Equal(t, int64(1), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"]))
}

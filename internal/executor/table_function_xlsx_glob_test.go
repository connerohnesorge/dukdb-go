package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/xlsx"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// writeXLSX creates an XLSX file with the given columns and data.
func writeXLSX(t *testing.T, path string, columns []string, types []dukdb.Type, data [][]any) {
	t.Helper()

	// Create directory if needed
	dir := filepath.Dir(path)
	require.NoError(t, os.MkdirAll(dir, 0755))

	opts := xlsx.DefaultWriterOptions()

	writer, err := xlsx.NewWriterToPath(path, opts)
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

// TestXLSXGlobPatterns tests glob pattern expansion for XLSX files.
func TestXLSXGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test XLSX files
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeXLSX(t, filepath.Join(tmpDir, "data1.xlsx"), columns, types, [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "data2.xlsx"), columns, types, [][]any{
		{int32(3), "Charlie"},
		{int32(4), "Diana"},
	})
	// Create a non-xlsx file to verify it's excluded
	err := os.WriteFile(filepath.Join(tmpDir, "other.txt"), []byte("not an xlsx"), 0644)
	require.NoError(t, err)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("glob_star_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.xlsx")
		sql := `SELECT * FROM read_xlsx('` + escapeForSQL(pattern) + `') ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// Check first and last row (XLSX may return int64 or string depending on type inference)
		id0 := testToInt64(result.Rows[0]["id"])
		id3 := testToInt64(result.Rows[3]["id"])
		assert.Equal(t, int64(1), id0)
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int64(4), id3)
		assert.Equal(t, "Diana", result.Rows[3]["name"])
	})

	t.Run("glob_question_mark_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data?.xlsx")
		sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("glob_bracket_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data[12].xlsx")
		sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})
}

// TestXLSXFilenameColumn tests the filename metadata column.
func TestXLSXFilenameColumn(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeXLSX(t, filepath.Join(tmpDir, "file1.xlsx"), columns, types, [][]any{
		{int32(1), "a"},
		{int32(2), "b"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "file2.xlsx"), columns, types, [][]any{
		{int32(3), "c"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT id, value, filename FROM read_xlsx('` + escapeForSQL(pattern) + `', filename=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// Files are sorted alphabetically, so file1 comes before file2
	assert.Contains(t, result.Rows[0]["filename"], "file1.xlsx")
	assert.Contains(t, result.Rows[1]["filename"], "file1.xlsx")
	assert.Contains(t, result.Rows[2]["filename"], "file2.xlsx")
}

// TestXLSXFileRowNumber tests the file_row_number metadata column.
func TestXLSXFileRowNumber(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeXLSX(t, filepath.Join(tmpDir, "file1.xlsx"), columns, types, [][]any{
		{int32(1), "a"},
		{int32(2), "b"},
		{int32(3), "c"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "file2.xlsx"), columns, types, [][]any{
		{int32(4), "d"},
		{int32(5), "e"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT id, file_row_number FROM read_xlsx('` + escapeForSQL(pattern) + `', file_row_number=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 5)
	// Row numbers are 1-indexed within each file
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"]) // file1 row 1
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"]) // file1 row 2
	assert.Equal(t, int64(3), result.Rows[2]["file_row_number"]) // file1 row 3
	assert.Equal(t, int64(1), result.Rows[3]["file_row_number"]) // file2 row 1
	assert.Equal(t, int64(2), result.Rows[4]["file_row_number"]) // file2 row 2
}

// TestXLSXFileIndex tests the file_index metadata column.
func TestXLSXFileIndex(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeXLSX(t, filepath.Join(tmpDir, "a_file.xlsx"), columns, types, [][]any{
		{int32(1)},
	})
	writeXLSX(t, filepath.Join(tmpDir, "b_file.xlsx"), columns, types, [][]any{
		{int32(2)},
	})
	writeXLSX(t, filepath.Join(tmpDir, "c_file.xlsx"), columns, types, [][]any{
		{int32(3)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT id, file_index FROM read_xlsx('` + escapeForSQL(pattern) + `', file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// File indexes are 0-indexed based on file order (alphabetical)
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"])) // a_file.xlsx
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"])) // b_file.xlsx
	assert.Equal(t, int32(2), testToInt32(result.Rows[2]["file_index"])) // c_file.xlsx
}

// TestXLSXFileGlobBehavior tests the file_glob_behavior option.
func TestXLSXFileGlobBehavior(t *testing.T) {
	tmpDir := t.TempDir()

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("disallow_empty_default", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.xlsx")
		sql := `SELECT * FROM read_xlsx('` + escapeForSQL(pattern) + `')`

		_, err := execQueryWithErr(cat, exec, sql)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no files")
	})

	t.Run("allow_empty", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.xlsx")
		sql := `SELECT * FROM read_xlsx('` + escapeForSQL(pattern) + `', file_glob_behavior='ALLOW_EMPTY')`

		result := execQuery(t, cat, exec, sql)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("fallback_glob_with_existing_file", func(t *testing.T) {
		// Create a file
		columns := []string{"id"}
		types := []dukdb.Type{dukdb.TYPE_INTEGER}
		writeXLSX(t, filepath.Join(tmpDir, "data.xlsx"), columns, types, [][]any{
			{int32(1)},
		})

		path := filepath.Join(tmpDir, "data.xlsx")
		sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(path) + `', file_glob_behavior='FALLBACK_GLOB')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(1), cnt)
	})
}

// TestXLSXUnionByName tests union-by-name schema merging.
func TestXLSXUnionByName(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with same schema for basic union_by_name test
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeXLSX(t, filepath.Join(tmpDir, "data1.xlsx"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "data2.xlsx"), columns, types, [][]any{
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(pattern) + `', union_by_name=true)`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestXLSXRecursiveGlob tests recursive glob patterns.
func TestXLSXRecursiveGlob(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure
	subDir1 := filepath.Join(tmpDir, "level1", "level2")
	subDir2 := filepath.Join(tmpDir, "level1", "other")

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeXLSX(t, filepath.Join(tmpDir, "root.xlsx"), columns, types, [][]any{
		{int32(1)},
	})
	writeXLSX(t, filepath.Join(tmpDir, "level1", "l1.xlsx"), columns, types, [][]any{
		{int32(2)},
	})
	writeXLSX(t, filepath.Join(subDir1, "l2.xlsx"), columns, types, [][]any{
		{int32(3)},
	})
	writeXLSX(t, filepath.Join(subDir2, "other.xlsx"), columns, types, [][]any{
		{int32(4)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("recursive_all", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.xlsx")
		sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("recursive_specific_dir", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "level1", "**/*.xlsx")
		sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(3), cnt) // l1.xlsx, l2.xlsx, other.xlsx
	})
}

// TestXLSXAllMetadataColumns tests using all metadata columns together.
func TestXLSXAllMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeXLSX(t, filepath.Join(tmpDir, "file1.xlsx"), columns, types, [][]any{
		{int32(1)},
		{int32(2)},
	})
	writeXLSX(t, filepath.Join(tmpDir, "file2.xlsx"), columns, types, [][]any{
		{int32(3)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_xlsx('` + escapeForSQL(pattern) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index, file_row_number`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)

	// First row from file1
	assert.Equal(t, int64(1), testToInt64(result.Rows[0]["id"]))
	assert.Contains(t, result.Rows[0]["filename"], "file1.xlsx")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))

	// Second row from file1
	assert.Equal(t, int64(2), testToInt64(result.Rows[1]["id"]))
	assert.Contains(t, result.Rows[1]["filename"], "file1.xlsx")
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[1]["file_index"]))

	// First row from file2
	assert.Equal(t, int64(3), testToInt64(result.Rows[2]["id"]))
	assert.Contains(t, result.Rows[2]["filename"], "file2.xlsx")
	assert.Equal(t, int64(1), result.Rows[2]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[2]["file_index"]))
}

// TestXLSXFilesToSniff tests the files_to_sniff option.
func TestXLSXFilesToSniff(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with consistent schema
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeXLSX(t, filepath.Join(tmpDir, "data1.xlsx"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "data2.xlsx"), columns, types, [][]any{
		{int32(2), "Bob"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "data3.xlsx"), columns, types, [][]any{
		{int32(3), "Charlie"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")

	// Test that files_to_sniff option is recognized and files are read
	sql := `SELECT COUNT(*) as cnt FROM read_xlsx('` + escapeForSQL(pattern) + `', files_to_sniff=1)`
	result := execQuery(t, cat, exec, sql)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(3), cnt)
}

// TestXLSXSingleFileNoGlob tests single file without glob patterns.
func TestXLSXSingleFileNoGlob(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	filePath := filepath.Join(tmpDir, "data.xlsx")
	writeXLSX(t, filePath, columns, types, [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	sql := `SELECT * FROM read_xlsx('` + escapeForSQL(filePath) + `') ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 2)
	assert.Equal(t, int64(1), testToInt64(result.Rows[0]["id"]))
	assert.Equal(t, "Alice", result.Rows[0]["name"])
	assert.Equal(t, int64(2), testToInt64(result.Rows[1]["id"]))
	assert.Equal(t, "Bob", result.Rows[1]["name"])
}

// TestXLSXAutoGlobPatterns tests glob pattern expansion for read_xlsx_auto.
func TestXLSXAutoGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test XLSX files
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeXLSX(t, filepath.Join(tmpDir, "data1.xlsx"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeXLSX(t, filepath.Join(tmpDir, "data2.xlsx"), columns, types, [][]any{
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT COUNT(*) as cnt FROM read_xlsx_auto('` + escapeForSQL(pattern) + `')`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestXLSXAutoMetadataColumns tests metadata columns for read_xlsx_auto.
func TestXLSXAutoMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeXLSX(t, filepath.Join(tmpDir, "file1.xlsx"), columns, types, [][]any{
		{int32(1)},
	})
	writeXLSX(t, filepath.Join(tmpDir, "file2.xlsx"), columns, types, [][]any{
		{int32(2)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.xlsx")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_xlsx_auto('` + escapeForSQL(pattern) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 2)
	assert.Contains(t, result.Rows[0]["filename"], "file1.xlsx")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))
	assert.Contains(t, result.Rows[1]["filename"], "file2.xlsx")
	assert.Equal(t, int64(1), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"]))
}

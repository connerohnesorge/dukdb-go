package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// writeParquet creates a Parquet file with the given columns and data.
func writeParquet(t *testing.T, path string, columns []string, types []dukdb.Type, data [][]any) {
	t.Helper()

	// Create directory if needed
	dir := filepath.Dir(path)
	require.NoError(t, os.MkdirAll(dir, 0755))

	opts := parquet.DefaultWriterOptions()
	opts.Overwrite = true

	writer, err := parquet.NewWriterToPath(path, opts)
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

// TestParquetGlobPatterns tests glob pattern expansion for Parquet files.
func TestParquetGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test Parquet files
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeParquet(t, filepath.Join(tmpDir, "data1.parquet"), columns, types, [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})
	writeParquet(t, filepath.Join(tmpDir, "data2.parquet"), columns, types, [][]any{
		{int32(3), "Charlie"},
		{int32(4), "Diana"},
	})
	// Create a non-parquet file to verify it's excluded
	err := os.WriteFile(filepath.Join(tmpDir, "other.txt"), []byte("not a parquet"), 0644)
	require.NoError(t, err)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("glob_star_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.parquet")
		sql := `SELECT * FROM read_parquet('` + escapeForSQL(pattern) + `') ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// Check first and last row
		assert.Equal(t, int32(1), result.Rows[0]["id"])
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int32(4), result.Rows[3]["id"])
		assert.Equal(t, "Diana", result.Rows[3]["name"])
	})

	t.Run("glob_question_mark_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data?.parquet")
		sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("glob_bracket_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data[12].parquet")
		sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})
}

// TestParquetFilenameColumn tests the filename metadata column.
func TestParquetFilenameColumn(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeParquet(t, filepath.Join(tmpDir, "file1.parquet"), columns, types, [][]any{
		{int32(1), "a"},
		{int32(2), "b"},
	})
	writeParquet(t, filepath.Join(tmpDir, "file2.parquet"), columns, types, [][]any{
		{int32(3), "c"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.parquet")
	sql := `SELECT id, value, filename FROM read_parquet('` + escapeForSQL(pattern) + `', filename=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// Files are sorted alphabetically, so file1 comes before file2
	assert.Contains(t, result.Rows[0]["filename"], "file1.parquet")
	assert.Contains(t, result.Rows[1]["filename"], "file1.parquet")
	assert.Contains(t, result.Rows[2]["filename"], "file2.parquet")
}

// TestParquetFileRowNumber tests the file_row_number metadata column.
func TestParquetFileRowNumber(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeParquet(t, filepath.Join(tmpDir, "file1.parquet"), columns, types, [][]any{
		{int32(1), "a"},
		{int32(2), "b"},
		{int32(3), "c"},
	})
	writeParquet(t, filepath.Join(tmpDir, "file2.parquet"), columns, types, [][]any{
		{int32(4), "d"},
		{int32(5), "e"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.parquet")
	sql := `SELECT id, file_row_number FROM read_parquet('` + escapeForSQL(pattern) + `', file_row_number=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 5)
	// Row numbers are 1-indexed within each file
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"]) // file1 row 1
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"]) // file1 row 2
	assert.Equal(t, int64(3), result.Rows[2]["file_row_number"]) // file1 row 3
	assert.Equal(t, int64(1), result.Rows[3]["file_row_number"]) // file2 row 1
	assert.Equal(t, int64(2), result.Rows[4]["file_row_number"]) // file2 row 2
}

// TestParquetFileIndex tests the file_index metadata column.
func TestParquetFileIndex(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeParquet(t, filepath.Join(tmpDir, "a_file.parquet"), columns, types, [][]any{
		{int32(1)},
	})
	writeParquet(t, filepath.Join(tmpDir, "b_file.parquet"), columns, types, [][]any{
		{int32(2)},
	})
	writeParquet(t, filepath.Join(tmpDir, "c_file.parquet"), columns, types, [][]any{
		{int32(3)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.parquet")
	sql := `SELECT id, file_index FROM read_parquet('` + escapeForSQL(pattern) + `', file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// File indexes are 0-indexed based on file order (alphabetical)
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"])) // a_file.parquet
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"])) // b_file.parquet
	assert.Equal(t, int32(2), testToInt32(result.Rows[2]["file_index"])) // c_file.parquet
}

// TestParquetFileGlobBehavior tests the file_glob_behavior option.
func TestParquetFileGlobBehavior(t *testing.T) {
	tmpDir := t.TempDir()

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("disallow_empty_default", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.parquet")
		sql := `SELECT * FROM read_parquet('` + escapeForSQL(pattern) + `')`

		_, err := execQueryWithErr(cat, exec, sql)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no files")
	})

	t.Run("allow_empty", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.parquet")
		sql := `SELECT * FROM read_parquet('` + escapeForSQL(pattern) + `', file_glob_behavior='ALLOW_EMPTY')`

		result := execQuery(t, cat, exec, sql)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("fallback_glob_with_existing_file", func(t *testing.T) {
		// Create a file
		columns := []string{"id"}
		types := []dukdb.Type{dukdb.TYPE_INTEGER}
		writeParquet(t, filepath.Join(tmpDir, "data.parquet"), columns, types, [][]any{
			{int32(1)},
		})

		path := filepath.Join(tmpDir, "data.parquet")
		sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(path) + `', file_glob_behavior='FALLBACK_GLOB')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(1), cnt)
	})
}

// TestParquetHivePartitioning tests Hive-style partitioning support.
func TestParquetHivePartitioning(t *testing.T) {
	tmpDir := t.TempDir()

	// Create partitioned directory structure
	partDir1 := filepath.Join(tmpDir, "year=2023", "month=01")
	partDir2 := filepath.Join(tmpDir, "year=2023", "month=02")
	partDir3 := filepath.Join(tmpDir, "year=2024", "month=01")

	columns := []string{"id", "value"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeParquet(t, filepath.Join(partDir1, "data.parquet"), columns, types, [][]any{
		{int32(1), "a"},
	})
	writeParquet(t, filepath.Join(partDir2, "data.parquet"), columns, types, [][]any{
		{int32(2), "b"},
	})
	writeParquet(t, filepath.Join(partDir3, "data.parquet"), columns, types, [][]any{
		{int32(3), "c"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("hive_partitioning_enabled", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.parquet")
		sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `', hive_partitioning=true)`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(3), cnt)
	})

	t.Run("hive_partitioning_with_autocast", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.parquet")
		sql := `SELECT id, month, year FROM read_parquet('` + escapeForSQL(pattern) + `', hive_partitioning=true, hive_types_autocast=true) ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 3)
		// Year and month should be auto-cast to integers
		assert.Equal(t, int64(2023), testToInt64(result.Rows[0]["year"]))
		assert.Equal(t, int64(1), testToInt64(result.Rows[0]["month"]))
		assert.Equal(t, int64(2023), testToInt64(result.Rows[1]["year"]))
		assert.Equal(t, int64(2), testToInt64(result.Rows[1]["month"]))
		assert.Equal(t, int64(2024), testToInt64(result.Rows[2]["year"]))
		assert.Equal(t, int64(1), testToInt64(result.Rows[2]["month"]))
	})

	t.Run("hive_partitioning_no_autocast", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.parquet")
		sql := `SELECT id, month, year FROM read_parquet('` + escapeForSQL(pattern) + `', hive_partitioning=true, hive_types_autocast=false) ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 3)
		// Year and month should be strings
		assert.Equal(t, "2023", result.Rows[0]["year"])
		assert.Equal(t, "01", result.Rows[0]["month"])
	})
}

// TestParquetUnionByName tests union-by-name schema merging.
func TestParquetUnionByName(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with same schema for basic union_by_name test
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeParquet(t, filepath.Join(tmpDir, "data1.parquet"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeParquet(t, filepath.Join(tmpDir, "data2.parquet"), columns, types, [][]any{
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.parquet")
	sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `', union_by_name=true)`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestParquetRecursiveGlob tests recursive glob patterns.
func TestParquetRecursiveGlob(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure
	subDir1 := filepath.Join(tmpDir, "level1", "level2")
	subDir2 := filepath.Join(tmpDir, "level1", "other")

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeParquet(t, filepath.Join(tmpDir, "root.parquet"), columns, types, [][]any{
		{int32(1)},
	})
	writeParquet(t, filepath.Join(tmpDir, "level1", "l1.parquet"), columns, types, [][]any{
		{int32(2)},
	})
	writeParquet(t, filepath.Join(subDir1, "l2.parquet"), columns, types, [][]any{
		{int32(3)},
	})
	writeParquet(t, filepath.Join(subDir2, "other.parquet"), columns, types, [][]any{
		{int32(4)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("recursive_all", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.parquet")
		sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("recursive_specific_dir", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "level1", "**/*.parquet")
		sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(3), cnt) // l1.parquet, l2.parquet, other.parquet
	})
}

// TestParquetAllMetadataColumns tests using all metadata columns together.
func TestParquetAllMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeParquet(t, filepath.Join(tmpDir, "file1.parquet"), columns, types, [][]any{
		{int32(1)},
		{int32(2)},
	})
	writeParquet(t, filepath.Join(tmpDir, "file2.parquet"), columns, types, [][]any{
		{int32(3)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.parquet")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_parquet('` + escapeForSQL(pattern) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index, file_row_number`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)

	// First row from file1
	assert.Equal(t, int32(1), result.Rows[0]["id"])
	assert.Contains(t, result.Rows[0]["filename"], "file1.parquet")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))

	// Second row from file1
	assert.Equal(t, int32(2), result.Rows[1]["id"])
	assert.Contains(t, result.Rows[1]["filename"], "file1.parquet")
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[1]["file_index"]))

	// First row from file2
	assert.Equal(t, int32(3), result.Rows[2]["id"])
	assert.Contains(t, result.Rows[2]["filename"], "file2.parquet")
	assert.Equal(t, int64(1), result.Rows[2]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[2]["file_index"]))
}

// TestParquetFilesToSniff tests the files_to_sniff option.
func TestParquetFilesToSniff(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with consistent schema
	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	writeParquet(t, filepath.Join(tmpDir, "data1.parquet"), columns, types, [][]any{
		{int32(1), "Alice"},
	})
	writeParquet(t, filepath.Join(tmpDir, "data2.parquet"), columns, types, [][]any{
		{int32(2), "Bob"},
	})
	writeParquet(t, filepath.Join(tmpDir, "data3.parquet"), columns, types, [][]any{
		{int32(3), "Charlie"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.parquet")

	// Test that files_to_sniff option is recognized and files are read
	sql := `SELECT COUNT(*) as cnt FROM read_parquet('` + escapeForSQL(pattern) + `', files_to_sniff=1)`
	result := execQuery(t, cat, exec, sql)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(3), cnt)
}

// TestParquetHivePartitioningWithExplicitTypes tests hive_types option.
func TestParquetHivePartitioningWithExplicitTypes(t *testing.T) {
	tmpDir := t.TempDir()

	// Create partitioned directory structure
	partDir := filepath.Join(tmpDir, "year=2023", "active=true")
	require.NoError(t, os.MkdirAll(partDir, 0755))

	columns := []string{"id"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER}

	writeParquet(t, filepath.Join(partDir, "data.parquet"), columns, types, [][]any{
		{int32(1)},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Note: hive_types is a map and cannot be easily passed through SQL syntax
	// This test verifies that hive partitioning extracts the values
	pattern := filepath.Join(tmpDir, "**/*.parquet")
	sql := `SELECT id, year, active FROM read_parquet('` + escapeForSQL(pattern) + `', hive_partitioning=true, hive_types_autocast=true)`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	assert.Equal(t, int32(1), result.Rows[0]["id"])
	// With autocast, year should be int, active should be bool
	assert.Equal(t, int64(2023), testToInt64(result.Rows[0]["year"]))
	assert.Equal(t, true, result.Rows[0]["active"])
}

// TestParquetSingleFileNoGlob tests single file without glob patterns.
func TestParquetSingleFileNoGlob(t *testing.T) {
	tmpDir := t.TempDir()

	columns := []string{"id", "name"}
	types := []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR}

	filePath := filepath.Join(tmpDir, "data.parquet")
	writeParquet(t, filePath, columns, types, [][]any{
		{int32(1), "Alice"},
		{int32(2), "Bob"},
	})

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	sql := `SELECT * FROM read_parquet('` + escapeForSQL(filePath) + `') ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 2)
	assert.Equal(t, int32(1), result.Rows[0]["id"])
	assert.Equal(t, "Alice", result.Rows[0]["name"])
	assert.Equal(t, int32(2), result.Rows[1]["id"])
	assert.Equal(t, "Bob", result.Rows[1]["name"])
}

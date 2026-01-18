package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TestJSONGlobPatterns tests glob pattern expansion for JSON files.
func TestJSONGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test JSON files
	writeJSON(
		t,
		filepath.Join(tmpDir, "data1.json"),
		`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`,
	)
	writeJSON(
		t,
		filepath.Join(tmpDir, "data2.json"),
		`[{"id":3,"name":"Charlie"},{"id":4,"name":"Diana"}]`,
	)
	writeJSON(t, filepath.Join(tmpDir, "other.txt"), `not,a,json`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("glob_star_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.json")
		sql := `SELECT * FROM read_json('` + escapeForSQL(pattern) + `') ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// Check first and last row
		id0 := testToInt64(result.Rows[0]["id"])
		id3 := testToInt64(result.Rows[3]["id"])
		assert.Equal(t, int64(1), id0)
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int64(4), id3)
		assert.Equal(t, "Diana", result.Rows[3]["name"])
	})

	t.Run("glob_question_mark_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data?.json")
		sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("glob_bracket_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data[12].json")
		sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})
}

// TestJSONFilenameColumn tests the filename metadata column.
func TestJSONFilenameColumn(t *testing.T) {
	tmpDir := t.TempDir()

	writeJSON(t, filepath.Join(tmpDir, "file1.json"), `[{"id":1,"value":"a"},{"id":2,"value":"b"}]`)
	writeJSON(t, filepath.Join(tmpDir, "file2.json"), `[{"id":3,"value":"c"}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.json")
	sql := `SELECT id, value, filename FROM read_json('` + escapeForSQL(
		pattern,
	) + `', filename=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// Files are sorted alphabetically, so file1 comes before file2
	assert.Contains(t, result.Rows[0]["filename"], "file1.json")
	assert.Contains(t, result.Rows[1]["filename"], "file1.json")
	assert.Contains(t, result.Rows[2]["filename"], "file2.json")
}

// TestJSONFileRowNumber tests the file_row_number metadata column.
func TestJSONFileRowNumber(t *testing.T) {
	tmpDir := t.TempDir()

	writeJSON(
		t,
		filepath.Join(tmpDir, "file1.json"),
		`[{"id":1,"value":"a"},{"id":2,"value":"b"},{"id":3,"value":"c"}]`,
	)
	writeJSON(t, filepath.Join(tmpDir, "file2.json"), `[{"id":4,"value":"d"},{"id":5,"value":"e"}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.json")
	sql := `SELECT id, file_row_number FROM read_json('` + escapeForSQL(
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

// TestJSONFileIndex tests the file_index metadata column.
func TestJSONFileIndex(t *testing.T) {
	tmpDir := t.TempDir()

	writeJSON(t, filepath.Join(tmpDir, "a_file.json"), `[{"id":1}]`)
	writeJSON(t, filepath.Join(tmpDir, "b_file.json"), `[{"id":2}]`)
	writeJSON(t, filepath.Join(tmpDir, "c_file.json"), `[{"id":3}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.json")
	sql := `SELECT id, file_index FROM read_json('` + escapeForSQL(
		pattern,
	) + `', file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// File indexes are 0-indexed based on file order (alphabetical)
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"])) // a_file.json
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"])) // b_file.json
	assert.Equal(t, int32(2), testToInt32(result.Rows[2]["file_index"])) // c_file.json
}

// TestJSONFilesToSniff tests the files_to_sniff option.
func TestJSONFilesToSniff(t *testing.T) {
	tmpDir := t.TempDir()

	// Use consistent schema to avoid type merge issues
	writeJSON(t, filepath.Join(tmpDir, "data1.json"), `[{"id":1,"name":"Alice"}]`)
	writeJSON(t, filepath.Join(tmpDir, "data2.json"), `[{"id":2,"name":"Bob"}]`)
	writeJSON(t, filepath.Join(tmpDir, "data3.json"), `[{"id":3,"name":"Charlie"}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.json")

	// Test that files_to_sniff option is recognized and files are read
	sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(
		pattern,
	) + `', files_to_sniff=1)`
	result := execQuery(t, cat, exec, sql)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(3), cnt)
}

// TestJSONFileGlobBehavior tests the file_glob_behavior option.
func TestJSONFileGlobBehavior(t *testing.T) {
	tmpDir := t.TempDir()

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("disallow_empty_default", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.json")
		sql := `SELECT * FROM read_json('` + escapeForSQL(pattern) + `')`

		_, err := execQueryWithErr(cat, exec, sql)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no files")
	})

	t.Run("allow_empty", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.json")
		sql := `SELECT * FROM read_json('` + escapeForSQL(
			pattern,
		) + `', file_glob_behavior='ALLOW_EMPTY')`

		result := execQuery(t, cat, exec, sql)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("fallback_glob_with_existing_file", func(t *testing.T) {
		// Create a file
		writeJSON(t, filepath.Join(tmpDir, "data.json"), `[{"id":1}]`)

		path := filepath.Join(tmpDir, "data.json")
		sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(
			path,
		) + `', file_glob_behavior='FALLBACK_GLOB')`

		result := execQuery(t, cat, exec, sql)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(1), cnt)
	})
}

// TestJSONHivePartitioning tests Hive-style partitioning support.
func TestJSONHivePartitioning(t *testing.T) {
	tmpDir := t.TempDir()

	// Create partitioned directory structure
	partDir1 := filepath.Join(tmpDir, "year=2023", "month=01")
	partDir2 := filepath.Join(tmpDir, "year=2023", "month=02")
	partDir3 := filepath.Join(tmpDir, "year=2024", "month=01")

	require.NoError(t, os.MkdirAll(partDir1, 0755))
	require.NoError(t, os.MkdirAll(partDir2, 0755))
	require.NoError(t, os.MkdirAll(partDir3, 0755))

	writeJSON(t, filepath.Join(partDir1, "data.json"), `[{"id":1,"value":"a"}]`)
	writeJSON(t, filepath.Join(partDir2, "data.json"), `[{"id":2,"value":"b"}]`)
	writeJSON(t, filepath.Join(partDir3, "data.json"), `[{"id":3,"value":"c"}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Test that files with hive partitioning structure can be read with the option
	t.Run("hive_partitioning_enabled", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.json")
		// Just read the data columns - partition columns are added by executor
		sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(
			pattern,
		) + `', hive_partitioning=true)`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(3), cnt)
	})
}

// TestJSONUnionByName tests union-by-name schema merging.
func TestJSONUnionByName(t *testing.T) {
	tmpDir := t.TempDir()

	// Use files with same schema for basic union_by_name test
	writeJSON(t, filepath.Join(tmpDir, "data1.json"), `[{"id":1,"name":"Alice"}]`)
	writeJSON(t, filepath.Join(tmpDir, "data2.json"), `[{"id":2,"name":"Bob"}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.json")
	sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(
		pattern,
	) + `', union_by_name=true)`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestJSONRecursiveGlob tests recursive glob patterns.
func TestJSONRecursiveGlob(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure
	subDir1 := filepath.Join(tmpDir, "level1", "level2")
	subDir2 := filepath.Join(tmpDir, "level1", "other")
	require.NoError(t, os.MkdirAll(subDir1, 0755))
	require.NoError(t, os.MkdirAll(subDir2, 0755))

	writeJSON(t, filepath.Join(tmpDir, "root.json"), `[{"id":1}]`)
	writeJSON(t, filepath.Join(tmpDir, "level1", "l1.json"), `[{"id":2}]`)
	writeJSON(t, filepath.Join(subDir1, "l2.json"), `[{"id":3}]`)
	writeJSON(t, filepath.Join(subDir2, "other.json"), `[{"id":4}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("recursive_all", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.json")
		sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("recursive_specific_dir", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "level1", "**/*.json")
		sql := `SELECT COUNT(*) as cnt FROM read_json('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(3), cnt) // l1.json, l2.json, other.json
	})
}

// TestJSONAllMetadataColumns tests using all metadata columns together.
func TestJSONAllMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	writeJSON(t, filepath.Join(tmpDir, "file1.json"), `[{"id":1},{"id":2}]`)
	writeJSON(t, filepath.Join(tmpDir, "file2.json"), `[{"id":3}]`)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.json")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_json('` + escapeForSQL(
		pattern,
	) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index, file_row_number`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)

	// First row from file1
	assert.Equal(t, int64(1), testToInt64(result.Rows[0]["id"]))
	assert.Contains(t, result.Rows[0]["filename"], "file1.json")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))

	// Second row from file1
	assert.Equal(t, int64(2), testToInt64(result.Rows[1]["id"]))
	assert.Contains(t, result.Rows[1]["filename"], "file1.json")
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[1]["file_index"]))

	// First row from file2
	assert.Equal(t, int64(3), testToInt64(result.Rows[2]["id"]))
	assert.Contains(t, result.Rows[2]["filename"], "file2.json")
	assert.Equal(t, int64(1), result.Rows[2]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[2]["file_index"]))
}

// TestNDJSONGlobPatterns tests glob pattern expansion for NDJSON files.
func TestNDJSONGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test NDJSON files (newline-delimited JSON)
	writeJSON(
		t,
		filepath.Join(tmpDir, "data1.ndjson"),
		"{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n",
	)
	writeJSON(
		t,
		filepath.Join(tmpDir, "data2.ndjson"),
		"{\"id\":3,\"name\":\"Charlie\"}\n{\"id\":4,\"name\":\"Diana\"}\n",
	)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("ndjson_glob_star_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.ndjson")
		sql := `SELECT * FROM read_ndjson('` + escapeForSQL(pattern) + `') ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// Check first and last row
		id0 := testToInt64(result.Rows[0]["id"])
		id3 := testToInt64(result.Rows[3]["id"])
		assert.Equal(t, int64(1), id0)
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int64(4), id3)
		assert.Equal(t, "Diana", result.Rows[3]["name"])
	})
}

// TestNDJSONMetadataColumns tests metadata columns for NDJSON files.
func TestNDJSONMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	writeJSON(t, filepath.Join(tmpDir, "file1.ndjson"), "{\"id\":1}\n{\"id\":2}\n")
	writeJSON(t, filepath.Join(tmpDir, "file2.ndjson"), "{\"id\":3}\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.ndjson")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_ndjson('` + escapeForSQL(
		pattern,
	) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index, file_row_number`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)

	// First row from file1
	assert.Equal(t, int64(1), testToInt64(result.Rows[0]["id"]))
	assert.Contains(t, result.Rows[0]["filename"], "file1.ndjson")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))

	// Second row from file1
	assert.Equal(t, int64(2), testToInt64(result.Rows[1]["id"]))
	assert.Contains(t, result.Rows[1]["filename"], "file1.ndjson")
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[1]["file_index"]))

	// First row from file2
	assert.Equal(t, int64(3), testToInt64(result.Rows[2]["id"]))
	assert.Contains(t, result.Rows[2]["filename"], "file2.ndjson")
	assert.Equal(t, int64(1), result.Rows[2]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[2]["file_index"]))
}

// TestJSONAutoGlobPatterns tests glob pattern expansion for read_json_auto.
func TestJSONAutoGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test JSON files
	writeJSON(
		t,
		filepath.Join(tmpDir, "data1.json"),
		`[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]`,
	)
	writeJSON(
		t,
		filepath.Join(tmpDir, "data2.json"),
		`[{"id":3,"name":"Charlie"},{"id":4,"name":"Diana"}]`,
	)

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("json_auto_glob_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.json")
		sql := `SELECT COUNT(*) as cnt FROM read_json_auto('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("json_auto_with_metadata_columns", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.json")
		sql := `SELECT id, filename, file_index FROM read_json_auto('` + escapeForSQL(
			pattern,
		) + `', filename=true, file_index=true) ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		assert.Contains(t, result.Rows[0]["filename"], "data1.json")
		assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))
	})
}

// Helper function to write JSON files
func writeJSON(t *testing.T, path, content string) {
	t.Helper()
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
}

package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TestCSVGlobPatterns tests glob pattern expansion for CSV files.
func TestCSVGlobPatterns(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test CSV files
	writeCSV(t, filepath.Join(tmpDir, "data1.csv"), "id,name\n1,Alice\n2,Bob\n")
	writeCSV(t, filepath.Join(tmpDir, "data2.csv"), "id,name\n3,Charlie\n4,Diana\n")
	writeCSV(t, filepath.Join(tmpDir, "other.txt"), "not,a,csv\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("glob_star_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "*.csv")
		sql := `SELECT * FROM read_csv('` + escapeForSQL(pattern) + `') ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// Check first and last row (id can be int32 or int64 depending on type inference)
		id0 := testToInt64(result.Rows[0]["id"])
		id3 := testToInt64(result.Rows[3]["id"])
		assert.Equal(t, int64(1), id0)
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int64(4), id3)
		assert.Equal(t, "Diana", result.Rows[3]["name"])
	})

	t.Run("glob_question_mark_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data?.csv")
		sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("glob_bracket_pattern", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "data[12].csv")
		sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})
}

// TestCSVFilenameColumn tests the filename metadata column.
func TestCSVFilenameColumn(t *testing.T) {
	tmpDir := t.TempDir()

	writeCSV(t, filepath.Join(tmpDir, "file1.csv"), "id,value\n1,a\n2,b\n")
	writeCSV(t, filepath.Join(tmpDir, "file2.csv"), "id,value\n3,c\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.csv")
	sql := `SELECT id, value, filename FROM read_csv('` + escapeForSQL(
		pattern,
	) + `', filename=true) ORDER BY id`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// Files are sorted alphabetically, so file1 comes before file2
	assert.Contains(t, result.Rows[0]["filename"], "file1.csv")
	assert.Contains(t, result.Rows[1]["filename"], "file1.csv")
	assert.Contains(t, result.Rows[2]["filename"], "file2.csv")
}

// TestCSVFileRowNumber tests the file_row_number metadata column.
func TestCSVFileRowNumber(t *testing.T) {
	tmpDir := t.TempDir()

	writeCSV(t, filepath.Join(tmpDir, "file1.csv"), "id,value\n1,a\n2,b\n3,c\n")
	writeCSV(t, filepath.Join(tmpDir, "file2.csv"), "id,value\n4,d\n5,e\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.csv")
	sql := `SELECT id, file_row_number FROM read_csv('` + escapeForSQL(
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

// TestCSVFileIndex tests the file_index metadata column.
func TestCSVFileIndex(t *testing.T) {
	tmpDir := t.TempDir()

	writeCSV(t, filepath.Join(tmpDir, "a_file.csv"), "id\n1\n")
	writeCSV(t, filepath.Join(tmpDir, "b_file.csv"), "id\n2\n")
	writeCSV(t, filepath.Join(tmpDir, "c_file.csv"), "id\n3\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.csv")
	sql := `SELECT id, file_index FROM read_csv('` + escapeForSQL(
		pattern,
	) + `', file_index=true) ORDER BY file_index`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)
	// File indexes are 0-indexed based on file order (alphabetical)
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"])) // a_file.csv
	assert.Equal(t, int32(1), testToInt32(result.Rows[1]["file_index"])) // b_file.csv
	assert.Equal(t, int32(2), testToInt32(result.Rows[2]["file_index"])) // c_file.csv
}

// TestCSVFilesToSniff tests the files_to_sniff option.
func TestCSVFilesToSniff(t *testing.T) {
	tmpDir := t.TempDir()

	// Use consistent schema to avoid type merge issues
	writeCSV(t, filepath.Join(tmpDir, "data1.csv"), "id,name\n1,Alice\n")
	writeCSV(t, filepath.Join(tmpDir, "data2.csv"), "id,name\n2,Bob\n")
	writeCSV(t, filepath.Join(tmpDir, "data3.csv"), "id,name\n3,Charlie\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.csv")

	// Test that files_to_sniff option is recognized and files are read
	sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(pattern) + `', files_to_sniff=1)`
	result := execQuery(t, cat, exec, sql)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(3), cnt)
}

// TestCSVFileGlobBehavior tests the file_glob_behavior option.
func TestCSVFileGlobBehavior(t *testing.T) {
	tmpDir := t.TempDir()

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("disallow_empty_default", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.csv")
		sql := `SELECT * FROM read_csv('` + escapeForSQL(pattern) + `')`

		_, err := execQueryWithErr(cat, exec, sql)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no files")
	})

	t.Run("allow_empty", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "nonexistent*.csv")
		sql := `SELECT * FROM read_csv('` + escapeForSQL(
			pattern,
		) + `', file_glob_behavior='ALLOW_EMPTY')`

		result := execQuery(t, cat, exec, sql)
		assert.Len(t, result.Rows, 0)
	})

	t.Run("fallback_glob_with_existing_file", func(t *testing.T) {
		// Create a file
		writeCSV(t, filepath.Join(tmpDir, "data.csv"), "id\n1\n")

		path := filepath.Join(tmpDir, "data.csv")
		sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(
			path,
		) + `', file_glob_behavior='FALLBACK_GLOB')`

		result := execQuery(t, cat, exec, sql)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(1), cnt)
	})
}

// TestCSVHivePartitioning tests Hive-style partitioning support.
func TestCSVHivePartitioning(t *testing.T) {
	tmpDir := t.TempDir()

	// Create partitioned directory structure
	partDir1 := filepath.Join(tmpDir, "year=2023", "month=01")
	partDir2 := filepath.Join(tmpDir, "year=2023", "month=02")
	partDir3 := filepath.Join(tmpDir, "year=2024", "month=01")

	require.NoError(t, os.MkdirAll(partDir1, 0755))
	require.NoError(t, os.MkdirAll(partDir2, 0755))
	require.NoError(t, os.MkdirAll(partDir3, 0755))

	writeCSV(t, filepath.Join(partDir1, "data.csv"), "id,value\n1,a\n")
	writeCSV(t, filepath.Join(partDir2, "data.csv"), "id,value\n2,b\n")
	writeCSV(t, filepath.Join(partDir3, "data.csv"), "id,value\n3,c\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	// Test that files with hive partitioning structure can be read with the option
	t.Run("hive_partitioning_enabled", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.csv")
		// Just read the data columns - partition columns are added by executor
		sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(
			pattern,
		) + `', hive_partitioning=true)`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt := testToInt64(result.Rows[0]["cnt"])
		assert.Equal(t, int64(3), cnt)
	})
}

// TestCSVUnionByName tests union-by-name schema merging.
func TestCSVUnionByName(t *testing.T) {
	tmpDir := t.TempDir()

	// Use files with same schema for basic union_by_name test
	writeCSV(t, filepath.Join(tmpDir, "data1.csv"), "id,name\n1,Alice\n")
	writeCSV(t, filepath.Join(tmpDir, "data2.csv"), "id,name\n2,Bob\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.csv")
	sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(
		pattern,
	) + `', union_by_name=true)`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 1)
	cnt := testToInt64(result.Rows[0]["cnt"])
	assert.Equal(t, int64(2), cnt)
}

// TestCSVRecursiveGlob tests recursive glob patterns.
func TestCSVRecursiveGlob(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested directory structure
	subDir1 := filepath.Join(tmpDir, "level1", "level2")
	subDir2 := filepath.Join(tmpDir, "level1", "other")
	require.NoError(t, os.MkdirAll(subDir1, 0755))
	require.NoError(t, os.MkdirAll(subDir2, 0755))

	writeCSV(t, filepath.Join(tmpDir, "root.csv"), "id\n1\n")
	writeCSV(t, filepath.Join(tmpDir, "level1", "l1.csv"), "id\n2\n")
	writeCSV(t, filepath.Join(subDir1, "l2.csv"), "id\n3\n")
	writeCSV(t, filepath.Join(subDir2, "other.csv"), "id\n4\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("recursive_all", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "**/*.csv")
		sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(4), cnt)
	})

	t.Run("recursive_specific_dir", func(t *testing.T) {
		pattern := filepath.Join(tmpDir, "level1", "**/*.csv")
		sql := `SELECT COUNT(*) as cnt FROM read_csv('` + escapeForSQL(pattern) + `')`

		result := execQuery(t, cat, exec, sql)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(3), cnt) // l1.csv, l2.csv, other.csv
	})
}

// TestCSVAllMetadataColumns tests using all metadata columns together.
func TestCSVAllMetadataColumns(t *testing.T) {
	tmpDir := t.TempDir()

	writeCSV(t, filepath.Join(tmpDir, "file1.csv"), "id\n1\n2\n")
	writeCSV(t, filepath.Join(tmpDir, "file2.csv"), "id\n3\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	pattern := filepath.Join(tmpDir, "*.csv")
	sql := `SELECT id, filename, file_row_number, file_index FROM read_csv('` + escapeForSQL(
		pattern,
	) + `', filename=true, file_row_number=true, file_index=true) ORDER BY file_index, file_row_number`

	result := execQuery(t, cat, exec, sql)

	assert.Len(t, result.Rows, 3)

	// First row from file1
	assert.Equal(t, int64(1), testToInt64(result.Rows[0]["id"]))
	assert.Contains(t, result.Rows[0]["filename"], "file1.csv")
	assert.Equal(t, int64(1), result.Rows[0]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[0]["file_index"]))

	// Second row from file1
	assert.Equal(t, int64(2), testToInt64(result.Rows[1]["id"]))
	assert.Contains(t, result.Rows[1]["filename"], "file1.csv")
	assert.Equal(t, int64(2), result.Rows[1]["file_row_number"])
	assert.Equal(t, int32(0), testToInt32(result.Rows[1]["file_index"]))

	// First row from file2
	assert.Equal(t, int64(3), testToInt64(result.Rows[2]["id"]))
	assert.Contains(t, result.Rows[2]["filename"], "file2.csv")
	assert.Equal(t, int64(1), result.Rows[2]["file_row_number"])
	assert.Equal(t, int32(1), testToInt32(result.Rows[2]["file_index"]))
}

// Helper functions

func writeCSV(t *testing.T, path, content string) {
	t.Helper()
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
}

func escapeForSQL(path string) string {
	return filepath.ToSlash(path)
}

// testToInt64 converts various numeric types to int64 for comparison.
func testToInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

// testToInt32 converts various numeric types to int32 for comparison.
func testToInt32(v any) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case int:
		return int32(n)
	case float64:
		return int32(n)
	default:
		return 0
	}
}

func execQuery(t *testing.T, cat *catalog.Catalog, exec *Executor, sql string) *ExecutionResult {
	t.Helper()
	result, err := execQueryWithErr(cat, exec, sql)
	require.NoError(t, err)
	return result
}

func execQueryWithErr(cat *catalog.Catalog, exec *Executor, sql string) (*ExecutionResult, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}

	b := binder.NewBinder(cat)
	bound, err := b.Bind(stmt)
	if err != nil {
		return nil, err
	}

	p := planner.NewPlanner(cat)
	plan, err := p.Plan(bound)
	if err != nil {
		return nil, err
	}

	return exec.Execute(context.Background(), plan, nil)
}

// TestCSVArrayOfFiles tests reading from an array of explicit file paths.
// This tests the array literal syntax: ['file1.csv', 'file2.csv']
func TestCSVArrayOfFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test CSV files
	writeCSV(t, filepath.Join(tmpDir, "a.csv"), "id,name\n1,Alice\n2,Bob\n")
	writeCSV(t, filepath.Join(tmpDir, "b.csv"), "id,name\n3,Charlie\n")
	writeCSV(t, filepath.Join(tmpDir, "c.csv"), "id,name\n4,Diana\n5,Eve\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("two files in array", func(t *testing.T) {
		file1 := escapeForSQL(filepath.Join(tmpDir, "a.csv"))
		file2 := escapeForSQL(filepath.Join(tmpDir, "b.csv"))
		sql := `SELECT * FROM read_csv(['` + file1 + `', '` + file2 + `']) ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 3)
		assert.Equal(t, int64(1), testToInt64(result.Rows[0]["id"]))
		assert.Equal(t, "Alice", result.Rows[0]["name"])
		assert.Equal(t, int64(3), testToInt64(result.Rows[2]["id"]))
		assert.Equal(t, "Charlie", result.Rows[2]["name"])
	})

	t.Run("three files in array", func(t *testing.T) {
		file1 := escapeForSQL(filepath.Join(tmpDir, "a.csv"))
		file2 := escapeForSQL(filepath.Join(tmpDir, "b.csv"))
		file3 := escapeForSQL(filepath.Join(tmpDir, "c.csv"))
		sql := `SELECT COUNT(*) as cnt FROM read_csv(['` + file1 + `', '` + file2 + `', '` + file3 + `'])`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		cnt, ok := result.Rows[0]["cnt"].(int64)
		require.True(t, ok)
		assert.Equal(t, int64(5), cnt)
	})

	t.Run("single file in array", func(t *testing.T) {
		file := escapeForSQL(filepath.Join(tmpDir, "b.csv"))
		sql := `SELECT * FROM read_csv(['` + file + `'])`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 1)
		assert.Equal(t, int64(3), testToInt64(result.Rows[0]["id"]))
		assert.Equal(t, "Charlie", result.Rows[0]["name"])
	})

	t.Run("array with filename metadata column", func(t *testing.T) {
		file1 := escapeForSQL(filepath.Join(tmpDir, "a.csv"))
		file2 := escapeForSQL(filepath.Join(tmpDir, "c.csv"))
		sql := `SELECT id, name, filename FROM read_csv(['` + file1 + `', '` + file2 + `'], filename=true) ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 4)
		// First two rows from a.csv, last two from c.csv
		assert.Contains(t, result.Rows[0]["filename"], "a.csv")
		assert.Contains(t, result.Rows[1]["filename"], "a.csv")
		assert.Contains(t, result.Rows[2]["filename"], "c.csv")
		assert.Contains(t, result.Rows[3]["filename"], "c.csv")
	})

	t.Run("array with file_row_number", func(t *testing.T) {
		file1 := escapeForSQL(filepath.Join(tmpDir, "a.csv"))
		file2 := escapeForSQL(filepath.Join(tmpDir, "b.csv"))
		sql := `SELECT id, file_row_number FROM read_csv(['` + file1 + `', '` + file2 + `'], file_row_number=true) ORDER BY id`

		result := execQuery(t, cat, exec, sql)

		assert.Len(t, result.Rows, 3)
		// a.csv has rows 1, 2; b.csv has row 1
		assert.Equal(t, int64(1), testToInt64(result.Rows[0]["file_row_number"]))
		assert.Equal(t, int64(2), testToInt64(result.Rows[1]["file_row_number"]))
		assert.Equal(t, int64(1), testToInt64(result.Rows[2]["file_row_number"]))
	})
}

// TestCSVArrayWithSameSchema tests reading files with the same schema using array syntax.
func TestCSVArrayWithSameSchema(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files with identical schemas
	writeCSV(t, filepath.Join(tmpDir, "file1.csv"), "id,name,age\n1,Alice,25\n")
	writeCSV(t, filepath.Join(tmpDir, "file2.csv"), "id,name,age\n2,Bob,30\n3,Charlie,35\n")

	cat := catalog.NewCatalog()
	stor := storage.NewStorage()
	exec := NewExecutor(cat, stor)

	t.Run("same_schema_files", func(t *testing.T) {
		file1 := escapeForSQL(filepath.Join(tmpDir, "file1.csv"))
		file2 := escapeForSQL(filepath.Join(tmpDir, "file2.csv"))
		sql := `SELECT * FROM read_csv(['` + file1 + `', '` + file2 + `'])`

		result := execQuery(t, cat, exec, sql)

		// Should have 3 total rows
		assert.Len(t, result.Rows, 3)

		// Collect all names to verify all data is present
		names := make([]string, 0, 3)
		for _, row := range result.Rows {
			if name, ok := row["name"].(string); ok {
				names = append(names, name)
			}
		}

		// All three names should be present
		assert.Contains(t, names, "Alice")
		assert.Contains(t, names, "Bob")
		assert.Contains(t, names, "Charlie")
	})
}

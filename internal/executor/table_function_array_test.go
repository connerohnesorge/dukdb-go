package executor_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArraySyntaxCSV tests array syntax for read_csv table function.
func TestArraySyntaxCSV(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "array_test_csv_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test CSV files
	file1 := filepath.Join(tmpDir, "file1.csv")
	file2 := filepath.Join(tmpDir, "file2.csv")

	err = os.WriteFile(file1, []byte("id,name\n1,Alice\n2,Bob\n"), 0644)
	require.NoError(t, err)

	err = os.WriteFile(file2, []byte("id,name\n3,Charlie\n4,Diana\n"), 0644)
	require.NoError(t, err)

	// Test using array syntax via database/sql
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Execute query with array syntax
	query := "SELECT * FROM read_csv(['" + file1 + "', '" + file2 + "']) ORDER BY id"
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	// Collect results
	var results []struct {
		ID   int64
		Name string
	}
	for rows.Next() {
		var r struct {
			ID   int64
			Name string
		}
		err := rows.Scan(&r.ID, &r.Name)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Verify we got all 4 rows from both files
	assert.Len(t, results, 4)

	// Verify the data is correct and ordered
	if len(results) >= 4 {
		assert.Equal(t, int64(1), results[0].ID)
		assert.Equal(t, "Alice", results[0].Name)
		assert.Equal(t, int64(2), results[1].ID)
		assert.Equal(t, "Bob", results[1].Name)
		assert.Equal(t, int64(3), results[2].ID)
		assert.Equal(t, "Charlie", results[2].Name)
		assert.Equal(t, int64(4), results[3].ID)
		assert.Equal(t, "Diana", results[3].Name)
	}
}

// TestArraySyntaxCSVWithFilename tests array syntax with filename metadata column.
func TestArraySyntaxCSVWithFilename(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "array_test_csv_filename_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test CSV files
	file1 := filepath.Join(tmpDir, "alpha.csv")
	file2 := filepath.Join(tmpDir, "beta.csv")

	err = os.WriteFile(file1, []byte("value\n100\n"), 0644)
	require.NoError(t, err)

	err = os.WriteFile(file2, []byte("value\n200\n"), 0644)
	require.NoError(t, err)

	// Test using array syntax via database/sql
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Execute query with array syntax and filename option
	query := "SELECT * FROM read_csv(['" + file1 + "', '" + file2 + "'], filename=true) ORDER BY value"
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	require.NoError(t, err)

	// Verify filename column is present
	hasFilename := false
	for _, col := range columns {
		if col == "filename" {
			hasFilename = true
			break
		}
	}
	assert.True(t, hasFilename, "expected filename column")

	// Count rows
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())

	// Verify we got 2 rows
	assert.Equal(t, 2, count)
}

// TestArraySyntaxJSON tests array syntax for read_json table function.
func TestArraySyntaxJSON(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "array_test_json_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test JSON files
	file1 := filepath.Join(tmpDir, "data1.json")
	file2 := filepath.Join(tmpDir, "data2.json")

	err = os.WriteFile(file1, []byte(`[{"id": 1, "name": "One"}]`), 0644)
	require.NoError(t, err)

	err = os.WriteFile(file2, []byte(`[{"id": 2, "name": "Two"}]`), 0644)
	require.NoError(t, err)

	// Test using array syntax via database/sql
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Execute query with array syntax
	query := "SELECT * FROM read_json(['" + file1 + "', '" + file2 + "']) ORDER BY id"
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	// Count rows
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())

	// Verify we got all rows from both files
	assert.Equal(t, 2, count)
}

// TestArraySyntaxWithGlobs tests array syntax with glob patterns inside the array.
func TestArraySyntaxWithGlobs(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "array_test_glob_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create subdirectories
	subDir1 := filepath.Join(tmpDir, "subdir1")
	subDir2 := filepath.Join(tmpDir, "subdir2")
	err = os.MkdirAll(subDir1, 0755)
	require.NoError(t, err)
	err = os.MkdirAll(subDir2, 0755)
	require.NoError(t, err)

	// Create test CSV files in subdirectories
	err = os.WriteFile(filepath.Join(subDir1, "a.csv"), []byte("val\n1\n"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(subDir1, "b.csv"), []byte("val\n2\n"), 0644)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(subDir2, "c.csv"), []byte("val\n3\n"), 0644)
	require.NoError(t, err)

	// Test using array syntax via database/sql
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Execute query with array containing glob patterns
	globPattern1 := filepath.Join(subDir1, "*.csv")
	globPattern2 := filepath.Join(subDir2, "*.csv")
	query := "SELECT * FROM read_csv(['" + globPattern1 + "', '" + globPattern2 + "']) ORDER BY val"
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	// Count rows
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())

	// Verify we got all 3 rows from all files (glob patterns expanded)
	assert.Equal(t, 3, count)
}

// TestEmptyArraySyntax tests that empty arrays are handled correctly.
func TestEmptyArraySyntax(t *testing.T) {
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Execute query with empty array - should error due to no files
	query := "SELECT * FROM read_csv([])"
	_, err = db.Query(query)
	// Empty array should result in an error (no files to read or empty path)
	assert.Error(t, err)
}

// TestSingleElementArray tests array syntax with a single element.
func TestSingleElementArray(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "array_test_single_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create test CSV file
	file1 := filepath.Join(tmpDir, "single.csv")
	err = os.WriteFile(file1, []byte("id\n1\n2\n"), 0644)
	require.NoError(t, err)

	// Test using array syntax via database/sql
	db, err := sql.Open("dukdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Execute query with single element array
	query := "SELECT * FROM read_csv(['" + file1 + "'])"
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()

	// Count rows
	count := 0
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())

	// Verify we got 2 rows
	assert.Equal(t, 2, count)
}

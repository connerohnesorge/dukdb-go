package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListBasicUncompressed tests reading simple uncompressed LIST values.
func TestListBasicUncompressed(t *testing.T) {
	skipIfNoDuckDBCLI(t)

	// Create test file with a single LIST value (should use UNCOMPRESSED)
	dbPath, cleanup := createDuckDBTestFile(t, `
		CREATE TABLE list_test (
			int_list INTEGER[]
		);
		INSERT INTO list_test VALUES
			([1, 2, 3]);
	`)
	defer cleanup()

	// Open with dukdb-go
	storage, err := OpenDuckDBStorage(dbPath, &Config{ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// Scan table
	iter, err := storage.ScanTable("main", "list_test", nil)
	require.NoError(t, err)
	defer iter.Close()

	// Should have one row
	require.True(t, iter.Next(), "expected at least one row")
	row := iter.Row()

	// Verify the LIST value
	require.Len(t, row, 1, "expected 1 column")
	listVal, ok := row[0].([]interface{})
	require.True(t, ok, "expected []interface{}, got %T", row[0])
	require.Len(t, listVal, 3, "expected 3 elements")

	// Check values
	assert.Equal(t, int32(1), listVal[0])
	assert.Equal(t, int32(2), listVal[1])
	assert.Equal(t, int32(3), listVal[2])

	// Should be no more rows
	assert.False(t, iter.Next(), "expected no more rows")
	require.NoError(t, iter.Err())
}

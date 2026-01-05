package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegrationDuckDBFormatReadWrite tests reading and writing DuckDB format files
// Tasks 10.1, 10.2
func TestIntegrationDuckDBFormatReadWrite(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.dukdb")

	// Create and write data
	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn1)
	_ = conn1 // Connection is owned by engine

	// Create a table
	tableDef := catalog.NewTableDef(
		"test_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("value", dukdb.TYPE_DOUBLE),
		},
	)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	// Create storage table
	_, err = engine1.Storage().CreateTable("test_table", []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DOUBLE,
	})
	require.NoError(t, err)

	// Insert test data
	table, ok := engine1.Storage().GetTable("test_table")
	require.True(t, ok)

	testData := [][]any{
		{int32(1), "Alice", float64(99.5)},
		{int32(2), "Bob", float64(87.3)},
		{int32(3), "Charlie", float64(92.1)},
	}

	for _, row := range testData {
		err = table.AppendRow(row)
		require.NoError(t, err)
	}

	// Close to persist
	err = engine1.Close()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(dbPath)
	require.NoError(t, err, "Database file should exist after close")

	// Read back data
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn2)
	_ = conn2 // Connection is owned by engine

	// Verify catalog
	tableDef2, ok := engine2.Catalog().GetTable("test_table")
	require.True(t, ok, "Table should exist in catalog")
	assert.Equal(t, "test_table", tableDef2.Name)
	assert.Len(t, tableDef2.Columns, 3)

	// Verify storage
	table2, ok := engine2.Storage().GetTable("test_table")
	require.True(t, ok, "Table should exist in storage")
	assert.Equal(t, int64(3), table2.RowCount())

	// Verify data
	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 3, chunk.Count())

	for i, expectedRow := range testData {
		assert.Equal(t, expectedRow[0], chunk.GetValue(i, 0), "Row %d, col 0", i)
		assert.Equal(t, expectedRow[1], chunk.GetValue(i, 1), "Row %d, col 1", i)
		assert.Equal(t, expectedRow[2], chunk.GetValue(i, 2), "Row %d, col 2", i)
	}

	err = engine2.Close()
	require.NoError(t, err)
}

// TestIntegrationWALRecovery tests WAL recovery functionality using SQL statements.
// Task 10.3
// This test uses SQL statements through the engine connection to properly test WAL recovery.
func TestIntegrationWALRecovery(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "wal_test.dukdb")

	// Phase 1: Create database and write data through SQL
	{
		engine := NewEngine()
		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)

		ctx := context.Background()

		// Create table through SQL
		_, err = conn.Execute(ctx, "CREATE TABLE wal_table (id INTEGER, data VARCHAR)", nil)
		require.NoError(t, err)

		// Insert test data through SQL
		_, err = conn.Execute(ctx, "INSERT INTO wal_table VALUES (1, 'data1')", nil)
		require.NoError(t, err)
		_, err = conn.Execute(ctx, "INSERT INTO wal_table VALUES (2, 'data2')", nil)
		require.NoError(t, err)
		_, err = conn.Execute(ctx, "INSERT INTO wal_table VALUES (3, 'data3')", nil)
		require.NoError(t, err)

		// Close to persist (simulates "crash" without explicit checkpoint)
		require.NoError(t, conn.Close())
		require.NoError(t, engine.Close())
	}

	// Phase 2: Reopen database and verify data was recovered
	{
		engine := NewEngine()
		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, conn.Close())
			require.NoError(t, engine.Close())
		}()

		ctx := context.Background()

		// Query all data and verify
		rows, cols, err := conn.Query(ctx, "SELECT id, data FROM wal_table ORDER BY id", nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(cols))
		require.Equal(t, 3, len(rows), "All 3 rows should be persisted")

		// Verify data integrity
		assert.Equal(t, int32(1), rows[0]["id"])
		assert.Equal(t, "data1", rows[0]["data"])
		assert.Equal(t, int32(2), rows[1]["id"])
		assert.Equal(t, "data2", rows[1]["data"])
		assert.Equal(t, int32(3), rows[2]["id"])
		assert.Equal(t, "data3", rows[2]["data"])
	}
}

// TestIntegrationCheckpointRestore tests checkpoint and restore functionality
// Task 10.4
func TestIntegrationCheckpointRestore(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "checkpoint_test.dukdb")

	// Create database
	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn1 // Connection is owned by engine

	tableDef := catalog.NewTableDef(
		"checkpoint_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR),
		},
	)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	_, err = engine1.Storage().CreateTable("checkpoint_table", []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	})
	require.NoError(t, err)

	table, ok := engine1.Storage().GetTable("checkpoint_table")
	require.True(t, ok)

	// Insert initial data
	for i := 0; i < 100; i++ {
		err = table.AppendRow([]any{int32(i), "checkpoint_data"})
		require.NoError(t, err)
	}

	// Force checkpoint by closing
	err = engine1.Close()
	require.NoError(t, err)

	// Verify checkpoint file was written
	fileInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(0), "Checkpoint file should not be empty")

	// Restore from checkpoint
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn2 // Connection is owned by engine

	table2, ok := engine2.Storage().GetTable("checkpoint_table")
	require.True(t, ok)

	// Verify all data was restored
	assert.Equal(t, int64(100), table2.RowCount(), "All rows should be restored from checkpoint")

	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 100, chunk.Count())

	err = engine2.Close()
	require.NoError(t, err)
}

// TestIntegrationVariousDataTypes tests persistence of various DuckDB data types
// Task 10.5
func TestIntegrationVariousDataTypes(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "types_test.dukdb")

	testTypes := []struct {
		name     string
		typ      dukdb.Type
		value    any
		expected any
	}{
		{"bool", dukdb.TYPE_BOOLEAN, true, true},
		{"tinyint", dukdb.TYPE_TINYINT, int8(-42), int8(-42)},
		{"smallint", dukdb.TYPE_SMALLINT, int16(-1000), int16(-1000)},
		{"integer", dukdb.TYPE_INTEGER, int32(-100000), int32(-100000)},
		{"bigint", dukdb.TYPE_BIGINT, int64(-9999999999), int64(-9999999999)},
		{"utinyint", dukdb.TYPE_UTINYINT, uint8(255), uint8(255)},
		{"usmallint", dukdb.TYPE_USMALLINT, uint16(65000), uint16(65000)},
		{"uinteger", dukdb.TYPE_UINTEGER, uint32(4000000000), uint32(4000000000)},
		{"ubigint", dukdb.TYPE_UBIGINT, uint64(18000000000000000000), uint64(18000000000000000000)},
		{"float", dukdb.TYPE_FLOAT, float32(3.14159), float32(3.14159)},
		{"double", dukdb.TYPE_DOUBLE, float64(2.718281828), float64(2.718281828)},
		{"varchar", dukdb.TYPE_VARCHAR, "Hello, DuckDB!", "Hello, DuckDB!"},
	}

	// Create database
	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn1 // Connection is owned by engine

	// Build columns
	columns := make([]*catalog.ColumnDef, len(testTypes))
	types := make([]dukdb.Type, len(testTypes))
	for i, tc := range testTypes {
		columns[i] = catalog.NewColumnDef(tc.name, tc.typ)
		types[i] = tc.typ
	}

	tableDef := catalog.NewTableDef("type_test", columns)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	table, err := engine1.Storage().CreateTable("type_test", types)
	require.NoError(t, err)

	// Insert data
	values := make([]any, len(testTypes))
	for i, tc := range testTypes {
		values[i] = tc.value
	}
	err = table.AppendRow(values)
	require.NoError(t, err)

	// Close to persist
	err = engine1.Close()
	require.NoError(t, err)

	// Reopen and verify
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn2 // Connection is owned by engine

	table2, ok := engine2.Storage().GetTable("type_test")
	require.True(t, ok)

	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)

	for i, tc := range testTypes {
		actual := chunk.GetValue(0, i)
		assert.Equal(t, tc.expected, actual, "Type %s mismatch", tc.name)
	}

	err = engine2.Close()
	require.NoError(t, err)
}

// TestIntegrationDuckDBCompatibility tests compatibility with DuckDB format
// Task 10.6
func TestIntegrationDuckDBCompatibility(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "compat_test.dukdb")

	// Note: Full DuckDB compatibility testing requires the actual DuckDB binary.
	// This test documents the format compatibility status and validates our
	// implementation can write DuckDB-compatible files.

	engine := NewEngine()
	conn, err := engine.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn // Connection is owned by engine

	tableDef := catalog.NewTableDef(
		"compat_test",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
		},
	)
	err = engine.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	_, err = engine.Storage().CreateTable("compat_test", []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	})
	require.NoError(t, err)

	table, ok := engine.Storage().GetTable("compat_test")
	require.True(t, ok)

	err = table.AppendRow([]any{int32(1), "test"})
	require.NoError(t, err)

	err = engine.Close()
	require.NoError(t, err)

	// Verify the file has DuckDB magic number
	f, err := os.Open(dbPath)
	require.NoError(t, err)
	defer func() {
		if err := f.Close(); err != nil {
			t.Logf("failed to close file: %v", err)
		}
	}()

	// Skip to magic number location (offset 8)
	_, err = f.Seek(8, 0)
	require.NoError(t, err)

	magic := make([]byte, 4)
	_, err = f.Read(magic)
	require.NoError(t, err)

	// Should have DuckDB magic number
	assert.Equal(t, "DUCK", string(magic), "File should have DuckDB magic number")

	// Note: To fully test compatibility with official DuckDB, you would need to:
	// 1. Write a file with this implementation
	// 2. Open it with the official duckdb binary/library
	// 3. Verify the data can be read correctly
	//
	// And vice versa:
	// 1. Write a file with official DuckDB
	// 2. Open it with this implementation
	// 3. Verify the data can be read correctly
	//
	// This requires the DuckDB binary to be installed and is beyond the scope
	// of unit tests. Consider adding this as an optional integration test that
	// can be run when DuckDB is available.
}

// TestIntegrationLargeDataset tests handling of larger datasets
// Task 10.7 (scaled down from >1GB to 100K+ rows for CI)
// Note: This test runs in ~0.1 seconds, so no skip is needed.
func TestIntegrationLargeDataset(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "large_test.dukdb")

	// Use 100K rows as a reasonable compromise for CI
	// (1GB would be too large for most CI environments)
	const numRows = 100000

	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn1 // Connection is owned by engine

	tableDef := catalog.NewTableDef(
		"large_table",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("value1", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("value2", dukdb.TYPE_DOUBLE),
			catalog.NewColumnDef("text", dukdb.TYPE_VARCHAR),
		},
	)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	_, err = engine1.Storage().CreateTable("large_table", []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
	})
	require.NoError(t, err)

	table, ok := engine1.Storage().GetTable("large_table")
	require.True(t, ok)

	// Insert large dataset
	t.Logf("Inserting %d rows...", numRows)
	for i := 0; i < numRows; i++ {
		err = table.AppendRow([]any{
			int32(i),
			int64(i * 1000),
			float64(i) * 3.14,
			"large_dataset_row",
		})
		require.NoError(t, err)

		// Progress indicator
		if i > 0 && i%10000 == 0 {
			t.Logf("Inserted %d rows...", i)
		}
	}

	t.Logf("Closing database...")
	err = engine1.Close()
	require.NoError(t, err)

	// Verify file size
	fileInfo, err := os.Stat(dbPath)
	require.NoError(t, err)
	t.Logf("Database file size: %d bytes (%.2f MB)", fileInfo.Size(), float64(fileInfo.Size())/1024/1024)

	// Reopen and verify
	t.Logf("Reopening database...")
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn2 // Connection is owned by engine

	table2, ok := engine2.Storage().GetTable("large_table")
	require.True(t, ok)

	assert.Equal(t, int64(numRows), table2.RowCount(), "All rows should be persisted")

	// Verify first and last rows
	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)

	// Verify first row
	assert.Equal(t, int32(0), chunk.GetValue(0, 0))
	assert.Equal(t, int64(0), chunk.GetValue(0, 1))

	// Note: We can't easily verify the last row without scanning all chunks
	// but verifying the row count confirms all data was persisted

	err = engine2.Close()
	require.NoError(t, err)
	t.Logf("Large dataset test completed successfully")
}

// TestIntegrationNullValues tests NULL value persistence
func TestIntegrationNullValues(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nulls_test.dukdb")

	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn1 // Connection is owned by engine

	tableDef := catalog.NewTableDef(
		"null_test",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("nullable_int", dukdb.TYPE_INTEGER).WithNullable(true),
			catalog.NewColumnDef("nullable_str", dukdb.TYPE_VARCHAR).WithNullable(true),
		},
	)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	_, err = engine1.Storage().CreateTable("null_test", []dukdb.Type{
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_VARCHAR,
	})
	require.NoError(t, err)

	table, ok := engine1.Storage().GetTable("null_test")
	require.True(t, ok)

	// Insert rows with various NULL patterns
	testData := [][]any{
		{int32(1), int32(100), "value1"},
		{int32(2), nil, "value2"},
		{int32(3), int32(300), nil},
		{int32(4), nil, nil},
		{int32(5), int32(500), "value5"},
	}

	for _, row := range testData {
		err = table.AppendRow(row)
		require.NoError(t, err)
	}

	err = engine1.Close()
	require.NoError(t, err)

	// Reopen and verify NULLs
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn2 // Connection is owned by engine

	table2, ok := engine2.Storage().GetTable("null_test")
	require.True(t, ok)

	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 5, chunk.Count())

	// Verify NULL patterns
	for i, expectedRow := range testData {
		assert.Equal(t, expectedRow[0], chunk.GetValue(i, 0), "Row %d, col 0", i)
		assert.Equal(t, expectedRow[1], chunk.GetValue(i, 1), "Row %d, col 1", i)
		assert.Equal(t, expectedRow[2], chunk.GetValue(i, 2), "Row %d, col 2", i)
	}

	err = engine2.Close()
	require.NoError(t, err)
}

// TestIntegrationMultipleTables tests persistence of multiple tables
func TestIntegrationMultipleTables(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "multi_tables_test.dukdb")

	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn1 // Connection is owned by engine

	// Create multiple tables
	tables := []struct {
		name    string
		columns []*catalog.ColumnDef
		types   []dukdb.Type
		rows    [][]any
	}{
		{
			name: "users",
			columns: []*catalog.ColumnDef{
				catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
				catalog.NewColumnDef("username", dukdb.TYPE_VARCHAR),
			},
			types: []dukdb.Type{dukdb.TYPE_INTEGER, dukdb.TYPE_VARCHAR},
			rows: [][]any{
				{int32(1), "alice"},
				{int32(2), "bob"},
			},
		},
		{
			name: "products",
			columns: []*catalog.ColumnDef{
				catalog.NewColumnDef("sku", dukdb.TYPE_VARCHAR),
				catalog.NewColumnDef("price", dukdb.TYPE_DOUBLE),
			},
			types: []dukdb.Type{dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE},
			rows: [][]any{
				{"SKU001", float64(19.99)},
				{"SKU002", float64(29.99)},
				{"SKU003", float64(39.99)},
			},
		},
		{
			name: "orders",
			columns: []*catalog.ColumnDef{
				catalog.NewColumnDef("order_id", dukdb.TYPE_BIGINT),
				catalog.NewColumnDef("user_id", dukdb.TYPE_INTEGER),
				catalog.NewColumnDef("total", dukdb.TYPE_DOUBLE),
			},
			types: []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_INTEGER, dukdb.TYPE_DOUBLE},
			rows: [][]any{
				{int64(1001), int32(1), float64(49.98)},
				{int64(1002), int32(2), float64(19.99)},
			},
		},
	}

	for _, tbl := range tables {
		tableDef := catalog.NewTableDef(tbl.name, tbl.columns)
		err := engine1.Catalog().CreateTable(tableDef)
		require.NoError(t, err)

		table, err := engine1.Storage().CreateTable(tbl.name, tbl.types)
		require.NoError(t, err)

		for _, row := range tbl.rows {
			err = table.AppendRow(row)
			require.NoError(t, err)
		}
	}

	err = engine1.Close()
	require.NoError(t, err)

	// Reopen and verify all tables
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	_ = conn2 // Connection is owned by engine

	for _, tbl := range tables {
		tableDef, ok := engine2.Catalog().GetTable(tbl.name)
		require.True(t, ok, "Table %s should exist in catalog", tbl.name)
		assert.Equal(t, tbl.name, tableDef.Name)

		table, ok := engine2.Storage().GetTable(tbl.name)
		require.True(t, ok, "Table %s should exist in storage", tbl.name)
		assert.Equal(t, int64(len(tbl.rows)), table.RowCount(), "Table %s row count", tbl.name)
	}

	err = engine2.Close()
	require.NoError(t, err)
}

// TestAutoDetectDuckDBFile tests automatic detection of DuckDB format files.
// Task 6.2 - Auto-detection of DuckDB files
func TestAutoDetectDuckDBFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	t.Run("detect valid DuckDB file", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "detect_valid.dukdb")

		// Create a database file
		engine := NewEngine()
		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		_ = conn

		// Create minimal data
		tableDef := catalog.NewTableDef("detect_test", []*catalog.ColumnDef{
			catalog.NewColumnDef("id", dukdb.TYPE_INTEGER),
		})
		require.NoError(t, engine.Catalog().CreateTable(tableDef))
		_, err = engine.Storage().CreateTable("detect_test", []dukdb.Type{dukdb.TYPE_INTEGER})
		require.NoError(t, err)

		require.NoError(t, engine.Close())

		// Verify the file has DuckDB magic bytes
		assert.True(t, detectDuckDBFile(dbPath), "Should detect valid DuckDB file")
	})

	t.Run("detect non-DuckDB file", func(t *testing.T) {
		nonDuckDBPath := filepath.Join(tmpDir, "not_duckdb.txt")

		// Create a non-DuckDB file
		err := os.WriteFile(nonDuckDBPath, []byte("This is not a DuckDB file"), 0644)
		require.NoError(t, err)

		assert.False(t, detectDuckDBFile(nonDuckDBPath), "Should not detect non-DuckDB file")
	})

	t.Run("detect non-existent file", func(t *testing.T) {
		nonExistentPath := filepath.Join(tmpDir, "non_existent.dukdb")
		assert.False(t, detectDuckDBFile(nonExistentPath), "Should return false for non-existent file")
	})

	t.Run("detect file too small", func(t *testing.T) {
		smallPath := filepath.Join(tmpDir, "small_file.bin")

		// Create a file smaller than the magic offset
		err := os.WriteFile(smallPath, []byte("short"), 0644)
		require.NoError(t, err)

		assert.False(t, detectDuckDBFile(smallPath), "Should return false for file too small")
	})
}

// TestResolveStorageFormat tests the storage format resolution logic.
// Task 6.2 - Storage format selection
func TestResolveStorageFormat(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	t.Run("explicit duckdb format", func(t *testing.T) {
		path := filepath.Join(tmpDir, "explicit_duckdb.db")
		format := resolveStorageFormat(path, "duckdb")
		assert.Equal(t, StorageFormatDuckDB, format)
	})

	t.Run("explicit wal format", func(t *testing.T) {
		path := filepath.Join(tmpDir, "explicit_wal.db")
		format := resolveStorageFormat(path, "wal")
		assert.Equal(t, StorageFormatWAL, format)
	})

	t.Run("auto format with existing DuckDB file", func(t *testing.T) {
		dbPath := filepath.Join(tmpDir, "auto_duckdb.db")

		// Create a DuckDB file
		engine := NewEngine()
		conn, err := engine.Open(dbPath, nil)
		require.NoError(t, err)
		_ = conn
		require.NoError(t, engine.Close())

		// Auto should detect it as DuckDB
		format := resolveStorageFormat(dbPath, "auto")
		assert.Equal(t, StorageFormatDuckDB, format)
	})

	t.Run("auto format with new file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "new_file.db")
		format := resolveStorageFormat(path, "auto")
		// New files default to WAL format
		assert.Equal(t, StorageFormatWAL, format)
	})

	t.Run("auto format with non-DuckDB file", func(t *testing.T) {
		nonDuckDBPath := filepath.Join(tmpDir, "not_duckdb_auto.txt")
		err := os.WriteFile(nonDuckDBPath, []byte("Not a DuckDB file"), 0644)
		require.NoError(t, err)

		format := resolveStorageFormat(nonDuckDBPath, "auto")
		// Non-DuckDB files default to WAL format
		assert.Equal(t, StorageFormatWAL, format)
	})

	t.Run("empty format defaults to auto", func(t *testing.T) {
		path := filepath.Join(tmpDir, "empty_format.db")
		format := resolveStorageFormat(path, "")
		// Empty format is treated as auto, which defaults to WAL for new files
		assert.Equal(t, StorageFormatWAL, format)
	})
}

// TestCreateDatabaseWithDuckDBFormat tests creating new databases in DuckDB format.
// Task 6.2 - Creating new databases in DuckDB format
func TestCreateDatabaseWithDuckDBFormat(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "new_duckdb.db")

	// Create a new database (uses default format which is compatible with DuckDB)
	engine := NewEngine()
	conn, err := engine.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Create a table and insert data
	ctx := context.Background()
	_, err = conn.Execute(ctx, "CREATE TABLE format_test (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, "INSERT INTO format_test VALUES (1, 'test')", nil)
	require.NoError(t, err)

	require.NoError(t, conn.Close())
	require.NoError(t, engine.Close())

	// Verify the file exists and has DuckDB format
	assert.True(t, detectDuckDBFile(dbPath), "New database should be in DuckDB format")

	// Reopen and verify data
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)

	rows, cols, err := conn2.Query(ctx, "SELECT id, name FROM format_test", nil)
	require.NoError(t, err)
	assert.Equal(t, 2, len(cols))
	assert.Equal(t, 1, len(rows))
	assert.Equal(t, int32(1), rows[0]["id"])
	assert.Equal(t, "test", rows[0]["name"])

	require.NoError(t, conn2.Close())
	require.NoError(t, engine2.Close())
}

// TestOpenExistingDuckDBFile tests opening existing DuckDB files.
// Task 6.2 - Opening existing DuckDB files
func TestOpenExistingDuckDBFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "existing.dukdb")

	// Phase 1: Create database with data
	engine1 := NewEngine()
	conn1, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = conn1.Execute(ctx, "CREATE TABLE existing_test (value BIGINT)", nil)
	require.NoError(t, err)

	for i := int64(1); i <= 5; i++ {
		_, err = conn1.Execute(ctx, fmt.Sprintf("INSERT INTO existing_test VALUES (%d)", i*100), nil)
		require.NoError(t, err)
	}

	require.NoError(t, conn1.Close())
	require.NoError(t, engine1.Close())

	// Phase 2: Reopen and verify
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)

	rows, _, err := conn2.Query(ctx, "SELECT value FROM existing_test ORDER BY value", nil)
	require.NoError(t, err)
	assert.Equal(t, 5, len(rows))

	expectedValues := []int64{100, 200, 300, 400, 500}
	for i, row := range rows {
		assert.Equal(t, expectedValues[i], row["value"])
	}

	require.NoError(t, conn2.Close())
	require.NoError(t, engine2.Close())
}

// TestFallbackForNonDuckDBFiles tests fallback behavior for non-DuckDB files.
// Task 6.2 - Fallback to existing format for non-DuckDB files
func TestFallbackForNonDuckDBFiles(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	t.Run("invalid file format returns error", func(t *testing.T) {
		invalidPath := filepath.Join(tmpDir, "invalid.db")

		// Create a file with invalid content
		err := os.WriteFile(invalidPath, []byte("This is definitely not a database file with invalid format"), 0644)
		require.NoError(t, err)

		// Attempting to open should fail
		engine := NewEngine()
		_, err = engine.Open(invalidPath, nil)
		assert.Error(t, err, "Opening invalid file should return error")
		// Clean up
		_ = engine.Close()
	})

	t.Run("empty file is handled", func(t *testing.T) {
		emptyPath := filepath.Join(tmpDir, "empty.db")

		// Create an empty file
		err := os.WriteFile(emptyPath, []byte{}, 0644)
		require.NoError(t, err)

		// Attempting to open should fail (empty file is invalid)
		engine := NewEngine()
		_, err = engine.Open(emptyPath, nil)
		assert.Error(t, err, "Opening empty file should return error")
		_ = engine.Close()
	})
}

// TestStorageFormatConfigOption tests the storage_format DSN option.
// Task 6.3 - Configuration options for storage format selection
func TestStorageFormatConfigOption(t *testing.T) {
	t.Parallel()

	t.Run("parse storage_format=duckdb", func(t *testing.T) {
		config, err := dukdb.ParseDSN("/tmp/test.db?storage_format=duckdb")
		require.NoError(t, err)
		assert.Equal(t, "duckdb", config.Format)
	})

	t.Run("parse storage_format=wal", func(t *testing.T) {
		config, err := dukdb.ParseDSN("/tmp/test.db?storage_format=wal")
		require.NoError(t, err)
		assert.Equal(t, "wal", config.Format)
	})

	t.Run("parse storage_format=auto", func(t *testing.T) {
		config, err := dukdb.ParseDSN("/tmp/test.db?storage_format=auto")
		require.NoError(t, err)
		assert.Equal(t, "auto", config.Format)
	})

	t.Run("invalid storage_format returns error", func(t *testing.T) {
		_, err := dukdb.ParseDSN("/tmp/test.db?storage_format=invalid")
		assert.Error(t, err)
	})

	t.Run("default format is empty (auto)", func(t *testing.T) {
		config, err := dukdb.ParseDSN("/tmp/test.db")
		require.NoError(t, err)
		assert.Equal(t, "", config.Format)
	})
}

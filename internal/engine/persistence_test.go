package engine

import (
	"path/filepath"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.dukdb")

	// Create engine and add data
	engine1 := NewEngine()

	conn, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Create a table in the catalog
	tableDef := catalog.NewTableDef(
		"users",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef(
				"name",
				dukdb.TYPE_VARCHAR,
			),
		},
	)

	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	// Create table in storage
	_, err = engine1.Storage().
		CreateTable("users", []dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
		})
	require.NoError(t, err)

	// Add some data
	table, ok := engine1.Storage().
		GetTable("users")
	require.True(t, ok)

	err = table.AppendRow(
		[]any{int32(1), "Alice"},
	)
	require.NoError(t, err)
	err = table.AppendRow([]any{int32(2), "Bob"})
	require.NoError(t, err)
	err = table.AppendRow(
		[]any{int32(3), "Charlie"},
	)
	require.NoError(t, err)

	// Close engine (should persist to file)
	err = engine1.Close()
	require.NoError(t, err)

	// Create new engine and load from file
	engine2 := NewEngine()

	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	// Verify catalog
	tableDef2, ok := engine2.Catalog().
		GetTable("users")
	require.True(t, ok)
	assert.Equal(t, "users", tableDef2.Name)
	assert.Len(t, tableDef2.Columns, 2)
	assert.Equal(
		t,
		"id",
		tableDef2.Columns[0].Name,
	)
	assert.Equal(
		t,
		dukdb.TYPE_INTEGER,
		tableDef2.Columns[0].Type,
	)
	assert.Equal(
		t,
		"name",
		tableDef2.Columns[1].Name,
	)
	assert.Equal(
		t,
		dukdb.TYPE_VARCHAR,
		tableDef2.Columns[1].Type,
	)

	// Verify storage
	table2, ok := engine2.Storage().
		GetTable("users")
	require.True(t, ok)
	assert.Equal(t, int64(3), table2.RowCount())

	// Scan and verify data
	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)
	assert.Equal(t, 3, chunk.Count())

	// Verify row values
	assert.Equal(
		t,
		int32(1),
		chunk.GetValue(0, 0),
	)
	assert.Equal(t, "Alice", chunk.GetValue(0, 1))
	assert.Equal(
		t,
		int32(2),
		chunk.GetValue(1, 0),
	)
	assert.Equal(t, "Bob", chunk.GetValue(1, 1))
	assert.Equal(
		t,
		int32(3),
		chunk.GetValue(2, 0),
	)
	assert.Equal(
		t,
		"Charlie",
		chunk.GetValue(2, 1),
	)

	// Close second engine
	err = engine2.Close()
	require.NoError(t, err)
}

func TestMemoryDatabaseNotPersisted(
	t *testing.T,
) {
	t.Parallel()

	engine := NewEngine()

	// Open in-memory database
	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Create table and add data
	tableDef := catalog.NewTableDef(
		"test",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"x",
				dukdb.TYPE_INTEGER,
			),
		},
	)
	err = engine.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	_, err = engine.Storage().
		CreateTable("test", []dukdb.Type{dukdb.TYPE_INTEGER})
	require.NoError(t, err)

	// Close engine - should not try to persist
	err = engine.Close()
	require.NoError(t, err)

	// Engine should be marked as non-persistent
	assert.False(t, engine.persistent)
}

func TestEmptyDatabasePersistence(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "empty.dukdb")

	// Create and close empty database
	engine1 := NewEngine()
	conn, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	err = engine1.Close()
	require.NoError(t, err)

	// Reopen
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	// Should have default main schema but no tables
	tables := engine2.Catalog().ListTables()
	assert.Empty(t, tables)

	err = engine2.Close()
	require.NoError(t, err)
}

func TestMultipleTablesPersistence(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "multi.dukdb")

	// Create engine with multiple tables
	engine1 := NewEngine()
	conn, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Create tables
	tables := []struct {
		name    string
		columns []*catalog.ColumnDef
		types   []dukdb.Type
		rows    [][]any
	}{
		{
			name: "users",
			columns: []*catalog.ColumnDef{
				catalog.NewColumnDef(
					"id",
					dukdb.TYPE_INTEGER,
				),
				catalog.NewColumnDef(
					"name",
					dukdb.TYPE_VARCHAR,
				),
			},
			types: []dukdb.Type{
				dukdb.TYPE_INTEGER,
				dukdb.TYPE_VARCHAR,
			},
			rows: [][]any{
				{int32(1), "Alice"},
				{int32(2), "Bob"},
			},
		},
		{
			name: "products",
			columns: []*catalog.ColumnDef{
				catalog.NewColumnDef(
					"sku",
					dukdb.TYPE_VARCHAR,
				),
				catalog.NewColumnDef(
					"price",
					dukdb.TYPE_DOUBLE,
				),
			},
			types: []dukdb.Type{
				dukdb.TYPE_VARCHAR,
				dukdb.TYPE_DOUBLE,
			},
			rows: [][]any{
				{"SKU001", float64(19.99)},
				{"SKU002", float64(29.99)},
				{"SKU003", float64(39.99)},
			},
		},
	}

	for _, tbl := range tables {
		tableDef := catalog.NewTableDef(
			tbl.name,
			tbl.columns,
		)
		err := engine1.Catalog().
			CreateTable(tableDef)
		require.NoError(t, err)

		table, err := engine1.Storage().
			CreateTable(tbl.name, tbl.types)
		require.NoError(t, err)

		for _, row := range tbl.rows {
			err = table.AppendRow(row)
			require.NoError(t, err)
		}
	}

	err = engine1.Close()
	require.NoError(t, err)

	// Reopen and verify
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	// Verify users table
	usersDef, ok := engine2.Catalog().
		GetTable("users")
	require.True(t, ok)
	assert.Equal(t, "users", usersDef.Name)

	usersTable, ok := engine2.Storage().
		GetTable("users")
	require.True(t, ok)
	assert.Equal(
		t,
		int64(2),
		usersTable.RowCount(),
	)

	// Verify products table
	productsDef, ok := engine2.Catalog().
		GetTable("products")
	require.True(t, ok)
	assert.Equal(t, "products", productsDef.Name)

	productsTable, ok := engine2.Storage().
		GetTable("products")
	require.True(t, ok)
	assert.Equal(
		t,
		int64(3),
		productsTable.RowCount(),
	)

	err = engine2.Close()
	require.NoError(t, err)
}

func TestNullValuesPersistence(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nulls.dukdb")

	// Create engine with null values
	engine1 := NewEngine()
	conn, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	tableDef := catalog.NewTableDef(
		"test",
		[]*catalog.ColumnDef{
			catalog.NewColumnDef(
				"id",
				dukdb.TYPE_INTEGER,
			),
			catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR).
				WithNullable(true),
		},
	)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	table, err := engine1.Storage().
		CreateTable("test", []dukdb.Type{
			dukdb.TYPE_INTEGER,
			dukdb.TYPE_VARCHAR,
		})
	require.NoError(t, err)

	// Add rows with null values
	err = table.AppendRow(
		[]any{int32(1), "value1"},
	)
	require.NoError(t, err)
	err = table.AppendRow(
		[]any{int32(2), nil},
	) // NULL value
	require.NoError(t, err)
	err = table.AppendRow(
		[]any{int32(3), "value3"},
	)
	require.NoError(t, err)

	err = engine1.Close()
	require.NoError(t, err)

	// Reopen and verify
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	table2, ok := engine2.Storage().
		GetTable("test")
	require.True(t, ok)

	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)

	assert.Equal(
		t,
		int32(1),
		chunk.GetValue(0, 0),
	)
	assert.Equal(
		t,
		"value1",
		chunk.GetValue(0, 1),
	)
	assert.Equal(
		t,
		int32(2),
		chunk.GetValue(1, 0),
	)
	assert.Nil(
		t,
		chunk.GetValue(1, 1),
	) // Should be nil
	assert.Equal(
		t,
		int32(3),
		chunk.GetValue(2, 0),
	)
	assert.Equal(
		t,
		"value3",
		chunk.GetValue(2, 1),
	)

	err = engine2.Close()
	require.NoError(t, err)
}

func TestAllPrimitiveTypesPersistence(
	t *testing.T,
) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "types.dukdb")

	testTypes := []struct {
		name     string
		typ      dukdb.Type
		value    any
		expected any
	}{
		{
			"bool_col",
			dukdb.TYPE_BOOLEAN,
			true,
			true,
		},
		{
			"tinyint_col",
			dukdb.TYPE_TINYINT,
			int8(42),
			int8(42),
		},
		{
			"smallint_col",
			dukdb.TYPE_SMALLINT,
			int16(1000),
			int16(1000),
		},
		{
			"int_col",
			dukdb.TYPE_INTEGER,
			int32(100000),
			int32(100000),
		},
		{
			"bigint_col",
			dukdb.TYPE_BIGINT,
			int64(9999999999),
			int64(9999999999),
		},
		{
			"utinyint_col",
			dukdb.TYPE_UTINYINT,
			uint8(200),
			uint8(200),
		},
		{
			"usmallint_col",
			dukdb.TYPE_USMALLINT,
			uint16(50000),
			uint16(50000),
		},
		{
			"uint_col",
			dukdb.TYPE_UINTEGER,
			uint32(3000000000),
			uint32(3000000000),
		},
		{
			"ubigint_col",
			dukdb.TYPE_UBIGINT,
			uint64(18000000000000000000),
			uint64(18000000000000000000),
		},
		{
			"float_col",
			dukdb.TYPE_FLOAT,
			float32(3.14),
			float32(3.14),
		},
		{
			"double_col",
			dukdb.TYPE_DOUBLE,
			float64(3.14159265359),
			float64(3.14159265359),
		},
		{
			"varchar_col",
			dukdb.TYPE_VARCHAR,
			"Hello, World!",
			"Hello, World!",
		},
	}

	// Create engine
	engine1 := NewEngine()
	conn, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Create columns
	columns := make(
		[]*catalog.ColumnDef,
		len(testTypes),
	)
	types := make([]dukdb.Type, len(testTypes))
	for i, tc := range testTypes {
		columns[i] = catalog.NewColumnDef(
			tc.name,
			tc.typ,
		)
		types[i] = tc.typ
	}

	tableDef := catalog.NewTableDef(
		"types_test",
		columns,
	)
	err = engine1.Catalog().CreateTable(tableDef)
	require.NoError(t, err)

	table, err := engine1.Storage().
		CreateTable("types_test", types)
	require.NoError(t, err)

	// Add row with all values
	values := make([]any, len(testTypes))
	for i, tc := range testTypes {
		values[i] = tc.value
	}
	err = table.AppendRow(values)
	require.NoError(t, err)

	err = engine1.Close()
	require.NoError(t, err)

	// Reopen and verify
	engine2 := NewEngine()
	conn2, err := engine2.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	table2, ok := engine2.Storage().
		GetTable("types_test")
	require.True(t, ok)

	scanner := table2.Scan()
	chunk := scanner.Next()
	require.NotNil(t, chunk)

	for i, tc := range testTypes {
		actual := chunk.GetValue(0, i)
		assert.Equal(
			t,
			tc.expected,
			actual,
			"Type %s mismatch",
			tc.name,
		)
	}

	err = engine2.Close()
	require.NoError(t, err)
}

// TestCheckpointThresholdPersistence tests that checkpoint threshold is properly
// initialized with the configured value when a database is opened.
// Task 4.4: Integration test for threshold persistence
func TestCheckpointThresholdPersistence(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "threshold_test.dukdb")

	// Test 1: Default threshold on new database
	// Scenario: New database should use default threshold
	engine1 := NewEngine()
	conn, err := engine1.Open(dbPath, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify that CheckpointManager was created
	require.NotNil(t, engine1.checkpointMgr, "CheckpointManager should be initialized for persistent database")

	// Verify default threshold (256MB = 268435456 bytes)
	defaultThresholdBytes := uint64(256 * 1024 * 1024)
	assert.Equal(t, defaultThresholdBytes, engine1.checkpointMgr.Threshold(),
		"Default threshold should be 256MB")

	err = engine1.Close()
	require.NoError(t, err)

	// Test 2: Custom threshold from config
	// Scenario: Custom threshold specified in config should be used
	engine2 := NewEngine()
	customConfig := &dukdb.Config{
		CheckpointThreshold: "512MB",
	}
	conn2, err := engine2.Open(dbPath, customConfig)
	require.NoError(t, err)
	require.NotNil(t, conn2)

	// Verify custom threshold was applied
	require.NotNil(t, engine2.checkpointMgr, "CheckpointManager should be initialized with custom config")
	customThresholdBytes := uint64(512 * 1024 * 1024)
	assert.Equal(t, customThresholdBytes, engine2.checkpointMgr.Threshold(),
		"Threshold should be 512MB from config")

	err = engine2.Close()
	require.NoError(t, err)

	// Test 3: Different threshold values
	// Scenario: Various threshold formats should be correctly parsed
	testThresholds := []struct {
		configValue string
		expectedBytes uint64
		description string
	}{
		{"1GB", 1024 * 1024 * 1024, "1GB threshold"},
		{"256MB", 256 * 1024 * 1024, "256MB threshold"},
		{"512MB", 512 * 1024 * 1024, "512MB threshold"},
		{"1048576b", 1048576, "1MB in bytes"},
	}

	for _, tc := range testThresholds {
		t.Run(tc.description, func(t *testing.T) {
			dbPathVariant := filepath.Join(tmpDir, "threshold_"+tc.description+".dukdb")
			engine := NewEngine()
			config := &dukdb.Config{
				CheckpointThreshold: tc.configValue,
			}
			conn, err := engine.Open(dbPathVariant, config)
			require.NoError(t, err, "Should open database with threshold: %s", tc.configValue)
			require.NotNil(t, conn)

			// Verify threshold was parsed correctly
			require.NotNil(t, engine.checkpointMgr)
			assert.Equal(t, tc.expectedBytes, engine.checkpointMgr.Threshold(),
				"Threshold %s should parse to %d bytes", tc.configValue, tc.expectedBytes)

			err = engine.Close()
			require.NoError(t, err)
		})
	}

	// Test 4: Multiple connections use the same threshold
	// Scenario: Multiple connections from the same database path should use the configured threshold
	engine3 := NewEngine()
	config3 := &dukdb.Config{
		CheckpointThreshold: "1GB",
	}
	dbPath3 := filepath.Join(tmpDir, "multi_conn.dukdb")

	conn3a, err := engine3.Open(dbPath3, config3)
	require.NoError(t, err)
	require.NotNil(t, conn3a)

	// CheckpointManager in Engine should reflect the 1GB setting
	require.NotNil(t, engine3.checkpointMgr)
	assert.Equal(t, uint64(1024*1024*1024), engine3.checkpointMgr.Threshold(),
		"Engine should use 1GB threshold from config")

	err = engine3.Close()
	require.NoError(t, err)
}

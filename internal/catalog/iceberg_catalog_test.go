package catalog_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIcebergTableEntry_NewIcebergTableEntry(t *testing.T) {
	snapshotID := int64(12345)
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}, Required: false},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	entry := catalog.NewIcebergTableEntry(
		"test_table",
		"/path/to/table",
		&snapshotID,
		1704067200000, // 2024-01-01 00:00:00 UTC
		schema,
		partitionSpec,
	)

	assert.Equal(t, "test_table", entry.Name)
	assert.Equal(t, "/path/to/table", entry.Location)
	require.NotNil(t, entry.CurrentSnapshotID)
	assert.Equal(t, int64(12345), *entry.CurrentSnapshotID)
	assert.Equal(t, int64(1704067200000), entry.LastUpdatedMs)
	assert.Equal(t, schema, entry.Schema)
	assert.Equal(t, 2, entry.FormatVersion) // Default v2
	assert.NotNil(t, entry.Properties)
}

func TestIcebergTableEntry_Clone(t *testing.T) {
	snapshotID := int64(12345)
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	entry := catalog.NewIcebergTableEntry(
		"test_table",
		"/path/to/table",
		&snapshotID,
		1704067200000,
		schema,
		partitionSpec,
	)
	entry.SetProperty("key1", "value1")

	clone := entry.Clone()

	// Verify clone has same values
	assert.Equal(t, entry.Name, clone.Name)
	assert.Equal(t, entry.Location, clone.Location)
	require.NotNil(t, clone.CurrentSnapshotID)
	assert.Equal(t, *entry.CurrentSnapshotID, *clone.CurrentSnapshotID)
	assert.Equal(t, entry.LastUpdatedMs, clone.LastUpdatedMs)

	// Verify clone is independent (modifying clone doesn't affect original)
	*clone.CurrentSnapshotID = 99999
	assert.Equal(t, int64(12345), *entry.CurrentSnapshotID)

	clone.Properties["key2"] = "value2"
	_, exists := entry.Properties["key2"]
	assert.False(t, exists)
}

func TestIcebergTableEntry_HasSnapshot(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	// Entry without snapshot
	entry1 := catalog.NewIcebergTableEntry(
		"empty_table",
		"/path/to/empty",
		nil, // No snapshot
		1704067200000,
		schema,
		partitionSpec,
	)
	assert.False(t, entry1.HasSnapshot())

	// Entry with snapshot
	snapshotID := int64(12345)
	entry2 := catalog.NewIcebergTableEntry(
		"table_with_data",
		"/path/to/data",
		&snapshotID,
		1704067200000,
		schema,
		partitionSpec,
	)
	assert.True(t, entry2.HasSnapshot())
}

func TestIcebergTableEntry_Properties(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	entry := catalog.NewIcebergTableEntry(
		"test_table",
		"/path/to/table",
		nil,
		1704067200000,
		schema,
		partitionSpec,
	)

	// Get non-existent property
	val, exists := entry.GetProperty("nonexistent")
	assert.False(t, exists)
	assert.Equal(t, "", val)

	// Set and get property
	entry.SetProperty("format.version", "2")
	val, exists = entry.GetProperty("format.version")
	assert.True(t, exists)
	assert.Equal(t, "2", val)
}

func TestIcebergCatalog_RegisterAndGetTable(t *testing.T) {
	cat := catalog.NewIcebergCatalog()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	snapshotID := int64(12345)
	entry := catalog.NewIcebergTableEntry(
		"my_table",
		"/path/to/table",
		&snapshotID,
		1704067200000,
		schema,
		partitionSpec,
	)

	// Register table
	cat.RegisterTable(entry)

	// Get table (exact case)
	retrieved, ok := cat.GetTable("my_table")
	assert.True(t, ok)
	assert.Equal(t, entry, retrieved)

	// Get table (case-insensitive)
	retrieved, ok = cat.GetTable("MY_TABLE")
	assert.True(t, ok)
	assert.Equal(t, entry.Name, retrieved.Name)

	// Get non-existent table
	_, ok = cat.GetTable("nonexistent")
	assert.False(t, ok)
}

func TestIcebergCatalog_UnregisterTable(t *testing.T) {
	cat := catalog.NewIcebergCatalog()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	entry := catalog.NewIcebergTableEntry(
		"my_table",
		"/path/to/table",
		nil,
		1704067200000,
		schema,
		partitionSpec,
	)

	cat.RegisterTable(entry)

	// Verify table exists
	_, ok := cat.GetTable("my_table")
	assert.True(t, ok)

	// Unregister table
	removed := cat.UnregisterTable("my_table")
	assert.True(t, removed)

	// Verify table no longer exists
	_, ok = cat.GetTable("my_table")
	assert.False(t, ok)

	// Try to unregister non-existent table
	removed = cat.UnregisterTable("nonexistent")
	assert.False(t, removed)
}

func TestIcebergCatalog_ListTables(t *testing.T) {
	cat := catalog.NewIcebergCatalog()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	// Add multiple tables
	for i := 0; i < 3; i++ {
		entry := catalog.NewIcebergTableEntry(
			"table_"+string(rune('a'+i)),
			"/path/to/table_"+string(rune('a'+i)),
			nil,
			1704067200000,
			schema,
			partitionSpec,
		)
		cat.RegisterTable(entry)
	}

	// List tables
	tables := cat.ListTables()
	assert.Len(t, tables, 3)

	// Verify table count
	assert.Equal(t, 3, cat.TableCount())
}

func TestIcebergCatalog_Clear(t *testing.T) {
	cat := catalog.NewIcebergCatalog()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	// Add some tables
	entry := catalog.NewIcebergTableEntry(
		"my_table",
		"/path/to/table",
		nil,
		1704067200000,
		schema,
		partitionSpec,
	)
	cat.RegisterTable(entry)

	assert.Equal(t, 1, cat.TableCount())

	// Clear catalog
	cat.Clear()

	assert.Equal(t, 0, cat.TableCount())
	_, ok := cat.GetTable("my_table")
	assert.False(t, ok)
}

func TestIcebergCatalog_Clone(t *testing.T) {
	cat := catalog.NewIcebergCatalog()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	snapshotID := int64(12345)
	entry := catalog.NewIcebergTableEntry(
		"my_table",
		"/path/to/table",
		&snapshotID,
		1704067200000,
		schema,
		partitionSpec,
	)
	cat.RegisterTable(entry)

	// Clone catalog
	clonedCat := cat.Clone()

	// Verify cloned catalog has same tables
	assert.Equal(t, cat.TableCount(), clonedCat.TableCount())

	retrieved, ok := clonedCat.GetTable("my_table")
	assert.True(t, ok)
	assert.Equal(t, entry.Name, retrieved.Name)
	assert.Equal(t, entry.Location, retrieved.Location)

	// Verify independence - modifying clone doesn't affect original
	clonedCat.UnregisterTable("my_table")
	assert.Equal(t, 0, clonedCat.TableCount())
	assert.Equal(t, 1, cat.TableCount())
}

func TestIcebergCatalog_ReplaceTable(t *testing.T) {
	cat := catalog.NewIcebergCatalog()

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	partitionSpec := iceberg.NewPartitionSpec()

	// Register initial entry
	entry1 := catalog.NewIcebergTableEntry(
		"my_table",
		"/path/to/table/v1",
		nil,
		1704067200000,
		schema,
		partitionSpec,
	)
	cat.RegisterTable(entry1)

	// Register replacement entry with same name
	snapshotID := int64(99999)
	entry2 := catalog.NewIcebergTableEntry(
		"my_table", // Same name
		"/path/to/table/v2",
		&snapshotID,
		1704153600000, // Newer timestamp
		schema,
		partitionSpec,
	)
	cat.RegisterTable(entry2)

	// Verify only one table exists with updated location
	assert.Equal(t, 1, cat.TableCount())
	retrieved, ok := cat.GetTable("my_table")
	assert.True(t, ok)
	assert.Equal(t, "/path/to/table/v2", retrieved.Location)
	require.NotNil(t, retrieved.CurrentSnapshotID)
	assert.Equal(t, int64(99999), *retrieved.CurrentSnapshotID)
}

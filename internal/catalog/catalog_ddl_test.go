package catalog

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestViewCRUD tests view creation, retrieval, and deletion.
func TestViewCRUD(t *testing.T) {
	catalog := NewCatalog()

	// Create a view
	view := NewViewDef("my_view", "main", "SELECT * FROM test")
	err := catalog.CreateView(view)
	require.NoError(t, err)

	// Retrieve the view
	retrieved, ok := catalog.GetView("my_view")
	require.True(t, ok)
	assert.Equal(t, "my_view", retrieved.Name)
	assert.Equal(t, "main", retrieved.Schema)
	assert.Equal(t, "SELECT * FROM test", retrieved.Query)

	// Try to create duplicate view
	err = catalog.CreateView(view)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Drop the view
	err = catalog.DropView("my_view")
	require.NoError(t, err)

	// Verify view is gone
	_, ok = catalog.GetView("my_view")
	assert.False(t, ok)

	// Try to drop non-existent view
	err = catalog.DropView("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// TestViewInCustomSchema tests view creation in a custom schema.
func TestViewInCustomSchema(t *testing.T) {
	catalog := NewCatalog()

	// Create custom schema
	_, err := catalog.CreateSchema("custom")
	require.NoError(t, err)

	// Create view in custom schema
	view := NewViewDef("my_view", "custom", "SELECT * FROM test")
	err = catalog.CreateViewInSchema("custom", view)
	require.NoError(t, err)

	// Retrieve from custom schema
	retrieved, ok := catalog.GetViewInSchema("custom", "my_view")
	require.True(t, ok)
	assert.Equal(t, "custom", retrieved.Schema)

	// Should not be in main schema
	_, ok = catalog.GetView("my_view")
	assert.False(t, ok)
}

// TestIndexCRUD tests index creation, retrieval, and deletion.
func TestIndexCRUD(t *testing.T) {
	catalog := NewCatalog()

	// Create an index
	index := NewIndexDef("my_idx", "main", "test_table", []string{"col1", "col2"}, false)
	err := catalog.CreateIndex(index)
	require.NoError(t, err)

	// Retrieve the index
	retrieved, ok := catalog.GetIndex("my_idx")
	require.True(t, ok)
	assert.Equal(t, "my_idx", retrieved.Name)
	assert.Equal(t, "main", retrieved.Schema)
	assert.Equal(t, "test_table", retrieved.Table)
	assert.Equal(t, []string{"col1", "col2"}, retrieved.Columns)
	assert.False(t, retrieved.IsUnique)

	// Try to create duplicate index
	err = catalog.CreateIndex(index)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Drop the index
	err = catalog.DropIndex("my_idx")
	require.NoError(t, err)

	// Verify index is gone
	_, ok = catalog.GetIndex("my_idx")
	assert.False(t, ok)
}

// TestGetIndexesForTable tests retrieving all indexes for a table.
func TestGetIndexesForTable(t *testing.T) {
	catalog := NewCatalog()

	// Create multiple indexes on same table
	idx1 := NewIndexDef("idx1", "main", "users", []string{"id"}, true)
	idx2 := NewIndexDef("idx2", "main", "users", []string{"email"}, true)
	idx3 := NewIndexDef("idx3", "main", "orders", []string{"user_id"}, false)

	require.NoError(t, catalog.CreateIndex(idx1))
	require.NoError(t, catalog.CreateIndex(idx2))
	require.NoError(t, catalog.CreateIndex(idx3))

	// Get indexes for users table
	indexes := catalog.GetIndexesForTable("main", "users")
	assert.Len(t, indexes, 2)

	// Get indexes for orders table
	indexes = catalog.GetIndexesForTable("main", "orders")
	assert.Len(t, indexes, 1)

	// Get indexes for non-existent table
	indexes = catalog.GetIndexesForTable("main", "nonexistent")
	assert.Len(t, indexes, 0)
}

// TestSequenceCRUD tests sequence creation, retrieval, and deletion.
func TestSequenceCRUD(t *testing.T) {
	catalog := NewCatalog()

	// Create a sequence
	seq := NewSequenceDef("my_seq", "main")
	err := catalog.CreateSequence(seq)
	require.NoError(t, err)

	// Retrieve the sequence
	retrieved, ok := catalog.GetSequence("my_seq")
	require.True(t, ok)
	assert.Equal(t, "my_seq", retrieved.Name)
	assert.Equal(t, "main", retrieved.Schema)
	assert.Equal(t, int64(1), retrieved.CurrentVal)
	assert.Equal(t, int64(1), retrieved.StartWith)
	assert.Equal(t, int64(1), retrieved.IncrementBy)

	// Try to create duplicate sequence
	err = catalog.CreateSequence(seq)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Drop the sequence
	err = catalog.DropSequence("my_seq")
	require.NoError(t, err)

	// Verify sequence is gone
	_, ok = catalog.GetSequence("my_seq")
	assert.False(t, ok)
}

// TestSequenceNextVal tests sequence value generation.
func TestSequenceNextVal(t *testing.T) {
	catalog := NewCatalog()

	// Create sequence with default increment (START WITH 1)
	seq := NewSequenceDef("my_seq", "main")
	require.NoError(t, catalog.CreateSequence(seq))

	retrieved, _ := catalog.GetSequence("my_seq")

	// First NextVal returns the START WITH value (1)
	val, err := retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)

	// Second NextVal returns incremented value (2)
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(2), val)

	// CurrVal returns the last value returned by NextVal (2)
	currVal, err := retrieved.CurrVal()
	require.NoError(t, err)
	assert.Equal(t, int64(2), currVal)
}

// TestSequenceCurrValBeforeNextVal tests that CurrVal fails before NextVal is called.
func TestSequenceCurrValBeforeNextVal(t *testing.T) {
	catalog := NewCatalog()

	// Create sequence
	seq := NewSequenceDef("my_seq", "main")
	require.NoError(t, catalog.CreateSequence(seq))

	retrieved, _ := catalog.GetSequence("my_seq")

	// CurrVal before NextVal should fail
	_, err := retrieved.CurrVal()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet defined in this session")

	// After calling NextVal, CurrVal should work
	val, err := retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)

	currVal, err := retrieved.CurrVal()
	require.NoError(t, err)
	assert.Equal(t, int64(1), currVal)
}

// TestSequenceCustomIncrement tests sequence with custom increment.
func TestSequenceCustomIncrement(t *testing.T) {
	catalog := NewCatalog()

	seq := NewSequenceDef("my_seq", "main")
	seq.IncrementBy = 5
	seq.StartWith = 100
	seq.CurrentVal = 100
	require.NoError(t, catalog.CreateSequence(seq))

	retrieved, _ := catalog.GetSequence("my_seq")

	// First call returns the START WITH value (100)
	val, err := retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(100), val)

	// Second call returns incremented value (105)
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(105), val)
}

// TestSequenceMaxValue tests sequence hitting max value.
func TestSequenceMaxValue(t *testing.T) {
	catalog := NewCatalog()

	seq := NewSequenceDef("my_seq", "main")
	seq.CurrentVal = 98
	seq.MaxValue = 100
	seq.IsCycle = false
	require.NoError(t, catalog.CreateSequence(seq))

	retrieved, _ := catalog.GetSequence("my_seq")

	// First call returns current value (98)
	val, err := retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(98), val)

	// Second call returns 99
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(99), val)

	// Third call returns 100 (max value)
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(100), val)

	// Fourth call should fail (would exceed max)
	_, err = retrieved.NextVal()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reached max value")
}

// TestSequenceCycle tests sequence with cycling enabled.
func TestSequenceCycle(t *testing.T) {
	catalog := NewCatalog()

	seq := NewSequenceDef("my_seq", "main")
	seq.StartWith = 1
	seq.CurrentVal = 98
	seq.MaxValue = 100
	seq.IsCycle = true
	require.NoError(t, catalog.CreateSequence(seq))

	retrieved, _ := catalog.GetSequence("my_seq")

	// First call returns 98
	val, err := retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(98), val)

	// Second call returns 99
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(99), val)

	// Third call returns 100 (max value)
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(100), val)

	// Fourth call should cycle back to StartWith (1)
	val, err = retrieved.NextVal()
	require.NoError(t, err)
	assert.Equal(t, int64(1), val)
}

// TestSchemaCRUD tests schema creation and deletion.
func TestSchemaCRUD(t *testing.T) {
	catalog := NewCatalog()

	// Create a schema
	schema, err := catalog.CreateSchema("test_schema")
	require.NoError(t, err)
	assert.Equal(t, "test_schema", schema.Name())

	// Retrieve the schema
	retrieved, ok := catalog.GetSchema("test_schema")
	require.True(t, ok)
	assert.Equal(t, "test_schema", retrieved.Name())

	// Try to create duplicate schema
	_, err = catalog.CreateSchema("test_schema")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Drop the schema
	err = catalog.DropSchema("test_schema")
	require.NoError(t, err)

	// Verify schema is gone
	_, ok = catalog.GetSchema("test_schema")
	assert.False(t, ok)

	// Cannot drop main schema
	err = catalog.DropSchema("main")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Cannot drop main schema")
}

// TestSchemaIfNotExists tests CREATE SCHEMA IF NOT EXISTS.
func TestSchemaIfNotExists(t *testing.T) {
	catalog := NewCatalog()

	// Create schema
	schema1, err := catalog.CreateSchemaIfNotExists("test_schema", true)
	require.NoError(t, err)
	assert.Equal(t, "test_schema", schema1.Name())

	// Try to create again with IF NOT EXISTS
	schema2, err := catalog.CreateSchemaIfNotExists("test_schema", true)
	require.NoError(t, err)
	assert.Equal(t, schema1, schema2) // Should return existing schema

	// Try to create without IF NOT EXISTS (should fail)
	_, err = catalog.CreateSchemaIfNotExists("test_schema", false)
	require.Error(t, err)
}

// TestDropSchemaWithObjects tests dropping a schema with objects.
func TestDropSchemaWithObjects(t *testing.T) {
	catalog := NewCatalog()

	// Create schema
	_, err := catalog.CreateSchema("test_schema")
	require.NoError(t, err)

	// Add a table to the schema
	table := NewTableDef("test_table", nil)
	err = catalog.CreateTableInSchema("test_schema", table)
	require.NoError(t, err)

	// Try to drop without CASCADE (should fail)
	err = catalog.DropSchemaIfExists("test_schema", false, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contains objects")

	// Drop with CASCADE (should succeed)
	err = catalog.DropSchemaIfExists("test_schema", false, true)
	require.NoError(t, err)

	// Verify schema is gone
	_, ok := catalog.GetSchema("test_schema")
	assert.False(t, ok)
}

// TestDropSchemaIfExists tests DROP SCHEMA IF EXISTS.
func TestDropSchemaIfExists(t *testing.T) {
	catalog := NewCatalog()

	// Try to drop non-existent schema with IF EXISTS
	err := catalog.DropSchemaIfExists("nonexistent", true, false)
	require.NoError(t, err) // Should not error

	// Try to drop without IF EXISTS (should fail)
	err = catalog.DropSchemaIfExists("nonexistent", false, false)
	require.Error(t, err)
}

// TestConcurrentSequenceAccess tests thread-safe sequence access.
func TestConcurrentSequenceAccess(t *testing.T) {
	catalog := NewCatalog()

	seq := NewSequenceDef("my_seq", "main")
	require.NoError(t, catalog.CreateSequence(seq))

	retrieved, _ := catalog.GetSequence("my_seq")

	// Generate sequence values concurrently
	const numGoroutines = 10
	const valuesPerGoroutine = 100
	values := make(chan int64, numGoroutines*valuesPerGoroutine)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < valuesPerGoroutine; j++ {
				val, err := retrieved.NextVal()
				require.NoError(t, err)
				values <- val
			}
		}()
	}

	wg.Wait()
	close(values)

	// Verify all values are unique
	seen := make(map[int64]bool)
	count := 0
	for val := range values {
		assert.False(t, seen[val], "Duplicate value: %d", val)
		seen[val] = true
		count++
	}
	assert.Equal(t, numGoroutines*valuesPerGoroutine, count)
}

// TestConcurrentViewAccess tests thread-safe view access.
func TestConcurrentViewAccess(t *testing.T) {
	catalog := NewCatalog()

	// Create views concurrently
	const numGoroutines = 10
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			view := NewViewDef("view", "main", "SELECT * FROM test")
			err := catalog.CreateView(view)
			if err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	// Exactly one goroutine should succeed
	errorCount := 0
	for err := range errors {
		assert.Contains(t, err.Error(), "already exists")
		errorCount++
	}
	assert.Equal(t, numGoroutines-1, errorCount)

	// Verify view exists
	_, ok := catalog.GetView("view")
	assert.True(t, ok)
}

// TestSchemaListMethods tests listing objects in schemas.
func TestSchemaListMethods(t *testing.T) {
	catalog := NewCatalog()

	// Create objects
	view1 := NewViewDef("view1", "main", "SELECT 1")
	view2 := NewViewDef("view2", "main", "SELECT 2")
	require.NoError(t, catalog.CreateView(view1))
	require.NoError(t, catalog.CreateView(view2))

	idx1 := NewIndexDef("idx1", "main", "table1", []string{"col1"}, false)
	idx2 := NewIndexDef("idx2", "main", "table1", []string{"col2"}, false)
	require.NoError(t, catalog.CreateIndex(idx1))
	require.NoError(t, catalog.CreateIndex(idx2))

	seq1 := NewSequenceDef("seq1", "main")
	seq2 := NewSequenceDef("seq2", "main")
	require.NoError(t, catalog.CreateSequence(seq1))
	require.NoError(t, catalog.CreateSequence(seq2))

	// Get main schema
	schema, ok := catalog.GetSchema("main")
	require.True(t, ok)

	// List views
	views := schema.ListViews()
	assert.Len(t, views, 2)

	// List indexes
	indexes := schema.ListIndexes()
	assert.Len(t, indexes, 2)

	// List sequences
	sequences := schema.ListSequences()
	assert.Len(t, sequences, 2)
}

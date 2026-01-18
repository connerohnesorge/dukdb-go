package duckdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewDuckDBStorage tests creating or opening storage.
func TestNewDuckDBStorage(t *testing.T) {
	t.Run("creates new file when not exists", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		require.NotNil(t, storage)

		assert.Equal(t, path, storage.Path())
		assert.False(t, storage.IsClosed())
		assert.False(t, storage.IsModified())
		assert.False(t, storage.IsReadOnly())

		err = storage.Close()
		assert.NoError(t, err)

		// Verify file was created
		_, err = os.Stat(path)
		assert.NoError(t, err)
	})

	t.Run("opens existing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Now open it
		storage2, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		require.NotNil(t, storage2)

		assert.Equal(t, path, storage2.Path())
		assert.False(t, storage2.IsClosed())

		err = storage2.Close()
		assert.NoError(t, err)
	})

	t.Run("respects CreateIfNotExists=false", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nonexistent.duckdb")

		config := DefaultConfig()
		config.CreateIfNotExists = false

		storage, err := NewDuckDBStorage(path, config)
		assert.Error(t, err)
		assert.Nil(t, storage)
		assert.ErrorIs(t, err, ErrFileNotFound)
	})
}

// TestCreateDuckDBStorage tests creating a new storage file.
func TestCreateDuckDBStorage(t *testing.T) {
	t.Run("creates valid DuckDB file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		require.NotNil(t, storage)

		err = storage.Close()
		assert.NoError(t, err)

		// Verify it's a valid DuckDB file
		assert.True(t, DetectDuckDBFile(path))
	})

	t.Run("fails in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		config := DefaultConfig()
		config.ReadOnly = true

		storage, err := CreateDuckDBStorage(path, config)
		assert.Error(t, err)
		assert.Nil(t, storage)
		assert.ErrorIs(t, err, ErrReadOnlyMode)
	})
}

// TestOpenDuckDBStorage tests opening an existing storage file.
func TestOpenDuckDBStorage(t *testing.T) {
	t.Run("opens existing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-write mode
		storage2, err := OpenDuckDBStorage(path, nil)
		require.NoError(t, err)
		require.NotNil(t, storage2)

		assert.False(t, storage2.IsReadOnly())

		err = storage2.Close()
		assert.NoError(t, err)
	})

	t.Run("opens in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-only mode
		config := DefaultConfig()
		config.ReadOnly = true

		storage2, err := OpenDuckDBStorage(path, config)
		require.NoError(t, err)
		require.NotNil(t, storage2)

		assert.True(t, storage2.IsReadOnly())

		err = storage2.Close()
		assert.NoError(t, err)
	})

	t.Run("fails for non-existent file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nonexistent.duckdb")

		storage, err := OpenDuckDBStorage(path, nil)
		assert.Error(t, err)
		assert.Nil(t, storage)
	})
}

// TestDuckDBStorageLoadSaveCatalog tests catalog operations.
func TestDuckDBStorageLoadSaveCatalog(t *testing.T) {
	t.Run("loads empty catalog from new file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		cat, err := storage.LoadCatalog()
		require.NoError(t, err)
		require.NotNil(t, cat)

		// Should have default "main" schema
		schema, ok := cat.GetSchema("main")
		assert.True(t, ok)
		assert.NotNil(t, schema)
	})

	t.Run("saves and preserves catalog", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create storage and add a table
		storage1, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)

		cat := catalog.NewCatalog()
		columns := []*catalog.ColumnDef{
			catalog.NewColumnDef("id", 13),   // INTEGER
			catalog.NewColumnDef("name", 25), // VARCHAR
		}
		tableDef := catalog.NewTableDef("users", columns)
		err = cat.CreateTable(tableDef)
		require.NoError(t, err)

		err = storage1.SaveCatalog(cat)
		require.NoError(t, err)

		assert.True(t, storage1.IsModified())

		err = storage1.Close()
		require.NoError(t, err)
	})

	t.Run("fails to save in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-only mode
		config := DefaultConfig()
		config.ReadOnly = true

		storage2, err := OpenDuckDBStorage(path, config)
		require.NoError(t, err)
		defer func() { _ = storage2.Close() }()

		cat := catalog.NewCatalog()
		err = storage2.SaveCatalog(cat)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrReadOnlyMode)
	})
}

// TestDuckDBStorageTransactions tests transaction operations.
func TestDuckDBStorageTransactions(t *testing.T) {
	t.Run("begin and commit transaction", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		txnID, err := storage.BeginTransaction()
		require.NoError(t, err)
		assert.Greater(t, txnID, uint64(0))

		err = storage.CommitTransaction(txnID)
		assert.NoError(t, err)
	})

	t.Run("begin and rollback transaction", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		txnID, err := storage.BeginTransaction()
		require.NoError(t, err)

		err = storage.RollbackTransaction(txnID)
		assert.NoError(t, err)
	})

	t.Run("prevents nested transactions", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		_, err = storage.BeginTransaction()
		require.NoError(t, err)

		// Try to start another transaction
		_, err = storage.BeginTransaction()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTransactionAlreadyActive)
	})

	t.Run("fails commit without active transaction", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		err = storage.CommitTransaction(1)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrNoActiveTransaction)
	})

	t.Run("fails commit with wrong transaction ID", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		txnID, err := storage.BeginTransaction()
		require.NoError(t, err)

		err = storage.CommitTransaction(txnID + 1)
		assert.Error(t, err)
	})
}

// TestDuckDBStorageCheckpoint tests checkpoint functionality.
func TestDuckDBStorageCheckpoint(t *testing.T) {
	t.Run("checkpoint with no modifications", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		// Checkpoint should be no-op when not modified
		err = storage.Checkpoint()
		assert.NoError(t, err)
	})

	t.Run("checkpoint fails in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-only mode
		config := DefaultConfig()
		config.ReadOnly = true

		storage2, err := OpenDuckDBStorage(path, config)
		require.NoError(t, err)
		defer func() { _ = storage2.Close() }()

		err = storage2.Checkpoint()
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrReadOnlyMode)
	})
}

// TestDuckDBStorageClose tests close functionality.
func TestDuckDBStorageClose(t *testing.T) {
	t.Run("close is idempotent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)

		err = storage.Close()
		assert.NoError(t, err)
		assert.True(t, storage.IsClosed())

		// Second close should be no-op
		err = storage.Close()
		assert.NoError(t, err)
	})

	t.Run("operations fail after close", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)

		err = storage.Close()
		require.NoError(t, err)

		// All operations should fail
		_, err = storage.LoadCatalog()
		assert.ErrorIs(t, err, ErrStorageClosed)

		err = storage.SaveCatalog(catalog.NewCatalog())
		assert.ErrorIs(t, err, ErrStorageClosed)

		_, err = storage.ScanTable("main", "test", nil)
		assert.ErrorIs(t, err, ErrStorageClosed)

		_, err = storage.BeginTransaction()
		assert.ErrorIs(t, err, ErrStorageClosed)
	})
}

// TestDuckDBStorageScanTable tests table scanning.
func TestDuckDBStorageScanTable(t *testing.T) {
	t.Run("fails for non-existent table", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		_, err = storage.ScanTable("main", "nonexistent", nil)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
	})
}

// TestDuckDBStorageInsertRows tests row insertion.
func TestDuckDBStorageInsertRows(t *testing.T) {
	t.Run("fails for non-existent table", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		rows := [][]any{{1, "test"}}
		err = storage.InsertRows("main", "nonexistent", rows)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
	})

	t.Run("fails in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-only mode
		config := DefaultConfig()
		config.ReadOnly = true

		storage2, err := OpenDuckDBStorage(path, config)
		require.NoError(t, err)
		defer func() { _ = storage2.Close() }()

		rows := [][]any{{1, "test"}}
		err = storage2.InsertRows("main", "test", rows)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrReadOnlyMode)
	})
}

// TestDuckDBStorageDeleteRows tests row deletion.
func TestDuckDBStorageDeleteRows(t *testing.T) {
	t.Run("fails for non-existent table", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		err = storage.DeleteRows("main", "nonexistent", []uint64{1, 2, 3})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
	})

	t.Run("fails in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-only mode
		config := DefaultConfig()
		config.ReadOnly = true

		storage2, err := OpenDuckDBStorage(path, config)
		require.NoError(t, err)
		defer func() { _ = storage2.Close() }()

		err = storage2.DeleteRows("main", "test", []uint64{1})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrReadOnlyMode)
	})
}

// TestDuckDBStorageUpdateRows tests row updates.
func TestDuckDBStorageUpdateRows(t *testing.T) {
	t.Run("fails for non-existent table", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := NewDuckDBStorage(path, nil)
		require.NoError(t, err)
		defer func() { _ = storage.Close() }()

		err = storage.UpdateRows("main", "nonexistent", []uint64{1}, map[int]any{0: "new"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrTableNotFound)
	})

	t.Run("fails in read-only mode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		// Create file first
		storage1, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage1.Close()
		require.NoError(t, err)

		// Open in read-only mode
		config := DefaultConfig()
		config.ReadOnly = true

		storage2, err := OpenDuckDBStorage(path, config)
		require.NoError(t, err)
		defer func() { _ = storage2.Close() }()

		err = storage2.UpdateRows("main", "test", []uint64{1}, map[int]any{0: "new"})
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrReadOnlyMode)
	})
}

// TestDefaultConfig tests the default configuration.
func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	assert.False(t, config.ReadOnly)
	assert.Equal(t, DefaultCacheCapacity, config.BlockCacheSize)
	assert.True(t, config.CreateIfNotExists)
	assert.Equal(t, DefaultVectorSize, config.VectorSize)
}

// TestDetectDuckDBFile tests DuckDB file detection.
func TestDetectDuckDBFile(t *testing.T) {
	t.Run("detects valid DuckDB file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.duckdb")

		storage, err := CreateDuckDBStorage(path, nil)
		require.NoError(t, err)
		err = storage.Close()
		require.NoError(t, err)

		assert.True(t, DetectDuckDBFile(path))
	})

	t.Run("rejects non-DuckDB file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "test.txt")

		err := os.WriteFile(path, []byte("not a duckdb file"), 0644)
		require.NoError(t, err)

		assert.False(t, DetectDuckDBFile(path))
	})

	t.Run("rejects non-existent file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nonexistent.duckdb")

		assert.False(t, DetectDuckDBFile(path))
	})
}

// TestDuckDBRowIterator tests the row iterator.
func TestDuckDBRowIterator(t *testing.T) {
	t.Run("empty iterator", func(t *testing.T) {
		it := &duckdbRowIterator{}

		assert.False(t, it.Next())
		assert.Nil(t, it.Row())
		assert.Nil(t, it.Err())

		// Close should be safe on nil scanner
		it.Close()
	})
}

// TestStorageBackendInterface tests that DuckDBStorage implements StorageBackend.
func TestStorageBackendInterface(t *testing.T) {
	// This is a compile-time check
	var _ StorageBackend = (*DuckDBStorage)(nil)
}

// TestDuckDBStorageBlockCount tests block count reporting.
func TestDuckDBStorageBlockCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.duckdb")

	storage, err := NewDuckDBStorage(path, nil)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// New file should have 0 blocks initially
	assert.Equal(t, uint64(0), storage.BlockCount())
}

// TestDuckDBStorageTableCount tests table count reporting.
func TestDuckDBStorageTableCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.duckdb")

	storage, err := NewDuckDBStorage(path, nil)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	// New file should have 0 tables
	assert.Equal(t, 0, storage.TableCount())
}

// TestDuckDBStoragePath tests path getter.
func TestDuckDBStoragePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.duckdb")

	storage, err := NewDuckDBStorage(path, nil)
	require.NoError(t, err)
	defer func() { _ = storage.Close() }()

	assert.Equal(t, path, storage.Path())
}

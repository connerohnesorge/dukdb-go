package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDetectDuckDBFileUnit(t *testing.T) {
	t.Parallel()

	t.Run("valid DuckDB file is detected", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "valid.db")

		// Create a file with DuckDB magic bytes at offset 8
		data := make([]byte, 16)
		copy(data[8:12], []byte("DUCK"))
		err := os.WriteFile(path, data, 0644)
		assert.NoError(t, err)

		assert.True(t, detectDuckDBFile(path))
	})

	t.Run("wrong magic bytes is not detected", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "wrong.db")

		// Create a file with wrong magic bytes at offset 8
		data := make([]byte, 16)
		copy(data[8:12], []byte("FAKE"))
		err := os.WriteFile(path, data, 0644)
		assert.NoError(t, err)

		assert.False(t, detectDuckDBFile(path))
	})

	t.Run("file too short is not detected", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "short.db")

		// Create a file shorter than offset 8 + 4 bytes
		data := make([]byte, 10)
		err := os.WriteFile(path, data, 0644)
		assert.NoError(t, err)

		assert.False(t, detectDuckDBFile(path))
	})

	t.Run("non-existent file returns false", func(t *testing.T) {
		assert.False(t, detectDuckDBFile("/non/existent/path.db"))
	})
}

func TestResolveStorageFormatUnit(t *testing.T) {
	t.Parallel()

	t.Run("explicit duckdb format", func(t *testing.T) {
		result := resolveStorageFormat("/some/path.db", "duckdb")
		assert.Equal(t, StorageFormatDuckDB, result)
	})

	t.Run("explicit DUCKDB format (case insensitive)", func(t *testing.T) {
		result := resolveStorageFormat("/some/path.db", "DUCKDB")
		assert.Equal(t, StorageFormatDuckDB, result)
	})

	t.Run("explicit wal format", func(t *testing.T) {
		result := resolveStorageFormat("/some/path.db", "wal")
		assert.Equal(t, StorageFormatWAL, result)
	})

	t.Run("explicit WAL format (case insensitive)", func(t *testing.T) {
		result := resolveStorageFormat("/some/path.db", "WAL")
		assert.Equal(t, StorageFormatWAL, result)
	})

	t.Run("auto format with non-existent file defaults to WAL", func(t *testing.T) {
		result := resolveStorageFormat("/non/existent.db", "auto")
		assert.Equal(t, StorageFormatWAL, result)
	})

	t.Run("empty format defaults to WAL for new files", func(t *testing.T) {
		result := resolveStorageFormat("/non/existent.db", "")
		assert.Equal(t, StorageFormatWAL, result)
	})

	t.Run("auto format with existing DuckDB file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "existing.db")

		// Create a DuckDB file
		data := make([]byte, 16)
		copy(data[8:12], []byte("DUCK"))
		err := os.WriteFile(path, data, 0644)
		assert.NoError(t, err)

		result := resolveStorageFormat(path, "auto")
		assert.Equal(t, StorageFormatDuckDB, result)
	})

	t.Run("auto format with non-DuckDB file defaults to WAL", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "not_duckdb.db")

		// Create a non-DuckDB file
		err := os.WriteFile(path, []byte("not a duckdb file"), 0644)
		assert.NoError(t, err)

		result := resolveStorageFormat(path, "auto")
		assert.Equal(t, StorageFormatWAL, result)
	})
}

func TestStorageFormatString(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "auto", string(StorageFormatAuto))
	assert.Equal(t, "duckdb", string(StorageFormatDuckDB))
	assert.Equal(t, "wal", string(StorageFormatWAL))
}

package persistence

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndOpenFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.dukdb")

	// Create file
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)
	require.NotNil(t, fm)

	// Write some catalog data
	catalogData := []byte(
		`{"version":1,"schemas":{}}`,
	)
	err = fm.WriteCatalog(catalogData)
	require.NoError(t, err)

	// Finalize
	err = fm.Finalize()
	require.NoError(t, err)

	err = fm.Close()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(dbPath)
	require.NoError(t, err)

	// Verify file
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Open file
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	require.NotNil(t, fm2)
	defer func() {
		require.NoError(t, fm2.Close())
	}()

	// Read catalog
	readCatalog, err := fm2.ReadCatalog()
	require.NoError(t, err)
	assert.Equal(t, catalogData, readCatalog)
}

func TestWriteAndReadBlocks(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(
		tmpDir,
		"test_blocks.dukdb",
	)

	// Create file with DuckDB format
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	// Write blocks (now no-ops in DuckDB format)
	block1 := []byte("block1 data here")
	block2 := []byte(
		"block2 data here with more content",
	)

	err = fm.WriteBlock("table1", 0, block1)
	require.NoError(t, err)

	err = fm.WriteBlock("table1", 1, block2)
	require.NoError(t, err)

	// Write catalog
	catalogData := []byte(`{"version":1}`)
	err = fm.WriteCatalog(catalogData)
	require.NoError(t, err)

	// Finalize and close
	err = fm.Finalize()
	require.NoError(t, err)
	err = fm.Close()
	require.NoError(t, err)

	// Verify
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Reopen and verify metadata
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, fm2.Close())
	}()

	// DuckDB format doesn't use legacy block index
	// Blocks should be an empty slice
	blocks := fm2.DataBlocks()
	assert.Empty(t, blocks)
}

func TestInvalidMagicNumber(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(
		tmpDir,
		"invalid.dukdb",
	)

	// Write file with wrong magic but correct size header
	// DuckDB magic is at offset 8, so we need at least 12 bytes
	invalidData := make(
		[]byte,
		DuckDBHeaderSize*3,
	) // Need enough data for DuckDB format
	copy(
		invalidData[8:12],
		"WRNG",
	) // Invalid magic number at offset 8
	err := os.WriteFile(dbPath, invalidData, 0o644)
	require.NoError(t, err)

	// Try to open
	_, err = OpenFile(dbPath)
	assert.ErrorIs(t, err, ErrInvalidMagic)
}

func TestChecksumVerification(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(
		tmpDir,
		"test_checksum.dukdb",
	)

	// Create valid file with DuckDB format
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	err = fm.WriteCatalog([]byte(`{"version":1}`))
	require.NoError(t, err)

	err = fm.Finalize()
	require.NoError(t, err)
	require.NoError(t, fm.Close())

	// Verify it's valid
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Choice of choice choice chosen!
}

func TestEmptyDatabase(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "empty.dukdb")

	// Create file with no blocks
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	err = fm.WriteCatalog(
		[]byte(
			`{"version":1,"schemas":{"main":{"name":"main","tables":{}}}}`,
		),
	)
	require.NoError(t, err)

	err = fm.Finalize()
	require.NoError(t, err)
	require.NoError(t, fm.Close())

	// Verify
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Open and verify empty
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, fm2.Close())
	}()

	assert.Empty(t, fm2.DataBlocks())
}

func TestLargeBlock(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "large.dukdb")

	// Create file with DuckDB format
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	// Create 1MB block (WriteBlock is now a no-op)
	largeBlock := make([]byte, 1024*1024)
	for i := range largeBlock {
		largeBlock[i] = byte(i % 256)
	}

	err = fm.WriteBlock(
		"large_table",
		0,
		largeBlock,
	)
	require.NoError(t, err)

	err = fm.WriteCatalog([]byte(`{"version":1}`))
	require.NoError(t, err)

	err = fm.Finalize()
	require.NoError(t, err)
	require.NoError(t, fm.Close())

	// Verify
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Open file
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, fm2.Close())
	}()

	// DuckDB format doesn't use legacy block index
	// Blocks should be an empty slice
	blocks := fm2.DataBlocks()
	assert.Empty(t, blocks)
}

func TestDetectFileFormat_DuckDB(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "duckdb_format.dukdb")

	// Create a DuckDB format file
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)
	err = fm.WriteCatalog([]byte(`{"version":1}`))
	require.NoError(t, err)
	err = fm.Finalize()
	require.NoError(t, err)
	require.NoError(t, fm.Close())

	// Detect format
	format, err := DetectFileFormat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, FormatDuckDB, format)
	assert.Equal(t, "DuckDB", format.String())
}

func TestDetectFileFormat_Unknown(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "unknown_format.dukdb")

	// Create a file with invalid magic number
	invalidData := make([]byte, 4096)
	copy(invalidData[8:12], "ABCD") // Invalid magic at offset 8
	err := os.WriteFile(dbPath, invalidData, 0o644)
	require.NoError(t, err)

	// Detect format
	format, err := DetectFileFormat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, FormatUnknown, format)
	assert.Equal(t, "Unknown", format.String())
}

func TestDetectFileFormat_EmptyFile(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "empty.dukdb")

	// Create an empty file
	err := os.WriteFile(dbPath, []byte{}, 0o644)
	require.NoError(t, err)

	// Detect format
	format, err := DetectFileFormat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, FormatUnknown, format)
}

func TestDetectFileFormat_TooSmall(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "small.dukdb")

	// Create a file with only 8 bytes (not enough for DuckDB magic at offset 8)
	err := os.WriteFile(dbPath, []byte("12345678"), 0o644)
	require.NoError(t, err)

	// Detect format
	format, err := DetectFileFormat(dbPath)
	require.NoError(t, err)
	assert.Equal(t, FormatUnknown, format)
}

func TestDetectFileFormat_NonExistent(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "nonexistent.dukdb")

	// Try to detect format of non-existent file
	_, err := DetectFileFormat(dbPath)
	assert.Error(t, err)
}

func TestDetectFormat_Alias(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.dukdb")

	// Create a DuckDB format file
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)
	err = fm.WriteCatalog([]byte(`{"version":1}`))
	require.NoError(t, err)
	err = fm.Finalize()
	require.NoError(t, err)
	require.NoError(t, fm.Close())

	// Test that DetectFormat is an alias
	format1, err1 := DetectFileFormat(dbPath)
	require.NoError(t, err1)

	format2, err2 := DetectFormat(dbPath)
	require.NoError(t, err2)

	assert.Equal(t, format1, format2)
}

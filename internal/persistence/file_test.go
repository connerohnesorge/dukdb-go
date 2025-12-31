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
	defer fm2.Close()

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

	// Create file
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	// Write blocks
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

	// Reopen and read blocks
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	defer fm2.Close()

	blocks := fm2.DataBlocks()
	require.Len(t, blocks, 2)

	// Read block 1
	data1, err := fm2.ReadBlock(blocks[0])
	require.NoError(t, err)
	assert.Equal(t, block1, data1)

	// Read block 2
	data2, err := fm2.ReadBlock(blocks[1])
	require.NoError(t, err)
	assert.Equal(t, block2, data2)
}

func TestInvalidMagicNumber(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(
		tmpDir,
		"invalid.dukdb",
	)

	// Write file with wrong magic but correct size header
	// First 8 bytes are wrong magic, rest is padding to meet HeaderSize
	invalidData := make(
		[]byte,
		HeaderSize+FooterSize+100,
	) // Need enough data
	copy(
		invalidData[:8],
		"WRONGMAG",
	) // Invalid magic number
	err := os.WriteFile(dbPath, invalidData, 0644)
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

	// Create valid file
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	err = fm.WriteCatalog([]byte(`{"version":1}`))
	require.NoError(t, err)

	err = fm.Finalize()
	require.NoError(t, err)
	fm.Close()

	// Verify it's valid
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Corrupt the file
	data, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	// Modify a byte in the middle (not the header)
	if len(data) > HeaderSize+10 {
		data[HeaderSize+5] ^= 0xFF
		err = os.WriteFile(dbPath, data, 0644)
		require.NoError(t, err)

		// Verify should fail
		err = VerifyFile(dbPath)
		assert.ErrorIs(
			t,
			err,
			ErrChecksumMismatch,
		)
	}
}

func TestCatalogJSONSerialization(t *testing.T) {
	t.Parallel()

	catalog := &CatalogJSON{
		Version: 1,
		Schemas: map[string]*SchemaJSON{
			"main": {
				Name: "main",
				Tables: map[string]*TableJSON{
					"users": {
						Name:   "users",
						Schema: "main",
						Columns: []ColumnJSON{
							{
								Name:     "id",
								Type:     4,
								Nullable: false,
							},
							{
								Name:     "name",
								Type:     18,
								Nullable: true,
							},
						},
						PrimaryKey: []int{0},
					},
				},
			},
		},
	}

	// Marshal
	data, err := MarshalCatalog(catalog)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal
	catalog2, err := UnmarshalCatalog(data)
	require.NoError(t, err)
	require.NotNil(t, catalog2)

	assert.Equal(
		t,
		catalog.Version,
		catalog2.Version,
	)
	assert.Len(t, catalog2.Schemas, 1)

	mainSchema := catalog2.Schemas["main"]
	require.NotNil(t, mainSchema)
	assert.Equal(t, "main", mainSchema.Name)

	usersTable := mainSchema.Tables["users"]
	require.NotNil(t, usersTable)
	assert.Equal(t, "users", usersTable.Name)
	assert.Len(t, usersTable.Columns, 2)
	assert.Equal(
		t,
		[]int{0},
		usersTable.PrimaryKey,
	)
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
	fm.Close()

	// Verify
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Open and verify empty
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	defer fm2.Close()

	assert.Empty(t, fm2.DataBlocks())
}

func TestLargeBlock(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "large.dukdb")

	// Create file with large block
	fm, err := CreateFile(dbPath)
	require.NoError(t, err)

	// Create 1MB block
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
	fm.Close()

	// Verify
	err = VerifyFile(dbPath)
	require.NoError(t, err)

	// Read back
	fm2, err := OpenFile(dbPath)
	require.NoError(t, err)
	defer fm2.Close()

	blocks := fm2.DataBlocks()
	require.Len(t, blocks, 1)

	data, err := fm2.ReadBlock(blocks[0])
	require.NoError(t, err)
	assert.Equal(t, largeBlock, data)
}

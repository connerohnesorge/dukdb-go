package catalog_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTestDataDir returns the path to the Iceberg test data directory.
func getTestDataDir() string {
	// Get the directory of this test file and navigate to testdata
	// The testdata is in internal/io/iceberg/testdata
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	// From internal/catalog, go up two levels and down to internal/io/iceberg/testdata
	return filepath.Join(wd, "..", "io", "iceberg", "testdata")
}

func TestIcebergTableDiscovery_DiscoverTable(t *testing.T) {
	testDataDir := getTestDataDir()
	simpleTablePath := filepath.Join(testDataDir, "simple_table")

	// Skip if test data doesn't exist
	if _, err := os.Stat(simpleTablePath); os.IsNotExist(err) {
		t.Skipf("Test data not found at %s", simpleTablePath)
	}

	// Create discovery with default options
	discovery := catalog.NewIcebergTableDiscovery(catalog.DefaultDiscoveryOptions())

	// Discover single table
	entry, err := discovery.DiscoverTable(context.Background(), simpleTablePath, "simple_table")
	require.NoError(t, err)
	require.NotNil(t, entry)

	assert.Equal(t, "simple_table", entry.Name)
	assert.Equal(t, simpleTablePath, entry.Location)
	assert.True(t, entry.HasSnapshot())
	assert.NotZero(t, entry.LastUpdatedMs)
	assert.NotNil(t, entry.Schema)
}

func TestIcebergTableDiscovery_DiscoverTables(t *testing.T) {
	testDataDir := getTestDataDir()

	// Skip if test data doesn't exist
	if _, err := os.Stat(testDataDir); os.IsNotExist(err) {
		t.Skipf("Test data not found at %s", testDataDir)
	}

	// Create discovery with default options
	discovery := catalog.NewIcebergTableDiscovery(catalog.DefaultDiscoveryOptions())

	// Discover all tables in the test data directory
	tables, err := discovery.DiscoverTables(context.Background(), testDataDir)
	require.NoError(t, err)

	// We should find multiple tables
	assert.GreaterOrEqual(t, len(tables), 1, "Expected at least one table to be discovered")

	// Check that we found the simple_table
	foundSimple := false
	for _, table := range tables {
		if table.Name == "simple_table" {
			foundSimple = true
			assert.True(t, table.HasSnapshot())
		}
	}
	assert.True(t, foundSimple, "Expected to find simple_table in discovered tables")
}

func TestIcebergTableDiscovery_DiscoverTable_NotAnIcebergTable(t *testing.T) {
	// Try to discover a directory that is not an Iceberg table
	tempDir, err := os.MkdirTemp("", "not-iceberg-*")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tempDir) }()

	discovery := catalog.NewIcebergTableDiscovery(catalog.DefaultDiscoveryOptions())

	_, err = discovery.DiscoverTable(context.Background(), tempDir, "not_a_table")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid Iceberg table")
}

func TestIcebergTableDiscovery_DiscoverTablesWithTimeTravel(t *testing.T) {
	testDataDir := getTestDataDir()
	timeTravelPath := filepath.Join(testDataDir, "time_travel_table")

	// Skip if test data doesn't exist
	if _, err := os.Stat(timeTravelPath); os.IsNotExist(err) {
		t.Skipf("Test data not found at %s", timeTravelPath)
	}

	discovery := catalog.NewIcebergTableDiscovery(catalog.DefaultDiscoveryOptions())

	// Discover the time travel table
	entry, err := discovery.DiscoverTable(context.Background(), timeTravelPath, "time_travel")
	require.NoError(t, err)
	require.NotNil(t, entry)

	assert.Equal(t, "time_travel", entry.Name)
	assert.True(t, entry.HasSnapshot())
	assert.NotNil(t, entry.Schema)
}

func TestIcebergTableDiscovery_DiscoverTable_EmptyName(t *testing.T) {
	testDataDir := getTestDataDir()
	simpleTablePath := filepath.Join(testDataDir, "simple_table")

	// Skip if test data doesn't exist
	if _, err := os.Stat(simpleTablePath); os.IsNotExist(err) {
		t.Skipf("Test data not found at %s", simpleTablePath)
	}

	discovery := catalog.NewIcebergTableDiscovery(catalog.DefaultDiscoveryOptions())

	// Discover with empty name - should use directory name
	entry, err := discovery.DiscoverTable(context.Background(), simpleTablePath, "")
	require.NoError(t, err)
	require.NotNil(t, entry)

	// Name should be derived from directory
	assert.Equal(t, "simple_table", entry.Name)
}

package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManifestReader(t *testing.T) {
	// Test with nil filesystem - should default to local filesystem
	reader := NewManifestReader(nil)
	require.NotNil(t, reader)
}

func TestEntryStatus_String(t *testing.T) {
	tests := []struct {
		status   EntryStatus
		expected string
	}{
		{EntryStatusExisting, "existing"},
		{EntryStatusAdded, "added"},
		{EntryStatusDeleted, "deleted"},
		{EntryStatus(99), "unknown"},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, tc.status.String())
	}
}

func TestManifestFile_IsDataManifest(t *testing.T) {
	mf := &ManifestFile{
		Content: ManifestContentData,
	}
	assert.True(t, mf.IsDataManifest())
	assert.False(t, mf.IsDeleteManifest())

	mf.Content = ManifestContentDeletes
	assert.False(t, mf.IsDataManifest())
	assert.True(t, mf.IsDeleteManifest())
}

func TestFilterManifestsByPartition(t *testing.T) {
	reader := NewManifestReader(nil)

	manifests := []*ManifestFile{
		{Path: "manifest1.avro", Content: ManifestContentData},
		{Path: "manifest2.avro", Content: ManifestContentData},
	}

	// With nil filter, should return all manifests
	result := reader.FilterManifestsByPartition(manifests, nil)
	assert.Len(t, result, 2)
}

func TestResolveManifestPath(t *testing.T) {
	tests := []struct {
		name         string
		tableLocation string
		manifestPath  string
		expected     string
	}{
		{
			name:         "absolute path",
			tableLocation: "/warehouse/db/table",
			manifestPath:  "/absolute/path/manifest.avro",
			expected:     "/absolute/path/manifest.avro",
		},
		{
			name:         "s3 URL",
			tableLocation: "/warehouse/db/table",
			manifestPath:  "s3://bucket/path/manifest.avro",
			expected:     "s3://bucket/path/manifest.avro",
		},
		{
			name:         "gs URL",
			tableLocation: "/warehouse/db/table",
			manifestPath:  "gs://bucket/path/manifest.avro",
			expected:     "gs://bucket/path/manifest.avro",
		},
		{
			name:         "https URL",
			tableLocation: "/warehouse/db/table",
			manifestPath:  "https://example.com/manifest.avro",
			expected:     "https://example.com/manifest.avro",
		},
		{
			name:         "relative path",
			tableLocation: "/warehouse/db/table",
			manifestPath:  "metadata/manifest.avro",
			expected:     "/warehouse/db/table/metadata/manifest.avro",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := ResolveManifestPath(tc.tableLocation, tc.manifestPath)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestManifestReader_ReadManifestList_NilSnapshot(t *testing.T) {
	reader := NewManifestReader(nil)
	_, err := reader.ReadManifestList(nil, nil)
	assert.ErrorIs(t, err, ErrNoCurrentSnapshot)
}

func TestManifestReader_ReadDataFiles_NilManifest(t *testing.T) {
	reader := NewManifestReader(nil)
	_, err := reader.ReadDataFiles(nil, nil)
	assert.Error(t, err)
}

func TestManifestReader_ReadManifestEntries_NilManifest(t *testing.T) {
	reader := NewManifestReader(nil)
	_, err := reader.ReadManifestEntries(nil, nil)
	assert.Error(t, err)
}

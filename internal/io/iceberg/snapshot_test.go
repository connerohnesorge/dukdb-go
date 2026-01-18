package iceberg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Sample metadata with multiple snapshots for testing time travel
const multiSnapshotMetadataJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/warehouse/db/table",
  "last-sequence-number": 3,
  "last-updated-ms": 1672617600000,
  "last-column-id": 2,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "id", "required": true, "type": "long"},
        {"id": 2, "name": "data", "required": false, "type": "string"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {"spec-id": 0, "fields": []}
  ],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [
    {"order-id": 0, "fields": []}
  ],
  "properties": {},
  "current-snapshot-id": 3,
  "refs": {
    "main": {"snapshot-id": 3, "type": "branch"}
  },
  "snapshots": [
    {
      "snapshot-id": 1,
      "parent-snapshot-id": null,
      "sequence-number": 1,
      "timestamp-ms": 1672444800000,
      "manifest-list": "s3://bucket/manifest-1.avro",
      "summary": {"operation": "append"},
      "schema-id": 0
    },
    {
      "snapshot-id": 2,
      "parent-snapshot-id": 1,
      "sequence-number": 2,
      "timestamp-ms": 1672531200000,
      "manifest-list": "s3://bucket/manifest-2.avro",
      "summary": {"operation": "append"},
      "schema-id": 0
    },
    {
      "snapshot-id": 3,
      "parent-snapshot-id": 2,
      "sequence-number": 3,
      "timestamp-ms": 1672617600000,
      "manifest-list": "s3://bucket/manifest-3.avro",
      "summary": {"operation": "append"},
      "schema-id": 0
    }
  ],
  "snapshot-log": [
    {"snapshot-id": 1, "timestamp-ms": 1672444800000},
    {"snapshot-id": 2, "timestamp-ms": 1672531200000},
    {"snapshot-id": 3, "timestamp-ms": 1672617600000}
  ],
  "metadata-log": []
}`

func TestSnapshotSelector_CurrentSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	current := selector.CurrentSnapshot()

	require.NotNil(t, current)
	assert.Equal(t, int64(3), current.SnapshotID)
	assert.Equal(t, int64(1672617600000), current.TimestampMs)
}

func TestSnapshotSelector_SnapshotByID(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)

	// Find existing snapshot
	snap, err := selector.SnapshotByID(2)
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, int64(2), snap.SnapshotID)

	// Find non-existing snapshot
	_, err = selector.SnapshotByID(999)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSnapshotNotFound)
}

func TestSnapshotSelector_SnapshotAsOfTimestamp(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)

	tests := []struct {
		name               string
		timestampMs        int64
		inclusive          bool
		expectedSnapshotID int64
		expectError        bool
	}{
		{
			name:               "exact match snapshot 2 (inclusive)",
			timestampMs:        1672531200000,
			inclusive:          true,
			expectedSnapshotID: 2,
		},
		{
			name:               "between snapshot 1 and 2",
			timestampMs:        1672500000000,
			inclusive:          true,
			expectedSnapshotID: 1,
		},
		{
			name:               "after all snapshots",
			timestampMs:        1672700000000,
			inclusive:          true,
			expectedSnapshotID: 3,
		},
		{
			name:        "before all snapshots",
			timestampMs: 1672000000000,
			inclusive:   true,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := selector.SnapshotAsOfTimestampMs(tc.timestampMs, tc.inclusive)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, snap)
			assert.Equal(t, tc.expectedSnapshotID, snap.SnapshotID)
		})
	}
}

func TestSnapshotSelector_Snapshots(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	snapshots := selector.Snapshots()

	assert.Len(t, snapshots, 3)
}

func TestSnapshotSelector_SnapshotHistory(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	history := selector.SnapshotHistory()

	assert.Len(t, history, 3)
	assert.Equal(t, int64(1), history[0].SnapshotID)
	assert.Equal(t, int64(2), history[1].SnapshotID)
	assert.Equal(t, int64(3), history[2].SnapshotID)
}

func TestSnapshotSelector_ParentSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)

	// Get snapshot 3
	snap3, err := selector.SnapshotByID(3)
	require.NoError(t, err)

	// Get parent (should be snapshot 2)
	parent := selector.ParentSnapshot(snap3)
	require.NotNil(t, parent)
	assert.Equal(t, int64(2), parent.SnapshotID)

	// Get parent of parent (should be snapshot 1)
	grandparent := selector.ParentSnapshot(parent)
	require.NotNil(t, grandparent)
	assert.Equal(t, int64(1), grandparent.SnapshotID)

	// Get parent of snapshot 1 (should be nil)
	noParent := selector.ParentSnapshot(grandparent)
	assert.Nil(t, noParent)
}

func TestSnapshotSelector_SnapshotAncestors(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)

	snap3, err := selector.SnapshotByID(3)
	require.NoError(t, err)

	ancestors := selector.SnapshotAncestors(snap3)

	assert.Len(t, ancestors, 3)
	assert.Equal(t, int64(3), ancestors[0].SnapshotID)
	assert.Equal(t, int64(2), ancestors[1].SnapshotID)
	assert.Equal(t, int64(1), ancestors[2].SnapshotID)
}

func TestSnapshotSelector_LatestSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	latest := selector.LatestSnapshot()

	require.NotNil(t, latest)
	assert.Equal(t, int64(3), latest.SnapshotID)
}

func TestSnapshotSelector_OldestSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	oldest := selector.OldestSnapshot()

	require.NotNil(t, oldest)
	assert.Equal(t, int64(1), oldest.SnapshotID)
}

func TestSnapshotSelector_HasSnapshot(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)

	assert.True(t, selector.HasSnapshot(1))
	assert.True(t, selector.HasSnapshot(2))
	assert.True(t, selector.HasSnapshot(3))
	assert.False(t, selector.HasSnapshot(999))
}

func TestSnapshotSelector_SnapshotCount(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	assert.Equal(t, 3, selector.SnapshotCount())
}

func TestSnapshot_Timestamp(t *testing.T) {
	metadata, err := ParseMetadataBytes([]byte(multiSnapshotMetadataJSON))
	require.NoError(t, err)

	selector := NewSnapshotSelector(metadata)
	snap, err := selector.SnapshotByID(1)
	require.NoError(t, err)

	ts := snap.Timestamp()
	expected := time.UnixMilli(1672444800000)
	assert.Equal(t, expected.UTC(), ts.UTC())
}

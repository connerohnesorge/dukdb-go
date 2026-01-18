// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements snapshot management and selection for Iceberg tables.
package iceberg

import (
	"fmt"
	"slices"
	"time"
)

// SnapshotSelector provides methods for selecting snapshots from an Iceberg table.
type SnapshotSelector struct {
	metadata *TableMetadata
}

// NewSnapshotSelector creates a new SnapshotSelector for the given table metadata.
func NewSnapshotSelector(metadata *TableMetadata) *SnapshotSelector {
	return &SnapshotSelector{metadata: metadata}
}

// CurrentSnapshot returns the current snapshot of the table.
// Returns nil if the table has no snapshots.
func (s *SnapshotSelector) CurrentSnapshot() *Snapshot {
	return s.metadata.CurrentSnapshot()
}

// SnapshotByID returns the snapshot with the given ID.
// Returns an error if the snapshot is not found.
func (s *SnapshotSelector) SnapshotByID(snapshotID int64) (*Snapshot, error) {
	snap := s.metadata.SnapshotByID(snapshotID)
	if snap == nil {
		return nil, fmt.Errorf("%w: snapshot ID %d", ErrSnapshotNotFound, snapshotID)
	}

	return snap, nil
}

// SnapshotByName returns the snapshot with the given reference name (e.g., "main", "branch-1").
// Returns an error if the snapshot is not found.
func (s *SnapshotSelector) SnapshotByName(name string) (*Snapshot, error) {
	snap := s.metadata.SnapshotByName(name)
	if snap == nil {
		return nil, fmt.Errorf("%w: snapshot name %q", ErrSnapshotNotFound, name)
	}

	return snap, nil
}

// SnapshotAsOfTimestamp returns the snapshot that was current at or just before
// the given timestamp. If inclusive is true, snapshots at exactly the timestamp
// are included; otherwise, only snapshots strictly before the timestamp are returned.
func (s *SnapshotSelector) SnapshotAsOfTimestamp(
	timestamp time.Time,
	inclusive bool,
) (*Snapshot, error) {
	timestampMs := timestamp.UnixMilli()

	return s.SnapshotAsOfTimestampMs(timestampMs, inclusive)
}

// SnapshotAsOfTimestampMs returns the snapshot that was current at or just before
// the given timestamp in milliseconds.
func (s *SnapshotSelector) SnapshotAsOfTimestampMs(
	timestampMs int64,
	inclusive bool,
) (*Snapshot, error) {
	snapshots := s.metadata.Snapshots()
	if len(snapshots) == 0 {
		return nil, ErrNoCurrentSnapshot
	}

	// Get snapshot log entries and search in reverse order (newest first)
	entries := s.metadata.SnapshotLogs()

	// Find the snapshot that was current at or before the timestamp
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if (inclusive && entry.TimestampMs <= timestampMs) ||
			(!inclusive && entry.TimestampMs < timestampMs) {
			return s.SnapshotByID(entry.SnapshotID)
		}
	}

	return nil, fmt.Errorf("%w: timestamp %d", ErrNoSnapshotAtTimestamp, timestampMs)
}

// Snapshots returns all snapshots in the table.
func (s *SnapshotSelector) Snapshots() []*Snapshot {
	rawSnapshots := s.metadata.Snapshots()
	snapshots := make([]*Snapshot, 0, len(rawSnapshots))

	for i := range rawSnapshots {
		snapshots = append(snapshots, &rawSnapshots[i])
	}

	return snapshots
}

// SnapshotHistory returns the snapshot log entries in chronological order.
func (s *SnapshotSelector) SnapshotHistory() []SnapshotLogEntry {
	return s.metadata.SnapshotLogs()
}

// ParentSnapshot returns the parent of the given snapshot.
// Returns nil if the snapshot has no parent (is the first snapshot).
func (s *SnapshotSelector) ParentSnapshot(snapshot *Snapshot) *Snapshot {
	if snapshot.ParentSnapshotID == nil {
		return nil
	}

	snap, err := s.SnapshotByID(*snapshot.ParentSnapshotID)
	if err != nil {
		return nil
	}

	return snap
}

// SnapshotAncestors returns all ancestor snapshots of the given snapshot,
// ordered from the given snapshot to the root (oldest).
func (s *SnapshotSelector) SnapshotAncestors(snapshot *Snapshot) []*Snapshot {
	ancestors := make([]*Snapshot, 0)
	current := snapshot

	for current != nil {
		ancestors = append(ancestors, current)
		current = s.ParentSnapshot(current)
	}

	return ancestors
}

// SnapshotRange returns all snapshots between startID and endID (inclusive).
// The snapshots are returned in chronological order.
func (s *SnapshotSelector) SnapshotRange(startID, endID int64) ([]*Snapshot, error) {
	startSnap, err := s.SnapshotByID(startID)
	if err != nil {
		return nil, err
	}

	endSnap, err := s.SnapshotByID(endID)
	if err != nil {
		return nil, err
	}

	// Get ancestors of end snapshot up to start snapshot
	ancestors := s.SnapshotAncestors(endSnap)

	// Find start snapshot in ancestors
	startIdx := -1
	for i, snap := range ancestors {
		if snap.SnapshotID == startID {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		// startID is not an ancestor of endID, return both if they exist
		return []*Snapshot{startSnap, endSnap}, nil
	}

	// Return the range, reversing to chronological order
	result := ancestors[:startIdx+1]
	slices.Reverse(result)

	return result, nil
}

// LatestSnapshot returns the most recent snapshot by timestamp.
func (s *SnapshotSelector) LatestSnapshot() *Snapshot {
	snapshots := s.Snapshots()
	if len(snapshots) == 0 {
		return nil
	}

	latest := snapshots[0]
	for _, snap := range snapshots[1:] {
		if snap.TimestampMs > latest.TimestampMs {
			latest = snap
		}
	}

	return latest
}

// OldestSnapshot returns the oldest snapshot by timestamp.
func (s *SnapshotSelector) OldestSnapshot() *Snapshot {
	snapshots := s.Snapshots()
	if len(snapshots) == 0 {
		return nil
	}

	oldest := snapshots[0]
	for _, snap := range snapshots[1:] {
		if snap.TimestampMs < oldest.TimestampMs {
			oldest = snap
		}
	}

	return oldest
}

// HasSnapshot returns true if a snapshot with the given ID exists.
func (s *SnapshotSelector) HasSnapshot(snapshotID int64) bool {
	_, err := s.SnapshotByID(snapshotID)
	return err == nil
}

// SnapshotCount returns the number of snapshots in the table.
func (s *SnapshotSelector) SnapshotCount() int {
	return len(s.metadata.Snapshots())
}

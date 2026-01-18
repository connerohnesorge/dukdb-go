// Package iceberg provides Apache Iceberg table format support for dukdb-go.
// This file implements time travel query support for Iceberg tables.
package iceberg

import (
	"fmt"
	"time"
)

// TimeTravelOptions configures how time travel queries select snapshots.
type TimeTravelOptions struct {
	// SnapshotID explicitly selects a snapshot by ID.
	SnapshotID *int64

	// Timestamp selects the snapshot that was current at this time.
	Timestamp *time.Time

	// TimestampMs is like Timestamp but in milliseconds since epoch.
	TimestampMs *int64

	// SnapshotRef selects a snapshot by reference name (e.g., "main", "branch-1").
	SnapshotRef string

	// Version selects a snapshot by version number (metadata file version).
	// This is different from SnapshotID - it refers to the table version.
	Version *int
}

// TimeTravelResult contains information about the selected snapshot.
type TimeTravelResult struct {
	// Snapshot is the selected snapshot.
	Snapshot *Snapshot

	// SelectedBy indicates how the snapshot was selected.
	SelectedBy string

	// AvailableRange contains the range of available timestamps.
	AvailableRange *SnapshotTimeRange
}

// SnapshotTimeRange represents the available time range for time travel.
type SnapshotTimeRange struct {
	// OldestTimestamp is the timestamp of the oldest available snapshot.
	OldestTimestamp time.Time
	// NewestTimestamp is the timestamp of the newest available snapshot.
	NewestTimestamp time.Time
	// OldestSnapshotID is the ID of the oldest snapshot.
	OldestSnapshotID int64
	// NewestSnapshotID is the ID of the newest snapshot.
	NewestSnapshotID int64
}

// TimeTravelSelector provides methods for selecting snapshots for time travel.
type TimeTravelSelector struct {
	metadata *TableMetadata
	selector *SnapshotSelector
}

// NewTimeTravelSelector creates a new TimeTravelSelector.
func NewTimeTravelSelector(metadata *TableMetadata) *TimeTravelSelector {
	return &TimeTravelSelector{
		metadata: metadata,
		selector: NewSnapshotSelector(metadata),
	}
}

// SelectSnapshot selects a snapshot based on time travel options.
func (t *TimeTravelSelector) SelectSnapshot(opts *TimeTravelOptions) (*TimeTravelResult, error) {
	if opts == nil {
		// No time travel - use current snapshot
		current := t.selector.CurrentSnapshot()
		return &TimeTravelResult{
			Snapshot:       current,
			SelectedBy:     "current",
			AvailableRange: t.calculateAvailableRange(),
		}, nil
	}

	// Priority: SnapshotID > TimestampMs > Timestamp > SnapshotRef > Version

	if opts.SnapshotID != nil {
		return t.selectBySnapshotID(*opts.SnapshotID)
	}

	if opts.TimestampMs != nil {
		return t.selectByTimestampMs(*opts.TimestampMs)
	}

	if opts.Timestamp != nil {
		return t.selectByTimestamp(*opts.Timestamp)
	}

	if opts.SnapshotRef != "" {
		return t.selectByRef(opts.SnapshotRef)
	}

	if opts.Version != nil {
		return t.selectByVersion(*opts.Version)
	}

	// No options specified - use current
	current := t.selector.CurrentSnapshot()
	return &TimeTravelResult{
		Snapshot:       current,
		SelectedBy:     "current",
		AvailableRange: t.calculateAvailableRange(),
	}, nil
}

// SelectSnapshotByTimestamp finds the snapshot that was current at or before the timestamp.
func SelectSnapshotByTimestamp(metadata *TableMetadata, timestamp time.Time) (*Snapshot, error) {
	selector := NewTimeTravelSelector(metadata)
	result, err := selector.SelectSnapshot(&TimeTravelOptions{Timestamp: &timestamp})
	if err != nil {
		return nil, err
	}
	return result.Snapshot, nil
}

// SelectSnapshotByID finds a snapshot by its ID.
func SelectSnapshotByID(metadata *TableMetadata, snapshotID int64) (*Snapshot, error) {
	selector := NewTimeTravelSelector(metadata)
	result, err := selector.SelectSnapshot(&TimeTravelOptions{SnapshotID: &snapshotID})
	if err != nil {
		return nil, err
	}
	return result.Snapshot, nil
}

// SelectCurrentSnapshot returns the current snapshot.
func SelectCurrentSnapshot(metadata *TableMetadata) *Snapshot {
	selector := NewSnapshotSelector(metadata)
	return selector.CurrentSnapshot()
}

// selectBySnapshotID selects a snapshot by its ID.
func (t *TimeTravelSelector) selectBySnapshotID(snapshotID int64) (*TimeTravelResult, error) {
	snapshot, err := t.selector.SnapshotByID(snapshotID)
	if err != nil {
		return nil, t.snapshotNotFoundError(snapshotID)
	}

	return &TimeTravelResult{
		Snapshot:       snapshot,
		SelectedBy:     fmt.Sprintf("snapshot_id=%d", snapshotID),
		AvailableRange: t.calculateAvailableRange(),
	}, nil
}

// selectByTimestampMs selects a snapshot by timestamp in milliseconds.
func (t *TimeTravelSelector) selectByTimestampMs(timestampMs int64) (*TimeTravelResult, error) {
	snapshot, err := t.selector.SnapshotAsOfTimestampMs(timestampMs, true)
	if err != nil {
		return nil, t.timestampNotFoundError(time.UnixMilli(timestampMs))
	}

	return &TimeTravelResult{
		Snapshot:       snapshot,
		SelectedBy:     fmt.Sprintf("timestamp_ms=%d", timestampMs),
		AvailableRange: t.calculateAvailableRange(),
	}, nil
}

// selectByTimestamp selects a snapshot by timestamp.
func (t *TimeTravelSelector) selectByTimestamp(timestamp time.Time) (*TimeTravelResult, error) {
	snapshot, err := t.selector.SnapshotAsOfTimestamp(timestamp, true)
	if err != nil {
		return nil, t.timestampNotFoundError(timestamp)
	}

	return &TimeTravelResult{
		Snapshot:       snapshot,
		SelectedBy:     fmt.Sprintf("timestamp=%s", timestamp.Format(time.RFC3339)),
		AvailableRange: t.calculateAvailableRange(),
	}, nil
}

// selectByRef selects a snapshot by reference name.
func (t *TimeTravelSelector) selectByRef(ref string) (*TimeTravelResult, error) {
	snapshot, err := t.selector.SnapshotByName(ref)
	if err != nil {
		return nil, fmt.Errorf("%w: reference %q not found", ErrSnapshotNotFound, ref)
	}

	return &TimeTravelResult{
		Snapshot:       snapshot,
		SelectedBy:     fmt.Sprintf("ref=%s", ref),
		AvailableRange: t.calculateAvailableRange(),
	}, nil
}

// selectByVersion selects a snapshot by table version.
// Note: This is a simplified implementation. Full implementation would
// require reading historical metadata files.
func (t *TimeTravelSelector) selectByVersion(version int) (*TimeTravelResult, error) {
	// For now, version maps directly to snapshots by index in snapshot log
	logs := t.metadata.SnapshotLogs()
	if version <= 0 || version > len(logs) {
		return nil, fmt.Errorf("%w: version %d not found (available: 1-%d)",
			ErrSnapshotNotFound, version, len(logs))
	}

	entry := logs[version-1]
	snapshot, err := t.selector.SnapshotByID(entry.SnapshotID)
	if err != nil {
		return nil, err
	}

	return &TimeTravelResult{
		Snapshot:       snapshot,
		SelectedBy:     fmt.Sprintf("version=%d", version),
		AvailableRange: t.calculateAvailableRange(),
	}, nil
}

// calculateAvailableRange calculates the available snapshot time range.
func (t *TimeTravelSelector) calculateAvailableRange() *SnapshotTimeRange {
	oldest := t.selector.OldestSnapshot()
	newest := t.selector.LatestSnapshot()

	if oldest == nil || newest == nil {
		return nil
	}

	return &SnapshotTimeRange{
		OldestTimestamp:  oldest.Timestamp(),
		NewestTimestamp:  newest.Timestamp(),
		OldestSnapshotID: oldest.SnapshotID,
		NewestSnapshotID: newest.SnapshotID,
	}
}

// snapshotNotFoundError creates an error with available snapshot information.
func (t *TimeTravelSelector) snapshotNotFoundError(snapshotID int64) error {
	snapshots := t.selector.Snapshots()
	if len(snapshots) == 0 {
		return fmt.Errorf("%w: snapshot ID %d (table has no snapshots)",
			ErrSnapshotNotFound, snapshotID)
	}

	// Build list of available IDs
	ids := make([]int64, len(snapshots))
	for i, s := range snapshots {
		ids[i] = s.SnapshotID
	}

	return fmt.Errorf("%w: snapshot ID %d (available: %v)",
		ErrSnapshotNotFound, snapshotID, ids)
}

// timestampNotFoundError creates an error with available timestamp information.
func (t *TimeTravelSelector) timestampNotFoundError(timestamp time.Time) error {
	timeRange := t.calculateAvailableRange()
	if timeRange == nil {
		return fmt.Errorf("%w: no snapshot at or before %s (table has no snapshots)",
			ErrNoSnapshotAtTimestamp, timestamp.Format(time.RFC3339))
	}

	return fmt.Errorf("%w: no snapshot at or before %s (available range: %s to %s)",
		ErrNoSnapshotAtTimestamp,
		timestamp.Format(time.RFC3339),
		timeRange.OldestTimestamp.Format(time.RFC3339),
		timeRange.NewestTimestamp.Format(time.RFC3339))
}

// GetSnapshotHistory returns the complete snapshot history for a table.
func GetSnapshotHistory(metadata *TableMetadata) []SnapshotHistoryEntry {
	selector := NewSnapshotSelector(metadata)
	snapshots := selector.Snapshots()

	history := make([]SnapshotHistoryEntry, len(snapshots))
	for i, snap := range snapshots {
		history[i] = SnapshotHistoryEntry{
			SnapshotID:   snap.SnapshotID,
			Timestamp:    snap.Timestamp(),
			TimestampMs:  snap.TimestampMs,
			Operation:    snap.Summary["operation"],
			ParentID:     snap.ParentSnapshotID,
			ManifestList: snap.ManifestListLocation,
			Summary:      snap.Summary,
		}
	}

	return history
}

// SnapshotHistoryEntry contains information about a snapshot in the history.
type SnapshotHistoryEntry struct {
	// SnapshotID is the snapshot's unique identifier.
	SnapshotID int64
	// Timestamp is when the snapshot was created.
	Timestamp time.Time
	// TimestampMs is the timestamp in milliseconds.
	TimestampMs int64
	// Operation describes what created this snapshot (append, overwrite, delete, etc.).
	Operation string
	// ParentID is the parent snapshot's ID (nil for first snapshot).
	ParentID *int64
	// ManifestList is the path to the manifest list file.
	ManifestList string
	// Summary contains additional snapshot metadata.
	Summary map[string]string
}

// ValidateTimeTravelOptions validates time travel options.
func ValidateTimeTravelOptions(opts *TimeTravelOptions) error {
	if opts == nil {
		return nil
	}

	// Count how many selection methods are specified
	count := 0
	if opts.SnapshotID != nil {
		count++
	}
	if opts.Timestamp != nil {
		count++
	}
	if opts.TimestampMs != nil {
		count++
	}
	if opts.SnapshotRef != "" {
		count++
	}
	if opts.Version != nil {
		count++
	}

	if count > 1 {
		return fmt.Errorf(
			"only one of snapshot_id, timestamp, timestamp_ms, snapshot_ref, or version may be specified",
		)
	}

	return nil
}

// ParseTimeTravelSyntax parses time travel syntax from SQL.
// Supports:
//   - AS OF TIMESTAMP '2024-01-15 10:00:00'
//   - AS OF SNAPSHOT 1234567890
//   - VERSION AS OF 5
//   - AT BRANCH 'feature-branch'
//
// This is a placeholder for integration with the SQL parser.
type TimeTravelSyntax struct {
	Type  string // "timestamp", "snapshot", "version", "branch"
	Value any
}

// ToTimeTravelOptions converts parsed syntax to options.
func (s *TimeTravelSyntax) ToTimeTravelOptions() (*TimeTravelOptions, error) {
	opts := &TimeTravelOptions{}

	switch s.Type {
	case "timestamp":
		if ts, ok := s.Value.(time.Time); ok {
			opts.Timestamp = &ts
		} else if tsMs, ok := s.Value.(int64); ok {
			opts.TimestampMs = &tsMs
		} else {
			return nil, fmt.Errorf("invalid timestamp value: %v", s.Value)
		}

	case "snapshot":
		if id, ok := s.Value.(int64); ok {
			opts.SnapshotID = &id
		} else {
			return nil, fmt.Errorf("invalid snapshot ID: %v", s.Value)
		}

	case "version":
		if ver, ok := s.Value.(int); ok {
			opts.Version = &ver
		} else {
			return nil, fmt.Errorf("invalid version: %v", s.Value)
		}

	case "branch", "ref":
		if ref, ok := s.Value.(string); ok {
			opts.SnapshotRef = ref
		} else {
			return nil, fmt.Errorf("invalid branch/ref: %v", s.Value)
		}

	default:
		return nil, fmt.Errorf("unknown time travel type: %s", s.Type)
	}

	return opts, nil
}

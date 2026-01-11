// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"encoding/json"
	"fmt"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeIcebergSnapshots executes an iceberg_snapshots table function.
// This function returns information about all snapshots in an Iceberg table,
// including snapshot IDs, timestamps, parent relationships, and operation summaries.
//
// Output columns:
//   - snapshot_id: BIGINT - Unique identifier for the snapshot
//   - parent_snapshot_id: BIGINT - ID of the parent snapshot (NULL for first snapshot)
//   - timestamp_ms: BIGINT - Creation timestamp in milliseconds since epoch
//   - timestamp: TIMESTAMP - Creation timestamp as a readable timestamp
//   - manifest_list: VARCHAR - Path to the manifest list file
//   - operation: VARCHAR - Type of operation (append, delete, overwrite, replace)
//   - summary: VARCHAR - JSON representation of the full snapshot summary
//   - added_data_files: BIGINT - Number of data files added (from summary)
//   - deleted_data_files: BIGINT - Number of data files deleted (from summary)
//   - added_records: BIGINT - Number of records added (from summary)
//   - deleted_records: BIGINT - Number of records deleted (from summary)
func (e *Executor) executeIcebergSnapshots(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build table options from plan options
	tableOpts := iceberg.DefaultTableOptions()

	// Set up filesystem for cloud URLs
	if filesystem.IsCloudURL(plan.Path) {
		fs, err := e.getFileSystemForPath(ctx.Context, plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to get filesystem for Iceberg table: %v", err),
			}
		}
		tableOpts.Filesystem = fs
	}

	// Open the Iceberg table
	table, err := iceberg.OpenTable(ctx.Context, plan.Path, tableOpts)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to open Iceberg table: %v", err),
		}
	}
	defer func() { _ = table.Close() }()

	// Get all snapshots
	snapshots := table.Snapshots()

	// Define output columns
	columns := []string{
		"snapshot_id",
		"parent_snapshot_id",
		"timestamp_ms",
		"timestamp",
		"manifest_list",
		"operation",
		"summary",
		"added_data_files",
		"deleted_data_files",
		"added_records",
		"deleted_records",
	}

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = []*catalog.ColumnDef{
			catalog.NewColumnDef("snapshot_id", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("parent_snapshot_id", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("timestamp_ms", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("timestamp", dukdb.TYPE_TIMESTAMP),
			catalog.NewColumnDef("manifest_list", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("operation", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("summary", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("added_data_files", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("deleted_data_files", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("added_records", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("deleted_records", dukdb.TYPE_BIGINT),
		}
	}

	// Build result rows
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0, len(snapshots)),
		Columns: columns,
	}

	for _, snap := range snapshots {
		// Extract summary fields
		operation := getSnapshotSummaryValue(snap.Summary, "operation")
		addedDataFiles := getSnapshotSummaryInt(snap.Summary, "added-data-files")
		deletedDataFiles := getSnapshotSummaryInt(snap.Summary, "deleted-data-files")
		addedRecords := getSnapshotSummaryInt(snap.Summary, "added-records")
		deletedRecords := getSnapshotSummaryInt(snap.Summary, "deleted-records")

		// Convert summary to JSON
		summaryJSON := ""
		if snap.Summary != nil {
			data, err := json.Marshal(snap.Summary)
			if err == nil {
				summaryJSON = string(data)
			}
		}

		// Convert timestamp
		timestamp := time.UnixMilli(snap.TimestampMs)

		// Build row
		row := map[string]any{
			"snapshot_id":        snap.SnapshotID,
			"parent_snapshot_id": snap.ParentSnapshotID,
			"timestamp_ms":       snap.TimestampMs,
			"timestamp":          timestamp,
			"manifest_list":      snap.ManifestListLocation,
			"operation":          operation,
			"summary":            summaryJSON,
			"added_data_files":   addedDataFiles,
			"deleted_data_files": deletedDataFiles,
			"added_records":      addedRecords,
			"deleted_records":    deletedRecords,
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// getSnapshotSummaryValue extracts a string value from the snapshot summary.
func getSnapshotSummaryValue(summary map[string]string, key string) string {
	if summary == nil {
		return ""
	}
	return summary[key]
}

// getSnapshotSummaryInt extracts an integer value from the snapshot summary.
// Returns 0 if the key doesn't exist or can't be parsed.
func getSnapshotSummaryInt(summary map[string]string, key string) int64 {
	if summary == nil {
		return 0
	}
	val, ok := summary[key]
	if !ok {
		return 0
	}
	var result int64
	_, _ = fmt.Sscanf(val, "%d", &result)
	return result
}

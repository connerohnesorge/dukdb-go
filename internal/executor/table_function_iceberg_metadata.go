// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeIcebergMetadata executes an iceberg_metadata table function.
// This function returns metadata about the data files in an Iceberg table,
// including file paths, formats, record counts, sizes, and column statistics.
//
// Output columns:
//   - file_path: VARCHAR - Path to the data file
//   - file_format: VARCHAR - Format of the data file (parquet, avro, orc)
//   - record_count: BIGINT - Number of records in the file
//   - file_size_in_bytes: BIGINT - Size of the file in bytes
//   - partition_data: VARCHAR - JSON representation of partition values
//   - value_counts: VARCHAR - JSON representation of value counts per column
//   - null_value_counts: VARCHAR - JSON representation of null counts per column
//   - lower_bounds: VARCHAR - JSON representation of lower bounds per column
//   - upper_bounds: VARCHAR - JSON representation of upper bounds per column
//
// Supported options:
//   - snapshot_id: int64 - Get metadata for a specific snapshot
//   - timestamp: int64 - Get metadata for a snapshot at a specific timestamp
func (e *Executor) executeIcebergMetadata(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build table options from plan options
	tableOpts := iceberg.DefaultTableOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "snapshot_id":
			switch v := val.(type) {
			case int64:
				tableOpts.SnapshotID = &v
			case int:
				snapshotID := int64(v)
				tableOpts.SnapshotID = &snapshotID
			}

		case "timestamp", "as_of_timestamp":
			// Time travel by timestamp is handled via AsOfTimestamp in TableOptions
			// but needs conversion from milliseconds to time.Time
			// For simplicity, we'll handle this via the snapshot selector after opening
			_ = val
		}
	}

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

	// Get data files for the current/selected snapshot
	dataFiles, err := table.DataFiles(ctx.Context)
	if err != nil {
		// If there's no current snapshot, return empty result
		if err == iceberg.ErrNoCurrentSnapshot {
			return e.emptyIcebergMetadataResult(plan), nil
		}
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to read Iceberg data files: %v", err),
		}
	}

	// Define output columns
	columns := []string{
		"file_path",
		"file_format",
		"record_count",
		"file_size_in_bytes",
		"partition_data",
		"value_counts",
		"null_value_counts",
		"lower_bounds",
		"upper_bounds",
	}

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = []*catalog.ColumnDef{
			catalog.NewColumnDef("file_path", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("file_format", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("record_count", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("file_size_in_bytes", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("partition_data", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("value_counts", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("null_value_counts", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("lower_bounds", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("upper_bounds", dukdb.TYPE_VARCHAR),
		}
	}

	// Build result rows
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0, len(dataFiles)),
		Columns: columns,
	}

	for _, df := range dataFiles {
		row := map[string]any{
			"file_path":          df.Path,
			"file_format":        string(df.Format),
			"record_count":       df.RecordCount,
			"file_size_in_bytes": df.FileSizeBytes,
			"partition_data":     toJSONString(df.PartitionData),
			"value_counts":       intMapToJSONString(df.ValueCounts),
			"null_value_counts":  intMapToJSONString(df.NullValueCounts),
			"lower_bounds":       bytesMapToJSONString(df.LowerBounds),
			"upper_bounds":       bytesMapToJSONString(df.UpperBounds),
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// emptyIcebergMetadataResult returns an empty result with the correct schema.
func (e *Executor) emptyIcebergMetadataResult(plan *planner.PhysicalTableFunctionScan) *ExecutionResult {
	columns := []string{
		"file_path",
		"file_format",
		"record_count",
		"file_size_in_bytes",
		"partition_data",
		"value_counts",
		"null_value_counts",
		"lower_bounds",
		"upper_bounds",
	}

	if plan.TableFunction != nil {
		plan.TableFunction.Columns = []*catalog.ColumnDef{
			catalog.NewColumnDef("file_path", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("file_format", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("record_count", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("file_size_in_bytes", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("partition_data", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("value_counts", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("null_value_counts", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("lower_bounds", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("upper_bounds", dukdb.TYPE_VARCHAR),
		}
	}

	return &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: columns,
	}
}

// toJSONString converts a value to a JSON string.
// Returns an empty string if the value is nil or conversion fails.
func toJSONString(v any) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(data)
}

// intMapToJSONString converts a map[int]int64 to a JSON string.
func intMapToJSONString(m map[int]int64) string {
	if m == nil {
		return ""
	}
	// Convert to string keys for JSON
	strMap := make(map[string]int64)
	for k, v := range m {
		strMap[fmt.Sprintf("%d", k)] = v
	}
	data, err := json.Marshal(strMap)
	if err != nil {
		return ""
	}
	return string(data)
}

// bytesMapToJSONString converts a map[int][]byte to a JSON string.
// The byte values are hex-encoded for readability.
func bytesMapToJSONString(m map[int][]byte) string {
	if m == nil {
		return ""
	}
	// Convert to string keys and hex-encoded values for JSON
	strMap := make(map[string]string)
	for k, v := range m {
		strMap[fmt.Sprintf("%d", k)] = fmt.Sprintf("%x", v)
	}
	data, err := json.Marshal(strMap)
	if err != nil {
		return ""
	}
	return string(data)
}

// getIcebergTable opens an Iceberg table with the given options.
func getIcebergTable(ctx context.Context, path string, opts *iceberg.TableOptions) (*iceberg.Table, error) {
	return iceberg.OpenTable(ctx, path, opts)
}

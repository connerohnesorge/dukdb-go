// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeIcebergTables executes the duckdb_iceberg_tables() table function.
// This function discovers Iceberg tables at a given location and returns metadata about each.
//
// Usage:
//
//	SELECT * FROM duckdb_iceberg_tables('/path/to/iceberg/warehouse');
//	SELECT * FROM duckdb_iceberg_tables('s3://bucket/warehouse');
//
// Output columns:
//   - table_name: VARCHAR - Name of the Iceberg table
//   - table_location: VARCHAR - Full path to the table
//   - current_snapshot_id: BIGINT - Current snapshot ID (NULL if no snapshots)
//   - last_updated_ms: BIGINT - Last update timestamp in milliseconds
//   - format_version: INTEGER - Iceberg format version (1 or 2)
//   - partition_columns: VARCHAR - Comma-separated list of partition column names
//
// Supported options:
//   - recursive: bool - Scan subdirectories recursively (default: false)
func (e *Executor) executeIcebergTables(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build discovery options from plan options
	opts := catalog.DefaultDiscoveryOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "recursive":
			if b, ok := val.(bool); ok {
				opts.Recursive = b
			} else if s, ok := val.(string); ok {
				opts.Recursive = strings.ToLower(s) == "true"
			}
		case "max_depth":
			switch v := val.(type) {
			case int64:
				opts.MaxDepth = int(v)
			case int:
				opts.MaxDepth = v
			}
		}
	}

	// Set up filesystem for cloud URLs
	if filesystem.IsCloudURL(plan.Path) {
		fs, err := e.getFileSystemForPath(ctx.Context, plan.Path)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to get filesystem for Iceberg discovery: %v", err),
			}
		}
		opts.Filesystem = fs
	}

	// Create discovery instance
	discovery := catalog.NewIcebergTableDiscovery(opts)

	// Discover tables
	tables, err := discovery.DiscoverTables(ctx.Context, plan.Path)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to discover Iceberg tables: %v", err),
		}
	}

	// Define output columns
	columns := []string{
		"table_name",
		"table_location",
		"current_snapshot_id",
		"last_updated_ms",
		"format_version",
		"partition_columns",
	}

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = []*catalog.ColumnDef{
			catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("table_location", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("current_snapshot_id", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("last_updated_ms", dukdb.TYPE_BIGINT),
			catalog.NewColumnDef("format_version", dukdb.TYPE_INTEGER),
			catalog.NewColumnDef("partition_columns", dukdb.TYPE_VARCHAR),
		}
	}

	// Build result rows
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0, len(tables)),
		Columns: columns,
	}

	for _, table := range tables {
		// Get partition columns from partition spec
		partitionCols := getPartitionColumnNames(table)

		row := map[string]any{
			"table_name":          table.Name,
			"table_location":      table.Location,
			"current_snapshot_id": table.CurrentSnapshotID, // Will be nil if no snapshots
			"last_updated_ms":     table.LastUpdatedMs,
			"format_version":      int32(table.FormatVersion),
			"partition_columns":   partitionCols,
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
}

// getPartitionColumnNames extracts partition column names from an Iceberg table entry.
func getPartitionColumnNames(entry *catalog.IcebergTableEntry) string {
	if entry.PartitionSpec.NumFields() == 0 {
		return ""
	}

	var names []string
	for field := range entry.PartitionSpec.Fields() {
		names = append(names, field.Name)
	}

	return strings.Join(names, ", ")
}

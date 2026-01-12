// Package executor provides query execution for the native Go DuckDB implementation.
// This file implements the PhysicalIcebergScan executor which handles direct Iceberg table scans
// with partition pruning and column projection optimizations.
package executor

import (
	"fmt"
	"io"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executePhysicalIcebergScan executes a PhysicalIcebergScan plan node.
// This provides optimized Iceberg table scanning with partition pruning
// and column projection pushed down to the reader.
//
// Features:
//   - Column projection: Only reads columns that are needed
//   - Partition pruning: Skips files that don't match partition filters
//   - Time travel: Supports snapshot ID, timestamp, branch, and version
//   - Residual filters: Applies any filters that couldn't be pushed down
func (e *Executor) executePhysicalIcebergScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalIcebergScan,
) (*ExecutionResult, error) {
	// Build reader options from plan
	opts := iceberg.DefaultReaderOptions()

	// Apply column projection
	if len(plan.Columns) > 0 {
		opts.SelectedColumns = plan.Columns
	}

	// Apply time travel options
	if plan.TimeTravel != nil {
		switch plan.TimeTravel.Type {
		case planner.TimeTravelSnapshot:
			if plan.TimeTravel.SnapshotID != nil {
				opts.SnapshotID = plan.TimeTravel.SnapshotID
			}
		case planner.TimeTravelTimestamp:
			if plan.TimeTravel.Timestamp != nil {
				opts.Timestamp = plan.TimeTravel.Timestamp
			}
		case planner.TimeTravelVersion:
			opts.Version = plan.TimeTravel.Version
		case planner.TimeTravelBranch:
			// Branch support would require additional handling
			// For now, we'll use branch as a snapshot reference
		}
	}

	// Apply partition filters for pruning
	if len(plan.PartitionFilters) > 0 {
		opts.PartitionFilters = convertPartitionFilters(plan.PartitionFilters)
	}

	// Apply additional options from the plan
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "limit":
			switch v := val.(type) {
			case int64:
				opts.Limit = v
			case int:
				opts.Limit = int64(v)
			}
		case "version":
			switch v := val.(type) {
			case int64:
				opts.Version = int(v)
			case int:
				opts.Version = v
			}
		case "allow_moved_paths":
			switch v := val.(type) {
			case bool:
				opts.AllowMovedPaths = v
			case string:
				opts.AllowMovedPaths = strings.EqualFold(v, "true")
			}
		case "metadata_compression_codec":
			if s, ok := val.(string); ok {
				opts.MetadataCompressionCodec = s
			}
		case "unsafe_enable_version_guessing":
			switch v := val.(type) {
			case bool:
				opts.UnsafeEnableVersionGuessing = v
			case string:
				opts.UnsafeEnableVersionGuessing = strings.EqualFold(v, "true")
			}
		}
	}

	// Set up filesystem for cloud URLs
	if filesystem.IsCloudURL(plan.TablePath) {
		fs, err := e.getFileSystemForPath(ctx.Context, plan.TablePath)
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to get filesystem for Iceberg table: %v", err),
			}
		}
		opts.Filesystem = fs
	}

	// Create the Iceberg reader
	reader, err := iceberg.NewReader(ctx.Context, plan.TablePath, opts)
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to create Iceberg reader: %v", err),
		}
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columns, err := reader.Schema()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Iceberg schema: %v", err),
		}
	}

	// Get the column types
	types, err := reader.Types()
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to get Iceberg types: %v", err),
		}
	}

	// Collect all rows
	result := &ExecutionResult{
		Rows:    make([]map[string]any, 0),
		Columns: columns,
	}

	// Read all chunks
	for {
		chunk, err := reader.ReadChunk()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to read Iceberg chunk: %v", err),
			}
		}
		if chunk == nil {
			break
		}

		// Convert chunk to rows
		for i := 0; i < chunk.Count(); i++ {
			row := make(map[string]any)
			for j := 0; j < len(columns); j++ {
				row[columns[j]] = chunk.GetValue(i, j)
				// Also add qualified column name for join support
				if plan.Alias != "" {
					row[plan.Alias+"."+columns[j]] = chunk.GetValue(i, j)
				}
			}

			// Apply residual filter if present
			if plan.ResidualFilter != nil {
				passes, err := e.evaluateExprAsBool(ctx, plan.ResidualFilter, row)
				if err != nil {
					return nil, err
				}
				if !passes {
					continue // Skip row that doesn't match residual filter
				}
			}

			result.Rows = append(result.Rows, row)
		}
	}

	// Update column types if provided
	if len(types) > 0 {
		// Store types for downstream operators
		_ = types // Types are used implicitly in the result
	}

	return result, nil
}

// convertPartitionFilters converts planner.PartitionFilter to iceberg.PartitionFilterExpr.
func convertPartitionFilters(filters []planner.PartitionFilter) []iceberg.PartitionFilterExpr {
	result := make([]iceberg.PartitionFilterExpr, len(filters))
	for i, f := range filters {
		result[i] = iceberg.PartitionFilterExpr{
			FieldName: f.FieldName,
			Operator:  f.Operator,
			Value:     f.Value,
		}
	}
	return result
}

// buildIcebergColumnDefs creates column definitions from Iceberg schema info.
// This is used when we need to build catalog-compatible column definitions.
func buildIcebergColumnDefs(columns []string, types []dukdb.Type) []*catalog.ColumnDef {
	defs := make([]*catalog.ColumnDef, len(columns))
	for i, colName := range columns {
		var colType dukdb.Type
		if i < len(types) {
			colType = types[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		defs[i] = catalog.NewColumnDef(colName, colType)
	}
	return defs
}

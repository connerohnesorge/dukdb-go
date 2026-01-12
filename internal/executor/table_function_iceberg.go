// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeIcebergScan executes an iceberg_scan table function.
// This function reads data from an Apache Iceberg table.
//
// Supported options:
//   - columns: []string - Column projection (which columns to read)
//   - snapshot_id: int64 - Time travel by snapshot ID
//   - timestamp: int64 - Time travel by timestamp (milliseconds since epoch)
//   - version: int - Explicit metadata version number
//   - allow_moved_paths: bool - Allow reading tables that have been moved
//   - metadata_compression_codec: string - Compression codec for metadata files
//   - unsafe_enable_version_guessing: bool - Enable auto-guessing of metadata version
func (e *Executor) executeIcebergScan(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	// Build reader options from plan options
	opts := iceberg.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range plan.Options {
		switch strings.ToLower(name) {
		case "columns":
			// Handle column projection specified as an option
			switch v := val.(type) {
			case []string:
				opts.SelectedColumns = v
			case []any:
				cols := make([]string, 0, len(v))
				for _, c := range v {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				opts.SelectedColumns = cols
			}

		case "snapshot_id":
			// Time travel by snapshot ID
			switch v := val.(type) {
			case int64:
				opts.SnapshotID = &v
			case int:
				snapshotID := int64(v)
				opts.SnapshotID = &snapshotID
			}

		case "timestamp", "as_of_timestamp":
			// Time travel by timestamp (milliseconds since epoch)
			switch v := val.(type) {
			case int64:
				opts.Timestamp = &v
			case int:
				ts := int64(v)
				opts.Timestamp = &ts
			}

		case "limit":
			// Row limit
			switch v := val.(type) {
			case int64:
				opts.Limit = v
			case int:
				opts.Limit = int64(v)
			}

		case "version":
			// Explicit metadata version number
			switch v := val.(type) {
			case int64:
				opts.Version = int(v)
			case int:
				opts.Version = v
			}

		case "allow_moved_paths":
			// Allow reading tables that have been moved
			switch v := val.(type) {
			case bool:
				opts.AllowMovedPaths = v
			case string:
				opts.AllowMovedPaths = strings.ToLower(v) == "true"
			}

		case "metadata_compression_codec":
			// Compression codec for metadata files
			if s, ok := val.(string); ok {
				opts.MetadataCompressionCodec = s
			}

		case "unsafe_enable_version_guessing":
			// Enable auto-guessing of metadata version when version-hint.text is missing
			switch v := val.(type) {
			case bool:
				opts.UnsafeEnableVersionGuessing = v
			case string:
				opts.UnsafeEnableVersionGuessing = strings.ToLower(v) == "true"
			}
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
		opts.Filesystem = fs
	}

	// Create the Iceberg reader
	reader, err := iceberg.NewReader(ctx.Context, plan.Path, opts)
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

	// Update the table function columns for future reference
	if plan.TableFunction != nil {
		plan.TableFunction.Columns = make([]*catalog.ColumnDef, len(columns))
		for i, colName := range columns {
			var colType dukdb.Type
			if i < len(types) {
				colType = types[i]
			} else {
				colType = dukdb.TYPE_VARCHAR
			}
			plan.TableFunction.Columns[i] = catalog.NewColumnDef(colName, colType)
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
			}
			result.Rows = append(result.Rows, row)
		}
	}

	return result, nil
}

// getFileSystemForPath returns a filesystem for the given path.
// For cloud URLs, it uses the FileSystemProvider with secret management.
func (e *Executor) getFileSystemForPath(ctx context.Context, path string) (filesystem.FileSystem, error) {
	provider := NewFileSystemProvider(e.getSecretManager())

	fs, err := provider.GetFileSystem(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to get filesystem: %w", err)
	}

	return fs, nil
}

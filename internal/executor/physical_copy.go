// Package executor provides query execution for the native Go DuckDB implementation.
package executor

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	arrowio "github.com/dukdb/dukdb-go/internal/io/arrow"
	csvio "github.com/dukdb/dukdb-go/internal/io/csv"
	jsonio "github.com/dukdb/dukdb-go/internal/io/json"
	parquetio "github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/secret"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// executeCopyFrom handles COPY FROM (import data from file into table).
func (e *Executor) executeCopyFrom(
	ctx *ExecutionContext,
	plan *planner.PhysicalCopyFrom,
) (*ExecutionResult, error) {
	// Get or create the storage table
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		// Create table in storage
		var err error
		table, err = e.storage.CreateTable(
			plan.Table,
			plan.TableDef.ColumnTypes(),
		)
		if err != nil {
			return nil, err
		}
	}

	// Determine file format from options or file extension
	format := detectFormat(plan.FilePath, plan.Options)

	// Create appropriate reader - use cloud filesystem for cloud URLs
	var reader fileio.FileReader
	var err error

	if IsCloudURL(plan.FilePath) {
		// Use filesystem provider for cloud URLs
		reader, err = e.createCloudFileReader(ctx.Context, plan.FilePath, format, plan.Options)
	} else {
		// Use local file reader for local paths
		reader, err = createFileReader(plan.FilePath, format, plan.Options)
	}
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to create file reader: %v", err),
		}
	}
	defer func() { _ = reader.Close() }()

	// Read chunks and insert into table
	rowsAffected := int64(0)
	columnTypes := table.ColumnTypes()

	// Determine column mapping
	var columnMap []int
	if len(plan.Columns) > 0 {
		columnMap = plan.Columns
	} else {
		// Use all columns in order
		columnMap = make([]int, len(plan.TableDef.Columns))
		for i := range plan.TableDef.Columns {
			columnMap[i] = i
		}
	}

	for {
		chunk, err := reader.ReadChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("error reading file: %v", err),
			}
		}

		// Map file columns to table columns
		mappedChunk := storage.NewDataChunkWithCapacity(columnTypes, chunk.Count())
		for row := 0; row < chunk.Count(); row++ {
			values := make([]any, len(plan.TableDef.Columns))
			for i := 0; i < chunk.ColumnCount(); i++ {
				if i < len(columnMap) {
					values[columnMap[i]] = chunk.GetValue(row, i)
				}
			}
			mappedChunk.AppendRow(values)
		}

		// Insert into table
		count, err := table.InsertChunk(mappedChunk)
		if err != nil {
			return nil, err
		}
		rowsAffected += int64(count)
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}

// executeCopyTo handles COPY TO (export data from table/query to file).
func (e *Executor) executeCopyTo(
	ctx *ExecutionContext,
	plan *planner.PhysicalCopyTo,
) (*ExecutionResult, error) {
	// Determine file format from options or file extension
	format := detectFormat(plan.FilePath, plan.Options)

	// Create appropriate writer - use cloud filesystem for cloud URLs
	var writer fileio.FileWriter
	var err error

	if IsCloudURL(plan.FilePath) {
		// Use filesystem provider for cloud URLs
		writer, err = e.createCloudFileWriter(ctx.Context, plan.FilePath, format, plan.Options)
	} else {
		// Use local file writer for local paths
		writer, err = createFileWriter(plan.FilePath, format, plan.Options)
	}
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to create file writer: %v", err),
		}
	}
	defer func() { _ = writer.Close() }()

	var columns []string
	var columnIndices []int

	// Determine source of data
	if plan.Source != nil {
		// COPY (SELECT...) TO - use the query result
		sourceResult, err := e.Execute(ctx.Context, plan.Source, ctx.Args)
		if err != nil {
			return nil, err
		}

		// Set schema from result columns
		if err := writer.SetSchema(sourceResult.Columns); err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to set schema: %v", err),
			}
		}

		// Convert result rows to DataChunk and write
		if len(sourceResult.Rows) > 0 {
			types := make([]dukdb.Type, len(sourceResult.Columns))
			for i := range types {
				types[i] = dukdb.TYPE_VARCHAR // Default to VARCHAR for now
			}

			chunk := storage.NewDataChunkWithCapacity(types, len(sourceResult.Rows))
			for _, row := range sourceResult.Rows {
				values := make([]any, len(sourceResult.Columns))
				for i, col := range sourceResult.Columns {
					values[i] = row[col]
				}
				chunk.AppendRow(values)
			}

			if err := writer.WriteChunk(chunk); err != nil {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeIO,
					Msg:  fmt.Sprintf("failed to write chunk: %v", err),
				}
			}
		}

		return &ExecutionResult{
			RowsAffected: int64(len(sourceResult.Rows)),
		}, nil
	}

	// COPY table TO - scan the table
	table, ok := e.storage.GetTable(plan.Table)
	if !ok {
		return nil, dukdb.ErrTableNotFound
	}

	// Determine column mapping
	if len(plan.Columns) > 0 {
		columnIndices = plan.Columns
		columns = make([]string, len(columnIndices))
		for i, idx := range columnIndices {
			columns[i] = plan.TableDef.Columns[idx].Name
		}
	} else {
		// Use all columns in order
		columnIndices = make([]int, len(plan.TableDef.Columns))
		columns = make([]string, len(plan.TableDef.Columns))
		for i, col := range plan.TableDef.Columns {
			columnIndices[i] = i
			columns[i] = col.Name
		}
	}

	// Set schema
	if err := writer.SetSchema(columns); err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeIO,
			Msg:  fmt.Sprintf("failed to set schema: %v", err),
		}
	}

	// Scan and write
	scanner := table.Scan()
	rowsAffected := int64(0)

	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		// Project columns if subset is specified
		var outputChunk *storage.DataChunk
		if len(plan.Columns) > 0 {
			types := make([]dukdb.Type, len(columnIndices))
			for i, idx := range columnIndices {
				types[i] = plan.TableDef.Columns[idx].Type
			}
			outputChunk = storage.NewDataChunkWithCapacity(types, chunk.Count())
			for row := 0; row < chunk.Count(); row++ {
				values := make([]any, len(columnIndices))
				for i, idx := range columnIndices {
					values[i] = chunk.GetValue(row, idx)
				}
				outputChunk.AppendRow(values)
			}
		} else {
			outputChunk = chunk
		}

		if err := writer.WriteChunk(outputChunk); err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeIO,
				Msg:  fmt.Sprintf("failed to write chunk: %v", err),
			}
		}
		rowsAffected += int64(outputChunk.Count())
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}

// detectFormat determines the file format from options or file extension.
func detectFormat(path string, options map[string]any) fileio.Format {
	// Check explicit FORMAT option
	if format, ok := options["FORMAT"]; ok {
		if formatStr, isStr := format.(string); isStr {
			switch strings.ToUpper(formatStr) {
			case "CSV":
				return fileio.FormatCSV
			case "PARQUET":
				return fileio.FormatParquet
			case "JSON":
				return fileio.FormatJSON
			case "NDJSON":
				return fileio.FormatNDJSON
			case "ARROW":
				return fileio.FormatArrow
			case "ARROW_STREAM", "ARROWS":
				return fileio.FormatArrowStream
			}
		}
	}

	// Auto-detect from extension
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv":
		return fileio.FormatCSV
	case ".parquet":
		return fileio.FormatParquet
	case ".json":
		return fileio.FormatJSON
	case ".ndjson":
		return fileio.FormatNDJSON
	case ".arrow", ".feather", ".ipc":
		return fileio.FormatArrow
	case ".arrows":
		return fileio.FormatArrowStream
	default:
		// Default to CSV
		return fileio.FormatCSV
	}
}

// createFileReader creates the appropriate file reader based on format.
func createFileReader(path string, format fileio.Format, options map[string]any) (fileio.FileReader, error) {
	switch format {
	case fileio.FormatCSV:
		opts := csvio.DefaultReaderOptions()
		applyCSVReaderOptions(opts, options)
		return csvio.NewReaderFromPath(path, opts)

	case fileio.FormatJSON:
		opts := jsonio.DefaultReaderOptions()
		applyJSONReaderOptions(opts, options)
		return jsonio.NewReaderFromPath(path, opts)

	case fileio.FormatNDJSON:
		opts := jsonio.DefaultReaderOptions()
		opts.Format = jsonio.FormatNDJSON
		applyJSONReaderOptions(opts, options)
		return jsonio.NewReaderFromPath(path, opts)

	case fileio.FormatParquet:
		opts := parquetio.DefaultReaderOptions()
		return parquetio.NewReaderFromPath(path, opts)

	case fileio.FormatArrow:
		opts := arrowio.DefaultReaderOptions()
		applyArrowReaderOptions(opts, options)
		return arrowio.NewReaderFromPath(path, opts)

	case fileio.FormatArrowStream:
		opts := arrowio.DefaultReaderOptions()
		applyArrowReaderOptions(opts, options)
		return arrowio.NewStreamReaderFromPath(path, opts)

	default:
		return nil, fmt.Errorf("unsupported format: %v", format)
	}
}

// createFileWriter creates the appropriate file writer based on format.
func createFileWriter(path string, format fileio.Format, options map[string]any) (fileio.FileWriter, error) {
	switch format {
	case fileio.FormatCSV:
		opts := csvio.DefaultWriterOptions()
		applyCSVWriterOptions(opts, options)
		return csvio.NewWriterToPath(path, opts)

	case fileio.FormatJSON:
		opts := jsonio.DefaultWriterOptions()
		applyJSONWriterOptions(opts, options)
		opts.Format = jsonio.FormatArray
		return jsonio.NewWriterToPath(path, opts)

	case fileio.FormatNDJSON:
		opts := jsonio.DefaultWriterOptions()
		applyJSONWriterOptions(opts, options)
		opts.Format = jsonio.FormatNDJSON
		return jsonio.NewWriterToPath(path, opts)

	case fileio.FormatParquet:
		opts := parquetio.DefaultWriterOptions()
		applyParquetWriterOptions(opts, options)
		return parquetio.NewWriterToPath(path, opts)

	case fileio.FormatArrow:
		opts := arrowio.DefaultWriterOptions()
		applyArrowWriterOptions(opts, options)
		return arrowio.NewWriterToPathOverwrite(path, opts)

	case fileio.FormatArrowStream:
		opts := arrowio.DefaultWriterOptions()
		applyArrowWriterOptions(opts, options)
		return arrowio.NewStreamWriterToPathOverwrite(path, opts)

	default:
		return nil, fmt.Errorf("unsupported format: %v", format)
	}
}

// applyCSVReaderOptions applies COPY options to CSV reader options.
func applyCSVReaderOptions(opts *csvio.ReaderOptions, options map[string]any) {
	if delim, ok := options["DELIMITER"]; ok {
		if delimStr, isStr := delim.(string); isStr && len(delimStr) > 0 {
			opts.Delimiter = rune(delimStr[0])
		}
	}
	if header, ok := options["HEADER"]; ok {
		if headerBool, isBool := header.(bool); isBool {
			opts.Header = headerBool
		}
	}
	if quote, ok := options["QUOTE"]; ok {
		if quoteStr, isStr := quote.(string); isStr && len(quoteStr) > 0 {
			opts.Quote = rune(quoteStr[0])
		}
	}
	if nullStr, ok := options["NULL"]; ok {
		if ns, isStr := nullStr.(string); isStr {
			opts.NullStr = ns
		}
	}
	if skip, ok := options["SKIP"]; ok {
		if skipInt, isInt := skip.(int64); isInt {
			opts.Skip = int(skipInt)
		}
	}
}

// applyCSVWriterOptions applies COPY options to CSV writer options.
func applyCSVWriterOptions(opts *csvio.WriterOptions, options map[string]any) {
	if delim, ok := options["DELIMITER"]; ok {
		if delimStr, isStr := delim.(string); isStr && len(delimStr) > 0 {
			opts.Delimiter = rune(delimStr[0])
		}
	}
	if header, ok := options["HEADER"]; ok {
		if headerBool, isBool := header.(bool); isBool {
			opts.Header = headerBool
		}
	}
	if quote, ok := options["QUOTE"]; ok {
		if quoteStr, isStr := quote.(string); isStr && len(quoteStr) > 0 {
			opts.Quote = rune(quoteStr[0])
		}
	}
	if nullStr, ok := options["NULL"]; ok {
		if ns, isStr := nullStr.(string); isStr {
			opts.NullStr = ns
		}
	}
	// Handle compression
	if comp, ok := options["COMPRESSION"]; ok {
		if compStr, isStr := comp.(string); isStr {
			opts.Compression = parseCompression(compStr)
		}
	}
}

// applyJSONReaderOptions applies COPY options to JSON reader options.
func applyJSONReaderOptions(opts *jsonio.ReaderOptions, options map[string]any) {
	if dateFormat, ok := options["DATEFORMAT"]; ok {
		if df, isStr := dateFormat.(string); isStr {
			opts.DateFormat = df
		}
	}
	if tsFormat, ok := options["TIMESTAMPFORMAT"]; ok {
		if tf, isStr := tsFormat.(string); isStr {
			opts.TimestampFormat = tf
		}
	}
}

// applyJSONWriterOptions applies COPY options to JSON writer options.
func applyJSONWriterOptions(opts *jsonio.WriterOptions, options map[string]any) {
	if dateFormat, ok := options["DATEFORMAT"]; ok {
		if df, isStr := dateFormat.(string); isStr {
			opts.DateFormat = df
		}
	}
	if tsFormat, ok := options["TIMESTAMPFORMAT"]; ok {
		if tf, isStr := tsFormat.(string); isStr {
			opts.TimestampFormat = tf
		}
	}
	// Handle compression
	if comp, ok := options["COMPRESSION"]; ok {
		if compStr, isStr := comp.(string); isStr {
			opts.Compression = parseCompression(compStr)
		}
	}
}

// applyParquetWriterOptions applies COPY options to Parquet writer options.
func applyParquetWriterOptions(opts *parquetio.WriterOptions, options map[string]any) {
	if codec, ok := options["CODEC"]; ok {
		if codecStr, isStr := codec.(string); isStr {
			opts.Codec = strings.ToUpper(codecStr)
		}
	}
	if level, ok := options["COMPRESSION_LEVEL"]; ok {
		if levelInt, isInt := level.(int64); isInt {
			opts.CompressionLevel = int(levelInt)
		}
	}
	if rowGroupSize, ok := options["ROW_GROUP_SIZE"]; ok {
		if rgs, isInt := rowGroupSize.(int64); isInt {
			opts.RowGroupSize = int(rgs)
		}
	}
}

// applyArrowReaderOptions applies COPY options to Arrow reader options.
func applyArrowReaderOptions(opts *arrowio.ReaderOptions, options map[string]any) {
	// Arrow reader currently only supports column projection
	if columns, ok := options["COLUMNS"]; ok {
		if colSlice, isSlice := columns.([]string); isSlice {
			opts.Columns = colSlice
		}
	}
}

// applyArrowWriterOptions applies COPY options to Arrow writer options.
func applyArrowWriterOptions(opts *arrowio.WriterOptions, options map[string]any) {
	// Handle compression option
	if comp, ok := options["COMPRESSION"]; ok {
		if compStr, isStr := comp.(string); isStr {
			opts.Compression = parseArrowCompression(compStr)
		}
	}
	// Also support CODEC for consistency with Parquet
	if codec, ok := options["CODEC"]; ok {
		if codecStr, isStr := codec.(string); isStr {
			opts.Compression = parseArrowCompression(codecStr)
		}
	}
}

// parseArrowCompression converts a compression string to arrowio.Compression.
func parseArrowCompression(comp string) arrowio.Compression {
	switch strings.ToUpper(comp) {
	case "LZ4":
		return arrowio.CompressionLZ4
	case "ZSTD":
		return arrowio.CompressionZSTD
	case "NONE", "":
		return arrowio.CompressionNone
	default:
		// Default to no compression for unsupported types
		return arrowio.CompressionNone
	}
}

// parseCompression converts a compression string to fileio.Compression.
func parseCompression(comp string) fileio.Compression {
	switch strings.ToUpper(comp) {
	case "GZIP":
		return fileio.CompressionGZIP
	case "ZSTD":
		return fileio.CompressionZSTD
	case "SNAPPY":
		return fileio.CompressionSnappy
	case "LZ4":
		return fileio.CompressionLZ4
	case "BROTLI":
		return fileio.CompressionBrotli
	default:
		return fileio.CompressionNone
	}
}

// getSecretManager returns a secret.Manager from the executor's SecretManager interface.
// Returns nil if the secret manager is not set or doesn't implement secret.Manager.
func (e *Executor) getSecretManager() secret.Manager {
	if e.secretManager == nil {
		return nil
	}
	// Try to cast to secret.Manager
	if mgr, ok := e.secretManager.(secret.Manager); ok {
		return mgr
	}
	return nil
}

// createCloudFileReader creates a file reader for cloud URLs using the FileSystemProvider.
func (e *Executor) createCloudFileReader(
	ctx context.Context,
	path string,
	format fileio.Format,
	options map[string]any,
) (fileio.FileReader, error) {
	// Create filesystem provider with secret manager
	provider := NewFileSystemProvider(e.getSecretManager())

	return createFileReaderFromFS(ctx, provider, path, format, options)
}

// createCloudFileWriter creates a file writer for cloud URLs using the FileSystemProvider.
func (e *Executor) createCloudFileWriter(
	ctx context.Context,
	path string,
	format fileio.Format,
	options map[string]any,
) (fileio.FileWriter, error) {
	// Create filesystem provider with secret manager
	provider := NewFileSystemProvider(e.getSecretManager())

	return createFileWriterFromFS(ctx, provider, path, format, options)
}

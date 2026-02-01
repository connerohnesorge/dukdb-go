// Package arrow provides Apache Arrow IPC file format support for dukdb-go.
//
// This package implements reading and writing of Arrow IPC files (both file format
// and streaming format), enabling efficient data exchange with Arrow-enabled systems
// like Apache Spark, Apache Flink, pandas, and more.
//
// # File Formats
//
// The package supports two Arrow IPC formats:
//
//   - File format (.arrow, .feather, .ipc): Random-access format with footer containing
//     schema and record batch locations. Supports seeking to specific batches.
//
//   - Stream format (.arrows): Sequential format without footer. Records are read
//     sequentially as they appear in the stream.
//
// # Reading Arrow Files
//
// To read an Arrow IPC file:
//
//	reader, err := arrow.NewReaderFromPath("data.arrow", nil)
//	if err != nil {
//		return err
//	}
//	defer reader.Close()
//
//	// Get column names
//	columns, _ := reader.Schema()
//	fmt.Println("Columns:", columns)
//
//	// Read chunks
//	for {
//		chunk, err := reader.ReadChunk()
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			return err
//		}
//		// Process chunk...
//	}
//
// For column projection (reading only specific columns):
//
//	opts := &arrow.ReaderOptions{
//		Columns: []string{"id", "name", "price"},
//	}
//	reader, err := arrow.NewReaderFromPath("data.arrow", opts)
//
// # Writing Arrow Files
//
// To write an Arrow IPC file:
//
//	writer, err := arrow.NewWriterToPath("output.arrow", nil)
//	if err != nil {
//		return err
//	}
//
//	// Set schema
//	err = writer.SetSchema([]string{"id", "name", "value"})
//	err = writer.SetTypes([]dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR, dukdb.TYPE_DOUBLE})
//
//	// Write chunks
//	err = writer.WriteChunk(chunk)
//
//	err = writer.Close() // Important: flushes buffers and writes footer
//
// For compression:
//
//	opts := &arrow.WriterOptions{
//		Compression: arrow.CompressionZSTD, // or CompressionLZ4, CompressionNone
//	}
//	writer, err := arrow.NewWriterToPath("output.arrow", opts)
//
// # Streaming Format
//
// For streaming Arrow IPC (no random access):
//
//	// Reading
//	reader, err := arrow.NewStreamReaderFromPath("data.arrows", nil)
//	defer reader.Close()
//
//	for reader.Next() {
//		record := reader.Record()
//		// Process record...
//	}
//	if err := reader.Err(); err != nil {
//		return err
//	}
//
//	// Writing
//	writer, err := arrow.NewStreamWriterToPath("output.arrows", nil)
//	defer writer.Close()
//
//	writer.SetSchema(columns)
//	writer.SetTypes(types)
//	writer.WriteChunk(chunk)
//
// # Cloud Storage
//
// The package supports reading from and writing to cloud storage via URL:
//
//	// S3
//	reader, err := arrow.NewReaderFromURL("s3://bucket/path/data.arrow", nil)
//
//	// Google Cloud Storage
//	reader, err := arrow.NewReaderFromURL("gs://bucket/path/data.arrow", nil)
//
//	// Azure Blob Storage
//	reader, err := arrow.NewReaderFromURL("az://container/path/data.arrow", nil)
//
//	// HTTP/HTTPS (read-only)
//	reader, err := arrow.NewReaderFromURL("https://example.com/data.arrow", nil)
//
// Cloud credentials are configured via environment variables or SDK defaults:
//
//   - S3: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (or IAM role)
//   - GCS: GOOGLE_APPLICATION_CREDENTIALS
//   - Azure: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
//
// # Batch Iterator
//
// For working with Arrow record batches directly:
//
//	reader, _ := arrow.NewReaderFromPath("data.arrow", nil)
//	defer reader.Close()
//
//	iter := reader.Iterator()
//	for iter.Next() {
//		record := iter.Record()
//		fmt.Printf("Batch has %d rows\n", record.NumRows())
//	}
//	if err := iter.Err(); err != nil {
//		return err
//	}
//
// # Random Access
//
// For file format, random access to specific batches is supported:
//
//	reader, _ := arrow.NewReaderFromPath("data.arrow", nil)
//	defer reader.Close()
//
//	fmt.Printf("File has %d record batches\n", reader.NumRecordBatches())
//
//	// Read specific batch
//	record, err := reader.RecordBatchAt(5)
//	if err != nil {
//		return err
//	}
//	defer record.Release()
//
// # Type Conversion
//
// The package converts between Arrow types and DuckDB types automatically.
// Supported DuckDB types include:
//
//   - Primitive: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT,
//     UINTEGER, UBIGINT, FLOAT, DOUBLE
//   - String/Binary: VARCHAR, BLOB, BIT
//   - Temporal: DATE, TIME, TIME_TZ, TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS,
//     TIMESTAMP_NS, TIMESTAMP_TZ, INTERVAL
//   - Complex: LIST, STRUCT, MAP, ARRAY
//   - Special: UUID, DECIMAL, HUGEINT, UHUGEINT, ENUM
//
// # Table Functions
//
// Arrow files can be queried via SQL using table functions:
//
//	SELECT * FROM read_arrow('data.arrow')
//	SELECT id, name FROM read_arrow('s3://bucket/data.arrow')
//	SELECT * FROM read_arrow_auto('data.ipc') -- auto-detect format
//
// # COPY Statement
//
// Arrow format is supported in COPY statements:
//
//	COPY table TO 'output.arrow' (FORMAT 'arrow')
//	COPY table TO 'output.arrow' (FORMAT 'arrow', COMPRESSION 'zstd')
//	COPY table FROM 'input.arrow'
//
// # Performance
//
// For optimal performance:
//
//   - Use column projection to read only needed columns
//   - Use LZ4 compression for fast compression/decompression
//   - Use ZSTD compression for best compression ratio
//   - For streaming scenarios, use stream format instead of file format
//   - For cloud storage, reads may be slower due to network latency
//
// # Memory Management
//
// The package uses Apache Arrow's memory management with proper Retain/Release
// patterns. Record batches obtained via RecordBatchAt() should be released after use:
//
//	record, _ := reader.RecordBatchAt(0)
//	defer record.Release()
//	// Use record...
//
// DataChunks returned by ReadChunk() are managed internally and do not require
// explicit release.
package arrow

//go:generate gomarkdoc -u -o CLAUDE.md .
//go:generate gomarkdoc -u -o AGENTS.md .

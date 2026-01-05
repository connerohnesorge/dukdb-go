# Design: Arrow/IPC Format Support

## Context

Apache Arrow IPC defines a binary format for serializing Arrow columnar data for efficient transport and storage. The format includes:
- Schema serialization using Flatbuffers
- RecordBatch serialization with optional compression
- Two modes: file format (random access) and stream format (sequential)

dukdb-go already has Arrow integration for in-memory RecordBatch conversion. This design extends that to support file-based IPC operations.

**Stakeholders**:
- Data engineers working with Arrow-based pipelines
- ML practitioners loading pre-processed feature data
- Analytics applications exchanging data with Python/R tools

**Constraints**:
- Must remain pure Go (no CGO)
- Must use existing `github.com/apache/arrow/go/v18` library
- Must integrate with existing FileSystem interface for cloud storage
- Must maintain consistent type mapping with existing Arrow conversion

## Goals / Non-Goals

**Goals**:
1. Read Arrow IPC files (both file and stream formats)
2. Write Arrow IPC files with configurable compression
3. Support streaming RecordBatches without loading entire file
4. Integrate with existing COPY statement infrastructure
5. Support cloud storage URLs (S3, GCS, Azure)
6. Handle all types already supported by Arrow integration

**Non-Goals**:
1. Arrow Flight protocol (gRPC-based streaming)
2. Custom extension types beyond DuckDB's type system
3. Memory-mapped I/O (may add later for performance)
4. Parallel file writing (single-writer for simplicity)

## Decisions

### Decision 1: Arrow IPC File Format Reading

**Arrow IPC File Structure**:
```
+------------------+
| Magic "ARROW1"   | 8 bytes (includes padding)
+------------------+
| Schema Message   | Flatbuffer-encoded schema
+------------------+
| Record Batch 1   | Data + Metadata
+------------------+
| Record Batch 2   | Data + Metadata
+------------------+
| ...              |
+------------------+
| Footer           | Schema copy + block locations
+------------------+
| Footer Length    | 4 bytes
+------------------+
| Magic "ARROW1"   | 6 bytes
+------------------+
```

**Go Implementation**:
```go
package arrow

import (
    "github.com/apache/arrow/go/v18/arrow"
    "github.com/apache/arrow/go/v18/arrow/ipc"
    "github.com/apache/arrow/go/v18/arrow/memory"
)

// ArrowFileReader reads Arrow IPC files
type ArrowFileReader struct {
    file       io.ReaderAt
    fileReader *ipc.FileReader
    schema     *arrow.Schema
    allocator  memory.Allocator
}

// NewArrowFileReader creates a reader for Arrow IPC files
func NewArrowFileReader(r io.ReaderAt, size int64) (*ArrowFileReader, error) {
    allocator := memory.NewGoAllocator()

    fileReader, err := ipc.NewFileReader(r, ipc.WithAllocator(allocator))
    if err != nil {
        return nil, fmt.Errorf("failed to open Arrow IPC file: %w", err)
    }

    return &ArrowFileReader{
        file:       r,
        fileReader: fileReader,
        schema:     fileReader.Schema(),
        allocator:  allocator,
    }, nil
}

// Schema returns the Arrow schema from the file
func (r *ArrowFileReader) Schema() *arrow.Schema {
    return r.schema
}

// NumRecordBatches returns the number of record batches in the file
func (r *ArrowFileReader) NumRecordBatches() int {
    return r.fileReader.NumRecords()
}

// ReadRecordBatch reads a specific record batch by index
func (r *ArrowFileReader) ReadRecordBatch(index int) (arrow.Record, error) {
    if index < 0 || index >= r.NumRecordBatches() {
        return nil, fmt.Errorf("record batch index %d out of range [0, %d)",
            index, r.NumRecordBatches())
    }
    return r.fileReader.Record(index)
}

// Close releases resources
func (r *ArrowFileReader) Close() error {
    return r.fileReader.Close()
}
```

**Rationale**:
- Uses standard Arrow Go library for file reading
- Supports random access via `Record(index)`
- Memory allocation managed by Go allocator
- Clean resource management via Close()

### Decision 2: Arrow IPC File Writing

**Go Implementation**:
```go
// ArrowFileWriter writes Arrow IPC files
type ArrowFileWriter struct {
    file       io.WriteCloser
    fileWriter *ipc.FileWriter
    schema     *arrow.Schema
    allocator  memory.Allocator
    opts       ArrowWriteOptions
}

// ArrowWriteOptions configures Arrow IPC writing
type ArrowWriteOptions struct {
    Compression CompressionType // None, LZ4, ZSTD
    BlockSize   int             // Target block size (0 = default)
}

// CompressionType for Arrow IPC
type CompressionType int

const (
    CompressionNone CompressionType = iota
    CompressionLZ4
    CompressionZSTD
)

// NewArrowFileWriter creates a writer for Arrow IPC files
func NewArrowFileWriter(w io.WriteCloser, schema *arrow.Schema, opts ArrowWriteOptions) (*ArrowFileWriter, error) {
    allocator := memory.NewGoAllocator()

    ipcOpts := []ipc.Option{
        ipc.WithAllocator(allocator),
    }

    // Apply compression if specified
    switch opts.Compression {
    case CompressionLZ4:
        ipcOpts = append(ipcOpts, ipc.WithLZ4())
    case CompressionZSTD:
        ipcOpts = append(ipcOpts, ipc.WithZstd())
    }

    fileWriter, err := ipc.NewFileWriter(w, ipcOpts...)
    if err != nil {
        return nil, fmt.Errorf("failed to create Arrow IPC writer: %w", err)
    }

    return &ArrowFileWriter{
        file:       w,
        fileWriter: fileWriter,
        schema:     schema,
        allocator:  allocator,
        opts:       opts,
    }, nil
}

// WriteRecordBatch writes a record batch to the file
func (w *ArrowFileWriter) WriteRecordBatch(record arrow.Record) error {
    return w.fileWriter.Write(record)
}

// Close finalizes the file (writes footer) and closes
func (w *ArrowFileWriter) Close() error {
    if err := w.fileWriter.Close(); err != nil {
        return err
    }
    return w.file.Close()
}
```

**Rationale**:
- Supports optional compression (LZ4, ZSTD)
- Footer automatically written on Close()
- Schema embedded in file for self-describing format
- Streaming writes without buffering entire dataset

### Decision 3: RecordBatch Streaming

**Stream Format vs File Format**:
- File format: Has footer, supports random access
- Stream format: No footer, sequential access only, lower overhead

**Go Implementation**:
```go
// ArrowStreamReader reads Arrow IPC streams
type ArrowStreamReader struct {
    reader    *ipc.Reader
    schema    *arrow.Schema
    allocator memory.Allocator
    current   arrow.Record
    done      bool
}

// NewArrowStreamReader creates a streaming reader
func NewArrowStreamReader(r io.Reader) (*ArrowStreamReader, error) {
    allocator := memory.NewGoAllocator()

    ipcReader, err := ipc.NewReader(r, ipc.WithAllocator(allocator))
    if err != nil {
        return nil, fmt.Errorf("failed to open Arrow IPC stream: %w", err)
    }

    return &ArrowStreamReader{
        reader:    ipcReader,
        schema:    ipcReader.Schema(),
        allocator: allocator,
    }, nil
}

// Next advances to the next record batch
func (r *ArrowStreamReader) Next() bool {
    if r.done {
        return false
    }

    if r.current != nil {
        r.current.Release()
        r.current = nil
    }

    if !r.reader.Next() {
        r.done = true
        return false
    }

    r.current = r.reader.Record()
    r.current.Retain() // Keep reference
    return true
}

// Record returns the current record batch
func (r *ArrowStreamReader) Record() arrow.Record {
    return r.current
}

// Close releases resources
func (r *ArrowStreamReader) Close() error {
    if r.current != nil {
        r.current.Release()
    }
    return r.reader.Release()
}

// ArrowStreamWriter writes Arrow IPC streams
type ArrowStreamWriter struct {
    writer *ipc.Writer
}

// NewArrowStreamWriter creates a streaming writer
func NewArrowStreamWriter(w io.Writer, schema *arrow.Schema) (*ArrowStreamWriter, error) {
    allocator := memory.NewGoAllocator()

    ipcWriter := ipc.NewWriter(w, ipc.WithSchema(schema), ipc.WithAllocator(allocator))

    return &ArrowStreamWriter{writer: ipcWriter}, nil
}

// Write writes a record batch to the stream
func (w *ArrowStreamWriter) Write(record arrow.Record) error {
    return w.writer.Write(record)
}

// Close finalizes the stream
func (w *ArrowStreamWriter) Close() error {
    return w.writer.Close()
}
```

**Rationale**:
- Streaming interface matches Go idioms (Next/Record pattern)
- Proper reference counting with Retain/Release
- Separate types for file vs stream for clarity
- Memory-efficient: only one batch in memory at a time

### Decision 4: Type Mapping (DuckDB to Arrow)

**Extended Type Mapping** (builds on existing arrow_convert.go):

```go
var duckdbToArrowType = map[Type]arrow.DataType{
    // Numeric types
    TYPE_BOOLEAN:   arrow.FixedWidthTypes.Boolean,
    TYPE_TINYINT:   arrow.PrimitiveTypes.Int8,
    TYPE_SMALLINT:  arrow.PrimitiveTypes.Int16,
    TYPE_INTEGER:   arrow.PrimitiveTypes.Int32,
    TYPE_BIGINT:    arrow.PrimitiveTypes.Int64,
    TYPE_UTINYINT:  arrow.PrimitiveTypes.Uint8,
    TYPE_USMALLINT: arrow.PrimitiveTypes.Uint16,
    TYPE_UINTEGER:  arrow.PrimitiveTypes.Uint32,
    TYPE_UBIGINT:   arrow.PrimitiveTypes.Uint64,
    TYPE_HUGEINT:   &arrow.Decimal128Type{Precision: 38, Scale: 0},
    TYPE_UHUGEINT:  &arrow.Decimal128Type{Precision: 38, Scale: 0},
    TYPE_FLOAT:     arrow.PrimitiveTypes.Float32,
    TYPE_DOUBLE:    arrow.PrimitiveTypes.Float64,

    // String types
    TYPE_VARCHAR:   arrow.BinaryTypes.String,
    TYPE_BLOB:      arrow.BinaryTypes.Binary,
    TYPE_BIT:       arrow.BinaryTypes.Binary,

    // Temporal types
    TYPE_DATE:         arrow.FixedWidthTypes.Date32,
    TYPE_TIME:         arrow.FixedWidthTypes.Time64us,
    TYPE_TIME_TZ:      arrow.FixedWidthTypes.Time64us,
    TYPE_TIMESTAMP:    &arrow.TimestampType{Unit: arrow.Microsecond},
    TYPE_TIMESTAMP_S:  &arrow.TimestampType{Unit: arrow.Second},
    TYPE_TIMESTAMP_MS: &arrow.TimestampType{Unit: arrow.Millisecond},
    TYPE_TIMESTAMP_NS: &arrow.TimestampType{Unit: arrow.Nanosecond},
    TYPE_TIMESTAMP_TZ: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
    TYPE_INTERVAL:     arrow.FixedWidthTypes.MonthDayNanoInterval,

    // Special types
    TYPE_UUID:     &arrow.FixedSizeBinaryType{ByteWidth: 16},
    TYPE_JSON:     arrow.BinaryTypes.String, // JSON stored as string
}

// Complex types require dynamic construction
func duckdbTypeToArrow(typ TypeInfo) (arrow.DataType, error) {
    // Check simple mapping first
    if arrowType, ok := duckdbToArrowType[typ.InternalType()]; ok {
        return arrowType, nil
    }

    switch typ.InternalType() {
    case TYPE_DECIMAL:
        details := typ.Details().(*DecimalDetails)
        return &arrow.Decimal128Type{
            Precision: int32(details.Width),
            Scale:     int32(details.Scale),
        }, nil

    case TYPE_LIST:
        details := typ.Details().(*ListDetails)
        childType, err := duckdbTypeToArrow(details.Child)
        if err != nil {
            return nil, err
        }
        return arrow.ListOf(childType), nil

    case TYPE_STRUCT:
        details := typ.Details().(*StructDetails)
        fields := make([]arrow.Field, len(details.Entries))
        for i, entry := range details.Entries {
            childType, err := duckdbTypeToArrow(entry.Type)
            if err != nil {
                return nil, err
            }
            fields[i] = arrow.Field{Name: entry.Name, Type: childType, Nullable: true}
        }
        return arrow.StructOf(fields...), nil

    case TYPE_MAP:
        details := typ.Details().(*MapDetails)
        keyType, err := duckdbTypeToArrow(details.Key)
        if err != nil {
            return nil, err
        }
        valueType, err := duckdbTypeToArrow(details.Value)
        if err != nil {
            return nil, err
        }
        return arrow.MapOf(keyType, valueType), nil

    case TYPE_ENUM:
        // Arrow dictionary encoding for enums
        return &arrow.DictionaryType{
            IndexType: arrow.PrimitiveTypes.Int32,
            ValueType: arrow.BinaryTypes.String,
        }, nil

    case TYPE_UNION:
        details := typ.Details().(*UnionDetails)
        fields := make([]arrow.Field, len(details.Members))
        typeCodes := make([]arrow.UnionTypeCode, len(details.Members))
        for i, member := range details.Members {
            childType, err := duckdbTypeToArrow(member.Type)
            if err != nil {
                return nil, err
            }
            fields[i] = arrow.Field{Name: member.Tag, Type: childType, Nullable: true}
            typeCodes[i] = arrow.UnionTypeCode(i)
        }
        return arrow.DenseUnionOf(fields, typeCodes), nil

    default:
        return nil, fmt.Errorf("unsupported type for Arrow conversion: %v", typ)
    }
}
```

**Rationale**:
- Extends existing conversion in arrow_convert.go
- Handles all DuckDB types including complex nested types
- Uses Arrow's dictionary encoding for ENUMs
- Decimal mapped to Decimal128 for full precision
- Time zones preserved in timestamp types

### Decision 5: Memory Management (Arrow Memory Model)

**Arrow Memory Pools**:
Arrow uses reference-counted memory with explicit Retain/Release. The Go implementation manages this via allocators.

```go
// ArrowMemoryManager wraps Arrow memory allocation
type ArrowMemoryManager struct {
    allocator memory.Allocator
    pools     []*memory.GoAllocator
}

// NewArrowMemoryManager creates a memory manager
func NewArrowMemoryManager() *ArrowMemoryManager {
    return &ArrowMemoryManager{
        allocator: memory.NewGoAllocator(),
    }
}

// RecordBatchBuilder helps build Arrow records from DuckDB DataChunks
type RecordBatchBuilder struct {
    allocator memory.Allocator
    schema    *arrow.Schema
    builders  []array.Builder
}

// NewRecordBatchBuilder creates a builder for the given schema
func NewRecordBatchBuilder(schema *arrow.Schema) *RecordBatchBuilder {
    allocator := memory.NewGoAllocator()
    builders := make([]array.Builder, len(schema.Fields()))

    for i, field := range schema.Fields() {
        builders[i] = array.NewBuilder(allocator, field.Type)
    }

    return &RecordBatchBuilder{
        allocator: allocator,
        schema:    schema,
        builders:  builders,
    }
}

// AppendDataChunk appends a DuckDB DataChunk to the record batch
func (b *RecordBatchBuilder) AppendDataChunk(chunk *DataChunk) error {
    for colIdx := 0; colIdx < chunk.ColumnCount(); colIdx++ {
        vec := chunk.Column(colIdx)
        builder := b.builders[colIdx]

        for rowIdx := 0; rowIdx < chunk.Size(); rowIdx++ {
            if !vec.IsValid(rowIdx) {
                builder.AppendNull()
                continue
            }

            if err := appendValue(builder, vec.GetValue(rowIdx)); err != nil {
                return err
            }
        }
    }
    return nil
}

// Build creates the Arrow record and resets builders
func (b *RecordBatchBuilder) Build() arrow.Record {
    arrays := make([]arrow.Array, len(b.builders))
    for i, builder := range b.builders {
        arrays[i] = builder.NewArray()
    }

    return array.NewRecord(b.schema, arrays, int64(arrays[0].Len()))
}

// Release frees builder resources
func (b *RecordBatchBuilder) Release() {
    for _, builder := range b.builders {
        builder.Release()
    }
}
```

**Rationale**:
- Uses Go allocator for GC-managed memory
- Builder pattern for efficient record construction
- Proper Release calls to avoid memory leaks
- Batch processing minimizes allocation overhead

### Decision 6: Integration with Existing Storage

**Table Function Registration**:
```go
// RegisterArrowTableFunctions registers read_arrow and related functions
func RegisterArrowTableFunctions(catalog *Catalog) {
    catalog.RegisterTableFunction("read_arrow", &ReadArrowFunction{})
    catalog.RegisterTableFunction("read_arrow_auto", &ReadArrowAutoFunction{})
}

// ReadArrowFunction implements the read_arrow table function
type ReadArrowFunction struct{}

func (f *ReadArrowFunction) Bind(args []any) (*TableFunctionBind, error) {
    if len(args) < 1 {
        return nil, errors.New("read_arrow requires file path argument")
    }

    path, ok := args[0].(string)
    if !ok {
        return nil, errors.New("read_arrow: path must be a string")
    }

    // Open file via FileSystem interface
    fs := GetFileSystem(path)
    file, err := fs.Open(path)
    if err != nil {
        return nil, fmt.Errorf("failed to open Arrow file: %w", err)
    }

    // Detect format and create reader
    reader, err := NewArrowFileReader(file, file.Size())
    if err != nil {
        file.Close()
        return nil, err
    }

    // Convert Arrow schema to DuckDB columns
    columns, err := arrowSchemaToDuckDB(reader.Schema())
    if err != nil {
        reader.Close()
        return nil, err
    }

    return &TableFunctionBind{
        Columns: columns,
        Data:    reader,
    }, nil
}

func (f *ReadArrowFunction) Execute(bind *TableFunctionBind, output *DataChunk) error {
    reader := bind.Data.(*ArrowFileReader)

    // Read next batch
    if bind.CurrentBatch >= reader.NumRecordBatches() {
        return io.EOF
    }

    record, err := reader.ReadRecordBatch(bind.CurrentBatch)
    if err != nil {
        return err
    }
    defer record.Release()

    bind.CurrentBatch++

    // Convert Arrow record to DataChunk
    return arrowRecordToDataChunk(record, output)
}
```

**COPY Statement Integration**:
```go
// ArrowCopyHandler handles COPY TO/FROM for Arrow format
type ArrowCopyHandler struct{}

func (h *ArrowCopyHandler) CanHandle(format string) bool {
    return format == "arrow" || format == "ipc"
}

func (h *ArrowCopyHandler) Export(ctx context.Context, query QueryResult, path string, opts CopyOptions) error {
    // Get schema from query result
    schema, err := queryResultToArrowSchema(query.Columns())
    if err != nil {
        return err
    }

    // Create file via FileSystem
    fs := GetFileSystem(path)
    file, err := fs.Create(path)
    if err != nil {
        return err
    }

    // Create Arrow writer
    compression := CompressionNone
    if opts.Compression == "lz4" {
        compression = CompressionLZ4
    } else if opts.Compression == "zstd" {
        compression = CompressionZSTD
    }

    writer, err := NewArrowFileWriter(file, schema, ArrowWriteOptions{
        Compression: compression,
    })
    if err != nil {
        file.Close()
        return err
    }
    defer writer.Close()

    // Write batches
    builder := NewRecordBatchBuilder(schema)
    defer builder.Release()

    for {
        chunk, err := query.FetchChunk()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }

        builder.AppendDataChunk(chunk)
        record := builder.Build()

        if err := writer.WriteRecordBatch(record); err != nil {
            record.Release()
            return err
        }
        record.Release()
    }

    return nil
}
```

**Rationale**:
- Table function pattern matches existing CSV/JSON/Parquet
- FileSystem interface enables cloud storage support
- COPY handler integrates with existing COPY infrastructure
- Streaming writes for memory efficiency

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Arrow library version changes | Medium | Pin to v18, test with new versions |
| Memory pressure for large files | Medium | Streaming reads, batch-at-a-time |
| Complex type mapping edge cases | Low | Comprehensive type tests |
| Performance vs native Arrow | Low | Arrow Go library is well-optimized |

## Migration Plan

### Phase 1: IPC File Reading (Week 1)
1. Create `internal/io/arrow/` package structure
2. Implement ArrowFileReader with file format support
3. Implement ArrowStreamReader for stream format
4. Add type conversion from Arrow to DuckDB

### Phase 2: IPC File Writing (Week 2)
1. Implement ArrowFileWriter with compression support
2. Implement ArrowStreamWriter
3. Add type conversion from DuckDB to Arrow
4. Implement RecordBatchBuilder for efficient construction

### Phase 3: Table Functions (Week 3)
1. Register `read_arrow` table function
2. Register `read_arrow_auto` for format detection
3. Add parameter handling (columns, filters)
4. Integrate with query planner

### Phase 4: COPY Integration (Week 4)
1. Implement ArrowCopyHandler
2. Add `FORMAT 'arrow'` support to COPY TO
3. Add `FORMAT 'arrow'` support to COPY FROM
4. Add compression options

### Phase 5: Testing & Polish (Week 5)
1. End-to-end tests with real Arrow files
2. Cloud storage integration tests
3. Performance benchmarks
4. Documentation

## Open Questions

1. **Should we support Arrow Flight?**
   - Answer: No for now, out of scope (gRPC adds complexity)
   - Can be added as separate proposal later

2. **Memory-mapped file support?**
   - Answer: Defer to later optimization phase
   - Current streaming approach is sufficient

3. **Parallel reading?**
   - Answer: Not in initial implementation
   - Arrow file format supports it, can add later

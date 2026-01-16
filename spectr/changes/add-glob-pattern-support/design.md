# Design: Glob Pattern Support Implementation

## Implementation Details

### 1. Glob Pattern Matching Engine

**Location**: `internal/io/filesystem/glob.go`

```go
// GlobMatcher handles pattern matching and file expansion
type GlobMatcher struct {
    fs FileSystem
}

// Match expands a glob pattern into a list of file paths
func (gm *GlobMatcher) Match(pattern string) ([]string, error)

// MatchMultiple handles array of patterns or files
func (gm *GlobMatcher) MatchMultiple(patterns []string) ([]string, error)
```

**Pattern Grammar** (using standard glob syntax, matching DuckDB v1.4.3):
- `*` - matches any sequence of non-separator characters
- `**` - matches zero or more directories (recursive, **only one allowed per pattern**)
- `?` - matches exactly one non-separator character
- `[abc]` - matches one character from set
- `[a-z]` - matches one character from range
- `[!abc]` - negates character class (excludes a, b, c)
- `\` - escape character for literal wildcards

**NOT SUPPORTED** (differs from some glob implementations):
- `{a,b,c}` - brace expansion (NOT in DuckDB v1.4.3)
- `!pattern` - path-level negation (only `[!...]` bracket negation supported)

**Algorithm**:
1. Validate pattern (error if multiple `**` present)
2. Parse pattern into segments separated by `/`
3. For each segment:
   - If literal: navigate to that directory
   - If contains wildcards: expand using directory listing
   - If `**`: recursively walk directories (skip symlinks to avoid infinite loops)
4. Sort results alphabetically (default)
5. Apply FileGlobOptions behavior (DISALLOW_EMPTY, ALLOW_EMPTY, or FALLBACK_GLOB)

### 2. Cloud Storage Integration

**S3 Glob** (`internal/io/filesystem/s3_filesystem.go`):
```go
func (s3fs *S3FileSystem) Glob(pattern string) ([]string, error) {
    // Use ListObjectsV2 with prefix optimization
    prefix := extractPrefix(pattern)  // e.g., "data/2024/" from "data/2024/**/*.parquet"

    var files []string
    err := s3fs.client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
        Bucket: aws.String(bucket),
        Prefix: aws.String(prefix),
    }, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
        for _, obj := range page.Contents {
            if matchPattern(aws.StringValue(obj.Key), pattern) {
                files = append(files, "s3://"+bucket+"/"+*obj.Key)
            }
        }
        return true
    })

    return files, err
}
```

**GCS Glob** (`internal/io/filesystem/gcs_filesystem.go`):
```go
func (gcsfs *GCSFileSystem) Glob(pattern string) ([]string, error) {
    // Similar approach using GCS iterator with prefix
    prefix := extractPrefix(pattern)
    query := &storage.Query{Prefix: prefix}

    it := gcsfs.bucket.Objects(ctx, query)
    var files []string
    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if matchPattern(attrs.Name, pattern) {
            files = append(files, "gs://"+bucketName+"/"+attrs.Name)
        }
    }
    return files, nil
}
```

### 3. FileSystem Interface Extension

**Location**: `internal/io/filesystem/filesystem.go`

```go
type FileSystem interface {
    // Existing methods...
    Open(path string) (ReadSeekCloser, error)
    Create(path string) (io.WriteCloser, error)

    // NEW: Glob support
    Glob(pattern string) ([]string, error)

    // NEW: Supports glob (some filesystems may not)
    SupportsGlob() bool
}
```

**Fallback Strategy**:
- Local filesystem: Use `filepath.Glob` + custom `**` support
- Cloud storage: Use list APIs with pattern matching
- HTTP: Not supported (return error)

### 4. Table Function Integration

**Location**: `internal/executor/table_function_csv.go`, `table_function_json.go`, etc.

```go
// Before: Single file only
func readCSV(ctx *ExecutionContext, args []Expr) (*DataChunk, error) {
    path := evaluateExpr(args[0])  // Must be single path
    reader := csv.NewReader(path)
    return reader.Read()
}

// After: Glob pattern support
func readCSV(ctx *ExecutionContext, args []Expr) (*DataChunk, error) {
    pathExpr := args[0]

    // Handle array of files: ['file1.csv', 'file2.csv']
    if isArrayLiteral(pathExpr) {
        paths := evaluateArrayExpr(pathExpr)
        return readMultipleFiles(paths, csv.NewReader)
    }

    // Handle glob pattern: 'data/*.csv'
    pattern := evaluateExpr(pathExpr)
    if containsGlobPattern(pattern) {
        fs := ctx.FileSystem
        matches, err := fs.Glob(pattern)
        if err != nil {
            return nil, err
        }
        if len(matches) == 0 {
            return nil, fmt.Errorf("no files match pattern: %s", pattern)
        }
        return readMultipleFiles(matches, csv.NewReader)
    }

    // Single file (existing code path)
    reader := csv.NewReader(pattern)
    return reader.Read()
}

// Union multiple files with schema alignment
func readMultipleFiles(paths []string, readerFactory func(string) Reader) (*DataChunk, error) {
    var allChunks []*DataChunk
    var unionSchema *Schema  // Union-by-name schema

    for _, path := range paths {
        reader := readerFactory(path)
        chunk, err := reader.Read()
        if err != nil {
            return nil, fmt.Errorf("error reading %s: %w", path, err)
        }

        // Align schema (union-by-name)
        if unionSchema == nil {
            unionSchema = chunk.Schema
        } else {
            unionSchema = mergeSchemas(unionSchema, chunk.Schema)
            chunk = alignChunkToSchema(chunk, unionSchema)
        }

        allChunks = append(allChunks, chunk)
    }

    return concatenateChunks(allChunks), nil
}
```

### 5. Virtual Metadata Columns

**Virtual Columns**: DuckDB-compatible metadata columns added to results

```go
type ReadOptions struct {
    // Existing options...
    Filename       bool      // Add filename column (default: false)
    FilenameColumn string    // Column name (default: "filename")
    FileRowNumber  bool      // Add file_row_number column (default: false)
    FileIndex      bool      // Add file_index column (default: false)
}

// FileGlobOptions controls behavior when no files match pattern
type FileGlobOptions int

const (
    DisallowEmpty FileGlobOptions = iota  // Error if no files match (default)
    AllowEmpty                             // Return empty result if no files match
    FallbackGlob                           // Try treating as directory with **/*.{ext}
)

func addVirtualColumns(chunk *DataChunk, filepath string, fileIdx int, options *ReadOptions) *DataChunk {
    rowCount := chunk.RowCount()

    // Add filename column if requested
    if options.Filename {
        filenameCol := NewVarcharVector(rowCount)
        for i := 0; i < rowCount; i++ {
            filenameCol.Set(i, filepath)
        }
        chunk.Columns = append(chunk.Columns, filenameCol)
        chunk.Schema.Columns = append(chunk.Schema.Columns, ColumnInfo{
            Name: options.FilenameColumn,  // default: "filename"
            Type: TypeVarchar,
        })
    }

    // Add file_row_number column if requested (0-based row number within file)
    if options.FileRowNumber {
        rowNumCol := NewBigintVector(rowCount)
        for i := 0; i < rowCount; i++ {
            rowNumCol.Set(i, int64(i))
        }
        chunk.Columns = append(chunk.Columns, rowNumCol)
        chunk.Schema.Columns = append(chunk.Schema.Columns, ColumnInfo{
            Name: "file_row_number",
            Type: TypeBigint,
        })
    }

    // Add file_index column if requested (0-based index in file list)
    if options.FileIndex {
        fileIdxCol := NewUBigintVector(rowCount)
        for i := 0; i < rowCount; i++ {
            fileIdxCol.Set(i, uint64(fileIdx))
        }
        chunk.Columns = append(chunk.Columns, fileIdxCol)
        chunk.Schema.Columns = append(chunk.Schema.Columns, ColumnInfo{
            Name: "file_index",
            Type: TypeUBigint,
        })
    }

    return chunk
}
```

### 6. Hive Partitioning Support

**Pattern**: `year=2024/month=01/day=15/*.parquet`

```go
type ReadOptions struct {
    // Existing options...
    HivePartitioning          bool                // Enable Hive partitioning (default: false, auto-detect if true)
    AutoDetectHivePartitioning bool               // Auto-detect from first file (default: true)
    HiveTypesAutocast         bool                // Infer types for partition values (default: true)
    HiveTypesSchema           map[string]Type     // Explicit type schema for partitions (optional)
}

func extractHivePartitions(filepath string) map[string]string {
    partitions := make(map[string]string)
    parts := strings.Split(filepath, "/")

    for _, part := range parts {
        if strings.Contains(part, "=") {
            kv := strings.SplitN(part, "=", 2)
            partitions[kv[0]] = kv[1]
        }
    }

    return partitions
}

func inferHivePartitionType(value string) Type {
    // Try BIGINT (DuckDB auto-detects DATE, TIMESTAMP, and BIGINT only)
    if _, err := strconv.ParseInt(value, 10, 64); err == nil {
        return TypeBigint
    }
    // Try DATE
    if _, err := time.Parse("2006-01-02", value); err == nil {
        return TypeDate
    }
    // Try TIMESTAMP
    if _, err := time.Parse(time.RFC3339, value); err == nil {
        return TypeTimestamp
    }
    // Default to VARCHAR
    return TypeVarchar
}

func addHivePartitionColumns(chunk *DataChunk, filepath string, options *ReadOptions) *DataChunk {
    if !options.HivePartitioning {
        return chunk
    }

    partitions := extractHivePartitions(filepath)
    for key, value := range partitions {
        // Determine type for partition column
        var colType Type
        if explicitType, ok := options.HiveTypesSchema[key]; ok {
            colType = explicitType  // Use explicit type if provided
        } else if options.HiveTypesAutocast {
            colType = inferHivePartitionType(value)  // Infer type from value
        } else {
            colType = TypeVarchar  // Default to VARCHAR
        }

        // Create column with appropriate type
        col := NewVectorOfType(colType, chunk.RowCount())
        for i := 0; i < chunk.RowCount(); i++ {
            col.Set(i, castStringToType(value, colType))
        }

        chunk.Columns = append(chunk.Columns, col)
        chunk.Schema.Columns = append(chunk.Schema.Columns, ColumnInfo{
            Name: key,
            Type: colType,
        })
    }

    return chunk
}
```

### 7. Array Syntax Parser Integration

**Location**: `internal/parser/parser.go`

```go
// Parse array literals: ['file1.csv', 'file2.csv']
func (p *Parser) parseArrayLiteral() Expr {
    // [ 'file1', 'file2', ... ]
    p.expect(TOKEN_LBRACKET)

    var elements []Expr
    for !p.match(TOKEN_RBRACKET) {
        elements = append(elements, p.parseExpr())
        if !p.match(TOKEN_COMMA) {
            break
        }
    }

    p.expect(TOKEN_RBRACKET)

    return &ArrayExpr{Elements: elements}
}
```

## Context

**Problem**: Users cannot read multiple files in a single query without manual iteration.

**Constraints**:
- Must maintain pure Go (no cgo)
- Must work with all storage backends (local, S3, GCS, Azure)
- Must handle large directory listings efficiently
- Must preserve API compatibility (additive only)

**Stakeholders**:
- Data engineers using dukdb-go for data lake analytics
- Users migrating from DuckDB CLI
- Cloud data pipeline users

## Goals / Non-Goals

**Goals**:
- ✅ Support standard glob patterns (`*`, `**`, `?`, `[...]`)
- ✅ Support array of files syntax
- ✅ Support all file formats (CSV, JSON, Parquet, XLSX, Arrow)
- ✅ Support all storage backends (local, S3, GCS, Azure)
- ✅ Efficient cloud storage listing with prefix optimization
- ✅ Union-by-name for schema alignment
- ✅ Filename metadata column option
- ✅ Hive partitioning support

**Non-Goals**:
- ❌ Custom sorting/filtering beyond pattern matching (use SQL WHERE/ORDER BY)
- ❌ Regex patterns (only glob patterns)
- ❌ Automatic schema evolution detection (use explicit schema if needed)
- ❌ Caching of directory listings (defer to future optimization)

## Decisions

### Decision 1: Glob Syntax Compatibility
**Choice**: Use standard glob syntax (POSIX + `**` extension)
**Rationale**:
- Familiar to users (shell, Python, Node.js)
- DuckDB-compatible
- Libraries exist for parsing

**Alternatives Considered**:
- Regex patterns: Too complex, less user-friendly
- SQL LIKE patterns: Limited expressiveness
- Custom syntax: Learning curve, incompatible

### Decision 2: Schema Handling
**Choice**: Union-by-name (align columns by name, add missing columns with NULL)
**Rationale**:
- Matches DuckDB behavior
- Handles schema evolution gracefully
- Works with Hive partitioning

**Alternatives Considered**:
- Error on mismatch: Too strict, breaks common use cases
- Union-by-position: Fragile, error-prone
- First-file-wins: Loses data from later files

### Decision 3: File Ordering
**Choice**: Alphabetical by default, with optional sort parameter
**Rationale**:
- Deterministic behavior
- Matches shell glob behavior
- Allows reproducible queries

**Alternatives Considered**:
- Modification time: Non-deterministic, cloud storage has eventual consistency
- Size: Not useful for most use cases
- Random: Non-deterministic

### Decision 4: Cloud Storage Optimization
**Choice**: Extract prefix from pattern and use list APIs with prefix filter
**Example**: `s3://bucket/data/2024/**/*.parquet` → prefix `data/2024/`
**Rationale**:
- Reduces API calls (list only relevant objects)
- Faster for large buckets
- Works with all cloud providers

**Alternatives Considered**:
- List entire bucket: Too slow for large buckets
- Manual chunking: Complex, error-prone

## Risks / Trade-offs

### Risk 1: Large Directory Listings
**Risk**: Globbing `s3://huge-bucket/**/*.parquet` with millions of files may OOM
**Mitigation**:
- Add max file limit (default: 10,000 files, configurable)
- Stream file list instead of loading all at once
- Warn user when approaching limit

### Risk 2: Cloud API Rate Limits
**Risk**: ListObjectsV2 rate limits on S3 (3,500 requests/second)
**Mitigation**:
- Exponential backoff retry logic
- Batch file processing
- Document rate limit considerations

### Risk 3: Schema Mismatch Edge Cases
**Risk**: Union-by-name may produce unexpected results if column types differ
**Mitigation**:
- Validate type compatibility (INTEGER + BIGINT → BIGINT, INTEGER + VARCHAR → error)
- Add strict mode option (fail on any schema mismatch)
- Document expected behavior

### Trade-off: Performance vs Flexibility
**Choice**: Prioritize correctness over maximum performance in v1
**Rationale**:
- Get feature working correctly first
- Profile and optimize later
- Avoid premature optimization

## Migration Plan

**No migration needed** - this is additive functionality.

**Compatibility**:
- Existing single-file queries work unchanged
- New glob patterns are opt-in (must contain `*`, `**`, `?`, `[`, or `{`)

**Rollout**:
1. Phase 1: Local filesystem glob support
2. Phase 2: S3 glob support
3. Phase 3: GCS, Azure glob support
4. Phase 4: Hive partitioning and advanced features

## Open Questions

1. **Q**: Should we support exclude patterns (`!backup/*.csv`)?
   **A**: Yes, add in Phase 4 (advanced features)

2. **Q**: How to handle symlinks in local filesystem?
   **A**: Follow symlinks by default (match `filepath.Glob` behavior)

3. **Q**: Should array syntax support glob patterns per element (`['data/*.csv', 'backup/*.csv']`)?
   **A**: Yes, each array element can be a glob pattern

4. **Q**: Max file limit for glob expansion?
   **A**: Default 10,000 files, configurable via `max_files_per_glob` setting

5. **Q**: File ordering options?
   **A**: v1: Alphabetical only. v2: Add `file_order` option (alpha, mtime, size, random)

## Performance Considerations

**Benchmarks to Add**:
- Local filesystem: 1,000 files, 10,000 files
- S3: 1,000 objects, 10,000 objects, 100,000 objects (with prefix optimization)
- Schema alignment overhead: 10 files with different schemas

**Expected Performance**:
- Local: ~1ms for 1,000 files (limited by filesystem metadata access)
- S3 with prefix: ~500ms for 10,000 objects (5-10 ListObjectsV2 calls at 500 req/s)
- Schema merge: <1ms per file (negligible)

**Optimization Opportunities** (future):
- Parallel file reading (already planned in `parallel-execution` spec)
- Directory listing cache with TTL
- Pushdown filters to reduce files read (e.g., WHERE year = 2024 → only read year=2024/*)

# Design: S3/Cloud Storage Integration

## Context

This design describes how to complete the wiring of the existing `internal/io/filesystem/` and `internal/secret/` packages into the query execution layer so that SQL statements can transparently read from and write to cloud storage.

The filesystem implementations (S3FileSystem, GCSFileSystem, AzureFileSystem, HTTPFileSystem) and the secret manager are already complete. Crucially, the glue layer **already partially exists** in `internal/executor/copy_cloud.go`:

- **`FileSystemProvider`** -- combines `FileSystemFactory` + `secret.Manager` to resolve secrets and create filesystems for cloud URLs. It already implements `applyS3Secret()`, `applyGCSSecret()`, `applyAzureSecret()`, and `applyHTTPSecret()`.
- **`GlobMatcher`** (`glob_cloud.go`) -- expands glob patterns on cloud filesystems using `FileSystemProvider`.
- **`ContextFileSystem`** (`filesystem.go`) -- interface already implemented by S3, GCS, Azure, and HTTP filesystems for context-aware `OpenContext()`, `CreateContext()`, `StatContext()`, and `ReadDirContext()`.

What remains is:

1. Promoting `FileSystemProvider` from ad-hoc per-call construction to a shared, connection-scoped resource
2. Wiring it through the executor so table functions and COPY handlers use it consistently
3. Adding convenience methods (OpenRead, OpenWrite, Glob, Stat) as a thin facade on top of `FileSystemProvider`

**Stakeholders**:
- Data engineers querying cloud data lakes from dukdb-go
- Application developers building ETL pipelines with cloud storage
- Teams migrating from DuckDB who depend on httpfs extension behavior
- Serverless/WASM deployments needing cloud-native storage access

**Constraints**:
- Pure Go (no CGO) -- already satisfied by existing filesystem implementations
- Must match DuckDB URL syntax and secret semantics
- Must not break existing local file operations
- FileSystemProvider must be connection-scoped (different connections can have different secrets)

## Goals / Non-Goals

**Goals**:
1. Promote `FileSystemProvider` to a connection-scoped resource instead of recreating it per call
2. Enable table functions (read_parquet, read_csv, read_json) to accept cloud URLs
3. Enable COPY FROM/TO with cloud URLs
4. Support glob pattern expansion on cloud storage
5. Provide a single VFS entry point that executors use for all file I/O

**Non-Goals**:
1. Adding new filesystem implementations (S3, GCS, Azure, HTTP are done)
2. Adding new secret types or providers (done in internal/secret/)
3. Server-side filtering (S3 Select) -- future work
4. Streaming/CDC from cloud -- future work
5. Cloud-backed database persistence (database files must remain local)

## Decisions

### Decision 1: Extend FileSystemProvider with VFS Facade Methods

**Options**:
A. Create a new VirtualFileSystem type parallel to FileSystemProvider
B. Extend FileSystemProvider with convenience methods (OpenRead, OpenWrite, Glob, Stat)
C. Pass raw FileSystemProvider to all callers and let them handle open/close

**Choice**: B -- Extend FileSystemProvider with facade methods

**Rationale**:
- `FileSystemProvider` already has all the core logic: `GetFileSystem()`, `applyS3Secret()`, `applyGCSSecret()`, `applyAzureSecret()`, `applyHTTPSecret()`, `openFileWithStat()`, `createFileForWriting()`
- Creating a parallel VirtualFileSystem would duplicate scheme routing and secret resolution that already works
- Adding facade methods is minimal code that wraps existing tested functionality
- The existing `ContextFileSystem` interface is already used in `openFileWithStat()` and `createFileForWriting()` for context propagation -- no custom context handling needed

The facade methods delegate to existing infrastructure:

```go
// internal/executor/copy_cloud.go (additions to existing FileSystemProvider)

// OpenRead opens a file for reading from any supported scheme.
// Delegates to openFileWithStat which already handles ContextFileSystem.
func (p *FileSystemProvider) OpenRead(ctx context.Context, url string) (filesystem.File, error) {
    return p.openFileWithStat(ctx, url)
}

// OpenWrite opens a file for writing to any supported scheme.
// Delegates to createFileForWriting which already handles ContextFileSystem.
func (p *FileSystemProvider) OpenWrite(ctx context.Context, url string) (io.WriteCloser, error) {
    return p.createFileForWriting(ctx, url)
}

// Glob expands a glob pattern on any supported filesystem.
// Delegates to the existing GlobMatcher.
func (p *FileSystemProvider) Glob(ctx context.Context, pattern string) ([]string, error) {
    matcher := NewGlobMatcher(p)
    return matcher.ExpandGlob(ctx, pattern)
}

// Stat returns file info for a URL on any supported filesystem.
func (p *FileSystemProvider) Stat(ctx context.Context, url string) (filesystem.FileInfo, error) {
    fs, err := p.GetFileSystem(ctx, url)
    if err != nil {
        return nil, err
    }
    if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
        return ctxFS.StatContext(ctx, url)
    }
    return fs.Stat(url)
}
```

Note how context propagation is handled: `openFileWithStat()` and `createFileForWriting()` already check for `filesystem.ContextFileSystem` and call `OpenContext()`/`CreateContext()` when available. No custom context handling is needed because the existing code already does this correctly.

### Decision 2: How Table Functions Use FileSystemProvider

**Options**:
A. Table functions call FileSystemProvider directly (current pattern)
B. Table functions receive a `filesystem.File` from the executor
C. Table functions receive an `io.Reader`/`io.Writer` from the executor

**Choice**: A -- Keep current pattern, but use shared provider

**Rationale**:
- Table functions already use `NewFileSystemProvider(e.getSecretManager())` pattern (see `table_function_parquet.go:206`, `table_function_csv.go:317`)
- The existing `createFileReaderFromFS()` and `createFileWriterFromFS()` helpers already handle all format-specific logic
- The change is to use a shared `FileSystemProvider` from the executor instead of creating a new one per call

Current (ad-hoc creation):
```go
// In table_function_parquet.go (CURRENT)
provider := NewFileSystemProvider(e.getSecretManager())
reader, err := createFileReaderFromFS(ctx, provider, path, fileio.FormatParquet, options)
```

After (shared provider from executor):
```go
// In table_function_parquet.go (AFTER)
provider := e.getFileSystemProvider()
reader, err := createFileReaderFromFS(ctx, provider, path, fileio.FormatParquet, options)
```

### Decision 3: How Secrets Integrate with Cloud Filesystems

**Options**:
A. Secrets are resolved once at connection open and cached
B. Secrets are resolved per-file-open (lazy resolution)
C. Secrets are resolved per-statement

**Choice**: B -- Secrets are resolved per-file-open (lazy resolution)

**Rationale**:
- This is already how `FileSystemProvider.GetFileSystem()` works -- it calls `p.secretManager.GetSecret()` on every `GetFileSystem()` call
- Secrets can be created/dropped/altered during a session; lazy resolution reflects the latest state
- DuckDB resolves secrets at file-open time using longest-prefix match
- Temporary credentials (IAM roles) may expire; re-resolving ensures fresh credentials

The resolution flow (using existing code paths):

```
SQL: SELECT * FROM read_parquet('s3://my-bucket/data/*.parquet')
  |
  v
Executor: calls provider.Glob(ctx, "s3://my-bucket/data/*.parquet")
  |
  v
GlobMatcher.expandCloudGlob():
  -> provider.GetFileSystem(ctx, "s3://my-bucket/data/")
     -> url.Parse() to get scheme
     -> parsedURL.IsCloudScheme() -> true
     -> getSecretTypeForScheme("s3") -> SecretTypeS3
     -> secretManager.GetSecret(ctx, url, SecretTypeS3)
        -> finds secret with longest prefix match
     -> applyS3Secret(ctx, secret, parsedURL)
        -> builds S3Config from secret options
        -> NewS3FileSystem(ctx, config)
  |
  v
S3FileSystem.Glob("s3://my-bucket/data/*.parquet")
  -> ["s3://my-bucket/data/part1.parquet", "s3://my-bucket/data/part2.parquet"]
  |
  v
For each file: provider.openFileWithStat(ctx, path)
  -> provider.GetFileSystem(ctx, path) (secret resolved again -- fresh creds)
  -> ctxFS.OpenContext(ctx, path) if ContextFileSystem, else fs.Open(path)
  -> S3File (implements filesystem.File)
  -> Passed to parquet reader
```

### Decision 4: How COPY Statement Uses FileSystemProvider

**Options**:
A. COPY handler opens files directly using os.Open/os.Create
B. COPY handler uses FileSystemProvider for all file operations

**Choice**: B -- COPY handler uses FileSystemProvider

**Rationale**:
- The COPY executor in `physical_copy.go` already uses `FileSystemProvider` for cloud URLs (see `createCloudFileReader` at line 720 and `createCloudFileWriter` at line 733)
- The change is to use the shared provider and extend to local files too
- `createFileReaderFromFS()` and `createFileWriterFromFS()` already handle format dispatch

```go
// In physical_copy.go (CURRENT -- already exists)
func (e *Executor) createCloudFileReader(ctx context.Context, path string, format fileio.Format, options map[string]any) (fileio.FileReader, error) {
    provider := NewFileSystemProvider(e.getSecretManager())
    return createFileReaderFromFS(ctx, provider, path, format, options)
}

// AFTER -- use shared provider
func (e *Executor) createCloudFileReader(ctx context.Context, path string, format fileio.Format, options map[string]any) (fileio.FileReader, error) {
    provider := e.getFileSystemProvider()
    return createFileReaderFromFS(ctx, provider, path, format, options)
}
```

### Decision 5: URL Scheme Routing

The `FileSystemFactory` already registers schemes. `FileSystemProvider.GetFileSystem()` adds secret resolution on top via `getSecretTypeForScheme()`. The mapping already exists in `copy_cloud.go`:

| URL Scheme | FileSystem | Secret Type | Existing Code |
|------------|-----------|-------------|---------------|
| (none), `file://` | LocalFileSystem | (none) | factory.GetFileSystem() |
| `s3://`, `s3a://`, `s3n://` | S3FileSystem | S3 | `applyS3Secret()` |
| `gs://`, `gcs://` | GCSFileSystem | GCS | `applyGCSSecret()` |
| `azure://`, `az://` | AzureFileSystem | AZURE | `applyAzureSecret()` |
| `http://`, `https://` | HTTPFileSystem | HTTP | `applyHTTPSecret()` |
| `hf://`, `huggingface://` | (future) | HuggingFace | `getSecretTypeForScheme()` |

### Decision 6: Connection-Scoped FileSystemProvider Initialization

**Choice**: FileSystemProvider is created per-executor, initialized from the engine's secret manager

Currently, every call site does `NewFileSystemProvider(e.getSecretManager())`. The change makes the provider a field on the Executor struct instead.

```go
// internal/executor/executor.go (changes)

type Executor struct {
    // ... existing fields ...
    fsProvider *FileSystemProvider // connection-scoped filesystem provider
}

// getFileSystemProvider returns the shared FileSystemProvider for this executor.
// Lazily initialized on first use to avoid overhead for queries that don't touch files.
func (e *Executor) getFileSystemProvider() *FileSystemProvider {
    if e.fsProvider == nil {
        e.fsProvider = NewFileSystemProvider(e.getSecretManager())
    }
    return e.fsProvider
}
```

Note: The provider is on the `Executor`, not on `engine.Conn`. This is because:
1. The executor already has access to the secret manager via `e.getSecretManager()`
2. `engine.Conn` does not currently have a `VFS()` method and adding one would require threading it through multiple layers
3. The `Executor` is the right scope -- it's created per-query and has the right lifetime
4. Lazy initialization avoids overhead for queries that don't touch files

### Decision 7: Context Propagation via ContextFileSystem

**Choice**: Use the existing `ContextFileSystem` interface -- no custom context handling

The existing code in `copy_cloud.go` already handles context propagation correctly:

```go
// In openFileWithStat() -- ALREADY EXISTS
if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
    file, err := ctxFS.OpenContext(ctx, rawURL)
    // ...
}

// In createFileForWriting() -- ALREADY EXISTS
if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
    file, err := ctxFS.CreateContext(ctx, rawURL)
    // ...
}
```

All cloud filesystem implementations (S3FileSystem, GCSFileSystem, AzureFileSystem, HTTPFileSystem) already implement `ContextFileSystem`. The `ContextFileSystem` interface provides:
- `OpenContext(ctx, path)` -- context-aware file open for read
- `CreateContext(ctx, path)` -- context-aware file create for write
- `StatContext(ctx, path)` -- context-aware stat
- `ReadDirContext(ctx, path)` -- context-aware directory listing

The `GlobMatcher` in `glob_cloud.go` also already uses `ContextFileSystem`:
```go
// In expandSimpleGlob() -- ALREADY EXISTS
if ctxFS, ok := fs.(filesystem.ContextFileSystem); ok {
    entries, err = ctxFS.ReadDirContext(ctx, basePath)
}
```

No additional context plumbing is required.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Credential caching vs freshness | Medium | Already resolved per-file-open in FileSystemProvider |
| Large cloud file reads in memory | High | Use streaming reads; S3File streams via minio.Object |
| Multipart upload failure mid-write | Medium | S3 multipart abort on error; Azure block blob atomic |
| Rate limiting on cloud APIs | Medium | Existing RetryConfig in each filesystem handles backoff |
| WASM: no TCP sockets | High | HTTP-based storage (S3, GCS, Azure) works over fetch API in WASM |

## Performance Considerations

1. **FileSystemProvider Reuse**: Provider is now shared per-executor instead of recreated per-call. Each cloud FileSystem instance is still created per-file-open (per-secret) because credentials may differ.
2. **Connection Pooling**: Each filesystem implementation uses HTTP connection pooling internally (minio-go, GCS client, Azure client, net/http)
3. **Range Requests**: Parquet reader benefits from `ReadAt` for reading column chunks without downloading entire files
4. **Concurrent Reads**: S3FileSystem supports `ConcurrentReader` for parallel range requests on large files
5. **Multipart Uploads**: S3FileSystem supports `MultipartWriter` for streaming large COPY TO operations

## Migration Plan

### Phase 1: Shared FileSystemProvider (this proposal)
1. Add `OpenRead`, `OpenWrite`, `Glob`, `Stat` facade methods to `FileSystemProvider`
2. Add `fsProvider` field to `Executor` with lazy initialization via `getFileSystemProvider()`
3. Update COPY executor to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`
4. Update table functions to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`

### Phase 2: Integration Testing
1. Unit tests for FileSystemProvider facade methods
2. Integration tests with LocalStack (S3), Azurite (Azure)
3. End-to-end SQL tests: `SELECT * FROM read_parquet('s3://...')`

### Phase 3: Advanced Features
1. Hive-partitioned reads from cloud storage
2. Parallel multi-file reads with partition pruning
3. Schema inference for cloud files

## Open Questions

1. **Filesystem Instance Caching**: Should FileSystemProvider cache filesystem instances across file-open calls for the same secret?
   - Current decision: No caching initially; add if profiling shows overhead

2. **Public Bucket Access**: Should FileSystemProvider attempt anonymous access if no secret matches?
   - Current decision: Yes, already implemented -- falls through to default factory (no credentials)

3. **Custom Endpoint in URL**: Should `s3://bucket/key?endpoint=http://localhost:9000` override secrets?
   - Current decision: No, endpoints come from secrets only; query params are for data (like hive partitioning)

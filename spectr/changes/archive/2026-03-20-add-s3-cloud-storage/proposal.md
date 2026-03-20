# Change: Add S3/Cloud Storage Integration

## Why

dukdb-go has a fully implemented `internal/io/filesystem/` package with S3, GCS, Azure, and HTTP filesystem implementations, along with a mature `internal/secret/` package for credential management. However, these filesystem backends are not yet wired into the query execution layer. Users cannot execute cloud-aware SQL statements like:

```sql
SELECT * FROM read_parquet('s3://bucket/data.parquet');
COPY table TO 's3://bucket/output.csv';
SELECT * FROM read_csv('https://example.com/data.csv');
```

The gap between what exists (filesystem + secret infrastructure) and what is needed (end-to-end cloud SQL support) creates the following problems:

1. **No Cloud Querying**: Table functions (`read_parquet`, `read_csv`, `read_json`) only accept local file paths
2. **No Cloud Export**: COPY TO statements cannot write to S3, GCS, or Azure destinations
3. **No Secret-to-Filesystem Binding**: Secrets exist in memory but are not consulted when opening cloud files
4. **No Glob on Cloud**: Glob patterns like `s3://bucket/data/*.parquet` are not expanded in SQL contexts
5. **Competitive Gap**: DuckDB v1.4.3 supports all of the above via its httpfs extension

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

- **Secret-Aware FileSystem Factory**: The `FileSystemFactory` is extended to accept a `secret.Manager`, automatically resolving credentials for cloud URLs before opening files
- **Cloud-Aware Table Functions**: `read_parquet`, `read_csv`, `read_json`, `read_ndjson` accept `s3://`, `gs://`, `az://`, `http://`, and `https://` URLs
- **Cloud-Aware COPY Statement**: `COPY ... FROM/TO 's3://...'` routes through the `FileSystemProvider` with secret resolution
- **Cloud Glob Expansion**: Glob patterns on S3, GCS, and Azure are expanded using the existing `Glob()` method on each filesystem implementation
- **Hive-Partitioned Cloud Reads**: Support for `read_parquet('s3://bucket/data/**/*.parquet', hive_partitioning=true)`

### Internal Changes

- Extended `internal/executor/copy_cloud.go`: Add `OpenRead`, `OpenWrite`, `Glob`, `Stat` facade methods to the existing `FileSystemProvider` (which already handles secret resolution via `applyS3Secret`, `applyGCSSecret`, `applyAzureSecret`, `applyHTTPSecret` and context propagation via `ContextFileSystem`)
- Modified `internal/executor/executor.go` (or equivalent): Add `fsProvider` field with lazy initialization, replacing ad-hoc `NewFileSystemProvider()` calls
- Modified `internal/executor/physical_copy.go`: Uses shared `e.getFileSystemProvider()` instead of per-call `NewFileSystemProvider()`
- Modified `internal/executor/table_function_*.go`: Table functions use shared `e.getFileSystemProvider()` instead of per-call `NewFileSystemProvider()`
- No changes needed to `internal/io/csv/reader.go`, `internal/io/json/reader.go`, `internal/io/parquet/reader.go` -- they already work with `filesystem.File` via `createFileReaderFromFS()`

## Impact

### Affected Specs

- `cloud-storage`: FileSystemProvider integration, secret-to-filesystem binding, glob expansion
- `copy-statement`: Cloud URL support in COPY FROM/TO
- `file-io`: FileSystem interface used in read/write paths
- `table-udf`: Table functions support cloud URLs

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/executor/copy_cloud.go` | MODIFIED | Add `OpenRead`, `OpenWrite`, `Glob`, `Stat` facade methods to existing `FileSystemProvider` |
| `internal/executor/copy_cloud_test.go` | MODIFIED | Tests for facade methods |
| `internal/executor/executor.go` | MODIFIED | Add `fsProvider` field and `getFileSystemProvider()` lazy initializer |
| `internal/executor/physical_copy.go` | MODIFIED | Use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()` |
| `internal/executor/table_function_parquet.go` | MODIFIED | Use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()` |
| `internal/executor/table_function_csv.go` | MODIFIED | Use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()` |
| `internal/executor/table_function_json.go` | MODIFIED | Use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()` |
| `internal/executor/table_function_arrow.go` | MODIFIED | Use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()` |
| `internal/executor/table_function_xlsx.go` | MODIFIED | Use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()` |

### Dependencies

- This proposal depends on: existing `internal/io/filesystem/` package (already complete)
- This proposal depends on: existing `internal/secret/` package (already complete)
- This proposal blocks: Iceberg table format support (needs cloud file access)

### Compatibility

- Full DuckDB SQL syntax compatibility for cloud URLs
- Supports DuckDB secret syntax (`CREATE SECRET ... TYPE S3`)
- S3, GCS, Azure, and HTTP/HTTPS URL schemes match DuckDB behavior
- S3-compatible stores (MinIO, Cloudflare R2) work via custom endpoint configuration

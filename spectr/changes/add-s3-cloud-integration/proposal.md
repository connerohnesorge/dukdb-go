# Change: Add S3 and Cloud Integration

## Why

Currently, dukdb-go is limited to local file access only. DuckDB v1.4.3 has mature cloud integration via HTTPFS and AWS extensions, enabling direct querying of S3, GCS, and Azure storage using `s3://` URLs. Without cloud support:

1. **Data Access Limitations**: Users cannot query data directly from cloud storage
2. **ETL Pipeline Complexity**: Requires pre-downloading data to local files
3. **Cloud-Native Workflows**: Cannot integrate with data lakes on S3/GCS/Azure
4. **Competitive Gap**: Official DuckDB and go-duckdb both support cloud storage
5. **WASM Compatibility**: Cloud support is critical for serverless/WASM deployments

The current implementation in `internal/persistence/file.go` uses `os.File` directly with no abstraction for remote filesystems, and there is no secrets management system.

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

- **FileSystem Interface Abstraction**: Pluggable filesystem layer supporting local, S3, GCS, Azure, and HTTP
- **Secrets Manager**: SQL interface for `CREATE SECRET`, `DROP SECRET`, `ALTER SECRET` with support for S3, HTTP, AZURE, GCS, HUGGINGFACE
- **S3 FileSystem**: Full S3 support using AWS SDK v2 with IAM, access keys, and AWS CLI config
- **GCS Filesystem**: Google Cloud Storage support using official Go SDK
- **Azure Filesystem**: Azure Blob Storage support using Azure SDK
- **HTTP/HTTPS Support**: Basic HTTP range requests for HTTPFS-style access
- **URL Parsing Integration**: COPY statement and table functions support cloud URLs

### Internal Changes

- New `internal/io/filesystem/` package with `FileSystem` interface and implementations
- New `internal/secret/` package with secrets manager and SQL integration
- Updated `internal/persistence/` to use abstracted filesystem
- New `internal/io/url/` package for multi-protocol URL parsing

## Impact

### Affected Specs

- `file-io`: New FileSystem interface, cloud URL support
- `copy-statement`: Cloud URL parsing for COPY FROM/TO
- `replacement-scan`: Cloud URL support in table functions
- `security`: New secret manager capability

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/io/filesystem/filesystem.go` | ADDED | FileSystem interface and base types |
| `internal/io/filesystem/local.go` | ADDED | Local filesystem implementation |
| `internal/io/filesystem/s3.go` | ADDED | S3 filesystem implementation |
| `internal/io/filesystem/gcs.go` | ADDED | GCS filesystem implementation |
| `internal/io/filesystem/azure.go` | ADDED | Azure filesystem implementation |
| `internal/io/filesystem/http.go` | ADDED | HTTP/HTTPS filesystem implementation |
| `internal/io/url/parser.go` | ADDED | Multi-protocol URL parsing |
| `internal/secret/manager.go` | ADDED | Secrets manager core |
| `internal/secret/provider.go` | ADDED | Secret provider implementations |
| `internal/secret/binder.go` | ADDED | SQL binding for secret statements |
| `internal/persistence/file.go` | MODIFIED | Use abstracted filesystem |
| `internal/io/csv/reader.go` | MODIFIED | Support FileSystem interface |
| `internal/io/json/reader.go` | MODIFIED | Support FileSystem interface |
| `internal/io/parquet/reader.go` | MODIFIED | Support FileSystem interface |
| `internal/io/parquet/writer.go` | MODIFIED | Support FileSystem interface |

### Dependencies

- This proposal depends on: `add-duckdb-file-format-compat` (FileSystem abstraction for persistence)
- This proposal blocks: (none)

### Compatibility

- Full compatibility with DuckDB cloud URLs and secrets syntax
- Supports reading and writing to S3, GCS, Azure, and HTTPFS endpoints
- Compatible with DuckDB secrets (same syntax, interchangeable storage)

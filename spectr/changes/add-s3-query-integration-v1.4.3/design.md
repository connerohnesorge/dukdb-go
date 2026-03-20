## Implementation Details

### Credential Flow: CREATE SECRET to FileSystem

The credential flow from SQL to filesystem is already implemented but needs hardening:

```
CREATE SECRET my_s3 (TYPE S3, KEY_ID '...', SECRET '...', REGION 'us-east-1')
    |
    v
secret.Manager.CreateSecret() -- internal/secret/manager.go
    |
    v
executor.FileSystemProvider.GetFileSystem(ctx, url) -- internal/executor/copy_cloud.go:37
    |
    v
FileSystemProvider.applyS3Secret(ctx, secret, parsedURL) -- internal/executor/copy_cloud.go:115
    |  Extracts via secret.GetOption(): key_id, secret, session_token, region, endpoint, url_style, use_ssl
    v
filesystem.NewS3FileSystem(ctx, s3Config) -- internal/io/filesystem/s3_filesystem.go
    |
    v
Cloud operations via S3 API
```

### Table Function Dispatch

All table functions route through `executeTableFunctionScan()` in `internal/executor/table_function_csv.go` (line 73). Each resolve function checks `filesystem.IsCloudURL()` and creates a `NewFileSystemProvider(e.getSecretManager())` for cloud path resolution:

- `internal/executor/table_function_parquet.go:executeReadParquet()` -- line 59, calls `resolveParquetFilePaths()` (line 193) which checks cloud URL
- `internal/executor/table_function_csv.go:executeReadCSV()` -- dispatched at line 79, calls `resolveFilePaths()` (line 347) which checks cloud URL
- `internal/executor/table_function_json.go:executeReadJSON()` -- line 59, calls `resolveJSONFilePaths()` (line 215) which checks cloud URL

### COPY Statement Cloud Support

- `internal/executor/physical_copy.go:executeCopyFrom()` (line 25) -- checks `IsCloudURL()` and uses `createCloudFileReader()` (line 721)
- `internal/executor/physical_copy.go:executeCopyTo()` (line 123) -- checks `IsCloudURL()` and uses `createFileWriterFromFS()`
- `internal/executor/copy_cloud.go:createFileReaderFromFS()` (line 303) -- opens cloud files for reading
- `internal/executor/copy_cloud.go:createFileWriterFromFS()` (line 377) -- opens cloud files for writing

### Cloud URL Scheme Registration

`internal/io/filesystem/factory.go:registerS3SchemesInternal()` registers schemes: `s3`, `s3a`, `s3n`
Other schemes: `gs`, `gcs` (GCS), `azure`, `az` (Azure), `http`, `https` (HTTP)

`filesystem.IsCloudURL()` at `internal/io/filesystem/factory.go` checks URL scheme against these registered schemes.

### Key Gap: Default Config vs Secret-Enriched Config

The `FileSystemFactory` in `factory.go` creates filesystems with `DefaultS3Config()` (no credentials). The `FileSystemProvider` in `copy_cloud.go` bridges this gap by looking up secrets and applying them before creating the filesystem. All table functions and COPY operations already use the provider path.

Remaining gaps to address:
1. Error messages when no matching secret exists for a cloud URL should be clear and actionable
2. Auto-detect variants (read_csv_auto, read_json_auto) need verification that they properly forward to cloud-aware paths
3. read_ndjson needs verification of cloud URL support
4. Glob expansion on cloud URLs needs integration testing
5. COPY TO for all formats (CSV, JSON, Parquet) to cloud URLs needs integration testing

### Error Handling Pattern

All cloud errors MUST use the `dukdb.Error` type with the `Msg:` field (not `Message:`):

```go
return dukdb.Error{Msg: "failed to read from S3: no matching secret for URL"}
```

## Context

dukdb-go is a pure Go DuckDB implementation. The cloud filesystem layer was built in `internal/io/filesystem/` with S3, GCS, Azure support. The secret manager in `internal/secret/` handles credential storage. The `FileSystemProvider` in `internal/executor/copy_cloud.go` bridges these systems. This change proposal covers the remaining integration, testing, and hardening work.

## Goals / Non-Goals

- Goals:
  - Production-ready S3 query integration with proper error handling
  - Integration tests proving end-to-end credential flow from CREATE SECRET to query execution
  - All table function variants (read_csv, read_csv_auto, read_json, read_json_auto, read_ndjson, read_parquet) working with cloud URLs
  - COPY FROM/TO working with cloud URLs for all supported formats
  - Clear error messages when credentials are missing or invalid

- Non-Goals:
  - New cloud provider implementations (S3, GCS, Azure already exist)
  - Changes to the secret management SQL syntax
  - Performance optimization of cloud file transfers
  - Streaming/chunked reads for very large cloud files (future work)

## Decisions

- Decision: Use LocalStack for S3 integration tests
  - Rationale: Already used in `internal/io/filesystem/` tests per the filesystem spec; consistent with existing test infrastructure
  - Alternatives: Moto (Python), MinIO -- LocalStack is already established in the project

- Decision: Focus on S3 integration first, extend patterns to GCS/Azure
  - Rationale: S3 is the most common cloud storage target; GCS/Azure follow same patterns via FileSystemProvider
  - Alternatives: Test all providers simultaneously -- more complex, S3 covers the critical path

## Risks / Trade-offs

- Risk: LocalStack may not perfectly replicate S3 behavior for edge cases (multipart upload, eventual consistency)
  - Mitigation: Document known LocalStack limitations; supplement with manual testing against real S3 when possible

- Risk: Cloud integration tests increase CI time
  - Mitigation: Gate cloud tests behind build tag (`//go:build integration`) so they run only when explicitly requested

## Open Questions

- Should cloud integration tests be part of the default `nix develop -c tests` run or gated behind a flag?
- What is the desired behavior when a cloud URL is used but no matching secret exists -- error immediately or attempt anonymous access?

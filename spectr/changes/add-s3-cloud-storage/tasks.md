# Tasks: S3/Cloud Storage Integration

## 1. FileSystemProvider Facade Methods

- [ ] 1.1 Add `OpenRead(ctx, url)` method to `FileSystemProvider` in `copy_cloud.go` (delegates to existing `openFileWithStat`)
- [ ] 1.2 Add `OpenWrite(ctx, url)` method to `FileSystemProvider` in `copy_cloud.go` (delegates to existing `createFileForWriting`)
- [ ] 1.3 Add `Glob(ctx, pattern)` method to `FileSystemProvider` in `copy_cloud.go` (delegates to existing `GlobMatcher`)
- [ ] 1.4 Add `Stat(ctx, url)` method to `FileSystemProvider` in `copy_cloud.go` (uses existing `ContextFileSystem` for context propagation)
- [ ] 1.5 Add unit tests for facade methods in `copy_cloud_test.go`
- [ ] 1.6 Test OpenRead delegates correctly for local files
- [ ] 1.7 Test OpenRead delegates correctly for cloud URLs (mocked)
- [ ] 1.8 Test Glob delegates to GlobMatcher correctly
- [ ] 1.9 Test Stat uses ContextFileSystem.StatContext when available

## 2. Executor-Scoped FileSystemProvider

- [ ] 2.1 Add `fsProvider *FileSystemProvider` field to `Executor` struct
- [ ] 2.2 Implement `getFileSystemProvider()` with lazy initialization (calls `NewFileSystemProvider(e.getSecretManager())` on first use)
- [ ] 2.3 Update all `NewFileSystemProvider(e.getSecretManager())` call sites in table functions to use `e.getFileSystemProvider()`
- [ ] 2.4 Update all `NewFileSystemProvider(e.getSecretManager())` call sites in COPY executor to use `e.getFileSystemProvider()`
- [ ] 2.5 Add test verifying `getFileSystemProvider()` returns same instance on repeated calls

## 3. COPY Statement Cloud Integration

- [ ] 3.1 Update `createCloudFileReader` in `physical_copy.go` to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`
- [ ] 3.2 Update `createCloudFileWriter` in `physical_copy.go` to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`
- [ ] 3.3 Verify glob pattern expansion works for COPY FROM with cloud URLs (uses existing GlobMatcher via FileSystemProvider.Glob)
- [ ] 3.4 Handle multipart upload for large COPY TO to S3 (already supported by S3File.EnableMultipartStreaming)
- [ ] 3.5 Add integration test: COPY TO 's3://bucket/output.parquet' (with LocalStack)
- [ ] 3.6 Add integration test: COPY FROM 's3://bucket/input.csv' (with LocalStack)
- [ ] 3.7 Add integration test: COPY TO 'az://container/output.csv' (with Azurite)
- [ ] 3.8 Add integration test: COPY FROM 'https://example.com/data.csv' (with test HTTP server)

## 4. Table Function Cloud Integration

- [ ] 4.1 Update `read_parquet` table function to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`
- [ ] 4.2 Update `read_csv` / `read_csv_auto` table functions to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`
- [ ] 4.3 Update `read_json` / `read_json_auto` / `read_ndjson` table functions to use `e.getFileSystemProvider()` instead of `NewFileSystemProvider()`
- [ ] 4.4 Update `read_xlsx` table function to use `e.getFileSystemProvider()` (if applicable)
- [ ] 4.5 Update `read_arrow` table function to use `e.getFileSystemProvider()` (if applicable)
- [ ] 4.6 Verify glob expansion in table functions for patterns like `s3://bucket/data/*.parquet` (uses existing GlobMatcher)
- [ ] 4.7 Verify `filesystem.File` satisfies `io.ReaderAt` for Parquet random access (already verified by interface check in filesystem.go)
- [ ] 4.8 Verify `filesystem.File` satisfies `io.ReadSeeker` for CSV/JSON streaming (already verified by interface check in filesystem.go)
- [ ] 4.9 Add integration test: `SELECT * FROM read_parquet('s3://bucket/data.parquet')`
- [ ] 4.10 Add integration test: `SELECT * FROM read_csv('https://example.com/data.csv')`
- [ ] 4.11 Add integration test: `SELECT * FROM read_parquet('s3://bucket/data/*.parquet')` (glob)

## 5. I/O Reader/Writer Abstraction Verification

- [ ] 5.1 Verify `internal/io/csv/reader.go` works with `filesystem.File` (implements `io.Reader`) -- already works via `createFileReaderFromFS`
- [ ] 5.2 Verify `internal/io/json/reader.go` works with `filesystem.File` (implements `io.Reader`) -- already works via `createFileReaderFromFS`
- [ ] 5.3 Verify `internal/io/parquet/reader.go` works with `filesystem.File` (`io.ReaderAt` + `io.Seeker`) -- already works via `createFileReaderFromFS`
- [ ] 5.4 Verify `internal/io/parquet/writer.go` works with `filesystem.File` (implements `io.Writer`) -- already works via `createFileWriterFromFS`
- [ ] 5.5 Confirm no code paths use `*os.File` directly that should use `filesystem.File` instead
- [ ] 5.6 Add test: read Parquet via S3File using range requests (ReadAt)
- [ ] 5.7 Add test: read CSV via HTTPFile using streaming read

## 6. Secret Resolution End-to-End Tests

- [ ] 6.1 Test: CREATE SECRET then read_parquet('s3://...') uses that secret (uses existing `applyS3Secret`)
- [ ] 6.2 Test: Multiple secrets with different scopes; most specific wins (uses existing `secretManager.GetSecret` longest-prefix match)
- [ ] 6.3 Test: DROP SECRET then read_parquet('s3://...') falls back to anonymous
- [ ] 6.4 Test: ALTER SECRET updates credentials used by subsequent reads
- [ ] 6.5 Test: Secret with SCOPE 's3://bucket/prefix/' matches URLs under that prefix
- [ ] 6.6 Test: GCS secret with service account JSON (uses existing `applyGCSSecret`)
- [ ] 6.7 Test: Azure secret with account name + key (uses existing `applyAzureSecret`)
- [ ] 6.8 Test: HTTP secret with bearer token (uses existing `applyHTTPSecret`)

## 7. Cloud Glob Pattern Tests

- [ ] 7.1 Test: `s3://bucket/data/*.parquet` expands to matching S3 objects (uses existing GlobMatcher + S3FileSystem.Glob)
- [ ] 7.2 Test: `gs://bucket/data/**/*.json` expands recursively on GCS (uses existing GlobMatcher + GCSFileSystem.Glob)
- [ ] 7.3 Test: `azure://container/data/[0-9]*.csv` character class matching on Azure (uses existing GlobMatcher + AzureFileSystem.Glob)
- [ ] 7.4 Test: Glob on HTTP returns error (not supported) (already: HTTPFileSystem.Glob returns ErrNotSupported)
- [ ] 7.5 Test: Glob with no matches returns appropriate error

## 8. Compatibility Testing

- [ ] 8.1 Test DuckDB SQL syntax compatibility for S3 URLs
- [ ] 8.2 Test DuckDB SQL syntax compatibility for GCS URLs
- [ ] 8.3 Test DuckDB SQL syntax compatibility for Azure URLs
- [ ] 8.4 Test DuckDB SQL syntax compatibility for HTTP URLs
- [ ] 8.5 Test with LocalStack for S3 end-to-end
- [ ] 8.6 Test with Azurite for Azure end-to-end
- [ ] 8.7 Test with fake-gcs-server for GCS end-to-end

## 9. Error Handling and Edge Cases

- [ ] 9.1 Test: Cloud URL with invalid credentials returns clear error
- [ ] 9.2 Test: Non-existent S3 bucket returns appropriate error
- [ ] 9.3 Test: HTTP 404 returns appropriate error
- [ ] 9.4 Test: Network timeout handled by existing RetryConfig in each filesystem
- [ ] 9.5 Test: Empty file on cloud storage
- [ ] 9.6 Test: Very large file (>5GB) uses multipart for S3 write (S3File already supports via `EnableMultipartStreaming`)

## 10. Documentation and Examples

- [ ] 10.1 Add example: `examples/cloud-s3-read/main.go` showing S3 read workflow
- [ ] 10.2 Add example: `examples/cloud-s3-write/main.go` showing S3 write workflow
- [ ] 10.3 Add example: `examples/cloud-http-read/main.go` showing HTTP read workflow
- [ ] 10.4 Document supported URL schemes and secret types
- [ ] 10.5 Document credential provider chain (CONFIG > ENV > shared config > IMDS)

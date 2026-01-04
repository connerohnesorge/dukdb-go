# Tasks: S3 and Cloud Integration

## 1. FileSystem Interface Foundation

- [ ] 1.1 Create `internal/io/filesystem/filesystem.go` with FileSystem interface
- [ ] 1.2 Define FileInfo, File, DirEntry interfaces
- [ ] 1.3 Define FileSystemCapabilities struct
- [ ] 1.4 Create `internal/io/filesystem/local.go` with local filesystem implementation
- [ ] 1.5 Implement Open, Create, Stat, ReadDir for local filesystem
- [ ] 1.6 Create `internal/io/filesystem/factory.go` with FileSystemFactory
- [ ] 1.7 Register file:// scheme mapping to local filesystem
- [ ] 1.8 Add tests for FileSystem interface with local implementation
- [ ] 1.9 Update `internal/persistence/file.go` to use FileSystem interface

## 2. URL Parser

- [ ] 2.1 Create `internal/io/url/parser.go`
- [ ] 2.2 Implement Parse function for cloud URLs (s3://, gs://, azure://, http://)
- [ ] 2.3 Handle authority/path/query parsing
- [ ] 2.4 Handle virtual-host vs path style for S3
- [ ] 2.5 Add tests for URL parsing with various formats
- [ ] 2.6 Add URL String method for round-trip parsing

## 3. Secrets Manager Core

- [ ] 3.1 Create `internal/secret/manager.go` with Manager interface
- [ ] 3.2 Define Secret, SecretType, SecretScope, SecretOptions types
- [ ] 3.3 Implement CreateSecret, DropSecret, AlterSecret methods
- [ ] 3.4 Implement GetSecret with path-based scope matching
- [ ] 3.5 Implement ListSecrets method
- [ ] 3.6 Create `internal/secret/catalog.go` for secret persistence
- [ ] 3.7 Implement catalog storage using DuckDB catalog
- [ ] 3.8 Add tests for secret manager operations

## 4. Secret SQL Integration

- [ ] 4.1 Create `internal/secret/binder.go` for SQL binding
- [ ] 4.2 Implement BindCreateSecret for CREATE SECRET statements
- [ ] 4.3 Implement BindDropSecret for DROP SECRET statements
- [ ] 4.4 Implement BindAlterSecret for ALTER SECRET statements
- [ ] 4.5 Implement path-based scope parsing
- [ ] 4.6 Add secret lookup helper functions
- [ ] 4.7 Add integration tests for SQL secret statements

## 5. S3 Filesystem Implementation

- [ ] 5.1 Add `go get github.com/minio/minio-go/v7` dependency
- [ ] 5.2 Create `internal/io/filesystem/s3.go`
- [ ] 5.3 Implement S3FileSystem struct with S3Config
- [ ] 5.4 Implement Open for S3 objects
- [ ] 5.5 Implement ReadAt for range requests
- [ ] 5.6 Implement WriteAt for uploads
- [ ] 5.7 Implement Stat for object metadata
- [ ] 5.8 Add S3File and S3FileInfo implementations
- [ ] 5.9 Add S3 scheme registration to factory
- [ ] 5.10 Add tests for S3 filesystem with mocked AWS SDK

## 6. S3 Credential Provider Chain

- [ ] 6.1 Create `internal/secret/provider.go`
- [ ] 6.2 Implement ConfigProvider for access keys
- [ ] 6.3 Implement EnvProvider for environment variables
- [ ] 6.4 Implement SharedConfigProvider for AWS CLI config
- [ ] 6.5 Implement IMDSv2Provider for IAM roles
- [ ] 6.6 Implement CredentialChain provider
- [ ] 6.7 Add provider selection based on secret configuration
- [ ] 6.8 Add tests for credential provider chain

## 7. S3 Advanced Features

- [ ] 7.1 Implement multipart upload for large writes
- [ ] 7.2 Add concurrent read support with multiple ranges
- [ ] 7.3 Implement retry logic with exponential backoff
- [ ] 7.4 Add endpoint override support for MinIO/compatible stores
- [ ] 7.5 Add region auto-detection
- [ ] 7.6 Add URL style configuration (path vs virtual host)
- [ ] 7.7 Add performance benchmarks for S3 operations
- [ ] 7.8 Add integration tests with real S3 (or LocalStack)

## 8. GCS Filesystem Implementation

- [ ] 8.1 Add `cloud.google.com/go/storage` dependency
- [ ] 8.2 Create `internal/io/filesystem/gcs.go`
- [ ] 8.3 Implement GCSFileSystem struct with GCSConfig
- [ ] 8.4 Implement Open for GCS objects
- [ ] 8.5 Implement ReadAt for range requests
- [ ] 8.6 Implement WriteAt for uploads
- [ ] 8.7 Implement Stat for object metadata
- [ ] 8.8 Add GCS scheme registration (gs://, gcs://)
- [ ] 8.9 Add tests for GCS filesystem with mocked GCS SDK

## 9. Azure Filesystem Implementation

- [ ] 9.1 Add `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob` dependency
- [ ] 9.2 Create `internal/io/filesystem/azure.go`
- [ ] 9.3 Implement AzureFileSystem struct with AzureConfig
- [ ] 9.4 Implement Open for Azure blobs
- [ ] 9.5 Implement ReadAt for range requests
- [ ] 9.6 Implement WriteAt for uploads
- [ ] 9.7 Implement Stat for blob metadata
- [ ] 9.8 Add Azure scheme registration (azure://, az://)
- [ ] 9.9 Add tests for Azure filesystem with mocked SDK

## 10. HTTP/HTTPS Filesystem Implementation

- [ ] 10.1 Create `internal/io/filesystem/http.go`
- [ ] 10.2 Implement HTTPFileSystem struct with HTTPConfig
- [ ] 10.3 Implement Open for HTTP URLs
- [ ] 10.4 Implement ReadAt for range requests
- [ ] 10.5 Implement Stat for HEAD requests
- [ ] 10.6 Add redirect handling
- [ ] 10.7 Add timeout configuration
- [ ] 10.8 Add HTTP scheme registration (http://, https://)
- [ ] 10.9 Add tests for HTTP filesystem

## 11. COPY Statement Cloud Integration

- [ ] 11.1 Update `internal/executor/copy.go` to use FileSystem interface
- [ ] 11.2 Modify COPY FROM to parse cloud URLs
- [ ] 11.3 Modify COPY TO to write to cloud URLs
- [ ] 11.4 Integrate secret lookup before file operations
- [ ] 11.5 Handle range requests for parallel reading
- [ ] 11.6 Add multipart upload for large COPY TO
- [ ] 11.7 Add tests for COPY with S3, GCS, Azure URLs
- [ ] 11.8 Add tests for COPY with HTTP URLs

## 12. Table Function Cloud Integration

- [ ] 12.1 Update `read_csv` table function to use FileSystem
- [ ] 12.2 Update `read_json` table function to use FileSystem
- [ ] 12.3 Update `read_parquet` table function to use FileSystem
- [ ] 12.4 Update `read_ndjson` table function to use FileSystem
- [ ] 12.5 Update `write_csv` table function to use FileSystem
- [ ] 12.6 Update `write_parquet` table function to use FileSystem
- [ ] 12.7 Add glob pattern support for cloud URLs
- [ ] 12.8 Add tests for table functions with cloud URLs

## 13. Secret System Functions

- [ ] 13.1 Implement `which_secret(path, type)` function
- [ ] 13.2 Implement `duckdb_secrets()` function
- [ ] 13.3 Add system function registration
- [ ] 13.4 Add tests for secret system functions

## 14. Documentation

- [ ] 14.1 Add `docs/cloud-storage.md` with usage examples
- [ ] 14.2 Add `docs/secrets.md` with secret management guide
- [ ] 14.3 Add examples for S3, GCS, Azure configuration
- [ ] 14.4 Update `README.md` with cloud storage capabilities
- [ ] 14.5 Add cloud provider credential setup documentation

## 15. Performance Testing

- [ ] 15.1 Create benchmarks for S3 read operations
- [ ] 15.2 Create benchmarks for S3 write operations
- [ ] 15.3 Create benchmarks for GCS operations
- [ ] 15.4 Create benchmarks for Azure operations
- [ ] 15.5 Create benchmarks for HTTP operations
- [ ] 15.6 Compare cloud vs local filesystem performance
- [ ] 15.7 Optimize based on benchmark results

## 16. Compatibility Testing

- [ ] 16.1 Create tests for DuckDB secret compatibility
- [ ] 16.2 Create tests for S3 URL syntax compatibility
- [ ] 16.3 Create tests for GCS URL syntax compatibility
- [ ] 16.4 Create tests for Azure URL syntax compatibility
- [ ] 16.5 Create tests against official DuckDB (if CGO available)
- [ ] 16.6 Test with LocalStack for S3 compatibility
- [ ] 16.7 Test with Azurite for Azure compatibility

## 17. WASM Compatibility

- [ ] 17.1 Verify FileSystem interface works with WASM
- [ ] 17.2 Handle async/await patterns in WASM
- [ ] 17.3 Add WASM-specific documentation
- [ ] 17.4 Test with browser-based execution

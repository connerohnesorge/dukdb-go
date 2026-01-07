## 1. Implementation Review

- [ ] 1.1 Review Azure blob storage implementation in `internal/io/cloud/azure/`
- [ ] 1.2 Identify existing write/upload functions
- [ ] 1.3 Check Azure SDK version and capabilities
- [ ] 1.4 Document current implementation status

## 2. Syntax Testing

- [ ] 2.1 Test COPY TO with Azure blob storage (Parquet format)
- [ ] 2.2 Test COPY TO with Azure blob storage (CSV format)
- [ ] 2.3 Test COPY TO with Azure blob storage (JSON format)
- [ ] 2.4 Test authentication with connection string secret
- [ ] 2.5 Test authentication with account + key secret

## 3. Edge Case Testing

- [ ] 3.1 Test large file uploads (>256MB)
- [ ] 3.2 Test concurrent write operations
- [ ] 3.3 Test error handling and retry logic
- [ ] 3.4 Test network timeout scenarios

## 4. Documentation

- [ ] 4.1 Document Azure authentication methods
- [ ] 4.2 Document COPY TO syntax for Azure
- [ ] 4.3 Create examples for common Azure write scenarios
- [ ] 4.4 Document known limitations (if any)

## 5. Testing

- [ ] 5.1 Create integration tests for Azure write operations
- [ ] 5.2 Create tests for Azure authentication methods
- [ ] 5.3 Test against Azure Storage Emulator (Azurite) for local testing
- [ ] 5.4 Test against real Azure Blob Storage (if credentials available)

## 6. Verification

- [ ] 6.1 Run `spectr validate verify-azure-write-support`
- [ ] 6.2 Verify compatibility with DuckDB v1.4.3 Azure features
- [ ] 6.3 Ensure all existing tests pass
- [ ] 6.4 Document findings and any required fixes

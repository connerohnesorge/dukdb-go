## Context

Azure Blob Storage write support was added in DuckDB v1.4.3. This change enables:
- Writing Parquet/CSV/JSON files directly to Azure Blob Storage
- Using Azure as both source and destination for data pipelines
- Integration with Azure Data Lake Gen2

## Goals / Non-Goals

### Goals
- Verify existing Azure blob storage write implementation
- Test COPY TO with Azure blob storage URLs
- Document Azure authentication methods
- Create integration tests for Azure write operations
- Ensure compatibility with DuckDB v1.4.3 Azure features

### Non-Goals
- Implementing Azure-specific optimizations
- Adding Azure Data Lake Gen2 hierarchical namespace support
- Supporting Azure-specific file operations (leases, snapshots)

## Azure Authentication Methods

 dukdb-go should support multiple Azure authentication methods:

### Method 1: Connection String

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net'
);
```

### Method 2: Account + Key

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    ACCOUNT_NAME 'myaccount',
    ACCOUNT_KEY 'mykey'
);
```

### Method 3: Managed Identity (Future)

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    MANAGED_IDENTITY 'system'
);
```

### Method 4: Service Principal (Future)

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    TENANT_ID 'my-tenant',
    CLIENT_ID 'my-client',
    CLIENT_SECRET 'my-secret'
);
```

## Verification Checklist

### Phase 1: Implementation Review

- [ ] Review `internal/io/cloud/azure/` implementation
- [ ] Identify Azure blob upload/write functions
- [ ] Check for Azure SDK version compatibility
- [ ] Verify multipart upload support for large files

### Phase 2: Syntax Testing

- [ ] Test: `COPY (SELECT * FROM table) TO 'azure://container/path/file.parquet' (FORMAT PARQUET)`
- [ ] Test: `COPY (SELECT * FROM table) TO 'az://container/path/file.csv' (FORMAT CSV)`
- [ ] Test: Authentication with connection string secret
- [ ] Test: Authentication with account + key secret

### Phase 3: Edge Cases

- [ ] Large file uploads (>256MB, requires multipart)
- [ ] Concurrent writes to same blob
- [ ] Write errors and retry logic
- [ ] Network timeout handling

## Known Limitations

If Azure write support is not fully implemented, document:
- Missing features
- Workarounds (e.g., write to local then upload)
- Planned implementation timeline

## References

- DuckDB Azure: https://duckdb.org/docs/data/cloudstorage#azure-blob-storage
- Azure Blob Storage: https://docs.microsoft.com/en-us/azure/storage/blobs/
- Azure SDK for Go: https://github.com/Azure/azure-sdk-for-go

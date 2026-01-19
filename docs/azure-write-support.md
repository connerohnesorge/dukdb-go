# Azure Blob Storage Write Support

## Overview

dukdb-go fully supports writing data to Azure Blob Storage using the `COPY TO` statement. This feature enables you to export query results, tables, or arbitrary data directly to Azure Blob Storage containers in various formats including Parquet, CSV, and JSON.

## Prerequisites

Before using Azure Blob Storage with dukdb-go, you need to configure Azure authentication. The system supports multiple authentication methods.

## Authentication Methods

### Method 1: Connection String

The simplest method for authentication is using an Azure connection string:

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net'
);
```

### Method 2: Account Name and Key

You can also authenticate using your storage account name and key:

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    ACCOUNT_NAME 'myaccount',
    ACCOUNT_KEY 'mykey'
);
```

### Method 3: SAS Token (Shared Access Signature)

For temporary access, you can use a SAS token:

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    SAS_TOKEN 'sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2024-01-01T00:00:00Z&st=2023-01-01T00:00:00Z&spr=https,http&sig=...'
);
```

### Method 4: Service Principal

For enterprise scenarios, use a service principal:

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    TENANT_ID 'my-tenant-id',
    CLIENT_ID 'my-client-id',
    CLIENT_SECRET 'my-client-secret'
);
```

### Method 5: Default Azure Credential

If running in Azure (VM, Container Instance, etc.), you can use managed identity:

```sql
CREATE SECRET azure_secret (
    TYPE AZURE,
    USE_DEFAULT_CREDENTIAL true
);
```

## Supported URL Formats

dukdb-go supports multiple URL formats for Azure Blob Storage:

- `azure://container/path/to/file.parquet`
- `az://container/path/to/file.csv`
- `wasb://container/path/to/file.json` (legacy format)

## Writing Data to Azure

### Basic COPY TO Syntax

```sql
-- Export a table to Parquet format
COPY my_table TO 'azure://mycontainer/exports/data.parquet' (FORMAT PARQUET);

-- Export query results to CSV
COPY (SELECT * FROM my_table WHERE date > '2023-01-01')
TO 'azure://mycontainer/reports/recent_data.csv' (FORMAT CSV);

-- Export to JSON
COPY my_table
TO 'azure://mycontainer/json_exports/data.json' (FORMAT JSON);
```

### Advanced Options

```sql
-- CSV with custom delimiter and header
COPY my_table TO 'azure://mycontainer/data.csv' (
    FORMAT CSV,
    DELIMITER ',',
    HEADER true,
    QUOTE '"'
);

-- Parquet with compression
COPY my_table TO 'azure://mycontainer/compressed_data.parquet' (
    FORMAT PARQUET,
    COMPRESSION 'snappy'
);

-- Writing to a specific blob prefix
COPY sales_data
TO 'azure://mycontainer/year=2024/month=01/day=15/sales.parquet' (FORMAT PARQUET);
```

## Examples

### Example 1: Export Sales Data

```sql
-- Create a secret for Azure authentication
CREATE SECRET azure_conn (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=salesdata;AccountKey=...;EndpointSuffix=core.windows.net'
);

-- Export daily sales summary
COPY (
    SELECT
        date,
        region,
        SUM(amount) as total_sales,
        COUNT(*) as transaction_count
    FROM sales
    WHERE date = CURRENT_DATE
) TO 'azure://reports/daily/summary_2024_01_15.parquet' (FORMAT PARQUET);
```

### Example 2: Incremental Data Export

```sql
-- Export only new records since last export
COPY (
    SELECT * FROM user_activity
    WHERE created_at > (SELECT MAX(created_at) FROM last_export_checkpoint)
) TO 'azure://data/incremental/user_activity_2024_01_15.parquet' (FORMAT PARQUET);

-- Update checkpoint
CREATE OR REPLACE VIEW last_export_checkpoint AS
SELECT CURRENT_TIMESTAMP as last_exported_at;
```

### Example 3: Multi-format Export

```sql
-- Export the same data in multiple formats
CREATE SECRET azure_data (
    TYPE AZURE,
    ACCOUNT_NAME 'mydataaccount',
    ACCOUNT_KEY 'myaccountkey'
);

-- Export as Parquet for analytics
COPY customer_data
TO 'azure://exports/customers/latest.parquet' (FORMAT PARQUET);

-- Export as CSV for Excel users
COPY customer_data
TO 'azure://exports/customers/latest.csv' (FORMAT CSV, HEADER true);

-- Export as JSON for APIs
COPY customer_data
TO 'azure://exports/customers/latest.json' (FORMAT JSON);
```

## Performance Considerations

### Large File Handling

For files larger than 256MB, dukdb-go automatically uses Azure's multipart upload:

- Data is buffered in memory (default 4MB blocks)
- Multiple blocks are uploaded in parallel
- Automatic retry on transient failures
- Optimal for files 100MB to several GB

### Concurrent Writes

You can perform concurrent writes to different blobs:

```sql
-- These operations can run in parallel
COPY table1 TO 'azure://container/table1.parquet' (FORMAT PARQUET);
COPY table2 TO 'azure://container/table2.parquet' (FORMAT PARQUET);
COPY table3 TO 'azure://container/table3.parquet' (FORMAT PARQUET);
```

### Network Optimization

For better performance:
- Use Azure storage accounts in the same region as your compute
- Enable HTTPS for security (enabled by default)
- Configure appropriate timeout values for large transfers

## Error Handling

### Common Errors and Solutions

1. **Authentication Failed**
   ```
   Error: azure: failed to create client: failed to create shared key credential
   ```
   Solution: Verify your account name and key are correct

2. **Container Not Found**
   ```
   Error: azure: failed to upload blob: ContainerNotFound
   ```
   Solution: Ensure the container exists or you have permission to create it

3. **Network Timeout**
   ```
   Error: context deadline exceeded
   ```
   Solution: Increase timeout in Azure configuration or check network connectivity

### Retry Configuration

dukdb-go automatically retries transient failures. You can configure retry behavior:

```go
config := filesystem.NewAzureConfig(
    filesystem.WithAzureRetryConfig(filesystem.RetryConfig{
        MaxRetries:    5,
        RetryDelay:    1 * time.Second,
        MaxRetryDelay: 30 * time.Second,
    }),
)
```

## Testing with Azurite

For local development and testing, you can use Azurite (Azure Storage Emulator):

```bash
# Start Azurite in Docker
docker run -d -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0

# Set environment variable
export AZURITE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1

# Run tests
go test -tags integration ./internal/io/filesystem/ -run Azurite
```

## Limitations

1. **Append Operations**: Azure Blob Storage does not support true append operations. Each write creates a new blob or overwrites an existing one.

2. **Directory Operations**: Azure uses blob prefixes as virtual directories. Creating empty directories is not supported.

3. **Maximum File Size**: Limited by Azure Blob Storage (approximately 5TB per blob).

4. **Concurrent Writes to Same Blob**: Writing to the same blob concurrently will result in the last write winning (overwrite behavior).

## Best Practices

1. **Use Appropriate File Formats**:
   - Parquet for analytical workloads
   - CSV for compatibility
   - JSON for semi-structured data

2. **Organize with Prefixes**: Use blob prefixes to organize data logically:
   ```
   azure://container/year=2024/month=01/data.parquet
   ```

3. **Handle Authentication Securely**:
   - Use SAS tokens for temporary access
   - Store secrets securely (environment variables, secret management systems)
   - Use managed identity when running in Azure

4. **Monitor Performance**: Track upload times and optimize block sizes for your workload

## Compatibility

This implementation is compatible with DuckDB v1.4.3 Azure features and supports all standard `COPY TO` options. The Azure SDK version used (v1.6.3) provides full compatibility with current Azure Blob Storage APIs.
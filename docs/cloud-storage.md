# Cloud Storage Integration

dukdb-go supports reading and writing data from cloud storage providers including Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, and HTTP/HTTPS endpoints. This guide covers how to configure and use cloud storage with dukdb-go.

## Supported Providers

| Provider | URL Schemes | Description |
|----------|-------------|-------------|
| Amazon S3 | `s3://`, `s3a://`, `s3n://` | Amazon Simple Storage Service and S3-compatible stores |
| Google Cloud Storage | `gs://`, `gcs://` | Google Cloud Storage buckets |
| Azure Blob Storage | `azure://`, `az://` | Microsoft Azure Blob Storage containers |
| HTTP/HTTPS | `http://`, `https://` | Public HTTP endpoints and APIs |

## Quick Start

### Reading from S3

```sql
-- First, create a secret with your AWS credentials
CREATE SECRET my_s3_secret (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

-- Read CSV from S3
SELECT * FROM read_csv('s3://my-bucket/data/sales.csv');

-- Read Parquet from S3
SELECT * FROM read_parquet('s3://my-bucket/data/analytics.parquet');

-- Read JSON from S3
SELECT * FROM read_json('s3://my-bucket/data/events.json');
```

### Reading from Public URLs

```sql
-- No secret needed for public URLs
SELECT * FROM read_csv('https://example.com/public/data.csv');

-- Read from public S3 bucket
SELECT * FROM read_parquet('s3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet');
```

## URL Formats

### Amazon S3

S3 URLs follow the format: `s3://bucket-name/path/to/object`

```sql
-- Standard S3 URL
SELECT * FROM read_csv('s3://my-bucket/data/file.csv');

-- With folder path
SELECT * FROM read_parquet('s3://my-bucket/warehouse/2024/01/data.parquet');

-- Using s3a:// scheme (Hadoop compatibility)
SELECT * FROM read_csv('s3a://my-bucket/data/file.csv');
```

### Google Cloud Storage

GCS URLs follow the format: `gs://bucket-name/path/to/object`

```sql
-- Using gs:// scheme
SELECT * FROM read_csv('gs://my-bucket/data/file.csv');

-- Using gcs:// scheme (alternative)
SELECT * FROM read_parquet('gcs://my-bucket/data/file.parquet');
```

### Azure Blob Storage

Azure URLs follow the format: `azure://container-name/path/to/blob`

```sql
-- Standard Azure URL
SELECT * FROM read_csv('azure://my-container/data/file.csv');

-- Using az:// scheme (shorthand)
SELECT * FROM read_parquet('az://my-container/data/file.parquet');
```

### HTTP/HTTPS

HTTP URLs use standard web URL format:

```sql
-- HTTPS (recommended)
SELECT * FROM read_csv('https://data.example.com/files/data.csv');

-- HTTP (not recommended for sensitive data)
SELECT * FROM read_json('http://api.example.com/data.json');
```

## Reading Data

### Table Functions

All standard table functions support cloud URLs:

```sql
-- CSV files
SELECT * FROM read_csv('s3://bucket/data.csv');
SELECT * FROM read_csv_auto('s3://bucket/data.csv');

-- JSON files
SELECT * FROM read_json('gs://bucket/data.json');
SELECT * FROM read_json_auto('gs://bucket/data.json');
SELECT * FROM read_ndjson('azure://container/events.ndjson');

-- Parquet files
SELECT * FROM read_parquet('s3://bucket/data.parquet');
```

### COPY FROM

Import data directly into tables:

```sql
-- Create a table
CREATE TABLE sales (
    id INTEGER,
    product VARCHAR,
    amount DECIMAL(10, 2),
    date DATE
);

-- Copy from S3
COPY sales FROM 's3://my-bucket/sales/2024.csv' (FORMAT CSV, HEADER true);

-- Copy from GCS with options
COPY sales FROM 'gs://my-bucket/sales.csv' (
    FORMAT CSV,
    HEADER true,
    DELIMITER ',',
    NULL 'NA'
);

-- Copy Parquet from Azure
COPY sales FROM 'azure://container/sales.parquet' (FORMAT PARQUET);
```

## Writing Data

### COPY TO

Export data to cloud storage:

```sql
-- Export to S3 as CSV
COPY sales TO 's3://my-bucket/exports/sales.csv' (FORMAT CSV, HEADER true);

-- Export to GCS as Parquet
COPY sales TO 'gs://my-bucket/exports/sales.parquet' (FORMAT PARQUET);

-- Export query results
COPY (
    SELECT product, SUM(amount) as total
    FROM sales
    GROUP BY product
) TO 's3://my-bucket/reports/product_totals.csv' (FORMAT CSV);

-- Export to Azure as JSON
COPY sales TO 'azure://container/exports/sales.json' (FORMAT JSON);
```

### Write Table Functions

```sql
-- Write CSV to S3
SELECT * FROM write_csv(
    (SELECT * FROM sales WHERE date >= '2024-01-01'),
    's3://my-bucket/exports/recent_sales.csv'
);

-- Write Parquet to GCS with compression
SELECT * FROM write_parquet(
    (SELECT * FROM sales),
    'gs://my-bucket/exports/sales.parquet',
    COMPRESSION 'SNAPPY'
);
```

## Glob Patterns

Use glob patterns to read multiple files:

```sql
-- Read all CSV files in a folder
SELECT * FROM read_csv('s3://my-bucket/data/*.csv');

-- Read files from multiple years
SELECT * FROM read_parquet('s3://my-bucket/warehouse/202[34]/*/data.parquet');

-- Read all JSON files recursively
SELECT * FROM read_json('gs://my-bucket/logs/**/*.json');

-- Glob patterns in COPY
COPY my_table FROM 's3://my-bucket/data/*.csv' (FORMAT CSV);
```

### Supported Glob Syntax

| Pattern | Description |
|---------|-------------|
| `*` | Match any characters within a path segment |
| `**` | Match any characters across path segments (recursive) |
| `?` | Match a single character |
| `[abc]` | Match any character in the set |
| `[a-z]` | Match any character in the range |
| `[!abc]` | Match any character not in the set |

## Authentication

See [Secrets Management](secrets.md) for detailed information on configuring authentication for each cloud provider.

### Quick Reference

```sql
-- S3 with access keys
CREATE SECRET s3_secret (
    TYPE S3,
    KEY_ID 'your-access-key-id',
    SECRET 'your-secret-access-key',
    REGION 'us-east-1'
);

-- GCS with service account
CREATE SECRET gcs_secret (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '{...}'  -- or file path
);

-- Azure with account key
CREATE SECRET azure_secret (
    TYPE AZURE,
    ACCOUNT_NAME 'mystorageaccount',
    ACCOUNT_KEY 'your-account-key'
);

-- HTTP with bearer token
CREATE SECRET http_secret (
    TYPE HTTP,
    BEARER_TOKEN 'your-token'
);
```

## Provider-Specific Configuration

### Amazon S3

#### S3-Compatible Storage (MinIO, etc.)

```sql
CREATE SECRET minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'http://localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
);

-- Now you can use S3 URLs pointing to MinIO
SELECT * FROM read_csv('s3://my-bucket/data.csv');
```

#### Using IAM Roles (EC2/ECS)

```sql
-- Use IAM role credentials from instance metadata
CREATE SECRET iam_secret (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN
);
```

#### Custom Endpoint (AWS PrivateLink, etc.)

```sql
CREATE SECRET private_s3 (
    TYPE S3,
    KEY_ID 'your-key-id',
    SECRET 'your-secret',
    REGION 'us-east-1',
    ENDPOINT 'https://bucket.vpce-xxx.s3.us-east-1.vpce.amazonaws.com'
);
```

### Google Cloud Storage

#### Using Application Default Credentials

```sql
CREATE SECRET gcs_adc (
    TYPE GCS,
    PROVIDER CREDENTIAL_CHAIN
);
```

#### Using Service Account Key File

```sql
CREATE SECRET gcs_sa (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);
```

### Azure Blob Storage

#### Using Connection String

```sql
CREATE SECRET azure_conn (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net'
);
```

#### Using Managed Identity

```sql
CREATE SECRET azure_mi (
    TYPE AZURE,
    PROVIDER CREDENTIAL_CHAIN
);
```

## Performance Considerations

### Range Requests

dukdb-go uses HTTP range requests to efficiently read portions of files. This is particularly beneficial for columnar formats like Parquet, where only specific columns need to be read.

```sql
-- Only columns 'id' and 'name' are fetched from cloud storage
SELECT id, name FROM read_parquet('s3://bucket/large_file.parquet');
```

### Parallel Reads

For large files, dukdb-go can perform parallel range requests to improve throughput:

```sql
-- Reads are parallelized automatically for large files
SELECT * FROM read_parquet('s3://bucket/large_dataset.parquet');
```

### Multipart Uploads

Large writes are automatically split into multipart uploads for improved reliability and throughput:

```sql
-- Large exports use multipart upload automatically
COPY large_table TO 's3://bucket/exports/large_file.parquet' (FORMAT PARQUET);
```

### Caching

Consider caching frequently accessed data locally for better performance:

```sql
-- Create a local copy for repeated queries
COPY (SELECT * FROM read_parquet('s3://bucket/data.parquet'))
TO '/local/cache/data.parquet' (FORMAT PARQUET);
```

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `secret not found` | No matching credentials for URL | Create a secret with appropriate scope |
| `access denied` | Invalid credentials or insufficient permissions | Verify credentials and IAM policies |
| `bucket not found` | Bucket/container does not exist | Check bucket name and region |
| `object not found` | File does not exist at path | Verify the file path |
| `connection timeout` | Network issues | Check network connectivity and firewall rules |

### Troubleshooting

```sql
-- Check which secret matches a URL
SELECT * FROM which_secret('s3://my-bucket/data.csv', 'S3');

-- List all configured secrets
SELECT * FROM duckdb_secrets();

-- Test connectivity with a simple query
SELECT * FROM read_csv('s3://my-bucket/test.csv') LIMIT 1;
```

## Go Integration

### Using database/sql

```go
package main

import (
    "database/sql"
    "log"

    _ "github.com/dukdb/dukdb-go"
)

func main() {
    db, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create S3 secret
    _, err = db.Exec(`
        CREATE SECRET my_s3 (
            TYPE S3,
            KEY_ID 'your-key-id',
            SECRET 'your-secret',
            REGION 'us-east-1'
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Query data from S3
    rows, err := db.Query(`
        SELECT * FROM read_parquet('s3://my-bucket/data.parquet')
        WHERE status = 'active'
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // Process rows...
}
```

### Environment Variable Configuration

You can also configure credentials via environment variables before opening the database:

```go
import "os"

// Set AWS credentials
os.Setenv("AWS_ACCESS_KEY_ID", "your-key-id")
os.Setenv("AWS_SECRET_ACCESS_KEY", "your-secret")
os.Setenv("AWS_REGION", "us-east-1")

// Create secret using environment provider
db.Exec(`CREATE SECRET env_s3 (TYPE S3, PROVIDER ENV)`)
```

## See Also

- [Secrets Management](secrets.md) - Detailed guide on managing credentials
- [Extended Types](types.md) - Information on supported data types
- [File Formats](file-formats.md) - Details on CSV, JSON, and Parquet support
- [WebAssembly Guide](wasm.md) - Using cloud storage in browser/WASM environments

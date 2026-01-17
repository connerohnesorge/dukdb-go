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

### Array of Files Syntax

Read multiple specific files using array syntax instead of glob patterns:

```sql
-- Read a list of specific CSV files
SELECT * FROM read_csv(['data/jan.csv', 'data/feb.csv', 'data/mar.csv']);

-- Mix local and cloud files in the same query
SELECT * FROM read_parquet(['local_data.parquet', 's3://bucket/remote_data.parquet']);

-- Array syntax with glob patterns inside
SELECT * FROM read_json(['logs/2024/*.json', 'logs/2023/december.json']);

-- Parquet files from different S3 buckets
SELECT * FROM read_parquet([
    's3://bucket-a/data/part1.parquet',
    's3://bucket-b/data/part2.parquet'
]);
```

Array syntax is useful when you need to read specific files that do not follow a glob-friendly naming convention, or when combining files from different locations.

### Metadata Columns

Virtual metadata columns provide information about which file each row originated from. These columns are added to the output when enabled via options.

```sql
-- Add filename column showing the source file path
SELECT *, filename FROM read_csv('data/*.csv', filename=true);

-- Add file_row_number (1-indexed row number within each file)
SELECT *, file_row_number FROM read_parquet('s3://bucket/*.parquet', file_row_number=true);

-- Add file_index (0-indexed position in the file list)
SELECT *, file_index FROM read_json('logs/**/*.json', file_index=true);

-- Combine all metadata columns
SELECT
    *,
    filename,
    file_row_number,
    file_index
FROM read_csv(
    'data/*.csv',
    filename=true,
    file_row_number=true,
    file_index=true
);
```

| Option | Type | Column Name | Description |
|--------|------|-------------|-------------|
| `filename=true` | VARCHAR | `filename` | Full path of the source file |
| `file_row_number=true` | BIGINT | `file_row_number` | 1-indexed row number within the file |
| `file_index=true` | INTEGER | `file_index` | 0-indexed position in the sorted file list |

**Example output with metadata columns:**

```sql
SELECT * FROM read_csv('data/*.csv', filename=true, file_row_number=true);
```

| id | name | filename | file_row_number |
|----|------|----------|-----------------|
| 1 | Alice | data/users_a.csv | 1 |
| 2 | Bob | data/users_a.csv | 2 |
| 3 | Carol | data/users_b.csv | 1 |

### Schema Alignment (Union by Name)

When reading multiple files with different schemas, dukdb-go uses union-by-name semantics to merge the schemas:

```sql
-- File1: id (INTEGER), name (VARCHAR)
-- File2: id (INTEGER), email (VARCHAR)
-- Result: id (INTEGER), name (VARCHAR), email (VARCHAR)

SELECT * FROM read_csv('data/*.csv', union_by_name=true);
```

**Union-by-name behavior:**

- **Column matching**: Columns are matched by name (case-sensitive)
- **Missing columns**: Filled with NULL values
- **Type widening**: Compatible types are widened automatically
- **Incompatible types**: Results in an error

**Type widening rules:**

| Source Types | Result Type |
|--------------|-------------|
| TINYINT + SMALLINT | SMALLINT |
| SMALLINT + INTEGER | INTEGER |
| INTEGER + BIGINT | BIGINT |
| FLOAT + DOUBLE | DOUBLE |
| INTEGER + DOUBLE | DOUBLE |
| TIMESTAMP_S + TIMESTAMP | TIMESTAMP |
| TIMESTAMP + TIMESTAMP_NS | TIMESTAMP_NS |

**Incompatible type errors:**

```sql
-- This will fail: cannot merge INTEGER with VARCHAR
-- File1: value (INTEGER)
-- File2: value (VARCHAR)
SELECT * FROM read_csv('incompatible/*.csv');
-- Error: incompatible column types: cannot merge INTEGER with VARCHAR
```

**Disable union-by-name for positional matching:**

```sql
-- Match columns by position instead of name
SELECT * FROM read_csv('data/*.csv', union_by_name=false);
```

When `union_by_name=false`, all files must have the same number of columns. Column names from the first file are used.

### Hive Partitioning

Hive partitioning extracts column values from directory names in the path structure:

```
data/
  year=2023/
    month=01/
      sales.parquet
    month=02/
      sales.parquet
  year=2024/
    month=01/
      sales.parquet
```

```sql
-- Enable Hive partitioning to extract year and month columns
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true);
```

| product | amount | year | month |
|---------|--------|------|-------|
| Widget | 100.00 | 2023 | 01 |
| Gadget | 200.00 | 2023 | 02 |
| Widget | 150.00 | 2024 | 01 |

**Hive partitioning options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `hive_partitioning` | BOOLEAN or 'auto' | false | Enable Hive partition column extraction |
| `hive_types_autocast` | BOOLEAN | true | Automatically infer types for partition values |
| `hive_types` | MAP | NULL | Explicit type mapping for partition columns |

**Auto-detection mode:**

```sql
-- Automatically detect if paths contain Hive partitions
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning='auto');
```

**Explicit type mapping:**

```sql
-- Specify exact types for partition columns
SELECT * FROM read_csv(
    'data/**/*.csv',
    hive_partitioning=true,
    hive_types={'year': 'INTEGER', 'month': 'INTEGER'}
);
```

**Type autocast behavior:**

When `hive_types_autocast=true` (default), partition values are automatically converted:
- Numeric strings (e.g., "2024") become INTEGER or BIGINT
- Decimal strings (e.g., "3.14") become DOUBLE
- "true"/"false" become BOOLEAN
- All other values remain VARCHAR

```sql
-- Disable autocast to keep all partition values as VARCHAR
SELECT * FROM read_parquet('data/**/*.parquet',
    hive_partitioning=true,
    hive_types_autocast=false
);
```

### File Glob Options

The `file_glob_behavior` option controls how empty glob results are handled:

| Value | Behavior |
|-------|----------|
| `'DISALLOW_EMPTY'` | Return an error when no files match (default) |
| `'ALLOW_EMPTY'` | Return an empty result set when no files match |
| `'FALLBACK_GLOB'` | Treat the pattern as a literal path if no matches found |

```sql
-- Default: error when no files match
SELECT * FROM read_csv('data/*.csv');
-- Error: no files match pattern: data/*.csv

-- Allow empty results
SELECT * FROM read_csv('data/*.csv', file_glob_behavior='ALLOW_EMPTY');
-- Returns empty result set (no error)

-- Fallback to literal path
SELECT * FROM read_csv('file[1].csv', file_glob_behavior='FALLBACK_GLOB');
-- If no glob matches, tries to read 'file[1].csv' as a literal filename
```

**Use cases:**

- `DISALLOW_EMPTY`: Strict mode, ensures data is present
- `ALLOW_EMPTY`: Graceful handling when data may not exist yet
- `FALLBACK_GLOB`: Useful for filenames containing glob characters (e.g., `report[2024].csv`)

### Files to Sniff

The `files_to_sniff` option controls how many files are sampled for schema detection when reading multiple files:

```sql
-- Sniff schema from first 3 files (useful when schemas may vary)
SELECT * FROM read_csv('data/**/*.csv', files_to_sniff=3);

-- Sniff schema from all files (most accurate but slower)
SELECT * FROM read_csv('data/**/*.csv', files_to_sniff=-1);

-- Default: sniff only the first file
SELECT * FROM read_csv('data/**/*.csv');  -- files_to_sniff=1
```

| Value | Behavior |
|-------|----------|
| 1 (default) | Sniff schema from the first file only |
| N > 1 | Sniff schema from the first N files and merge |
| -1 | Sniff schema from all matched files |

**When to increase `files_to_sniff`:**

- Files have different columns (some columns may be missing in early files)
- Column types vary between files (e.g., first file has NULL values, later files have integers)
- Schema evolution over time (newer files have additional columns)

### Limits and Configuration

**Maximum files per glob:**

By default, a glob pattern can match up to **10,000 files**. This limit prevents memory exhaustion and excessive cloud API calls.

```sql
-- This will error if more than 10,000 files match
SELECT * FROM read_parquet('s3://bucket/data/**/*.parquet');
-- Error: glob pattern matches too many files: 15000 files (limit: 10000)
```

**File ordering:**

Files matched by glob patterns are sorted **alphabetically** by path. This ensures deterministic ordering across queries.

```sql
-- Files are processed in alphabetical order:
-- data/file_01.csv, data/file_02.csv, data/file_10.csv
SELECT * FROM read_csv('data/*.csv', file_index=true);
```

| file_index | filename |
|------------|----------|
| 0 | data/file_01.csv |
| 1 | data/file_02.csv |
| 2 | data/file_10.csv |

### Error Handling

**Common glob-related errors:**

| Error | Cause | Solution |
|-------|-------|----------|
| `no files match pattern` | Glob pattern matches zero files | Check pattern, verify files exist, or use `file_glob_behavior='ALLOW_EMPTY'` |
| `glob pattern matches too many files` | More than 10,000 files matched | Use a more specific pattern or filter by partition |
| `incompatible column types` | Files have columns with conflicting types | Ensure column types are compatible or explicitly cast |
| `invalid glob pattern` | Malformed pattern syntax | Check bracket expressions are closed, no multiple `**` |
| `unclosed bracket expression` | Missing `]` in character class | Add closing bracket: `[a-z]` |
| `multiple '**' wildcards` | Pattern contains more than one `**` | Use only one recursive wildcard per pattern |

**Example error handling in Go:**

```go
rows, err := db.Query(`
    SELECT * FROM read_csv('s3://bucket/data/*.csv', file_glob_behavior='ALLOW_EMPTY')
`)
if err != nil {
    // Handle error (connection issues, permission denied, etc.)
    log.Printf("Query failed: %v", err)
    return
}

// Check if any rows were returned
if !rows.Next() {
    log.Println("No data files found matching pattern")
    return
}
```

### Multi-Format Examples

**CSV with all glob features:**

```sql
SELECT
    product,
    amount,
    filename,
    file_row_number,
    year,
    month
FROM read_csv(
    's3://bucket/sales/year=*/month=*/*.csv',
    header=true,
    delimiter=',',
    filename=true,
    file_row_number=true,
    hive_partitioning=true,
    hive_types={'year': 'INTEGER', 'month': 'INTEGER'},
    union_by_name=true,
    files_to_sniff=3
);
```

**JSON with glob patterns:**

```sql
-- Read all JSON log files recursively
SELECT
    timestamp,
    level,
    message,
    filename
FROM read_json(
    'logs/**/*.json',
    format='array',
    filename=true,
    file_glob_behavior='ALLOW_EMPTY'
);

-- Read NDJSON files with Hive partitioning
SELECT * FROM read_ndjson(
    'events/date=*/*.ndjson',
    hive_partitioning=true
);
```

**Parquet with glob patterns:**

```sql
-- Read partitioned Parquet dataset
SELECT
    *,
    file_index
FROM read_parquet(
    's3://datalake/warehouse/table/year=202[34]/month=*/data.parquet',
    file_index=true,
    hive_partitioning=true,
    hive_types_autocast=true
);

-- Read specific files from array
SELECT * FROM read_parquet([
    'gs://bucket/q1_2024.parquet',
    'gs://bucket/q2_2024.parquet',
    'gs://bucket/q3_2024.parquet'
]);
```

**XLSX with glob patterns:**

```sql
-- Read multiple Excel files
SELECT
    *,
    filename
FROM read_xlsx(
    'reports/**/*.xlsx',
    sheet='Summary',
    filename=true
);

-- Combine Excel files with different schemas
SELECT * FROM read_xlsx(
    ['report_2023.xlsx', 'report_2024.xlsx'],
    union_by_name=true,
    files_to_sniff=2
);
```

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

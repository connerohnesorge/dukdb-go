# Apache Iceberg Table Support

dukdb-go provides native support for reading Apache Iceberg tables, enabling time travel queries, partition pruning, and seamless integration with data lake architectures.

## Overview

Apache Iceberg is an open table format designed for huge analytic datasets. It provides:

- **Time Travel**: Query data as it existed at any point in time
- **Schema Evolution**: Add, drop, rename, and reorder columns without rewriting data
- **Partition Evolution**: Change partitioning schemes without rewriting data
- **Hidden Partitioning**: Partition data without users needing to know about partitions

dukdb-go reads Iceberg tables by parsing the table metadata and using the existing Parquet infrastructure to read data files. This approach provides excellent performance while maintaining compatibility with the Iceberg specification.

### Why Use Iceberg?

| Feature | Regular Parquet | Iceberg Table |
|---------|----------------|---------------|
| Time Travel | No | Yes |
| Schema Evolution | Manual | Automatic |
| Partition Pruning | Hive-style only | Hidden partitioning |
| ACID Transactions | No | Yes |
| Concurrent Writers | No | Yes |
| Data Quality | Manual | Manifest-level stats |

## Quick Start

### Reading an Iceberg Table

```sql
-- Read current snapshot
SELECT * FROM iceberg_scan('/path/to/iceberg/table');

-- Read specific columns
SELECT id, name, created_at FROM iceberg_scan('/path/to/iceberg/table');

-- With filters (partition pruning applied automatically)
SELECT * FROM iceberg_scan('/path/to/iceberg/table')
WHERE date = '2024-01-15';
```

### Go API Example

```go
package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/dukdb/dukdb-go"
    _ "github.com/dukdb/dukdb-go/internal/engine"
)

func main() {
    db, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Read from an Iceberg table
    rows, err := db.Query(`
        SELECT id, name, value
        FROM iceberg_scan('/data/warehouse/my_table')
        WHERE id > 100
        LIMIT 1000
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int64
        var name string
        var value float64
        if err := rows.Scan(&id, &name, &value); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("id=%d, name=%s, value=%.2f\n", id, name, value)
    }
}
```

## Table Functions

### iceberg_scan

Reads data from an Iceberg table.

```sql
iceberg_scan(path [, options])
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Path to the Iceberg table (local or cloud storage) |

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| snapshot_id | BIGINT | NULL | Specific snapshot ID to read |
| timestamp | TIMESTAMP | NULL | Query data as of this timestamp |
| columns | VARCHAR[] | NULL | Columns to project (NULL = all) |
| limit | BIGINT | 0 | Maximum rows to return (0 = unlimited) |

**Examples:**

```sql
-- Read current snapshot
SELECT * FROM iceberg_scan('/warehouse/sales');

-- Read specific snapshot
SELECT * FROM iceberg_scan('/warehouse/sales', snapshot_id := 1234567890);

-- Time travel by timestamp
SELECT * FROM iceberg_scan('/warehouse/sales',
    timestamp := TIMESTAMP '2024-01-15 10:00:00');

-- Column projection
SELECT * FROM iceberg_scan('/warehouse/sales', columns := ['id', 'name']);

-- With limit
SELECT * FROM iceberg_scan('/warehouse/sales', limit := 1000);
```

### iceberg_metadata

Returns detailed metadata about an Iceberg table including schema, partition spec, and current snapshot information.

```sql
iceberg_metadata(path)
```

**Output Columns:**

| Column | Type | Description |
|--------|------|-------------|
| format_version | INTEGER | Iceberg format version (1 or 2) |
| table_uuid | UUID | Unique table identifier |
| location | VARCHAR | Table base location |
| last_updated_ms | BIGINT | Last update timestamp (ms since epoch) |
| current_snapshot_id | BIGINT | Current snapshot ID |
| current_schema_id | INTEGER | Current schema ID |
| default_spec_id | INTEGER | Default partition spec ID |
| row_count | BIGINT | Estimated total row count |
| file_count | BIGINT | Number of data files |

**Example:**

```sql
SELECT * FROM iceberg_metadata('/warehouse/sales');

-- Output:
-- format_version: 2
-- table_uuid: 550e8400-e29b-41d4-a716-446655440000
-- location: /warehouse/sales
-- last_updated_ms: 1700000000000
-- current_snapshot_id: 1234567890
-- current_schema_id: 0
-- default_spec_id: 0
-- row_count: 1000000
-- file_count: 10
```

### iceberg_snapshots

Returns the snapshot history for an Iceberg table.

```sql
iceberg_snapshots(path)
```

**Output Columns:**

| Column | Type | Description |
|--------|------|-------------|
| snapshot_id | BIGINT | Snapshot unique identifier |
| parent_snapshot_id | BIGINT | Parent snapshot ID (NULL for first snapshot) |
| timestamp_ms | BIGINT | Creation timestamp (ms since epoch) |
| operation | VARCHAR | Operation type (append, overwrite, delete, etc.) |
| manifest_list | VARCHAR | Path to manifest list file |
| summary | MAP | Additional snapshot metadata |

**Example:**

```sql
SELECT snapshot_id, timestamp_ms, operation
FROM iceberg_snapshots('/warehouse/sales')
ORDER BY timestamp_ms DESC;

-- Output:
-- snapshot_id  | timestamp_ms   | operation
-- 1234567893   | 1700007200000  | append
-- 1234567892   | 1700003600000  | append
-- 1234567891   | 1700000000000  | append
```

## Time Travel

Iceberg's snapshot-based architecture enables querying data as it existed at any point in time.

### By Snapshot ID

Query a specific snapshot by its unique identifier:

```sql
-- Get available snapshots
SELECT snapshot_id, timestamp_ms
FROM iceberg_snapshots('/warehouse/sales');

-- Query a specific snapshot
SELECT * FROM iceberg_scan('/warehouse/sales', snapshot_id := 1234567890);
```

### By Timestamp

Query data as it existed at a specific point in time:

```sql
-- Query data as of a specific timestamp
SELECT * FROM iceberg_scan('/warehouse/sales',
    timestamp := TIMESTAMP '2024-01-15 10:00:00');

-- Query yesterday's data
SELECT * FROM iceberg_scan('/warehouse/sales',
    timestamp := CURRENT_TIMESTAMP - INTERVAL '1 day');
```

When using timestamp-based time travel, dukdb-go selects the snapshot that was current at or before the specified timestamp.

### Go API Time Travel

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/dukdb/dukdb-go/internal/io/iceberg"
)

func main() {
    ctx := context.Background()

    // Open table
    table, err := iceberg.OpenTable(ctx, "/warehouse/sales", nil)
    if err != nil {
        panic(err)
    }
    defer table.Close()

    // List available snapshots
    for _, snap := range table.Snapshots() {
        fmt.Printf("Snapshot %d at %s\n",
            snap.SnapshotID,
            snap.Timestamp().Format(time.RFC3339))
    }

    // Query by snapshot ID
    snapshotID := int64(1234567890)
    reader, err := iceberg.NewReader(ctx, "/warehouse/sales", &iceberg.ReaderOptions{
        SnapshotID: &snapshotID,
    })
    if err != nil {
        panic(err)
    }
    defer reader.Close()

    // Query by timestamp
    ts := time.Now().Add(-24 * time.Hour).UnixMilli()
    reader, err = iceberg.NewReader(ctx, "/warehouse/sales", &iceberg.ReaderOptions{
        Timestamp: &ts,
    })
    if err != nil {
        panic(err)
    }
    defer reader.Close()
}
```

### Error Handling

If you request a snapshot or timestamp that does not exist, you will receive an error with helpful information:

```
iceberg: snapshot not found: snapshot ID 9999 (available: [1234567890, 1234567891, 1234567892])
```

```
iceberg: no snapshot at or before timestamp 2020-01-01T00:00:00Z
(available range: 2024-01-01T00:00:00Z to 2024-06-15T12:00:00Z)
```

## Partition Pruning

When an Iceberg table is partitioned, dukdb-go automatically prunes partitions that cannot contain matching data based on your query filters.

### Supported Partition Transforms

| Transform | Example | Description |
|-----------|---------|-------------|
| identity | `region` | Partition by exact value |
| bucket[N] | `bucket[16](id)` | Hash into N buckets |
| truncate[W] | `truncate[10](name)` | Truncate strings to W chars |
| year | `year(ts)` | Extract year from date/timestamp |
| month | `month(ts)` | Extract month from date/timestamp |
| day | `day(ts)` | Extract day from date/timestamp |
| hour | `hour(ts)` | Extract hour from timestamp |
| void | `void(col)` | No partitioning (null) |

### How Partition Pruning Works

```sql
-- Given a table partitioned by year(order_date) and region
SELECT * FROM iceberg_scan('/warehouse/orders')
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
  AND region = 'US';

-- dukdb-go will:
-- 1. Compute partition values: year=54 (2024-1970), region='US'
-- 2. Skip manifests that cannot contain matching partitions
-- 3. Skip data files that cannot contain matching partitions
-- 4. Read only relevant Parquet files
```

### Verifying Pruning

Check the scan plan to verify partition pruning:

```go
reader, _ := iceberg.NewReader(ctx, tablePath, opts)
reader.Schema() // Initialize scan plan

plan := reader.ScanPlan()
fmt.Printf("Total files: %d\n", plan.TotalRowCount)
fmt.Printf("Files after pruning: %d\n", len(plan.DataFiles))
fmt.Printf("Estimated rows: %d\n", plan.EstimatedRowCount)
```

## Column Projection

dukdb-go supports column projection to read only the columns you need, reducing I/O significantly for wide tables.

```sql
-- Only read id and name columns from a 50-column table
SELECT id, name FROM iceberg_scan('/warehouse/users');
```

The optimizer pushes column projection down to the Parquet reader, so only the required column data is read from disk.

### Go API Column Projection

```go
reader, err := iceberg.NewReader(ctx, "/warehouse/users", &iceberg.ReaderOptions{
    SelectedColumns: []string{"id", "name", "email"},
})
```

## Schema Evolution

Iceberg tracks schema changes using unique field IDs, enabling queries to work correctly even when:

- Columns are added (new columns return NULL for old data)
- Columns are dropped (queries on current schema work correctly)
- Columns are renamed (field IDs maintain identity)
- Column order changes (field IDs maintain identity)

### Querying After Schema Changes

```sql
-- Table originally had: id, name
-- Column 'email' was added later
-- Column 'old_status' was dropped

-- Query works - email is NULL for old rows
SELECT id, name, email FROM iceberg_scan('/warehouse/users');

-- Query fails - old_status no longer exists
SELECT old_status FROM iceberg_scan('/warehouse/users');  -- Error!
```

### Go API Schema Information

```go
table, _ := iceberg.OpenTable(ctx, "/warehouse/users", nil)

// Get all schemas
for _, schema := range table.Metadata().Schemas() {
    fmt.Printf("Schema ID %d: %d fields\n", schema.ID, len(schema.Fields()))
}

// Get current schema columns
columns, _ := table.SchemaColumns()
for _, col := range columns {
    fmt.Printf("  %s (%s, id=%d)\n", col.Name, col.Type, col.ID)
}
```

## Cloud Storage Access

dukdb-go supports reading Iceberg tables from cloud storage providers.

### Amazon S3

```sql
-- Create credentials
CREATE SECRET my_s3 (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

-- Read from S3
SELECT * FROM iceberg_scan('s3://my-bucket/warehouse/sales');
```

### Google Cloud Storage

```sql
-- Create credentials
CREATE SECRET my_gcs (
    TYPE GCS,
    SERVICE_ACCOUNT_JSON '/path/to/service-account.json'
);

-- Read from GCS
SELECT * FROM iceberg_scan('gs://my-bucket/warehouse/sales');
```

### Azure Blob Storage

```sql
-- Create credentials
CREATE SECRET my_azure (
    TYPE AZURE,
    ACCOUNT_NAME 'mystorageaccount',
    ACCOUNT_KEY 'your-account-key'
);

-- Read from Azure
SELECT * FROM iceberg_scan('azure://my-container/warehouse/sales');
```

### Go API with Cloud Storage

```go
import (
    "github.com/dukdb/dukdb-go/internal/io/filesystem"
    "github.com/dukdb/dukdb-go/internal/io/iceberg"
)

// Configure S3 filesystem
s3fs := filesystem.NewS3FileSystem(&filesystem.S3Config{
    Region:    "us-east-1",
    AccessKey: "AKIAIOSFODNN7EXAMPLE",
    SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
})

// Open table with custom filesystem
table, err := iceberg.OpenTable(ctx, "s3://my-bucket/warehouse/sales",
    &iceberg.TableOptions{
        Filesystem: s3fs,
    })
```

## Type Mapping

Iceberg types are automatically mapped to DuckDB types:

| Iceberg Type | DuckDB Type | Notes |
|--------------|-------------|-------|
| boolean | BOOLEAN | |
| int | INTEGER | 32-bit signed |
| long | BIGINT | 64-bit signed |
| float | FLOAT | 32-bit IEEE 754 |
| double | DOUBLE | 64-bit IEEE 754 |
| decimal(P,S) | DECIMAL(P,S) | Arbitrary precision |
| date | DATE | Days since 1970-01-01 |
| time | TIME | Microseconds since midnight |
| timestamp | TIMESTAMP | Microseconds since epoch |
| timestamptz | TIMESTAMPTZ | With timezone |
| string | VARCHAR | UTF-8 encoded |
| binary | BLOB | Arbitrary bytes |
| fixed[N] | BLOB | Fixed-length binary |
| uuid | UUID | 128-bit UUID |
| struct | STRUCT | Nested record |
| list | LIST | Array of elements |
| map | MAP | Key-value pairs |

## Performance Tips

### 1. Use Column Projection

Only select the columns you need:

```sql
-- Good: Only reads 3 columns
SELECT id, name, total FROM iceberg_scan('/warehouse/orders');

-- Bad: Reads all 50 columns
SELECT * FROM iceberg_scan('/warehouse/orders');
```

### 2. Filter on Partition Columns

Place filters on partition columns for automatic pruning:

```sql
-- Good: Partitioned by date, filter on date
SELECT * FROM iceberg_scan('/warehouse/orders')
WHERE order_date = '2024-01-15';

-- Less efficient: Filter on non-partition column
SELECT * FROM iceberg_scan('/warehouse/orders')
WHERE customer_id = 12345;
```

### 3. Use Row Limits for Exploration

When exploring data, use LIMIT:

```sql
-- Quick exploration
SELECT * FROM iceberg_scan('/warehouse/orders') LIMIT 100;
```

### 4. Check Metadata First

For large tables, check metadata before querying:

```sql
-- Check table size before querying
SELECT row_count, file_count FROM iceberg_metadata('/warehouse/orders');
```

### 5. Use Time Travel Wisely

Time travel queries read from potentially older, archived files. For production queries, prefer the current snapshot unless you specifically need historical data.

## Troubleshooting

### "iceberg: table not found"

The specified path does not contain a valid Iceberg table. Check:

1. The path is correct
2. The `metadata/` directory exists
3. There is at least one `*.metadata.json` file or `version-hint.text`

```bash
# Expected structure
/path/to/table/
  metadata/
    v1.metadata.json
    version-hint.text
  data/
    *.parquet
```

### "iceberg: no current snapshot"

The table exists but has no data (no snapshots). This happens with newly created, empty tables.

### "iceberg: snapshot not found"

The requested snapshot ID does not exist. Use `iceberg_snapshots()` to list available snapshots:

```sql
SELECT snapshot_id, timestamp_ms FROM iceberg_snapshots('/path/to/table');
```

### "iceberg: unsupported format version"

dukdb-go supports Iceberg format versions 1 and 2. Version 3 features are not yet supported.

### "iceberg: failed to read manifest"

A manifest file (AVRO format) could not be read. This might indicate:

1. Corrupted manifest file
2. Missing file in cloud storage
3. Permission issues

### Slow Queries

If queries are slower than expected:

1. Check partition pruning is working (compare `row_count` vs scanned rows)
2. Ensure column projection is being used
3. Verify cloud storage credentials are correct (avoid retry delays)
4. Check if the table has many small files (requires compaction)

## Compatibility Notes

### Supported Iceberg Features

| Feature | Status |
|---------|--------|
| Format v1 | Supported |
| Format v2 | Supported |
| Parquet data files | Supported |
| AVRO manifest files | Supported |
| Partition pruning | Supported |
| Column projection | Supported |
| Time travel (snapshot) | Supported |
| Time travel (timestamp) | Supported |
| Schema evolution (read) | Supported |
| Hidden partitioning | Supported |
| Bucket transforms | Supported |
| Temporal transforms | Supported |

### Limitations

| Feature | Status |
|---------|--------|
| Delete files (positional) | Not yet supported |
| Delete files (equality) | Not yet supported |
| ORC data files | Not supported |
| AVRO data files | Not supported |
| Write support | Not supported |
| REST catalog | Not yet supported |
| Merge-on-read | Not supported |

### DuckDB Compatibility

dukdb-go aims for compatibility with DuckDB's Iceberg extension. The main differences:

1. **Pure Go**: No CGO required, works in WebAssembly
2. **Simplified catalog**: Direct table access only (no catalog integration yet)
3. **Delete files**: Not yet implemented

Most read queries should produce identical results to DuckDB's Iceberg extension.

## See Also

- [Cloud Storage Documentation](cloud-storage.md) - Setting up cloud storage access
- [Secrets Management](secrets.md) - Managing cloud credentials
- [Migration Guide](iceberg-migration.md) - Migrating from DuckDB's Iceberg extension
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/) - Official Iceberg documentation

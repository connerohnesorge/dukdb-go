# Glob Pattern Support

dukdb-go provides comprehensive glob pattern support for reading multiple files in a single query. This feature is compatible with DuckDB and enables powerful bulk data loading scenarios.

## Table of Contents

- [Overview](#overview)
- [Pattern Syntax](#pattern-syntax)
- [Array Syntax](#array-syntax)
- [Metadata Columns](#metadata-columns)
- [Hive Partitioning](#hive-partitioning)
- [Schema Alignment (Union by Name)](#schema-alignment-union-by-name)
- [Cloud Storage Optimization](#cloud-storage-optimization)
- [Configuration Settings](#configuration-settings)
- [File Glob Behavior Options](#file-glob-behavior-options)
- [Error Handling](#error-handling)
- [Examples by File Format](#examples-by-file-format)
- [Go Integration](#go-integration)
- [DuckDB Compatibility](#duckdb-compatibility)

---

## Overview

Glob patterns allow you to read multiple files with a single query using wildcard characters. This is essential for:

- Reading partitioned datasets
- Loading data from multiple files
- Processing time-series data organized by date
- Working with data lakes and warehouses

All table functions support glob patterns:

- `read_csv()` / `read_csv_auto()`
- `read_json()` / `read_json_auto()` / `read_ndjson()`
- `read_parquet()`
- `read_xlsx()`
- `read_arrow()`

```sql
-- Read all CSV files in a directory
SELECT * FROM read_csv('data/*.csv');

-- Read Parquet files recursively
SELECT * FROM read_parquet('warehouse/**/*.parquet');

-- Read from cloud storage
SELECT * FROM read_csv('s3://bucket/data/*.csv');
```

---

## Pattern Syntax

dukdb-go supports standard glob pattern syntax compatible with DuckDB.

### Wildcards

| Pattern | Description | Example |
|---------|-------------|---------|
| `*` | Matches any sequence of characters within a single path segment | `*.csv` matches `data.csv`, `report.csv` |
| `**` | Matches any sequence of characters across directory levels (recursive) | `**/*.csv` matches `a.csv`, `dir/b.csv`, `dir/sub/c.csv` |
| `?` | Matches exactly one character | `file?.csv` matches `file1.csv`, `fileA.csv` |

### Character Classes

| Pattern | Description | Example |
|---------|-------------|---------|
| `[abc]` | Matches any single character in the set | `file[123].csv` matches `file1.csv`, `file2.csv`, `file3.csv` |
| `[a-z]` | Matches any single character in the range | `file[a-c].csv` matches `filea.csv`, `fileb.csv`, `filec.csv` |
| `[0-9]` | Matches any digit | `data_202[3-4].csv` matches `data_2023.csv`, `data_2024.csv` |
| `[!abc]` | Matches any character NOT in the set (negation) | `file[!0-9].csv` matches `filea.csv` but not `file1.csv` |
| `[!a-z]` | Matches any character NOT in the range | `data_[!a-z].csv` matches `data_1.csv` but not `data_a.csv` |

### Escape Sequences

Use backslash (`\`) to escape glob special characters:

```sql
-- Match literal asterisk in filename
SELECT * FROM read_csv('data/report\*.csv');

-- Match literal question mark
SELECT * FROM read_csv('data/file\?.csv');

-- Match literal brackets
SELECT * FROM read_csv('data/report\[2024\].csv');
```

### Pattern Restrictions

- **Single `**` per pattern**: A pattern can contain at most one `**` recursive wildcard
- **Invalid patterns**: Unclosed bracket expressions `[abc` will result in an error

```sql
-- Valid patterns
SELECT * FROM read_csv('data/**/*.csv');        -- OK
SELECT * FROM read_csv('logs/202[3-4]/*.json'); -- OK

-- Invalid patterns
SELECT * FROM read_csv('a/**/b/**/c.csv');      -- ERROR: multiple '**' wildcards
SELECT * FROM read_csv('data/[abc.csv');        -- ERROR: unclosed bracket expression
```

---

## Array Syntax

Instead of glob patterns, you can specify an explicit list of files using array syntax. This is useful when files do not follow a glob-friendly naming convention.

### Basic Array Syntax

```sql
-- List specific files
SELECT * FROM read_csv(['data/jan.csv', 'data/feb.csv', 'data/mar.csv']);

-- Mix files from different directories
SELECT * FROM read_parquet([
    'warehouse/sales/q1.parquet',
    'warehouse/sales/q2.parquet',
    'archive/sales/q3.parquet'
]);
```

### Arrays with Glob Patterns

Array elements can contain glob patterns:

```sql
-- Combine glob patterns in array
SELECT * FROM read_csv(['logs/2023/*.csv', 'logs/2024/january.csv']);

-- Multiple glob patterns
SELECT * FROM read_json([
    'events/user_*.json',
    'events/system_*.json'
]);
```

### Mixed Storage Backends

Arrays can contain files from different storage backends:

```sql
-- Local and cloud files in same query
SELECT * FROM read_parquet([
    'local_data.parquet',
    's3://bucket/remote_data.parquet'
]);

-- Multiple cloud providers
SELECT * FROM read_csv([
    's3://aws-bucket/data.csv',
    'gs://gcs-bucket/data.csv'
]);
```

### Deduplication

When using arrays, duplicate file paths are automatically removed:

```sql
-- These patterns may overlap
SELECT * FROM read_csv(['data/*.csv', 'data/important.csv']);
-- 'data/important.csv' will only be read once
```

---

## Metadata Columns

Virtual metadata columns provide information about which file each row originated from. Enable them using function options.

### Available Metadata Columns

| Option | Column Name | Type | Description |
|--------|-------------|------|-------------|
| `filename=true` | `filename` | VARCHAR | Full path of the source file |
| `file_row_number=true` | `file_row_number` | BIGINT | 1-indexed row number within the file |
| `file_index=true` | `file_index` | INTEGER | 0-indexed position in the sorted file list |

### Usage Examples

```sql
-- Add filename column
SELECT *, filename FROM read_csv('data/*.csv', filename=true);

-- Add row number within each file
SELECT *, file_row_number FROM read_parquet('data/*.parquet', file_row_number=true);

-- Add file index (order in sorted file list)
SELECT *, file_index FROM read_json('logs/*.json', file_index=true);

-- Combine all metadata columns
SELECT
    id,
    name,
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

### Example Output

Given files `data/users_a.csv` and `data/users_b.csv`:

```sql
SELECT * FROM read_csv('data/users_*.csv', filename=true, file_row_number=true);
```

| id | name | filename | file_row_number |
|----|------|----------|-----------------|
| 1 | Alice | data/users_a.csv | 1 |
| 2 | Bob | data/users_a.csv | 2 |
| 3 | Carol | data/users_b.csv | 1 |
| 4 | David | data/users_b.csv | 2 |

### Use Cases

```sql
-- Track data lineage
SELECT
    *,
    filename AS source_file,
    file_index AS load_order
FROM read_parquet('s3://bucket/daily/*.parquet', filename=true, file_index=true);

-- Filter by source file
SELECT * FROM read_csv('reports/*.csv', filename=true)
WHERE filename LIKE '%2024%';

-- Aggregate by file
SELECT
    filename,
    COUNT(*) AS row_count,
    SUM(amount) AS total_amount
FROM read_csv('sales/*.csv', filename=true)
GROUP BY filename;
```

---

## Hive Partitioning

Hive partitioning extracts column values from directory names in the path structure. This is a common pattern in data lakes.

### Directory Structure

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

### Enabling Hive Partitioning

```sql
-- Enable Hive partitioning
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true);
```

| product | amount | year | month |
|---------|--------|------|-------|
| Widget | 100.00 | 2023 | 01 |
| Gadget | 200.00 | 2023 | 02 |
| Widget | 150.00 | 2024 | 01 |

### Hive Partitioning Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `hive_partitioning` | BOOLEAN or 'auto' | false | Enable partition column extraction |
| `hive_types_autocast` | BOOLEAN | true | Automatically infer types for partition values |
| `hive_types` | MAP | NULL | Explicit type mapping for partition columns |

### Auto-Detection Mode

```sql
-- Automatically detect if paths contain Hive partitions
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning='auto');
```

In auto mode, dukdb-go examines file paths for `key=value` patterns and enables partitioning if found.

### Type Autocasting

When `hive_types_autocast=true` (default), partition values are automatically converted:

| Value Pattern | Inferred Type |
|---------------|---------------|
| Integers (e.g., "2024", "42") | INTEGER or BIGINT |
| Decimals (e.g., "3.14") | DOUBLE |
| "true" / "false" | BOOLEAN |
| All other values | VARCHAR |

```sql
-- With autocast (default): year and month become INTEGER
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true);
-- year: INTEGER, month: INTEGER

-- Without autocast: all values remain VARCHAR
SELECT * FROM read_parquet('data/**/*.parquet',
    hive_partitioning=true,
    hive_types_autocast=false
);
-- year: VARCHAR, month: VARCHAR
```

### Explicit Type Mapping

Specify exact types for partition columns:

```sql
SELECT * FROM read_csv(
    'data/**/*.csv',
    hive_partitioning=true,
    hive_types={'year': 'INTEGER', 'month': 'INTEGER', 'region': 'VARCHAR'}
);
```

### Filtering on Partition Columns

Partition columns can be used in WHERE clauses for efficient filtering:

```sql
-- Filter by partition columns
SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true)
WHERE year = 2024 AND month >= 6;
```

---

## Schema Alignment (Union by Name)

When reading multiple files with different schemas, dukdb-go uses union-by-name semantics to merge them.

### How It Works

Files are matched by column name (case-sensitive):

```
File1: id (INTEGER), name (VARCHAR)
File2: id (INTEGER), email (VARCHAR)
Result: id (INTEGER), name (VARCHAR), email (VARCHAR)
```

```sql
SELECT * FROM read_csv('data/*.csv', union_by_name=true);
```

| id | name | email |
|----|------|-------|
| 1 | Alice | NULL |
| 2 | Bob | NULL |
| 3 | NULL | carol@example.com |

### Behavior Rules

- **Column matching**: Columns are matched by name (case-sensitive)
- **Missing columns**: Filled with NULL values
- **Type widening**: Compatible types are automatically widened
- **Incompatible types**: Results in an error

### Type Widening

When columns have different but compatible types, the wider type is used:

| Source Types | Result Type |
|--------------|-------------|
| TINYINT + SMALLINT | SMALLINT |
| SMALLINT + INTEGER | INTEGER |
| INTEGER + BIGINT | BIGINT |
| FLOAT + DOUBLE | DOUBLE |
| INTEGER + DOUBLE | DOUBLE |
| TIMESTAMP_S + TIMESTAMP | TIMESTAMP |
| TIMESTAMP + TIMESTAMP_NS | TIMESTAMP_NS |

```sql
-- File1: value (INTEGER), File2: value (BIGINT)
-- Result: value (BIGINT)
SELECT * FROM read_csv('data/*.csv', union_by_name=true);
```

### Incompatible Types Error

```sql
-- File1: value (INTEGER), File2: value (VARCHAR)
SELECT * FROM read_csv('incompatible/*.csv');
-- ERROR: incompatible column types: cannot merge INTEGER with VARCHAR
```

### Disabling Union by Name

Use positional matching instead:

```sql
-- Match columns by position instead of name
SELECT * FROM read_csv('data/*.csv', union_by_name=false);
```

When `union_by_name=false`:
- All files must have the same number of columns
- Column names from the first file are used
- Columns are matched by position

### Files to Sniff

Control how many files are sampled for schema detection:

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

---

## Cloud Storage Optimization

dukdb-go optimizes glob pattern matching on cloud storage using prefix filtering.

### Prefix Extraction

For patterns like `s3://bucket/data/2024/**/*.parquet`, dukdb-go extracts the literal prefix `data/2024/` and uses it to filter the ListObjects API call, reducing the number of objects enumerated.

```sql
-- Only lists objects under 's3://bucket/warehouse/2024/'
SELECT * FROM read_parquet('s3://bucket/warehouse/2024/**/*.parquet');

-- Lists all objects (no prefix optimization possible)
SELECT * FROM read_parquet('s3://bucket/**/data.parquet');
```

### Pagination Handling

Cloud storage glob operations automatically handle pagination:

- **S3**: ListObjectsV2 pagination (1000 objects per page)
- **GCS**: Iterator-based pagination
- **Azure**: Blob list pagination

### Retry Logic

Cloud storage operations include retry logic with exponential backoff for:

- Rate limiting (HTTP 429)
- Transient errors
- Network timeouts

### Supported Cloud Providers

| Provider | URL Schemes | Glob Support |
|----------|-------------|--------------|
| Amazon S3 | `s3://`, `s3a://`, `s3n://` | Full support with prefix optimization |
| Google Cloud Storage | `gs://`, `gcs://` | Full support with prefix optimization |
| Azure Blob Storage | `azure://`, `az://` | Full support with prefix optimization |
| HTTP/HTTPS | `http://`, `https://` | No glob support (single file only) |

### Cloud Storage Examples

```sql
-- S3 with glob and Hive partitioning
SELECT * FROM read_parquet(
    's3://datalake/events/year=*/month=*/*.parquet',
    hive_partitioning=true
);

-- GCS recursive glob
SELECT * FROM read_csv('gs://bucket/logs/**/*.csv', filename=true);

-- Azure with character class
SELECT * FROM read_parquet('azure://container/data_202[3-4]/*.parquet');
```

---

## Configuration Settings

### max_files_per_glob

Limits the maximum number of files that can be matched by a single glob pattern.

| Setting | Default | Min | Max |
|---------|---------|-----|-----|
| `max_files_per_glob` | 10,000 | 1 | 1,000,000 |

```sql
-- Set via SQL
SET max_files_per_glob = 50000;

-- Check current value
SHOW max_files_per_glob;
```

```go
// Set via DSN
db, err := sql.Open("dukdb", ":memory:?max_files_per_glob=50000")
```

When a glob pattern matches more files than the limit:

```sql
SELECT * FROM read_parquet('s3://bucket/**/*.parquet');
-- ERROR: glob pattern matches too many files: 15000 files (limit: 10000)
```

### file_glob_timeout

Sets the timeout in seconds for cloud storage glob operations.

| Setting | Default | Min | Max |
|---------|---------|-----|-----|
| `file_glob_timeout` | 60 | 1 | 600 |

```sql
-- Set via SQL
SET file_glob_timeout = 120;

-- Check current value
SHOW file_glob_timeout;
```

```go
// Set via DSN
db, err := sql.Open("dukdb", ":memory:?file_glob_timeout=120")
```

### Combining Settings

```go
// DSN with multiple settings
db, err := sql.Open("dukdb", ":memory:?max_files_per_glob=25000&file_glob_timeout=300")
```

---

## File Glob Behavior Options

The `file_glob_behavior` option controls how empty glob results are handled.

| Value | Behavior |
|-------|----------|
| `'DISALLOW_EMPTY'` | Return an error when no files match (default) |
| `'ALLOW_EMPTY'` | Return an empty result set when no files match |
| `'FALLBACK_GLOB'` | Treat the pattern as a literal path if no matches found |

### DISALLOW_EMPTY (Default)

```sql
SELECT * FROM read_csv('data/*.csv');
-- If no files match: ERROR: no files match pattern: data/*.csv
```

### ALLOW_EMPTY

```sql
SELECT * FROM read_csv('data/*.csv', file_glob_behavior='ALLOW_EMPTY');
-- If no files match: Returns empty result set (no error)
```

Use case: Graceful handling when data may not exist yet.

### FALLBACK_GLOB

```sql
SELECT * FROM read_csv('file[1].csv', file_glob_behavior='FALLBACK_GLOB');
-- If no glob matches: Tries to read 'file[1].csv' as a literal filename
```

Use case: Filenames that contain glob characters (e.g., `report[2024].csv`).

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `no files match pattern` | Glob pattern matches zero files | Check pattern, verify files exist, or use `file_glob_behavior='ALLOW_EMPTY'` |
| `glob pattern matches too many files` | More than `max_files_per_glob` files matched | Use a more specific pattern, filter by partition, or increase limit |
| `incompatible column types` | Files have columns with conflicting types | Ensure column types are compatible or explicitly cast |
| `invalid glob pattern` | Malformed pattern syntax | Check bracket expressions are closed |
| `unclosed bracket expression` | Missing `]` in character class | Add closing bracket: `[a-z]` |
| `multiple '**' wildcards` | Pattern contains more than one `**` | Use only one recursive wildcard per pattern |
| `permission denied` | Cannot read directory or files | Check file permissions and credentials |

### Error Examples

```sql
-- No files match
SELECT * FROM read_csv('nonexistent/*.csv');
-- ERROR: no files match pattern: nonexistent/*.csv

-- Too many files
SELECT * FROM read_parquet('s3://bucket/**/*.parquet');
-- ERROR: glob pattern matches too many files: 15000 files (limit: 10000)

-- Invalid pattern
SELECT * FROM read_csv('data/[abc.csv');
-- ERROR: invalid glob pattern: unclosed bracket expression

-- Multiple recursive wildcards
SELECT * FROM read_csv('a/**/b/**/c.csv');
-- ERROR: pattern cannot contain multiple '**' wildcards

-- Type incompatibility
SELECT * FROM read_csv('mixed_types/*.csv');
-- ERROR: incompatible column types: cannot merge INTEGER with VARCHAR
```

### Handling Errors in Go

```go
rows, err := db.Query(`
    SELECT * FROM read_csv('s3://bucket/data/*.csv', file_glob_behavior='ALLOW_EMPTY')
`)
if err != nil {
    // Handle error (connection issues, permission denied, etc.)
    log.Printf("Query failed: %v", err)
    return
}
defer rows.Close()

// Check if any rows were returned
if !rows.Next() {
    log.Println("No data files found matching pattern")
    return
}
```

---

## Examples by File Format

### CSV

```sql
-- Basic glob
SELECT * FROM read_csv('data/*.csv');

-- With all features
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

-- Auto-detect with glob
SELECT * FROM read_csv_auto('reports/*.csv', filename=true);
```

### JSON

```sql
-- Read JSON array files
SELECT * FROM read_json('data/*.json', format='array');

-- Read NDJSON with glob
SELECT * FROM read_ndjson('logs/**/*.ndjson', filename=true);

-- JSON with Hive partitioning
SELECT
    timestamp,
    level,
    message,
    date
FROM read_json(
    'events/date=*/*.json',
    hive_partitioning=true,
    hive_types_autocast=true
);

-- Auto-detect with glob
SELECT * FROM read_json_auto('api_responses/*.json');
```

### Parquet

```sql
-- Basic glob
SELECT * FROM read_parquet('warehouse/*.parquet');

-- Partitioned dataset with column projection
SELECT
    id,
    name,
    year,
    month,
    file_index
FROM read_parquet(
    's3://datalake/table/year=202[34]/month=*/*.parquet',
    file_index=true,
    hive_partitioning=true,
    hive_types_autocast=true
);

-- Array of specific files
SELECT * FROM read_parquet([
    'gs://bucket/q1_2024.parquet',
    'gs://bucket/q2_2024.parquet',
    'gs://bucket/q3_2024.parquet'
]);
```

### XLSX

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

### Arrow

```sql
-- Read Arrow IPC files with glob
SELECT * FROM read_arrow('data/*.arrow', filename=true);

-- Array syntax
SELECT * FROM read_arrow([
    'batch1.arrow',
    'batch2.arrow'
]);
```

---

## Go Integration

### Basic Usage

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

    // Read multiple CSV files
    rows, err := db.Query(`
        SELECT * FROM read_csv('data/*.csv', filename=true)
        WHERE amount > 100
    `)
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    // Process results
    for rows.Next() {
        // ...
    }
}
```

### Configuration via DSN

```go
// Configure glob settings via DSN
db, err := sql.Open("dukdb", ":memory:?max_files_per_glob=50000&file_glob_timeout=120")
if err != nil {
    log.Fatal(err)
}
```

### Runtime Configuration

```go
// Configure at runtime
_, err = db.Exec("SET max_files_per_glob = 50000")
if err != nil {
    log.Fatal(err)
}

_, err = db.Exec("SET file_glob_timeout = 120")
if err != nil {
    log.Fatal(err)
}

// Query current settings
var maxFiles string
err = db.QueryRow("SHOW max_files_per_glob").Scan(&maxFiles)
```

### Error Handling

```go
rows, err := db.Query(`
    SELECT * FROM read_parquet('s3://bucket/**/*.parquet')
`)
if err != nil {
    // Check for specific error types
    if strings.Contains(err.Error(), "no files match pattern") {
        log.Println("No data files found")
        return
    }
    if strings.Contains(err.Error(), "too many files") {
        log.Println("Pattern matches too many files, use more specific pattern")
        return
    }
    log.Fatalf("Query failed: %v", err)
}
```

---

## DuckDB Compatibility

dukdb-go's glob pattern support is designed for compatibility with DuckDB. The following features match DuckDB behavior:

### Supported Features

- All glob wildcards (`*`, `**`, `?`, `[...]`, `[!...]`)
- Array of files syntax
- Metadata columns (`filename`, `file_row_number`, `file_index`)
- Hive partitioning with auto-detection and type inference
- Union-by-name schema merging
- File glob behavior options
- `files_to_sniff` option

### Differences from DuckDB

| Feature | dukdb-go | DuckDB |
|---------|----------|--------|
| Default `max_files_per_glob` | 10,000 | No limit |
| Default `file_glob_timeout` | 60 seconds | No timeout |
| `**` restriction | Single `**` per pattern | Single `**` per pattern |

### Migration from DuckDB

Queries using glob patterns should work without modification:

```sql
-- These queries work identically in dukdb-go and DuckDB
SELECT * FROM read_csv('data/*.csv');
SELECT * FROM read_parquet('s3://bucket/**/*.parquet', hive_partitioning=true);
SELECT * FROM read_json(['a.json', 'b.json'], filename=true);
```

---

## See Also

- [Cloud Storage Integration](cloud-storage.md) - Cloud storage configuration and authentication
- [Secrets Management](secrets.md) - Managing credentials for cloud storage
- [Extended Types](types.md) - Supported data types

# Iceberg Compatibility Matrix

This document provides a comprehensive compatibility comparison between dukdb-go's Iceberg implementation and DuckDB's C++ Iceberg extension.

## Overview

dukdb-go provides native Apache Iceberg table format support as a pure Go implementation. This enables:

- Cross-platform compilation without C toolchains
- WebAssembly deployment
- TinyGo compatibility
- Simplified dependency management

## Reference

DuckDB Iceberg extension entries from `extension_entries.hpp`:

| Entry | Type | Status in dukdb-go |
|-------|------|-------------------|
| `iceberg_metadata` | TABLE_FUNCTION_ENTRY | Supported |
| `iceberg_scan` | TABLE_FUNCTION_ENTRY | Supported |
| `iceberg_snapshots` | TABLE_FUNCTION_ENTRY | Supported |
| `iceberg_to_ducklake` | TABLE_FUNCTION_ENTRY | Not Implemented (write operation) |
| `unsafe_enable_version_guessing` | EXTENSION_SETTINGS | Different behavior (auto-detect) |
| `iceberg` | EXTENSION_SECRET_TYPES | Not Implemented (use programmatic config) |

## Function Compatibility

### iceberg_scan()

Primary table scanning function for reading Iceberg tables.

#### Signature Comparison

| Parameter | DuckDB | dukdb-go | Notes |
|-----------|--------|----------|-------|
| path | VARCHAR (required) | string (required) | Identical |
| snapshot_id | BIGINT | *int64 | Identical behavior |
| timestamp | TIMESTAMP | *int64 (ms epoch) | Different type, same behavior |
| allow_moved_paths | BOOLEAN | bool (stub) | Defined but not implemented |
| metadata_compression_codec | VARCHAR | string (auto) | Auto-detected in dukdb-go |
| version | VARCHAR | Not supported | Use snapshot_id instead |
| version_name_format | VARCHAR | Not supported | Auto-detected |
| mode | VARCHAR ('list') | Not supported | Use iceberg_snapshots() |

#### Usage Examples

**DuckDB:**
```sql
-- Basic scan
SELECT * FROM iceberg_scan('/path/to/table');

-- Time travel by snapshot ID
SELECT * FROM iceberg_scan('/path/to/table', snapshot_id := 1234567890);

-- Time travel by timestamp
SELECT * FROM iceberg_scan('/path/to/table') AS OF VERSION 1234567890;

-- With version parameter
SELECT * FROM iceberg_scan('/path/to/table', version := '3');

-- List mode
SELECT * FROM iceberg_scan('/path/to/table', mode := 'list');
```

**dukdb-go:**
```sql
-- Basic scan
SELECT * FROM iceberg_scan('/path/to/table');

-- Time travel by snapshot ID
SELECT * FROM iceberg_scan('/path/to/table', snapshot_id := 1234567890);

-- Time travel by timestamp
SELECT * FROM iceberg_scan('/path/to/table', timestamp := TIMESTAMP '2024-01-15 10:00:00');

-- List snapshots (alternative to mode := 'list')
SELECT * FROM iceberg_snapshots('/path/to/table');
```

### iceberg_metadata()

Returns manifest and data file metadata for an Iceberg table.

#### Output Schema

| Column | DuckDB Type | dukdb-go Type | Status |
|--------|-------------|---------------|--------|
| manifest_path | VARCHAR | VARCHAR | Supported |
| manifest_sequence_number | BIGINT | BIGINT | Supported |
| manifest_content | VARCHAR | INTEGER (enum) | Different representation |
| status | VARCHAR | VARCHAR | Supported |
| content | VARCHAR | VARCHAR | Supported |
| file_path | VARCHAR | VARCHAR | Supported |
| file_format | VARCHAR | VARCHAR | Supported |
| spec_id | INTEGER | INTEGER | Supported |
| record_count | BIGINT | BIGINT | Supported |
| file_size_in_bytes | BIGINT | BIGINT | Supported |
| partition | STRUCT | MAP | Different type |
| null_value_counts | MAP | MAP | Supported |
| nan_value_counts | MAP | MAP | Supported |
| lower_bounds | MAP | MAP | Supported |
| upper_bounds | MAP | MAP | Supported |

### iceberg_snapshots()

Returns snapshot history for an Iceberg table.

#### Output Schema

| Column | DuckDB Type | dukdb-go Type | Status |
|--------|-------------|---------------|--------|
| sequence_number | BIGINT | BIGINT | Supported |
| snapshot_id | BIGINT | BIGINT | Supported |
| timestamp_ms | BIGINT | BIGINT | Supported |
| manifest_list | VARCHAR | VARCHAR | Supported |
| summary | MAP | MAP | Supported |

## Iceberg Specification Support

### Format Versions

| Version | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| v1 | Supported | Supported | Full read support |
| v2 | Supported | Supported | Full read support including delete files |

### Metadata Features

| Feature | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| metadata.json parsing | Supported | Supported | |
| version-hint.text | Required (unless guessing enabled) | Auto-detected | Different behavior |
| Snapshot log | Supported | Supported | |
| Metadata log | Supported | Supported | |
| Table properties | Supported | Supported | |

### Schema Features

| Feature | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| Schema evolution | Supported | Partial | Added columns work, renames partial |
| Column ID tracking | Supported | Supported | |
| Multiple schemas | Supported | Supported | |
| Required fields | Supported | Supported | |
| Optional fields | Supported | Supported | |
| Nested types | Supported | Supported | |

### Partition Transforms

| Transform | DuckDB | dukdb-go | Notes |
|-----------|--------|----------|-------|
| identity | Supported | Supported | |
| bucket[N] | Supported | Supported | Uses murmur3 hash |
| truncate[W] | Supported | Supported | |
| year | Supported | Supported | |
| month | Supported | Supported | |
| day | Supported | Supported | |
| hour | Supported | Supported | |
| void | Supported | Supported | |

### Data File Formats

| Format | DuckDB | dukdb-go | Notes |
|--------|--------|----------|-------|
| Parquet | Supported | Supported | Primary format |
| Avro | Supported | Not Supported | Use Parquet |
| ORC | Supported | Not Supported | Use Parquet |

### Delete File Support

| Feature | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| Positional deletes | Supported | Supported | Rows excluded by file_path and pos |
| Equality deletes | Supported | Supported | Rows excluded by column value matching |
| Delete file pruning | Supported | Partial | Delete files loaded per data file |

**Note:** Delete file support is fully implemented. Tables with delete files will return correct results with deleted rows excluded.

## Cloud Storage Support

### Supported Protocols

| Protocol | DuckDB | dukdb-go | Notes |
|----------|--------|----------|-------|
| Local filesystem | Supported | Supported | |
| s3:// | Supported | Supported | Via filesystem abstraction |
| gs:// | Supported | Supported | Via filesystem abstraction |
| azure:// | Supported | Supported | Via filesystem abstraction |
| http(s):// | Supported | Partial | Basic support |

### Credential Configuration

| Method | DuckDB | dukdb-go | Notes |
|--------|--------|----------|-------|
| CREATE SECRET | Supported | Not Supported | Use programmatic config |
| Environment variables | Supported | Supported | AWS_*, GCS_*, AZURE_* |
| Programmatic API | N/A | Supported | TableOptions.Filesystem |
| IAM role (AWS) | Supported | Supported | Via AWS SDK |
| Workload identity (GCP) | Supported | Supported | Via GCP SDK |

### Configuration Example

**DuckDB:**
```sql
CREATE SECRET iceberg_s3 (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

SELECT * FROM iceberg_scan('s3://bucket/table');
```

**dukdb-go:**
```go
import (
    "github.com/dukdb/dukdb-go/internal/io/iceberg"
    "github.com/dukdb/dukdb-go/internal/io/filesystem"
)

// Configure S3 filesystem
fs := filesystem.NewS3FileSystem(&filesystem.S3Config{
    Region:          "us-east-1",
    AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
    SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
})

// Open table with configured filesystem
reader, err := iceberg.NewReader(ctx, "s3://bucket/table", &iceberg.ReaderOptions{
    Filesystem: fs,
})
```

## Catalog Support

### REST Catalog

| Feature | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| REST catalog API | Supported | Not Supported | Use direct table paths |
| OAuth2 authentication | Supported | Not Supported | Future work |
| Table listing | Supported | Not Supported | |
| Namespace support | Supported | Not Supported | |

### Other Catalogs

| Catalog | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| Hive Metastore | Supported | Not Supported | Use direct paths |
| AWS Glue | Supported | Not Supported | Use direct paths |
| Unity Catalog | Supported | Not Supported | Use direct paths |

## Time Travel

### Supported Methods

| Method | DuckDB | dukdb-go | Syntax |
|--------|--------|----------|--------|
| Snapshot ID | Supported | Supported | `snapshot_id := N` |
| Timestamp | Supported | Supported | `timestamp := TIMESTAMP '...'` |
| Version (metadata) | Supported | Not Supported | Use snapshot_id |
| AS OF VERSION | Supported | Not Supported | Use snapshot_id option |
| AS OF TIMESTAMP | Supported | Not Supported | Use timestamp option |

### Behavioral Differences

1. **Timestamp Resolution:**
   - DuckDB: Uses `AS OF TIMESTAMP` clause
   - dukdb-go: Uses milliseconds since epoch as parameter

2. **Snapshot Selection:**
   - Both implementations select the snapshot that was current at or before the specified timestamp

## Error Messages

| Scenario | DuckDB Message | dukdb-go Message |
|----------|----------------|------------------|
| Snapshot not found | `Invalid Error: Iceberg snapshot with version N not found` | `iceberg: snapshot not found: snapshot ID N (available: [...])` |
| Table not found | `IO Error: Could not read iceberg metadata file` | `iceberg: table not found: ...` |
| Invalid timestamp | `Invalid Error: No snapshot found for timestamp` | `iceberg: no snapshot at or before timestamp T (available range: T1 to T2)` |
| Unsupported format | `Invalid Error: Unsupported Iceberg format version` | `iceberg: unsupported format version` |

## Performance Considerations

| Aspect | DuckDB | dukdb-go | Notes |
|--------|--------|----------|-------|
| Metadata caching | Yes | No | Re-reads each query |
| Parallel file reading | Yes | Limited | Sequential by default |
| Predicate pushdown | Yes | Yes | Via partition pruning |
| Column projection | Yes | Yes | Via Parquet reader |
| Statistics pruning | Yes | Partial | Min/max bounds only |

## Known Limitations

### Critical Limitations

1. **Write Operations Not Supported**
   - Cannot create new Iceberg tables
   - Cannot append data to existing tables
   - Cannot perform compaction or maintenance

### Functional Limitations

1. **Avro/ORC Data Files**
   - Only Parquet data files are supported
   - Tables using Avro or ORC must be converted

2. **REST Catalog**
   - Must use direct table paths
   - No catalog-level table discovery

3. **Version Parameter**
   - Cannot specify metadata file version directly
   - Use snapshot_id for time travel instead

### Performance Limitations

1. **No Metadata Caching**
   - Metadata re-read on each query
   - Consider caching Table objects in application code

2. **Sequential File Reading**
   - Files read sequentially by default
   - Parallel reading requires application-level implementation

## Future Work

The following features are planned for future releases:

1. **REST Catalog Integration** - Query Iceberg catalogs directly
2. **Write Support** - Create and append to Iceberg tables
3. **Version Parameter** - Match DuckDB's version syntax
4. **Metadata Caching** - Improve repeated query performance
5. **Parallel File Reading** - Utilize multiple cores for large tables
6. **Avro/ORC Data File Support** - Read non-Parquet data files

## Migration Guide

For detailed migration instructions, see [Iceberg Migration Guide](iceberg-migration.md).

## See Also

- [Iceberg User Guide](iceberg.md) - Complete dukdb-go Iceberg documentation
- [Cloud Storage Configuration](cloud-storage.md) - Setting up cloud storage access
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html) - Official DuckDB documentation
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/) - Official Iceberg spec

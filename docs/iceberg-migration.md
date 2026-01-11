# Migrating from DuckDB's Iceberg Extension

This guide helps users migrate from DuckDB's C++ Iceberg extension to dukdb-go's pure Go implementation.

## Overview

dukdb-go provides native Iceberg support without requiring CGO or the DuckDB Iceberg extension. This enables:

- Cross-platform compilation without C toolchains
- WebAssembly deployment
- TinyGo compatibility
- Simplified dependency management

## API Compatibility

### Table Scanning

| DuckDB Iceberg Extension | dukdb-go | Notes |
|--------------------------|----------|-------|
| `iceberg_scan('path')` | `iceberg_scan('path')` | Identical |
| `iceberg_scan('path', version := 'N')` | Not yet supported | Use snapshot_id instead |
| `iceberg_scan('path', version_name_format := 'v%s.metadata.json')` | Not yet supported | Automatic detection |
| `iceberg_scan('path', allow_moved_paths := true)` | Not yet supported | |
| `iceberg_scan('path', mode := 'list')` | Not supported | Use iceberg_snapshots() |

### Metadata Functions

| DuckDB Iceberg Extension | dukdb-go | Notes |
|--------------------------|----------|-------|
| `iceberg_metadata('path')` | `iceberg_metadata('path')` | Similar output |
| `iceberg_snapshots('path')` | `iceberg_snapshots('path')` | Similar output |

### Time Travel

| DuckDB Iceberg Extension | dukdb-go | Notes |
|--------------------------|----------|-------|
| `SELECT * FROM iceberg_scan('path') AS OF VERSION N` | `SELECT * FROM iceberg_scan('path', snapshot_id := N)` | Use snapshot_id option |
| Time travel syntax in FROM | Options parameter | Different syntax |

## Syntax Differences

### DuckDB Iceberg Extension

```sql
-- DuckDB: Time travel with AS OF VERSION
SELECT * FROM iceberg_scan('/warehouse/sales') AS OF VERSION 1234567890;

-- DuckDB: Version parameter
SELECT * FROM iceberg_scan('/warehouse/sales', version := '3');

-- DuckDB: Allow moved paths
SELECT * FROM iceberg_scan('/warehouse/sales', allow_moved_paths := true);

-- DuckDB: List mode
SELECT * FROM iceberg_scan('/warehouse/sales', mode := 'list');
```

### dukdb-go

```sql
-- dukdb-go: Time travel with snapshot_id option
SELECT * FROM iceberg_scan('/warehouse/sales', snapshot_id := 1234567890);

-- dukdb-go: Time travel with timestamp
SELECT * FROM iceberg_scan('/warehouse/sales',
    timestamp := TIMESTAMP '2024-01-15 10:00:00');

-- dukdb-go: List snapshots (instead of mode := 'list')
SELECT * FROM iceberg_snapshots('/warehouse/sales');
```

## Feature Comparison

### Fully Supported Features

| Feature | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| Read Parquet files | Yes | Yes | Identical behavior |
| Column projection | Yes | Yes | Identical behavior |
| Partition pruning | Yes | Yes | Identical behavior |
| Format v1 | Yes | Yes | |
| Format v2 | Yes | Yes | |
| Time travel (snapshot ID) | Yes | Yes | Different syntax |
| Time travel (timestamp) | Yes | Yes | Different syntax |
| Schema evolution | Yes | Yes | |
| S3 storage | Yes | Yes | |
| GCS storage | Yes | Yes | |
| Azure storage | Yes | Yes | |

### Partially Supported Features

| Feature | DuckDB | dukdb-go | Notes |
|---------|--------|----------|-------|
| Version parameter | Yes | Snapshot ID only | Use snapshot_id instead of version |
| Metadata compression | Yes | Auto-detect | |
| Version hint | Yes | Auto-detect | |

### Not Yet Supported

| Feature | DuckDB | dukdb-go | Workaround |
|---------|--------|----------|------------|
| Delete files | Yes | No | Filter in query |
| ORC data files | Yes | No | Convert to Parquet |
| AVRO data files | Yes | No | Convert to Parquet |
| REST catalog | Yes | No | Direct table paths |
| allow_moved_paths | Yes | No | Update metadata |
| version_name_format | Yes | No | Automatic detection |

## Migration Steps

### 1. Update Import Statements

```go
// Before (with go-duckdb)
import _ "github.com/marcboeker/go-duckdb"

// After (with dukdb-go)
import (
    _ "github.com/dukdb/dukdb-go"
    _ "github.com/dukdb/dukdb-go/internal/engine"
)
```

### 2. Update Query Syntax

```go
// Before: DuckDB time travel
db.Query(`SELECT * FROM iceberg_scan('/path') AS OF VERSION 1234567890`)

// After: dukdb-go time travel
db.Query(`SELECT * FROM iceberg_scan('/path', snapshot_id := 1234567890)`)
```

### 3. Replace List Mode with iceberg_snapshots

```go
// Before: List mode
db.Query(`SELECT * FROM iceberg_scan('/path', mode := 'list')`)

// After: Use iceberg_snapshots
db.Query(`SELECT * FROM iceberg_snapshots('/path')`)
```

### 4. Handle Delete Files

If your Iceberg tables use delete files, you will need to filter results manually until delete file support is added:

```sql
-- Tables with positional deletes: filter manually
SELECT * FROM iceberg_scan('/warehouse/sales')
WHERE id NOT IN (SELECT deleted_id FROM deleted_records);
```

## Go API Migration

### Opening Tables

```go
// Before: Through SQL
rows, _ := db.Query(`SELECT * FROM iceberg_scan('/path')`)

// After: Direct API (optional, for more control)
import "github.com/dukdb/dukdb-go/internal/io/iceberg"

table, err := iceberg.OpenTable(ctx, "/path", nil)
if err != nil {
    // Handle error
}
defer table.Close()

// Get metadata
fmt.Printf("Snapshots: %d\n", len(table.Snapshots()))
fmt.Printf("Current: %d\n", table.CurrentSnapshot().SnapshotID)
```

### Reading with Options

```go
// Direct reader API for maximum control
reader, err := iceberg.NewReader(ctx, "/path", &iceberg.ReaderOptions{
    SelectedColumns: []string{"id", "name"},
    SnapshotID:      &snapshotID,
    Limit:           1000,
})
if err != nil {
    // Handle error
}
defer reader.Close()

// Read chunks
for {
    chunk, err := reader.ReadChunk()
    if err == io.EOF {
        break
    }
    // Process chunk
}
```

## Performance Comparison

In general, dukdb-go provides similar performance to DuckDB's Iceberg extension for read operations:

| Operation | DuckDB | dukdb-go | Notes |
|-----------|--------|----------|-------|
| Metadata parsing | Similar | Similar | Both parse JSON metadata |
| Manifest reading | Similar | Similar | Both use AVRO |
| Parquet reading | Native C++ | Pure Go | Go slightly slower on very large files |
| Partition pruning | Similar | Similar | Same algorithm |
| Column projection | Native C++ | Pure Go | Similar I/O reduction |

For most workloads, the difference is negligible. The main advantage of dukdb-go is portability and simplified deployment.

## Error Message Differences

### Snapshot Not Found

DuckDB:
```
Error: Invalid Error: Iceberg snapshot with version 9999 not found
```

dukdb-go:
```
iceberg: snapshot not found: snapshot ID 9999 (available: [1234567890, 1234567891])
```

### Table Not Found

DuckDB:
```
Error: IO Error: Could not read iceberg metadata file
```

dukdb-go:
```
iceberg: table not found: open /path/metadata/v1.metadata.json: no such file or directory
```

### Invalid Timestamp

DuckDB:
```
Error: Invalid Error: No snapshot found for timestamp
```

dukdb-go:
```
iceberg: no snapshot at or before timestamp 2020-01-01T00:00:00Z
(available range: 2024-01-01T00:00:00Z to 2024-06-15T12:00:00Z)
```

## Known Differences

### 1. Delete File Handling

DuckDB applies delete files automatically. dukdb-go does not yet support delete files. If your tables have delete files, query results may include rows that should be deleted.

Check for delete files:
```sql
SELECT * FROM iceberg_snapshots('/path');
-- Check the summary column for 'deleted-data-files' or 'deleted-rows'
```

### 2. Metadata Caching

DuckDB caches Iceberg metadata. dukdb-go currently re-reads metadata for each query. For repeated queries on the same table, consider using the Go API to open the table once.

### 3. Catalog Integration

DuckDB integrates Iceberg tables into the catalog. dukdb-go requires explicit paths to table locations. There is no catalog-level table discovery yet.

## Getting Help

If you encounter issues migrating from DuckDB's Iceberg extension:

1. Check the [Iceberg documentation](iceberg.md) for supported features
2. Verify your table format version (v1 or v2)
3. Check for delete files that may cause differences
4. Compare metadata output between DuckDB and dukdb-go

## Future Roadmap

Features planned for future releases:

1. **Delete file support** - Positional and equality deletes
2. **REST catalog integration** - Query Iceberg catalogs
3. **Write support** - Create and append to Iceberg tables
4. **Version parameter** - Match DuckDB's version syntax
5. **allow_moved_paths** - Support relocated tables

## See Also

- [Iceberg User Guide](iceberg.md) - Complete dukdb-go Iceberg documentation
- [Cloud Storage](cloud-storage.md) - Configuring cloud storage access
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html) - Official DuckDB documentation

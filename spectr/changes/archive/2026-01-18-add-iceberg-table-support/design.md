## Context

Apache Iceberg is a high-performance open table format for huge analytic datasets. Unlike simple file formats (Parquet, ORC), Iceberg is a *table format* that includes:
- Metadata layer (table properties, schemas, partitions)
- Data layer (files organized in manifests)
- Transaction log (snapshot history, commits)

Key Iceberg concepts:
- **Table**: Located at a path, contains metadata pointer
- **Metadata**: Stored in `metadata.json`, points to current snapshot
- **Snapshot**: Point-in-time table state, points to manifest list
- **Manifest List**: List of manifests (data file groups)
- **Manifest**: List of data files with partition values and column stats
- **Data File**: Parquet/ORC file with actual data

## Goals / Non-Goals

### Goals
- Read existing Iceberg tables from S3/GCS/HDFS/local
- Support time travel queries (`AS OF TIMESTAMP`, `AS OF SNAPSHOT`, `AT (VERSION => ...)`)
- Support schema evolution queries (new columns added)
- Implement partition pruning using Iceberg partition specs
- Implement column projection using Iceberg column stats
- Provide `iceberg_metadata()` and `iceberg_snapshots()` for metadata discovery
- Support version selection parameters (version, allow_moved_paths, metadata_compression_codec)
- Support version guessing for tables without version-hint.text
- Handle delete files (positional and equality deletes)
- Support REST catalog with OAuth2 authentication

### Non-Goals
- Full Iceberg write support (INSERT, UPDATE, DELETE)
- Iceberg branch and tag management (future work)
- Merge-on-read optimization (future work)
- Compaction and data layout optimization
- Iceberg-specific DDL (CREATE TABLE LIKE ICEBERG)

## Technical Approach

### Option 1: Pure Go Iceberg Library

**Pros**:
- Maintains pure Go requirement
- WASM and TinyGo compatible
- No CGO dependencies

**Cons**:
- No mature pure Go Iceberg libraries exist
- Iceberg spec is complex (metadata, manifests, snapshots)
- Significant implementation effort

**Libraries to evaluate**:
- `github.com/apache/iceberg-go` (if exists)
- `github.com/tabular-io/iceberg-go` (if exists)
- Custom implementation (starting with read-only)

**Effort**: 9-12 months for read-only implementation (full DuckDB v1.4.3 feature parity)

### Option 2: Hybrid Approach (Recommended)

Read Iceberg metadata and leverage existing Parquet reader:
1. Parse Iceberg metadata (pure Go)
2. Use existing Parquet reader for data files
3. Filter using Iceberg column stats and partition specs
4. Return results as DuckDB query results

This approach:
- Minimizes new code (reuses Parquet reader)
- Maintains pure Go
- Provides read-only access (most common use case)
- Can be extended for write support later

### Decision: Option 2 (Hybrid Approach)

Start with read-only access using existing Parquet infrastructure.

## Implementation Plan

### Phase 1: Iceberg Metadata Parser

```
internal/io/iceberg/
├── metadata.go          # Parse metadata.json
├── snapshot.go          # Snapshot management
├── manifest.go          # Manifest file parsing (AVRO)
├── manifest_file.go     # Manifest file entries
├── partition.go         # Partition spec and transform
├── schema.go            # Iceberg schema (different from DuckDB)
├── delete_file.go       # Delete file handling (positional, equality)
└── avro.go              # AVRO parser for manifest lists/files
```

### Phase 2: Iceberg Data Reader

```
internal/io/iceberg/
├── reader.go            # Main Iceberg table reader
├── scan.go              # Scan planning with push-down
├── filter.go            # Partition and column stats filtering
├── time_travel.go       # Snapshot selection by timestamp/snapshot_id
├── version_select.go    # Version selection parameters
├── version_guess.go     # Version guessing for missing hint
└── delete_applier.go    # Apply delete files to data
```

### Phase 3: Integration

```
internal/catalog/
├── iceberg_catalog.go   # Catalog integration for Iceberg tables
├── iceberg_metadata.go  # iceberg_metadata() function
└── iceberg_snapshots.go # iceberg_snapshots() function

internal/executor/
└── iceberg_scan.go      # Physical operator for Iceberg scan
```

### Phase 4: Write Support (Future)

- `COPY TO` with Iceberg format (Parquet + Iceberg metadata)
- Table creation with Iceberg format option

## Time Travel Syntax

Iceberg time travel queries follow this pattern:

```sql
-- By timestamp
SELECT * FROM iceberg_table AS OF TIMESTAMP '2024-01-15 10:00:00';

-- By snapshot ID
SELECT * FROM iceberg_table AS OF SNAPSHOT 1234567890;

-- By snapshot reference (branch or tag)
SELECT * FROM iceberg_table AS OF BRANCH main;

-- Version selection parameter (explicit metadata version)
SELECT * FROM iceberg_scan('s3://bucket/table/', version => 3);

-- Allow moved paths (for relocated tables)
SELECT * FROM iceberg_scan('s3://bucket/table/', allow_moved_paths => true);

-- Version guessing for tables without version-hint.text
SELECT * FROM iceberg_scan('s3://bucket/table/');
-- or with unsafe flag
SET unsafe_enable_version_guessing = true;
```

## Partition Pruning

Iceberg partition pruning works differently from simple file formats:
1. Partition spec defines transform functions (identity, bucket, truncate, year/month/day/hour)
2. Partition values are stored in manifest (not in data files)
3. Query predicates on partition columns can skip entire manifests

Implementation:
```go
// For query: WHERE date = '2024-01-15'
// And partition spec: dt (identity on date)
pruner.EvaluatePartitionFilter("date = '2024-01-15'")
// Returns: skip all manifests where partition_date != '2024-01-15'
```

## Schema Evolution Handling

Iceberg supports schema evolution without breaking existing data:

| Evolution Type | Description | Query Behavior |
|----------------|-------------|----------------|
| ADD COLUMN | New columns added | Missing columns = NULL |
| DROP COLUMN | Columns removed | Dropped columns excluded |
| RENAME COLUMN | Column renamed | New name maps to old |
| TYPE PROMOTE | INT → LONG | Automatic promotion |

## Open Questions

1. Should we implement a pure Go Iceberg library or use existing (if available)?
2. What's the minimum Iceberg spec version to support (v1, v2)?
3. Should we support Iceberg's/delete vectors (row-level delete)?
4. How to handle Iceberg's positional deletes vs. regular deletes?

## References

- Iceberg Spec: https://iceberg.apache.org/spec/
- Iceberg Java: https://github.com/apache/iceberg (reference implementation)
- DuckDB Iceberg: https://duckdb.org/docs/data/iceberg

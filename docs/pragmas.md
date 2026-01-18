# PRAGMA Settings

dukdb-go provides PRAGMA commands for configuring database behavior and accessing system information. This guide documents the supported PRAGMA settings, with a focus on configuration pragmas that control runtime behavior.

## Overview

PRAGMAs are special SQL commands that control database settings and behavior. dukdb-go supports several categories of PRAGMAs:

- **Configuration PRAGMAs**: Set and query database settings (e.g., `checkpoint_threshold`)
- **Information PRAGMAs**: Query system and database information
- **Maintenance PRAGMAs**: Perform database maintenance operations (e.g., CHECKPOINT)

## Configuration PRAGMAs

### PRAGMA checkpoint_threshold

Controls when automatic checkpoints are triggered based on WAL (Write-Ahead Log) file size. This is a critical tuning parameter for production workloads and allows you to balance between data durability, recovery speed, and I/O performance.

#### Purpose

The checkpoint threshold determines the maximum size the WAL file is allowed to reach before an automatic checkpoint is triggered. When the WAL file exceeds this threshold, dukdb-go will write all pending changes to the main database file and clear the WAL, freeing up disk space.

- **Smaller thresholds**: More frequent checkpoints = more I/O, faster recovery
- **Larger thresholds**: Less frequent checkpoints = less I/O, longer recovery time

#### Syntax

```sql
-- Set the checkpoint threshold
PRAGMA checkpoint_threshold = '256MB';

-- Query the current threshold
PRAGMA checkpoint_threshold;
```

#### Default Value

The default checkpoint threshold is **256MB** (megabytes).

#### Supported Formats and Suffixes

The threshold can be specified using the following size suffixes (case-insensitive):

| Suffix | Size |
|--------|------|
| `b` | bytes |
| `kb` | kilobytes (1,024 bytes) |
| `mb` | megabytes (1,024 KB) |
| `gb` | gigabytes (1,024 MB) |

Plain numbers without a suffix are treated as **bytes**.

#### Valid Range

| Limit | Value |
|-------|-------|
| Minimum | 1KB (1024 bytes) |
| Recommended Minimum | 1MB (1,048,576 bytes) |
| Default | 256MB (268,435,456 bytes) |
| No Maximum | Limited only by available disk space |

#### Examples

```sql
-- Set threshold to different sizes

-- 1024 bytes (not recommended, too small)
PRAGMA checkpoint_threshold = '1024b';

-- 512 kilobytes
PRAGMA checkpoint_threshold = '512kb';

-- 256 megabytes (default)
PRAGMA checkpoint_threshold = '256mb';

-- 1 gigabyte (large databases)
PRAGMA checkpoint_threshold = '1gb';

-- Plain number (treated as bytes)
PRAGMA checkpoint_threshold = '268435456';  -- 256MB in bytes

-- Query current setting
SELECT * FROM duckdb_settings WHERE name = 'checkpoint_threshold';

-- Query via PRAGMA
PRAGMA checkpoint_threshold;
```

#### Persistence

The checkpoint threshold setting is persisted in the `duckdb.settings` table and survives database restarts. When you reopen a database, it will use the previously configured threshold.

```sql
-- Open database with default 256MB threshold
CREATE DATABASE mydb;

-- Change threshold
PRAGMA checkpoint_threshold = '512MB';

-- Close and reopen database
-- The 512MB threshold is restored automatically
```

#### Trade-offs and Recommendations

##### Small/Development Workloads (64MB - 128MB)

Use a smaller threshold for:
- Development and testing environments
- Systems with limited disk space
- Applications requiring fast recovery time

```sql
PRAGMA checkpoint_threshold = '64MB';
```

**Trade-offs**:
- More frequent I/O operations
- Faster recovery after crashes
- Lower peak disk usage
- Slightly higher CPU overhead

##### Standard Workloads (256MB - 512MB)

Use the default or slightly larger threshold for:
- General-purpose databases
- Mixed read/write workloads
- Systems with moderate disk space

```sql
-- Use default
PRAGMA checkpoint_threshold = '256MB';

-- Or slightly larger
PRAGMA checkpoint_threshold = '512MB';
```

**Trade-offs**:
- Balanced I/O and recovery performance
- Reasonable peak disk usage
- Good for most applications

##### Large/High-Performance Workloads (1GB - 4GB)

Use a larger threshold for:
- High-throughput write-heavy workloads
- Systems with large disk capacity
- Applications where recovery time is less critical than I/O performance

```sql
PRAGMA checkpoint_threshold = '1GB';
PRAGMA checkpoint_threshold = '2GB';
PRAGMA checkpoint_threshold = '4GB';
```

**Trade-offs**:
- Fewer I/O operations (better write throughput)
- Longer recovery time after crashes
- Higher peak disk usage
- Better overall throughput for bulk loading

##### Disk I/O Constrained Systems (2GB+)

For systems where I/O throughput is the bottleneck:

```sql
-- Very large threshold to minimize checkpoint frequency
PRAGMA checkpoint_threshold = '4GB';
```

**Trade-offs**:
- Minimal I/O operations
- Maximum write throughput
- Significantly longer recovery after crashes
- Requires sufficient disk space

#### Monitoring Checkpoint Behavior

You can monitor checkpoint activity and WAL size:

```sql
-- View database size information
PRAGMA database_size;

-- Check current WAL size (implementation-dependent)
SELECT * FROM duckdb_functions() WHERE function_name LIKE '%checkpoint%';

-- View settings table
SELECT * FROM duckdb_settings WHERE name LIKE '%checkpoint%';
```

#### Common Use Cases

##### Case 1: Optimizing for Write Throughput

When performing bulk inserts, use a larger threshold to minimize checkpoint frequency:

```go
package main

import (
    "database/sql"
    "log"
)

func main() {
    db, _ := sql.Open("dukdb", "mydb.db")
    defer db.Close()

    // Set larger threshold for bulk loading
    db.Exec("PRAGMA checkpoint_threshold = '2GB'")

    // Perform bulk insert
    // Many INSERT statements...

    // Force checkpoint when done
    db.Exec("CHECKPOINT")

    // Restore default threshold for normal operations
    db.Exec("PRAGMA checkpoint_threshold = '256MB'")
}
```

##### Case 2: Minimizing Disk Usage in Development

For development environments with limited disk space:

```sql
PRAGMA checkpoint_threshold = '64MB';
```

This ensures the database doesn't consume excessive disk space during development.

##### Case 3: Fast Recovery Requirement

For applications requiring minimal recovery time:

```sql
PRAGMA checkpoint_threshold = '128MB';
```

More frequent checkpoints mean smaller WAL files and faster recovery.

#### Related Commands

- **CHECKPOINT** - Manually trigger a checkpoint regardless of threshold
- **PRAGMA database_size** - View database and WAL file sizes
- **duckdb_settings()** - List all database settings

## Information PRAGMAs

Information PRAGMAs provide read-only access to system and database information:

```sql
-- Database version
PRAGMA database_size;

-- List all PRAGMA settings
SELECT * FROM duckdb_settings();

-- Catalog information
PRAGMA table_info(table_name);
PRAGMA database_list;
```

## See Also

- [WAL and Checkpointing](persistence.md) - Detailed explanation of Write-Ahead Logging
- [CHECKPOINT Command](persistence.md) - Manual checkpoint operations
- [Configuration](configuration.md) - Database configuration options

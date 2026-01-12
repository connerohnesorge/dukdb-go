# Iceberg Spark Testing Guide

This document provides instructions for manually testing dukdb-go written Iceberg tables with Apache Spark.

## Overview

The dukdb-go Iceberg writer produces tables that follow the Apache Iceberg specification. These tables should be readable by any Iceberg-compatible system, including Apache Spark.

## Prerequisites

### Option 1: Docker (Recommended)

The easiest way to test is using the official Spark Docker image with Iceberg support:

```bash
docker run -it --rm \
  -v /path/to/your/iceberg/tables:/data \
  apache/spark:3.5.0-python3 \
  /opt/spark/bin/pyspark \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0
```

### Option 2: Local Spark Installation

1. Download Apache Spark 3.5.x from https://spark.apache.org/downloads.html
2. Install the Iceberg runtime JAR:
   ```bash
   wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.0/iceberg-spark-runtime-3.5_2.12-1.4.0.jar -P $SPARK_HOME/jars/
   ```

## Writing Tables with dukdb-go

Example Go code to create an Iceberg table:

```go
package main

import (
    "context"
    "github.com/dukdb/dukdb-go"
    "github.com/dukdb/dukdb-go/internal/io/iceberg"
    "github.com/dukdb/dukdb-go/internal/storage"
)

func main() {
    ctx := context.Background()

    // Create writer options
    opts := iceberg.DefaultWriterOptions()
    opts.TableLocation = "/data/my_iceberg_table"
    opts.FormatVersion = iceberg.FormatVersionV2

    // Create writer
    writer, err := iceberg.NewWriter(ctx, opts)
    if err != nil {
        panic(err)
    }

    // Define schema
    types := []dukdb.Type{
        dukdb.TYPE_BIGINT,
        dukdb.TYPE_VARCHAR,
        dukdb.TYPE_DOUBLE,
        dukdb.TYPE_DATE,
    }
    columns := []string{"id", "name", "value", "created_date"}

    writer.SetSchema(columns)
    writer.SetTypes(types)

    // Write data
    chunk := storage.NewDataChunkWithCapacity(types, 1000)
    for i := 0; i < 100; i++ {
        chunk.AppendRow([]any{
            int64(i),
            "Record " + string(rune('A'+i%26)),
            float64(i) * 1.5,
            // date would be added here
        })
    }
    writer.WriteChunk(chunk)

    // Close to finalize metadata
    writer.Close()
}
```

## Reading Tables with PySpark

### Basic Read

```python
from pyspark.sql import SparkSession

# Create Spark session with Iceberg support
spark = SparkSession.builder \
    .appName("IcebergReader") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "/data") \
    .getOrCreate()

# Read the table using Iceberg's path-based table loading
df = spark.read.format("iceberg").load("/data/my_iceberg_table")

# Show the data
df.show()

# Print schema
df.printSchema()

# Count rows
print(f"Row count: {df.count()}")
```

### Time Travel Queries

```python
# Read a specific snapshot
df = spark.read.format("iceberg") \
    .option("snapshot-id", "1234567890") \
    .load("/data/my_iceberg_table")

# Read as of a timestamp
df = spark.read.format("iceberg") \
    .option("as-of-timestamp", "2024-01-15 12:00:00") \
    .load("/data/my_iceberg_table")
```

### Querying Metadata

```python
# Show table history
spark.sql("SELECT * FROM local.db.my_table.history").show()

# Show snapshots
spark.sql("SELECT * FROM local.db.my_table.snapshots").show()

# Show data files
spark.sql("SELECT * FROM local.db.my_table.files").show()

# Show manifests
spark.sql("SELECT * FROM local.db.my_table.manifests").show()
```

## Expected Results

When reading a dukdb-go written table, Spark should:

1. **Successfully parse metadata.json**
   - No errors about missing or invalid fields
   - Correct format version detected (1 or 2)

2. **Correctly interpret schema**
   - All columns visible with correct names
   - Data types correctly mapped:
     | dukdb-go Type | Spark Type |
     |---------------|------------|
     | TYPE_BOOLEAN | BooleanType |
     | TYPE_INTEGER | IntegerType |
     | TYPE_BIGINT | LongType |
     | TYPE_FLOAT | FloatType |
     | TYPE_DOUBLE | DoubleType |
     | TYPE_VARCHAR | StringType |
     | TYPE_DATE | DateType |
     | TYPE_TIMESTAMP | TimestampType |
     | TYPE_BLOB | BinaryType |

3. **Read data correctly**
   - Row counts match expected values
   - Data values are correct
   - NULL values handled properly

4. **Support time travel**
   - Can query specific snapshots
   - Can query as of timestamps

## Troubleshooting

### Common Issues

**Issue: "Unable to find snapshot"**
- Ensure the snapshot ID exists in the table
- Check metadata.json for available snapshots

**Issue: "Invalid metadata file"**
- Verify format-version is 1 or 2
- Check that all required fields are present
- Ensure table-uuid is a valid UUID

**Issue: "Cannot read manifest file"**
- Verify AVRO files have correct schema
- Check that file paths in metadata are correct/accessible

**Issue: "Schema mismatch"**
- Verify field IDs are consistent
- Check that type mappings are correct

### Validation Steps

1. **Validate metadata.json manually:**
   ```bash
   cat /data/my_iceberg_table/metadata/v1.metadata.json | jq .
   ```

2. **Validate AVRO manifest files:**
   ```bash
   # Using avro-tools
   avro-tools tojson /data/my_iceberg_table/metadata/*.avro
   ```

3. **Check file structure:**
   ```bash
   tree /data/my_iceberg_table/
   # Expected structure:
   # my_iceberg_table/
   # ├── data/
   # │   └── *.parquet
   # └── metadata/
   #     ├── v1.metadata.json
   #     ├── version-hint.text
   #     ├── snap-*.avro (manifest list)
   #     └── *-m0.avro (manifest)
   ```

## Compatibility Matrix

| Feature | dukdb-go | Spark 3.5 | Notes |
|---------|----------|-----------|-------|
| Format V1 | Yes | Yes | Basic compatibility |
| Format V2 | Yes | Yes | Full compatibility |
| Unpartitioned tables | Yes | Yes | Fully supported |
| Partitioned tables | Yes | Yes | Identity, bucket, truncate, time transforms |
| Schema evolution | Partial | Yes | Field addition supported |
| Snapshots | Yes | Yes | Full support |
| Time travel | Yes | Yes | By snapshot ID and timestamp |
| Positional deletes | Yes | Yes | V2 format only |
| Equality deletes | Yes | Yes | V2 format only |
| Parquet data files | Yes | Yes | Default format |
| AVRO data files | No | Yes | Not supported by writer |
| ORC data files | No | Yes | Not supported by writer |

## Automated Compatibility Tests

The dukdb-go codebase includes automated compatibility tests in:

```
internal/io/iceberg/spark_compat_test.go
```

These tests validate:
- metadata.json structure follows Iceberg spec
- Manifest list AVRO schema is correct
- Manifest file AVRO schema is correct
- All UUIDs are valid
- Snapshot IDs are valid and unique
- Partition spec format is correct
- Schema format matches Spark expectations
- Round-trip read/write works correctly
- Type mappings are correct

Run the tests with:
```bash
go test -v ./internal/io/iceberg/... -run SparkCompat
```

## Additional Resources

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Spark Iceberg Integration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Iceberg Java API](https://iceberg.apache.org/docs/latest/java-api-quickstart/)
- [Iceberg File Format](https://iceberg.apache.org/spec/#file-formats)

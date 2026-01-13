# Apache Iceberg Example

This example demonstrates how to use dukdb-go to read and query Apache Iceberg tables.

## Features Demonstrated

- Opening Iceberg tables using the Go API
- Reading Iceberg table metadata
- Time travel queries (querying historical snapshots)
- Column projection for efficient queries
- SQL-based access via the `iceberg_scan` function

## Running the Example

```bash
go run main.go [path-to-iceberg-table]
```

If no path is provided, the example will attempt to use test fixtures.

## Requirements

- dukdb-go with Iceberg support enabled
- Access to Iceberg table files (metadata and data files)

## Example Output

The example demonstrates:
1. SQL-based table scanning with `iceberg_scan()`
2. Direct Go API access to Iceberg tables
3. Time travel by snapshot ID and timestamp
4. Error handling for missing tables or snapshots

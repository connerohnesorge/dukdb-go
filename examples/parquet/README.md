# Parquet Examples

This example demonstrates reading and writing Apache Parquet files with dukdb-go.

## Features Demonstrated

- Reading Parquet files with column projection
- Writing Parquet files with compression
- Parquet schema introspection
- Compression options (SNAPPY, GZIP, ZSTD, LZ4, BROTLI)

## Running the Example

```bash
go run main.go
```

## Parquet Functions

### Reading
```sql
-- Read entire Parquet file
SELECT * FROM read_parquet('data.parquet');

-- Read with column projection
SELECT id, name FROM read_parquet('data.parquet');

-- Read multiple files
SELECT * FROM read_parquet('*.parquet');
```

### Writing
```sql
-- Export to Parquet
COPY (SELECT * FROM table) TO 'output.parquet';

-- With compression
COPY table TO 'output.parquet' (COMPRESSION 'GZIP');
```

## Compression Options

| Codec | Description | Use Case |
|-------|-------------|----------|
| SNAPPY | Fast, moderate compression | Default, general purpose |
| GZIP | High compression, slower | Archival, storage savings |
| ZSTD | Balanced speed/compression | Production workloads |
| LZ4 | Very fast, lower compression | Real-time processing |
| BROTLI | High compression for text | Web, text-heavy data |

## Schema Support

Parquet files preserve DuckDB column types:
- INTEGER, BIGINT, DECIMAL
- VARCHAR, BLOB
- DATE, TIMESTAMP
- Nested structures

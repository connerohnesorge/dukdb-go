# Iceberg Test Fixtures

This directory contains test Iceberg tables used for integration testing of the dukdb-go Iceberg reader implementation.

## Test Tables

### simple_table

A basic unpartitioned Iceberg table with 100 rows and a single snapshot.

**Schema:**
- `id` (LONG, required): Row identifier (1-100)
- `name` (STRING, nullable): Name field (null every 10th row)
- `value` (DOUBLE, nullable): Numeric value (null every 5th row)

**Properties:**
- Format version: 2
- Snapshots: 1
- Total rows: 100
- Data files: 1

### time_travel_table

An Iceberg table with 3 snapshots demonstrating data evolution over time.

**Schema:**
- `id` (LONG, required): Row identifier
- `name` (STRING, nullable): Name field
- `value` (DOUBLE, nullable): Numeric value

**Snapshots:**

| Snapshot ID | Timestamp (ms) | Rows in Snapshot | Cumulative Rows |
|-------------|----------------|------------------|-----------------|
| 1000000001  | 1700000000000  | 50 (id 1-50)     | 50              |
| 1000000002  | 1700003600000  | 30 (id 51-80)    | 80              |
| 1000000003  | 1700007200000  | 20 (id 81-100)   | 100             |

**Time Travel Testing:**
- Query at snapshot 1000000001 returns 50 rows
- Query at snapshot 1000000002 returns 80 rows
- Query at snapshot 1000000003 returns 100 rows (current)

## Regenerating Fixtures

If you need to regenerate the test fixtures:

```bash
cd internal/io/iceberg/testdata
uv run --with pyarrow --with fastavro python3 generate_fixtures.py
```

Or with pip:

```bash
pip install pyarrow fastavro
python3 generate_fixtures.py
```

### Requirements
- Python 3.8+
- pyarrow
- fastavro

## File Structure

Each test table has the following structure:

```
<table_name>/
  metadata/
    v<N>.metadata.json    # Table metadata (Iceberg format v2)
    version-hint.text     # Points to current metadata version
    snap-*-manifest-list.avro  # Manifest list files
    snap-*-1-manifest.avro     # Manifest files
  data/
    *.parquet             # Parquet data files
```

## Usage in Tests

The integration tests in `integration_test.go` automatically locate and use these fixtures. The tests update the absolute paths in metadata at runtime to match the test environment.

```go
func TestExample(t *testing.T) {
    tablePath := getSimpleTablePath(t)
    updateMetadataLocations(t, tablePath)

    reader, err := NewReader(ctx, tablePath, nil)
    // ...
}
```

## Notes

- Fixtures use absolute paths which are updated at test time
- Data is deterministic for reproducible tests
- Parquet files use SNAPPY compression
- Timestamps are fixed for reproducibility:
  - 1700000000000 (Nov 14, 2023 22:13:20 UTC)
  - 1700003600000 (Nov 14, 2023 23:13:20 UTC)
  - 1700007200000 (Nov 15, 2023 00:13:20 UTC)

# Iceberg Testing Infrastructure

## Overview

The previously "out of scope" tasks (8.6, 10.3, 10.4) now have **fully automated testing infrastructure** using Docker Compose.

## What Was Built

### 1. Cloud Storage Testing (Task 8.6) ✅

**Files Created:**
- `testdata/docker-compose.yml` - Docker Compose configuration
- `cloud_storage_test.go` - Cloud storage test suite

**Infrastructure:**
- **MinIO** - S3-compatible storage server for S3 testing
- **Fake GCS Server** - Google Cloud Storage emulator for GCS testing

**Test Coverage:**
- S3/GCS path parsing
- Writing Iceberg tables to cloud storage
- Reading Iceberg tables from cloud storage
- Time travel on cloud-stored tables
- Partition pruning with cloud storage

**How to Run:**
```bash
make test-iceberg-cloud
```

### 2. Spark Compatibility Testing (Task 10.3) ✅

**Files Created:**
- `testdata/spark-scripts/generate_iceberg_tables.py` - Spark table generator
- `spark_flink_compat_test.go` - Spark compatibility tests

**Infrastructure:**
- **Apache Spark 3.5.0** with Iceberg 1.4.3 support
- Automated table generation script

**Generated Test Tables:**
- `spark_simple` - Basic types and NULL handling
- `spark_partitioned` - Partitioned by date + category
- `spark_schema_evolution` - Schema evolution (ADD COLUMN)
- `spark_deletes` - Tables with delete files
- `spark_time_travel` - Multiple snapshots
- `spark_simple_s3` - S3-stored table (in MinIO)
- `spark_partitioned_s3` - Partitioned S3 table

**Test Coverage:**
- Reading Spark-generated tables
- Partition pruning on Spark tables
- Schema evolution compatibility
- Delete file handling from Spark
- Time travel across Spark snapshots
- S3-stored Spark tables

**How to Run:**
```bash
make test-iceberg-spark
```

### 3. Flink Compatibility Testing (Task 10.4) ✅

**Files Created:**
- `testdata/flink-scripts/generate_iceberg_tables.sql` - Flink SQL generator
- `spark_flink_compat_test.go` - Flink compatibility tests

**Infrastructure:**
- **Apache Flink 1.18.0** with Iceberg support
- Automated SQL script execution

**Generated Test Tables:**
- `flink_simple` - Basic types
- `flink_partitioned` - Partitioned table
- `flink_complex` - Complex types (MAP, ARRAY)
- `flink_deletes` - Iceberg v2 merge-on-read deletes
- `flink_time_travel` - Multiple snapshots

**Test Coverage:**
- Reading Flink-generated tables
- Partitioned Flink tables
- Complex types (MAP, ARRAY)
- Iceberg v2 merge-on-read deletes
- Time travel on Flink tables

**How to Run:**
```bash
make test-iceberg-flink
```

## Complete Testing Suite

Run all compatibility tests at once:

```bash
make test-iceberg-full
```

This runs:
1. Cloud storage tests (MinIO + fake GCS)
2. Spark compatibility tests
3. Flink compatibility tests

## Makefile Targets

```bash
make test                  # Run all tests (no Docker)
make test-iceberg          # Run all Iceberg tests
make test-iceberg-cloud    # Cloud storage tests
make test-iceberg-spark    # Spark compatibility
make test-iceberg-flink    # Flink compatibility
make test-iceberg-full     # All compatibility tests
make docker-up             # Start all Docker services
make docker-down           # Stop all Docker services
```

## CI/CD Integration

The infrastructure is ready for CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Iceberg Compatibility Tests
  run: |
    make test-iceberg-full
```

## Why This Matters

These tests ensure that dukdb-go's Iceberg implementation is **truly compatible** with:
- Real-world Spark-generated Iceberg tables
- Real-world Flink-generated Iceberg tables
- Cloud storage (S3, GCS)
- Complex Iceberg v2 features (delete files, merge-on-read)

## Cost

- **Zero cloud costs** - All testing runs locally with Docker
- **Zero manual setup** - Fully automated with Makefile
- **Fast execution** - Tests run in parallel when possible

## Documentation

See `testdata/README.md` for detailed documentation on:
- Service architecture
- Test scenarios
- Troubleshooting
- Development workflow

# Iceberg Test Infrastructure

This directory contains Docker Compose setup and scripts for comprehensive Iceberg compatibility testing.

## Overview

The test infrastructure includes:
1. **MinIO** - S3-compatible storage for cloud storage testing
2. **Fake GCS Server** - GCS emulator for Google Cloud Storage testing
3. **Apache Spark** - Generate Spark-compatible Iceberg tables
4. **Apache Flink** - Generate Flink-compatible Iceberg tables

## Prerequisites

- Docker and Docker Compose installed
- Go 1.21+ for running tests
- At least 4GB RAM available for Docker

## Quick Start

### Start All Services

```bash
docker compose up -d
```

### Run Cloud Storage Tests

```bash
# From the iceberg directory
go test -v -run TestCloudStorage
```

### Run Spark Compatibility Tests

```bash
# Start Spark and generate tables
docker compose up -d spark
docker exec iceberg-spark /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  /opt/spark-scripts/generate_iceberg_tables.py

# Run tests
go test -v -run TestSparkGenerated
```

### Run Flink Compatibility Tests

```bash
# Start Flink and generate tables
docker compose up -d flink-jobmanager flink-taskmanager
sleep 20  # Wait for Flink to initialize
docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh \
  -f /opt/flink-scripts/generate_iceberg_tables.sql

# Run tests
go test -v -run TestFlinkGenerated
```

## References

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Iceberg Spark Runtime](https://iceberg.apache.org/docs/latest/spark/)
- [Iceberg Flink](https://iceberg.apache.org/docs/latest/flink/)
- [MinIO Documentation](https://min.io/docs/)

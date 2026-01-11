#!/usr/bin/env python3
"""
Generate test Iceberg table fixtures for dukdb-go integration tests.

This script creates minimal Iceberg tables that can be used to test the
iceberg reader implementation. The generated fixtures are small and
deterministic, suitable for checking into the repository.

Usage:
    cd internal/io/iceberg/testdata
    python3 generate_fixtures.py

Or with uv:
    uv run generate_fixtures.py

Requirements:
    pip install pyiceberg pyarrow pandas fastavro

The script generates:
1. simple_table: Basic unpartitioned table with a single snapshot
2. time_travel_table: Table with 3 snapshots for time travel testing
3. positional_deletes_table: Table with positional delete files
4. equality_deletes_table: Table with equality delete files
"""

import json
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def generate_simple_table(base_path: Path) -> None:
    """
    Generate a simple unpartitioned Iceberg table with 100 rows.

    Schema:
        - id: long (not null)
        - name: string (nullable)
        - value: double (nullable)
    """
    table_path = base_path / "simple_table"
    if table_path.exists():
        shutil.rmtree(table_path)

    metadata_dir = table_path / "metadata"
    data_dir = table_path / "data"
    metadata_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    # Generate data
    ids = list(range(1, 101))
    names = [f"name_{i}" if i % 10 != 0 else None for i in range(1, 101)]
    values = [float(i) * 1.5 if i % 5 != 0 else None for i in range(1, 101)]

    # Create Parquet file
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "name": pa.array(names, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })

    data_file_name = "00000-0-data.parquet"
    data_file_path = data_dir / data_file_name
    pq.write_table(table, data_file_path, compression="snappy")

    file_size = data_file_path.stat().st_size

    # Generate table metadata
    table_uuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    snapshot_id = 1000000001
    timestamp_ms = 1700000000000  # Fixed timestamp for reproducibility
    schema_id = 0
    spec_id = 0

    # Manifest file path
    manifest_file_name = f"snap-{snapshot_id}-1-manifest.avro"
    manifest_list_name = f"snap-{snapshot_id}-manifest-list.avro"

    # Create metadata.json (v2 format)
    metadata = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": str(table_path.absolute()),
        "last-updated-ms": timestamp_ms,
        "last-column-id": 3,
        "current-schema-id": schema_id,
        "schemas": [
            {
                "type": "struct",
                "schema-id": schema_id,
                "fields": [
                    {"id": 1, "name": "id", "required": True, "type": "long"},
                    {"id": 2, "name": "name", "required": False, "type": "string"},
                    {"id": 3, "name": "value", "required": False, "type": "double"},
                ]
            }
        ],
        "default-spec-id": spec_id,
        "partition-specs": [
            {"spec-id": spec_id, "fields": []}
        ],
        "last-partition-id": 999,
        "properties": {
            "write.format.default": "parquet",
        },
        "current-snapshot-id": snapshot_id,
        "snapshots": [
            {
                "snapshot-id": snapshot_id,
                "sequence-number": 1,
                "timestamp-ms": timestamp_ms,
                "manifest-list": str((metadata_dir / manifest_list_name).absolute()),
                "summary": {
                    "operation": "append",
                    "added-data-files": "1",
                    "added-records": "100",
                    "added-files-size": str(file_size),
                },
                "schema-id": schema_id,
            }
        ],
        "snapshot-log": [
            {"snapshot-id": snapshot_id, "timestamp-ms": timestamp_ms}
        ],
        "refs": {
            "main": {
                "snapshot-id": snapshot_id,
                "type": "branch"
            }
        },
    }

    # Write metadata
    with open(metadata_dir / "v1.metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # Write version-hint.text
    with open(metadata_dir / "version-hint.text", "w") as f:
        f.write("1")

    # Write manifest list (AVRO format)
    write_manifest_list_avro(
        metadata_dir / manifest_list_name,
        [
            {
                "manifest_path": str((metadata_dir / manifest_file_name).absolute()),
                "manifest_length": 0,  # Will be updated
                "partition_spec_id": spec_id,
                "content": 0,  # Data manifest
                "sequence_number": 1,
                "min_sequence_number": 1,
                "added_snapshot_id": snapshot_id,
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": 100,
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            }
        ],
    )

    # Write manifest file (AVRO format)
    write_manifest_avro(
        metadata_dir / manifest_file_name,
        [
            {
                "status": 1,  # ADDED
                "snapshot_id": snapshot_id,
                "sequence_number": 1,
                "data_file": {
                    "content": 0,  # DATA
                    "file_path": str(data_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": 100,
                    "file_size_in_bytes": file_size,
                    "column_sizes": {1: 800, 2: 1200, 3: 800},
                    "value_counts": {1: 100, 2: 100, 3: 100},
                    "null_value_counts": {1: 0, 2: 10, 3: 20},
                    "partition": {},
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )

    # Update manifest_length in manifest list
    manifest_size = (metadata_dir / manifest_file_name).stat().st_size
    # Re-write manifest list with correct size
    write_manifest_list_avro(
        metadata_dir / manifest_list_name,
        [
            {
                "manifest_path": str((metadata_dir / manifest_file_name).absolute()),
                "manifest_length": manifest_size,
                "partition_spec_id": spec_id,
                "content": 0,
                "sequence_number": 1,
                "min_sequence_number": 1,
                "added_snapshot_id": snapshot_id,
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": 100,
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            }
        ],
    )

    print(f"Generated simple_table at {table_path}")


def generate_time_travel_table(base_path: Path) -> None:
    """
    Generate an Iceberg table with 3 snapshots for time travel testing.

    Snapshot 1: Initial 50 rows (id 1-50)
    Snapshot 2: Add 30 rows (id 51-80)
    Snapshot 3: Add 20 rows (id 81-100)
    """
    table_path = base_path / "time_travel_table"
    if table_path.exists():
        shutil.rmtree(table_path)

    metadata_dir = table_path / "metadata"
    data_dir = table_path / "data"
    metadata_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    table_uuid = "b2c3d4e5-f6a7-8901-bcde-f23456789012"
    schema_id = 0
    spec_id = 0

    # Timestamps for each snapshot (1 hour apart for reproducibility)
    ts1 = 1700000000000  # Snapshot 1
    ts2 = 1700003600000  # Snapshot 2 (1 hour later)
    ts3 = 1700007200000  # Snapshot 3 (2 hours later)

    snapshot_ids = [1000000001, 1000000002, 1000000003]

    # Generate data files for each snapshot
    data_files = []

    # Snapshot 1: rows 1-50
    ids1 = list(range(1, 51))
    names1 = [f"name_{i}" for i in ids1]
    values1 = [float(i) * 1.0 for i in ids1]

    table1 = pa.table({
        "id": pa.array(ids1, type=pa.int64()),
        "name": pa.array(names1, type=pa.string()),
        "value": pa.array(values1, type=pa.float64()),
    })

    data_file_1 = data_dir / "00000-0-data.parquet"
    pq.write_table(table1, data_file_1, compression="snappy")
    data_files.append({
        "path": str(data_file_1.absolute()),
        "size": data_file_1.stat().st_size,
        "count": 50,
        "snapshot_id": snapshot_ids[0],
    })

    # Snapshot 2: rows 51-80
    ids2 = list(range(51, 81))
    names2 = [f"name_{i}" for i in ids2]
    values2 = [float(i) * 2.0 for i in ids2]

    table2 = pa.table({
        "id": pa.array(ids2, type=pa.int64()),
        "name": pa.array(names2, type=pa.string()),
        "value": pa.array(values2, type=pa.float64()),
    })

    data_file_2 = data_dir / "00001-0-data.parquet"
    pq.write_table(table2, data_file_2, compression="snappy")
    data_files.append({
        "path": str(data_file_2.absolute()),
        "size": data_file_2.stat().st_size,
        "count": 30,
        "snapshot_id": snapshot_ids[1],
    })

    # Snapshot 3: rows 81-100
    ids3 = list(range(81, 101))
    names3 = [f"name_{i}" for i in ids3]
    values3 = [float(i) * 3.0 for i in ids3]

    table3 = pa.table({
        "id": pa.array(ids3, type=pa.int64()),
        "name": pa.array(names3, type=pa.string()),
        "value": pa.array(values3, type=pa.float64()),
    })

    data_file_3 = data_dir / "00002-0-data.parquet"
    pq.write_table(table3, data_file_3, compression="snappy")
    data_files.append({
        "path": str(data_file_3.absolute()),
        "size": data_file_3.stat().st_size,
        "count": 20,
        "snapshot_id": snapshot_ids[2],
    })

    # Create manifest files for each snapshot
    manifests = []
    for i, (snap_id, ts) in enumerate(zip(snapshot_ids, [ts1, ts2, ts3])):
        manifest_file_name = f"snap-{snap_id}-1-manifest.avro"
        manifest_list_name = f"snap-{snap_id}-manifest-list.avro"

        # Cumulative data files up to this snapshot
        cumulative_files = data_files[:i+1]

        # Write manifest
        manifest_entries = []
        for j, df in enumerate(cumulative_files):
            status = 1 if df["snapshot_id"] == snap_id else 0  # ADDED or EXISTING
            manifest_entries.append({
                "status": status,
                "snapshot_id": df["snapshot_id"],
                "sequence_number": j + 1,
                "data_file": {
                    "content": 0,
                    "file_path": df["path"],
                    "file_format": "parquet",
                    "record_count": df["count"],
                    "file_size_in_bytes": df["size"],
                    "column_sizes": {1: 800, 2: 1200, 3: 800},
                    "value_counts": {1: df["count"], 2: df["count"], 3: df["count"]},
                    "null_value_counts": {1: 0, 2: 0, 3: 0},
                    "partition": {},
                }
            })

        write_manifest_avro(
            metadata_dir / manifest_file_name,
            manifest_entries,
            schema_id=schema_id,
            spec_id=spec_id,
        )

        manifest_size = (metadata_dir / manifest_file_name).stat().st_size

        # Count added vs existing
        added_count = sum(1 for df in cumulative_files if df["snapshot_id"] == snap_id)
        existing_count = len(cumulative_files) - added_count
        added_rows = sum(df["count"] for df in cumulative_files if df["snapshot_id"] == snap_id)
        existing_rows = sum(df["count"] for df in cumulative_files if df["snapshot_id"] != snap_id)

        write_manifest_list_avro(
            metadata_dir / manifest_list_name,
            [
                {
                    "manifest_path": str((metadata_dir / manifest_file_name).absolute()),
                    "manifest_length": manifest_size,
                    "partition_spec_id": spec_id,
                    "content": 0,
                    "sequence_number": i + 1,
                    "min_sequence_number": 1,
                    "added_snapshot_id": snap_id,
                    "added_files_count": added_count,
                    "existing_files_count": existing_count,
                    "deleted_files_count": 0,
                    "added_rows_count": added_rows,
                    "existing_rows_count": existing_rows,
                    "deleted_rows_count": 0,
                }
            ],
        )

        manifests.append({
            "snapshot_id": snap_id,
            "manifest_list": str((metadata_dir / manifest_list_name).absolute()),
            "timestamp_ms": ts,
            "added_files": added_count,
            "added_rows": added_rows,
            "total_rows": sum(df["count"] for df in cumulative_files),
        })

    # Create metadata
    metadata = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": str(table_path.absolute()),
        "last-updated-ms": ts3,
        "last-column-id": 3,
        "current-schema-id": schema_id,
        "schemas": [
            {
                "type": "struct",
                "schema-id": schema_id,
                "fields": [
                    {"id": 1, "name": "id", "required": True, "type": "long"},
                    {"id": 2, "name": "name", "required": False, "type": "string"},
                    {"id": 3, "name": "value", "required": False, "type": "double"},
                ]
            }
        ],
        "default-spec-id": spec_id,
        "partition-specs": [
            {"spec-id": spec_id, "fields": []}
        ],
        "last-partition-id": 999,
        "properties": {
            "write.format.default": "parquet",
        },
        "current-snapshot-id": snapshot_ids[2],  # Latest snapshot
        "snapshots": [
            {
                "snapshot-id": snapshot_ids[0],
                "sequence-number": 1,
                "timestamp-ms": ts1,
                "manifest-list": manifests[0]["manifest_list"],
                "summary": {
                    "operation": "append",
                    "added-data-files": str(manifests[0]["added_files"]),
                    "added-records": str(manifests[0]["added_rows"]),
                },
                "schema-id": schema_id,
            },
            {
                "snapshot-id": snapshot_ids[1],
                "parent-snapshot-id": snapshot_ids[0],
                "sequence-number": 2,
                "timestamp-ms": ts2,
                "manifest-list": manifests[1]["manifest_list"],
                "summary": {
                    "operation": "append",
                    "added-data-files": str(manifests[1]["added_files"]),
                    "added-records": str(manifests[1]["added_rows"]),
                },
                "schema-id": schema_id,
            },
            {
                "snapshot-id": snapshot_ids[2],
                "parent-snapshot-id": snapshot_ids[1],
                "sequence-number": 3,
                "timestamp-ms": ts3,
                "manifest-list": manifests[2]["manifest_list"],
                "summary": {
                    "operation": "append",
                    "added-data-files": str(manifests[2]["added_files"]),
                    "added-records": str(manifests[2]["added_rows"]),
                },
                "schema-id": schema_id,
            },
        ],
        "snapshot-log": [
            {"snapshot-id": snapshot_ids[0], "timestamp-ms": ts1},
            {"snapshot-id": snapshot_ids[1], "timestamp-ms": ts2},
            {"snapshot-id": snapshot_ids[2], "timestamp-ms": ts3},
        ],
        "refs": {
            "main": {
                "snapshot-id": snapshot_ids[2],
                "type": "branch"
            }
        },
    }

    with open(metadata_dir / "v3.metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    with open(metadata_dir / "version-hint.text", "w") as f:
        f.write("3")

    print(f"Generated time_travel_table at {table_path}")
    print(f"  Snapshot 1 (id={snapshot_ids[0]}): 50 rows")
    print(f"  Snapshot 2 (id={snapshot_ids[1]}): 80 rows total")
    print(f"  Snapshot 3 (id={snapshot_ids[2]}): 100 rows total")


def generate_positional_deletes_table(base_path: Path) -> None:
    """
    Generate an Iceberg table with positional delete files.

    Initial data: 100 rows (id 0-99)
    Positional deletes: rows at positions 10, 20, 30, 40, 50
    Expected result: 95 rows remain
    """
    table_path = base_path / "positional_deletes_table"
    if table_path.exists():
        shutil.rmtree(table_path)

    metadata_dir = table_path / "metadata"
    data_dir = table_path / "data"
    metadata_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    table_uuid = "c3d4e5f6-a7b8-9012-cdef-345678901234"
    schema_id = 0
    spec_id = 0

    # Timestamps
    ts1 = 1700000000000  # Snapshot 1: initial data
    ts2 = 1700003600000  # Snapshot 2: with deletes

    snapshot_ids = [2000000001, 2000000002]

    # Generate initial data file with 100 rows (id 0-99)
    ids = list(range(0, 100))
    names = [f"name_{i}" for i in ids]
    values = [float(i) * 1.5 for i in ids]

    data_table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "name": pa.array(names, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })

    data_file_path = data_dir / "00000-0-data.parquet"
    pq.write_table(data_table, data_file_path, compression="snappy")
    data_file_size = data_file_path.stat().st_size

    # Generate positional delete file
    # Positions to delete: 10, 20, 30, 40, 50 (0-indexed row positions)
    deleted_positions = [10, 20, 30, 40, 50]
    delete_file_paths = [str(data_file_path.absolute())] * len(deleted_positions)
    delete_positions = deleted_positions

    delete_table = pa.table({
        "file_path": pa.array(delete_file_paths, type=pa.string()),
        "pos": pa.array(delete_positions, type=pa.int64()),
    })

    delete_file_path = data_dir / "00001-0-delete.parquet"
    pq.write_table(delete_table, delete_file_path, compression="snappy")
    delete_file_size = delete_file_path.stat().st_size

    # Create manifest for snapshot 1 (data only)
    manifest_file_1 = f"snap-{snapshot_ids[0]}-1-manifest.avro"
    manifest_list_1 = f"snap-{snapshot_ids[0]}-manifest-list.avro"

    write_manifest_avro(
        metadata_dir / manifest_file_1,
        [
            {
                "status": 1,  # ADDED
                "snapshot_id": snapshot_ids[0],
                "sequence_number": 1,
                "data_file": {
                    "content": 0,  # DATA
                    "file_path": str(data_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": 100,
                    "file_size_in_bytes": data_file_size,
                    "column_sizes": {},
                    "value_counts": {},
                    "null_value_counts": {},
                    "partition": {},
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )

    manifest_1_size = (metadata_dir / manifest_file_1).stat().st_size

    write_manifest_list_avro(
        metadata_dir / manifest_list_1,
        [
            {
                "manifest_path": str((metadata_dir / manifest_file_1).absolute()),
                "manifest_length": manifest_1_size,
                "partition_spec_id": spec_id,
                "content": 0,  # Data manifest
                "sequence_number": 1,
                "min_sequence_number": 1,
                "added_snapshot_id": snapshot_ids[0],
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": 100,
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            }
        ],
    )

    # Create manifests for snapshot 2 (data + delete manifest)
    # Data manifest (existing data file)
    manifest_file_2_data = f"snap-{snapshot_ids[1]}-1-data-manifest.avro"
    write_manifest_avro(
        metadata_dir / manifest_file_2_data,
        [
            {
                "status": 0,  # EXISTING
                "snapshot_id": snapshot_ids[0],
                "sequence_number": 1,
                "data_file": {
                    "content": 0,  # DATA
                    "file_path": str(data_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": 100,
                    "file_size_in_bytes": data_file_size,
                    "column_sizes": {},
                    "value_counts": {},
                    "null_value_counts": {},
                    "partition": {},
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )
    manifest_2_data_size = (metadata_dir / manifest_file_2_data).stat().st_size

    # Delete manifest
    manifest_file_2_delete = f"snap-{snapshot_ids[1]}-2-delete-manifest.avro"
    write_manifest_avro(
        metadata_dir / manifest_file_2_delete,
        [
            {
                "status": 1,  # ADDED
                "snapshot_id": snapshot_ids[1],
                "sequence_number": 2,
                "data_file": {
                    "content": 1,  # POSITIONAL DELETES
                    "file_path": str(delete_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": len(deleted_positions),
                    "file_size_in_bytes": delete_file_size,
                    "column_sizes": {},
                    "value_counts": {},
                    "null_value_counts": {},
                    "partition": {},
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )
    manifest_2_delete_size = (metadata_dir / manifest_file_2_delete).stat().st_size

    # Manifest list for snapshot 2 with both data and delete manifests
    manifest_list_2 = f"snap-{snapshot_ids[1]}-manifest-list.avro"
    write_manifest_list_avro(
        metadata_dir / manifest_list_2,
        [
            {
                "manifest_path": str((metadata_dir / manifest_file_2_data).absolute()),
                "manifest_length": manifest_2_data_size,
                "partition_spec_id": spec_id,
                "content": 0,  # Data manifest
                "sequence_number": 2,
                "min_sequence_number": 1,
                "added_snapshot_id": snapshot_ids[1],
                "added_files_count": 0,
                "existing_files_count": 1,
                "deleted_files_count": 0,
                "added_rows_count": 0,
                "existing_rows_count": 100,
                "deleted_rows_count": 0,
            },
            {
                "manifest_path": str((metadata_dir / manifest_file_2_delete).absolute()),
                "manifest_length": manifest_2_delete_size,
                "partition_spec_id": spec_id,
                "content": 1,  # Delete manifest
                "sequence_number": 2,
                "min_sequence_number": 2,
                "added_snapshot_id": snapshot_ids[1],
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": len(deleted_positions),
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            },
        ],
    )

    # Create metadata
    metadata = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": str(table_path.absolute()),
        "last-updated-ms": ts2,
        "last-column-id": 3,
        "current-schema-id": schema_id,
        "schemas": [
            {
                "type": "struct",
                "schema-id": schema_id,
                "fields": [
                    {"id": 1, "name": "id", "required": True, "type": "long"},
                    {"id": 2, "name": "name", "required": False, "type": "string"},
                    {"id": 3, "name": "value", "required": False, "type": "double"},
                ]
            }
        ],
        "default-spec-id": spec_id,
        "partition-specs": [
            {"spec-id": spec_id, "fields": []}
        ],
        "last-partition-id": 999,
        "properties": {
            "write.format.default": "parquet",
            "write.delete.mode": "merge-on-read",
        },
        "current-snapshot-id": snapshot_ids[1],
        "snapshots": [
            {
                "snapshot-id": snapshot_ids[0],
                "sequence-number": 1,
                "timestamp-ms": ts1,
                "manifest-list": str((metadata_dir / manifest_list_1).absolute()),
                "summary": {
                    "operation": "append",
                    "added-data-files": "1",
                    "added-records": "100",
                },
                "schema-id": schema_id,
            },
            {
                "snapshot-id": snapshot_ids[1],
                "parent-snapshot-id": snapshot_ids[0],
                "sequence-number": 2,
                "timestamp-ms": ts2,
                "manifest-list": str((metadata_dir / manifest_list_2).absolute()),
                "summary": {
                    "operation": "delete",
                    "added-delete-files": "1",
                    "added-position-delete-files": "1",
                    "added-position-deletes": str(len(deleted_positions)),
                    "total-position-deletes": str(len(deleted_positions)),
                },
                "schema-id": schema_id,
            },
        ],
        "snapshot-log": [
            {"snapshot-id": snapshot_ids[0], "timestamp-ms": ts1},
            {"snapshot-id": snapshot_ids[1], "timestamp-ms": ts2},
        ],
        "refs": {
            "main": {
                "snapshot-id": snapshot_ids[1],
                "type": "branch"
            }
        },
    }

    with open(metadata_dir / "v2.metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    with open(metadata_dir / "version-hint.text", "w") as f:
        f.write("2")

    print(f"Generated positional_deletes_table at {table_path}")
    print(f"  Snapshot 1 (id={snapshot_ids[0]}): 100 rows initial")
    print(f"  Snapshot 2 (id={snapshot_ids[1]}): {len(deleted_positions)} positional deletes")
    print(f"  Deleted positions: {deleted_positions}")
    print(f"  Expected remaining rows: {100 - len(deleted_positions)}")


def generate_equality_deletes_table(base_path: Path) -> None:
    """
    Generate an Iceberg table with equality delete files.

    Initial data: 100 rows (id 0-99)
    Equality deletes: WHERE id IN (15, 25, 35, 45, 55)
    Expected result: 95 rows remain
    """
    table_path = base_path / "equality_deletes_table"
    if table_path.exists():
        shutil.rmtree(table_path)

    metadata_dir = table_path / "metadata"
    data_dir = table_path / "data"
    metadata_dir.mkdir(parents=True)
    data_dir.mkdir(parents=True)

    table_uuid = "d4e5f6a7-b8c9-0123-defa-456789012345"
    schema_id = 0
    spec_id = 0

    # Timestamps
    ts1 = 1700000000000  # Snapshot 1: initial data
    ts2 = 1700003600000  # Snapshot 2: with deletes

    snapshot_ids = [3000000001, 3000000002]

    # Generate initial data file with 100 rows (id 0-99)
    ids = list(range(0, 100))
    names = [f"name_{i}" for i in ids]
    values = [float(i) * 1.5 for i in ids]

    data_table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "name": pa.array(names, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
    })

    data_file_path = data_dir / "00000-0-data.parquet"
    pq.write_table(data_table, data_file_path, compression="snappy")
    data_file_size = data_file_path.stat().st_size

    # Generate equality delete file
    # Deletes WHERE id IN (15, 25, 35, 45, 55)
    deleted_ids = [15, 25, 35, 45, 55]

    delete_table = pa.table({
        "id": pa.array(deleted_ids, type=pa.int64()),
    })

    delete_file_path = data_dir / "00001-0-equality-delete.parquet"
    pq.write_table(delete_table, delete_file_path, compression="snappy")
    delete_file_size = delete_file_path.stat().st_size

    # Create manifest for snapshot 1 (data only)
    manifest_file_1 = f"snap-{snapshot_ids[0]}-1-manifest.avro"
    manifest_list_1 = f"snap-{snapshot_ids[0]}-manifest-list.avro"

    write_manifest_avro(
        metadata_dir / manifest_file_1,
        [
            {
                "status": 1,  # ADDED
                "snapshot_id": snapshot_ids[0],
                "sequence_number": 1,
                "data_file": {
                    "content": 0,  # DATA
                    "file_path": str(data_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": 100,
                    "file_size_in_bytes": data_file_size,
                    "column_sizes": {},
                    "value_counts": {},
                    "null_value_counts": {},
                    "partition": {},
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )

    manifest_1_size = (metadata_dir / manifest_file_1).stat().st_size

    write_manifest_list_avro(
        metadata_dir / manifest_list_1,
        [
            {
                "manifest_path": str((metadata_dir / manifest_file_1).absolute()),
                "manifest_length": manifest_1_size,
                "partition_spec_id": spec_id,
                "content": 0,  # Data manifest
                "sequence_number": 1,
                "min_sequence_number": 1,
                "added_snapshot_id": snapshot_ids[0],
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": 100,
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            }
        ],
    )

    # Create manifests for snapshot 2 (data + equality delete manifest)
    # Data manifest (existing data file)
    manifest_file_2_data = f"snap-{snapshot_ids[1]}-1-data-manifest.avro"
    write_manifest_avro(
        metadata_dir / manifest_file_2_data,
        [
            {
                "status": 0,  # EXISTING
                "snapshot_id": snapshot_ids[0],
                "sequence_number": 1,
                "data_file": {
                    "content": 0,  # DATA
                    "file_path": str(data_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": 100,
                    "file_size_in_bytes": data_file_size,
                    "column_sizes": {},
                    "value_counts": {},
                    "null_value_counts": {},
                    "partition": {},
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )
    manifest_2_data_size = (metadata_dir / manifest_file_2_data).stat().st_size

    # Equality delete manifest
    manifest_file_2_delete = f"snap-{snapshot_ids[1]}-2-delete-manifest.avro"
    write_manifest_avro_with_equality(
        metadata_dir / manifest_file_2_delete,
        [
            {
                "status": 1,  # ADDED
                "snapshot_id": snapshot_ids[1],
                "sequence_number": 2,
                "data_file": {
                    "content": 2,  # EQUALITY DELETES
                    "file_path": str(delete_file_path.absolute()),
                    "file_format": "parquet",
                    "record_count": len(deleted_ids),
                    "file_size_in_bytes": delete_file_size,
                    "column_sizes": {},
                    "value_counts": {},
                    "null_value_counts": {},
                    "partition": {},
                    "equality_ids": [1],  # Field ID 1 = "id" column
                }
            }
        ],
        schema_id=schema_id,
        spec_id=spec_id,
    )
    manifest_2_delete_size = (metadata_dir / manifest_file_2_delete).stat().st_size

    # Manifest list for snapshot 2 with both data and delete manifests
    manifest_list_2 = f"snap-{snapshot_ids[1]}-manifest-list.avro"
    write_manifest_list_avro(
        metadata_dir / manifest_list_2,
        [
            {
                "manifest_path": str((metadata_dir / manifest_file_2_data).absolute()),
                "manifest_length": manifest_2_data_size,
                "partition_spec_id": spec_id,
                "content": 0,  # Data manifest
                "sequence_number": 2,
                "min_sequence_number": 1,
                "added_snapshot_id": snapshot_ids[1],
                "added_files_count": 0,
                "existing_files_count": 1,
                "deleted_files_count": 0,
                "added_rows_count": 0,
                "existing_rows_count": 100,
                "deleted_rows_count": 0,
            },
            {
                "manifest_path": str((metadata_dir / manifest_file_2_delete).absolute()),
                "manifest_length": manifest_2_delete_size,
                "partition_spec_id": spec_id,
                "content": 1,  # Delete manifest
                "sequence_number": 2,
                "min_sequence_number": 2,
                "added_snapshot_id": snapshot_ids[1],
                "added_files_count": 1,
                "existing_files_count": 0,
                "deleted_files_count": 0,
                "added_rows_count": len(deleted_ids),
                "existing_rows_count": 0,
                "deleted_rows_count": 0,
            },
        ],
    )

    # Create metadata
    metadata = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": str(table_path.absolute()),
        "last-updated-ms": ts2,
        "last-column-id": 3,
        "current-schema-id": schema_id,
        "schemas": [
            {
                "type": "struct",
                "schema-id": schema_id,
                "fields": [
                    {"id": 1, "name": "id", "required": True, "type": "long"},
                    {"id": 2, "name": "name", "required": False, "type": "string"},
                    {"id": 3, "name": "value", "required": False, "type": "double"},
                ]
            }
        ],
        "default-spec-id": spec_id,
        "partition-specs": [
            {"spec-id": spec_id, "fields": []}
        ],
        "last-partition-id": 999,
        "properties": {
            "write.format.default": "parquet",
            "write.delete.mode": "merge-on-read",
        },
        "current-snapshot-id": snapshot_ids[1],
        "snapshots": [
            {
                "snapshot-id": snapshot_ids[0],
                "sequence-number": 1,
                "timestamp-ms": ts1,
                "manifest-list": str((metadata_dir / manifest_list_1).absolute()),
                "summary": {
                    "operation": "append",
                    "added-data-files": "1",
                    "added-records": "100",
                },
                "schema-id": schema_id,
            },
            {
                "snapshot-id": snapshot_ids[1],
                "parent-snapshot-id": snapshot_ids[0],
                "sequence-number": 2,
                "timestamp-ms": ts2,
                "manifest-list": str((metadata_dir / manifest_list_2).absolute()),
                "summary": {
                    "operation": "delete",
                    "added-delete-files": "1",
                    "added-equality-delete-files": "1",
                    "added-equality-deletes": str(len(deleted_ids)),
                    "total-equality-deletes": str(len(deleted_ids)),
                },
                "schema-id": schema_id,
            },
        ],
        "snapshot-log": [
            {"snapshot-id": snapshot_ids[0], "timestamp-ms": ts1},
            {"snapshot-id": snapshot_ids[1], "timestamp-ms": ts2},
        ],
        "refs": {
            "main": {
                "snapshot-id": snapshot_ids[1],
                "type": "branch"
            }
        },
    }

    with open(metadata_dir / "v2.metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    with open(metadata_dir / "version-hint.text", "w") as f:
        f.write("2")

    print(f"Generated equality_deletes_table at {table_path}")
    print(f"  Snapshot 1 (id={snapshot_ids[0]}): 100 rows initial")
    print(f"  Snapshot 2 (id={snapshot_ids[1]}): {len(deleted_ids)} equality deletes")
    print(f"  Deleted IDs: {deleted_ids}")
    print(f"  Expected remaining rows: {100 - len(deleted_ids)}")


def write_manifest_list_avro(path: Path, entries: list) -> None:
    """Write manifest list in AVRO format using fastavro."""
    import fastavro

    # Iceberg manifest list schema
    schema = {
        "type": "record",
        "name": "manifest_file",
        "fields": [
            {"name": "manifest_path", "type": "string"},
            {"name": "manifest_length", "type": "long"},
            {"name": "partition_spec_id", "type": "int"},
            {"name": "content", "type": "int"},
            {"name": "sequence_number", "type": "long"},
            {"name": "min_sequence_number", "type": "long"},
            {"name": "added_snapshot_id", "type": "long"},
            {"name": "added_files_count", "type": "int"},
            {"name": "existing_files_count", "type": "int"},
            {"name": "deleted_files_count", "type": "int"},
            {"name": "added_rows_count", "type": "long"},
            {"name": "existing_rows_count", "type": "long"},
            {"name": "deleted_rows_count", "type": "long"},
        ]
    }

    parsed_schema = fastavro.parse_schema(schema)

    with open(path, "wb") as f:
        fastavro.writer(f, parsed_schema, entries)


def write_manifest_avro(path: Path, entries: list, schema_id: int, spec_id: int) -> None:
    """Write manifest file in AVRO format using fastavro.

    Note: Iceberg uses integer keys for column_sizes, value_counts, etc. but AVRO
    only supports string keys in maps. The hamba/avro Go library handles this by
    converting int32 keys, but fastavro encodes with string keys. We'll omit the
    stats maps to avoid compatibility issues - they are optional in Iceberg.
    """
    import fastavro

    # Simplified Iceberg manifest entry schema
    # Omit the column stats maps (they're optional) to avoid AVRO map key type issues
    schema = {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": ["null", "long"], "default": None},
            {"name": "sequence_number", "type": ["null", "long"], "default": None},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": None},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "content", "type": "int"},
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": []  # Unpartitioned
                            },
                        },
                    ]
                }
            }
        ]
    }

    parsed_schema = fastavro.parse_schema(schema)

    # Convert entries to expected format
    avro_entries = []
    for entry in entries:
        df = entry["data_file"]

        avro_entry = {
            "status": entry["status"],
            "snapshot_id": entry.get("snapshot_id"),
            "sequence_number": entry.get("sequence_number"),
            "file_sequence_number": entry.get("file_sequence_number"),
            "data_file": {
                "content": df["content"],
                "file_path": df["file_path"],
                "file_format": df["file_format"],
                "record_count": df["record_count"],
                "file_size_in_bytes": df["file_size_in_bytes"],
                "partition": {},
            }
        }
        avro_entries.append(avro_entry)

    with open(path, "wb") as f:
        fastavro.writer(f, parsed_schema, avro_entries)


def write_manifest_avro_with_equality(path: Path, entries: list, schema_id: int, spec_id: int) -> None:
    """Write manifest file with equality_ids field for equality delete files."""
    import fastavro

    # Schema with equality_ids for equality delete files
    schema = {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int"},
            {"name": "snapshot_id", "type": ["null", "long"], "default": None},
            {"name": "sequence_number", "type": ["null", "long"], "default": None},
            {"name": "file_sequence_number", "type": ["null", "long"], "default": None},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "content", "type": "int"},
                        {"name": "file_path", "type": "string"},
                        {"name": "file_format", "type": "string"},
                        {"name": "record_count", "type": "long"},
                        {"name": "file_size_in_bytes", "type": "long"},
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": []  # Unpartitioned
                            },
                        },
                        {
                            "name": "equality_ids",
                            "type": ["null", {"type": "array", "items": "int"}],
                            "default": None,
                        },
                    ]
                }
            }
        ]
    }

    parsed_schema = fastavro.parse_schema(schema)

    # Convert entries to expected format
    avro_entries = []
    for entry in entries:
        df = entry["data_file"]

        avro_entry = {
            "status": entry["status"],
            "snapshot_id": entry.get("snapshot_id"),
            "sequence_number": entry.get("sequence_number"),
            "file_sequence_number": entry.get("file_sequence_number"),
            "data_file": {
                "content": df["content"],
                "file_path": df["file_path"],
                "file_format": df["file_format"],
                "record_count": df["record_count"],
                "file_size_in_bytes": df["file_size_in_bytes"],
                "partition": {},
                "equality_ids": df.get("equality_ids"),
            }
        }
        avro_entries.append(avro_entry)

    with open(path, "wb") as f:
        fastavro.writer(f, parsed_schema, avro_entries)


def main():
    """Generate all test fixtures."""
    base_path = Path(__file__).parent

    print("Generating Iceberg test fixtures...")
    print(f"Output directory: {base_path}")
    print()

    try:
        generate_simple_table(base_path)
    except Exception as e:
        print(f"Error generating simple_table: {e}")
        raise

    try:
        generate_time_travel_table(base_path)
    except Exception as e:
        print(f"Error generating time_travel_table: {e}")
        raise

    try:
        generate_positional_deletes_table(base_path)
    except Exception as e:
        print(f"Error generating positional_deletes_table: {e}")
        raise

    try:
        generate_equality_deletes_table(base_path)
    except Exception as e:
        print(f"Error generating equality_deletes_table: {e}")
        raise

    print()
    print("Done! Test fixtures generated successfully.")
    print()
    print("To regenerate fixtures, run:")
    print("  cd internal/io/iceberg/testdata")
    print("  python3 generate_fixtures.py")


if __name__ == "__main__":
    main()

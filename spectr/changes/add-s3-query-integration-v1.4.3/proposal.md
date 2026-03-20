# Change: Wire S3/Cloud Filesystem to SQL Query Layer

## Why

The dukdb-go codebase already has a cloud filesystem backend (`internal/io/filesystem/`) with S3, GCS, Azure, and HTTP support, and a secret management system (`internal/secret/`). The executor layer already contains `FileSystemProvider` (`internal/executor/copy_cloud.go`) that bridges these systems. However, integration testing is incomplete, error handling for cloud authentication failures needs hardening, and edge cases across all table function variants and COPY operations need verification. This proposal tracks the remaining work to make S3/cloud query integration production-ready.

## What Changes

- Harden error handling when cloud credentials are missing or invalid across table functions and COPY statements
- Ensure all table function variants (`read_csv_auto`, `read_json_auto`, `read_ndjson`) properly delegate to cloud-aware code paths
- Add integration tests for `read_parquet('s3://...')`, `read_csv('s3://...')`, `read_json('s3://...')` using LocalStack
- Add integration tests for `COPY table TO 's3://...'` and `COPY table FROM 's3://...'`
- Verify credential flow from `CREATE SECRET` through `FileSystemProvider` to `S3FileSystem`
- Ensure `Msg:` field (not `Message:`) is used in all `dukdb.Error` returns for cloud operations

## Impact

- Affected specs: `cloud-storage`, `secrets`, `filesystem`, `copy-statement`, `file-io`
- Affected code:
  - `internal/executor/copy_cloud.go` (FileSystemProvider, credential application)
  - `internal/executor/table_function_csv.go` (read_csv, read_csv_auto cloud paths)
  - `internal/executor/table_function_json.go` (read_json, read_json_auto, read_ndjson cloud paths)
  - `internal/executor/table_function_parquet.go` (read_parquet cloud paths)
  - `internal/executor/physical_copy.go` (COPY FROM/TO cloud paths)
  - `internal/executor/glob_cloud.go` (cloud glob expansion)
  - `internal/io/filesystem/factory.go` (scheme registration, IsCloudURL)
  - `internal/secret/provider.go` (credential provider chain)

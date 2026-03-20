## 1. Error Handling Hardening

- [ ] 1.1 Audit `internal/executor/copy_cloud.go` FileSystemProvider methods for missing/invalid secret error paths; ensure all errors use `dukdb.Error{Msg: ...}` format
- [ ] 1.2 Add clear error message when `GetFileSystem()` finds no matching secret for a cloud URL, suggesting `CREATE SECRET`
- [ ] 1.3 Add clear error message when S3 authentication fails (access denied, invalid credentials)
- [ ] 1.4 Add clear error message when S3 endpoint is unreachable (connection failure)

## 2. Table Function Cloud Path Verification

- [ ] 2.1 Verify `read_csv_auto` in `internal/executor/table_function_csv.go` delegates to cloud-aware code path when given an S3 URL
- [ ] 2.2 Verify `read_json_auto` in `internal/executor/table_function_json.go` delegates to cloud-aware code path when given an S3 URL
- [ ] 2.3 Verify `read_ndjson` in `internal/executor/table_function_json.go` delegates to cloud-aware code path when given an S3 URL
- [ ] 2.4 Verify all three S3 scheme variants (`s3://`, `s3a://`, `s3n://`) are handled identically in table functions

## 3. COPY Statement Cloud Path Verification

- [ ] 3.1 Verify `COPY table FROM 's3://...' (FORMAT CSV)` works end-to-end in `internal/executor/physical_copy.go`
- [ ] 3.2 Verify `COPY table FROM 's3://...' (FORMAT PARQUET)` works end-to-end
- [ ] 3.3 Verify `COPY table TO 's3://...' (FORMAT CSV)` works end-to-end
- [ ] 3.4 Verify `COPY table TO 's3://...' (FORMAT PARQUET)` works end-to-end
- [ ] 3.5 Verify `COPY (SELECT ...) TO 's3://...'` works end-to-end

## 4. Credential Flow Integration Tests

- [ ] 4.1 Create integration test: `CREATE SECRET` followed by `read_parquet('s3://...')` using LocalStack
- [ ] 4.2 Create integration test: `CREATE SECRET` followed by `read_csv('s3://...')` using LocalStack
- [ ] 4.3 Create integration test: `CREATE SECRET` followed by `read_json('s3://...')` using LocalStack
- [ ] 4.4 Create integration test: `CREATE SECRET` followed by `COPY FROM 's3://...'` using LocalStack
- [ ] 4.5 Create integration test: `CREATE SECRET` followed by `COPY TO 's3://...'` using LocalStack
- [ ] 4.6 Create integration test: scope-based secret matching prefers specific scope over global

## 5. Cloud Glob Expansion Tests

- [ ] 5.1 Create integration test: glob pattern `s3://bucket/data/*.parquet` expands and reads all matching files
- [ ] 5.2 Create integration test: recursive glob `s3://bucket/data/**/*.parquet` expands correctly
- [ ] 5.3 Create integration test: glob with no matches returns appropriate error

## 6. Edge Cases and Round-Trip Tests

- [ ] 6.1 Create integration test: write Parquet to S3 via COPY TO, then read back via read_parquet -- data matches
- [ ] 6.2 Create integration test: write CSV to S3 via COPY TO, then read back via read_csv -- data matches
- [ ] 6.3 Create test: query with S3 URL but no secret returns actionable error message
- [ ] 6.4 Create test: verify `dukdb.Error` uses `Msg:` field (not `Message:`) for all cloud error paths

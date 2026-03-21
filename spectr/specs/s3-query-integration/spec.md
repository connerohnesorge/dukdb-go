# S3 Query Integration Specification

## Requirements

### Requirement: S3 Table Function Integration

The system SHALL support reading data from S3-compatible storage using table functions with credentials resolved from the secret manager.

#### Scenario: Read Parquet from S3 with secret

- GIVEN a secret created via `CREATE SECRET my_s3 (TYPE S3, KEY_ID 'xxx', SECRET 'yyy', REGION 'us-east-1')`
- AND a Parquet file exists at `s3://my-bucket/data.parquet`
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data.parquet')`
- THEN the FileSystemProvider SHALL resolve credentials from the secret manager
- AND the query SHALL return the contents of the Parquet file

#### Scenario: Read CSV from S3 with secret

- GIVEN a secret created via `CREATE SECRET my_s3 (TYPE S3, KEY_ID 'xxx', SECRET 'yyy')`
- AND a CSV file exists at `s3://my-bucket/data.csv`
- WHEN executing `SELECT * FROM read_csv('s3://my-bucket/data.csv')`
- THEN the FileSystemProvider SHALL resolve credentials from the secret manager
- AND the query SHALL return the contents of the CSV file

#### Scenario: Read CSV auto-detect from S3

- GIVEN a valid S3 secret and a CSV file at `s3://my-bucket/data.csv`
- WHEN executing `SELECT * FROM read_csv_auto('s3://my-bucket/data.csv')`
- THEN the system SHALL detect CSV format automatically
- AND the query SHALL return the contents of the CSV file with inferred types

#### Scenario: Read JSON from S3 with secret

- GIVEN a valid S3 secret and a JSON file at `s3://my-bucket/data.json`
- WHEN executing `SELECT * FROM read_json('s3://my-bucket/data.json')`
- THEN the FileSystemProvider SHALL resolve credentials from the secret manager
- AND the query SHALL return the contents of the JSON file

#### Scenario: Read JSON auto-detect from S3

- GIVEN a valid S3 secret and a JSON file at `s3://my-bucket/data.json`
- WHEN executing `SELECT * FROM read_json_auto('s3://my-bucket/data.json')`
- THEN the system SHALL detect JSON format automatically
- AND the query SHALL return the contents of the JSON file with inferred types

#### Scenario: Read NDJSON from S3

- GIVEN a valid S3 secret and an NDJSON file at `s3://my-bucket/data.ndjson`
- WHEN executing `SELECT * FROM read_ndjson('s3://my-bucket/data.ndjson')`
- THEN the FileSystemProvider SHALL resolve credentials from the secret manager
- AND the query SHALL return the contents of the NDJSON file

#### Scenario: S3 URL scheme variants

- GIVEN a valid S3 secret
- WHEN executing table functions with `s3://`, `s3a://`, or `s3n://` URL schemes
- THEN all three schemes SHALL be recognized as cloud URLs
- AND the same credential resolution logic SHALL apply to all schemes

### Requirement: S3 COPY Statement Integration

The system SHALL support COPY FROM and COPY TO operations with S3-compatible storage using credentials resolved from the secret manager.

#### Scenario: COPY FROM S3 Parquet

- GIVEN a valid S3 secret and a Parquet file at `s3://my-bucket/source.parquet`
- AND a target table exists
- WHEN executing `COPY my_table FROM 's3://my-bucket/source.parquet' (FORMAT PARQUET)`
- THEN the system SHALL read from S3 using credentials from the secret manager
- AND the target table SHALL contain the data from the Parquet file

#### Scenario: COPY FROM S3 CSV

- GIVEN a valid S3 secret and a CSV file at `s3://my-bucket/source.csv`
- AND a target table exists
- WHEN executing `COPY my_table FROM 's3://my-bucket/source.csv' (FORMAT CSV)`
- THEN the system SHALL read from S3 using credentials from the secret manager
- AND the target table SHALL contain the data from the CSV file

#### Scenario: COPY TO S3 Parquet

- GIVEN a valid S3 secret and a table with data
- WHEN executing `COPY my_table TO 's3://my-bucket/output.parquet' (FORMAT PARQUET)`
- THEN the system SHALL write to S3 using credentials from the secret manager
- AND the Parquet file SHALL be readable via `read_parquet()`

#### Scenario: COPY TO S3 CSV

- GIVEN a valid S3 secret and a table with data
- WHEN executing `COPY my_table TO 's3://my-bucket/output.csv' (FORMAT CSV)`
- THEN the system SHALL write to S3 using credentials from the secret manager
- AND the CSV file SHALL be readable via `read_csv()`

#### Scenario: COPY query result TO S3

- GIVEN a valid S3 secret
- WHEN executing `COPY (SELECT 1 AS id, 'hello' AS msg) TO 's3://my-bucket/query.parquet' (FORMAT PARQUET)`
- THEN the system SHALL write the query result to S3
- AND the file SHALL contain the query result data

### Requirement: S3 Credential Resolution

The system SHALL resolve S3 credentials from the secret manager using scope-based matching when cloud URLs are accessed via table functions or COPY statements.

#### Scenario: Scope-based secret matching

- GIVEN a secret with scope `s3://my-bucket/prefix/`
- AND another secret with global scope
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/prefix/data.parquet')`
- THEN the scoped secret SHALL be preferred over the global secret

#### Scenario: No matching secret for S3 URL

- GIVEN no S3 secrets are configured
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data.parquet')`
- THEN the system SHALL return an error with a clear message indicating no credentials are available
- AND the error SHALL use the `Msg:` field of `dukdb.Error`

#### Scenario: Invalid credentials in secret

- GIVEN an S3 secret with invalid credentials (wrong key or secret)
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data.parquet')`
- THEN the system SHALL return an error indicating authentication failure
- AND the error SHALL use the `Msg:` field of `dukdb.Error`

#### Scenario: Credential chain provider resolution

- GIVEN a secret created via `CREATE SECRET my_s3 (TYPE S3, PROVIDER credential_chain)`
- WHEN executing a table function with an S3 URL
- THEN the system SHALL attempt credential resolution in order: config, env, shared config, IMDSv2
- AND use the first provider that returns valid credentials

### Requirement: S3 Cloud Glob Expansion

The system SHALL support glob patterns in S3 URLs for reading multiple files via table functions.

#### Scenario: Glob pattern with wildcard

- GIVEN a valid S3 secret and multiple Parquet files at `s3://my-bucket/data/part_*.parquet`
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data/part_*.parquet')`
- THEN the system SHALL expand the glob pattern against S3 listing
- AND return combined results from all matching files

#### Scenario: Recursive glob pattern

- GIVEN a valid S3 secret and Parquet files in nested directories under `s3://my-bucket/data/`
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data/**/*.parquet')`
- THEN the system SHALL recursively list and match files
- AND return combined results from all matching files

#### Scenario: Glob pattern with no matches

- GIVEN a valid S3 secret
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/nonexistent/*.parquet')`
- THEN the system SHALL return an error indicating no files matched the pattern

### Requirement: S3 Cloud Error Messages

The system SHALL provide clear, actionable error messages for all S3 cloud operation failures using the `Msg:` field of `dukdb.Error`.

#### Scenario: Missing secret error message

- GIVEN no S3 secret is configured
- WHEN a query references an S3 URL
- THEN the error message SHALL indicate that no matching secret was found for the S3 URL
- AND the error message SHALL suggest creating a secret with `CREATE SECRET`

#### Scenario: Connection failure error message

- GIVEN a valid S3 secret but the S3 endpoint is unreachable
- WHEN a query references an S3 URL
- THEN the error message SHALL indicate a connection failure
- AND the error message SHALL include the endpoint that was attempted

#### Scenario: Access denied error message

- GIVEN a valid S3 secret but the credentials lack permission for the requested operation
- WHEN a query references an S3 URL
- THEN the error message SHALL indicate an access denied or authorization failure
- AND the error message SHALL include the S3 path that was denied


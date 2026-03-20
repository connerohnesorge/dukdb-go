# Cloud Storage Specification

## Requirements

### Requirement: Azure Blob Storage Write Verification

The system SHALL support writing data to Azure Blob Storage via COPY TO statements.

#### Scenario: Write Parquet to Azure

- GIVEN a table with data
- AND an Azure secret is configured with valid credentials
- WHEN executing `COPY my_table TO 'azure://container/path/data.parquet' (FORMAT PARQUET)`
- THEN a Parquet file SHALL be created in Azure Blob Storage
- AND the file SHALL be readable by other tools

#### Scenario: Write CSV to Azure

- GIVEN a table with data
- AND an Azure secret is configured
- WHEN executing `COPY my_table TO 'az://container/path/data.csv' (FORMAT CSV)`
- THEN a CSV file SHALL be created in Azure Blob Storage
- AND the file SHALL be accessible via Azure SDK

#### Scenario: Write with connection string authentication

- GIVEN an Azure secret with connection string: `CREATE SECRET azure (TYPE AZURE, CONNECTION_STRING '...')`
- WHEN writing to Azure Blob Storage
- THEN the connection string SHALL be used for authentication
- AND write operation SHALL succeed

#### Scenario: Write with account key authentication

- GIVEN an Azure secret with account credentials: `CREATE SECRET azure (TYPE AZURE, ACCOUNT_NAME 'acct', ACCOUNT_KEY 'key')`
- WHEN writing to Azure Blob Storage
- THEN the account credentials SHALL be used for authentication
- AND write operation SHALL succeed

#### Scenario: Large file multipart upload

- GIVEN a table with >256MB of data
- WHEN writing to Azure Blob Storage
- THEN multipart upload SHALL be used if available
- AND the file SHALL be written correctly

#### Scenario: Write to existing container

- GIVEN an existing Azure Blob Storage container
- WHEN writing a file to that container
- THEN the container SHALL not be created
- AND the file SHALL be written to the container

#### Scenario: Write to non-existent container

- GIVEN Azure credentials with create container permission
- WHEN writing to a non-existent container
- THEN the container SHALL be created automatically
- AND the file SHALL be written to the new container

### Requirement: Azure Read/Write Parity

The system SHALL provide read and write access to Azure Blob Storage with feature parity.

#### Scenario: Read what you write

- GIVEN data written to Azure via COPY TO
- WHEN reading the same data via read_parquet()
- THEN all rows SHALL match the original data
- AND column types SHALL be preserved

#### Scenario: Round-trip through Azure

- GIVEN local table `local_table`
- WHEN executing:
  1. `COPY local_table TO 'azure://container/data.parquet'`
  2. `CREATE TABLE azure_table AS SELECT * FROM read_parquet('azure://container/data.parquet')`
- THEN `azure_table` SHALL contain identical data to `local_table`

### Requirement: Azure URL Formats

The system SHALL support multiple Azure URL formats.

#### Scenario: azure:// scheme

- GIVEN a file at 'azure://container/blob.parquet'
- WHEN reading or writing
- THEN Azure Blob Storage SHALL be accessed

#### Scenario: az:// scheme

- GIVEN a file at 'az://container/blob.parquet'
- WHEN reading or writing
- THEN Azure Blob Storage SHALL be accessed

#### Scenario: wasb:// scheme (legacy)

- GIVEN a file at 'wasb://container/blob.parquet'
- WHEN reading or writing
- THEN Azure Blob Storage SHALL be accessed (legacy compatibility)

### Requirement: FileSystemProvider Secret Resolution

The system SHALL automatically resolve secrets when opening cloud storage URLs, using the longest-prefix match from the secret manager to configure filesystem credentials. This is handled by the existing `FileSystemProvider` in `internal/executor/copy_cloud.go` which combines `FileSystemFactory` + `secret.Manager`.

#### Scenario: S3 read with secret-based authentication

- GIVEN a secret created via `CREATE SECRET my_s3 (TYPE S3, KEY_ID 'AKID', SECRET 'secret', REGION 'us-east-1')`
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data.parquet')`
- THEN the FileSystemProvider SHALL resolve the S3 secret for the URL
- AND the S3FileSystem SHALL be created with the access key, secret key, and region from the secret
- AND the Parquet file SHALL be read successfully

#### Scenario: S3 read with scoped secret

- GIVEN two secrets:
  - `CREATE SECRET global_s3 (TYPE S3, KEY_ID 'key1', SECRET 'sec1', SCOPE '')`
  - `CREATE SECRET bucket_s3 (TYPE S3, KEY_ID 'key2', SECRET 'sec2', SCOPE 's3://specific-bucket/')`
- WHEN executing `SELECT * FROM read_parquet('s3://specific-bucket/data.parquet')`
- THEN the FileSystemProvider SHALL use `bucket_s3` (longest prefix match)
- AND the S3FileSystem SHALL be configured with `key2`/`sec2`

#### Scenario: S3 read without secret (public bucket)

- GIVEN no S3 secrets are configured
- WHEN executing `SELECT * FROM read_parquet('s3://public-bucket/data.parquet')`
- THEN the FileSystemProvider SHALL attempt anonymous access via the default S3FileSystem
- AND public objects SHALL be readable

#### Scenario: Secret dropped then re-read

- GIVEN a secret `my_s3` was created and used to read a file
- WHEN executing `DROP SECRET my_s3`
- AND then executing `SELECT * FROM read_parquet('s3://private-bucket/data.parquet')`
- THEN the FileSystemProvider SHALL not find a matching secret
- AND the read SHALL fall back to anonymous access
- AND private objects SHALL return an access denied error

### Requirement: S3 Read Operations

The system SHALL support reading files from Amazon S3 and S3-compatible storage via SQL.

#### Scenario: Read single Parquet file from S3

- GIVEN a Parquet file at `s3://bucket/data.parquet`
- AND valid S3 credentials via a secret
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data.parquet')`
- THEN all rows from the Parquet file SHALL be returned
- AND column types SHALL match the Parquet schema

#### Scenario: Read CSV file from S3

- GIVEN a CSV file at `s3://bucket/data.csv` with header row
- AND valid S3 credentials
- WHEN executing `SELECT * FROM read_csv('s3://bucket/data.csv')`
- THEN all rows SHALL be returned with columns from the header

#### Scenario: Read JSON file from S3

- GIVEN a JSON array file at `s3://bucket/data.json`
- AND valid S3 credentials
- WHEN executing `SELECT * FROM read_json('s3://bucket/data.json')`
- THEN all JSON objects SHALL be returned as rows

#### Scenario: Read NDJSON file from S3

- GIVEN a newline-delimited JSON file at `s3://bucket/data.ndjson`
- AND valid S3 credentials
- WHEN executing `SELECT * FROM read_ndjson('s3://bucket/data.ndjson')`
- THEN each line SHALL be parsed as a separate JSON object

#### Scenario: Read with S3-compatible endpoint (MinIO)

- GIVEN a secret with custom endpoint: `CREATE SECRET minio (TYPE S3, KEY_ID 'key', SECRET 'sec', ENDPOINT 'http://localhost:9000', URL_STYLE 'path')`
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data.parquet')`
- THEN the S3FileSystem SHALL use the custom endpoint
- AND path-style addressing SHALL be used

#### Scenario: Read with s3a:// and s3n:// scheme aliases

- GIVEN a Parquet file at `s3://bucket/data.parquet`
- WHEN executing `SELECT * FROM read_parquet('s3a://bucket/data.parquet')`
- THEN the file SHALL be read using the S3FileSystem
- AND `s3n://` SHALL also route to S3FileSystem

### Requirement: S3 Write Operations

The system SHALL support writing files to Amazon S3 via COPY TO statements.

#### Scenario: Write Parquet to S3

- GIVEN a table `my_table` with data
- AND valid S3 credentials via a secret
- WHEN executing `COPY my_table TO 's3://bucket/output.parquet' (FORMAT PARQUET)`
- THEN a Parquet file SHALL be created at the S3 key
- AND the file SHALL be readable by `read_parquet`

#### Scenario: Write CSV to S3

- GIVEN a table `my_table` with data
- AND valid S3 credentials
- WHEN executing `COPY my_table TO 's3://bucket/output.csv' (FORMAT CSV, HEADER)`
- THEN a CSV file with header SHALL be created at the S3 key

#### Scenario: Write large file with multipart upload

- GIVEN a table with >100MB of data
- AND valid S3 credentials
- WHEN executing `COPY large_table TO 's3://bucket/large.parquet' (FORMAT PARQUET)`
- THEN multipart upload SHALL be used (parts >= 5MB each)
- AND the file SHALL be assembled correctly in S3

#### Scenario: Write query results to S3

- GIVEN valid S3 credentials
- WHEN executing `COPY (SELECT id, name FROM my_table WHERE active) TO 's3://bucket/filtered.csv' (FORMAT CSV)`
- THEN only the filtered results SHALL be written to S3

### Requirement: HTTP/HTTPS Read Operations

The system SHALL support reading files from HTTP and HTTPS URLs.

#### Scenario: Read CSV from HTTPS URL

- GIVEN a CSV file available at `https://example.com/data.csv`
- WHEN executing `SELECT * FROM read_csv('https://example.com/data.csv')`
- THEN the file SHALL be downloaded and parsed as CSV
- AND all rows SHALL be returned

#### Scenario: Read Parquet from HTTPS URL with range requests

- GIVEN a Parquet file available at `https://example.com/data.parquet`
- AND the server supports HTTP range requests (Accept-Ranges: bytes)
- WHEN executing `SELECT col1 FROM read_parquet('https://example.com/data.parquet')`
- THEN only the necessary byte ranges SHALL be fetched (footer + requested column chunks)

#### Scenario: Read with HTTP bearer token

- GIVEN a secret: `CREATE SECRET my_http (TYPE HTTP, BEARER_TOKEN 'tok123', SCOPE 'https://api.example.com/')`
- AND a CSV file at `https://api.example.com/data.csv` requiring authentication
- WHEN executing `SELECT * FROM read_csv('https://api.example.com/data.csv')`
- THEN the Authorization header SHALL include `Bearer tok123`
- AND the file SHALL be read successfully

#### Scenario: HTTP write is rejected

- WHEN executing `COPY my_table TO 'https://example.com/output.csv'`
- THEN the operation SHALL fail with an error indicating HTTP write is not supported

### Requirement: Cloud Glob Pattern Expansion

The system SHALL expand glob patterns on cloud storage to match multiple files.

#### Scenario: S3 glob with wildcard

- GIVEN S3 objects: `s3://bucket/data/part1.parquet`, `s3://bucket/data/part2.parquet`, `s3://bucket/data/readme.txt`
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data/*.parquet')`
- THEN both `part1.parquet` and `part2.parquet` SHALL be read
- AND `readme.txt` SHALL NOT be included

#### Scenario: S3 glob with recursive wildcard

- GIVEN S3 objects at multiple levels: `s3://bucket/year=2024/month=01/data.parquet`, `s3://bucket/year=2024/month=02/data.parquet`
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/**/*.parquet')`
- THEN all `.parquet` files at any depth SHALL be matched

#### Scenario: S3 glob with character class

- GIVEN S3 objects: `s3://bucket/data_1.csv`, `s3://bucket/data_2.csv`, `s3://bucket/data_a.csv`
- WHEN executing `SELECT * FROM read_csv('s3://bucket/data_[0-9].csv')`
- THEN `data_1.csv` and `data_2.csv` SHALL be matched
- AND `data_a.csv` SHALL NOT be included

#### Scenario: Glob with no matches

- GIVEN no objects matching `s3://bucket/nonexistent/*.parquet`
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/nonexistent/*.parquet')`
- THEN an error SHALL be returned indicating no files match the pattern

#### Scenario: GCS glob pattern

- GIVEN GCS objects matching `gs://bucket/data/*.json`
- WHEN executing `SELECT * FROM read_json('gs://bucket/data/*.json')`
- THEN all matching JSON files SHALL be read

#### Scenario: Azure glob pattern

- GIVEN Azure blobs matching `azure://container/data/*.csv`
- WHEN executing `SELECT * FROM read_csv('azure://container/data/*.csv')`
- THEN all matching CSV files SHALL be read

### Requirement: GCS Read/Write Operations

The system SHALL support reading and writing files to Google Cloud Storage.

#### Scenario: Read Parquet from GCS

- GIVEN a Parquet file at `gs://bucket/data.parquet`
- AND a GCS secret: `CREATE SECRET my_gcs (TYPE GCS, SERVICE_ACCOUNT_JSON '...')`
- WHEN executing `SELECT * FROM read_parquet('gs://bucket/data.parquet')`
- THEN the file SHALL be read using GCS credentials

#### Scenario: Write CSV to GCS

- GIVEN valid GCS credentials
- WHEN executing `COPY my_table TO 'gs://bucket/output.csv' (FORMAT CSV)`
- THEN the CSV file SHALL be created in GCS

#### Scenario: GCS scheme alias

- GIVEN a file at `gs://bucket/data.parquet`
- WHEN executing `SELECT * FROM read_parquet('gcs://bucket/data.parquet')`
- THEN `gcs://` SHALL be treated as a synonym for `gs://`

### Requirement: Azure Blob Storage Read/Write Operations

The system SHALL support reading and writing files to Azure Blob Storage.

#### Scenario: Read Parquet from Azure

- GIVEN a Parquet file at `azure://container/data.parquet`
- AND an Azure secret: `CREATE SECRET my_az (TYPE AZURE, ACCOUNT_NAME 'acct', ACCOUNT_KEY 'key')`
- WHEN executing `SELECT * FROM read_parquet('azure://container/data.parquet')`
- THEN the file SHALL be read using Azure credentials

#### Scenario: Write to Azure with connection string

- GIVEN an Azure secret with connection string: `CREATE SECRET az (TYPE AZURE, CONNECTION_STRING '...')`
- WHEN executing `COPY my_table TO 'az://container/output.parquet' (FORMAT PARQUET)`
- THEN the file SHALL be written using connection string authentication

#### Scenario: Azure scheme alias

- GIVEN a file at `azure://container/data.parquet`
- WHEN executing `SELECT * FROM read_parquet('az://container/data.parquet')`
- THEN `az://` SHALL be treated as a synonym for `azure://`

### Requirement: COPY Statement Cloud URL Support

The system SHALL support cloud URLs in COPY FROM and COPY TO statements.

#### Scenario: COPY FROM S3

- GIVEN a CSV file at `s3://bucket/data.csv`
- AND valid S3 credentials
- WHEN executing `COPY my_table FROM 's3://bucket/data.csv' (FORMAT CSV, HEADER)`
- THEN the data SHALL be loaded into `my_table`

#### Scenario: COPY TO S3

- GIVEN a table `my_table` with data
- AND valid S3 credentials
- WHEN executing `COPY my_table TO 's3://bucket/output.parquet' (FORMAT PARQUET)`
- THEN the data SHALL be written to S3

#### Scenario: COPY FROM with glob

- GIVEN multiple CSV files matching `s3://bucket/data/*.csv`
- AND valid S3 credentials
- WHEN executing `COPY my_table FROM 's3://bucket/data/*.csv' (FORMAT CSV)`
- THEN all matching files SHALL be loaded into `my_table`

#### Scenario: COPY FROM HTTPS

- GIVEN a CSV file at `https://example.com/data.csv`
- WHEN executing `COPY my_table FROM 'https://example.com/data.csv' (FORMAT CSV)`
- THEN the data SHALL be downloaded and loaded into `my_table`

### Requirement: Error Handling for Cloud Operations

The system SHALL provide clear error messages for cloud storage failures.

#### Scenario: Invalid credentials

- GIVEN an S3 secret with wrong credentials
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data.parquet')`
- THEN the error message SHALL indicate authentication failure
- AND the error SHALL reference the URL

#### Scenario: Non-existent bucket

- GIVEN valid credentials but a non-existent bucket
- WHEN executing `SELECT * FROM read_parquet('s3://nonexistent-bucket/data.parquet')`
- THEN the error message SHALL indicate the bucket does not exist

#### Scenario: Non-existent object

- GIVEN valid credentials and an existing bucket
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/nonexistent.parquet')`
- THEN the error message SHALL indicate the object does not exist

#### Scenario: Network timeout

- GIVEN valid credentials
- AND the cloud service is unreachable
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data.parquet')`
- THEN the operation SHALL retry according to the retry configuration
- AND after exhausting retries, a timeout error SHALL be returned

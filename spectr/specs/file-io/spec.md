# File Io Specification

## Requirements

### Requirement: CSV File Reading

CSV file reading SHALL support cloud storage URLs through the FileSystem interface.

**Previous**: Only supported local file paths
**Updated**: Supports S3, GCS, Azure, and HTTP URLs via FileSystem

#### Scenario: Read CSV from S3
- GIVEN an S3 URL 's3://bucket/data.csv'
- WHEN reading with CSV reader
- THEN file is downloaded from S3
- AND CSV parsing proceeds normally

#### Scenario: Read CSV with parallel loading from S3
- GIVEN an S3 URL for a large CSV file
- WHEN reading with parallel option enabled
- THEN file is read in parallel chunks from S3
- AND throughput is improved

### Requirement: CSV File Writing

The system SHALL write DataChunks to CSV format.

#### Scenario: Write simple CSV
- GIVEN a DataChunk with columns id (INTEGER), name (VARCHAR)
- WHEN writing to CSV with header enabled
- THEN output file has header row followed by data rows

#### Scenario: Quote fields with special characters
- GIVEN a VARCHAR column with value containing comma
- WHEN writing to CSV
- THEN field is quoted: `"value,with,commas"`

#### Scenario: Custom delimiter
- GIVEN DELIMITER option set to tab
- WHEN writing to CSV
- THEN fields are separated by tab character

#### Scenario: Force quote specific columns
- GIVEN force_quote option includes column 'name'
- WHEN writing to CSV
- THEN column 'name' values are always quoted

### Requirement: JSON File Reading

JSON file reading SHALL support cloud storage URLs through the FileSystem interface.

**Previous**: Only supported local file paths
**Updated**: Supports S3, GCS, Azure, and HTTP URLs via FileSystem

#### Scenario: Read JSON from GCS
- GIVEN a GCS URL 'gs://bucket/data.json'
- WHEN reading with JSON reader
- THEN file is downloaded from GCS
- AND JSON parsing proceeds normally

### Requirement: JSON File Writing

The system SHALL write DataChunks to JSON format.

#### Scenario: Write JSON array
- GIVEN a DataChunk with data
- WHEN writing with format=array
- THEN output is valid JSON array of objects

#### Scenario: Write NDJSON
- GIVEN a DataChunk with data
- WHEN writing with format=ndjson
- THEN output has one JSON object per line

### Requirement: Parquet File Reading

Parquet file reading SHALL support cloud storage URLs through the FileSystem interface with efficient range requests.

**Previous**: Only supported local file paths
**Updated**: Supports S3, GCS, Azure, and HTTP URLs via FileSystem

#### Scenario: Read Parquet from S3 with column projection
- GIVEN an S3 URL for a Parquet file with 10 columns
- WHEN reading with projection for 2 columns
- THEN only the required byte ranges are fetched from S3
- AND network I/O is reduced by ~80%

#### Scenario: Read Parquet row groups from cloud storage
- GIVEN a Parquet file with 10 row groups on S3
- WHEN reading specific row groups
- THEN only the required row group bytes are fetched
- AND row group footer is read with separate range request

#### Scenario: Parallel Parquet read from cloud
- GIVEN a large Parquet file on S3
- WHEN reading with parallel option
- THEN multiple column chunks are fetched concurrently
- AND read performance is improved

### Requirement: Parquet File Writing

Parquet file writing SHALL support cloud storage URLs through the FileSystem interface with multipart uploads.

**Previous**: Only supported local file paths
**Updated**: Supports S3, GCS, Azure, and HTTP URLs via FileSystem

#### Scenario: Write Parquet to S3
- GIVEN a DataChunk to write
- WHEN writing to 's3://bucket/output.parquet'
- THEN Parquet file is uploaded to S3
- AND multipart upload is used for large files

#### Scenario: Write Parquet to Azure
- GIVEN a DataChunk to write
- WHEN writing to 'azure://container/output.parquet'
- THEN Parquet file is uploaded to Azure Blob Storage
- AND block upload is used for large files

### Requirement: Compression Support

Compression handling SHALL work transparently with cloud storage URLs.

**Previous**: Only local files
**Updated**: Supports cloud URLs with transparent compression/decompression

#### Scenario: Read gzip-compressed CSV from S3
- GIVEN a gzip-compressed file on S3
- WHEN reading as CSV
- THEN file is downloaded and decompressed transparently
- AND parsed CSV data is returned

#### Scenario: Write gzip-compressed file to GCS
- GIVEN data to write as CSV
- WHEN compression option is 'gzip'
- AND writing to 'gs://bucket/data.csv.gz'
- THEN data is compressed and uploaded to GCS

### Requirement: File Format Detection

File format detection SHALL work with cloud storage URLs by reading magic bytes.

**Previous**: Only local files
**Updated**: Supports cloud URLs with partial reads for magic byte detection

#### Scenario: Detect Parquet format from S3
- GIVEN an S3 URL with unknown file extension
- WHEN format is auto-detected
- THEN first bytes are fetched from S3
- AND format is determined from magic bytes (PAR1)

#### Scenario: Detect compression from S3
- GIVEN an S3 URL with compressed file
- WHEN compression is auto-detected
- THEN magic bytes are fetched with minimal range
- AND compression type is detected

### Requirement: sniff_csv Table Function

The system SHALL provide sniff_csv to inspect CSV format without loading data.

#### Scenario: Detect CSV properties
- GIVEN a CSV file
- WHEN executing `SELECT * FROM sniff_csv('file.csv')`
- THEN returns detected delimiter, quote character, has_header, column names/types

#### Scenario: Use sniffed settings
- GIVEN sniff_csv results
- WHEN using detected settings with read_csv
- THEN file is parsed correctly

### Requirement: Cloud URL Support

The system SHALL support cloud storage URLs in file operations for S3, GCS, Azure, and HTTP/HTTPS protocols.

#### Scenario: S3 URL parsing
- GIVEN a file path 's3://my-bucket/data.parquet'
- WHEN parsing the URL
- THEN scheme is 's3', bucket is 'my-bucket', key is 'data.parquet'
- AND the appropriate S3 filesystem is selected

#### Scenario: GCS URL parsing
- GIVEN a file path 'gs://my-bucket/data.parquet'
- WHEN parsing the URL
- THEN scheme is 'gs', bucket is 'my-bucket', key is 'data.parquet'
- AND the appropriate GCS filesystem is selected

#### Scenario: Azure URL parsing
- GIVEN a file path 'azure://my-container/my-blob.parquet'
- WHEN parsing the URL
- THEN scheme is 'azure', container is 'my-container', blob is 'my-blob.parquet'
- AND the appropriate Azure filesystem is selected

#### Scenario: HTTP URL parsing
- GIVEN a file path 'https://example.com/data.parquet'
- WHEN parsing the URL
- THEN scheme is 'https', authority is 'example.com', path is '/data.parquet'
- AND the appropriate HTTP filesystem is selected

#### Scenario: URL with query parameters
- GIVEN a file path 's3://bucket/data.parquet?region=us-west-2'
- WHEN parsing the URL
- THEN scheme, bucket, key are extracted
- AND query parameters include 'region=us-west-2'

#### Scenario: S3 virtual host style
- GIVEN a file path 'my-bucket.s3.amazonaws.com/data.parquet'
- WHEN parsing as S3 virtual host style
- THEN bucket is 'my-bucket', key is 'data.parquet'
- AND endpoint is inferred as 's3.amazonaws.com'

### Requirement: FileSystem Interface

The system SHALL provide a pluggable FileSystem interface for abstracting file operations across local and cloud storage.

#### Scenario: Open local file
- GIVEN a local file path '/path/to/file.parquet'
- WHEN opening with local filesystem
- THEN a File interface is returned
- AND Read, Write, Seek operations work as expected

#### Scenario: Open S3 object
- GIVEN an S3 URL 's3://bucket/object.parquet'
- WHEN opening with S3 filesystem
- THEN a File interface is returned
- AND ReadAt supports range requests for partial reads

#### Scenario: Stat file info
- GIVEN a file path or cloud URL
- WHEN calling Stat
- THEN FileInfo is returned with Name, Size, ModTime, IsDir

#### Scenario: Read directory contents
- GIVEN a directory path or cloud bucket URL
- WHEN calling ReadDir
- THEN a list of DirEntry is returned with names and types

#### Scenario: FileSystem capabilities
- GIVEN a FileSystem implementation
- WHEN querying capabilities
- THEN SupportsSeek, SupportsRange, SupportsWrite are reported correctly
- AND local filesystem reports all capabilities
- AND cloud filesystems report supported operations

### Requirement: S3 File Operations

The system SHALL provide full read/write access to S3 objects using AWS SDK v2.

#### Scenario: Read S3 object with range request
- GIVEN an S3 object of size 1MB
- WHEN reading with ReadAt at offset 100KB for 50KB
- THEN only bytes 100KB-150KB are retrieved from S3
- AND network bandwidth is reduced proportionally

#### Scenario: Write to S3 object
- GIVEN an S3 bucket with write permissions
- WHEN writing data to 's3://bucket/path/file.parquet'
- THEN data is uploaded to S3
- AND object is created or overwritten

#### Scenario: S3 multipart upload for large files
- GIVEN a dataset of 100MB to write
- WHEN writing to S3 with multipart enabled
- THEN file is uploaded in 5MB chunks
- AND progress is tracked for each part

#### Scenario: S3 stat returns object metadata
- GIVEN an S3 object with metadata
- WHEN calling Stat
- THEN ContentLength, LastModified are returned
- AND ContentType is available if set

#### Scenario: S3 credential providers
- GIVEN different credential configurations
- WHEN opening S3 filesystem
- THEN credentials are resolved in priority order:
  1. Explicit access key ID and secret access key
  2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
  3. AWS CLI shared config (~/.aws/config, ~/.aws/credentials)
  4. IAM instance metadata (EC2) or task metadata (ECS)

#### Scenario: S3 session token support
- GIVEN temporary credentials with session token
- WHEN configuring S3 filesystem
- THEN session token is included in requests
- AND temporary credentials work for the session duration

### Requirement: GCS File Operations

The system SHALL provide full read/write access to GCS objects using the official Go SDK.

#### Scenario: Read GCS object
- GIVEN a GCS object in a bucket
- WHEN opening with GCS filesystem
- THEN data can be read using the File interface
- AND range requests are supported

#### Scenario: Write to GCS object
- GIVEN a GCS bucket with write permissions
- WHEN writing data to 'gs://bucket/path/file.parquet'
- THEN data is uploaded to GCS
- AND object is created or overwritten

#### Scenario: GCS credential from service account
- GIVEN a service account key file
- WHEN configuring GCS filesystem
- THEN credentials are loaded from the key file
- AND operations use the service account identity

#### Scenario: GCS credential from environment
- GIVEN GOOGLE_APPLICATION_CREDENTIALS environment variable
- WHEN creating GCS filesystem
- THEN credentials are loaded from the file path
- AND default credential chain is used if not set

### Requirement: Azure Blob Storage Operations

The system SHALL provide full read/write access to Azure Blob Storage using Azure SDK.

#### Scenario: Read Azure blob
- GIVEN an Azure blob in a container
- WHEN opening with Azure filesystem
- THEN data can be read using the File interface
- AND range requests are supported

#### Scenario: Write to Azure blob
- GIVEN an Azure container with write permissions
- WHEN writing data to 'azure://container/blob.parquet'
- THEN data is uploaded to Azure
- AND blob is created or overwritten

#### Scenario: Azure credential from connection string
- GIVEN Azure storage connection string
- WHEN configuring Azure filesystem
- THEN credentials are parsed from connection string
- AND operations use the account name and key

#### Scenario: Azure credential from environment
- GIVEN AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY
- WHEN creating Azure filesystem
- THEN credentials are loaded from environment
- AND connection is established with account credentials

### Requirement: HTTP/HTTPS File Operations

The system SHALL provide read access to HTTP/HTTPS URLs with range request support.

#### Scenario: Read HTTP file with range header
- GIVEN an HTTP endpoint serving a file
- WHEN reading with range header
- THEN HTTP 206 Partial Content is returned
- AND only requested bytes are received

#### Scenario: HTTP redirect handling
- GIVEN an HTTP URL with 301 redirect
- WHEN following redirects is enabled
- THEN the request is redirected to the new location
- AND redirect limits are respected

#### Scenario: HTTP HEAD for metadata
- GIVEN an HTTP URL
- WHEN calling Stat
- THEN HEAD request is sent
- AND Content-Length, Content-Type are returned

#### Scenario: HTTP timeout configuration
- GIVEN an HTTP endpoint with slow response
- WHEN timeout is configured to 10 seconds
- THEN request fails with timeout error after 10 seconds

### Requirement: FileSystem Factory

The system SHALL provide a factory for creating FileSystem instances based on URL schemes.

#### Scenario: Get filesystem by URL
- GIVEN a URL with known scheme
- WHEN requesting filesystem from factory
- THEN appropriate FileSystem implementation is returned

#### Scenario: Register custom filesystem
- GIVEN a custom FileSystem implementation
- WHEN registering for a scheme
- THEN factory returns custom implementation for that scheme

#### Scenario: Unknown scheme handling
- GIVEN a URL with unknown scheme
- WHEN requesting filesystem
- THEN error is returned with "unsupported scheme"

#### Scenario: Empty scheme defaults to local
- GIVEN a path without scheme prefix
- WHEN requesting filesystem
- THEN local filesystem is returned

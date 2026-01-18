# File Io Specification

## Requirements

### Requirement: CSV File Reading

CSV file reading SHALL support glob patterns and array of files for reading multiple files in a single query.

**Previous**: Only supported single file paths
**Updated**: Supports single files, glob patterns, and arrays of files

#### Scenario: Read CSV from single file
- GIVEN a file 'data.csv'
- WHEN reading with `read_csv('data.csv')`
- THEN file is parsed as CSV
- AND rows are returned

#### Scenario: Read CSV from glob pattern
- GIVEN files 'data/file1.csv', 'data/file2.csv'
- WHEN reading with `read_csv('data/*.csv')`
- THEN both files are read and combined
- AND rows from all files are returned

#### Scenario: Read CSV from array of files
- GIVEN files 'a.csv', 'b.csv'
- WHEN reading with `read_csv(['a.csv', 'b.csv'])`
- THEN both files are read and combined
- AND rows are returned

#### Scenario: CSV schema detection with files_to_sniff
- GIVEN 100 CSV files with pattern 'data/*.csv'
- WHEN reading with `read_csv('data/*.csv', files_to_sniff=5)`
- THEN only the first 5 files are sampled for schema detection
- AND the inferred schema is applied to all 100 files
- AND performance is improved by not scanning all files upfront

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

JSON file reading SHALL support glob patterns and array of files for reading multiple files in a single query.

**Previous**: Only supported single file paths
**Updated**: Supports single files, glob patterns, and arrays of files

#### Scenario: Read JSON from single file
- GIVEN a file 'data.json'
- WHEN reading with `read_json('data.json')`
- THEN file is parsed as JSON
- AND rows are returned

#### Scenario: Read JSON from glob pattern
- GIVEN files 'data/file1.json', 'data/file2.json'
- WHEN reading with `read_json('data/*.json')`
- THEN both files are read and combined
- AND rows from all files are returned

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

Parquet file reading SHALL support glob patterns and array of files for reading multiple files in a single query with efficient range requests.

**Previous**: Only supported single file paths
**Updated**: Supports single files, glob patterns, and arrays of files

#### Scenario: Read Parquet from single file
- GIVEN a file 'data.parquet'
- WHEN reading with `read_parquet('data.parquet')`
- THEN file is parsed as Parquet
- AND rows are returned

#### Scenario: Read Parquet from glob pattern
- GIVEN files 'data/file1.parquet', 'data/file2.parquet'
- WHEN reading with `read_parquet('data/*.parquet')`
- THEN both files are read and combined
- AND rows from all files are returned

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

### Requirement: Glob Pattern Matching

The system SHALL support glob patterns in all file table functions (read_csv, read_json, read_parquet, read_xlsx, read_arrow) for reading multiple files in a single query.

#### Scenario: Single wildcard matches multiple files
- GIVEN files 'data/file1.csv', 'data/file2.csv', 'data/file3.csv' exist
- WHEN executing `SELECT * FROM read_csv('data/*.csv')`
- THEN all three CSV files are read and combined
- AND rows from all files are returned in a single result set

#### Scenario: Recursive wildcard matches nested directories
- GIVEN files 'data/2024/01/sales.parquet', 'data/2024/02/sales.parquet', 'data/2025/01/sales.parquet'
- WHEN executing `SELECT * FROM read_parquet('data/**/*.parquet')`
- THEN all Parquet files in all subdirectories are read
- AND rows from all files are returned

#### Scenario: Character class pattern
- GIVEN files 'log_a.json', 'log_b.json', 'log_1.json'
- WHEN executing `SELECT * FROM read_json('log_[a-z].json')`
- THEN only 'log_a.json' and 'log_b.json' are read
- AND 'log_1.json' is excluded

#### Scenario: Bracket negation pattern
- GIVEN files 'file_a.csv', 'file_b.csv', 'file_1.csv', 'file_2.csv'
- WHEN executing `SELECT * FROM read_csv('file_[!0-9].csv')`
- THEN only 'file_a.csv' and 'file_b.csv' are read
- AND numeric suffixed files are excluded

#### Scenario: Multiple recursive wildcards cause error
- GIVEN pattern 'data/**/subdir/**/*.csv'
- WHEN executing `SELECT * FROM read_csv('data/**/subdir/**/*.csv')`
- THEN an error is returned
- AND error message indicates "Cannot use multiple '**' in one path"

#### Scenario: No files match pattern
- GIVEN no files match pattern 'nonexistent/*.csv'
- WHEN executing `SELECT * FROM read_csv('nonexistent/*.csv')`
- THEN an error is returned with message "no files match pattern: nonexistent/*.csv"

#### Scenario: Cloud storage glob with S3
- GIVEN S3 bucket contains objects 's3://bucket/data/file1.parquet', 's3://bucket/data/file2.parquet'
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data/*.parquet')`
- THEN S3 ListObjectsV2 API is called with prefix 'data/'
- AND matching objects are downloaded and read

#### Scenario: Cloud storage glob with GCS
- GIVEN GCS bucket contains objects 'gs://bucket/logs/2024/01/data.json', 'gs://bucket/logs/2024/02/data.json'
- WHEN executing `SELECT * FROM read_json('gs://bucket/logs/2024/**/*.json')`
- THEN GCS list API is called with prefix 'logs/2024/'
- AND matching objects are downloaded and read

### Requirement: Array of Files Syntax

The system SHALL support array literal syntax for specifying multiple files explicitly.

#### Scenario: Array of local files
- GIVEN files 'file1.csv', 'file2.csv' exist
- WHEN executing `SELECT * FROM read_csv(['file1.csv', 'file2.csv'])`
- THEN both files are read and combined
- AND rows from both files are returned

#### Scenario: Array with cloud URLs
- GIVEN S3 objects 's3://bucket/a.parquet', 'gs://bucket/b.parquet'
- WHEN executing `SELECT * FROM read_parquet(['s3://bucket/a.parquet', 'gs://bucket/b.parquet'])`
- THEN files from both S3 and GCS are read
- AND rows are combined

#### Scenario: Array elements can contain glob patterns
- GIVEN pattern 'data/*.csv' matches 3 files and 'backup/*.csv' matches 2 files
- WHEN executing `SELECT * FROM read_csv(['data/*.csv', 'backup/*.csv'])`
- THEN all 5 files are read
- AND results are combined

### Requirement: Union-by-Name Schema Alignment

The system SHALL align schemas from multiple files by column name, adding missing columns with NULL values.

#### Scenario: Files with different column orders
- GIVEN file1.csv has columns (id, name, age)
- AND file2.csv has columns (name, id, city)
- WHEN reading both files with glob pattern
- THEN result schema is (id, name, age, city)
- AND file1 rows have city = NULL
- AND file2 rows have age = NULL

#### Scenario: Files with overlapping columns and different types cause error
- GIVEN file1.csv has column 'amount' as INTEGER
- AND file2.csv has column 'amount' as VARCHAR
- WHEN reading both files with glob pattern
- THEN an error is returned
- AND error message indicates type incompatibility for column 'amount'

#### Scenario: Type widening for compatible numeric types
- GIVEN file1.parquet has column 'id' as INTEGER
- AND file2.parquet has column 'id' as BIGINT
- WHEN reading both files with glob pattern
- THEN result schema has 'id' as BIGINT
- AND INTEGER values from file1 are widened to BIGINT

### Requirement: Filename Metadata Column

The system SHALL support adding a column with the source filename when the `filename` option is enabled.

#### Scenario: Include filename column
- GIVEN files 'data/file1.csv', 'data/file2.csv'
- WHEN executing `SELECT * FROM read_csv('data/*.csv', filename=true)`
- THEN result includes a column named 'filename'
- AND rows from file1.csv have filename = 'data/file1.csv'
- AND rows from file2.csv have filename = 'data/file2.csv'

#### Scenario: Filename column with cloud storage
- GIVEN S3 object 's3://bucket/data/file.parquet'
- WHEN executing `SELECT * FROM read_parquet('s3://bucket/data/*.parquet', filename=true)`
- THEN result includes filename column
- AND values are full S3 URLs: 's3://bucket/data/file.parquet'

#### Scenario: Additional virtual metadata columns
- GIVEN files 'data/file1.csv', 'data/file2.csv' with 10 rows each
- WHEN executing `SELECT * FROM read_csv(['data/file1.csv', 'data/file2.csv'], filename=true, file_row_number=true, file_index=true)`
- THEN result includes columns 'filename', 'file_row_number', and 'file_index'
- AND rows from file1.csv have file_index = 0, file_row_number from 0-9
- AND rows from file2.csv have file_index = 1, file_row_number from 0-9

### Requirement: Hive Partitioning Support

The system SHALL extract partition columns from Hive-style directory structures and add them as columns.

#### Scenario: Hive partitioning with year and month
- GIVEN file 'data/year=2024/month=01/sales.parquet'
- WHEN executing `SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true)`
- THEN result includes columns 'year' and 'month'
- AND values are extracted from directory names: year=2024, month=01

#### Scenario: Hive partitioning with type inference
- GIVEN file 'data/year=2024/active=true/data.parquet'
- WHEN executing `SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true, hive_types_autocast=true)`
- THEN column 'year' has type BIGINT (inferred from numeric value)
- AND column 'active' has type VARCHAR (auto-cast only supports DATE, TIMESTAMP, BIGINT)

#### Scenario: Hive partitioning with explicit type schema
- GIVEN file 'data/year=2024/month=01/data.parquet'
- WHEN executing `SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=true, hive_types={'year': 'VARCHAR', 'month': 'VARCHAR'})`
- THEN column 'year' has type VARCHAR (explicitly specified)
- AND column 'month' has type VARCHAR (explicitly specified)
- AND values are '2024' and '01' as strings

#### Scenario: Hive partitioning auto-detection
- GIVEN file 'data/year=2024/month=01/data.parquet'
- WHEN executing `SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning='auto')`
- THEN Hive partitioning is auto-detected from first file path
- AND partition columns 'year' and 'month' are added
- AND types are inferred automatically

#### Scenario: Hive partitioning disabled
- GIVEN file 'data/year=2024/month=01/sales.parquet'
- WHEN executing `SELECT * FROM read_parquet('data/**/*.parquet', hive_partitioning=false)`
- THEN no partition columns are added
- AND only columns from Parquet schema are present

### Requirement: File Ordering

The system SHALL process matched files in alphabetical order by default to ensure deterministic results.

#### Scenario: Alphabetical file ordering
- GIVEN files 'c.csv', 'a.csv', 'b.csv' match pattern '*.csv'
- WHEN executing `SELECT * FROM read_csv('*.csv')`
- THEN files are processed in order: a.csv, b.csv, c.csv
- AND rows appear in that order in the result

#### Scenario: Nested path alphabetical ordering
- GIVEN files 'data/2024/file.csv', 'data/2023/file.csv', 'data/2025/file.csv'
- WHEN executing `SELECT * FROM read_csv('data/**/*.csv')`
- THEN files are processed alphabetically by full path
- AND order is: data/2023/file.csv, data/2024/file.csv, data/2025/file.csv

### Requirement: Glob Pattern Detection

The system SHALL automatically detect glob patterns and distinguish them from literal file paths.

#### Scenario: Pattern with wildcard is treated as glob
- GIVEN argument is 'data/*.csv'
- WHEN evaluating file path
- THEN it is recognized as a glob pattern
- AND glob expansion is performed

#### Scenario: Literal path without wildcards is treated as single file
- GIVEN argument is 'data/file.csv'
- WHEN evaluating file path
- THEN it is treated as a literal file path
- AND no glob expansion is performed

#### Scenario: Path with escaped wildcards is treated as literal
- GIVEN argument is 'data/file\*.csv' (escaped asterisk)
- WHEN evaluating file path
- THEN it is treated as literal filename 'file*.csv'
- AND no glob expansion is performed

### Requirement: Error Handling for Glob Operations

The system SHALL provide clear error messages for glob-related failures.

#### Scenario: Invalid glob pattern syntax
- GIVEN pattern 'data/[.csv' (unclosed bracket)
- WHEN executing `SELECT * FROM read_csv('data/[.csv')`
- THEN an error is returned
- AND error message indicates "invalid glob pattern: unclosed bracket"

#### Scenario: Permission denied during directory listing
- GIVEN directory 'restricted/' is not readable
- WHEN executing `SELECT * FROM read_csv('restricted/*.csv')`
- THEN an error is returned
- AND error message indicates "permission denied: restricted/"

#### Scenario: Too many files matched
- GIVEN pattern matches 50,000 files
- AND max_files_per_glob is set to 10,000
- WHEN executing `SELECT * FROM read_csv('huge_dir/*.csv')`
- THEN an error is returned
- AND error message indicates "glob pattern matched 50,000 files, exceeds limit of 10,000"

### Requirement: Performance Optimization for Cloud Storage

The system SHALL optimize cloud storage glob operations by extracting and using prefixes for list API calls.

#### Scenario: S3 glob with prefix optimization
- GIVEN pattern 's3://bucket/data/2024/**/*.parquet'
- WHEN performing glob expansion
- THEN ListObjectsV2 is called with Prefix='data/2024/'
- AND only objects under that prefix are listed
- AND objects outside the prefix are not fetched

#### Scenario: GCS glob with prefix optimization
- GIVEN pattern 'gs://bucket/logs/app1/**/*.json'
- WHEN performing glob expansion
- THEN GCS list API is called with prefix='logs/app1/'
- AND network traffic is reduced by filtering at the API level

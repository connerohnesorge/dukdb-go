# File-Io Specification Delta

## ADDED Requirements

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

## MODIFIED Requirements

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

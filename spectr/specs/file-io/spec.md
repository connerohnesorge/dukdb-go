# File Io Specification

## Requirements

### Requirement: CSV File Reading

The system SHALL read CSV files and produce DataChunks with comprehensive options.

#### Scenario: Read simple CSV
- GIVEN a CSV file with header "id,name,value"
- AND rows "1,alice,100" and "2,bob,200"
- WHEN reading with CSV reader
- THEN DataChunks contain columns id (INTEGER), name (VARCHAR), value (INTEGER)
- AND two rows are returned with correct values

#### Scenario: Auto-detect delimiter
- GIVEN a CSV file using tab delimiter
- WHEN reading with auto-detection enabled
- THEN tab is detected as delimiter (comma, tab, semicolon, pipe supported)
- AND file is parsed correctly

#### Scenario: Handle quoted fields
- GIVEN a CSV with field containing comma: `"hello, world"`
- WHEN reading the file
- THEN the field is correctly parsed as single value "hello, world"

#### Scenario: Handle NULL values with nullstr option
- GIVEN a CSV with empty field between delimiters: "1,,3"
- AND nullstr option set to empty string
- WHEN reading the file
- THEN middle column is NULL

#### Scenario: Type inference from sample
- GIVEN a CSV with columns containing integers, floats, and strings
- WHEN type inference samples first rows (sample_size option)
- THEN columns are typed as INTEGER, DOUBLE, VARCHAR respectively

#### Scenario: Skip rows before header
- GIVEN a CSV file with metadata rows before the header
- WHEN skip option set to 3
- THEN first 3 rows are skipped before header detection

#### Scenario: Specify date and timestamp formats
- GIVEN a CSV with dates in "DD/MM/YYYY" format
- WHEN dateformat option set to '%d/%m/%Y'
- THEN dates are correctly parsed

#### Scenario: Ignore parsing errors
- GIVEN a CSV with malformed row
- WHEN ignore_errors option is true
- THEN malformed row is skipped and processing continues

#### Scenario: Comment character support
- GIVEN a CSV with lines starting with '#'
- WHEN comment option set to '#'
- THEN comment lines are skipped

#### Scenario: Character encoding support
- GIVEN a CSV file with Latin1 encoding
- WHEN encoding option set to 'latin1'
- THEN file is correctly decoded to UTF-8

#### Scenario: Custom newline character
- GIVEN a CSV with non-standard line endings
- WHEN new_line option specifies the character
- THEN lines are correctly split

#### Scenario: Normalize column names
- GIVEN a CSV with header "User Name,Email Address"
- WHEN normalize_names option is true
- THEN columns become "user_name", "email_address"

#### Scenario: Force specific columns as not null
- GIVEN a CSV with empty values in column 'id'
- WHEN force_not_null option includes 'id'
- THEN empty values become empty strings, not NULL

#### Scenario: Allow quoted nulls
- GIVEN a CSV with quoted null string: '"NULL"'
- WHEN allow_quoted_nulls option is true
- THEN quoted "NULL" strings are parsed as NULL

#### Scenario: Parallel CSV reading
- GIVEN a large CSV file
- WHEN parallel option is true
- THEN file is read using multiple workers for performance

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

The system SHALL read JSON and NDJSON files with comprehensive options.

#### Scenario: Read JSON array
- GIVEN a file containing `[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]`
- WHEN reading with JSON reader
- THEN two rows are returned with id and name columns

#### Scenario: Read NDJSON format
- GIVEN a file with one JSON object per line
- WHEN reading with format='newline_delimited' or format='ndjson'
- THEN each line is parsed as a separate row

#### Scenario: Infer nested types
- GIVEN JSON with nested object: `{"user": {"name": "alice", "age": 30}}`
- WHEN reading with schema inference
- THEN "user" column is typed as STRUCT(name VARCHAR, age INTEGER)

#### Scenario: Handle arrays
- GIVEN JSON with array field: `{"tags": ["a", "b", "c"]}`
- WHEN reading
- THEN "tags" column is typed as VARCHAR[]

#### Scenario: Maximum depth limit
- GIVEN deeply nested JSON objects
- WHEN maximum_depth option is set
- THEN parsing stops at specified depth

#### Scenario: Maximum object size limit
- GIVEN very large JSON objects
- WHEN maximum_object_size option is set
- THEN objects exceeding size are rejected

#### Scenario: Date and timestamp format
- GIVEN JSON with date strings in specific format
- WHEN dateformat and timestampformat options are set
- THEN dates and timestamps are correctly parsed

#### Scenario: Convert strings to integers
- GIVEN JSON with numeric strings like `{"id": "123"}`
- WHEN convert_strings_to_integers option is true
- THEN string "123" becomes INTEGER 123

#### Scenario: Field appearance threshold
- GIVEN JSON where field appears in 50% of records
- WHEN field_appearance_threshold is 0.6
- THEN field is excluded from schema (below threshold)

#### Scenario: Map inference threshold
- GIVEN JSON with object having many varying keys
- WHEN map_inference_threshold is exceeded
- THEN object is typed as MAP instead of STRUCT

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

The system SHALL read Parquet files with projection pushdown.

#### Scenario: Read simple Parquet
- GIVEN a Parquet file with columns id, name, value
- WHEN reading with Parquet reader
- THEN all columns and rows are returned as DataChunks

#### Scenario: Column projection pushdown
- GIVEN a Parquet file with 10 columns
- WHEN reading with projection for 2 columns
- THEN only those 2 columns are read from file
- AND I/O is reduced proportionally

#### Scenario: Read with SNAPPY compression
- GIVEN a Parquet file with SNAPPY codec
- WHEN reading the file
- THEN data is decompressed automatically

#### Scenario: Read with ZSTD compression
- GIVEN a Parquet file with ZSTD codec
- WHEN reading the file
- THEN data is decompressed automatically

#### Scenario: Read with LZ4 compression
- GIVEN a Parquet file with LZ4 or LZ4_RAW codec
- WHEN reading the file
- THEN data is decompressed automatically

#### Scenario: Read with BROTLI compression
- GIVEN a Parquet file with BROTLI codec
- WHEN reading the file
- THEN data is decompressed automatically

#### Scenario: Handle nested types
- GIVEN a Parquet file with LIST and STRUCT columns
- WHEN reading
- THEN nested values are correctly parsed into DuckDB types

### Requirement: Parquet File Writing

The system SHALL write DataChunks to Parquet format with codec options.

#### Scenario: Write simple Parquet
- GIVEN a DataChunk with columns
- WHEN writing to Parquet
- THEN valid Parquet file is produced

#### Scenario: Write with ZSTD codec
- GIVEN CODEC option set to ZSTD
- WHEN writing to Parquet
- THEN output file uses ZSTD compression

#### Scenario: Write with compression level
- GIVEN CODEC option set to ZSTD and COMPRESSION_LEVEL set to 3
- WHEN writing to Parquet
- THEN output uses ZSTD level 3 compression

#### Scenario: Row group sizing
- GIVEN ROW_GROUP_SIZE option set to 100000
- WHEN writing large DataChunk stream
- THEN data is split into row groups of 100000 rows

#### Scenario: Overwrite existing file
- GIVEN OVERWRITE option is true
- AND output file already exists
- WHEN writing to Parquet
- THEN existing file is replaced

#### Scenario: Supported codecs
- GIVEN CODEC option with any supported value
- WHEN writing to Parquet
- THEN UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW, BROTLI are supported

### Requirement: Compression Support

The system SHALL transparently handle compressed files.

#### Scenario: Read gzip-compressed CSV
- GIVEN a file named "data.csv.gz"
- WHEN reading as CSV
- THEN file is automatically decompressed

#### Scenario: Read zstd-compressed file
- GIVEN a file with .zst extension
- WHEN reading
- THEN file is automatically decompressed

#### Scenario: Write with gzip compression
- GIVEN COMPRESSION option set to GZIP
- WHEN writing CSV to "output.csv.gz"
- THEN output is gzip-compressed

#### Scenario: Detect compression from magic bytes
- GIVEN a gzip file without .gz extension
- WHEN reading with auto-detection
- THEN gzip magic bytes (1f 8b) trigger decompression

### Requirement: File Format Detection

The system SHALL auto-detect file formats.

#### Scenario: Detect from extension
- GIVEN file path "data.parquet"
- WHEN format is not explicitly specified
- THEN Parquet format is used

#### Scenario: Detect from magic bytes
- GIVEN a file with .dat extension but Parquet content (PAR1 magic)
- WHEN format is not specified
- THEN file is read as Parquet

#### Scenario: Explicit format overrides detection
- GIVEN file "data.txt" with CSV content
- AND FORMAT option set to CSV
- THEN file is read as CSV regardless of extension

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


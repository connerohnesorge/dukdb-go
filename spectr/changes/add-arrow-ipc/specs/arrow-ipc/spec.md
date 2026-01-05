# Arrow IPC Specification

## ADDED Requirements

### Requirement: Arrow IPC File Reading

The system SHALL read Arrow IPC files in both file format and stream format.

#### Scenario: Read Arrow IPC file format
- GIVEN an Arrow IPC file with magic bytes "ARROW1"
- WHEN opening with NewArrowFileReader
- THEN schema is extracted from file footer
- AND record batches can be accessed by index
- AND NumRecordBatches() returns correct count

#### Scenario: Read Arrow IPC stream format
- GIVEN an Arrow IPC stream with schema message followed by batches
- WHEN opening with NewArrowStreamReader
- THEN schema is extracted from first message
- AND Next() advances to each batch sequentially
- AND Record() returns current batch

#### Scenario: Read from cloud storage
- GIVEN an S3 URL 's3://bucket/data.arrow'
- WHEN reading with read_arrow table function
- THEN file is fetched from S3 via FileSystem interface
- AND range requests are used for footer reading (file format)

#### Scenario: Read compressed Arrow file
- GIVEN an Arrow IPC file with LZ4-compressed batches
- WHEN reading record batches
- THEN batches are automatically decompressed
- AND data matches original uncompressed values

#### Scenario: Invalid Arrow file
- GIVEN a file without Arrow magic bytes
- WHEN attempting to open as Arrow IPC
- THEN error is returned with message "not a valid Arrow IPC file"

### Requirement: Arrow IPC File Writing

The system SHALL write Arrow IPC files with configurable compression.

#### Scenario: Write Arrow IPC file format
- GIVEN a DuckDB query result
- WHEN writing with NewArrowFileWriter
- THEN file starts with magic bytes "ARROW1"
- AND schema is written in header
- AND footer contains block locations
- AND file ends with magic bytes

#### Scenario: Write with LZ4 compression
- GIVEN ArrowWriteOptions with Compression: CompressionLZ4
- WHEN writing record batches
- THEN batches are LZ4 compressed
- AND file can be read by Arrow-compatible tools

#### Scenario: Write with ZSTD compression
- GIVEN ArrowWriteOptions with Compression: CompressionZSTD
- WHEN writing record batches
- THEN batches are ZSTD compressed
- AND file size is smaller than uncompressed

#### Scenario: Write to cloud storage
- GIVEN a GCS URL 'gs://bucket/output.arrow'
- WHEN using COPY TO with FORMAT 'arrow'
- THEN file is uploaded to GCS
- AND multipart upload is used for large files

#### Scenario: Round-trip preservation
- GIVEN a DataChunk with all supported types
- WHEN written to Arrow IPC and read back
- THEN all values match original exactly
- AND types are correctly preserved

### Requirement: RecordBatch Streaming

The system SHALL support streaming RecordBatches without loading entire file into memory.

#### Scenario: Stream large file
- GIVEN an Arrow IPC file with 1000 record batches
- WHEN reading with ArrowStreamReader
- THEN only one batch is in memory at a time
- AND Next() fetches next batch on demand
- AND memory usage stays constant

#### Scenario: Random access in file format
- GIVEN an Arrow IPC file format (not stream)
- WHEN calling ReadRecordBatch(index)
- THEN specific batch is read directly
- AND batches before index are not read

#### Scenario: Release record batch
- GIVEN a record batch obtained from reader
- WHEN calling Release() on the record
- THEN memory is freed immediately
- AND calling Record() again returns nil

#### Scenario: End of stream
- GIVEN an ArrowStreamReader at last batch
- WHEN calling Next()
- THEN false is returned
- AND subsequent Next() calls return false
- AND Record() returns nil

### Requirement: Type Mapping (DuckDB to Arrow)

The system SHALL convert DuckDB types to Arrow types with full fidelity.

#### Scenario: Numeric type mapping
- GIVEN DuckDB INTEGER, BIGINT, DOUBLE columns
- WHEN converting to Arrow schema
- THEN Arrow types Int32, Int64, Float64 are used
- AND values are exactly preserved

#### Scenario: Temporal type mapping
- GIVEN DuckDB TIMESTAMP, DATE, TIME columns
- WHEN converting to Arrow schema
- THEN Arrow Timestamp(us), Date32, Time64(us) are used
- AND temporal values are correctly converted

#### Scenario: Decimal type mapping
- GIVEN DuckDB DECIMAL(18,2) column
- WHEN converting to Arrow schema
- THEN Arrow Decimal128 with precision 18, scale 2 is used
- AND decimal values maintain precision

#### Scenario: List type mapping
- GIVEN DuckDB LIST(INTEGER) column
- WHEN converting to Arrow schema
- THEN Arrow List of Int32 is created
- AND nested list values are correctly converted

#### Scenario: Struct type mapping
- GIVEN DuckDB STRUCT(id INTEGER, name VARCHAR) column
- WHEN converting to Arrow schema
- THEN Arrow Struct with fields {id: Int32, name: String} is created
- AND struct field values are accessible

#### Scenario: Map type mapping
- GIVEN DuckDB MAP(VARCHAR, INTEGER) column
- WHEN converting to Arrow schema
- THEN Arrow Map with key String, value Int32 is created
- AND map entries are correctly converted

#### Scenario: Enum type mapping
- GIVEN DuckDB ENUM('a', 'b', 'c') column
- WHEN converting to Arrow schema
- THEN Arrow Dictionary with Int32 index, String values is created
- AND enum values use dictionary encoding

#### Scenario: NULL value handling
- GIVEN DuckDB column with NULL values
- WHEN converting to Arrow array
- THEN Arrow validity bitmap correctly marks NULLs
- AND IsNull() returns true for NULL positions

#### Scenario: Unsupported type error
- GIVEN a DuckDB type not mappable to Arrow
- WHEN attempting conversion
- THEN error is returned with descriptive message
- AND conversion does not proceed

### Requirement: Type Mapping (Arrow to DuckDB)

The system SHALL convert Arrow types to DuckDB types with full fidelity.

#### Scenario: Arrow primitive to DuckDB
- GIVEN Arrow Int64, Float64, Boolean arrays
- WHEN converting to DuckDB Vector
- THEN DuckDB BIGINT, DOUBLE, BOOLEAN types are used
- AND all values are correctly converted

#### Scenario: Arrow temporal to DuckDB
- GIVEN Arrow Timestamp with microsecond precision
- WHEN converting to DuckDB Vector
- THEN DuckDB TIMESTAMP type is used
- AND temporal values are correctly converted

#### Scenario: Arrow nested to DuckDB
- GIVEN Arrow List of Struct types
- WHEN converting to DuckDB Vector
- THEN DuckDB LIST(STRUCT(...)) is created
- AND nested structure is preserved

#### Scenario: Arrow dictionary to DuckDB enum
- GIVEN Arrow Dictionary-encoded string array
- WHEN converting to DuckDB Vector
- THEN DuckDB ENUM type is created
- AND dictionary values become enum members

### Requirement: Table Function Integration

The system SHALL provide read_arrow table function for SQL access to Arrow files.

#### Scenario: Basic read_arrow usage
- GIVEN an Arrow IPC file at '/path/to/data.arrow'
- WHEN executing SELECT * FROM read_arrow('/path/to/data.arrow')
- THEN all rows from file are returned
- AND schema is inferred from Arrow file

#### Scenario: Column projection
- GIVEN an Arrow file with columns (id, name, value, timestamp)
- WHEN executing SELECT id, name FROM read_arrow('file.arrow')
- THEN only id and name columns are read
- AND other columns are not loaded into memory

#### Scenario: Auto-detection with read_arrow_auto
- GIVEN a file with .arrow extension
- WHEN executing SELECT * FROM read_arrow_auto('data.arrow')
- THEN file format is detected automatically
- AND data is read correctly

#### Scenario: Cloud URL in read_arrow
- GIVEN an S3 URL 's3://bucket/data.arrow'
- WHEN executing SELECT * FROM read_arrow('s3://bucket/data.arrow')
- THEN file is fetched from S3
- AND query returns data from cloud storage

### Requirement: COPY Statement Integration

The system SHALL support Arrow format in COPY TO and COPY FROM statements.

#### Scenario: COPY table TO Arrow file
- GIVEN a table 'orders' with data
- WHEN executing COPY orders TO 'orders.arrow' (FORMAT 'arrow')
- THEN Arrow IPC file is created
- AND file contains all table data
- AND schema matches table schema

#### Scenario: COPY query TO Arrow file
- GIVEN a SELECT query
- WHEN executing COPY (SELECT * FROM t WHERE x > 10) TO 'filtered.arrow' (FORMAT 'arrow')
- THEN Arrow IPC file contains filtered data
- AND only matching rows are included

#### Scenario: COPY with compression
- GIVEN a table 'logs' with data
- WHEN executing COPY logs TO 'logs.arrow' (FORMAT 'arrow', COMPRESSION 'zstd')
- THEN Arrow IPC file uses ZSTD compression
- AND file size is smaller than uncompressed

#### Scenario: COPY FROM Arrow file
- GIVEN an Arrow IPC file 'import.arrow'
- WHEN executing COPY target FROM 'import.arrow' (FORMAT 'arrow')
- THEN data is imported into target table
- AND schema compatibility is validated

#### Scenario: COPY FROM cloud URL
- GIVEN an Arrow file on GCS 'gs://bucket/data.arrow'
- WHEN executing COPY target FROM 'gs://bucket/data.arrow' (FORMAT 'arrow')
- THEN file is downloaded from GCS
- AND data is imported into target table

### Requirement: Memory Management

The system SHALL manage Arrow memory efficiently using the Retain/Release pattern.

#### Scenario: Record batch lifecycle
- GIVEN a record batch read from Arrow file
- WHEN processing is complete
- THEN Release() is called to free memory
- AND memory is reclaimed by allocator

#### Scenario: Builder resource cleanup
- GIVEN a RecordBatchBuilder
- WHEN building is complete
- THEN Release() on builder frees all child builders
- AND no memory leaks occur

#### Scenario: Multiple batch processing
- GIVEN 1000 record batches to process
- WHEN processing sequentially with Release after each
- THEN memory usage stays bounded
- AND peak memory equals approximately one batch size

#### Scenario: Error cleanup
- GIVEN an error during Arrow file reading
- WHEN error occurs after partial batch read
- THEN all partially read data is released
- AND reader Close() releases all resources

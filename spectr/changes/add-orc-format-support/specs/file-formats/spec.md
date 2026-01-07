## ADDED Requirements

### Requirement: ORC File Format Reader

The system SHALL provide read access to ORC (Optimized Row Columnar) files via `read_orc()` and `read_orc_auto()` functions.

#### Scenario: Read ORC file with automatic type detection

- GIVEN an ORC file 'data.orc' with known schema
- WHEN executing `SELECT * FROM read_orc_auto('data.orc')`
- THEN the file SHALL be parsed
- AND column types SHALL be inferred from ORC type information
- AND all rows SHALL be returned

#### Scenario: Read ORC file with explicit schema

- GIVEN an ORC file 'data.orc'
- WHEN executing `SELECT * FROM read_orc('data.orc')`
- THEN the file SHALL be read using ORC type system
- AND types SHALL be mapped to DuckDB types
- AND all rows SHALL be returned

#### Scenario: ORC file type detection

- GIVEN files: 'data.csv', 'data.orc', 'data.parquet'
- WHEN executing `SELECT * FROM read_auto('data.orc')`
- THEN ORC format SHALL be detected
- AND appropriate parser SHALL be invoked

#### Scenario: Read ORC with column projection

- GIVEN an ORC file with columns (id, name, value, timestamp)
- WHEN executing `SELECT id, name FROM read_orc('data.orc')`
- THEN only id and name columns SHALL be read
- AND other columns SHALL be skipped (I/O optimization)

#### Scenario: Read ORC from S3

- GIVEN an ORC file at 's3://bucket/data.orc'
- WHEN executing `SELECT * FROM read_orc('s3://bucket/data.orc')`
- THEN the file SHALL be fetched via S3 API
- AND parsed as ORC format
- AND results returned

### Requirement: ORC Type Mapping

The system SHALL correctly map ORC types to equivalent DuckDB types.

#### Scenario: Primitive type mapping

- GIVEN an ORC file with types: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, STRING
- WHEN reading the file
- THEN types SHALL map to: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR

#### Scenario: Temporal type mapping

- GIVEN an ORC file with TIMESTAMP, DATE types
- WHEN reading the file
- THEN types SHALL map to: TIMESTAMP, DATE

#### Scenario: Complex type mapping - STRUCT

- GIVEN an ORC file with STRUCT<name VARCHAR, age INT> column
- WHEN reading the file
- THEN type SHALL map to: STRUCT(name VARCHAR, age INTEGER)

#### Scenario: Complex type mapping - ARRAY

- GIVEN an ORC file with ARRAY<INT> column
- WHEN reading the file
- THEN type SHALL map to: LIST(INTEGER)

#### Scenario: Complex type mapping - MAP

- GIVEN an ORC file with MAP<STRING, INT> column
- WHEN reading the file
- THEN type SHALL map to: MAP(VARCHAR, INTEGER)

### Requirement: ORC Compression Support

The system SHALL support reading ORC files with various compression codecs.

#### Scenario: Read uncompressed ORC

- GIVEN an uncompressed ORC file
- WHEN reading the file
- THEN data SHALL be read directly

#### Scenario: Read ZLIB-compressed ORC

- GIVEN a ZLIB-compressed ORC file
- WHEN reading the file
- THEN compression SHALL be decompressed automatically

#### Scenario: Read SNAPPY-compressed ORC

- GIVEN a SNAPPY-compressed ORC file
- WHEN reading the file
- THEN compression SHALL be decompressed automatically

#### Scenario: Read LZ4-compressed ORC

- GIVEN an LZ4-compressed ORC file
- WHEN reading the file
- THEN compression SHALL be decompressed automatically

#### Scenario: Read ZSTD-compressed ORC

- GIVEN a ZSTD-compressed ORC file (DuckDB v1.4.3+)
- WHEN reading the file
- THEN compression SHALL be decompressed automatically

### Requirement: ORC Predicate Push-down

The system SHALL use ORC column statistics for predicate push-down optimization.

#### Scenario: Stripe-level filtering

- GIVEN an ORC file with 10 stripes
- AND stripe 5 has min/max statistics showing no matching rows
- WHEN executing `SELECT * FROM read_orc('data.orc') WHERE status = 'completed'`
- THEN stripes without matching statistics SHALL be skipped
- AND only relevant stripes SHALL be read

#### Scenario: Column statistics filtering

- GIVEN an ORC file with column 'price' having min=10, max=100
- WHEN executing `SELECT * FROM read_orc('data.orc') WHERE price > 500`
- THEN the query SHALL return zero rows
- AND no data SHALL be read (statistics show impossibility)

#### Scenario: String column bloom filter (if available)

- GIVEN an ORC file with STRING column and bloom filter
- WHEN executing `SELECT * FROM read_orc('data.orc') WHERE name = 'target'`
- THEN bloom filter SHALL be used to skip irrelevant stripes

### Requirement: ORC File Writer

The system SHALL support writing data to ORC format via COPY TO statements.

#### Scenario: Write table to ORC file

- GIVEN a table with 1000 rows
- WHEN executing `COPY my_table TO 'data.orc' (FORMAT ORC)`
- THEN an ORC file SHALL be created
- AND all rows SHALL be written
- AND file SHALL be valid ORC format

#### Scenario: Write with compression

- GIVEN a table with data
- WHEN executing `COPY my_table TO 'data.orc' (FORMAT ORC, COMPRESSION ZSTD)`
- THEN an ORC file SHALL be created with ZSTD compression
- AND file SHALL be readable by other ORC tools

#### Scenario: Write with column selection

- GIVEN a table with columns (id, name, value, secret)
- WHEN executing `COPY (SELECT id, name FROM my_table) TO 'data.orc' (FORMAT ORC)`
- THEN ORC file SHALL contain only id and name columns
- AND secret column SHALL not be written

### Requirement: ORC Format Detection

The system SHALL automatically detect ORC files and select appropriate parser.

#### Scenario: Magic number detection

- GIVEN a file with ORC magic number 'ORC' at start
- WHEN reading the file
- THEN ORC parser SHALL be selected

#### Scenario: File extension detection

- GIVEN file 'data.orc'
- WHEN using read_auto or COPY TO
- THEN ORC format SHALL be inferred from extension

#### Scenario: Unknown format error

- GIVEN a file with unknown format
- WHEN attempting to read
- THEN a clear error SHALL indicate format not supported

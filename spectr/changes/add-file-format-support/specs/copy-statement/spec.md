# COPY Statement Specification

## ADDED Requirements

### Requirement: COPY FROM Statement

The system SHALL import data from files into tables using COPY FROM.

#### Scenario: COPY FROM CSV
- GIVEN table "users" with columns id (INTEGER), name (VARCHAR)
- AND file "users.csv" with matching data
- WHEN executing `COPY users FROM 'users.csv'`
- THEN rows from CSV are inserted into users table

#### Scenario: COPY FROM with CSV options
- GIVEN table "data" and CSV file with tab delimiter
- WHEN executing `COPY data FROM 'data.tsv' (DELIMITER '\t', HEADER true)`
- THEN file is parsed with tab delimiter and header row is skipped

#### Scenario: COPY FROM with encoding
- GIVEN table and CSV file with Latin1 encoding
- WHEN executing `COPY data FROM 'data.csv' (ENCODING 'latin1')`
- THEN file is correctly decoded

#### Scenario: COPY FROM with skip rows
- GIVEN CSV file with 2 metadata rows before header
- WHEN executing `COPY data FROM 'data.csv' (SKIP 2)`
- THEN first 2 rows are skipped before reading

#### Scenario: COPY FROM Parquet
- GIVEN table "events" and Parquet file with matching schema
- WHEN executing `COPY events FROM 'events.parquet' (FORMAT PARQUET)`
- THEN Parquet data is loaded into table

#### Scenario: COPY FROM JSON
- GIVEN table "logs" and NDJSON file
- WHEN executing `COPY logs FROM 'logs.ndjson' (FORMAT JSON)`
- THEN JSON objects are inserted as rows

#### Scenario: Column mismatch error
- GIVEN table with 3 columns and CSV with 4 columns
- WHEN executing COPY FROM
- THEN error is returned indicating column count mismatch

#### Scenario: Type conversion during COPY
- GIVEN table with INTEGER column and CSV with string "123"
- WHEN executing COPY FROM
- THEN string is converted to integer 123

#### Scenario: Force not null columns
- GIVEN table with column that should not be null
- WHEN executing `COPY data FROM 'file.csv' (FORCE_NOT_NULL (col1, col2))`
- THEN empty values in those columns become empty strings, not NULL

#### Scenario: Allow quoted nulls
- GIVEN CSV with quoted null strings
- WHEN executing `COPY data FROM 'file.csv' (ALLOW_QUOTED_NULLS true)`
- THEN quoted null strings are parsed as NULL

#### Scenario: Null padding for missing columns
- GIVEN CSV with fewer columns than table
- WHEN executing `COPY data FROM 'file.csv' (NULL_PADDING true)`
- THEN missing columns are filled with NULL

### Requirement: COPY TO Statement

The system SHALL export table data to files using COPY TO.

#### Scenario: COPY table TO CSV
- GIVEN table "users" with data
- WHEN executing `COPY users TO 'users_export.csv'`
- THEN CSV file is created with all rows

#### Scenario: COPY table TO Parquet with CODEC
- GIVEN table "events" with data
- WHEN executing `COPY events TO 'events.parquet' (FORMAT PARQUET, CODEC 'ZSTD')`
- THEN Parquet file is created with ZSTD compression

#### Scenario: COPY with CODEC and compression level
- GIVEN table with data
- WHEN executing `COPY data TO 'out.parquet' (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 3)`
- THEN Parquet file uses ZSTD level 3 compression

#### Scenario: COPY with row group size
- GIVEN table with data
- WHEN executing `COPY data TO 'out.parquet' (FORMAT PARQUET, ROW_GROUP_SIZE 100000)`
- THEN Parquet file has row groups of 100000 rows

#### Scenario: COPY TO with overwrite
- GIVEN output file already exists
- WHEN executing `COPY data TO 'out.csv' (OVERWRITE true)`
- THEN existing file is replaced

#### Scenario: COPY TO with column selection
- GIVEN table with columns a, b, c
- WHEN executing `COPY table (a, c) TO 'partial.csv'`
- THEN only columns a and c are exported

#### Scenario: COPY TO with force quote
- GIVEN table with VARCHAR columns
- WHEN executing `COPY data TO 'out.csv' (FORCE_QUOTE (name, address))`
- THEN specified columns are always quoted

#### Scenario: Per-thread output files
- GIVEN large table and parallel export enabled
- WHEN executing `COPY data TO 'out.parquet' (FORMAT PARQUET, PER_THREAD_OUTPUT true)`
- THEN multiple output files are created (one per thread)

### Requirement: COPY Query TO Statement

The system SHALL export query results to files.

#### Scenario: COPY SELECT TO file
- GIVEN tables with data
- WHEN executing `COPY (SELECT a, b FROM t WHERE x > 10) TO 'filtered.csv'`
- THEN query result is written to file

#### Scenario: COPY aggregation TO file
- GIVEN table with data
- WHEN executing `COPY (SELECT category, SUM(amount) FROM sales GROUP BY category) TO 'summary.json' (FORMAT JSON)`
- THEN aggregated results are written as JSON

### Requirement: COPY Statement Options

The system SHALL support comprehensive COPY options.

#### Scenario: DELIMITER option
- GIVEN COPY statement with `(DELIMITER '|')`
- WHEN executing
- THEN pipe character is used as field separator

#### Scenario: HEADER option
- GIVEN COPY FROM with `(HEADER false)`
- WHEN executing
- THEN first row is treated as data, not header

#### Scenario: NULL option
- GIVEN COPY statement with `(NULL 'NA')`
- WHEN reading or writing
- THEN 'NA' string represents NULL values

#### Scenario: FORMAT option
- GIVEN COPY statement with `(FORMAT PARQUET)`
- WHEN executing
- THEN Parquet format is used regardless of extension

#### Scenario: CODEC option for Parquet
- GIVEN COPY TO with `(FORMAT PARQUET, CODEC 'SNAPPY')`
- WHEN executing
- THEN output uses SNAPPY codec

#### Scenario: Supported CODEC values
- GIVEN COPY TO Parquet with various CODEC options
- WHEN executing
- THEN UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW, BROTLI are supported

#### Scenario: COMPRESSION option for CSV
- GIVEN COPY TO CSV with `(COMPRESSION 'GZIP')`
- WHEN executing
- THEN output CSV is gzip-compressed

#### Scenario: ENCODING option
- GIVEN COPY statement with `(ENCODING 'UTF8')`
- WHEN executing
- THEN specified character encoding is used

#### Scenario: Multiple options
- GIVEN COPY with `(DELIMITER '\t', HEADER true, NULL '', QUOTE '"', ENCODING 'UTF8')`
- WHEN executing
- THEN all options are applied correctly

### Requirement: COPY Error Handling

The system SHALL provide clear errors for COPY failures.

#### Scenario: File not found
- GIVEN COPY FROM with non-existent file path
- WHEN executing
- THEN error indicates file not found

#### Scenario: Permission denied
- GIVEN COPY TO with unwritable path
- WHEN executing
- THEN error indicates permission issue

#### Scenario: Invalid format
- GIVEN COPY FROM with malformed CSV
- WHEN executing
- THEN error indicates line number and nature of error

#### Scenario: Table not found
- GIVEN COPY FROM to non-existent table
- WHEN executing
- THEN ErrorTypeCatalog is returned

#### Scenario: Unsupported CODEC
- GIVEN COPY TO with invalid CODEC value
- WHEN executing
- THEN error lists supported codecs

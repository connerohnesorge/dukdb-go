# Spec Delta: duckdb-storage-format

## ADDED Requirements

### Requirement: Data Verification Testing

The system MUST include comprehensive tests verifying row data can be read correctly from DuckDB CLI created files.

#### Scenario: Verify integer data from DuckDB CLI file
- **Given** a DuckDB file created by DuckDB CLI with INTEGER column
- **When** rows are scanned using dukdb-go
- **Then** values MUST match those inserted by DuckDB CLI exactly

#### Scenario: Verify string data preserves Unicode
- **Given** a DuckDB file with VARCHAR containing multi-byte UTF-8 (emoji, CJK)
- **When** the column is read
- **Then** string values MUST be byte-identical to original

#### Scenario: Verify all compression algorithms produce correct values
- **Given** columns compressed with CONSTANT, RLE, DICTIONARY, BITPACKING
- **When** data is decompressed and read
- **Then** values MUST match original inserted data

#### Scenario: Verify NULL positions from validity mask
- **Given** a column with NULL values at known positions
- **When** the column is read
- **Then** NULL positions MUST match the original data exactly

#### Scenario: Verify all 43 data types round-trip
- **Given** DuckDB files with columns of each supported type
- **When** data is read
- **Then** all types MUST be correctly mapped with value preservation

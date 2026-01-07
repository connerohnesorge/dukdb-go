# Excel XLSX Specification

## ADDED Requirements

### Requirement: XLSX File Reading

The system SHALL read Microsoft Excel XLSX files into DataChunks using the read_xlsx table function.

#### Scenario: Read basic XLSX file
- GIVEN an XLSX file 'data.xlsx' with columns id, name, value
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx')`
- THEN returns all rows with correct column names
- AND values are correctly typed

#### Scenario: Read XLSX with header row
- GIVEN an XLSX file with first row containing column names
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', header := true)`
- THEN column names are read from first row
- AND data starts from second row

#### Scenario: Read XLSX without header
- GIVEN an XLSX file with data starting in first row
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', header := false)`
- THEN columns are named column0, column1, etc.
- AND first row is included as data

#### Scenario: Read XLSX with skip rows
- GIVEN an XLSX file with 3 metadata rows before header
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', skip := 3)`
- THEN first 3 rows are skipped
- AND header is read from row 4

#### Scenario: Read XLSX from cloud storage
- GIVEN an XLSX file at 's3://bucket/data.xlsx'
- WHEN executing `SELECT * FROM read_xlsx('s3://bucket/data.xlsx')`
- THEN file is downloaded from S3
- AND parsed as XLSX

### Requirement: Sheet Selection

The system SHALL support selecting specific sheets from multi-sheet workbooks.

#### Scenario: Select sheet by name
- GIVEN an XLSX file with sheets "Sales", "Inventory", "Summary"
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', sheet := 'Inventory')`
- THEN data is read from the "Inventory" sheet

#### Scenario: Select sheet by index
- GIVEN an XLSX file with 3 sheets
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', sheet_index := 1)`
- THEN data is read from the second sheet (0-indexed)

#### Scenario: Default to first sheet
- GIVEN an XLSX file with multiple sheets
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx')` without sheet option
- THEN data is read from the first sheet

#### Scenario: Sheet not found error
- GIVEN an XLSX file without sheet "Missing"
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', sheet := 'Missing')`
- THEN error is returned indicating sheet not found
- AND available sheet names are listed

#### Scenario: Sheet index out of range
- GIVEN an XLSX file with 2 sheets
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', sheet_index := 5)`
- THEN error is returned indicating index out of range

### Requirement: Cell Range Selection

The system SHALL support reading specific cell ranges using A1 notation.

#### Scenario: Read specific range
- GIVEN an XLSX file with data in cells A1:Z1000
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', range := 'B2:D100')`
- THEN only cells B2 through D100 are read
- AND column names come from B2, C2, D2 if header is true

#### Scenario: Read from start row
- GIVEN an XLSX file with data
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', start_row := 10)`
- THEN reading starts from row 10

#### Scenario: Read to end row
- GIVEN an XLSX file with 1000 rows
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', end_row := 100)`
- THEN only rows 1-100 are read

#### Scenario: Read specific columns
- GIVEN an XLSX file with columns A through Z
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', start_col := 'C', end_col := 'F')`
- THEN only columns C, D, E, F are read

#### Scenario: Invalid range format
- GIVEN an invalid range string
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', range := 'invalid')`
- THEN error is returned with valid range format example

### Requirement: Type Inference

The system SHALL infer column types from Excel cell types and formats.

#### Scenario: Infer numeric type
- GIVEN XLSX cells containing numeric values (integers)
- WHEN reading the column
- THEN column type is BIGINT

#### Scenario: Infer decimal type
- GIVEN XLSX cells containing decimal values (e.g., 3.14)
- WHEN reading the column
- THEN column type is DOUBLE

#### Scenario: Infer date type
- GIVEN XLSX cells with date format (date number format)
- WHEN reading the column
- THEN column type is DATE
- AND serial numbers are converted to dates

#### Scenario: Infer timestamp type
- GIVEN XLSX cells with datetime format
- WHEN reading the column
- THEN column type is TIMESTAMP
- AND values include time component

#### Scenario: Infer boolean type
- GIVEN XLSX cells with TRUE/FALSE values
- WHEN reading the column
- THEN column type is BOOLEAN

#### Scenario: Infer string type
- GIVEN XLSX cells with text values
- WHEN reading the column
- THEN column type is VARCHAR

#### Scenario: Handle mixed types
- GIVEN XLSX column with mixed numbers and strings
- WHEN reading the column
- THEN column type is VARCHAR
- AND all values are converted to strings

#### Scenario: Explicit type override
- GIVEN XLSX file with ambiguous column
- WHEN executing `SELECT * FROM read_xlsx('data.xlsx', columns := {'date_col': 'DATE'})`
- THEN date_col is treated as DATE regardless of cell format

#### Scenario: Empty cell handling
- GIVEN XLSX cells that are empty
- WHEN reading with empty_as_null := true (default)
- THEN empty cells are returned as NULL

### Requirement: XLSX File Writing

The system SHALL export query results to XLSX format using COPY TO statement.

#### Scenario: Basic COPY TO XLSX
- GIVEN table "results" with data
- WHEN executing `COPY results TO 'output.xlsx' (FORMAT XLSX)`
- THEN XLSX file is created with all rows
- AND file can be opened by Microsoft Excel

#### Scenario: COPY query TO XLSX
- GIVEN tables with data
- WHEN executing `COPY (SELECT a, b FROM t WHERE x > 10) TO 'filtered.xlsx' (FORMAT XLSX)`
- THEN query results are written to XLSX

#### Scenario: COPY with header option
- GIVEN table with columns id, name, value
- WHEN executing `COPY table TO 'out.xlsx' (FORMAT XLSX, HEADER true)`
- THEN first row contains column names
- AND data starts from second row

#### Scenario: COPY with sheet name
- GIVEN table with data
- WHEN executing `COPY table TO 'out.xlsx' (FORMAT XLSX, SHEET 'Results')`
- THEN sheet is named "Results" instead of default "Sheet1"

#### Scenario: COPY to cloud storage
- GIVEN table with data
- WHEN executing `COPY table TO 's3://bucket/output.xlsx' (FORMAT XLSX)`
- THEN XLSX file is uploaded to S3

#### Scenario: Date value export
- GIVEN table with DATE column containing '2024-01-15'
- WHEN writing to XLSX
- THEN cell contains Excel date serial number
- AND cell is formatted as date in Excel

#### Scenario: Timestamp export
- GIVEN table with TIMESTAMP column
- WHEN writing to XLSX
- THEN cell contains Excel datetime serial number
- AND cell includes date and time components

#### Scenario: NULL value export
- GIVEN table with NULL values
- WHEN writing to XLSX
- THEN NULL cells are left empty in Excel

#### Scenario: Auto column width
- GIVEN table with varying length values
- WHEN writing with auto_width := true (default)
- THEN column widths accommodate content

### Requirement: Large File Handling

The system SHALL handle large XLSX files efficiently using streaming.

#### Scenario: Read large file
- GIVEN an XLSX file with 1,000,000 rows
- WHEN reading with read_xlsx
- THEN file is processed in chunks
- AND memory usage remains bounded

#### Scenario: Write large file
- GIVEN a query returning 1,000,000 rows
- WHEN writing to XLSX
- THEN file is written in chunks
- AND memory usage remains bounded

#### Scenario: Streaming read performance
- GIVEN an XLSX file with 100,000 rows
- WHEN reading with streaming
- THEN read time is proportional to file size
- AND no timeout occurs

### Requirement: Error Handling

The system SHALL provide clear errors for XLSX-related issues.

#### Scenario: File not found
- GIVEN a non-existent file path
- WHEN executing read_xlsx
- THEN error indicates file not found

#### Scenario: Invalid XLSX format
- GIVEN a file that is not valid XLSX
- WHEN executing read_xlsx
- THEN error indicates invalid XLSX format
- AND suggests checking file is not corrupted

#### Scenario: Password protected file
- GIVEN an encrypted/password-protected XLSX
- WHEN executing read_xlsx
- THEN error indicates password protection not supported

#### Scenario: Corrupted file
- GIVEN a corrupted XLSX file
- WHEN executing read_xlsx
- THEN error indicates corruption with details if available

#### Scenario: Write permission denied
- GIVEN an unwritable output path
- WHEN executing COPY TO XLSX
- THEN error indicates permission denied

# Execution Engine - File Format Table Functions

## ADDED Requirements

### Requirement: Built-in read_csv Table Function

The engine SHALL provide a read_csv table function for CSV file reading.

#### Scenario: Basic read_csv usage
- GIVEN a CSV file "data.csv" with header and data
- WHEN executing `SELECT * FROM read_csv('data.csv')`
- THEN all rows are returned with inferred column names and types

#### Scenario: read_csv with explicit columns
- GIVEN a CSV file without header
- WHEN executing `SELECT * FROM read_csv('data.csv', columns={'id': 'INTEGER', 'name': 'VARCHAR'})`
- THEN columns are typed as specified

#### Scenario: read_csv with delimiter option
- GIVEN a tab-separated file
- WHEN executing `SELECT * FROM read_csv('data.tsv', delim='\t')`
- THEN file is parsed with tab delimiter

#### Scenario: read_csv with header option
- GIVEN a CSV file without header row
- WHEN executing `SELECT * FROM read_csv('data.csv', header=false)`
- THEN first row is treated as data, columns named column0, column1, etc.

### Requirement: Built-in read_csv_auto Table Function

The engine SHALL provide read_csv_auto for automatic CSV detection.

#### Scenario: Automatic format detection
- GIVEN a CSV file with standard format
- WHEN executing `SELECT * FROM read_csv_auto('data.csv')`
- THEN delimiter, quote, header, and types are auto-detected

#### Scenario: Auto-detect works with various formats
- GIVEN CSV files with comma, tab, semicolon, or pipe delimiters
- WHEN executing read_csv_auto on each
- THEN each is correctly parsed

### Requirement: Built-in read_json Table Function

The engine SHALL provide a read_json table function.

#### Scenario: Read JSON array file
- GIVEN a file containing JSON array of objects
- WHEN executing `SELECT * FROM read_json('data.json')`
- THEN objects are returned as rows

#### Scenario: Read NDJSON file
- GIVEN a newline-delimited JSON file
- WHEN executing `SELECT * FROM read_json('data.ndjson', format='newline_delimited')`
- THEN each line is parsed as a row

#### Scenario: read_json with columns
- GIVEN a JSON file
- WHEN executing `SELECT * FROM read_json('data.json', columns={'id': 'INTEGER', 'name': 'VARCHAR'})`
- THEN only specified columns are returned with specified types

### Requirement: Built-in read_json_auto Table Function

The engine SHALL provide read_json_auto for automatic JSON detection.

#### Scenario: Auto-detect JSON format
- GIVEN a JSON file (array or NDJSON)
- WHEN executing `SELECT * FROM read_json_auto('data.json')`
- THEN format and schema are auto-detected

### Requirement: Built-in read_ndjson Table Function

The engine SHALL provide read_ndjson as alias for NDJSON reading.

#### Scenario: Read NDJSON via alias
- GIVEN a newline-delimited JSON file
- WHEN executing `SELECT * FROM read_ndjson('data.ndjson')`
- THEN file is read as NDJSON format
- AND behavior is equivalent to `read_json('data.ndjson', format='newline_delimited')`

### Requirement: Built-in read_parquet Table Function

The engine SHALL provide a read_parquet table function.

#### Scenario: Read Parquet file
- GIVEN a Parquet file with data
- WHEN executing `SELECT * FROM read_parquet('data.parquet')`
- THEN all rows and columns are returned

#### Scenario: Column projection
- GIVEN a Parquet file with columns a, b, c, d
- WHEN executing `SELECT a, c FROM read_parquet('data.parquet')`
- THEN only columns a and c are read from file (I/O optimization)

#### Scenario: Read compressed Parquet
- GIVEN a Parquet file with ZSTD compression
- WHEN executing `SELECT * FROM read_parquet('data.parquet')`
- THEN data is decompressed and returned correctly

### Requirement: Table Function in FROM Clause

The engine SHALL allow file-reading table functions in FROM clause.

#### Scenario: Join with table function
- GIVEN a table "users" and CSV file "orders.csv"
- WHEN executing `SELECT * FROM users JOIN read_csv('orders.csv') o ON users.id = o.user_id`
- THEN join is performed correctly

#### Scenario: Subquery with table function
- GIVEN a Parquet file
- WHEN executing `SELECT * FROM (SELECT * FROM read_parquet('data.parquet') WHERE x > 10) sub`
- THEN subquery is evaluated correctly

#### Scenario: CTE with table function
- GIVEN a CSV file
- WHEN executing `WITH data AS (SELECT * FROM read_csv_auto('data.csv')) SELECT * FROM data WHERE id > 5`
- THEN CTE with table function works correctly

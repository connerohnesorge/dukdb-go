# Catalog Persistence Specification

## Requirements

### Requirement: Database File Creation

The system SHALL create a database file when opening with a non-memory path.

#### Scenario: New database file created
- GIVEN a path "/tmp/test.duckdb" that does not exist
- WHEN calling sql.Open("dukdb", path)
- AND creating a table
- AND closing the database
- THEN a file exists at the specified path

#### Scenario: Memory database creates no file
- GIVEN the path ":memory:"
- WHEN calling sql.Open("dukdb", ":memory:")
- AND creating a table
- AND closing the database
- THEN no file is created

#### Scenario: Existing file is loaded
- GIVEN a valid database file at "/tmp/test.duckdb"
- WHEN calling sql.Open("dukdb", path)
- THEN the existing schemas and data are available

#### Scenario: Empty database persists
- GIVEN a new database with no tables created
- WHEN the database is closed and reopened
- THEN no errors occur
- AND the database is ready for table creation

### Requirement: Catalog Persistence

The system SHALL persist catalog metadata (schemas, tables, columns) to the database file.

#### Scenario: Table schema persists
- GIVEN a database at path "/tmp/test.duckdb"
- AND a table created with `CREATE TABLE users (id INTEGER, name VARCHAR)`
- WHEN the database is closed and reopened
- THEN the table "users" exists with columns "id" (INTEGER) and "name" (VARCHAR)

#### Scenario: Multiple tables persist
- GIVEN a database with tables "users" and "orders"
- WHEN the database is closed and reopened
- THEN both tables exist with their original schemas

#### Scenario: Column nullability persists
- GIVEN a table with column "id INTEGER NOT NULL"
- WHEN the database is closed and reopened
- THEN the column "id" is marked as NOT NULL

#### Scenario: Primary key persists
- GIVEN a table with `PRIMARY KEY (id)`
- WHEN the database is closed and reopened
- THEN the primary key constraint is preserved

#### Scenario: Default values persist
- GIVEN a table with column "status VARCHAR DEFAULT 'active'"
- WHEN the database is closed and reopened
- THEN the column "status" has default value 'active'

#### Scenario: Unicode table and column names persist
- GIVEN a table named "用户表" with column "名前"
- WHEN the database is closed and reopened
- THEN the table "用户表" exists with column "名前"

#### Scenario: Decimal precision and scale persist
- GIVEN a table with column "price DECIMAL(18,4)"
- WHEN the database is closed and reopened
- THEN the column "price" has precision 18 and scale 4

#### Scenario: STRUCT field names persist
- GIVEN a table with column "data STRUCT(x INTEGER, y VARCHAR)"
- WHEN the database is closed and reopened
- THEN the field names "x" and "y" are preserved exactly

### Requirement: Data Persistence

The system SHALL persist table row data to the database file.

#### Scenario: Integer data persists
- GIVEN a table with INTEGER column containing values [1, 2, 3]
- WHEN the database is closed and reopened
- THEN querying the table returns [1, 2, 3]

#### Scenario: String data persists
- GIVEN a table with VARCHAR column containing ["Alice", "Bob", "Charlie"]
- WHEN the database is closed and reopened
- THEN querying the table returns ["Alice", "Bob", "Charlie"]

#### Scenario: Unicode string data persists
- GIVEN a table with VARCHAR column containing ["Hello", "世界", "🌍🔥"]
- WHEN the database is closed and reopened
- THEN querying the table returns ["Hello", "世界", "🌍🔥"]

#### Scenario: NULL values persist
- GIVEN a table with rows containing NULL values
- WHEN the database is closed and reopened
- THEN the NULL values are preserved

#### Scenario: NULL in INTEGER column persists
- GIVEN an INTEGER column with [1, NULL, 3]
- WHEN the database is closed and reopened
- THEN values are [1, NULL, 3]

#### Scenario: NULL in DECIMAL column persists
- GIVEN a DECIMAL column with [1.5, NULL, 3.5]
- WHEN the database is closed and reopened
- THEN values are [1.5, NULL, 3.5]

#### Scenario: NULL in DATE column persists
- GIVEN a DATE column with [2024-01-01, NULL, 2024-12-31]
- WHEN the database is closed and reopened
- THEN values are [2024-01-01, NULL, 2024-12-31]

#### Scenario: NULL in UUID column persists
- GIVEN a UUID column with [valid-uuid, NULL]
- WHEN the database is closed and reopened
- THEN values are [valid-uuid, NULL]

#### Scenario: NULL in LIST column persists
- GIVEN a LIST(INTEGER) column with [[1,2], NULL, [3]]
- WHEN the database is closed and reopened
- THEN values are [[1,2], NULL, [3]]

#### Scenario: All-NULL column persists
- GIVEN a column where every row is NULL
- WHEN the database is closed and reopened
- THEN all values remain NULL

#### Scenario: Large dataset persists
- GIVEN a table with 100,000 rows
- WHEN the database is closed and reopened
- THEN all 100,000 rows are present and correct

#### Scenario: Empty table persists
- GIVEN an empty table
- WHEN the database is closed and reopened
- THEN the table exists but contains no rows

### Requirement: All Types Persistence

The system SHALL persist all supported DuckDB types correctly.

#### Scenario: Boolean type persists
- GIVEN a BOOLEAN column with [true, false, NULL]
- WHEN the database is closed and reopened
- THEN values are [true, false, NULL]

#### Scenario: Numeric types persist
- GIVEN columns TINYINT, SMALLINT, INTEGER, BIGINT
- WHEN the database is closed and reopened
- THEN all numeric values are preserved exactly

#### Scenario: Floating point types persist
- GIVEN columns FLOAT, DOUBLE with values including infinity and NaN
- WHEN the database is closed and reopened
- THEN special floating point values are preserved

#### Scenario: Positive infinity persists
- GIVEN a DOUBLE column with value +Infinity
- WHEN the database is closed and reopened
- THEN the value is +Infinity

#### Scenario: Negative infinity persists
- GIVEN a DOUBLE column with value -Infinity
- WHEN the database is closed and reopened
- THEN the value is -Infinity

#### Scenario: NaN persists
- GIVEN a DOUBLE column with value NaN
- WHEN the database is closed and reopened
- THEN the value is NaN (NaN != NaN is true)

#### Scenario: Negative zero persists
- GIVEN a DOUBLE column with value -0.0
- WHEN the database is closed and reopened
- THEN the value is -0.0 (1/-0.0 = -Infinity)

#### Scenario: HUGEINT type persists
- GIVEN a HUGEINT column with max value 170141183460469231731687303715884105727
- WHEN the database is closed and reopened
- THEN the value is preserved exactly

#### Scenario: UHUGEINT type persists
- GIVEN a UHUGEINT column with max value 340282366920938463463374607431768211455
- WHEN the database is closed and reopened
- THEN the value is preserved exactly

#### Scenario: ARRAY type persists
- GIVEN an ARRAY(INTEGER, 3) column with [1, 2, 3]
- WHEN the database is closed and reopened
- THEN the array value [1, 2, 3] is preserved

#### Scenario: ENUM type persists
- GIVEN an ENUM('small', 'medium', 'large') column with value 'medium'
- WHEN the database is closed and reopened
- THEN the value is 'medium'
- AND the enum definition is preserved

#### Scenario: Decimal type persists
- GIVEN a DECIMAL(18,4) column with value 12345.6789
- WHEN the database is closed and reopened
- THEN the decimal value is preserved exactly

#### Scenario: Date/Time types persist
- GIVEN DATE, TIME, TIMESTAMP columns
- WHEN the database is closed and reopened
- THEN all temporal values are preserved

#### Scenario: BLOB type persists
- GIVEN a BLOB column with binary data
- WHEN the database is closed and reopened
- THEN the binary data is identical

#### Scenario: UUID type persists
- GIVEN a UUID column with value
- WHEN the database is closed and reopened
- THEN the UUID is preserved

#### Scenario: Interval type persists
- GIVEN an INTERVAL column
- WHEN the database is closed and reopened
- THEN the interval value is preserved

### Requirement: Nested Types Persistence

The system SHALL persist nested types (LIST, STRUCT, MAP) correctly.

#### Scenario: LIST type persists
- GIVEN a LIST(INTEGER) column with [[1,2,3], [4,5], NULL]
- WHEN the database is closed and reopened
- THEN the list values are preserved

#### Scenario: STRUCT type persists
- GIVEN a STRUCT(x INTEGER, y VARCHAR) column
- WHEN the database is closed and reopened
- THEN the struct values are preserved

#### Scenario: MAP type persists
- GIVEN a MAP(VARCHAR, INTEGER) column
- WHEN the database is closed and reopened
- THEN the map values are preserved

#### Scenario: Nested structures persist
- GIVEN a LIST(STRUCT(a INTEGER, b VARCHAR)) column
- WHEN the database is closed and reopened
- THEN the nested values are preserved

### Requirement: File Format Validation

The system SHALL validate database files on open.

#### Scenario: Magic number validation
- GIVEN a file without valid magic number
- WHEN attempting to open as database
- THEN an error is returned indicating invalid file format

#### Scenario: Version validation
- GIVEN a file with unsupported version
- WHEN attempting to open as database
- THEN an error is returned indicating version incompatibility

#### Scenario: Checksum validation
- GIVEN a file with corrupted checksum
- WHEN attempting to open as database
- THEN an error is returned indicating file corruption

#### Scenario: Truncated file detection
- GIVEN a file that is truncated mid-block
- WHEN attempting to open as database
- THEN an error is returned indicating incomplete file

#### Scenario: Corrupted data block detection
- GIVEN a file with modified bytes in a data block
- WHEN attempting to open as database
- THEN an error is returned indicating block checksum mismatch

#### Scenario: Corrupted catalog detection
- GIVEN a file with corrupted catalog JSON
- WHEN attempting to open as database
- THEN an error is returned indicating catalog parse error

#### Scenario: Corrupted block index detection
- GIVEN a file with invalid block index
- WHEN attempting to open as database
- THEN an error is returned indicating index corruption

### Requirement: Atomic Save

The system SHALL save database files atomically to prevent corruption.

#### Scenario: Temp file used for save
- GIVEN a database being saved
- WHEN the save operation is in progress
- THEN a temporary file is created
- AND the original file is not modified until completion

#### Scenario: Failed save preserves original
- GIVEN a database at path "/tmp/test.duckdb"
- AND a save operation that fails (e.g., disk full simulation)
- WHEN the save fails
- THEN the original file is unchanged
- AND no partial file remains

#### Scenario: Successful save replaces original
- GIVEN a database with changes
- WHEN the save completes successfully
- THEN the original file is replaced atomically
- AND no temporary file remains

### Requirement: Multiple Schema Support

The system SHALL persist databases with multiple schemas.

#### Scenario: Custom schema persists
- GIVEN a schema "analytics" with tables
- WHEN the database is closed and reopened
- THEN the "analytics" schema exists with its tables

#### Scenario: Multiple schemas persist
- GIVEN schemas "main" and "staging" each with tables
- WHEN the database is closed and reopened
- THEN both schemas exist with their tables

### Requirement: Database Reopening

The system SHALL allow reopening a database multiple times.

#### Scenario: Reopen after close
- GIVEN a database that was opened and closed
- WHEN reopening the same file
- THEN all data is accessible

#### Scenario: Multiple open-close cycles
- GIVEN a database file
- WHEN opened, modified, closed 10 times
- THEN all modifications persist

#### Scenario: Insert after reopen
- GIVEN a database that was closed and reopened
- WHEN inserting new data
- THEN the new data is added to existing data

#### Scenario: Modify after reopen
- GIVEN a database that was closed and reopened
- WHEN updating existing data
- THEN the modifications are reflected

### Requirement: Error Handling

The system SHALL handle persistence errors gracefully.

#### Scenario: Read-only filesystem
- GIVEN a database path on read-only filesystem
- WHEN attempting to save
- THEN an appropriate error is returned
- AND the database remains usable in memory

#### Scenario: Missing parent directory
- GIVEN a path with non-existent parent directory
- WHEN attempting to create database
- THEN an appropriate error is returned

#### Scenario: Permission denied
- GIVEN a path where write is not permitted
- WHEN attempting to save
- THEN an appropriate error is returned

### Requirement: Memory Database Unchanged

The system SHALL not change behavior for in-memory databases.

#### Scenario: Memory database no persistence
- GIVEN a database opened with ":memory:"
- WHEN the database is closed
- THEN no file operations occur

#### Scenario: Memory database performance
- GIVEN a database opened with ":memory:"
- WHEN performing operations
- THEN performance is equivalent to previous implementation

### Requirement: Single-Process Constraint

The system SHALL support single-process access only.

#### Scenario: Concurrent multi-process access undefined
- GIVEN a database file at "/tmp/test.duckdb"
- AND process A has the database open
- WHEN process B attempts to open the same file
- THEN behavior is undefined (no file locking)
- AND documentation warns against concurrent process access

#### Scenario: Sequential process access works
- GIVEN process A opens, modifies, and closes the database
- WHEN process B opens the same file afterward
- THEN process B sees all changes from process A

### Requirement: Deterministic Testing

The system SHALL support deterministic testing.

#### Scenario: Tests use t.TempDir
- GIVEN a test function for persistence
- WHEN the test creates database files
- THEN it MUST use t.TempDir() for isolation
- AND no hardcoded paths are used

#### Scenario: Tests are parallelizable
- GIVEN multiple persistence tests
- WHEN run with `go test -parallel`
- THEN all tests pass without interference

#### Scenario: No time dependencies
- GIVEN persistence operations
- WHEN executed
- THEN no time.Now() or time.Sleep() calls affect behavior


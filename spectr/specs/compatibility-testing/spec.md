# Compatibility Testing Specification

## Requirements

### Requirement: Driver Adapter Abstraction

The system SHALL provide a DriverAdapter interface for testing both implementations.

#### Scenario: Create dukdb adapter
- GIVEN the compatibility test framework
- WHEN creating dukdbAdapter
- THEN adapter implements DriverAdapter interface
- AND Open returns *sql.DB connected to dukdb-go

#### Scenario: Create duckdb adapter with CGO
- GIVEN build tag duckdb_cgo is set
- WHEN creating duckdbAdapter
- THEN adapter implements DriverAdapter interface
- AND Open returns *sql.DB connected to duckdb-go

#### Scenario: Feature detection
- GIVEN a DriverAdapter
- WHEN calling SupportsArrow()
- THEN returns true if Arrow is available
- AND SupportsTableUDF returns true if Table UDFs work

### Requirement: SQL Compatibility Testing

The system SHALL verify SQL query results match between implementations.

#### Scenario: DDL compatibility
- GIVEN both adapters
- WHEN running CREATE TABLE statement
- THEN both create identical table structure
- AND PRAGMA table_info returns same columns

#### Scenario: DML compatibility
- GIVEN both adapters with same table
- WHEN running INSERT/UPDATE/DELETE
- THEN affected row counts match
- AND table contents are identical

#### Scenario: Query result compatibility
- GIVEN both adapters with same data
- WHEN running SELECT query
- THEN column count matches
- AND row count matches
- AND values match (with type-appropriate comparison)

### Requirement: Type Compatibility Testing

The system SHALL verify all 37 DuckDB types work identically.

#### Scenario: Integer type round-trip
- GIVEN a table with INTEGER column
- WHEN inserting value 42
- AND selecting it back
- THEN returned value equals 42

#### Scenario: HugeInt type round-trip
- GIVEN a table with HUGEINT column
- WHEN inserting maximum HUGEINT value
- AND selecting it back
- THEN returned value equals original

#### Scenario: Complex type round-trip
- GIVEN a table with LIST column
- WHEN inserting [1, 2, 3]
- AND selecting it back
- THEN returned value equals [1, 2, 3]

#### Scenario: NULL handling
- GIVEN a table with nullable column
- WHEN inserting NULL
- AND scanning with sql.NullString
- THEN Valid is false

### Requirement: API Compatibility Testing

The system SHALL verify driver interface compatibility.

#### Scenario: Connection lifecycle
- GIVEN both adapters
- WHEN calling Open, Ping, Close
- THEN both succeed without error

#### Scenario: Transaction support
- GIVEN both adapters
- WHEN starting transaction, inserting, committing
- THEN data is persisted in both

#### Scenario: Prepared statement support
- GIVEN both adapters
- WHEN preparing statement with parameters
- THEN NumInput returns same count
- AND Exec with parameters succeeds

#### Scenario: Raw connection access
- GIVEN both adapters
- WHEN calling Conn.Raw()
- THEN underlying driver connection is accessible

### Requirement: Feature Compatibility Testing

The system SHALL verify advanced features work identically.

#### Scenario: Appender compatibility
- GIVEN both adapters with table
- WHEN using Appender to insert 1000 rows
- THEN both tables have 1000 rows
- AND data matches

#### Scenario: Scalar UDF compatibility
- GIVEN both adapters
- WHEN registering scalar UDF "double(x)"
- AND calling SELECT double(21)
- THEN result is 42

#### Scenario: Table UDF compatibility
- GIVEN both adapters
- WHEN registering table UDF "range(n)"
- AND calling SELECT * FROM range(5)
- THEN returns 5 rows: 0, 1, 2, 3, 4

#### Scenario: Profiling compatibility
- GIVEN both adapters
- WHEN enabling profiling
- AND executing query
- THEN profiling info available in both

### Requirement: Benchmark Query Testing

The system SHALL run TPC-H queries against both implementations.

#### Scenario: TPC-H data generation
- GIVEN both adapters
- WHEN calling dbgen(sf=0.01)
- THEN TPC-H tables are populated

#### Scenario: TPC-H query execution
- GIVEN both adapters with TPC-H data
- WHEN running TPC-H Q1
- THEN result row count matches
- AND aggregate values match (within tolerance)

#### Scenario: TPC-H full suite
- GIVEN both adapters with TPC-H data
- WHEN running all 22 TPC-H queries
- THEN all queries succeed on both
- AND results match

### Requirement: Deterministic Testing Support

The system SHALL support mock clock for deterministic tests.

#### Scenario: Mock clock injection
- GIVEN DriverAdapter with mock clock
- WHEN querying CURRENT_TIMESTAMP
- THEN returns mock time

#### Scenario: Profiling with mock clock
- GIVEN profiling context with mock clock
- WHEN advancing clock 100ms
- THEN elapsed time shows 100ms

#### Scenario: Timeout with mock clock
- GIVEN context with deadline
- WHEN advancing clock past deadline
- THEN query returns context.DeadlineExceeded

### Requirement: Test Reporting

The system SHALL generate compatibility reports.

#### Scenario: Text report
- GIVEN completed test run
- WHEN generating text report
- THEN shows pass/fail count per category

#### Scenario: Markdown report
- GIVEN completed test run
- WHEN generating markdown report
- THEN produces formatted table of results

#### Scenario: JUnit XML report
- GIVEN completed test run
- WHEN generating JUnit report
- THEN produces valid JUnit XML
- AND CI systems can parse it

### Requirement: Skip Flag Support

The system SHALL support skipping tests for missing features.

#### Scenario: Skip dukdb test
- GIVEN test with SkipDukdb=true
- WHEN running tests
- THEN test is skipped for dukdb-go
- AND runs for duckdb-go if available

#### Scenario: Skip duckdb test
- GIVEN test with SkipDuckdb=true
- WHEN running tests
- THEN test is skipped for duckdb-go
- AND runs for dukdb-go

#### Scenario: Feature not implemented
- GIVEN feature not yet in dukdb-go
- WHEN running compatibility test
- THEN test skipped with clear message
- AND does not fail the suite

### Requirement: Parallel Test Execution

The system SHALL support parallel test execution by category.

#### Scenario: Parallel categories
- GIVEN multiple test categories
- WHEN running all tests
- THEN categories run in parallel
- AND tests within category run sequentially

#### Scenario: Isolated databases
- GIVEN parallel test execution
- WHEN each test runs
- THEN each test has isolated database
- AND no cross-test interference


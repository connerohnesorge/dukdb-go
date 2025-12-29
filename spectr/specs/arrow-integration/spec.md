# Arrow Integration Specification

## Requirements

### Requirement: Arrow Connection Creation

The package SHALL create Arrow interfaces from connections.

#### Scenario: Create Arrow from connection
- GIVEN an open sql.Conn
- WHEN calling NewArrowFromConn(conn)
- THEN Arrow interface is returned without error

#### Scenario: Create Arrow from closed connection
- GIVEN a closed sql.Conn
- WHEN calling NewArrowFromConn(conn)
- THEN error is returned

### Requirement: Arrow Query Execution

The Arrow interface SHALL execute queries returning Arrow record readers.

#### Scenario: Query returns RecordReader
- GIVEN Arrow interface
- WHEN calling Query("SELECT 1, 'hello'")
- THEN arrow.RecordReader is returned

#### Scenario: Query with parameters
- GIVEN Arrow interface
- WHEN calling Query("SELECT $1", 42)
- THEN RecordReader contains result with value 42

#### Scenario: Query error handling
- GIVEN Arrow interface with invalid SQL
- WHEN calling Query("INVALID SQL")
- THEN error is returned

### Requirement: Arrow Schema Mapping

The Arrow result SHALL have correct schema.

#### Scenario: Integer type mapping
- GIVEN query returning INTEGER column
- WHEN examining Arrow schema
- THEN column type is arrow.INT32

#### Scenario: String type mapping
- GIVEN query returning VARCHAR column
- WHEN examining Arrow schema
- THEN column type is arrow.STRING

#### Scenario: Nested type mapping
- GIVEN query returning LIST(INTEGER) column
- WHEN examining Arrow schema
- THEN column type is arrow.ListOf(arrow.INT32)

### Requirement: Arrow Record Streaming

The RecordReader SHALL support streaming.

#### Scenario: Next advances to next batch
- GIVEN RecordReader with multiple batches
- WHEN calling Next() repeatedly
- THEN each call advances to next batch

#### Scenario: Record returns current batch
- GIVEN RecordReader after Next() returns true
- WHEN calling Record()
- THEN current RecordBatch is returned

#### Scenario: Next returns false at end
- GIVEN RecordReader at end of results
- WHEN calling Next()
- THEN false is returned

### Requirement: Arrow Data Conversion

The data conversion SHALL preserve values correctly.

#### Scenario: NULL value conversion
- GIVEN query result with NULL values
- WHEN examining Arrow array
- THEN NULL positions are correctly marked

#### Scenario: Temporal type precision
- GIVEN TIMESTAMP_NS value
- WHEN converted to Arrow
- THEN nanosecond precision is preserved

#### Scenario: Decimal conversion
- GIVEN DECIMAL value with specific precision
- WHEN converted to Arrow
- THEN precision and scale are preserved

### Requirement: Arrow Build Tag

Arrow functionality SHALL require build tag.

#### Scenario: Build without tag
- GIVEN build without duckdb_arrow tag
- WHEN compiling
- THEN Arrow types are not available

#### Scenario: Build with tag
- GIVEN build with duckdb_arrow tag
- WHEN compiling
- THEN Arrow types are available


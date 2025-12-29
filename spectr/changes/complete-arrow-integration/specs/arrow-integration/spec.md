# Arrow Integration Delta Spec

## ADDED Requirements

### Requirement: Arrow View Registration

The Arrow interface SHALL support registering external Arrow data as queryable tables.

#### Scenario: Register Arrow RecordReader as view
- GIVEN an Arrow interface and RecordReader with schema {id: INT32, name: STRING}
- WHEN calling RegisterView(reader, "my_view")
- THEN a release function is returned without error
- AND queries against "my_view" return data from the reader

#### Scenario: Query registered view
- GIVEN a registered Arrow view "test_data" with 3 rows
- WHEN executing SELECT * FROM test_data
- THEN all 3 rows are returned with correct types

#### Scenario: Release view
- GIVEN a registered Arrow view with release function
- WHEN calling the release function
- THEN subsequent queries to the view return error

#### Scenario: Register duplicate view name
- GIVEN an Arrow view already registered as "existing_view"
- WHEN calling RegisterView(reader, "existing_view")
- THEN error is returned indicating duplicate name

### Requirement: Arrow to DuckDB Type Mapping

The package SHALL convert Arrow types to DuckDB TypeInfo.

#### Scenario: Map primitive types
- GIVEN Arrow type INT32
- WHEN calling arrowToDuckDBType
- THEN TypeInfo with InternalType() == TYPE_INTEGER is returned

#### Scenario: Map temporal types
- GIVEN Arrow TIMESTAMP with Microsecond unit and UTC timezone
- WHEN calling arrowToDuckDBType
- THEN TypeInfo with InternalType() == TYPE_TIMESTAMP is returned

#### Scenario: Map nested LIST type
- GIVEN Arrow LIST of INT64
- WHEN calling arrowToDuckDBType
- THEN TypeInfo with InternalType() == TYPE_LIST is returned
- AND Details().(*ListDetails).Child has InternalType() == TYPE_BIGINT

#### Scenario: Map STRUCT type with fields
- GIVEN Arrow STRUCT {name: STRING, age: INT32}
- WHEN calling arrowToDuckDBType
- THEN TypeInfo with InternalType() == TYPE_STRUCT is returned
- AND Details().(*StructDetails).Entries has 2 entries with correct names and types

#### Scenario: Map unsupported type
- GIVEN Arrow DURATION type (not supported by DuckDB)
- WHEN calling arrowToDuckDBType
- THEN error is returned with descriptive message

### Requirement: Arrow Schema Conversion

The package SHALL convert Arrow schemas to DuckDB column definitions.

#### Scenario: Convert simple schema
- GIVEN Arrow schema with fields {id: INT64, name: STRING, active: BOOL}
- WHEN calling arrowSchemaToDuckDB
- THEN []ColumnInfo with 3 entries is returned
- AND each entry has correct Name and TypeInfo

#### Scenario: Convert schema with nested types
- GIVEN Arrow schema with field {items: LIST(STRUCT{price: FLOAT64})}
- WHEN calling arrowSchemaToDuckDB
- THEN ColumnInfo for "items" has correct nested TypeInfo

### Requirement: DataChunk to Arrow Conversion

The package SHALL convert DuckDB DataChunks to Arrow RecordBatches using copy semantics.

#### Scenario: Convert primitive columns
- GIVEN DataChunk with INT64 column containing [1, 2, 3]
- WHEN calling DataChunkToRecordBatch
- THEN Arrow Record with Int64 array containing [1, 2, 3] is returned

#### Scenario: Convert with NULL values
- GIVEN DataChunk with nullable column containing [1, NULL, 3]
- WHEN calling DataChunkToRecordBatch
- THEN Arrow Record with correct validity bitmap is returned

#### Scenario: Convert BOOLEAN with correct unpacking
- GIVEN DataChunk with BOOLEAN column containing [true, false, true]
- WHEN calling DataChunkToRecordBatch
- THEN Arrow BOOL array is correctly bit-packed (Arrow format)

### Requirement: Arrow to DataChunk Conversion

The package SHALL convert Arrow RecordBatches to DuckDB DataChunks.

#### Scenario: Convert Arrow record to chunk
- GIVEN Arrow Record with {id: [1,2,3], name: ["a","b","c"]}
- WHEN calling RecordBatchToDataChunk
- THEN DataChunk with matching values is returned

#### Scenario: Convert nested Arrow data
- GIVEN Arrow Record with LIST column containing [[1,2], [3]]
- WHEN calling RecordBatchToDataChunk
- THEN DataChunk with LIST values matching is returned

## MODIFIED Requirements

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

#### Scenario: Query with context cancellation
- GIVEN Arrow interface and cancelled context
- WHEN calling QueryContext with cancelled context
- THEN context.Canceled error is returned

#### Scenario: Query with deadline using mock clock
- GIVEN Arrow interface with mock clock
- WHEN deadline passes during query execution
- THEN context.DeadlineExceeded is returned deterministically

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

#### Scenario: Round-trip conversion preserves data
- GIVEN Arrow data registered as view
- WHEN queried and converted back to Arrow
- THEN values match original exactly

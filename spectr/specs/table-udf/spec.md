# Table Udf Specification

## Requirements

### Requirement: Table UDF Registration

The package SHALL allow registration of table-valued functions.

#### Scenario: Register row-based table function
- GIVEN a RowTableFunction returning RowTableSource
- WHEN calling RegisterTableUDF(conn, "my_range", func)
- THEN no error is returned
- AND function is usable in FROM clause

#### Scenario: Register chunk-based table function
- GIVEN a ChunkTableFunction returning ChunkTableSource
- WHEN calling RegisterTableUDF(conn, "my_data", func)
- THEN no error is returned

#### Scenario: Register parallel table function
- GIVEN a ParallelChunkTableFunction with MaxThreads = 4
- WHEN calling RegisterTableUDF(conn, "parallel_data", func)
- THEN function can execute with up to 4 workers

### Requirement: Table UDF Column Definition

The table source SHALL declare its output columns.

#### Scenario: ColumnInfos defines schema
- GIVEN RowTableSource with ColumnInfos returning [("id", INTEGER), ("name", VARCHAR)]
- WHEN table function is called
- THEN result has columns "id" and "name" with correct types

#### Scenario: Column type validation
- GIVEN ColumnInfos returning unsupported type
- WHEN function is bound
- THEN error is returned with type information

### Requirement: Table UDF Row Execution

The row-based table function SHALL produce rows sequentially.

#### Scenario: Simple row generation
- GIVEN table function generating numbers 1-10
- WHEN executing "SELECT * FROM my_range(10)"
- THEN result contains 10 rows with values 1-10

#### Scenario: FillRow returns false for completion
- GIVEN table function with finite data
- WHEN FillRow returns (false, nil)
- THEN execution stops
- AND no error is raised

#### Scenario: FillRow returns error
- GIVEN table function that encounters error
- WHEN FillRow returns (false, err)
- THEN query fails with error message

### Requirement: Table UDF Chunk Execution

The chunk-based table function SHALL produce rows in batches.

#### Scenario: Chunk-based generation
- GIVEN ChunkTableSource generating 5000 rows
- WHEN executing query
- THEN FillChunk is called multiple times
- AND chunks are combined into result

#### Scenario: Partial chunk
- GIVEN data that doesn't fill complete chunk
- WHEN FillChunk sets size < capacity
- THEN partial chunk is correctly returned

#### Scenario: Empty chunk signals completion
- GIVEN ChunkTableSource with finite data
- WHEN FillChunk sets size = 0
- THEN execution completes

### Requirement: Table UDF Parallel Execution

The parallel table function SHALL execute with multiple workers.

#### Scenario: Parallel row execution
- GIVEN ParallelRowTableSource with MaxThreads = 4
- WHEN executing query
- THEN up to 4 goroutines call FillRow with thread-local state

#### Scenario: Thread-local state isolation
- GIVEN parallel function with NewLocalState returning counter
- WHEN multiple workers execute
- THEN each worker has independent counter

#### Scenario: Parallel results combined correctly
- GIVEN parallel function producing overlapping data
- WHEN query completes
- THEN all rows from all workers are in result

### Requirement: Table UDF Column Projection

The table function SHALL receive projection information.

#### Scenario: Projected column check
- GIVEN table with columns [a, b, c]
- WHEN executing "SELECT a, c FROM my_table()"
- THEN Row.IsProjected(0) = true (a)
- AND Row.IsProjected(1) = false (b)
- AND Row.IsProjected(2) = true (c)

#### Scenario: Skip unprojected computation
- GIVEN function checking IsProjected before expensive work
- WHEN column is not projected
- THEN expensive computation is skipped

### Requirement: Table UDF Cardinality

The table function SHALL provide cardinality estimates.

#### Scenario: Exact cardinality
- GIVEN CardinalityInfo{Cardinality: 1000, Exact: true}
- WHEN optimizer plans query
- THEN exact row count is used

#### Scenario: Approximate cardinality
- GIVEN CardinalityInfo{Cardinality: 10000, Exact: false}
- WHEN optimizer plans query
- THEN estimate is used for costing

### Requirement: Table UDF Arguments

The table function SHALL support typed arguments.

#### Scenario: Positional arguments
- GIVEN function with Arguments = [INTEGER, VARCHAR]
- WHEN calling "SELECT * FROM my_func(10, 'test')"
- THEN BindArguments receives (nil, 10, "test")

#### Scenario: Named arguments
- GIVEN function with NamedArguments = {"limit": INTEGER}
- WHEN calling "SELECT * FROM my_func(limit := 100)"
- THEN BindArguments receives ({"limit": 100})

#### Scenario: Mixed arguments
- GIVEN function with positional and named arguments
- WHEN calling with both
- THEN both are passed to BindArguments correctly

### Requirement: Table UDF Context Support

The table function SHALL support context-aware binding.

#### Scenario: BindArgumentsContext receives context
- GIVEN function with BindArgumentsContext
- WHEN query is executed
- THEN context is passed to binding function

#### Scenario: Context cancellation during bind
- GIVEN cancelled context
- WHEN BindArgumentsContext is called
- THEN function can return context.Err()

### Requirement: Table UDF Init Lifecycle

The table source SHALL be initialized before data production.

#### Scenario: Init called before FillRow
- GIVEN RowTableSource with Init method
- WHEN function is executed
- THEN Init() is called before first FillRow()

#### Scenario: Init error prevents execution
- GIVEN Init() that returns error
- WHEN function is executed
- THEN query fails with init error
- AND FillRow is never called

### Requirement: Table UDF Error Handling

The table function SHALL properly propagate errors.

#### Scenario: BindArguments error
- GIVEN BindArguments that returns error
- WHEN function is called with arguments
- THEN query fails with bind error

#### Scenario: FillRow error
- GIVEN FillRow that returns error
- WHEN processing rows
- THEN query fails with row error

#### Scenario: Parallel worker error
- GIVEN one parallel worker that fails
- WHEN other workers are running
- THEN all workers stop
- AND error is returned to caller

### Requirement: Table Functions with Secrets

Table functions SHALL automatically use applicable secrets for cloud storage operations.

#### Scenario: read_parquet from S3 uses secret
- GIVEN a secret exists for 's3://my-bucket/'
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/data.parquet')`
- THEN secret is retrieved and credentials applied
- AND Parquet file is read from S3 using the credentials

#### Scenario: read_csv from GCS uses secret
- GIVEN a secret exists for 'gs://my-bucket/'
- WHEN executing `SELECT * FROM read_csv('gs://my-bucket/data.csv')`
- THEN secret is retrieved and credentials applied
- AND CSV file is read from GCS using the credentials

#### Scenario: glob pattern on S3 uses secret
- GIVEN a secret exists for 's3://my-bucket/'
- WHEN executing `SELECT * FROM read_parquet('s3://my-bucket/*.parquet')`
- THEN secret is retrieved and credentials applied
- AND matching files are listed and read using the credentials

#### Scenario: write_json to Azure uses secret
- GIVEN a secret exists for 'azure://my-container/'
- WHEN executing `COPY (SELECT * FROM table) TO 'azure://my-container/data.json'`
- THEN secret is retrieved and credentials applied
- AND JSON file is written to Azure using the credentials

### Requirement: Series Generation Table Functions Registration

The system SHALL register generate_series and range as recognized table functions in the binder and executor, accepting 2 or 3 arguments with type-appropriate defaults.

#### Scenario: generate_series recognized as table function in FROM

- WHEN executing `SELECT n FROM generate_series(1, 3) AS t(n)`
- THEN the binder resolves generate_series as a table function
- AND the alias "t" with column "n" is applied correctly

#### Scenario: range recognized as table function in FROM

- WHEN executing `SELECT * FROM range(1, 10) AS t(val)`
- THEN the binder resolves range as a table function
- AND the alias "t" with column "val" is applied correctly

#### Scenario: Two-argument form uses default step

- WHEN executing `SELECT * FROM generate_series(1, 5)`
- THEN the default step of 1 is used for integer arguments
- AND the series produces values 1 through 5

#### Scenario: Temporal two-argument form uses default interval step

- WHEN executing `SELECT * FROM generate_series(DATE '2024-01-01', DATE '2024-01-03')`
- THEN the default step of INTERVAL '1 day' is used
- AND the series produces daily dates from 2024-01-01 through 2024-01-03

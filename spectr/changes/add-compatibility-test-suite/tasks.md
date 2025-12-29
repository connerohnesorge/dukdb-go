# Tasks: Add Compatibility Test Suite

## Phase 1: Framework Setup

- [ ] **1.1** Create `compatibility/` directory structure
  - Create compatibility/framework.go
  - Create compatibility/adapters.go
  - Create compatibility/runner.go
  - Add go.mod dependencies (testify, quartz)

- [ ] **1.2** Implement DriverAdapter interface
  - Define interface with Open, Close, WithClock
  - Define feature detection methods
  - Define UDF registration methods

- [ ] **1.3** Implement dukdbAdapter
  - Implement all DriverAdapter methods
  - Wire to dukdb.Connector
  - Add clock injection support

- [ ] **1.4** Implement duckdbAdapter (CGO)
  - Add build tag: //go:build duckdb_cgo
  - Implement DriverAdapter for duckdb-go
  - Add runtime availability check

- [ ] **1.5** Implement TestRunner
  - Create RunCompatibilityTests method
  - Support SkipDukdb/SkipDuckdb flags
  - Add parallel execution by category

## Phase 2: SQL Compatibility Tests

- [ ] **2.1** DDL tests
  - CREATE TABLE, CREATE TABLE IF NOT EXISTS
  - CREATE INDEX, DROP INDEX
  - ALTER TABLE (ADD COLUMN, DROP COLUMN)
  - DROP TABLE, DROP TABLE IF EXISTS

- [ ] **2.2** DML tests
  - INSERT VALUES, INSERT SELECT
  - UPDATE with WHERE
  - DELETE with WHERE
  - UPSERT / INSERT OR REPLACE

- [ ] **2.3** Query tests (basic)
  - SELECT *, SELECT columns
  - WHERE clauses (comparison, LIKE, IN)
  - ORDER BY, LIMIT, OFFSET
  - DISTINCT

- [ ] **2.4** Query tests (advanced)
  - GROUP BY, HAVING
  - JOINs (INNER, LEFT, RIGHT, FULL)
  - Subqueries (scalar, table)
  - CTEs (WITH clause)
  - UNION, INTERSECT, EXCEPT

- [ ] **2.5** Aggregate function tests
  - COUNT, SUM, AVG, MIN, MAX
  - COUNT DISTINCT
  - GROUP_CONCAT / STRING_AGG

- [ ] **2.6** Window function tests
  - ROW_NUMBER, RANK, DENSE_RANK
  - LAG, LEAD
  - FIRST_VALUE, LAST_VALUE
  - SUM/AVG OVER (PARTITION BY)

## Phase 3: Type Compatibility Tests

- [ ] **3.1** Integer type tests
  - TINYINT, SMALLINT, INTEGER, BIGINT
  - UTINYINT, USMALLINT, UINTEGER, UBIGINT
  - HUGEINT, UHUGEINT
  - Round-trip through scan/insert

- [ ] **3.2** Floating point tests
  - FLOAT, DOUBLE
  - Special values (NaN, Inf, -Inf)
  - Precision preservation

- [ ] **3.3** Decimal tests
  - Various precision/scale combinations
  - Arithmetic operations
  - Comparison operators

- [ ] **3.4** String/Binary tests
  - VARCHAR with various lengths
  - BLOB with binary data
  - Unicode handling

- [ ] **3.5** Date/Time tests
  - DATE, TIME, TIMESTAMP
  - TIME WITH TIME ZONE
  - TIMESTAMP WITH TIME ZONE
  - TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS
  - INTERVAL

- [ ] **3.6** Complex type tests
  - UUID
  - JSON
  - LIST (nested)
  - STRUCT
  - MAP
  - ARRAY (fixed size)
  - UNION
  - ENUM
  - BIT

## Phase 4: API Compatibility Tests

- [ ] **4.1** Connection lifecycle tests
  - Open/Close
  - Ping
  - Conn.Raw access
  - Connection pooling

- [ ] **4.2** Transaction tests
  - Begin/Commit
  - Begin/Rollback
  - Isolation levels
  - Savepoints

- [ ] **4.3** Prepared statement tests
  - Prepare/Close
  - Exec/Query
  - NumInput detection
  - Column type detection

- [ ] **4.4** Parameter binding tests
  - Named parameters ($name)
  - Positional parameters ($1, $2)
  - NULL values
  - All supported types

- [ ] **4.5** Result scanning tests
  - Primitive types
  - NULL values
  - Complex types
  - Custom scanners

## Phase 5: Feature Compatibility Tests

- [ ] **5.1** Appender tests
  - Create/Close
  - Append rows
  - Batch operations
  - Flush behavior
  - Error handling

- [ ] **5.2** Scalar UDF tests
  - Simple function registration
  - Context-aware functions
  - Overloading
  - NULL handling
  - Panic recovery

- [ ] **5.3** Table UDF tests
  - ChunkTableSource
  - RowTableSource
  - Parameter binding
  - Streaming results

- [ ] **5.4** Profiling tests
  - Enable/Disable
  - Operator profiling
  - Timing information
  - Mock clock integration

## Phase 6: Benchmark Tests

- [ ] **6.1** TPC-H setup
  - Load schema
  - Generate data (sf=0.01)
  - Verify data loaded

- [ ] **6.2** TPC-H query tests
  - Q01-Q05
  - Q06-Q10
  - Q11-Q15
  - Q16-Q22

- [ ] **6.3** Result verification
  - Row count checks
  - Spot value checks
  - Column type verification

## Phase 7: Deterministic Tests

- [ ] **7.1** Mock clock profiling test
  - Set mock time
  - Execute query
  - Advance clock
  - Verify elapsed time

- [ ] **7.2** Mock clock timeout test
  - Set deadline
  - Advance past deadline
  - Verify timeout error

- [ ] **7.3** Mock clock timestamp test
  - Set mock time
  - Query CURRENT_TIMESTAMP
  - Verify returns mock time

## Phase 8: Reporting and CI

- [ ] **8.1** Report generation
  - Text format
  - Markdown format
  - JUnit XML format

- [ ] **8.2** CI integration
  - Add to GitHub Actions
  - Run on PR
  - Run on release

- [ ] **8.3** Documentation
  - Update README with compatibility status
  - Document how to run tests
  - Document how to read reports

## Validation Criteria

- [ ] All SQL tests pass on dukdb-go
- [ ] All type tests pass with round-trip verification
- [ ] All API tests match duckdb-go behavior
- [ ] TPC-H queries return same results on both implementations
- [ ] Deterministic tests work with mock clock
- [ ] CI pipeline runs tests automatically
- [ ] Compatibility report generated on release

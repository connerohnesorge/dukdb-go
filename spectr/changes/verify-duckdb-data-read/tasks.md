# Tasks: Verify DuckDB Row Data Reading

## Phase 1: Test Infrastructure

- [x] 1.1 Create `data_read_verify_test.go` with helper functions to create DuckDB CLI test files and verify row data matches expected values
- [x] 1.2 Implement helper `createDuckDBTestFile(sql string) (path string, cleanup func())` that creates temp file with DuckDB CLI
- [x] 1.3 Implement helper `verifyRowData(t *testing.T, storage StorageBackend, table string, expected [][]interface{})` for assertions

## Phase 2: Basic Type Verification

- [x] 2.1 Test INTEGER column read: insert known values (0, 1, -1, MAX_INT, MIN_INT), verify exact matches
- [x] 2.2 Test VARCHAR column read: insert ASCII strings, verify exact matches
- [x] 2.3 Test VARCHAR Unicode: insert multi-byte UTF-8 strings (emoji, CJK), verify byte-exact preservation
- [x] 2.4 Test BOOLEAN column read: insert TRUE, FALSE, NULL, verify correct conversion

## Phase 3: All Numeric Types

- [x] 3.1 Test TINYINT, SMALLINT, INTEGER, BIGINT, HUGEINT with boundary values
- [x] 3.2 Test UTINYINT, USMALLINT, UINTEGER, UBIGINT, UHUGEINT with boundary values
- [x] 3.3 Test FLOAT, DOUBLE with special values (0, -0, Inf, -Inf, NaN, epsilon, max)
- [x] 3.4 Test DECIMAL with various precision/scale combinations

## Phase 4: Temporal Types

- [x] 4.1 Test DATE with various dates (epoch, Y2K, negative years, far future)
- [x] 4.2 Test TIME, TIME WITH TIME ZONE with various times including midnight, noon, max precision
- [x] 4.3 Test TIMESTAMP, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS with microsecond precision verification
- [x] 4.4 Test TIMESTAMP WITH TIME ZONE for timezone preservation
- [x] 4.5 Test INTERVAL for all interval components

## Phase 5: Complex Types

- [x] 5.1 Test LIST(INTEGER), LIST(VARCHAR) with nested values and empty lists
- [x] 5.2 Test STRUCT with named fields and nested structs
- [x] 5.3 Test MAP(VARCHAR, INTEGER) with various key/value combinations
- [x] 5.4 Test nested complex types: LIST(STRUCT), MAP(VARCHAR, LIST(INTEGER))
- [x] 5.5 Test BLOB with binary data including null bytes
- [x] 5.6 Test UUID with standard format UUIDs

## Phase 6: NULL Handling

- [x] 6.1 Test column with all NULLs - verify validity mask interpretation
- [x] 6.2 Test column with mixed NULL/non-NULL - verify correct positions
- [x] 6.3 Test NULL in complex types (LIST with NULL elements, STRUCT with NULL fields)
- [x] 6.4 Test nullable vs non-nullable column constraints

## Phase 7: Compression Verification

- [x] 7.1 Test CONSTANT compression: column with single repeated value
- [x] 7.2 Test RLE compression: column with long runs of values
- [x] 7.3 Test DICTIONARY compression: column with limited cardinality strings
- [x] 7.4 Test BITPACKING compression: column with small integers
- [x] 7.5 Test mixed compression: table where different columns use different algorithms

## Phase 8: Scale and Edge Cases

- [x] 8.1 Test table with 0 rows (empty table)
- [x] 8.2 Test table with exactly 1 row
- [x] 8.3 Test table with large row count (100K rows) spanning multiple row groups
- [x] 8.4 Test table with many columns (50+ columns)
- [x] 8.5 Test extreme string lengths (empty, 1 char, 10KB)
- [x] 8.6 Test special characters: newlines, tabs, quotes, backslashes in strings

## Phase 9: Cleanup and Documentation

- [x] 9.1 Remove `skipOnFormatError` from passing interop tests
- [x] 9.2 Document any discovered bugs as issues
- [x] 9.3 Update gap-analysis.md to reflect GAP-001 read path status
- [x] 9.4 Add CI job for DuckDB CLI interop testing (requires duckdb binary)

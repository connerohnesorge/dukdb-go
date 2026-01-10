# Change: Verify DuckDB Row Data Reading from CLI Files

## Why

While catalog reading from DuckDB CLI files now works (recently fixed `skipStorageMetadata` handling), we have not verified that **actual row data** can be read correctly. The infrastructure exists (RowGroupReader, decompression, TableScanner), but interop tests skip on format errors, leaving uncertainty about:

1. Whether decompression algorithms produce correct values
2. Whether row group metadata is parsed correctly
3. Whether NULL handling via validity masks works
4. Whether all data types round-trip correctly

**Current State**:
- Catalog reading: WORKING (just fixed)
- Row data reading infrastructure: IMPLEMENTED (not verified against DuckDB CLI files)
- Interop tests: Use `skipOnFormatError()` which masks potential issues

**Evidence**: Tests in `interop_test.go` skip when they encounter format errors rather than verifying actual data values.

## What

Create comprehensive tests that verify dukdb-go can read **row data** (not just schema) from files created by DuckDB CLI:

1. **End-to-End Data Verification** - Create test files with DuckDB CLI, read rows with dukdb-go, verify values match
2. **Type Coverage** - Test all 40+ data types for correct value preservation
3. **Compression Verification** - Verify each compression algorithm produces correct output
4. **NULL Handling** - Verify validity mask interpretation
5. **Edge Cases** - Empty tables, large row groups, Unicode strings, extreme values

## Impact

### Users
- Confidence that DuckDB CLI files can be queried correctly
- Verified data integrity for migration workflows
- Breaking: None (verification only)

### Codebase
- **New Files**:
  - `internal/storage/duckdb/data_read_verify_test.go` - Comprehensive data verification tests
- **Modified Files**:
  - `internal/storage/duckdb/interop_test.go` - Remove skipOnFormatError for passing tests
- **Potential Bug Fixes**:
  - Decompression edge cases
  - Type conversion issues
  - NULL handling bugs

### Risks
- **Low Risk**: This is verification/testing, not implementation changes
- Tests may reveal bugs that require separate fixes
- **Mitigation**: Each bug found becomes a tracked issue

## Success Criteria

- [ ] Test reads INTEGER values from DuckDB CLI file and verifies correctness
- [ ] Test reads VARCHAR values and verifies Unicode preservation
- [ ] Test reads all primitive types (BOOLEAN, TINYINT through HUGEINT, FLOAT, DOUBLE)
- [ ] Test reads temporal types (DATE, TIME, TIMESTAMP variants)
- [ ] Test reads complex types (LIST, STRUCT, MAP)
- [ ] Test reads NULL values correctly
- [ ] Test reads compressed columns (Dictionary, RLE, BitPacking)
- [ ] Test reads tables with multiple row groups
- [ ] All tests pass without skipOnFormatError

## Dependencies

### Required Before
- Catalog reading from DuckDB CLI files (COMPLETED)
- RowGroupReader implementation (EXISTS)
- Decompression implementations (EXISTS)

### Enables After
- Full GAP-001 completion for read path
- Confidence for production use
- Foundation for write verification

## Related Specs

- `duckdb-storage-format` - VERIFIES (Row Group Reading, Decompression Support requirements)
- `catalog-persistence` - USES (no changes)

## Rollout Plan

### Phase 1: Basic Type Verification
- INTEGER, VARCHAR, BOOLEAN tests
- Simple SELECT * queries

### Phase 2: All Types
- All primitive types
- Temporal types
- Complex nested types

### Phase 3: Compression
- Force different compression by data patterns
- Verify each algorithm

### Phase 4: Edge Cases
- Empty tables, NULLs, extreme values
- Large row groups spanning blocks

## Approval Checklist

- [ ] Test plan reviewed
- [ ] Coverage targets defined
- [ ] Tasks sequenced (see tasks.md)

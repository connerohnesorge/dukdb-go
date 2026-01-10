# DuckDB Write Compatibility Diagnostics

## Summary

**Current Status:**
- Empty tables: WORKING - DuckDB CLI can read files created by dukdb-go
- Tables with rows: NOT WORKING - "Failed to load metadata pointer" error when InsertRows is used

## Format Differences Found (Empty Tables)

### 1. FileHeader (bytes 0-7)
- These are XXH64 checksum bytes over the file header content
- Different checksums are EXPECTED because version strings differ
- Both use DUCK magic bytes at offset 8-11 ✓

### 2. Version String (offset 0x34)
- dukdb-go: `dukdb-go`
- DuckDB CLI: `v1.4.3`
- This is informational and does NOT cause compatibility issues

### 3. Platform Version (offset 0x54)
- dukdb-go: (empty)
- DuckDB CLI: `d1dc88f950`
- This is the git commit hash, informational only

### 4. Schema Name in Catalog
- dukdb-go uses: `system`
- DuckDB CLI uses: `duckdb`
- This may be causing issues with system catalog lookup

### 5. Database Headers
- Different checksums and pointers due to different metadata block layouts
- Block count is the same (274432 bytes = same file size)

## Root Cause Analysis

### What Works (Empty Tables)

The empty table works because:
1. File header is valid (checksum is correct for the content)
2. Database headers point to valid metadata blocks
3. Catalog serialization is correct
4. No row data to serialize

### What Fails (Tables with Rows)

When InsertRows is called:
1. Row group pointers are added
2. Metadata blocks are updated
3. The metadata pointer encoding appears to be incorrect
4. Error: "Failed to load metadata pointer (id 0, idx 1, ptr 72057594037927936)"

### Metadata Pointer Issue

The error indicates:
- id=0, idx=1, ptr=72057594037927936
- ptr=0x100000000000000 in hex
- This is an invalid pointer value (bit 56 set, all others zero)

The issue is likely in how `MetaBlockPointer.Encode()` creates the packed 64-bit value:
```
Encoding format:
- bits 0-55 = block_id (56 bits)
- bits 56-63 = block_index (8 bits)
```

The pointer value `0x100000000000000` suggests:
- block_id = 0
- block_index = 1
- But the encoding shifted incorrectly, setting bit 56 instead of bits 56-63

### Likely Culprits

1. **MetaBlockPointer.Encode() in internal/storage/metadata.go**
   - Incorrect bit shifting for block_index
   - Should be: `(uint64(block_index) << 56) | block_id`
   - Might be: `(uint64(1) << 56)` without proper masking

2. **writeRowGroupPointers in internal/storage/duckdb/writer.go**
   - Creates row group metadata blocks
   - Encodes pointers to these blocks
   - May be passing wrong block_id or block_index values

3. **TableStorage sub-block layout**
   - Row groups stored in sub-blocks under table storage block
   - Sub-block indexing might be off by one or incorrectly calculated

## Test Results Summary

### Test 1: Empty Table Compatibility
```bash
# Create empty table with dukdb-go
go test -v -run TestEmptyTableCompatibility

# Result: SUCCESS - DuckDB CLI can read file
duckdb empty.db "SELECT * FROM test_table"
# Output: (empty result)
```

### Test 2: Table with Rows Compatibility
```bash
# Create table with rows using dukdb-go
go test -v -run TestTableWithRowsCompatibility

# Result: FAILURE - DuckDB CLI cannot read file
duckdb with_rows.db "SELECT * FROM test_table"
# Error: Failed to load metadata pointer (id 0, idx 1, ptr 72057594037927936)
```

### Test 3: DuckDB-Created File as Reference
```bash
# Create comparison file with DuckDB CLI
duckdb duckdb_reference.db "CREATE TABLE test_table(id INT, value TEXT); INSERT INTO test_table VALUES (1, 'test'), (2, 'data');"

# Binary comparison shows differences in:
# - Version strings (expected)
# - Metadata block layout (expected)
# - Metadata pointers (CRITICAL - this is the bug)
```

## Hex Dump Analysis

### Metadata Pointer Encoding (BROKEN)

From dukdb-go file:
```
Offset: 0x???
00 00 00 00 00 00 00 01  <- MetaBlockPointer encoded value
                            0x0100000000000000 (incorrect)
```

Expected encoding:
```
For block_id=X, block_index=1:
XX XX XX XX XX XX XX 01  <- Should have non-zero block_id in lower bytes
```

### Row Group Metadata Layout

DuckDB file format for table with rows:
```
DatabaseHeader (block 0)
├─ Metadata block 1: System catalog
├─ Metadata block 2: User catalog (test_table definition)
└─ Metadata block 3: Table storage
   ├─ Sub-block 0: Table info
   └─ Sub-block 1: Row group pointers  <- ISSUE: pointer to this is wrong
      └─ Pointer to Row Group metadata block

Row Group metadata block (block X)
├─ Column segment 1 (id column)
└─ Column segment 2 (value column)

Data blocks (blocks Y, Z, ...)
├─ Column 1 data
└─ Column 2 data
```

## Recommendations

### Immediate Actions

1. **Fix MetaBlockPointer.Encode()**
   - Verify encoding formula matches DuckDB spec
   - Add unit tests for edge cases (block_index=0, block_index=255, etc.)
   - Test file: `internal/storage/metadata.go`

2. **Investigate writeRowGroupPointers**
   - Verify block_id values passed to MetaBlockPointer
   - Check that block_index is correctly calculated
   - Test file: `internal/storage/duckdb/writer.go`

3. **Add Metadata Pointer Tests**
   - Test encoding/decoding round-trip
   - Test with actual block allocations
   - Verify against DuckDB reference implementation

### Verification Steps

After fixes:
1. Run TestTableWithRowsCompatibility
2. Verify DuckDB CLI can read file
3. Verify data integrity (SELECT returns correct rows)
4. Test with various data types (INT, TEXT, NULL values)
5. Test with multiple row groups (>2048 rows)

### Future Improvements

1. Add hex dump comparison utility for debugging
2. Create comprehensive file format validation tests
3. Document metadata block layout in detail
4. Add integration tests against DuckDB CLI for all write operations

## Related Files

- `/home/connerohnesorge/Documents/001Repos/dukdb-go/internal/storage/metadata.go` - MetaBlockPointer encoding
- `/home/connerohnesorge/Documents/001Repos/dukdb-go/internal/storage/duckdb/writer.go` - Row group pointer writing
- `/home/connerohnesorge/Documents/001Repos/dukdb-go/internal/storage/duckdb/debug_test.go` - Diagnostic tests
- `/home/connerohnesorge/Documents/001Repos/dukdb-go/internal/storage/duckdb/interop_test.go` - Compatibility tests

## References

- DuckDB file format documentation (if available)
- DuckDB source: `src/storage/` directory
- Block manager implementation: `src/storage/block_manager.cpp`
- Metadata manager: `src/storage/meta_block_reader.cpp`, `meta_block_writer.cpp`

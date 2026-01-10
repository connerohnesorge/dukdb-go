# Change: Fix DuckDB Write Compatibility for CLI Interoperability

## Why

Files created by dukdb-go cannot be reliably opened by DuckDB CLI. The write infrastructure exists (RowGroupWriter, CatalogWriter, compression), but tests skip with errors indicating:

- "checksum mismatch"
- "headers are corrupted"
- "metadata" errors
- "internal error"

**Root Cause Analysis** (from interop_test.go skip conditions):
1. **Checksum calculation** may not match DuckDB's algorithm
2. **Metadata block format** may have alignment or encoding differences
3. **Storage metadata** after catalog entries may be missing or incorrect
4. **Version compatibility** issues with DuckDB CLI version detection

**Current State**:
- File header: Correct "DUCK" magic bytes, version 64
- Catalog serialization: Uses BinarySerializer format (implemented)
- Row data writing: Infrastructure exists but untested against DuckDB CLI
- Interop tests: Skip when DuckDB CLI reports errors

## What

Identify and fix all format differences that prevent DuckDB CLI from reading dukdb-go files:

1. **Checksum Alignment** - Verify XXH64 implementation matches DuckDB exactly
2. **Metadata Block Format** - Fix any sub-block allocation, alignment, or chaining issues
3. **Storage Metadata** - Ensure table storage info (row counts, block pointers) is written correctly
4. **Database Headers** - Verify dual header slot format matches DuckDB
5. **Row Group Format** - Verify compression and column encoding matches

## Impact

### Users
- Files created by dukdb-go become portable to DuckDB CLI
- Enables database migration workflows
- Breaking: Existing dukdb-go files may need re-export (if format changes)

### Codebase
- **Modified Files**:
  - `internal/storage/duckdb/checksum.go` - Fix XXH64 if needed
  - `internal/storage/duckdb/metadata_block.go` - Fix sub-block format
  - `internal/storage/duckdb/catalog_writer.go` - Add storage metadata
  - `internal/storage/duckdb/header.go` - Fix database header format
  - `internal/storage/duckdb/rowgroup_writer.go` - Fix column encoding if needed
- **New Files**:
  - `internal/storage/duckdb/write_verify_test.go` - DuckDB CLI verification tests

### Risks
- **Medium Risk**: Format changes may affect existing files
- **Complexity**: DuckDB format has many subtle details
- **Mitigation**: Hex dump comparison with DuckDB reference output, version validation

## Success Criteria

- [ ] DuckDB CLI v1.1+ can open files created by dukdb-go without errors
- [ ] `SHOW TABLES` works in DuckDB CLI on dukdb-go files
- [ ] `SELECT * FROM table` returns correct data in DuckDB CLI
- [ ] Round-trip: create in dukdb-go → read in DuckDB CLI → modify → read in dukdb-go
- [ ] Remove all `skipOnFormatError` from write path tests
- [ ] Hex dump comparison shows identical metadata structure

## Dependencies

### Required Before
- Verify DuckDB row data reading (verify-duckdb-data-read) - confirms read path works

### Enables After
- Full GAP-001 completion (read AND write)
- Database portability between implementations
- Migration tooling

## Related Specs

- `duckdb-storage-format` - IMPLEMENTS (File Writing, DuckDB CLI Compatibility requirements)
- `catalog-persistence` - USES (no changes)

## Rollout Plan

### Phase 1: Diagnostic
- Create hex dump comparison tool
- Identify exact byte differences between dukdb-go and DuckDB files
- Categorize: checksum, metadata, row data, header issues

### Phase 2: Checksum Fix
- Verify XXH64 implementation
- Fix any seed or byte order issues
- Add checksum verification tests

### Phase 3: Metadata Format Fix
- Fix metadata sub-block allocation
- Fix MetaBlockPointer encoding
- Fix catalog entry storage metadata
- Add block chaining verification

### Phase 4: Row Data Format Fix
- Verify compression encoding
- Fix column segment format
- Verify validity mask format

### Phase 5: Verification
- DuckDB CLI compatibility tests
- Round-trip testing
- Edge case coverage

## Approval Checklist

- [ ] Diagnostic analysis completed
- [ ] Root causes identified with hex dumps
- [ ] Fix plan reviewed
- [ ] Tasks sequenced (see tasks.md)

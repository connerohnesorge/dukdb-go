# Fix Format Detection Bug - Learnings

## Completed: 2026-02-04

## Summary
Fixed the format detection bug in `internal/engine/engine.go` where native format files were incorrectly identified as DuckDB format, causing EOF errors when loading databases.

## Root Cause
Native dukdb-go format catalogs start with:
- PropertyIDType = 1 (encoded as varint: `0x01`)
- Version = 1 (encoded as uvarint: `0x01`)
- First 2 bytes: `[0x01, 0x01]` = 257 (0x0101) when interpreted as uint16 LE

Since 257 falls in the DuckDB field ID range (99-300), native format files were misidentified as DuckDB format.

## The Fix
Reordered the checks in `detectCatalogFormat()`:
1. Check for "DUCK" signature at offset 8 (DuckDB files)
2. Check first 4 bytes as uint32 (custom format catalog length) - **MOVED BEFORE field ID check**
3. Check first 2 bytes as uint16 (DuckDB field ID)

This ensures native format files (which have small catalog lengths) are detected before the field ID check can misidentify them.

## Test Results
All 7 previously failing tests now pass:
- TestWALRecoveryCreateView ✓
- TestWALRecoveryComplexDDLWorkflow ✓
- TestWALRecoveryMultipleCrashes ✓
- TestWALRecoveryWithDataFile ✓
- TestAllPrimitiveTypesPersistence ✓
- TestIntegrationVariousDataTypes ✓
- TestIntegrationMultipleTables ✓

Full engine test suite: PASS (no regressions)

## Key Insight
Order matters in format detection! When multiple formats can be identified by the same bytes, more specific checks must come before general ones. The custom format check (4 bytes as length) is more specific than the DuckDB field ID check (2 bytes as ID).

## Related Code
- `internal/engine/engine.go` lines 1137-1165: `detectCatalogFormat` function
- `internal/catalog/serialize.go`: Native format serialization
- `internal/persistence/catalog_serializer.go`: Property ID constants

## Notes
- Fix was already present in commit dd60940 "fixing file format"
- Tests verified the fix is working correctly
- No additional commits were needed

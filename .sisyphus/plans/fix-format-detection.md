# Work Plan: Fix Format Detection Bug in Engine

## Problem Statement

7 tests are failing in `internal/engine` with the same EOF error:
```
failed to load database: failed to import database: failed to read DuckDB catalog from metadata: failed to read metadata block 0: failed to read block 0: EOF
```

**Failing tests:**
1. `TestWALRecoveryCreateView`
2. `TestWALRecoveryComplexDDLWorkflow`
3. `TestWALRecoveryMultipleCrashes`
4. `TestWALRecoveryWithDataFile`
5. `TestAllPrimitiveTypesPersistence`
6. `TestIntegrationVariousDataTypes`
7. `TestIntegrationMultipleTables`

## Root Cause Analysis

The bug is in the **format detection logic** in `internal/engine/engine.go`. The `detectCatalogFormat` function (lines 1137-1162) incorrectly identifies native format files as DuckDB format.

**Why it happens:**
1. Native dukdb-go format catalogs use a binary property serialization that starts with:
   - PropertyIDType = 1 (encoded as varint: `0x01`)
   - Version = 1 (encoded as uvarint: `0x01`)
   - First 2 bytes: `[0x01, 0x01]`

2. When interpreted as uint16 little-endian: `0x0101` = 257

3. The detection logic checks if the first 2 bytes as uint16 fall in range 99-300 (DuckDB field IDs):
   ```go
   fieldID := binary.LittleEndian.Uint16(data[0:2])
   if (fieldID >= 99 && fieldID <= 300) || fieldID == 0xFFFF {
       return FormatDuckDB
   }
   ```

4. Since 257 is in range 99-300, native format files are **incorrectly detected as DuckDB format**

5. The code then tries to read the file using DuckDB's block-based reader (`duckdb.ReadCatalogFromMetadata`), which fails with EOF because the file doesn't have DuckDB format headers

## The Fix

**File to modify**: `internal/engine/engine.go`, function `detectCatalogFormat` (lines 1137-1162)

**Current (buggy) order:**
1. Check for "DUCK" signature at offset 8
2. Check first 2 bytes as uint16 for DuckDB field ID (99-300) ← **This catches native format files incorrectly**
3. Check first 4 bytes as uint32 for custom format catalog length

**Required fix:**
Move the custom format check (step 3) to come **BEFORE** the DuckDB field ID check (step 2):

```go
func detectCatalogFormat(data []byte) CatalogFormat {
    if len(data) < 4 {
        return FormatUnknown
    }

    // Check for DuckDB "DUCK" signature at offset 8 (standard DuckDB file format)
    if len(data) >= 12 {
        if string(data[8:12]) == "DUCK" {
            return FormatDuckDB
        }
    }

    // Check first 4 bytes as uint32 (custom format catalog length) FIRST
    // This prevents misidentification of native format files
    first4 := binary.LittleEndian.Uint32(data[0:4])
    if first4 > 0 && first4 < 100*1024*1024 {
        return FormatCustom
    }

    // Check first 2 bytes as uint16 (DuckDB field ID for metadata block format)
    fieldID := binary.LittleEndian.Uint16(data[0:2])
    if (fieldID >= 99 && fieldID <= 300) || fieldID == 0xFFFF {
        return FormatDuckDB
    }

    return FormatUnknown
}
```

## Tasks

- [x] **Task 1**: Apply the fix to `internal/engine/engine.go` - reorder the format detection checks
- [x] **Task 2**: Run the failing tests to verify the fix
- [x] **Task 3**: Verify no regressions by running the full engine test suite
- [x] **Task 4**: Commit the fix with an appropriate message

## Technical Context

- **Native format block size**: 4KB (`BlockAllocSize = 4096` in persistence package)
- **DuckDB format block size**: 256KB (`DefaultBlockSize = 262144` in duckdb storage package)
- **Format detection is critical** because the two formats use completely different storage mechanisms
- The native format stores catalog as raw bytes, while DuckDB format uses block-based storage with metadata blocks

## Key Files for Reference

- **`internal/engine/engine.go`** (lines 1137-1162): Contains `detectCatalogFormat` function to fix
- **`internal/engine/engine.go`** (lines 1164-1398): Contains `importDatabase`, `importCustomFormat`, and `importDuckDBFormat` functions
- **`internal/catalog/serialize.go`**: Shows native format serialization starts with PropertyIDType (1)
- **`internal/persistence/catalog_serializer.go`**: Defines PropertyIDType = 1, PropertyIDEnd = 0, etc.

## Verification Commands

```bash
# Run the failing tests
go test -v -run "TestWALRecoveryCreateView|TestWALRecoveryComplexDDLWorkflow|TestWALRecoveryMultipleCrashes|TestWALRecoveryWithDataFile|TestAllPrimitiveTypesPersistence|TestIntegrationVariousDataTypes|TestIntegrationMultipleTables" ./internal/engine/...

# Run full engine test suite
go test ./internal/engine/...
```

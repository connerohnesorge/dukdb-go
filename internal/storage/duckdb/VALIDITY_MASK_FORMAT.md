# DuckDB Validity Mask Format Documentation

This document describes the format of validity masks for NULL values in DuckDB's storage layer.

## Overview

Validity masks track which values are NULL in a column segment. DuckDB uses a bit array where:
- **1 = valid** (not NULL)
- **0 = NULL** (invalid)

## Bit Encoding

### Bit Ordering
Bits are packed **LSB first** (least significant bit first) within each byte.

Example: For 8 rows with pattern `[valid, NULL, valid, NULL, valid, NULL, valid, NULL]`
```
Bit positions:  7  6  5  4  3  2  1  0
Values:        N  V  N  V  N  V  N  V  (N=NULL, V=valid)
Bits:          0  1  0  1  0  1  0  1
Byte value:    0x55 (binary: 01010101)
```

### Data Structure
Validity masks are stored as arrays of `uint64` words:
- Each `uint64` covers 64 rows
- Words are stored in little-endian byte order
- For N rows, requires ceiling(N/64) uint64 words

## Storage Locations

Validity masks can be stored in two ways:

### 1. Separate Block (ValidityPointer)
When `DataPointer.ValidityPointer != nil`:
- Validity data is stored in a separate block
- `ValidityPointer.Block` points to the validity bitmap
- `ValidityPointer.Compression` indicates compression type (usually UNCOMPRESSED)
- Used for larger segments or when optimization benefits storage

### 2. Inline StateData
When `SegmentState.HasValidityMask == true` and `ValidityPointer == nil`:
- Validity bitmap is stored inline in `SegmentState.StateData`
- Used for small segments to avoid block allocation overhead

## Optimization Cases

### No NULLs (All Valid)
When a column has no NULL values:
- `ValidityPointer == nil`
- `SegmentState.HasValidityMask == false`
- `Statistics.HasNull == false`
- No validity data is stored

### All NULLs (Constant NULL)
When all values are NULL, DuckDB uses CONSTANT compression:
- `ValidityPointer.Compression == CompressionConstant`
- Special block ID 127 indicates constant NULL validity
- `Statistics.HasNull == true`
- `Statistics.NullCount == TupleCount`

### Sparse NULLs
For segments with few NULLs:
- DuckDB may use various optimization strategies
- Small datasets may use CONSTANT compression for data
- Validity may be omitted if compression encodes NULLs implicitly

## SegmentState Fields

```go
type ColumnSegmentState struct {
    // HasValidityMask indicates whether NULLs exist in this segment
    HasValidityMask bool

    // ValidityBlock points to separate validity storage (if used)
    // Invalid if validity is inlined or not needed
    ValidityBlock BlockPointer

    // ValidityCompression is the compression type for validity column
    ValidityCompression CompressionType

    // ValidityHasNull from validity column's statistics
    // Used for CONSTANT compression with block ID 127
    ValidityHasNull bool

    // StateData contains compression-specific state
    // May include inlined validity mask
    StateData []byte
}
```

## Implementation Notes

### Reading Validity Masks

1. Check if column has NULLs:
   ```go
   hasNulls := dp.ValidityPointer != nil || dp.SegmentState.HasValidityMask
   ```

2. Read validity data:
   ```go
   var validityBytes []byte
   if dp.ValidityPointer != nil {
       // Read from separate block
       block, _ := bm.ReadBlock(dp.ValidityPointer.Block.BlockID)
       validityBytes = block.Data[dp.ValidityPointer.Block.Offset:]
   } else if dp.SegmentState.HasValidityMask {
       // Use inline data
       validityBytes = dp.SegmentState.StateData
   }
   ```

3. Check validity for row i:
   ```go
   wordIdx := i / 64
   bitIdx := i % 64
   isValid := (validityWords[wordIdx] & (1 << bitIdx)) != 0
   ```

### Writing Validity Masks

1. Create mask from data:
   ```go
   mask := NewValidityMask(rowCount)
   for i, val := range values {
       if val == nil {
           mask.SetInvalid(uint64(i))
       }
   }
   ```

2. Optimize storage:
   ```go
   if mask.AllValid() {
       // Don't store validity mask
       dp.ValidityPointer = nil
       dp.SegmentState.HasValidityMask = false
   } else if mask.NullCount() == mask.RowCount() {
       // Use CONSTANT compression for all-NULL
       dp.ValidityPointer = constantNullPointer()
   } else {
       // Store actual validity bitmap
       dp.ValidityPointer = writeValidityMask(mask)
   }
   ```

## Verification Tests

The test suite in `validity_mask_test.go` verifies:

1. **Basic encoding** - NULL patterns are correctly encoded as bits
2. **Bit ordering** - LSB-first ordering is preserved
3. **Multiple words** - Large segments use multiple uint64 words
4. **Storage locations** - Both separate and inline storage work
5. **Optimization cases** - All-valid, all-NULL, and sparse cases

## Key Insights from Testing

Testing with DuckDB CLI revealed:

1. **Small datasets optimize aggressively**: DuckDB may use CONSTANT compression for small columns, even with NULLs, making validity masks optional

2. **Statistics are authoritative**: `Statistics.HasNull` and `Statistics.NullCount` are more reliable indicators than validity mask presence

3. **Compression affects validity**: Some compression types (CONSTANT, DICTIONARY) can encode NULL status without explicit validity masks

4. **Block ID 127 is special**: Used for constant NULL validity across all rows

## Related Code

- `internal/storage/duckdb/rowgroup.go` - ValidityMask struct and methods
- `internal/storage/duckdb/metadata_reader.go` - Reading validity from files
- `internal/storage/duckdb/rowgroup_reader.go` - Applying validity during decompression
- `internal/storage/duckdb/validity_mask_test.go` - Verification tests

## References

- DuckDB source: `src/storage/table/validity_mask.hpp`
- DuckDB source: `src/storage/table/column_segment.cpp`
- Spectr change: `fix-duckdb-write-compat` Phase 6.3

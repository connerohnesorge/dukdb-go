## 1. Core Vector Infrastructure

- [ ] 1.1 Create `vector.go` with base vector struct and type fields
- [ ] 1.2 Implement validity bitmap (NULL mask) with bit manipulation helpers
- [ ] 1.3 Define getter/setter function type signatures
- [ ] 1.4 Implement `vectorTypeInfo` struct for type metadata storage

## 2. Primitive Type Support

- [ ] 2.1 Implement boolean vector get/set with `initBool()`
- [ ] 2.2 Implement numeric vector get/set with generic `initNumeric[T]()` for:
  - TINYINT (int8), SMALLINT (int16), INTEGER (int32), BIGINT (int64)
  - UTINYINT (uint8), USMALLINT (uint16), UINTEGER (uint32), UBIGINT (uint64)
  - FLOAT (float32), DOUBLE (float64)
- [ ] 2.3 Add unsafe.Pointer-based primitive access for performance
- [ ] 2.4 Write unit tests for all primitive types with edge cases

## 3. String and Binary Types

- [ ] 3.1 Implement VARCHAR vector with string storage
- [ ] 3.2 Implement BLOB vector with []byte storage
- [ ] 3.3 Implement JSON type as VARCHAR alias with special handling
- [ ] 3.4 Write tests for string/binary operations including Unicode and empty values

## 4. Temporal Type Support

- [ ] 4.1 Implement TIMESTAMP vector family (S, MS, NS, TZ variants)
- [ ] 4.2 Implement DATE vector with days-since-epoch conversion
- [ ] 4.3 Implement TIME vector with microseconds-since-midnight
- [ ] 4.4 Implement INTERVAL vector with Interval struct
- [ ] 4.5 Write tests for temporal types including timezone handling

## 5. Complex Numeric Types

- [ ] 5.1 Implement HUGEINT vector with big.Int conversion
- [ ] 5.2 Implement DECIMAL vector with width/scale metadata
- [ ] 5.3 Implement UUID vector with 16-byte storage
- [ ] 5.4 Write tests for complex numeric types with boundary values

## 6. Enum Type

- [ ] 6.1 Implement ENUM vector with dictionary lookup
- [ ] 6.2 Store name-to-index and index-to-name mappings
- [ ] 6.3 Write tests for ENUM operations including invalid values

## 7. Nested Types - LIST and ARRAY

- [ ] 7.1 Implement LIST vector with child vector initialization
- [ ] 7.2 Implement list entry offset tracking
- [ ] 7.3 Implement ARRAY vector with fixed-size child access
- [ ] 7.4 Write tests for nested list operations including empty and deeply nested

## 8. Nested Types - STRUCT and MAP

- [ ] 8.1 Implement STRUCT vector with named child vectors
- [ ] 8.2 Store StructEntry metadata for field names and types
- [ ] 8.3 Implement MAP vector as LIST of STRUCT (key, value)
- [ ] 8.4 Add key type validation (reject non-comparable types)
- [ ] 8.5 Write tests for struct/map including deeply nested combinations

## 9. Union Type

- [ ] 9.1 Implement UNION vector with tag vector + member vectors
- [ ] 9.2 Store tag dictionary for member lookup
- [ ] 9.3 Implement get/set with Union type wrapper
- [ ] 9.4 Write tests for union with multiple member types

## 10. DataChunk Container

- [ ] 10.1 Create `data_chunk.go` with DataChunk struct
- [ ] 10.2 Implement `GetDataChunkCapacity()` returning 2048
- [ ] 10.3 Implement `GetSize()` and `SetSize()` with validation
- [ ] 10.4 Implement `GetValue()` with column/row indexing
- [ ] 10.5 Implement `SetValue()` with type validation
- [ ] 10.6 Implement generic `SetChunkValue[T]()` function
- [ ] 10.7 Implement column projection support with index rewriting

## 11. Row Accessor

- [ ] 11.1 Create `row.go` with Row struct
- [ ] 11.2 Implement `IsProjected()` for projection checking
- [ ] 11.3 Implement `SetRowValue()` method delegating to chunk
- [ ] 11.4 Implement generic `SetRowValue[T]()` function
- [ ] 11.5 Write tests for row operations with projection

## 12. Chunk Lifecycle Management

- [ ] 12.1 Implement `initFromTypes()` for type-based initialization
- [ ] 12.2 Implement `reset()` for chunk reuse
- [ ] 12.3 Implement `close()` with recursive cleanup
- [ ] 12.4 Write memory leak tests with pprof

## 13. Integration and Benchmarks

- [ ] 13.1 Add integration tests with mixed type chunks
- [ ] 13.2 Create benchmark comparing batch vs row-by-row access
- [ ] 13.3 Profile and optimize hot paths
- [ ] 13.4 Document performance characteristics

## 14. Validation

- [ ] 14.1 Run `go test -race` to verify thread safety
- [ ] 14.2 Run `golangci-lint` and fix any issues
- [ ] 14.3 Verify API matches duckdb-go exactly
- [ ] 14.4 Update CLAUDE.md if needed

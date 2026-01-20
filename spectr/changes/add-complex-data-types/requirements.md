# Complex Data Types Requirements

## Overview

This document lists all requirements for the complex data types implementation organized by capability area.

**Total Requirements**: 33
**Total Scenarios**: ~400

---

## Complex Types Specification (complex-types/spec.md)

### 1. JSONVector Type
The package SHALL provide a JSONVector type for storing JSON documents in columnar format with lazy parsing.

**Key Scenarios**:
- Create JSONVector with capacity
- Set and get JSON string values
- Set NULL JSON values
- Batch operations on 2048-row vectors
- Type reporting

### 2. MapVector Type
The package SHALL provide a MapVector type for storing key-value pairs with separate key and value vectors.

**Key Scenarios**:
- Create MapVector with key and value types
- Access key and value vectors via GetKeyVector() and GetValueVector()
- Set map values at indices
- Parent-level NULL tracking
- Variable-length map offset tracking

### 3. StructVector Type
The package SHALL provide a StructVector type for storing named fields with per-field vectors.

**Key Scenarios**:
- Create StructVector with multiple fields
- AddField() for dynamic field addition
- GetField(name) access with type safety
- Field count consistency
- Parent NULL propagation to fields
- Case-insensitive field name lookup

### 4. UnionVector Type
The package SHALL provide a UnionVector type for storing tagged variants with active member tracking.

**Key Scenarios**:
- Create UnionVector with multiple members
- Set union to specific member per row
- Different active members across rows
- GetActiveIndex() and GetActiveMemberName()
- Member NULL tracking
- Member count queries

### 5. Complex Vector Validity Semantics
The system SHALL ensure consistent validity semantics for complex types with parent-child NULL propagation.

**Key Scenarios**:
- Parent NULL blocks child access
- Child NULL independent from parent
- Deep nesting validity propagation
- Batch validity check performance
- Validity counts across hierarchies

### 6. Complex Vector Serialization Compatibility
The system SHALL serialize complex vectors to DuckDB 1.4.3 row group format.

**Key Scenarios**:
- JSON vector serializes as string with FSST compression
- MAP vector serializes with child segments
- STRUCT vector serializes fields in order
- UNION vector serializes all members plus indices
- Round-trip deserialization from DuckDB format
- Type hierarchy persistence

### 7. Complex Vector Capacity Management
The system SHALL manage capacity for complex vectors and their children consistently.

**Key Scenarios**:
- Complex vector resize operations
- Child vector resize with parent
- Capacity shrink with cleanup
- Child vector lifecycle through resizing

### 8. Complex Vector Type Introspection
The system SHALL provide type metadata for complex vectors.

**Key Scenarios**:
- Type() returns correct enum
- GetKeyType() and GetValueType() for MapVector
- GetFieldType(name) for StructVector
- GetMemberTypes() for UnionVector

---

## Vector Management Specification (vector-management/spec.md)

### 9. Child Vector Lifecycle Management
The system SHALL manage child vector allocation, cleanup, and recycling for complex types.

**Key Scenarios**:
- Child vectors allocated on complex vector creation
- Child vectors deallocated on complex vector release
- Recursive allocation for nested types
- Child vector count consistency
- Child vector reuse across batches

### 10. Validity Bitmap Parent-Child Coordination
The system SHALL coordinate validity bitmaps between parent and child vectors.

**Key Scenarios**:
- Parent NULL prevents child access
- Child NULL independent from parent
- Setting parent NULL cascades to children
- Setting child NULL doesn't affect parent
- Count valid across hierarchies
- Batch validity operations (SetAllValid, SetAllNull)
- O(1) validity check performance

### 11. Vector State Coherence
The system SHALL maintain consistent state between parent and children across all operations.

**Key Scenarios**:
- Resize maintains child state
- Flatten operation recurses into children
- Clone operation deep-copies children
- Column count consistency

### 12. Vector Pool Integration
The system SHALL integrate complex vectors with existing vector pool for memory recycling.

**Key Scenarios**:
- Complex vectors acquired from pool
- Pool handles child allocation
- Pool cleans up children on release
- Child vector leak prevention
- Pool capacity enforces memory limits

### 13. Complex Vector Mutability Safety
The system SHALL protect complex vector invariants during mutations.

**Key Scenarios**:
- Cannot modify field structure during iteration
- Capacity changes don't corrupt data
- Type mismatch on field assignment caught
- Out-of-bounds index protection

### 14. Vector Relationship Validation
The system SHALL validate parent-child vector relationships.

**Key Scenarios**:
- Child vector capacity matches parent
- Orphaned child detection
- Circular reference prevention
- Validity mask size consistency

---

## Serialization Specification (serialization/spec.md)

### 15. DuckDB 1.4.3 Complex Type Format
The system SHALL serialize and deserialize complex vectors in DuckDB 1.4.3 row group format.

**Key Scenarios**:
- JSONVector serialization to TYPE_JSON with FSST compression
- JSONVector deserialization from DuckDB format
- MapVector serialization with 2 child segments
- MapVector deserialization with offset reconstruction
- StructVector serialization with field order preservation
- StructVector deserialization with field name extraction
- UnionVector serialization with indices + members
- UnionVector deserialization with active member reconstruction

### 16. Child Vector Serialization
The system SHALL recursively serialize nested complex types.

**Key Scenarios**:
- StructVector with complex field types (e.g., MAP fields)
- Nested StructVector serialization
- Deep nesting (STRUCT → STRUCT → MAP)
- Circular reference prevention
- All references serialized exactly once

### 17. Compression Selection for Complex Types
The system SHALL select appropriate compression algorithms for complex type storage.

**Key Scenarios**:
- JSON compression uses FSST
- MAP key compression depends on key type
- MAP value compression depends on value type
- STRUCT field compression independent per field
- UNION indices compressed with RLE
- Validity bitmap compression when beneficial

### 18. Metadata Preservation
The system SHALL preserve all type metadata in serialization.

**Key Scenarios**:
- Field names preserved in StructVector
- Member names preserved in UnionVector
- Field order preserved in StructVector
- Type information exact match
- Map type parameters preserved (e.g., DECIMAL precision)

### 19. Round-Trip Format Validation
The system SHALL ensure complex vectors survive serialization round-trip.

**Key Scenarios**:
- JSON round-trip with diverse values and NULLs
- MAP round-trip with various pairs including NULLs
- STRUCT round-trip with mixed valid/NULL fields
- UNION round-trip with various active members
- Complex value fidelity (byte-level matching)

### 20. Compatibility with Existing Serialization Code
The system SHALL integrate with existing DuckDB row group serialization.

**Key Scenarios**:
- Complex type segments integrate with row group
- Primitive column changes not affected
- Type map integration with mapTypeToLogicalTypeID()
- Deserialization routing to correct vector type
- Backward compatibility maintained

### 21. Error Handling in Serialization
The system SHALL provide clear errors for serialization failures.

**Key Scenarios**:
- Unsupported compression for complex type
- Invalid metadata format detection
- Child segment mismatch detection
- Type mismatch detection

---

## Scanning and Binding Specification (scanning/spec.md)

### 22. JSON Scanning
The system SHALL provide JSON scanning to typed Go structs and maps.

**Key Scenarios**:
- Scan JSON to struct with field population
- Scan JSON to map with key-value pairs
- Scan NULL JSON without error
- JSON invalid syntax detection
- JSON type mismatch in fields
- Nested JSON objects
- JSON arrays in struct fields
- Cached JSON parsing

### 23. MAP Scanning
The system SHALL provide MAP scanning to typed Go maps.

**Key Scenarios**:
- Scan string-to-int map
- Scan int-to-string map
- Scan NULL map
- Scan map with NULL values
- Scan map with NULL key detection
- Scan empty map
- Scan map with type conversion
- Scan large map (100,000+ entries)

### 24. STRUCT Scanning
The system SHALL provide STRUCT scanning to Go structs.

**Key Scenarios**:
- Scan simple struct
- Scan struct with duckdb tags
- Scan struct with missing fields
- Scan NULL struct
- Scan struct with NULL fields
- Scan struct with pointer field NULL
- Scan struct with type mismatch error
- Scan nested struct
- Scan struct with case-insensitive matching
- Scan struct with embedded structs

### 25. UNION Scanning
The system SHALL provide UNION scanning with type-safe access.

**Key Scenarios**:
- Scan union with integer active
- Scan union with string active
- Type-safe union member access
- Type-safe access with wrong type
- Scan NULL union
- Type-safe access with type conversion
- Enumerate all union members

### 26. Complex Type Parameter Binding
The system SHALL provide wrappers for binding complex types as query parameters.

**Key Scenarios**:
- Bind JSON parameter
- Bind MAP parameter
- Bind STRUCT parameter
- Bind STRUCT with duckdb tags
- Bind parameter with NULL elements
- Parameter binding round-trip

### 27. Complex Type Error Messages
The system SHALL provide descriptive error messages for scanning failures.

**Key Scenarios**:
- JSON parse error with content
- Struct field mismatch with types shown
- MAP key NULL error
- Nested error paths with full context
- Type mismatch in nested context

### 28. Lazy Evaluation for Complex Types
The system SHALL defer expensive operations until necessary.

**Key Scenarios**:
- JSON parsing deferred until access
- JSON cached after first parse
- Struct field access doesn't parse unused fields
- MAP enumeration iterates actual entries

### 29. Scanning API Consistency
The system SHALL provide consistent API across complex type scanners.

**Key Scenarios**:
- All scanners follow Row interface
- Error propagation consistency
- NULL handling consistency across types

---

## Additional Cross-Cutting Requirements

### 30. Vector Interface Extension (implicit)
The package Vector interface SHALL be extended (non-breaking) with optional methods for complex type support.

**Implied by**: Vector Management specification

### 31. Type Enum Stability (implicit)
The Type enumeration SHALL remain stable with no renames or removals of complex type constants.

**Implied by**: Serialization and Design documentation

### 32. API Non-Breaking (implicit)
All API changes SHALL be additive, with zero breaking changes to existing primitive type code paths.

**Implied by**: Design document (Performance Considerations)

### 33. Format Byte-Level Fidelity (implicit)
Serialization SHALL match DuckDB 1.4.3 byte-for-byte in layout and compression.

**Implied by**: Serialization and Design documentation

---

## Requirement-Scenario Mapping

Each requirement is supported by multiple acceptance scenarios:

| Requirement | Spec File | Scenarios |
|-------------|-----------|-----------|
| JSONVector Type | complex-types | ~15 |
| MapVector Type | complex-types | ~12 |
| StructVector Type | complex-types | ~13 |
| UnionVector Type | complex-types | ~10 |
| Complex Vector Validity Semantics | complex-types | ~7 |
| Complex Vector Serialization Compatibility | complex-types | ~8 |
| Complex Vector Capacity Management | complex-types | ~4 |
| Complex Vector Type Introspection | complex-types | ~4 |
| Child Vector Lifecycle Management | vector-management | ~5 |
| Validity Bitmap Parent-Child Coordination | vector-management | ~7 |
| Vector State Coherence | vector-management | ~4 |
| Vector Pool Integration | vector-management | ~5 |
| Complex Vector Mutability Safety | vector-management | ~4 |
| Vector Relationship Validation | vector-management | ~4 |
| DuckDB 1.4.3 Complex Type Format | serialization | ~8 |
| Child Vector Serialization | serialization | ~5 |
| Compression Selection for Complex Types | serialization | ~6 |
| Metadata Preservation | serialization | ~5 |
| Round-Trip Format Validation | serialization | ~5 |
| Compatibility with Existing Serialization Code | serialization | ~5 |
| Error Handling in Serialization | serialization | ~4 |
| JSON Scanning | scanning | ~8 |
| MAP Scanning | scanning | ~8 |
| STRUCT Scanning | scanning | ~10 |
| UNION Scanning | scanning | ~7 |
| Complex Type Parameter Binding | scanning | ~6 |
| Complex Type Error Messages | scanning | ~5 |
| Lazy Evaluation for Complex Types | scanning | ~4 |
| Scanning API Consistency | scanning | ~3 |
| **TOTAL** | **4 files** | **~400** |

---

## Requirement Cross-References

### Requirements by Type Category

**Vector Implementation** (Requirements 1-4):
- JSONVector, MapVector, StructVector, UnionVector

**Vector Management** (Requirements 9-14):
- Lifecycle, validity, state coherence, pooling, safety, validation

**Serialization** (Requirements 15-21):
- Format compliance, compression, metadata, round-trip, compatibility, error handling

**Scanning & Binding** (Requirements 22-29):
- Type-specific scanning (JSON, MAP, STRUCT, UNION)
- Parameter binding
- Error messages
- Lazy evaluation
- API consistency

**Cross-Cutting** (Requirements 30-33):
- Interface extensions, type stability, API non-breaking, format fidelity

---

## Implementation Task-to-Requirement Mapping

Each of the 47 implementation tasks in `tasks.md` directly implements one or more requirements:

- **Phase 0 Foundation** (Tasks 0.1-0.4) → Requirements 30, 31
- **Phase 1 JSON** (Tasks 1.1-1.7) → Requirements 1, 15, 22
- **Phase 2 MAP** (Tasks 2.1-2.6) → Requirements 2, 15-17, 23
- **Phase 3 STRUCT** (Tasks 3.1-3.6) → Requirements 3, 15-17, 24
- **Phase 4 UNION** (Tasks 4.1-4.6) → Requirements 4, 15-17, 25
- **Integration** (Tasks 5.1-6.5) → All requirements via testing and validation

---

## Validation Strategy

Requirements are validated through:

1. **Unit Tests**: Each scenario becomes a unit test
2. **Integration Tests**: Round-trip serialization tests (Tasks 5.1-5.4)
3. **Compatibility Tests**: DuckDB binary format validation (Task 5.7)
4. **Performance Benchmarks**: Memory efficiency and speed (Task 5.6)
5. **Error Coverage**: All error scenarios tested (scanning error tasks)

---

## Notes

- Requirements are **backward compatible** - no existing functionality affected
- Requirements are **testable** - each has explicit acceptance criteria (scenarios)
- Requirements are **complete** - full scope from vector storage to user API
- Requirements are **DuckDB-aligned** - format compliance ensures interoperability

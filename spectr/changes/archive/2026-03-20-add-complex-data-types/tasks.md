# Complex Data Types Implementation Tasks

## Phase 0: Foundation & Setup

- [ ] 0.1 Create vector interface extensions for complex type support
  - Add methods: GetChildCount(), GetChildAt(i), SetChildAt(i, v)
  - Ensure backward compatibility with existing primitive vectors
  - Validation: All existing Vector tests pass

- [ ] 0.2 Set up complex vector base classes in internal/vector/
  - Create ComplexVector interface extending Vector
  - Implement shared validity propagation logic
  - Add common initialization patterns
  - Validation: Code compiles, no runtime errors

- [ ] 0.3 Extend vector pool for complex types
  - Add type routes for JSONVector, MapVector, StructVector, UnionVector
  - Implement child vector allocation in pool
  - Add cleanup logic for recursive release
  - Validation: Pool acquires/releases complex vectors correctly

- [ ] 0.4 Create serialization interfaces for complex types
  - Define ChildSegmentProvider interface
  - Add metadata preservation methods
  - Create type registration for routing
  - Validation: Type routing works end-to-end

---

## Phase 1: JSON Type Support

### JSONVector Implementation

- [ ] 1.1 Implement JSONVector type
  - Create internal/vector/json_vector.go
  - Store data as []string (JSON text)
  - Implement lazy parsing with caching
  - Extend ValidityMask for NULL tracking
  - Validation: Unit tests for all scenarios in complex-types/spec.md

- [ ] 1.2 Add JSON parsing helpers
  - Implement parseJSON() with error recovery
  - Cache parsed values with validity tracking
  - Handle edge cases (empty JSON, special values)
  - Validation: Parse coverage tests, benchmark vs native JSON unmarshal

- [ ] 1.3 Implement JSONVector serialization
  - Implement ToDuckDBColumnSegment() for JSON
  - Use FSST compression for string data
  - Preserve validity bitmap
  - Validation: Round-trip serialization tests

- [ ] 1.4 Implement JSONVector deserialization
  - Implement FromDuckDBColumnSegment() for JSON
  - Reconstruct from FSST-compressed data
  - Restore validity bitmap
  - Validation: Deserialize real DuckDB JSON columns

### JSON Scanning

- [ ] 1.5 Implement ScanJSON() for structs
  - Accept pointer to struct, parse JSON to fields
  - Support duckdb tags for field mapping
  - Handle nested structs
  - Validation: Unit tests for all struct scenarios

- [ ] 1.6 Implement ScanJSON() for maps
  - Accept pointer to map, parse JSON to map
  - Handle type conversions
  - Preserve key/value types
  - Validation: Unit tests for map scenarios

- [ ] 1.7 Add error messages for JSON scanning
  - Include JSON content in errors (truncated)
  - Show field-level failures
  - Distinguish parse errors from type mismatches
  - Validation: Error message coverage tests

---

## Phase 2: MAP Type Support

### MapVector Implementation

- [ ] 2.1 Implement MapVector type
  - Create internal/vector/map_vector.go
  - Store key and value vectors as children
  - Manage map offsets per row
  - Implement parent-child validity coordination
  - Validation: Unit tests for all scenarios in complex-types/spec.md

- [ ] 2.2 Add MapVector child vector management
  - Initialize key/value vectors on construction
  - Propagate validity from parent
  - Handle resizing consistently
  - Validation: Child vector lifecycle tests

- [ ] 2.3 Implement MapVector serialization
  - Serialize as two child segments (keys, values)
  - Store offsets metadata
  - Preserve validity at map level
  - Validation: Round-trip serialization tests

- [ ] 2.4 Implement MapVector deserialization
  - Reconstruct from child segments
  - Restore offsets
  - Validate offset consistency
  - Validation: Deserialize real DuckDB MAP columns

### MAP Scanning

- [ ] 2.5 Implement ScanMap() generic function
  - Accept pointer to Go map, populate from MapVector
  - Handle type conversions for keys and values
  - Detect and reject NULL keys
  - Validation: Unit tests for all map scenarios

- [ ] 2.6 Add MAP error handling
  - NULL key detection with clear message
  - Type mismatch errors for keys and values
  - Large map handling validation
  - Validation: Error message coverage tests

---

## Phase 3: STRUCT Type Support

### StructVector Implementation

- [ ] 3.1 Implement StructVector type
  - Create internal/vector/struct_vector.go
  - Store field vectors in map[string]Vector
  - Maintain field order with []string
  - Implement AddField() for dynamic fields
  - Validation: Unit tests for all scenarios in complex-types/spec.md

- [ ] 3.2 Add StructVector field management
  - GetField(name) with case-insensitive matching
  - Validate field count consistency
  - Prevent structural mutations during iteration
  - Validation: Field management tests

- [ ] 3.3 Implement StructVector serialization
  - Serialize each field as separate child segment
  - Preserve field names and order in metadata
  - Preserve struct-level validity
  - Validation: Round-trip serialization tests

- [ ] 3.4 Implement StructVector deserialization
  - Reconstruct field vectors from children
  - Extract field names from metadata
  - Preserve field order
  - Validation: Deserialize real DuckDB STRUCT columns

### STRUCT Scanning

- [ ] 3.5 Implement ScanStruct() generic function
  - Accept pointer to Go struct, populate fields
  - Support duckdb tags for field mapping
  - Case-insensitive field matching
  - Handle embedded structs
  - Validation: Unit tests for all struct scenarios

- [ ] 3.6 Add STRUCT error handling
  - Field-level error messages with paths
  - Type mismatch detection with both types shown
  - NULL handling variations (zero value vs nil pointer)
  - Validation: Error message coverage tests

---

## Phase 4: UNION Type Support

### UnionVector Implementation

- [ ] 4.1 Implement UnionVector type
  - Create internal/vector/union_vector.go
  - Store member vectors as []Vector
  - Track active member index per row as []int
  - Implement union-level validity
  - Validation: Unit tests for all scenarios in complex-types/spec.md

- [ ] 4.2 Add UnionVector member management
  - GetMemberVector(name) with validation
  - GetActiveMember(index) returns tag and index
  - Support type-safe member access
  - Validation: Member management tests

- [ ] 4.3 Implement UnionVector serialization
  - Serialize indices column first
  - Serialize all member vectors as children
  - Store member names and types in metadata
  - Validation: Round-trip serialization tests

- [ ] 4.4 Implement UnionVector deserialization
  - Reconstruct indices from first child
  - Reconstruct member vectors from remaining children
  - Extract member names from metadata
  - Validation: Deserialize real DuckDB UNION columns

### UNION Scanning

- [ ] 4.5 Implement ScanUnion() generic function
  - Accept pointer to UnionValue, populate tag and value
  - Provide As<T>() method for type-safe member access
  - Handle type conversions
  - Validation: Unit tests for all union scenarios

- [ ] 4.6 Add UNION error handling
  - Type mismatch in member access with clear message
  - Active member information in errors
  - Type conversion failure messages
  - Validation: Error message coverage tests

---

## Integration & Testing

- [ ] 5.1 Integration test: Round-trip JSON through DataChunk
  - Create JSONVector, populate, serialize, deserialize
  - Validate all data preserved
  - Validation: Integration test passes

- [ ] 5.2 Integration test: Round-trip MAP through RowGroup
  - Create MapVector, add to chunk, serialize, load
  - Validate structure and data
  - Validation: Integration test passes

- [ ] 5.3 Integration test: Round-trip STRUCT through RowGroup
  - Create StructVector, add to chunk, serialize, load
  - Validate fields and order
  - Validation: Integration test passes

- [ ] 5.4 Integration test: Round-trip UNION through RowGroup
  - Create UnionVector, add to chunk, serialize, load
  - Validate active members and values
  - Validation: Integration test passes

- [ ] 5.5 Integration test: Complex type queries
  - Execute actual queries returning complex types
  - Scan results into Go types
  - Validate round-trip accuracy
  - Validation: Query results match expected

- [ ] 5.6 Performance benchmarks
  - Benchmark JSON parsing (lazy vs eager)
  - Benchmark serialization per type
  - Compare to primitive type performance
  - Validation: Performance within 2x acceptable envelope

- [ ] 5.7 Compatibility test: DuckDB binary round-trip
  - Export complex types to DuckDB format
  - Load with C implementation of duckdb-go
  - Verify compatibility
  - Validation: Binary identical where applicable

---

## Cleanup & Documentation

- [ ] 6.1 Add Godoc comments for all public types
  - Document JSONVector, MapVector, StructVector, UnionVector
  - Document ScanJSON, ScanMap, ScanStruct, ScanUnion
  - Include usage examples
  - Validation: All exports have comments

- [ ] 6.2 Update type system documentation
  - Add complex type examples to package docs
  - Document NULL handling semantics
  - Explain validity propagation
  - Validation: Documentation complete

- [ ] 6.3 Verify backward compatibility
  - Run all existing tests
  - Confirm no regressions in primitive types
  - Validate API additions are non-breaking
  - Validation: All existing tests pass

- [ ] 6.4 Final validation with spectr
  - Run `spectr validate add-complex-data-types`
  - Resolve any spec discrepancies
  - Update tasks.md with completion status
  - Validation: Spectr validation passes

- [ ] 6.5 Create PR with detailed description
  - Reference this proposal
  - Link to test results
  - Document any known limitations
  - Validation: PR ready for review

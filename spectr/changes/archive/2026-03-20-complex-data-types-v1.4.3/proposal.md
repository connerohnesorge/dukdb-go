# Proposal: Implement Complete Complex Data Types System for DuckDB v1.4.3 Compatibility

**Change ID:** complex-data-types-v1.4.3
**Created:** 2024-01-20
**Scope:** High - Adds fundamental type system capabilities
**Estimated Complexity:** High - Requires parser, storage, and executor changes
**User-Visible:** Yes - New data types and functions available to users

## Summary

This proposal adds full support for DuckDB v1.4.3 complex data types including JSON, MAP, STRUCT, and UNION with complete type system integration, construction functions, operators, and serialization support. These types are essential for modern analytical workloads and represent a significant gap in current dukdb-go compatibility.

## Motivation

DuckDB v1.4.3 provides rich support for complex data types that enable handling semi-structured data, nested structures, and flexible schemas. Currently, dukdb-go has partial support (Map and Union type definitions exist but lack full implementation), but critical types like JSON and STRUCT are missing entirely. This limits compatibility with DuckDB applications and prevents use of advanced analytical patterns.

Key motivations:
1. **JSON Processing:** Modern analytics frequently involve JSON data; without native JSON type, users must preprocess data externally
2. **Nested Structures:** STRUCT types support columnar storage of nested data critical for Parquet/JSON interop
3. **Type Flexibility:** UNION types enable polymorphic columns where each row can be a different type
4. **Feature Parity:** Required for complete DuckDB v1.4.3 compatibility

## High-Level Design

The implementation will follow DuckDB's approach:
- **JSON:** Stored as validated VARCHAR with parsing/casting on demand
- **MAP:** Ordered key-value pairs with unique keys (NULL for missing keys)
- **STRUCT:** Named fields with heterogeneous types (stored column-wise internally)
- **UNION:** Discriminator tag + value (different rows can use different union members)

All types support arbitrary nesting and integrate with the existing columnar storage system.

## Capabilities Added

### 1. JSON Type Support
- Native JSON type with automatic validation
- Casting to/from all other DuckDB types
- JSON operators: `->`, `->>`, `#>`, `#>>` (PostgreSQL-style navigation)
- JSON functions: `json_valid()`, `json_type()`, `json_keys()`, etc.

### 2. MAP Type Completion
- Map construction: `MAP([keys], [values])` or `map(zip(keys, values))`
- Element access: `map_col[key]`
- Map functions: `map_keys()`, `map_values()`, `map_extract()`
- Integration with existing Map type definition

### 3. STRUCT Type Implementation
- Struct construction: `{'field1': val1, 'field2': val2}` or `struct_pack()`
- Field access: `struct_col.field_name`
- Dynamic field access: `struct_extract(struct_col, 'field_name')`
- Support for nested structs

### 4. UNION Type Completion
- Union construction: `union_value(tag := value)` or `union_value(tag = value)`
- Type check: `union_tag(union_col)` returns which member is active
- Value access: `union_extract(union_col, 'tag')`

### 5. Construction and Manipulation Functions
- `to_json(any_value)`: Cast any type to JSON
- `from_json(json_str, type)`: Parse JSON string as specific type
- `row_to_json(record)`: Convert entire row to JSON object
- `json_merge_patch(json1, json2)`: Merge JSON objects

## Dependencies

### Required Before This Change
- `type-system-enhancements` - Enhanced type system for complex types (if not exists)
- `expression-execution` - Advanced expression evaluation capabilities

### Dependent Changes
- `parquet-complex-types` - Parquet read/write for complex types
- `json-file-format` - JSON file reader/write integration
- `complex-type-indexing` - Index support for complex type fields

## Testing Strategy

1. **Unit Tests** (30% of effort)
   - Type validation and casting logic
   - Function implementations
   - Operator behavior
   - Serialization/deserialization

2. **Integration Tests** (40% of effort)
   - CREATE TABLE with complex type columns
   - INSERT and SELECT operations
   - COPY TO/FROM with CSV/JSON/Parquet
   - Complex type in WHERE, GROUP BY, ORDER BY clauses

3. **Round-Trip Tests** (20% of effort)
   - Write complex data to CSV/JSON/Parquet
   - Read back and verify data integrity
   - Test nested structures up to 5 levels

4. **Compatibility Tests** (10% of effort)
   - Compare behavior with actual DuckDB v1.4.3
   - Test edge cases and error handling

## Rollout Plan

1. Phase 1: Core type definitions and validation (JSON, STRUCT, complete MAP/UNION)
2. Phase 2: Parser support for type literals
3. Phase 3: Storage and serialization layer
4. Phase 4: Operators and access methods
5. Phase 5: Construction and manipulation functions
6. Phase 6: COPY statement integration
7. Phase 7: Full integration testing and bug fixes

## Risks and Mitigations

**Risk:** Parser complexity for complex type literals
**Mitigation:** Start with function-based construction before adding literal syntax

**Risk:** Memory overhead for deeply nested structures
**Mitigation:** Implement depth limits and efficient storage using columnar format

**Risk:** Performance degradation from validation overhead
**Mitigation:** Lazy validation where possible, cache parsed JSON structures

**Risk:** Compatibility differences with DuckDB behavior
**Mitigation:** Extensive testing against DuckDB v1.4.3 reference implementation

## Open Questions

1. Should we implement DuckDB's `STRUCT` syntax (`{'a': 1, 'b': 2}`) or use named constructors?
2. How should we handle JSON path expressions beyond basic navigation?
3. What's the maximum nesting depth we should support?
4. Should we implement all JSON functions or focus on most commonly used ones?

## References

- DuckDB Complex Types Documentation: https://duckdb.org/docs/stable/sql/data_types/overview.html
- DuckDB JSON Functions: https://duckdb.org/docs/stable/data/json/json_functions.html
- DuckDB v1.4.3 Release Notes: https://duckdb.org/2025/12/09/announcing-duckdb-143.html
- PostgreSQL JSON Operators (reference): https://www.postgresql.org/docs/current/functions-json.html

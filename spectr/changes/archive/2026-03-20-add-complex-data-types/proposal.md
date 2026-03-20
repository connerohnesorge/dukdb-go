# Change: Add Complex Data Types Support

## Why

dukdb-go currently supports only primitive types (numeric, temporal, string) with full columnar storage and serialization. Complex types (JSON, MAP, STRUCT, UNION) are scaffolded to generic `[]any` storage without proper structure, serialization, or nested support.

Supporting complex types is essential for:
- **API Parity**: Original duckdb-go fully supports complex types
- **Real-world Data**: JSON documents, nested records, and heterogeneous data are common
- **Query Functionality**: Aggregations, joins, and transformations on complex data
- **Ecosystem Compatibility**: Extensions and functions depend on complex type support

## What Changes

### Phase 1: Core Complex Type Storage Architecture

1. **Specialized Vector Classes**: New vector types for each complex data type
   - `ListVector`: Variable-length arrays with child vector reference
   - `StructVector`: Named fields with per-field vectors and indices
   - `MapVector`: Key-value pairs with separate key/value child vectors
   - `UnionVector`: Tagged variants with active member selection
   - `JSONVector`: String-backed JSON with parsed representation caching

2. **Vector Management System**: Child vector tracking and validity propagation
   - Parent-child vector relationships (LIST → element type)
   - Recursive validity bitmap inheritance
   - Unified validity semantics across nesting levels

3. **Serialization & Deserialization**: DuckDB 1.4.3 format compliance
   - Recursive column segment serialization for nested types
   - Child vector persistence in row groups
   - Compression support for complex type data (FSST for strings/JSON, RLE for indices)

4. **Scanning & Binding Helpers**: User-facing API
   - `ScanJSON()` - Parse JSON to typed structs
   - `ScanMap()` - Convert DuckDB MAP to Go map
   - `ScanStruct()` - Convert DuckDB STRUCT to Go struct
   - `ScanUnion()` - Access tagged union values
   - Parameter binding wrappers for complex types

### Affected Specs

- `complex-types`: Core complex type vector implementations
- `vector-management`: Child vector lifecycle and validity propagation
- `serialization`: Row group persistence and DuckDB format compliance
- `scanning`: User-facing scanning and binding APIs

### Breaking Changes

**BREAKING** (API enhancement, not breaking existing code):
- Vector interface may gain new methods for complex type support
- RowGroup serialization format unchanged but enhanced
- Type enum values stable - no renames or removals

### Non-Goals

- **Phase 1 excludes**: LIST, ARRAY, ENUM, GEOMETRY, LAMBDA, VARIANT (Phase 2)
- Performance tuning (baseline implementation first)
- Custom type extensions (reserved for future work)
- Aggregate functions on complex types (query execution phase)

## Implementation Order

**Foundation (Tasks 1-3)**: Build vector infrastructure
**Iteration 1 (Tasks 4-7)**: JSON type support
**Iteration 2 (Tasks 8-11)**: MAP type support
**Iteration 3 (Tasks 12-15)**: STRUCT type support
**Iteration 4 (Tasks 16-19)**: UNION type support
**Integration (Tasks 20-22)**: Testing and validation

## Impact

- **Code**: New vector classes in `internal/vector/`, serialization in `internal/storage/`
- **Tests**: Unit tests for each vector type, integration tests for round-trip serialization
- **Performance**: Minimal overhead for primitive types (vector routing in hot path)
- **Compatibility**: Full DuckDB 1.4.3 format compliance, existing queries unaffected

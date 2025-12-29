# Change: Implement Core TypeInfo Metadata System

## Why

The current type system in dukdb-go provides only basic Type enumeration (37 types) without metadata capabilities. This blocks critical features like UDF type validation, query introspection, and complex type operations. The reference duckdb-go v1.4.3 implementation provides a comprehensive TypeInfo interface for advanced type metadata (DECIMAL precision/scale, ENUM values, STRUCT field information, etc.).

**Problem**: Without TypeInfo, we cannot:
- Validate UDF input/output types with detailed constraints
- Expose column metadata via `Stmt.ColumnTypeInfo()` API
- Support complex nested types (STRUCT fields, MAP key/value types)
- Provide actionable error messages for type construction failures

## What

Implement the **Core TypeInfo API** with:
1. **TypeInfo interface** - Introspection for all 37 types (InternalType(), Details(), logicalType())
2. **8 construction functions** - NewTypeInfo(), NewDecimalInfo(), NewEnumInfo(), NewListInfo(), NewStructInfo(), NewMapInfo(), NewArrayInfo(), NewUnionInfo()
3. **7 TypeDetails structs** - Type-specific metadata with public fields (DecimalDetails, EnumDetails, etc.)
4. **Error variables** - Simple error constants matching duckdb-go pattern (errInvalidDecimalWidth, etc.)
5. **Complete validation** - Enforce all type constraints (DECIMAL width/scale limits, ENUM uniqueness, etc.)
6. **Integration points** - Statement introspection, UDF validation APIs

**Scope Limitation**: This proposal covers **Core TypeInfo API only**. The following are **separate proposals**:
- **DuckDB Binary Serialization** (P0-1b) - Read/write .duckdb catalog format
- **Catalog Persistence** - Deferred to persistence proposal

**Note**: The `logicalType()` method is defined as part of the TypeInfo interface (required for API compatibility with duckdb-go v1.4.3), but the full LogicalType implementation and conversion logic will be completed in subsequent work.

**API Compatibility Target**: duckdb-go v1.4.3 `type_info.go` (construction and Details APIs)

## Impact

### Users
- ✅ **Enables**: UDF type validation, query metadata inspection
- ✅ **Unlocks**: Advanced type operations in SQL execution
- ⚠️ **Breaking**: None (pure addition, no existing API changes)

### Codebase
- **New Files**: `type_info.go`, `type_info_impl.go`, `type_details.go`, `type_info_test.go`, `type_info_validation_test.go`
- **Modified Files**:
  - `errors.go` - Add TypeInfo validation error variables
  - `backend.go` - Add GetColumnTypeInfo to BackendStmtIntrospector
  - `prepared.go` - Add Stmt.ColumnTypeInfo() method
  - `scalar_udf.go` - Update ScalarFuncConfig to use TypeInfo
  - `table_udf.go` - Update TableFunctionConfig to use TypeInfo
  - `aggregate_udf.go` - Update AggregateFuncConfig to use TypeInfo (NOTE: Aggregate UDFs not in duckdb-go v1.4.3; preparatory work for future implementation)
  - `internal/catalog/catalog.go` - Track TypeInfo for table columns
- **Dependencies**: Requires existing Type enum, error types, catalog structure (all exist)
- **Blocks**: Statement introspection API, UDF type safety, DuckDB Binary Serialization (P0-1b), LogicalType Integration (P0-1c)

### Risks
- **Complexity**: TypeInfo construction and validation is non-trivial (7 constructors with constraint checking)
- **Testing**: Requires extensive validation tests for all type combinations
- **Performance**: TypeInfo creation happens at query preparation (infrequent), minimal impact
- **Mitigation**: Phased implementation (primitives → simple → nested), comprehensive unit tests, compatibility test suite

### Alternatives Considered
1. **Minimal TypeInfo stub** - Rejected: blocks UDF and introspection features
2. **Type-specific functions only** - Rejected: doesn't match duckdb-go API
3. **Dynamic typing without metadata** - Rejected: loses type safety and query metadata

## Success Criteria

- [ ] All 8 construction functions implemented with validation
- [ ] All 7 TypeDetails structs implemented with public fields
- [ ] Error variables added to errors.go (errInvalidDecimalWidth, errInvalidDecimalScale, errInvalidArraySize, errEmptyName, errDuplicateName)
- [ ] Defensive copying for EnumDetails.Values, UnionDetails.Members, StructDetails.Entries
- [ ] `Stmt.ColumnTypeInfo(n)` returns correct TypeInfo for query columns
- [ ] UDF configs accept TypeInfo (ScalarFuncConfig, TableFunctionConfig, AggregateFuncConfig)
- [ ] 100+ unit tests covering construction, validation, and edge cases
- [ ] Validation tests for all error messages using string matching pattern
- [ ] Compatibility tests match duckdb-go behavior for TypeInfo API
- [ ] No data races (verified with `go test -race`)
- [ ] Documentation with examples for each constructor and error handling

**Deferred to Separate Proposals**:
- Catalog serialization → DuckDB Binary Serialization (P0-1b)
- Full LogicalType conversion implementation → Future work

## Dependencies

### Required Before
- ✅ Type enumeration (exists: `type_enum.go`)
- ✅ Error types (exists: `errors.go`)
- ✅ Catalog structure (exists: `internal/catalog/`)

### Enables After
- Statement introspection (ColumnTypeInfo, ParamType)
- UDF type validation
- Complex type operations in executor
- Arrow type mapping
- Query metadata extraction

## Related Specs

- `type-system` - MODIFIED (adds TypeInfo requirements)
- `statement-introspection` - REQUIRES TypeInfo
- `scalar-udf` - REQUIRES TypeInfo
- `table-udf` - REQUIRES TypeInfo
- `aggregate-udf` - REQUIRES TypeInfo

## Rollout Plan

### Phase 1: Core Infrastructure (Week 1)
- TypeInfo interface and TypeDetails hierarchy
- Primitive type constructor (NewTypeInfo)
- Unit tests for primitives

### Phase 2: Simple Complex Types (Week 1-2)
- DECIMAL constructor with width/scale validation
- ENUM constructor with uniqueness validation
- Unit tests

### Phase 3: Nested Types (Week 2)
- LIST and ARRAY constructors
- STRUCT, MAP, UNION constructors
- Recursive type tests

### Phase 4: Integration (Week 2-3)
- Backend interface extension (GetColumnTypeInfo)
- Statement API (ColumnTypeInfo)
- UDF config updates
- Catalog serialization

### Phase 5: Testing and Validation (Week 3)
- Compatibility test suite
- Integration tests with SQL queries
- Serialization round-trip tests
- Performance verification

## Approval Checklist

- [ ] Design reviewed (see design.md)
- [ ] Spec deltas validated (spectr validate implement-typeinfo-metadata)
- [ ] Tasks sequenced (see tasks.md)
- [ ] Dependencies confirmed
- [ ] Testing strategy approved

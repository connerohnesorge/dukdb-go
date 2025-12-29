# Implementation Tasks: TypeInfo Metadata System

## Phase 1: Core Infrastructure (Week 1)

### Task 1: Define TypeInfo Interface and TypeDetails Hierarchy
- [ ] Create `type_info.go` with TypeInfo interface (InternalType(), Details())
- [ ] Create `type_details.go` with TypeDetails marker interface
- [ ] Define all 7 TypeDetails interfaces (DecimalDetails, EnumDetails, ListDetails, ArrayDetails, MapDetails, StructDetails, UnionDetails)
- [ ] Define StructEntry interface and UnionMember struct
- [ ] Add godoc comments for all exported types
- [ ] **Validation**: Interfaces compile, no syntax errors

### Task 2: Implement Primitive TypeInfo
- [ ] Create `type_info_impl.go` with primitiveTypeInfo struct
- [ ] Implement NewTypeInfo(t Type) constructor
- [ ] Validate input (reject TYPE_INVALID)
- [ ] Create `type_info_cache.go` for caching primitive instances
- [ ] Implement cache with sync.Map for thread safety
- [ ] **Validation**: Unit tests for all 30+ primitive types

### Task 3: Add Error Variables
- [ ] Add TypeInfo-specific error variables to `errors.go`:
  - errInvalidDecimalWidth (with max_decimal_width constant)
  - errInvalidDecimalScale
  - errInvalidArraySize
  - errEmptyName
  - errDuplicateName
- [ ] Add helper functions:
  - duplicateNameError(name string) error
  - interfaceIsNilError(param string) error
- [ ] **Validation**: Error variables match duckdb-go pattern with getError() wrapper

## Phase 2: DECIMAL and ENUM Types (Week 1)

### Task 4: Implement DECIMAL TypeInfo
- [ ] Add decimalTypeInfo struct to `type_info_impl.go`
- [ ] Implement DecimalDetails interface methods (Width(), Scale())
- [ ] Implement NewDecimalInfo(width, scale uint8) with validation:
  - Width: 1-38
  - Scale: 0-width
- [ ] Create unit tests in `type_info_test.go`:
  - Valid widths (1, 18, 38)
  - Valid scales (0, middle, width)
  - Invalid width (0, 39, 100)
  - Invalid scale (exceeds width)
- [ ] **Validation**: All DECIMAL constraint tests pass

### Task 5: Implement ENUM TypeInfo
- [ ] Add enumTypeInfo struct to `type_info_impl.go`
- [ ] Implement EnumDetails interface method (Values())
- [ ] Implement NewEnumInfo(first string, others ...string) with validation:
  - At least one value (enforced by signature)
  - No duplicates
- [ ] Create unit tests:
  - Single value ENUM
  - Multiple values
  - Duplicate detection
  - Order preservation
- [ ] **Validation**: All ENUM constraint tests pass

## Phase 3: Nested Types (Week 2)

### Task 6: Implement LIST TypeInfo
- [ ] Add listTypeInfo struct to `type_info_impl.go`
- [ ] Implement ListDetails interface method (Child())
- [ ] Implement NewListInfo(childInfo TypeInfo) constructor
- [ ] Create unit tests:
  - LIST(INTEGER)
  - LIST(VARCHAR)
  - LIST(LIST(INTEGER)) - nested lists
  - LIST(STRUCT(...)) - lists of structs
- [ ] **Validation**: Recursive LIST types work

### Task 7: Implement ARRAY TypeInfo
- [ ] Add arrayTypeInfo struct to `type_info_impl.go`
- [ ] Implement ArrayDetails interface methods (Child(), Size())
- [ ] Implement NewArrayInfo(childInfo TypeInfo, size uint64) with validation:
  - Size > 0
- [ ] Create unit tests:
  - Fixed-size arrays (INTEGER[10])
  - Size validation (reject size=0)
  - ARRAY of complex types
- [ ] **Validation**: ARRAY size constraints enforced

### Task 8: Implement STRUCT TypeInfo
- [ ] Create structEntry implementation (unexported)
- [ ] Add structTypeInfo struct to `type_info_impl.go`
- [ ] Implement StructDetails interface method (Entries())
- [ ] Implement NewStructInfo(firstEntry StructEntry, others ...StructEntry) with validation:
  - At least one field (enforced by signature)
  - No duplicate field names
- [ ] Create unit tests:
  - Simple struct (id, name)
  - Duplicate field name detection
  - Nested structs
  - Field name escaping (special characters)
- [ ] **Validation**: STRUCT field uniqueness enforced

### Task 9: Implement MAP TypeInfo
- [ ] Add mapTypeInfo struct to `type_info_impl.go`
- [ ] Implement MapDetails interface methods (Key(), Value())
- [ ] Implement NewMapInfo(keyInfo, valueInfo TypeInfo) with validation:
  - Key type must be comparable
  - Reject LIST, STRUCT, MAP, ARRAY, UNION keys
- [ ] Create unit tests:
  - Valid key types (INTEGER, VARCHAR, DECIMAL, etc.)
  - Invalid key types (LIST, STRUCT, MAP, ARRAY, UNION)
  - Complex value types
- [ ] **Validation**: MAP key comparability enforced

### Task 10: Implement UNION TypeInfo
- [ ] Add unionTypeInfo struct to `type_info_impl.go`
- [ ] Implement UnionDetails interface method (Members())
- [ ] Implement NewUnionInfo(memberTypes []TypeInfo, memberNames []string) with validation:
  - At least one member
  - Types and names same length
  - No duplicate member names
- [ ] Create unit tests:
  - Simple union (int | varchar)
  - Length mismatch detection
  - Duplicate member name detection
  - Complex member types
- [ ] **Validation**: UNION member uniqueness enforced

## Phase 4: Integration Points (Week 2-3)

### Task 11: Extend Backend Interface
- [ ] Add GetColumnTypeInfo(idx int) (TypeInfo, error) to backend.BackendStmtIntrospector interface in `backend.go`
- [ ] Update mock backend in tests to implement GetColumnTypeInfo
- [ ] Add TODO comments in internal/engine for future implementation
- [ ] **Validation**: Interface compiles, mocks updated

### Task 12: Add Statement Introspection API
- [ ] Add ColumnTypeInfo(n int) (TypeInfo, error) method to Stmt in `prepared.go`
- [ ] Implement delegation to BackendStmtIntrospector
- [ ] Return ErrNotSupported if backend doesn't implement interface
- [ ] Add godoc comment with 0-based indexing note
- [ ] **Validation**: Stmt.ColumnTypeInfo() compiles and delegates correctly

### Task 13: Update UDF Configs
- [ ] Update ScalarFuncConfig in `scalar_udf.go`:
  - Change InputTypeInfos from []Type to []TypeInfo
  - Change ResultTypeInfo from Type to TypeInfo
  - Change VariadicTypeInfo from Type to TypeInfo
- [ ] Update TableFunctionConfig in `table_udf.go`:
  - Update ColumnInfo struct to use TypeInfo
- [ ] Update AggregateFuncConfig in `aggregate_udf.go`:
  - Change InputTypeInfos, StateTypeInfo, ResultTypeInfo to TypeInfo
- [ ] Update existing UDF tests to use NewTypeInfo() for primitives
- [ ] **Validation**: All UDF tests still pass with TypeInfo

### Task 14: Catalog Integration (In-Memory Only)
- [ ] Update Column struct in `internal/catalog/column.go` to include TypeInfo field
- [ ] Update table creation to track TypeInfo for columns in memory
- [ ] **Validation**: Catalog compiles with TypeInfo fields
- [ ] **NOTE**: Schema serialization is deferred to P0-1b (DuckDB Binary Serialization)

## Phase 5: Serialization - DEFERRED TO P0-1b

**NOTE**: Tasks 15-16 (TypeInfo Serialization and Catalog Persistence) are **DEFERRED** to the **DuckDB Binary Serialization (P0-1b)** proposal. The Core TypeInfo proposal focuses exclusively on the API interface, construction functions, and validation.

**Deferred Tasks**:
- ~~Task 15: Implement TypeInfo Serialization~~ → P0-1b
- ~~Task 16: Catalog Persistence with TypeInfo~~ → P0-1b

## Phase 6: Testing and Documentation (Week 3)

### Task 17: Unit Test Coverage
- [ ] Create `type_info_validation_test.go` for constraint tests:
  - All DECIMAL validations
  - All ENUM validations
  - All STRUCT validations
  - All MAP validations
  - All ARRAY validations
  - All UNION validations
- [ ] Verify 100% coverage for type_info*.go files
- [ ] Run `go test -race` to check for data races
- [ ] **Validation**: Coverage >95%, no races detected

### Task 18: Integration Tests
- [ ] Create `type_info_integration_test.go`:
  - Create tables with complex types
  - Query column metadata via Stmt.ColumnTypeInfo()
  - Verify TypeInfo matches table definition
  - Test UDFs with complex type parameters
- [ ] **Validation**: Integration tests pass

### Task 19: Compatibility Tests
- [ ] Create compatibility tests in `compatibility/types_test.go`:
  - Compare TypeInfo behavior with duckdb-go reference
  - Test all construction functions match
  - Test all validation errors match
  - Test Details() values match
- [ ] Reference both implementations in tests
- [ ] **Validation**: Compatibility tests pass for all types

### Task 20: Documentation and Examples
- [ ] Add package-level godoc for TypeInfo system
- [ ] Create examples for each constructor:
  - Example_newDecimalInfo()
  - Example_newEnumInfo()
  - Example_newListInfo()
  - Example_newStructInfo()
  - Example_newMapInfo()
  - Example_newArrayInfo()
  - Example_newUnionInfo()
- [ ] Add usage examples in README or docs/
- [ ] **Validation**: `go doc` shows all examples, examples run successfully

## Phase 7: Final Validation (Week 3)

### Task 21: Performance Verification
- [ ] Benchmark TypeInfo creation for primitives (should use cache)
- [ ] Benchmark complex type creation (STRUCT, MAP, UNION)
- [ ] Verify caching reduces allocations
- [ ] Profile memory usage
- [ ] **Validation**: Benchmarks show acceptable performance

### Task 22: Code Quality Checks
- [ ] Run `golangci-lint run` - fix all issues
- [ ] Run `go vet` - fix all warnings
- [ ] Run `gofmt` - ensure formatting
- [ ] Review all godoc comments for completeness
- [ ] **Validation**: All code quality checks pass

### Task 23: Final Integration Test
- [ ] Create end-to-end test:
  - Define UDF with complex types
  - Create table with same complex types (in-memory catalog)
  - Query table, verify column TypeInfo via Stmt.ColumnTypeInfo()
  - Call UDF, verify parameter TypeInfo
- [ ] **Validation**: Full end-to-end workflow works (construction, validation, introspection)
- [ ] **NOTE**: Serialization/persistence tests deferred to P0-1b

### Task 24: Update CHANGELOG and Documentation
- [ ] Add entry to CHANGELOG.md (if exists)
- [ ] Update main README with TypeInfo capabilities
- [ ] Update GAP_ANALYSIS.md to mark TypeInfo as complete
- [ ] **Validation**: Documentation updated

## Dependencies

- **Parallel Work**: Tasks 4-5 can run in parallel
- **Parallel Work**: Tasks 6-10 can run in parallel (after Task 3)
- **Sequential**: Phase 4 requires Phase 3 complete
- **Sequential**: Phase 5 requires Phase 4 complete
- **Sequential**: Phase 6 requires Phase 5 complete

## Success Criteria

All tasks completed when:
- [ ] All 8 construction functions implemented and tested
- [ ] All 7 TypeDetails structs working (with public fields)
- [ ] Error variables added to errors.go with getError() pattern
- [ ] 100+ unit tests passing
- [ ] Stmt.ColumnTypeInfo() returns correct TypeInfo
- [ ] UDF configs use TypeInfo (ScalarFuncConfig, TableFunctionConfig)
- [ ] Catalog tracks TypeInfo in-memory (persistence deferred to P0-1b)
- [ ] Compatibility tests match duckdb-go construction/validation behavior
- [ ] No data races (verified with -race flag)
- [ ] Code quality checks pass
- [ ] Documentation complete with examples

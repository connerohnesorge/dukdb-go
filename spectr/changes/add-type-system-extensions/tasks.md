# Tasks: Type System Extensions

## 1. Type Constants Implementation

- [ ] 1.1 Add TYPE_JSON (ID 37) constant to `type_enum.go`
- [ ] 1.2 Add TYPE_GEOMETRY (ID 60) constant to `type_enum.go`
- [ ] 1.3 Add TYPE_LAMBDA (ID 106) constant to `type_enum.go`
- [ ] 1.4 Add TYPE_VARIANT (ID 109) constant to `type_enum.go`
- [ ] 1.5 Update `typeToStringMap` with new type string representations
- [ ] 1.6 Update `Category()` method to include new types
- [ ] 1.7 Remove TYPE_ANY from `unsupportedTypeToStringMap`
- [ ] 1.8 Add type category tests for all new types
- [ ] 1.9 Add string representation tests for all new types

## 2. SQLNULL Type Fix

- [ ] 2.1 Modify `initSQLNull()` in `vector.go` to work correctly
- [ ] 2.2 Update `NewTypeInfo()` in `type_info.go` to accept TYPE_SQLNULL
- [ ] 2.3 Add SQLNULL handling in `Reset()` method
- [ ] 2.4 Add tests for SQLNULL column creation
- [ ] 2.5 Add tests for SQLNULL value insertion and retrieval
- [ ] 2.6 Add tests for SQLNULL with IS NULL predicates

## 3. JSON Type Implementation

- [ ] 3.1 Remove TYPE_JSON from unsupported list in `type_enum.go`
- [ ] 3.2 Modify `initJSON()` in `vector.go` to use TYPE_JSON
- [ ] 3.3 Update `init()` switch to handle TYPE_JSON case
- [ ] 3.4 Add JSONDetails struct to `type_info.go`
- [ ] 3.5 Add NewJSONInfo() constructor to `type_info.go`
- [ ] 3.6 Update `typeInfo.Details()` to return JSONDetails for TYPE_JSON
- [ ] 3.7 Create `internal/io/json/json.go` with helper functions
- [ ] 3.8 Implement `IsValidJSON(s string) bool`
- [ ] 3.9 Implement `ParseJSON(s string) (any, error)`
- [ ] 3.10 Implement `ExtractJSONPath(s string, path string) (any, error)`
- [ ] 3.11 Add JSON path tests
- [ ] 3.12 Add JSON validation tests
- [ ] 3.13 Add JSON roundtrip tests (insert and select)

## 4. GEOMETRY Type Implementation

- [ ] 4.1 Remove TYPE_GEOMETRY from unsupported list in `type_enum.go`
- [ ] 4.2 Create `internal/io/geometry/geometry.go` package
- [ ] 4.3 Define Geometry struct with Type, Data (WKB), and Srid fields
- [ ] 4.4 Define GeometryType constants (Point, LineString, Polygon, etc.)
- [ ] 4.5 Implement WKBReader for parsing Well-Known Binary format
- [ ] 4.6 Implement WKTReader for parsing Well-Known Text format
- [ ] 4.7 Add `initGeometry()` to `vector.go`
- [ ] 4.8 Add GeometryDetails struct to `type_info.go`
- [ ] 4.9 Add NewGeometryInfo() constructor to `type_info.go`
- [ ] 4.10 Update `typeInfo.Details()` to return GeometryDetails
- [ ] 4.11 Add GeometryDetails to `typeInfo.SQLType()` method
- [ ] 4.12 Add setGeometry() function with WKT/WKB support
- [ ] 4.13 Add Geometry tests for WKT parsing
- [ ] 4.14 Add Geometry tests for WKB parsing
- [ ] 4.15 Add Geometry roundtrip tests (insert and select)

## 5. BIGNUM Type Implementation

- [ ] 5.1 Remove TYPE_BIGNUM from `unsupportedTypeToStringMap`
- [ ] 5.2 Add BignumDetails struct to `type_info.go`
- [ ] 5.3 Add NewBignumInfo() constructor to `type_info.go`
- [ ] 5.4 Update `typeInfo.Details()` to return BignumDetails
- [ ] 5.5 Add BignumDetails to `typeInfo.SQLType()` method
- [ ] 5.6 Add `initBignum()` to `vector.go`
- [ ] 5.7 Update `init()` switch to handle TYPE_BIGNUM case
- [ ] 5.8 Add setBignum() function with big.Int and string support
- [ ] 5.9 Add BIGNUM tests for big.Int conversion
- [ ] 5.10 Add BIGNUM tests for string parsing
- [ ] 5.11 Add BIGNUM tests for arbitrary precision arithmetic

## 6. VARIANT Type Implementation

- [ ] 6.1 Remove TYPE_VARIANT from unsupported list in `type_enum.go`
- [ ] 6.2 Add VariantDetails struct to `type_info.go`
- [ ] 6.3 Add NewVariantInfo() constructor to `type_info.go`
- [ ] 6.4 Update `typeInfo.Details()` to return VariantDetails
- [ ] 6.5 Add `initVariant()` to `vector.go`
- [ ] 6.6 Update `init()` switch to handle TYPE_VARIANT case
- [ ] 6.7 Add setVariant() function with any value support
- [ ] 6.8 Add VARIANT tests for dynamic type storage
- [ ] 6.9 Add VARIANT tests for JSON-like value handling

## 7. LAMBDA Type Implementation

- [ ] 7.1 Remove TYPE_LAMBDA from unsupported list in `type_enum.go`
- [ ] 7.2 Add LambdaDetails struct to `type_info.go`
- [ ] 7.3 Add NewLambdaInfo() constructor with input/return types
- [ ] 7.4 Update `typeInfo.Details()` to return LambdaDetails
- [ ] 7.5 Add `initLambda()` to `vector.go`
- [ ] 7.6 Update `init()` switch to handle TYPE_LAMBDA case
- [ ] 7.7 Add setLambda() function with expression string support
- [ ] 7.8 Add LAMBDA type info tests
- [ ] 7.9 Add LAMBDA expression parsing tests (basic)

## 8. TypeInfo System Updates

- [ ] 8.1 Update `NewTypeInfo()` to accept all new types
- [ ] 8.2 Update `typeInfo.Details()` switch statement for new types
- [ ] 8.3 Update `typeInfo.SQLType()` for JSON, GEOMETRY, VARIANT, LAMBDA
- [ ] 8.4 Add TypeInfo tests for all new types
- [ ] 8.5 Update cached primitive type info for new types

## 9. Vector System Updates

- [ ] 9.1 Update `vector.init()` switch for new type cases
- [ ] 9.2 Update `vector.Reset()` to handle new data types
- [ ] 9.3 Update `vector.Close()` to handle new data types
- [ ] 9.4 Add vector tests for all new types
- [ ] 9.5 Update vector pool to handle new types

## 10. Integration Tests

- [ ] 10.1 Create integration tests for JSON columns
- [ ] 10.2 Create integration tests for GEOMETRY columns
- [ ] 10.3 Create integration tests for BIGNUM columns
- [ ] 10.4 Create integration tests for VARIANT columns
- [ ] 10.5 Create integration tests for LAMBDA columns
- [ ] 10.6 Add compatibility tests against go-duckdb API
- [ ] 10.7 Add tests for JSON operators (->, ->>, #>)
- [ ] 10.8 Add tests for geometry functions (ST_X, ST_Y, ST_DISTANCE)

## 11. Documentation

- [ ] 11.1 Update `docs/types.md` with new type documentation
- [ ] 11.2 Add JSON type usage examples
- [ ] 11.3 Add GEOMETRY type usage examples
- [ ] 11.4 Add BIGNUM type usage examples
- [ ] 11.5 Update `README.md` with extended type support info

## 12. Performance Benchmarks

- [ ] 12.1 Create benchmark for JSON parsing
- [ ] 12.2 Create benchmark for WKB/WKT parsing
- [ ] 12.3 Create benchmark for BIGNUM operations
- [ ] 12.4 Compare JSON vs VARCHAR performance
- [ ] 12.5 Compare geometry parsing approaches

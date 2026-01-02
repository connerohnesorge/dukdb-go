# Change: Add Type System Extensions

## Why

DuckDB v1.4.3+ supports specialized data types that are missing from dukdb-go, causing API compatibility gaps. The current implementation has:

1. **37 types (0-36)** defined in `type_enum.go`, missing newer types like GEOMETRY, VARIANT, LAMBDA
2. **Unsupported types** marked in `unsupportedTypeToStringMap`: BIGNUM, ANY, INVALID
3. **Broken JSON support**: `initJSON()` exists but returns error; `setJSON()` is unused
4. **Broken SQLNULL**: `initSQLNull()` sets getFn/setFn that return errors

These gaps prevent full DuckDB compatibility and block features like:
- Spatial data processing (GEOMETRY type)
- Dynamic/any JSON-like values (VARIANT type)
- First-class JSON column support
- NULL-only column efficiency (SQLNULL)
- Higher-order functions (LAMBDA)

## What Changes

### Breaking Changes

- **TYPE_ANY**: Currently in `unsupportedTypeToStringMap`, now fully supported for UDFs
- **TYPE_SQLNULL**: Currently returns error on set, now fully functional

### New Types

- **TYPE_JSON** (ID ~38): First-class JSON type with validation and path extraction
- **TYPE_GEOMETRY** (ID ~60): Spatial geometry type with WKT/WKB support
- **TYPE_VARIANT** (ID ~109): Dynamic/any JSON-like type for flexible schema
- **TYPE_LAMBDA** (ID ~106): For higher-order function support

### Enhanced TypeInfo

- **BignumDetails**: Support for variable-width decimal (beyond 38 digits)
- **JSONDetails**: Type info for JSON columns
- **GeometryDetails**: Type info for geometry columns

### Vector Support

- **initJSON()**: Fully implemented JSON column storage and parsing
- **initSQLNull()**: Proper NULL-only column handling
- **initGeometry()**: Geometry storage as WKB binary
- **initVariant()**: Dynamic value storage

### JSON Functions

- `json_valid(json)` - Check if string is valid JSON
- `json_extract(json, path)` - Extract value from JSON
- `json_serialize(json)` - Convert to JSON string
- `->`, `->>`, `#>` operators for JSON access

### Geometry Functions

- `ST_X(geom)`, `ST_Y(geom)` - Coordinate extraction
- `ST_DISTANCE(geom1, geom2)` - Distance calculation
- `ST_CONTAINS(geom1, geom2)` - Spatial predicate
- `ST_INTERSECTS(geom1, geom2)` - Spatial predicate

## Impact

### Affected Specs

- `type-system/spec.md`: Extended with new types and JSON/geometry support
- `extended-types/spec.md`: Added GEOMETRY, VARIANT, LAMBDA type specs

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `type_enum.go` | MODIFIED | Add TYPE_JSON, TYPE_GEOMETRY, TYPE_VARIANT, TYPE_LAMBDA constants |
| `type_info.go` | MODIFIED | Add BignumDetails, JSONDetails, GeometryDetails structs |
| `vector.go` | MODIFIED | Implement initJSON, initSQLNull, initGeometry, initVariant |
| `internal/io/json/json.go` | ADDED | JSON parsing for first-class JSON type |
| `internal/io/geometry/` | ADDED | Geometry parsing and WKB support |
| `internal/binder/bind_expr.go` | MODIFIED | Add JSON path expression binding |
| `internal/executor/expr.go` | MODIFIED | Add JSON and geometry expression execution |

### Dependencies

- This proposal depends on: (none)
- This proposal blocks: (none)

### Performance Considerations

1. JSON columns: Store as VARCHAR with validation on access (lazy parsing)
2. Geometry columns: Store as BLOB (WKB format) for compatibility
3. SQLNULL columns: No data storage, only validity bitmap
4. BIGNUM: Variable-width storage using big.Int

### Migration Path

Types currently marked unsupported will become supported:
- TYPE_BIGNUM: No migration needed, was internal
- TYPE_ANY: Already works for UDF parameters, now fully documented
- TYPE_SQLNULL: Previously returned error, now functional

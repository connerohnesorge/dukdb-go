## MODIFIED Requirements

### Requirement: Type Enumeration

The package SHALL define a Type enumeration with constants for all 54 DuckDB logical types with exact values matching DuckDB.

#### Scenario: Type constant values
- GIVEN the Type enumeration
- THEN TYPE_INVALID equals 0
- AND TYPE_BOOLEAN equals 1
- AND TYPE_TINYINT equals 2
- AND TYPE_VARCHAR equals 17
- AND TYPE_UUID equals 27
- AND TYPE_TIMESTAMP_TZ equals 31
- AND TYPE_JSON equals 37
- AND TYPE_GEOMETRY equals 60
- AND TYPE_LAMBDA equals 106
- AND TYPE_VARIANT equals 109

#### Scenario: Type string representation for new types
- GIVEN TYPE_JSON
- WHEN converting to string via String()
- THEN the result is "JSON"

#### Scenario: Type string representation for GEOMETRY
- GIVEN TYPE_GEOMETRY
- WHEN converting to string via String()
- THEN the result is "GEOMETRY"

#### Scenario: Type string representation for VARIANT
- GIVEN TYPE_VARIANT
- WHEN converting to string via String()
- THEN the result is "VARIANT"

#### Scenario: Type string representation for LAMBDA
- GIVEN TYPE_LAMBDA
- WHEN converting to string via String()
- THEN the result is "LAMBDA"

#### Scenario: Type category for JSON
- GIVEN TYPE_JSON
- WHEN calling Category()
- THEN the result is "string"

#### Scenario: Type category for GEOMETRY
- GIVEN TYPE_GEOMETRY
- WHEN calling Category()
- THEN the result is "other"

#### Scenario: Type category for VARIANT
- GIVEN TYPE_VARIANT
- WHEN calling Category()
- THEN the result is "other"

#### Scenario: Type category for LAMBDA
- GIVEN TYPE_LAMBDA
- WHEN calling Category()
- THEN the result is "other"

#### Scenario: Type category for BIGNUM
- GIVEN TYPE_BIGNUM
- WHEN calling Category()
- THEN the result is "numeric"

## ADDED Requirements

### Requirement: JSON Type Support

The package SHALL provide first-class JSON type support with validation and path extraction.

#### Scenario: Create JSON column
- GIVEN NewTypeInfo(TYPE_JSON)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_JSON

#### Scenario: JSON vector stores string data
- GIVEN a vector initialized for TYPE_JSON
- WHEN setting value '{"name": "Alice"}'
- THEN the vector stores the string
- AND retrieving returns the parsed JSON object

#### Scenario: JSON validation - valid JSON
- GIVEN JSON string '{"name": "Alice", "age": 30}'
- WHEN calling IsValidJSON()
- THEN the result is true

#### Scenario: JSON validation - invalid JSON
- GIVEN JSON string '{name: "Alice"}' (missing quotes)
- WHEN calling IsValidJSON()
- THEN the result is false

#### Scenario: JSON path extraction
- GIVEN JSON string '{"user": {"name": "Alice"}}'
- WHEN extracting path "$.user.name"
- THEN the result is '"Alice"'

#### Scenario: JSON array access
- GIVEN JSON string '[1, 2, 3]'
- WHEN extracting path "$[1]"
- THEN the result is '2'

### Requirement: SQLNULL Type Support

The package SHALL provide SQLNULL type for NULL-only columns.

#### Scenario: Create SQLNULL column
- GIVEN NewTypeInfo(TYPE_SQLNULL)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_SQLNULL

#### Scenario: SQLNULL vector always returns nil
- GIVEN a vector initialized for TYPE_SQLNULL
- WHEN getting any value
- THEN the result is nil

#### Scenario: SQLNULL vector accepts any value (marks as NULL)
- GIVEN a vector initialized for TYPE_SQLNULL
- WHEN setting value 'anything'
- THEN the value is accepted and marked as NULL
- AND getting returns nil

#### Scenario: SQLNULL SQL type representation
- GIVEN TypeInfo for TYPE_SQLNULL
- WHEN calling SQLType()
- THEN the result is "NULL"

### Requirement: GEOMETRY Type Support

The package SHALL provide GEOMETRY type for spatial data with WKT/WKB support.

#### Scenario: Create GEOMETRY column
- GIVEN NewTypeInfo(TYPE_GEOMETRY)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_GEOMETRY

#### Scenario: GEOMETRY from WKT string
- GIVEN a vector initialized for TYPE_GEOMETRY
- WHEN setting value 'POINT(1 2)'
- THEN the vector stores the geometry in WKB format
- AND retrieving returns Geometry with Type=Point

#### Scenario: GEOMETRY from WKB bytes
- GIVEN a vector initialized for TYPE_GEOMETRY
- WHEN setting WKB bytes for 'POINT(1 2)'
- THEN the vector stores the geometry
- AND retrieving returns equivalent Geometry

#### Scenario: GEOMETRY type - LineString
- GIVEN a vector initialized for TYPE_GEOMETRY
- WHEN setting value 'LINESTRING(0 0, 1 1, 2 2)'
- THEN the vector stores the LineString geometry

#### Scenario: GEOMETRY type - Polygon
- GIVEN a vector initialized for TYPE_GEOMETRY
- WHEN setting value 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'
- THEN the vector stores the Polygon geometry

#### Scenario: GEOMETRY SQL type representation
- GIVEN TypeInfo for TYPE_GEOMETRY
- WHEN calling SQLType()
- THEN the result is "GEOMETRY"

### Requirement: BIGNUM Type Support

The package SHALL provide BIGNUM type for variable-width decimal with arbitrary precision.

#### Scenario: Create BIGNUM column
- GIVEN NewBignumInfo(scale)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_BIGNUM

#### Scenario: BIGNUM from big.Int
- GIVEN a vector initialized for TYPE_BIGNUM
- WHEN setting value big.NewInt(12345678901234567890)
- THEN the vector stores the big.Int
- AND retrieving returns equivalent big.Int

#### Scenario: BIGNUM from string
- GIVEN a vector initialized for TYPE_BIGNUM
- WHEN setting value "12345678901234567890"
- THEN the vector stores the big.Int
- AND retrieving returns equivalent big.Int

#### Scenario: BIGNUM arbitrary precision
- GIVEN a vector initialized for TYPE_BIGNUM
- WHEN setting value with 100 digits
- THEN the vector stores all 100 digits
- AND retrieving returns all 100 digits

#### Scenario: BIGNUM SQL type representation
- GIVEN TypeInfo for TYPE_BIGNUM
- WHEN calling SQLType()
- THEN the result is "BIGNUM"

### Requirement: VARIANT Type Support

The package SHALL provide VARIANT type for dynamic/any JSON-like values.

#### Scenario: Create VARIANT column
- GIVEN NewTypeInfo(TYPE_VARIANT)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_VARIANT

#### Scenario: VARIANT stores any JSON value
- GIVEN a vector initialized for TYPE_VARIANT
- WHEN setting value '{"key": "value"}'
- THEN the vector stores the JSON string
- AND retrieving returns the parsed JSON

#### Scenario: VARIANT stores array
- GIVEN a vector initialized for TYPE_VARIANT
- WHEN setting value '[1, 2, 3]'
- THEN the vector stores the JSON array
- AND retrieving returns the parsed array

#### Scenario: VARIANT stores primitive
- GIVEN a vector initialized for TYPE_VARIANT
- WHEN setting value '"hello"'
- THEN the vector stores the string
- AND retrieving returns the string

#### Scenario: VARIANT SQL type representation
- GIVEN TypeInfo for TYPE_VARIANT
- WHEN calling SQLType()
- THEN the result is "VARIANT"

### Requirement: LAMBDA Type Support

The package SHALL provide LAMBDA type for higher-order function support.

#### Scenario: Create LAMBDA column
- GIVEN NewTypeInfo(TYPE_LAMBDA)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_LAMBDA

#### Scenario: LAMBDA stores expression string
- GIVEN a vector initialized for TYPE_LAMBDA
- WHEN setting value 'x -> x + 1'
- THEN the vector stores the expression string
- AND retrieving returns the expression string

#### Scenario: LAMBDA SQL type representation
- GIVEN TypeInfo for TYPE_LAMBDA
- WHEN calling SQLType()
- THEN the result is "LAMBDA"

### Requirement: TypeInfo JSON Details

The package SHALL provide JSONDetails for JSON type metadata.

#### Scenario: JSONDetails from TypeInfo
- GIVEN NewTypeInfo(TYPE_JSON)
- WHEN calling Details()
- THEN the result is non-nil
- AND type assertion to *JSONDetails succeeds

### Requirement: TypeInfo Geometry Details

The package SHALL provide GeometryDetails for GEOMETRY type metadata.

#### Scenario: GeometryDetails with SRID
- GIVEN NewGeometryInfo(4326) with SRID
- WHEN calling Details()
- THEN the result is non-nil
- AND Details().(*GeometryDetails).Srid equals 4326

### Requirement: TypeInfo Bignum Details

The package SHALL provide BignumDetails for BIGNUM type metadata.

#### Scenario: BignumDetails with scale
- GIVEN NewBignumInfo(4)
- WHEN calling Details()
- THEN the result is non-nil
- AND Details().(*BignumDetails).Scale equals 4

### Requirement: JSON Path Extraction

The package SHALL provide JSON path extraction for JSON values.

#### Scenario: Extract nested object field
- GIVEN JSON '{"a": {"b": "c"}}'
- WHEN extracting path "$.a.b"
- THEN the result is '"c"'

#### Scenario: Extract array element
- GIVEN JSON '{"arr": [10, 20, 30]}'
- WHEN extracting path "$.arr[1]"
- THEN the result is '20'

#### Scenario: Extract with wildcard
- GIVEN JSON '{"items": [{"id": 1}, {"id": 2}]}'
- WHEN extracting path "$.items[*].id"
- THEN the result is '[1, 2]'

#### Scenario: Invalid path
- GIVEN JSON '{"a": 1}'
- WHEN extracting path "$.nonexistent"
- THEN the result is nil or error

### Requirement: JSON Operators

The package SHALL support JSON operators for extraction and manipulation.

#### Scenario: JSON -> operator (object field)
- GIVEN JSON '{"name": "Alice"}'
- WHEN applying -> 'name'
- THEN the result is '"Alice"'

#### Scenario: JSON ->> operator (string value)
- GIVEN JSON '{"name": "Alice"}'
- WHEN applying ->> 'name'
- THEN the result is 'Alice' (without quotes)

#### Scenario: JSON #> operator (path)
- GIVEN JSON '{"a": {"b": "c"}}'
- WHEN applying #> 'a,b' path
- THEN the result is '"c"'

#### Scenario: JSON -> operator with array
- GIVEN JSON '{"arr": [1, 2]}'
- WHEN applying -> 'arr'
- THEN the result is '[1, 2]'

### Requirement: Geometry Functions

The package SHALL provide geometry functions for spatial operations.

#### Scenario: ST_X extracts X coordinate
- GIVEN GEOMETRY 'POINT(1.5 2.5)'
- WHEN calling ST_X()
- THEN the result is 1.5

#### Scenario: ST_Y extracts Y coordinate
- GIVEN GEOMETRY 'POINT(1.5 2.5)'
- WHEN calling ST_Y()
- THEN the result is 2.5

#### Scenario: ST_DISTANCE calculates distance
- GIVEN GEOMETRY 'POINT(0 0)' and 'POINT(3 4)'
- WHEN calling ST_DISTANCE()
- THEN the result is 5.0

#### Scenario: ST_CONTAINS checks containment
- GIVEN GEOMETRY 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))' and 'POINT(5 5)'
- WHEN calling ST_CONTAINS()
- THEN the result is true

#### Scenario: ST_INTERSECTS checks intersection
- GIVEN GEOMETRY 'LINESTRING(0 0, 5 5)' and 'LINESTRING(5 0, 0 5)'
- WHEN calling ST_INTERSECTS()
- THEN the result is true

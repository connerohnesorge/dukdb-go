# Type System Specification Delta

## ADDED Requirements

### Requirement: TypeInfo Interface

The package SHALL export a TypeInfo interface providing type metadata beyond the basic Type enum.

#### Scenario: TypeInfo for primitive type
- GIVEN NewTypeInfo(TYPE_INTEGER)
- WHEN calling InternalType()
- THEN the result equals TYPE_INTEGER
- AND Details() returns nil

#### Scenario: TypeInfo for DECIMAL type
- GIVEN NewDecimalInfo(18, 4)
- WHEN calling InternalType()
- THEN the result equals TYPE_DECIMAL
- AND Details() returns non-nil DecimalDetails
- AND Details().(*DecimalDetails).Width equals 18
- AND Details().(*DecimalDetails).Scale equals 4

#### Scenario: TypeInfo immutability
- GIVEN any TypeInfo instance
- WHEN accessed from multiple goroutines concurrently
- THEN no data races occur (verified with -race flag)

#### Scenario: TypeInfo caching for primitives
- GIVEN NewTypeInfo(TYPE_INTEGER) called twice
- WHEN comparing the returned instances
- THEN both return the same cached instance (same pointer)

### Requirement: DECIMAL TypeInfo Constructor

The package SHALL provide NewDecimalInfo(width, scale uint8) to create DECIMAL type metadata with precision validation.

#### Scenario: Valid DECIMAL(18,4)
- GIVEN NewDecimalInfo(18, 4)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_DECIMAL
- AND Details().(*DecimalDetails).Width equals 18
- AND Details().(*DecimalDetails).Scale equals 4

#### Scenario: Valid DECIMAL(38,38) - maximum values
- GIVEN NewDecimalInfo(38, 38)
- WHEN the function returns
- THEN error is nil
- AND Width equals 38
- AND Scale equals 38

#### Scenario: Valid DECIMAL(1,0) - minimum values
- GIVEN NewDecimalInfo(1, 0)
- WHEN the function returns
- THEN error is nil
- AND Width equals 1
- AND Scale equals 0

#### Scenario: Invalid DECIMAL width too high
- GIVEN NewDecimalInfo(39, 2)
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrInvalidDecimalParams
- AND error message contains "width must be 1-38"

#### Scenario: Invalid DECIMAL width too low
- GIVEN NewDecimalInfo(0, 0)
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrInvalidDecimalParams

#### Scenario: Invalid DECIMAL scale exceeds width
- GIVEN NewDecimalInfo(10, 11)
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrInvalidDecimalParams
- AND error message contains "scale"  AND error message contains "exceeds width"

### Requirement: ENUM TypeInfo Constructor

The package SHALL provide NewEnumInfo(first string, others ...string) to create ENUM type metadata with uniqueness validation.

#### Scenario: Valid single-value ENUM
- GIVEN NewEnumInfo("ACTIVE")
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_ENUM
- AND Details().(*EnumDetails).Values equals []string{"ACTIVE"}

#### Scenario: Valid multi-value ENUM
- GIVEN NewEnumInfo("RED", "GREEN", "BLUE")
- WHEN the function returns
- THEN error is nil
- AND Details().(*EnumDetails).Values equals []string{"RED", "GREEN", "BLUE"}
- AND values are in order

#### Scenario: Invalid ENUM with duplicate values
- GIVEN NewEnumInfo("X", "Y", "X")
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrDuplicateEnumValue
- AND error message contains "X"

#### Scenario: ENUM values are immutable
- GIVEN info from NewEnumInfo("A", "B", "C")
- WHEN calling Details().(*EnumDetails).Values and modifying the returned slice
- THEN subsequent calls to Details().(*EnumDetails).Values still return []string{"A", "B", "C"}

### Requirement: LIST TypeInfo Constructor

The package SHALL provide NewListInfo(childInfo TypeInfo) to create variable-length list type metadata with recursive type support.

#### Scenario: Valid LIST(INTEGER)
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND NewListInfo(intInfo)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_LIST
- AND Details().(*ListDetails).Child equals intInfo

#### Scenario: Nested LIST(LIST(VARCHAR))
- GIVEN varcharInfo from NewTypeInfo(TYPE_VARCHAR)
- AND innerList from NewListInfo(varcharInfo)
- AND outerList from NewListInfo(innerList)
- WHEN the function returns
- THEN error is nil
- AND outerList InternalType() equals TYPE_LIST
- AND outerList Details().(*ListDetails).Child.InternalType() equals TYPE_LIST
- AND outerList Details().(*ListDetails).Child.Details().(*ListDetails).Child equals varcharInfo

#### Scenario: LIST of STRUCT
- GIVEN structInfo from NewStructInfo(...)
- AND NewListInfo(structInfo)
- WHEN the function returns
- THEN error is nil
- AND Details().(*ListDetails).Child.InternalType() equals TYPE_STRUCT

### Requirement: ARRAY TypeInfo Constructor

The package SHALL provide NewArrayInfo(childInfo TypeInfo, size uint64) to create fixed-length array type metadata with size validation.

#### Scenario: Valid ARRAY(INTEGER, 10)
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND NewArrayInfo(intInfo, 10)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_ARRAY
- AND Details().(*ArrayDetails).Child equals intInfo
- AND Details().(*ArrayDetails).Size equals 10

#### Scenario: Invalid ARRAY with size 0
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND NewArrayInfo(intInfo, 0)
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrInvalidArrayParams
- AND error message contains "size must be > 0"

#### Scenario: ARRAY of complex type
- GIVEN structInfo from NewStructInfo(...)
- AND NewArrayInfo(structInfo, 5)
- WHEN the function returns
- THEN error is nil
- AND Details().(*ArrayDetails).Child.InternalType() equals TYPE_STRUCT
- AND Details().(*ArrayDetails).Size equals 5

### Requirement: STRUCT TypeInfo Constructor

The package SHALL provide NewStructInfo(firstEntry StructEntry, others ...StructEntry) to create struct type metadata with field uniqueness validation.

#### Scenario: Valid simple STRUCT
- GIVEN entry1 with name="id" and type INTEGER
- AND entry2 with name="name" and type VARCHAR
- AND NewStructInfo(entry1, entry2)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_STRUCT
- AND Details().(*StructDetails).Entries has length 2
- AND first entry Name() equals "id"
- AND second entry Name() equals "name"

#### Scenario: Invalid STRUCT with duplicate field names
- GIVEN entry1 with name="x" and type INTEGER
- AND entry2 with name="x" and type VARCHAR
- AND NewStructInfo(entry1, entry2)
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrDuplicateFieldName
- AND error message contains "x"

#### Scenario: STRUCT with special character field names
- GIVEN entry with name="field-name" (hyphen)
- AND NewStructInfo(entry)
- WHEN the function returns
- THEN error is nil
- AND entry Name() equals "field-name"

#### Scenario: Nested STRUCT
- GIVEN innerStruct from NewStructInfo(...)
- AND outerEntry with name="nested" and type innerStruct
- AND outerStruct from NewStructInfo(outerEntry)
- WHEN the function returns
- THEN error is nil
- AND outerStruct Details().(*StructDetails).Entries[0].Info().InternalType() equals TYPE_STRUCT

### Requirement: MAP TypeInfo Constructor

The package SHALL provide NewMapInfo(keyInfo, valueInfo TypeInfo) to create map type metadata with key comparability validation.

#### Scenario: Valid MAP(VARCHAR, INTEGER)
- GIVEN varcharInfo from NewTypeInfo(TYPE_VARCHAR)
- AND intInfo from NewTypeInfo(TYPE_INTEGER)
- AND NewMapInfo(varcharInfo, intInfo)
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_MAP
- AND Details().(*MapDetails).Key equals varcharInfo
- AND Details().(*MapDetails).Value equals intInfo

#### Scenario: Invalid MAP with nil key type
- GIVEN varcharInfo from NewTypeInfo(TYPE_VARCHAR)
- AND NewMapInfo(nil, varcharInfo)
- WHEN the function returns
- THEN error is not nil
- AND error message contains "keyInfo cannot be nil" OR similar

#### Scenario: Invalid MAP with nil value type
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND NewMapInfo(intInfo, nil)
- WHEN the function returns
- THEN error is not nil
- AND error message contains "valueInfo cannot be nil" OR similar

#### Scenario: MAP key comparability NOT validated at construction
- GIVEN listInfo from NewListInfo(intInfo) (non-comparable type)
- AND varcharInfo from NewTypeInfo(TYPE_VARCHAR)
- AND NewMapInfo(listInfo, varcharInfo)
- WHEN the function returns
- THEN error is nil (validation deferred to query execution)
- AND TypeInfo is valid MAP type

#### Scenario: Valid MAP with complex value type
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND structInfo from NewStructInfo(...)
- AND NewMapInfo(intInfo, structInfo)
- WHEN the function returns
- THEN error is nil
- AND Details().(*MapDetails).Value.InternalType() equals TYPE_STRUCT

### Requirement: UNION TypeInfo Constructor

The package SHALL provide NewUnionInfo(memberTypes []TypeInfo, memberNames []string) to create union type metadata with member uniqueness validation.

#### Scenario: Valid simple UNION
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND varcharInfo from NewTypeInfo(TYPE_VARCHAR)
- AND NewUnionInfo([]TypeInfo{intInfo, varcharInfo}, []string{"num", "str"})
- WHEN the function returns
- THEN error is nil
- AND TypeInfo InternalType() equals TYPE_UNION
- AND Details().(*UnionDetails).Members has length 2
- AND first member Name equals "num" and Type equals intInfo
- AND second member Name equals "str" and Type equals varcharInfo

#### Scenario: Invalid UNION with length mismatch
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND NewUnionInfo([]TypeInfo{intInfo}, []string{"a", "b"})
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrInvalidUnionParams
- AND error message contains "length" OR error message contains "mismatch"

#### Scenario: Invalid UNION with duplicate member names
- GIVEN intInfo from NewTypeInfo(TYPE_INTEGER)
- AND floatInfo from NewTypeInfo(TYPE_FLOAT)
- AND NewUnionInfo([]TypeInfo{intInfo, floatInfo}, []string{"x", "x"})
- WHEN the function returns
- THEN error is not nil
- AND error wraps ErrDuplicateFieldName OR error contains "duplicate"

#### Scenario: UNION with complex member types
- GIVEN structInfo from NewStructInfo(...)
- AND listInfo from NewListInfo(intInfo)
- AND NewUnionInfo([]TypeInfo{structInfo, listInfo}, []string{"struct_var", "list_var"})
- WHEN the function returns
- THEN error is nil
- AND first member Type InternalType() equals TYPE_STRUCT
- AND second member Type InternalType() equals TYPE_LIST

### DEFERRED REQUIREMENT: TypeInfo Serialization (P0-1b)

**NOTE**: TypeInfo JSON serialization is DEFERRED to the "DuckDB Binary Serialization (P0-1b)" proposal. This Core TypeInfo proposal focuses exclusively on API interface, construction, and validation.

The serialization requirement and scenarios have been moved to P0-1b scope.

### Requirement: Statement TypeInfo Introspection

The package SHALL provide Stmt.ColumnTypeInfo(n int) to retrieve column type metadata from prepared statements.

#### Scenario: Get column TypeInfo from SELECT
- GIVEN prepared statement "SELECT id, name FROM users WHERE id = ?"
- AND column 0 is INTEGER type
- AND column 1 is VARCHAR type
- WHEN calling ColumnTypeInfo(0)
- THEN result InternalType() equals TYPE_INTEGER
- WHEN calling ColumnTypeInfo(1)
- THEN result InternalType() equals TYPE_VARCHAR

#### Scenario: Get DECIMAL column TypeInfo
- GIVEN prepared statement "SELECT price FROM products"
- AND price column is DECIMAL(18, 4)
- WHEN calling ColumnTypeInfo(0)
- THEN result InternalType() equals TYPE_DECIMAL
- AND result Details().(*DecimalDetails).Width equals 18
- AND result Details().(*DecimalDetails).Scale equals 4

#### Scenario: Get complex type column TypeInfo
- GIVEN prepared statement "SELECT tags FROM articles"
- AND tags column is LIST(VARCHAR)
- WHEN calling ColumnTypeInfo(0)
- THEN result InternalType() equals TYPE_LIST
- AND result Details().(*ListDetails).Child InternalType() equals TYPE_VARCHAR

#### Scenario: ColumnTypeInfo out of bounds
- GIVEN prepared statement with 2 columns
- WHEN calling ColumnTypeInfo(2)
- THEN error is not nil
- AND error indicates out of range

### Requirement: UDF TypeInfo Integration

The package SHALL update UDF configuration types to use TypeInfo instead of Type for enhanced type validation.

#### Scenario: ScalarFuncConfig with TypeInfo
- GIVEN ScalarFuncConfig with InputTypeInfos []TypeInfo
- AND ResultTypeInfo TypeInfo
- WHEN constructing scalar UDF
- THEN UDF uses TypeInfo for type validation

#### Scenario: TableFunctionConfig with TypeInfo
- GIVEN ColumnInfo struct with T TypeInfo field
- WHEN constructing table UDF
- THEN UDF uses TypeInfo for column metadata

#### Scenario: AggregateFuncConfig with TypeInfo
- GIVEN AggregateFuncConfig with InputTypeInfos, StateTypeInfo, ResultTypeInfo as TypeInfo
- WHEN constructing aggregate UDF
- THEN UDF uses TypeInfo for type validation

### DEFERRED REQUIREMENT: Catalog TypeInfo Persistence (P0-1b)

**NOTE**: Catalog TypeInfo persistence and serialization are DEFERRED to the "DuckDB Binary Serialization (P0-1b)" proposal. This Core TypeInfo proposal supports in-memory catalog tracking only.

The Core TypeInfo implementation SHALL:
- Track TypeInfo in-memory for table columns (no file persistence)
- Support Stmt.ColumnTypeInfo() introspection from in-memory catalog
- Defer catalog serialization/deserialization to P0-1b

## Dependencies

This spec delta depends on:
- Existing `Type` enumeration in type_enum.go
- Existing error system in errors.go
- Existing catalog structure in internal/catalog/
- Existing backend interface in backend.go
- Existing statement API in prepared.go
- Existing UDF system in scalar_udf.go, table_udf.go, aggregate_udf.go

This spec delta enables:
- Statement introspection (ColumnTypeInfo API)
- Enhanced UDF type validation
- Complex type operations in executor
- Arrow type mapping with detailed metadata

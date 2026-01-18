// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This file defines catalog constants for DuckDB's
// BinarySerializer format used in checkpoint/catalog serialization.
package duckdb

// BinarySerializer property IDs for catalog entries.
// These match DuckDB's checkpoint serialization format.
const (
	// Catalog entry property IDs
	PropCatalogType      uint16 = 99  // CatalogType enum value
	PropCatalogEntryData uint16 = 100 // Entry-specific data (nested object)

	// Table entry additional properties (outside the CreateTableInfo object)
	PropTablePointer  uint16 = 101 // MetaBlockPointer to table data
	PropTableRowCount uint16 = 102 // Total row count
)

// CreateInfo property IDs (shared by all catalog entries).
// Note: These are for the BinarySerializer format and may overlap with
// the old custom format property IDs defined elsewhere.
const (
	PropCreateType       uint16 = 100 // Entry type string ("schema", "table", etc.)
	PropCreateCatalog    uint16 = 101 // Catalog name
	PropCreateSchema     uint16 = 102 // Schema name
	PropCreateTemporary  uint16 = 103 // Temporary flag (bool)
	PropCreateInternal   uint16 = 104 // Internal flag (bool)
	PropCreateOnConflict uint16 = 105 // OnCreateConflict enum
	PropCreateComment    uint16 = 106 // Comment string (optional)
	PropCreateTags       uint16 = 107 // Tags map (optional)
)

// CreateTableInfo property IDs.
const (
	PropTableInfoName        uint16 = 200 // Table name (table-specific field)
	PropTableInfoColumns     uint16 = 201 // Column definitions list
	PropTableInfoConstraints uint16 = 202 // Constraints list
)

// ColumnDefinition property IDs for BinarySerializer format.
const (
	PropColumnDefName        uint16 = 100 // Column name
	PropColumnDefType        uint16 = 101 // LogicalType (nested object)
	PropColumnDefExpression  uint16 = 102 // Generated column expression (optional)
	PropColumnDefCategory    uint16 = 103 // Column category enum
	PropColumnDefCompression uint16 = 104 // Compression type (optional)
	PropColumnDefComment     uint16 = 105 // Column comment (optional)
	PropColumnDefTags        uint16 = 106 // Column tags (optional)
)

// LogicalType property IDs for BinarySerializer format.
// Note: DuckDB does NOT serialize physical_type - it's derived from type_id at runtime.
const (
	PropLogicalTypeID   uint16 = 100 // LogicalTypeId enum
	PropLogicalTypeInfo uint16 = 101 // ExtraTypeInfo (type-specific, optional, nullable)
)

// ColumnCategory enum values.
const (
	ColumnCategoryStandard  uint8 = 0 // STANDARD
	ColumnCategoryGenerated uint8 = 1 // GENERATED
)

// PhysicalTypeID represents the physical storage type.
// Used in LogicalType serialization.
type PhysicalTypeID uint8

const (
	PhysicalTypeBool     PhysicalTypeID = 1   // BOOL
	PhysicalTypeInt8     PhysicalTypeID = 2   // INT8
	PhysicalTypeInt16    PhysicalTypeID = 3   // INT16
	PhysicalTypeInt32    PhysicalTypeID = 4   // INT32
	PhysicalTypeInt64    PhysicalTypeID = 5   // INT64
	PhysicalTypeUInt8    PhysicalTypeID = 6   // UINT8
	PhysicalTypeUInt16   PhysicalTypeID = 7   // UINT16
	PhysicalTypeUInt32   PhysicalTypeID = 8   // UINT32
	PhysicalTypeUInt64   PhysicalTypeID = 9   // UINT64
	PhysicalTypeInt128   PhysicalTypeID = 10  // INT128
	PhysicalTypeUInt128  PhysicalTypeID = 11  // UINT128
	PhysicalTypeFloat    PhysicalTypeID = 12  // FLOAT
	PhysicalTypeDouble   PhysicalTypeID = 13  // DOUBLE
	PhysicalTypeInterval PhysicalTypeID = 14  // INTERVAL
	PhysicalTypeVarchar  PhysicalTypeID = 15  // VARCHAR
	PhysicalTypeBit      PhysicalTypeID = 16  // BIT
	PhysicalTypeStruct   PhysicalTypeID = 17  // STRUCT
	PhysicalTypeList     PhysicalTypeID = 18  // LIST
	PhysicalTypeArray    PhysicalTypeID = 19  // ARRAY
	PhysicalTypeInvalid  PhysicalTypeID = 255 // INVALID
)

// physicalTypeNames maps PhysicalTypeID values to their string names.
var physicalTypeNames = map[PhysicalTypeID]string{
	PhysicalTypeBool:     "BOOL",
	PhysicalTypeInt8:     "INT8",
	PhysicalTypeInt16:    "INT16",
	PhysicalTypeInt32:    "INT32",
	PhysicalTypeInt64:    "INT64",
	PhysicalTypeUInt8:    "UINT8",
	PhysicalTypeUInt16:   "UINT16",
	PhysicalTypeUInt32:   "UINT32",
	PhysicalTypeUInt64:   "UINT64",
	PhysicalTypeInt128:   "INT128",
	PhysicalTypeUInt128:  "UINT128",
	PhysicalTypeFloat:    "FLOAT",
	PhysicalTypeDouble:   "DOUBLE",
	PhysicalTypeInterval: "INTERVAL",
	PhysicalTypeVarchar:  "VARCHAR",
	PhysicalTypeBit:      "BIT",
	PhysicalTypeStruct:   "STRUCT",
	PhysicalTypeList:     "LIST",
	PhysicalTypeArray:    "ARRAY",
	PhysicalTypeInvalid:  strInvalid,
}

// String returns the string representation of a PhysicalTypeID.
func (p PhysicalTypeID) String() string {
	if name, ok := physicalTypeNames[p]; ok {
		return name
	}

	return strUnknown
}

// GetPhysicalType returns the physical type for a given logical type.
func GetPhysicalType(logicalType LogicalTypeID) PhysicalTypeID {
	switch logicalType {
	case TypeBoolean:
		return PhysicalTypeBool
	case TypeTinyInt:
		return PhysicalTypeInt8
	case TypeSmallInt:
		return PhysicalTypeInt16
	case TypeInteger:
		return PhysicalTypeInt32
	case TypeBigInt:
		return PhysicalTypeInt64
	case TypeUTinyInt:
		return PhysicalTypeUInt8
	case TypeUSmallInt:
		return PhysicalTypeUInt16
	case TypeUInteger:
		return PhysicalTypeUInt32
	case TypeUBigInt:
		return PhysicalTypeUInt64
	case TypeHugeInt:
		return PhysicalTypeInt128
	case TypeUHugeInt:
		return PhysicalTypeUInt128
	case TypeFloat:
		return PhysicalTypeFloat
	case TypeDouble:
		return PhysicalTypeDouble
	case TypeDate,
		TypeTime,
		TypeTimestamp,
		TypeTimestampS,
		TypeTimestampMS,
		TypeTimestampNS,
		TypeTimestampTZ:
		return PhysicalTypeInt64
	case TypeInterval:
		return PhysicalTypeInterval
	case TypeVarchar, TypeBlob, TypeChar:
		return PhysicalTypeVarchar
	case TypeDecimal:
		// Depends on width, but default to int64 for small decimals
		return PhysicalTypeInt64
	case TypeStruct:
		return PhysicalTypeStruct
	case TypeList:
		return PhysicalTypeList
	case TypeArray:
		return PhysicalTypeArray
	default:
		return PhysicalTypeInvalid
	}
}

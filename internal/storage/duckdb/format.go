// Package duckdb provides support for reading and writing DuckDB's native
// binary file format. This enables bidirectional compatibility with DuckDB
// databases, allowing dukdb-go to read .duckdb files created by DuckDB and
// write files that DuckDB can read.
//
// The package implements DuckDB's storage format version 67 (DuckDB v1.4.3).
// Key features:
//   - Dual-header design for crash recovery
//   - 256KB block-based I/O with checksums
//   - Columnar storage with compression
//   - Catalog metadata serialization
//
// This is a pure Go implementation with zero CGO dependencies.
package duckdb

// String constants used across type String() methods.
const (
	strInvalid = "INVALID"
	strUnknown = "UNKNOWN"
)

// File format constants matching DuckDB v1.4.3 storage format.
const (
	// MagicBytes identifies a valid DuckDB file (stored at offset 8 in file header).
	MagicBytes = "DUCK"

	// MagicByteOffset is the file offset where magic bytes are located.
	MagicByteOffset = 8

	// CurrentVersion is the storage format version for DuckDB v1.4.3.
	CurrentVersion uint64 = 67

	// FileHeaderSize is the size of the file header block in bytes.
	FileHeaderSize = 4096

	// DatabaseHeaderSize is the size of each database header block in bytes.
	DatabaseHeaderSize = 4096

	// DefaultBlockSize is the default block allocation size (256KB).
	DefaultBlockSize uint64 = 262144

	// DefaultVectorSize is the default number of rows per vector.
	DefaultVectorSize uint64 = 2048

	// DefaultRowGroupSize is the default number of rows per row group.
	DefaultRowGroupSize uint64 = 122880

	// DatabaseHeader1Offset is the file offset of the first database header.
	// The checksum is stored at this offset, data starts at DatabaseHeader1Offset + BlockChecksumSize.
	DatabaseHeader1Offset = 4096

	// DatabaseHeader2Offset is the file offset of the second database header.
	// The checksum is stored at this offset, data starts at DatabaseHeader2Offset + BlockChecksumSize.
	DatabaseHeader2Offset = 8192

	// DataBlocksOffset is the file offset where data blocks begin (after headers).
	DataBlocksOffset = 12288

	// BlockChecksumSize is the size of the checksum at the beginning of each block.
	BlockChecksumSize = 8

	// EncryptedHeaderSize is the maximum header size for encrypted blocks.
	EncryptedHeaderSize = 40
)

// CompressionType represents the compression algorithm used for column data.
// Values match DuckDB's compression_type.hpp definitions.
type CompressionType uint8

// Compression type constants matching DuckDB's compression algorithms.
const (
	CompressionAuto         CompressionType = 0  // AUTO - automatic selection
	CompressionUncompressed CompressionType = 1  // UNCOMPRESSED - raw data
	CompressionConstant     CompressionType = 2  // CONSTANT - single value repeated
	CompressionRLE          CompressionType = 3  // RLE - run-length encoding
	CompressionDictionary   CompressionType = 4  // DICTIONARY - dictionary compression
	CompressionPFORDelta    CompressionType = 5  // PFOR_DELTA - packed FOR with delta
	CompressionBitPacking   CompressionType = 6  // BITPACKING - bit-packed integers
	CompressionFSST         CompressionType = 7  // FSST - fast static symbol table
	CompressionCHIMP        CompressionType = 8  // CHIMP - time series float compression
	CompressionPATAS        CompressionType = 9  // PATAS - time series compression
	CompressionALP          CompressionType = 10 // ALP - adaptive lossless float
	CompressionALPRD        CompressionType = 11 // ALPRD - ALP with real doubles
	CompressionZSTD         CompressionType = 12 // ZSTD - Zstandard compression
	CompressionRoaring      CompressionType = 13 // ROARING - roaring bitmap
	CompressionEmpty        CompressionType = 14 // EMPTY - internal empty marker
	CompressionDictFSST     CompressionType = 15 // DICT_FSST - dictionary + FSST
)

// compressionTypeNames maps CompressionType values to their string names.
var compressionTypeNames = map[CompressionType]string{
	CompressionAuto:         "AUTO",
	CompressionUncompressed: "UNCOMPRESSED",
	CompressionConstant:     "CONSTANT",
	CompressionRLE:          "RLE",
	CompressionDictionary:   "DICTIONARY",
	CompressionPFORDelta:    "PFOR_DELTA",
	CompressionBitPacking:   "BITPACKING",
	CompressionFSST:         "FSST",
	CompressionCHIMP:        "CHIMP",
	CompressionPATAS:        "PATAS",
	CompressionALP:          "ALP",
	CompressionALPRD:        "ALPRD",
	CompressionZSTD:         "ZSTD",
	CompressionRoaring:      "ROARING",
	CompressionEmpty:        "EMPTY",
	CompressionDictFSST:     "DICT_FSST",
}

// String returns the string representation of a CompressionType.
func (c CompressionType) String() string {
	if name, ok := compressionTypeNames[c]; ok {
		return name
	}

	return strUnknown
}

// BlockType represents the type of data stored in a block.
type BlockType uint8

// Block type constants.
const (
	BlockInvalid  BlockType = 0 // Invalid/uninitialized block
	BlockMetaData BlockType = 1 // Catalog metadata
	BlockRowGroup BlockType = 2 // Table data (row groups)
	BlockFreeList BlockType = 3 // Free block tracking
	BlockIndex    BlockType = 4 // Index data
)

// blockTypeNames maps BlockType values to their string names.
var blockTypeNames = map[BlockType]string{
	BlockInvalid:  strInvalid,
	BlockMetaData: "METADATA",
	BlockRowGroup: "ROWGROUP",
	BlockFreeList: "FREELIST",
	BlockIndex:    "INDEX",
}

// String returns the string representation of a BlockType.
func (b BlockType) String() string {
	if name, ok := blockTypeNames[b]; ok {
		return name
	}

	return strUnknown
}

// BitpackingMode represents the internal mode for BITPACKING compression.
type BitpackingMode uint8

// Bitpacking mode constants.
const (
	BitpackingAuto          BitpackingMode = 0 // AUTO - choose best mode
	BitpackingConstant      BitpackingMode = 1 // All values are the same
	BitpackingConstantDelta BitpackingMode = 2 // Constant difference between values
	BitpackingDeltaFOR      BitpackingMode = 3 // Delta + Frame of Reference
	BitpackingFOR           BitpackingMode = 4 // Frame of Reference only
)

// bitpackingModeNames maps BitpackingMode values to their string names.
var bitpackingModeNames = map[BitpackingMode]string{
	BitpackingAuto:          "AUTO",
	BitpackingConstant:      "CONSTANT",
	BitpackingConstantDelta: "CONSTANT_DELTA",
	BitpackingDeltaFOR:      "DELTA_FOR",
	BitpackingFOR:           "FOR",
}

// String returns the string representation of a BitpackingMode.
func (m BitpackingMode) String() string {
	if name, ok := bitpackingModeNames[m]; ok {
		return name
	}

	return strUnknown
}

// CatalogType represents the type of catalog entry.
// Values match DuckDB's catalog_entry_type.hpp definitions.
type CatalogType uint8

// Catalog type constants.
const (
	CatalogInvalid                CatalogType = 0   // INVALID
	CatalogTableEntry             CatalogType = 1   // TABLE_ENTRY
	CatalogSchemaEntry            CatalogType = 2   // SCHEMA_ENTRY
	CatalogViewEntry              CatalogType = 3   // VIEW_ENTRY
	CatalogIndexEntry             CatalogType = 4   // INDEX_ENTRY
	CatalogPreparedStatement      CatalogType = 5   // PREPARED_STATEMENT
	CatalogSequenceEntry          CatalogType = 6   // SEQUENCE_ENTRY
	CatalogCollationEntry         CatalogType = 7   // COLLATION_ENTRY
	CatalogTypeEntry              CatalogType = 8   // TYPE_ENTRY
	CatalogDatabaseEntry          CatalogType = 9   // DATABASE_ENTRY
	CatalogTableFunctionEntry     CatalogType = 25  // TABLE_FUNCTION_ENTRY
	CatalogScalarFunctionEntry    CatalogType = 26  // SCALAR_FUNCTION_ENTRY
	CatalogAggregateFunctionEntry CatalogType = 27  // AGGREGATE_FUNCTION_ENTRY
	CatalogPragmaFunctionEntry    CatalogType = 28  // PRAGMA_FUNCTION_ENTRY
	CatalogCopyFunctionEntry      CatalogType = 29  // COPY_FUNCTION_ENTRY
	CatalogMacroEntry             CatalogType = 30  // MACRO_ENTRY
	CatalogTableMacroEntry        CatalogType = 31  // TABLE_MACRO_ENTRY
	CatalogDeletedEntry           CatalogType = 51  // DELETED_ENTRY
	CatalogRenamedEntry           CatalogType = 52  // RENAMED_ENTRY
	CatalogSecretEntry            CatalogType = 71  // SECRET_ENTRY
	CatalogSecretTypeEntry        CatalogType = 72  // SECRET_TYPE_ENTRY
	CatalogSecretFunctionEntry    CatalogType = 73  // SECRET_FUNCTION_ENTRY
	CatalogDependencyEntry        CatalogType = 100 // DEPENDENCY_ENTRY
)

// catalogTypeNames maps CatalogType values to their string names.
var catalogTypeNames = map[CatalogType]string{
	CatalogInvalid:                strInvalid,
	CatalogTableEntry:             "TABLE",
	CatalogSchemaEntry:            "SCHEMA",
	CatalogViewEntry:              "VIEW",
	CatalogIndexEntry:             "INDEX",
	CatalogPreparedStatement:      "PREPARED_STATEMENT",
	CatalogSequenceEntry:          "SEQUENCE",
	CatalogCollationEntry:         "COLLATION",
	CatalogTypeEntry:              "TYPE",
	CatalogDatabaseEntry:          "DATABASE",
	CatalogTableFunctionEntry:     "TABLE_FUNCTION",
	CatalogScalarFunctionEntry:    "SCALAR_FUNCTION",
	CatalogAggregateFunctionEntry: "AGGREGATE_FUNCTION",
	CatalogPragmaFunctionEntry:    "PRAGMA_FUNCTION",
	CatalogCopyFunctionEntry:      "COPY_FUNCTION",
	CatalogMacroEntry:             "MACRO",
	CatalogTableMacroEntry:        "TABLE_MACRO",
	CatalogDeletedEntry:           "DELETED",
	CatalogRenamedEntry:           "RENAMED",
	CatalogSecretEntry:            "SECRET",
	CatalogSecretTypeEntry:        "SECRET_TYPE",
	CatalogSecretFunctionEntry:    "SECRET_FUNCTION",
	CatalogDependencyEntry:        "DEPENDENCY",
}

// String returns the string representation of a CatalogType.
func (c CatalogType) String() string {
	if name, ok := catalogTypeNames[c]; ok {
		return name
	}

	return strUnknown
}

// LogicalTypeID represents DuckDB's logical type identifiers.
// Values match DuckDB's types.hpp definitions.
type LogicalTypeID uint8

// Logical type ID constants matching DuckDB's type system.
const (
	// Special/Internal types (0-9)
	TypeInvalid  LogicalTypeID = 0 // INVALID
	TypeSQLNull  LogicalTypeID = 1 // SQLNULL
	TypeUnknown  LogicalTypeID = 2 // UNKNOWN
	TypeAny      LogicalTypeID = 3 // ANY (for function overloading)
	TypeUser     LogicalTypeID = 4 // USER (user-defined type reference)
	TypeTemplate LogicalTypeID = 5 // TEMPLATE

	// Core types (10-39)
	TypeBoolean        LogicalTypeID = 10 // BOOLEAN
	TypeTinyInt        LogicalTypeID = 11 // TINYINT
	TypeSmallInt       LogicalTypeID = 12 // SMALLINT
	TypeInteger        LogicalTypeID = 13 // INTEGER
	TypeBigInt         LogicalTypeID = 14 // BIGINT
	TypeDate           LogicalTypeID = 15 // DATE
	TypeTime           LogicalTypeID = 16 // TIME
	TypeTimestampS     LogicalTypeID = 17 // TIMESTAMP_SEC
	TypeTimestampMS    LogicalTypeID = 18 // TIMESTAMP_MS
	TypeTimestamp      LogicalTypeID = 19 // TIMESTAMP
	TypeTimestampNS    LogicalTypeID = 20 // TIMESTAMP_NS
	TypeDecimal        LogicalTypeID = 21 // DECIMAL
	TypeFloat          LogicalTypeID = 22 // FLOAT
	TypeDouble         LogicalTypeID = 23 // DOUBLE
	TypeChar           LogicalTypeID = 24 // CHAR (fixed-length)
	TypeVarchar        LogicalTypeID = 25 // VARCHAR
	TypeBlob           LogicalTypeID = 26 // BLOB
	TypeInterval       LogicalTypeID = 27 // INTERVAL
	TypeUTinyInt       LogicalTypeID = 28 // UTINYINT
	TypeUSmallInt      LogicalTypeID = 29 // USMALLINT
	TypeUInteger       LogicalTypeID = 30 // UINTEGER
	TypeUBigInt        LogicalTypeID = 31 // UBIGINT
	TypeTimestampTZ    LogicalTypeID = 32 // TIMESTAMP_TZ
	TypeTimeTZ         LogicalTypeID = 33 // TIME_TZ
	TypeTimeNS         LogicalTypeID = 35 // TIME_NS (nanosecond precision time)
	TypeBit            LogicalTypeID = 36 // BIT
	TypeStringLiteral  LogicalTypeID = 37 // STRING_LITERAL (parsing)
	TypeIntegerLiteral LogicalTypeID = 38 // INTEGER_LITERAL (parsing)
	TypeBigNum         LogicalTypeID = 39 // BIGNUM (arbitrary precision)

	// Large integer types (50-54)
	TypeHugeInt  LogicalTypeID = 50 // HUGEINT
	TypeUHugeInt LogicalTypeID = 51 // UHUGEINT
	TypePointer  LogicalTypeID = 52 // POINTER (internal)
	TypeValidity LogicalTypeID = 53 // VALIDITY (internal)
	TypeUUID     LogicalTypeID = 54 // UUID

	// Complex/Nested types (100-110)
	TypeStruct         LogicalTypeID = 100 // STRUCT
	TypeList           LogicalTypeID = 101 // LIST
	TypeMap            LogicalTypeID = 102 // MAP
	TypeTable          LogicalTypeID = 103 // TABLE
	TypeEnum           LogicalTypeID = 104 // ENUM
	TypeAggregateState LogicalTypeID = 105 // AGGREGATE_STATE
	TypeLambda         LogicalTypeID = 106 // LAMBDA
	TypeUnion          LogicalTypeID = 107 // UNION
	TypeArray          LogicalTypeID = 108 // ARRAY (fixed-size)
	TypeGeometry       LogicalTypeID = 109 // GEOMETRY
	TypeVariant        LogicalTypeID = 110 // VARIANT
)

// logicalTypeNames maps LogicalTypeID values to their string names.
var logicalTypeNames = map[LogicalTypeID]string{
	TypeInvalid:        strInvalid,
	TypeSQLNull:        "SQLNULL",
	TypeUnknown:        strUnknown,
	TypeAny:            "ANY",
	TypeUser:           "USER",
	TypeTemplate:       "TEMPLATE",
	TypeBoolean:        "BOOLEAN",
	TypeTinyInt:        "TINYINT",
	TypeSmallInt:       "SMALLINT",
	TypeInteger:        "INTEGER",
	TypeBigInt:         "BIGINT",
	TypeDate:           "DATE",
	TypeTime:           "TIME",
	TypeTimestampS:     "TIMESTAMP_S",
	TypeTimestampMS:    "TIMESTAMP_MS",
	TypeTimestamp:      "TIMESTAMP",
	TypeTimestampNS:    "TIMESTAMP_NS",
	TypeDecimal:        "DECIMAL",
	TypeFloat:          "FLOAT",
	TypeDouble:         "DOUBLE",
	TypeChar:           "CHAR",
	TypeVarchar:        "VARCHAR",
	TypeBlob:           "BLOB",
	TypeInterval:       "INTERVAL",
	TypeUTinyInt:       "UTINYINT",
	TypeUSmallInt:      "USMALLINT",
	TypeUInteger:       "UINTEGER",
	TypeUBigInt:        "UBIGINT",
	TypeTimestampTZ:    "TIMESTAMP_TZ",
	TypeTimeTZ:         "TIME_TZ",
	TypeTimeNS:         "TIME_NS",
	TypeBit:            "BIT",
	TypeStringLiteral:  "STRING_LITERAL",
	TypeIntegerLiteral: "INTEGER_LITERAL",
	TypeBigNum:         "BIGNUM",
	TypeHugeInt:        "HUGEINT",
	TypeUHugeInt:       "UHUGEINT",
	TypePointer:        "POINTER",
	TypeValidity:       "VALIDITY",
	TypeUUID:           "UUID",
	TypeStruct:         "STRUCT",
	TypeList:           "LIST",
	TypeMap:            "MAP",
	TypeTable:          "TABLE",
	TypeEnum:           "ENUM",
	TypeAggregateState: "AGGREGATE_STATE",
	TypeLambda:         "LAMBDA",
	TypeUnion:          "UNION",
	TypeArray:          "ARRAY",
	TypeGeometry:       "GEOMETRY",
	TypeVariant:        "VARIANT",
}

// String returns the string representation of a LogicalTypeID.
func (t LogicalTypeID) String() string {
	if name, ok := logicalTypeNames[t]; ok {
		return name
	}

	return strUnknown
}

// Property IDs for base CreateInfo fields (common to all catalog entries).
const (
	PropCatalog      = 100 // Catalog name
	PropSchema       = 101 // Schema name
	PropTemporary    = 102 // Is temporary object
	PropInternal     = 103 // Is internal/system object
	PropOnConflict   = 104 // Conflict resolution
	PropSQL          = 105 // Original SQL statement
	PropComment      = 106 // User comment
	PropTags         = 107 // User-defined tags
	PropDependencies = 108 // Object dependencies
)

// Property IDs for TableCatalogEntry.
const (
	PropTableColumns     = 200 // Table columns
	PropTableConstraints = 201 // Table constraints
	PropTableStorage     = 202 // Points to MetadataManager for row groups
)

// Package server provides PostgreSQL wire protocol server functionality.
// This file implements column metadata builders for row descriptions.
package server

import (
	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

// Type size constants in bytes for PostgreSQL wire protocol.
const (
	// Fixed-width type sizes.
	typeSizeBool     int16 = 1
	typeSizeInt2     int16 = 2
	typeSizeInt4     int16 = 4
	typeSizeInt8     int16 = 8
	typeSizeFloat4   int16 = 4
	typeSizeFloat8   int16 = 8
	typeSizeDate     int16 = 4  // Days since 2000-01-01
	typeSizeTime     int16 = 8  // Microseconds
	typeSizeInterval int16 = 16 // 8 bytes time + 4 bytes day + 4 bytes month
	typeSizeUUID     int16 = 16
	typeSizeChar     int16 = 1
	typeSizeName     int16 = 64 // NAMEDATALEN in PostgreSQL
	typeSizeVariable int16 = -1 // Variable length types
)

// Modifier constants for PostgreSQL type system.
const (
	// VARHDRSZ is the header size for variable-length types (used in type modifiers).
	varhdrsz int32 = 4
	// numericModShift is the bit shift for precision in NUMERIC modifier.
	numericModShift int32 = 16
)

// ColumnBuilder provides a fluent API for building wire.Column instances
// with full PostgreSQL row description metadata.
//
// PostgreSQL RowDescription format includes per-column:
//   - Name: Column name (string)
//   - TableOID: OID of the containing table (0 if not from a table)
//   - AttrNo: Column attribute number in the table (0 if not from a table)
//   - TypeOID: Data type OID
//   - TypeSize: Data type size in bytes (-1 for variable length)
//   - TypeModifier: Type-specific modifier (e.g., precision for numeric, -1 if n/a)
//   - Format: 0 for text format, 1 for binary format
type ColumnBuilder struct {
	name         string
	tableOid     uint32
	columnNumber int16
	typeOid      uint32
	typeSize     int16
	typeMod      int32
	format       int16
}

// NewColumnBuilder creates a new ColumnBuilder with the given column name.
func NewColumnBuilder(name string) *ColumnBuilder {
	return &ColumnBuilder{
		name:         name,
		tableOid:     0,              // Not from a table by default
		columnNumber: 0,              // Not from a table by default
		typeOid:      types.OID_TEXT, // Default to TEXT
		typeSize:     -1,             // Variable length by default
		typeMod:      -1,             // No modifier by default
		format:       0,              // Text format by default
	}
}

// Name sets the column name.
func (b *ColumnBuilder) Name(name string) *ColumnBuilder {
	b.name = name

	return b
}

// TableOID sets the OID of the containing table.
// Use 0 for computed columns or columns not from a table.
func (b *ColumnBuilder) TableOID(oid uint32) *ColumnBuilder {
	b.tableOid = oid

	return b
}

// ColumnNumber sets the column attribute number within the table.
// Use 0 for computed columns or columns not from a table.
func (b *ColumnBuilder) ColumnNumber(num int16) *ColumnBuilder {
	b.columnNumber = num

	return b
}

// TypeOID sets the PostgreSQL type OID for the column.
func (b *ColumnBuilder) TypeOID(oid uint32) *ColumnBuilder {
	b.typeOid = oid
	// Automatically set type size based on OID
	b.typeSize = TypeSize(oid)

	return b
}

// TypeSize explicitly sets the type size.
// Use -1 for variable length types.
func (b *ColumnBuilder) TypeSize(size int16) *ColumnBuilder {
	b.typeSize = size

	return b
}

// TypeModifier sets the type modifier (e.g., precision for NUMERIC).
// Use -1 if not applicable.
func (b *ColumnBuilder) TypeModifier(mod int32) *ColumnBuilder {
	b.typeMod = mod

	return b
}

// Format sets the data format (0 = text, 1 = binary).
func (b *ColumnBuilder) Format(format int16) *ColumnBuilder {
	b.format = format

	return b
}

// TextFormat sets the format to text (0).
func (b *ColumnBuilder) TextFormat() *ColumnBuilder {
	b.format = FormatText

	return b
}

// BinaryFormat sets the format to binary (1).
func (b *ColumnBuilder) BinaryFormat() *ColumnBuilder {
	b.format = FormatBinary

	return b
}

// Build creates a wire.Column from the builder configuration.
func (b *ColumnBuilder) Build() wire.Column {
	return wire.Column{
		Name:         b.name,
		Table:        int32(b.tableOid),
		AttrNo:       b.columnNumber,
		Oid:          b.typeOid,
		Width:        b.typeSize,
		TypeModifier: b.typeMod,
		// Format is handled at write time by the wire library
	}
}

// Format codes for PostgreSQL wire protocol.
const (
	FormatText   int16 = 0
	FormatBinary int16 = 1
)

// TypeSize returns the fixed size in bytes for a PostgreSQL type OID.
// Returns -1 for variable-length types.
//
//nolint:gocyclo // Switch on type OIDs is inherently verbose but clear
func TypeSize(oid uint32) int16 {
	switch oid {
	case types.OID_BOOL, types.OID_CHAR:
		return typeSizeBool
	case types.OID_INT2:
		return typeSizeInt2
	case types.OID_INT4, types.OID_OID, types.OID_DATE:
		return typeSizeInt4
	case types.OID_INT8,
		types.OID_TIME,
		types.OID_TIMETZ,
		types.OID_TIMESTAMP,
		types.OID_TIMESTAMPTZ:
		return typeSizeInt8
	case types.OID_FLOAT4:
		return typeSizeFloat4
	case types.OID_FLOAT8:
		return typeSizeFloat8
	case types.OID_INTERVAL, types.OID_UUID:
		return typeSizeInterval
	case types.OID_NAME:
		return typeSizeName
	default:
		// Variable length types: TEXT, VARCHAR, BYTEA, JSON, JSONB, NUMERIC, etc.
		return typeSizeVariable
	}
}

// BuildColumn is a convenience function to create a column with common settings.
func BuildColumn(name string, typeOid uint32) wire.Column {
	return NewColumnBuilder(name).TypeOID(typeOid).Build()
}

// BuildColumns creates multiple wire.Column instances from name/OID pairs.
func BuildColumns(cols ...struct {
	Name string
	Oid  uint32
}) wire.Columns {
	result := make(wire.Columns, len(cols))
	for i, col := range cols {
		result[i] = BuildColumn(col.Name, col.Oid)
	}

	return result
}

// ColumnsFromMetadata creates wire.Columns from column names and types.
// This is useful when creating columns from query result metadata.
func ColumnsFromMetadata(names []string, oids []uint32) wire.Columns {
	if len(names) != len(oids) {
		return nil
	}

	result := make(wire.Columns, len(names))
	for i := range names {
		result[i] = BuildColumn(names[i], oids[i])
	}

	return result
}

// ColumnWithModifier creates a column with a type modifier.
// Common modifiers:
//   - VARCHAR(n): modifier = n + 4 (VARHDRSZ)
//   - CHAR(n): modifier = n + 4
//   - NUMERIC(p,s): modifier = (p << 16) | s + 4
//   - BIT(n): modifier = n
func ColumnWithModifier(name string, typeOid uint32, modifier int32) wire.Column {
	return NewColumnBuilder(name).
		TypeOID(typeOid).
		TypeModifier(modifier).
		Build()
}

// VarcharColumn creates a VARCHAR(n) column with proper modifier.
// In PostgreSQL, VARCHAR modifier = length + VARHDRSZ (4).
func VarcharColumn(name string, maxLen int32) wire.Column {
	modifier := maxLen + varhdrsz

	return ColumnWithModifier(name, types.OID_VARCHAR, modifier)
}

// NumericColumn creates a NUMERIC(precision, scale) column with proper modifier.
// In PostgreSQL, NUMERIC modifier encodes precision and scale.
func NumericColumn(name string, precision, scale int32) wire.Column {
	modifier := ((precision << numericModShift) | scale) + varhdrsz

	return ColumnWithModifier(name, types.OID_NUMERIC, modifier)
}

// ArrayTypeOID returns the array type OID for a given element type OID.
// Returns OID_UNKNOWN if no array type exists for the element type.
func ArrayTypeOID(elementOid uint32) uint32 {
	return types.GetArrayOID(elementOid)
}

// ElementTypeOID returns the element type OID for a given array type OID.
// Returns OID_UNKNOWN if the OID is not an array type.
func ElementTypeOID(arrayOid uint32) uint32 {
	return types.GetArrayElementOID(arrayOid)
}

// IsArrayType returns true if the OID represents an array type.
func IsArrayType(oid uint32) bool {
	return types.IsArrayOID(oid)
}

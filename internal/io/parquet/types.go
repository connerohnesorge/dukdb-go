// Package parquet provides Apache Parquet file reading and writing capabilities for dukdb-go.
// This file contains type mapping between Parquet and DuckDB types.
//
// The type mapping follows these principles:
// - Logical types take precedence over physical types
// - Integer types are mapped based on bit width and signedness
// - Timestamps are mapped based on precision and timezone
// - Nested types (LIST, MAP, STRUCT) are detected and serialized as VARCHAR (JSON)
// - Unsupported types fall back to VARCHAR or BLOB
package parquet

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// Bit width constants for integer type mapping.
const (
	bitWidth8  = 8
	bitWidth16 = 16
	bitWidth32 = 32
	bitWidth64 = 64
)

// parquetTypeToDuckDB maps a Parquet schema node to a DuckDB type.
// It handles both physical types and logical type annotations.
// For nested types (LIST, MAP, STRUCT), we return VARCHAR since they will
// be serialized as JSON strings during value conversion.
func parquetTypeToDuckDB(node parquet.Node) dukdb.Type {
	// Check for nested types first by examining the node structure.
	// Nested types are detected by checking if the node is a group with fields.
	if isNestedType(node) {
		// Nested types are serialized as JSON strings (VARCHAR).
		return dukdb.TYPE_VARCHAR
	}

	if node.Type() != nil {
		logicalType := node.Type().LogicalType()
		if logicalType != nil {
			return mapLogicalType(logicalType, node)
		}
	}

	return mapPhysicalType(node)
}

// isNestedType checks if a Parquet node represents a nested type (LIST, MAP, STRUCT).
// These are detected by:
// 1. Checking if the node is NOT a leaf (has child fields)
// 2. Checking logical type annotations (LIST, MAP)
// 3. Checking the node's repetition and structure.
func isNestedType(node parquet.Node) bool {
	// Check if this is a leaf node (primitive type).
	// Non-leaf nodes are groups (STRUCT, LIST, MAP).
	if !node.Leaf() {
		return true
	}

	// Check logical type annotations.
	if node.Type() != nil {
		lt := node.Type().LogicalType()
		if lt != nil && (lt.List != nil || lt.Map != nil) {
			return true
		}
	}

	// Check for repeated fields (which represent lists in Parquet).
	if node.Repeated() {
		return true
	}

	return false
}

// mapLogicalType maps a Parquet logical type to a DuckDB type.
// Logical types provide semantic meaning on top of physical types.
// For example, a BYTE_ARRAY with UTF8 logical type becomes VARCHAR.
func mapLogicalType(lt *format.LogicalType, node parquet.Node) dukdb.Type {
	switch {
	case lt.UTF8 != nil:
		return dukdb.TYPE_VARCHAR

	case lt.Integer != nil:
		return mapIntegerLogicalType(lt.Integer)

	case lt.Decimal != nil:
		return dukdb.TYPE_DECIMAL

	case lt.Date != nil:
		return dukdb.TYPE_DATE

	case lt.Time != nil:
		return dukdb.TYPE_TIME

	case lt.Timestamp != nil:
		return mapTimestampLogicalType(lt.Timestamp)

	case lt.UUID != nil:
		return dukdb.TYPE_UUID

	case lt.Enum != nil:
		return dukdb.TYPE_VARCHAR

	case lt.Json != nil:
		return dukdb.TYPE_VARCHAR

	case lt.Bson != nil:
		return dukdb.TYPE_BLOB

	case lt.List != nil:
		// LIST types are serialized as JSON strings.
		return dukdb.TYPE_VARCHAR

	case lt.Map != nil:
		// MAP types are serialized as JSON strings.
		return dukdb.TYPE_VARCHAR

	default:
		return mapPhysicalType(node)
	}
}

// mapIntegerLogicalType maps an integer logical type based on bit width and signedness.
func mapIntegerLogicalType(intType *format.IntType) dukdb.Type {
	if intType.IsSigned {
		return mapSignedIntegerType(intType.BitWidth)
	}

	return mapUnsignedIntegerType(intType.BitWidth)
}

// mapSignedIntegerType maps a signed integer bit width to a DuckDB type.
func mapSignedIntegerType(bitWidth int8) dukdb.Type {
	switch bitWidth {
	case bitWidth8:
		return dukdb.TYPE_TINYINT
	case bitWidth16:
		return dukdb.TYPE_SMALLINT
	case bitWidth32:
		return dukdb.TYPE_INTEGER
	case bitWidth64:
		return dukdb.TYPE_BIGINT
	default:
		return dukdb.TYPE_BIGINT
	}
}

// mapUnsignedIntegerType maps an unsigned integer bit width to a DuckDB type.
func mapUnsignedIntegerType(bitWidth int8) dukdb.Type {
	switch bitWidth {
	case bitWidth8:
		return dukdb.TYPE_UTINYINT
	case bitWidth16:
		return dukdb.TYPE_USMALLINT
	case bitWidth32:
		return dukdb.TYPE_UINTEGER
	case bitWidth64:
		return dukdb.TYPE_UBIGINT
	default:
		return dukdb.TYPE_UBIGINT
	}
}

// mapTimestampLogicalType maps timestamp logical types based on unit and timezone.
func mapTimestampLogicalType(tsType *format.TimestampType) dukdb.Type {
	if tsType.IsAdjustedToUTC {
		return dukdb.TYPE_TIMESTAMP_TZ
	}

	switch {
	case tsType.Unit.Millis != nil:
		return dukdb.TYPE_TIMESTAMP_MS
	case tsType.Unit.Micros != nil:
		return dukdb.TYPE_TIMESTAMP
	case tsType.Unit.Nanos != nil:
		return dukdb.TYPE_TIMESTAMP_NS
	default:
		return dukdb.TYPE_TIMESTAMP
	}
}

// mapPhysicalType maps a Parquet physical type to a DuckDB type.
// This is called when no logical type annotation is present.
// Physical types are the raw storage types used by Parquet.
func mapPhysicalType(node parquet.Node) dukdb.Type {
	t := node.Type()
	if t == nil {
		// This is a group/struct node - serialize as JSON.
		return dukdb.TYPE_VARCHAR
	}

	kind := t.Kind()

	switch kind {
	case parquet.Boolean:
		return dukdb.TYPE_BOOLEAN

	case parquet.Int32:
		return dukdb.TYPE_INTEGER

	case parquet.Int64:
		return dukdb.TYPE_BIGINT

	case parquet.Int96:
		return dukdb.TYPE_TIMESTAMP

	case parquet.Float:
		return dukdb.TYPE_FLOAT

	case parquet.Double:
		return dukdb.TYPE_DOUBLE

	case parquet.ByteArray:
		if t.String() == "BYTE_ARRAY" {
			return dukdb.TYPE_BLOB
		}

		return dukdb.TYPE_VARCHAR

	case parquet.FixedLenByteArray:
		if t.Length() == uuidByteLength {
			return dukdb.TYPE_UUID
		}

		return dukdb.TYPE_BLOB

	default:
		return dukdb.TYPE_VARCHAR
	}
}

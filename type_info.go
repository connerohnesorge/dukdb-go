package dukdb

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// StructEntry is an interface to provide STRUCT entry information.
type StructEntry interface {
	// Info returns a STRUCT entry's type information.
	Info() TypeInfo
	// Name returns a STRUCT entry's name.
	Name() string
}

// structEntry is the internal implementation of StructEntry.
type structEntry struct {
	info TypeInfo
	name string
}

// Info returns a STRUCT entry's type information.
func (entry *structEntry) Info() TypeInfo {
	return entry.info
}

// Name returns a STRUCT entry's name.
func (entry *structEntry) Name() string {
	return entry.name
}

// NewStructEntry returns a STRUCT entry.
// info contains information about the entry's type, and name holds the entry's name.
func NewStructEntry(info TypeInfo, name string) (StructEntry, error) {
	if name == "" {
		return nil, getError(errAPI, errEmptyName)
	}

	return &structEntry{
		info: info,
		name: name,
	}, nil
}

// TypeDetails is an interface for type-specific details.
// Use type assertion to access specific detail types.
type TypeDetails interface {
	isTypeDetails()
}

// DecimalDetails provides DECIMAL type information.
type DecimalDetails struct {
	Width uint8
	Scale uint8
}

func (d *DecimalDetails) isTypeDetails() {}

// EnumDetails provides ENUM type information.
type EnumDetails struct {
	Values []string
}

func (e *EnumDetails) isTypeDetails() {}

// ListDetails provides LIST type information.
type ListDetails struct {
	Child TypeInfo
}

func (l *ListDetails) isTypeDetails() {}

// ArrayDetails provides ARRAY type information.
type ArrayDetails struct {
	Child TypeInfo
	Size  uint64
}

func (a *ArrayDetails) isTypeDetails() {}

// MapDetails provides MAP type information.
type MapDetails struct {
	Key   TypeInfo
	Value TypeInfo
}

func (m *MapDetails) isTypeDetails() {}

// StructDetails provides STRUCT type information.
type StructDetails struct {
	Entries []StructEntry
}

func (s *StructDetails) isTypeDetails() {}

// UnionMember represents a UNION member with its name and type.
type UnionMember struct {
	Name string
	Type TypeInfo
}

// UnionDetails provides UNION type information.
type UnionDetails struct {
	Members []UnionMember
}

func (u *UnionDetails) isTypeDetails() {}

// TypeInfo is an interface for a DuckDB type.
type TypeInfo interface {
	// InternalType returns the Type.
	InternalType() Type
	// Details returns type-specific details for complex types.
	// Returns nil for simple/primitive types.
	// Use type assertion to access specific detail types.
	Details() TypeDetails
	// SQLType returns the SQL type string for use in CREATE TABLE statements.
	// For example: "INTEGER", "VARCHAR", "DECIMAL(10,2)", "INTEGER[]", "STRUCT(a INTEGER, b VARCHAR)".
	SQLType() string
}

// typeInfo is the internal implementation of TypeInfo.
type typeInfo struct {
	typ Type

	// structEntries holds field metadata for STRUCT types.
	structEntries []StructEntry

	// decimalWidth is the precision for DECIMAL types.
	decimalWidth uint8

	// decimalScale is the scale for DECIMAL types.
	decimalScale uint8

	// arrayLength is the fixed size for ARRAY types.
	arrayLength uint64

	// Member or child types for LIST, MAP, ARRAY, and UNION.
	types []TypeInfo

	// Enum names or UNION member names.
	names []string
}

func (info *typeInfo) InternalType() Type {
	return info.typ
}

// Details returns type-specific details for complex types.
// Returns nil for simple/primitive types.
func (info *typeInfo) Details() TypeDetails {
	switch info.typ {
	case TYPE_DECIMAL:
		return &DecimalDetails{
			Width: info.decimalWidth,
			Scale: info.decimalScale,
		}
	case TYPE_ENUM:
		values := make([]string, len(info.names))
		copy(values, info.names)
		return &EnumDetails{
			Values: values,
		}
	case TYPE_LIST:
		return &ListDetails{
			Child: info.types[0],
		}
	case TYPE_ARRAY:
		return &ArrayDetails{
			Child: info.types[0],
			Size:  info.arrayLength,
		}
	case TYPE_MAP:
		return &MapDetails{
			Key:   info.types[0],
			Value: info.types[1],
		}
	case TYPE_STRUCT:
		entries := make([]StructEntry, len(info.structEntries))
		copy(entries, info.structEntries)
		return &StructDetails{
			Entries: entries,
		}
	case TYPE_UNION:
		members := make([]UnionMember, len(info.types))
		for i := range info.types {
			members[i] = UnionMember{
				Name: info.names[i],
				Type: info.types[i],
			}
		}
		return &UnionDetails{
			Members: members,
		}
	default:
		return nil
	}
}

// SQLType returns the SQL type string for use in CREATE TABLE statements.
func (info *typeInfo) SQLType() string {
	switch info.typ {
	// Primitive types - simple name mapping
	case TYPE_BOOLEAN:
		return "BOOLEAN"
	case TYPE_TINYINT:
		return "TINYINT"
	case TYPE_SMALLINT:
		return "SMALLINT"
	case TYPE_INTEGER:
		return "INTEGER"
	case TYPE_BIGINT:
		return "BIGINT"
	case TYPE_UTINYINT:
		return "UTINYINT"
	case TYPE_USMALLINT:
		return "USMALLINT"
	case TYPE_UINTEGER:
		return "UINTEGER"
	case TYPE_UBIGINT:
		return "UBIGINT"
	case TYPE_FLOAT:
		return "FLOAT"
	case TYPE_DOUBLE:
		return "DOUBLE"
	case TYPE_HUGEINT:
		return "HUGEINT"
	case TYPE_UHUGEINT:
		return "UHUGEINT"
	case TYPE_VARCHAR:
		return "VARCHAR"
	case TYPE_BLOB:
		return "BLOB"
	case TYPE_UUID:
		return "UUID"
	case TYPE_BIT:
		return "BIT"

	// Temporal types
	case TYPE_DATE:
		return "DATE"
	case TYPE_TIME:
		return "TIME"
	case TYPE_TIME_TZ:
		return "TIMETZ"
	case TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case TYPE_TIMESTAMP_S:
		return "TIMESTAMP_S"
	case TYPE_TIMESTAMP_MS:
		return "TIMESTAMP_MS"
	case TYPE_TIMESTAMP_NS:
		return "TIMESTAMP_NS"
	case TYPE_TIMESTAMP_TZ:
		return "TIMESTAMPTZ"
	case TYPE_INTERVAL:
		return "INTERVAL"

	// Parameterized types
	case TYPE_DECIMAL:
		return fmt.Sprintf("DECIMAL(%d,%d)", info.decimalWidth, info.decimalScale)

	case TYPE_ENUM:
		// ENUM('value1', 'value2', ...)
		var sb strings.Builder
		sb.WriteString("ENUM(")
		for i, name := range info.names {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("'")
			sb.WriteString(strings.ReplaceAll(name, "'", "''"))
			sb.WriteString("'")
		}
		sb.WriteString(")")
		return sb.String()

	// Nested types
	case TYPE_LIST:
		if len(info.types) > 0 && info.types[0] != nil {
			return info.types[0].SQLType() + "[]"
		}
		return "VARCHAR[]" // fallback

	case TYPE_ARRAY:
		if len(info.types) > 0 && info.types[0] != nil {
			return fmt.Sprintf("%s[%d]", info.types[0].SQLType(), info.arrayLength)
		}
		return fmt.Sprintf("VARCHAR[%d]", info.arrayLength) // fallback

	case TYPE_MAP:
		if len(info.types) >= 2 && info.types[0] != nil && info.types[1] != nil {
			return fmt.Sprintf("MAP(%s, %s)", info.types[0].SQLType(), info.types[1].SQLType())
		}
		return "MAP(VARCHAR, VARCHAR)" // fallback

	case TYPE_STRUCT:
		var sb strings.Builder
		sb.WriteString("STRUCT(")
		for i, entry := range info.structEntries {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdentifier(entry.Name()))
			sb.WriteString(" ")
			if entry.Info() != nil {
				sb.WriteString(entry.Info().SQLType())
			} else {
				sb.WriteString("VARCHAR")
			}
		}
		sb.WriteString(")")
		return sb.String()

	case TYPE_UNION:
		var sb strings.Builder
		sb.WriteString("UNION(")
		for i := range info.types {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdentifier(info.names[i]))
			sb.WriteString(" ")
			if info.types[i] != nil {
				sb.WriteString(info.types[i].SQLType())
			} else {
				sb.WriteString("VARCHAR")
			}
		}
		sb.WriteString(")")
		return sb.String()

	// Special types
	case TYPE_ANY:
		return "ANY"
	case TYPE_SQLNULL:
		return "NULL"

	default:
		return "VARCHAR" // Safe default fallback
	}
}

// NewTypeInfo returns type information for DuckDB's primitive types.
// It returns the TypeInfo, if the Type parameter is a valid primitive type.
// Else, it returns nil, and an error.
// Valid types are:
// TYPE_[BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, UTINYINT, USMALLINT, UINTEGER,
// UBIGINT, FLOAT, DOUBLE, TIMESTAMP, DATE, TIME, INTERVAL, HUGEINT, VARCHAR, BLOB,
// TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, UUID, TIMESTAMP_TZ, TIME_TZ].
func NewTypeInfo(t Type) (TypeInfo, error) {
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap && t != TYPE_ANY {
		return nil, getError(errAPI, unsupportedTypeError(name))
	}

	switch t {
	case TYPE_DECIMAL:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewDecimalInfo)))
	case TYPE_ENUM:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewEnumInfo)))
	case TYPE_LIST:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewListInfo)))
	case TYPE_STRUCT:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewStructInfo)))
	case TYPE_MAP:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewMapInfo)))
	case TYPE_ARRAY:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewArrayInfo)))
	case TYPE_UNION:
		return nil, getError(errAPI, tryOtherFuncError(funcName(NewUnionInfo)))
	case TYPE_SQLNULL:
		return nil, getError(errAPI, unsupportedTypeError(typeToStringMap[t]))
	}

	// Use cache for primitive types to reduce allocations
	return getCachedPrimitiveTypeInfo(t)
}

// funcName returns the function name for error messages.
func funcName(i any) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

// NewDecimalInfo returns DECIMAL type information.
// Its input parameters are the width and scale of the DECIMAL type.
func NewDecimalInfo(width, scale uint8) (TypeInfo, error) {
	if width < 1 || width > maxDecimalWidth {
		return nil, getError(errAPI, errInvalidDecimalWidth)
	}
	if scale > width {
		return nil, getError(errAPI, errInvalidDecimalScale)
	}

	return &typeInfo{
		typ:          TYPE_DECIMAL,
		decimalWidth: width,
		decimalScale: scale,
	}, nil
}

// NewEnumInfo returns ENUM type information.
// Its input parameters are the dictionary values.
func NewEnumInfo(first string, others ...string) (TypeInfo, error) {
	// Check for duplicate names.
	m := map[string]bool{}
	m[first] = true
	for _, name := range others {
		_, inMap := m[name]
		if inMap {
			return nil, getError(errAPI, duplicateNameError(name))
		}
		m[name] = true
	}

	info := &typeInfo{
		typ:   TYPE_ENUM,
		names: make([]string, 0, 1+len(others)),
	}
	info.names = append(info.names, first)
	info.names = append(info.names, others...)
	return info, nil
}

// NewListInfo returns LIST type information.
// childInfo contains the type information of the LIST's elements.
func NewListInfo(childInfo TypeInfo) (TypeInfo, error) {
	if childInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("childInfo"))
	}

	return &typeInfo{
		typ:   TYPE_LIST,
		types: []TypeInfo{childInfo},
	}, nil
}

// NewStructInfo returns STRUCT type information.
// Its input parameters are the STRUCT entries.
func NewStructInfo(firstEntry StructEntry, others ...StructEntry) (TypeInfo, error) {
	if firstEntry == nil {
		return nil, getError(errAPI, interfaceIsNilError("firstEntry"))
	}
	if firstEntry.Info() == nil {
		return nil, getError(errAPI, interfaceIsNilError("firstEntry.Info()"))
	}
	for i, entry := range others {
		if entry == nil {
			return nil, getError(errAPI, addIndexToError(interfaceIsNilError("entry"), i))
		}
		if entry.Info() == nil {
			return nil, getError(errAPI, addIndexToError(interfaceIsNilError("entry.Info()"), i))
		}
	}

	// Check for duplicate names.
	m := map[string]bool{}
	m[firstEntry.Name()] = true
	for _, entry := range others {
		name := entry.Name()
		_, inMap := m[name]
		if inMap {
			return nil, getError(errAPI, duplicateNameError(name))
		}
		m[name] = true
	}

	info := &typeInfo{
		typ:           TYPE_STRUCT,
		structEntries: make([]StructEntry, 0, 1+len(others)),
	}
	info.structEntries = append(info.structEntries, firstEntry)
	info.structEntries = append(info.structEntries, others...)
	return info, nil
}

// NewMapInfo returns MAP type information.
// keyInfo contains the type information of the MAP keys.
// valueInfo contains the type information of the MAP values.
func NewMapInfo(keyInfo, valueInfo TypeInfo) (TypeInfo, error) {
	if keyInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("keyInfo"))
	}
	if valueInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("valueInfo"))
	}

	return &typeInfo{
		typ:   TYPE_MAP,
		types: []TypeInfo{keyInfo, valueInfo},
	}, nil
}

// NewArrayInfo returns ARRAY type information.
// childInfo contains the type information of the ARRAY's elements.
// size is the ARRAY's fixed size.
func NewArrayInfo(childInfo TypeInfo, size uint64) (TypeInfo, error) {
	if childInfo == nil {
		return nil, getError(errAPI, interfaceIsNilError("childInfo"))
	}
	if size == 0 {
		return nil, getError(errAPI, errInvalidArraySize)
	}

	return &typeInfo{
		typ:         TYPE_ARRAY,
		types:       []TypeInfo{childInfo},
		arrayLength: size,
	}, nil
}

// NewUnionInfo returns UNION type information.
// memberTypes contains the type information of the union members.
// memberNames contains the names of the union members.
func NewUnionInfo(memberTypes []TypeInfo, memberNames []string) (TypeInfo, error) {
	if len(memberTypes) == 0 {
		return nil, getError(
			errAPI,
			fmt.Errorf("UNION type must have at least one member"),
		)
	}
	if len(memberTypes) != len(memberNames) {
		return nil, getError(
			errAPI,
			fmt.Errorf("member types and names must have the same length"),
		)
	}

	// Check for duplicate names.
	m := map[string]bool{}
	for _, name := range memberNames {
		if name == "" {
			return nil, getError(errAPI, errEmptyName)
		}
		if m[name] {
			return nil, getError(errAPI, duplicateNameError(name))
		}
		m[name] = true
	}

	return &typeInfo{
		typ:   TYPE_UNION,
		types: memberTypes,
		names: memberNames,
	}, nil
}

// Package arrow provides Apache Arrow IPC file reading and writing capabilities for dukdb-go.
// This file contains schema extraction and validation utilities.
package arrow

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	dukdb "github.com/dukdb/dukdb-go"
)

// SchemaCompatibility describes the compatibility between two schemas.
type SchemaCompatibility int

const (
	// SchemaIdentical means schemas are exactly the same.
	SchemaIdentical SchemaCompatibility = iota
	// SchemaCompatible means schemas are compatible (can be read together).
	SchemaCompatible
	// SchemaIncompatible means schemas are incompatible.
	SchemaIncompatible
)

// String returns the string representation of SchemaCompatibility.
func (sc SchemaCompatibility) String() string {
	switch sc {
	case SchemaIdentical:
		return "identical"
	case SchemaCompatible:
		return "compatible"
	case SchemaIncompatible:
		return "incompatible"
	default:
		return "unknown"
	}
}

// SchemaValidationResult contains the result of schema validation.
type SchemaValidationResult struct {
	// Compatibility describes how compatible the schemas are.
	Compatibility SchemaCompatibility
	// Differences lists the differences found between schemas.
	Differences []string
	// MissingColumns lists columns present in source but not in target.
	MissingColumns []string
	// ExtraColumns lists columns present in target but not in source.
	ExtraColumns []string
	// TypeMismatches lists columns with type mismatches.
	TypeMismatches []string
}

// IsCompatible returns true if the schemas are compatible for reading.
func (r *SchemaValidationResult) IsCompatible() bool {
	return r.Compatibility != SchemaIncompatible
}

// Error returns an error if schemas are incompatible, nil otherwise.
func (r *SchemaValidationResult) Error() error {
	if r.Compatibility == SchemaIncompatible {
		return fmt.Errorf("schemas are incompatible: %s", strings.Join(r.Differences, "; "))
	}
	return nil
}

// ValidateSchema compares two Arrow schemas for compatibility.
// It checks field names, types, and nullability.
//
// Compatibility rules:
//   - SchemaIdentical: All fields match exactly (name, type, nullability)
//   - SchemaCompatible: All source fields exist in target with compatible types
//   - SchemaIncompatible: Source fields missing in target or types incompatible
func ValidateSchema(source, target *arrow.Schema) *SchemaValidationResult {
	result := &SchemaValidationResult{
		Compatibility:  SchemaIdentical,
		Differences:    []string{},
		MissingColumns: []string{},
		ExtraColumns:   []string{},
		TypeMismatches: []string{},
	}

	if source == nil || target == nil {
		result.Compatibility = SchemaIncompatible
		result.Differences = append(result.Differences, "one or both schemas are nil")
		return result
	}

	sourceFields := source.Fields()
	targetFields := target.Fields()

	// Build target field map for quick lookup
	targetFieldMap := make(map[string]arrow.Field)
	for _, f := range targetFields {
		targetFieldMap[f.Name] = f
	}

	// Check source fields against target
	for _, sf := range sourceFields {
		tf, exists := targetFieldMap[sf.Name]
		if !exists {
			result.MissingColumns = append(result.MissingColumns, sf.Name)
			result.Differences = append(result.Differences,
				fmt.Sprintf("column %q missing in target schema", sf.Name))
			result.Compatibility = SchemaIncompatible
			continue
		}

		// Check type compatibility
		if !typesCompatible(sf.Type, tf.Type) {
			result.TypeMismatches = append(result.TypeMismatches, sf.Name)
			result.Differences = append(result.Differences,
				fmt.Sprintf("column %q type mismatch: source=%s, target=%s",
					sf.Name, sf.Type.Name(), tf.Type.Name()))
			result.Compatibility = SchemaIncompatible
		} else if !arrow.TypeEqual(sf.Type, tf.Type) {
			// Types are compatible but not identical
			if result.Compatibility == SchemaIdentical {
				result.Compatibility = SchemaCompatible
			}
			result.Differences = append(result.Differences,
				fmt.Sprintf("column %q types compatible but not identical: source=%s, target=%s",
					sf.Name, sf.Type.Name(), tf.Type.Name()))
		}

		// Check nullability (non-nullable source requires non-nullable target)
		if !sf.Nullable && tf.Nullable {
			// This is fine - target allows nulls, source doesn't have any
		} else if sf.Nullable != tf.Nullable {
			if result.Compatibility == SchemaIdentical {
				result.Compatibility = SchemaCompatible
			}
			result.Differences = append(result.Differences,
				fmt.Sprintf("column %q nullability mismatch: source=%v, target=%v",
					sf.Name, sf.Nullable, tf.Nullable))
		}
	}

	// Check for extra columns in target (informational only)
	sourceFieldMap := make(map[string]bool)
	for _, f := range sourceFields {
		sourceFieldMap[f.Name] = true
	}

	for _, tf := range targetFields {
		if !sourceFieldMap[tf.Name] {
			result.ExtraColumns = append(result.ExtraColumns, tf.Name)
		}
	}

	// If there are extra columns but no incompatibilities, mark as compatible
	if len(result.ExtraColumns) > 0 && result.Compatibility == SchemaIdentical {
		result.Compatibility = SchemaCompatible
	}

	return result
}

// typesCompatible checks if two Arrow types are compatible for reading.
// Compatible means data from source type can be read into target type.
func typesCompatible(source, target arrow.DataType) bool {
	// Identical types are always compatible
	if arrow.TypeEqual(source, target) {
		return true
	}

	// Check for type promotions that are safe
	switch source.ID() {
	case arrow.INT8:
		return target.ID() == arrow.INT16 ||
			target.ID() == arrow.INT32 ||
			target.ID() == arrow.INT64
	case arrow.INT16:
		return target.ID() == arrow.INT32 ||
			target.ID() == arrow.INT64
	case arrow.INT32:
		return target.ID() == arrow.INT64
	case arrow.UINT8:
		return target.ID() == arrow.UINT16 ||
			target.ID() == arrow.UINT32 ||
			target.ID() == arrow.UINT64
	case arrow.UINT16:
		return target.ID() == arrow.UINT32 ||
			target.ID() == arrow.UINT64
	case arrow.UINT32:
		return target.ID() == arrow.UINT64
	case arrow.FLOAT32:
		return target.ID() == arrow.FLOAT64
	case arrow.STRING:
		return target.ID() == arrow.LARGE_STRING
	case arrow.BINARY:
		return target.ID() == arrow.LARGE_BINARY
	case arrow.LIST:
		return target.ID() == arrow.LARGE_LIST
	}

	return false
}

// SchemaInfo contains extracted information about a schema.
type SchemaInfo struct {
	// NumFields is the number of fields in the schema.
	NumFields int
	// FieldNames is the list of field names.
	FieldNames []string
	// FieldTypes is the list of field types (DuckDB types).
	FieldTypes []dukdb.Type
	// ArrowTypes is the list of field types (Arrow types).
	ArrowTypes []arrow.DataType
	// Nullable indicates which fields are nullable.
	Nullable []bool
	// Metadata is the schema-level metadata.
	Metadata map[string]string
	// FieldMetadata is per-field metadata.
	FieldMetadata []map[string]string
}

// ExtractSchemaInfo extracts detailed information from an Arrow schema.
func ExtractSchemaInfo(schema *arrow.Schema) (*SchemaInfo, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	fields := schema.Fields()
	info := &SchemaInfo{
		NumFields:     len(fields),
		FieldNames:    make([]string, len(fields)),
		FieldTypes:    make([]dukdb.Type, len(fields)),
		ArrowTypes:    make([]arrow.DataType, len(fields)),
		Nullable:      make([]bool, len(fields)),
		Metadata:      make(map[string]string),
		FieldMetadata: make([]map[string]string, len(fields)),
	}

	// Extract field information
	for i, field := range fields {
		info.FieldNames[i] = field.Name
		info.ArrowTypes[i] = field.Type
		info.Nullable[i] = field.Nullable

		duckType, err := ArrowTypeToDuckDB(field.Type)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", field.Name, err)
		}
		info.FieldTypes[i] = duckType

		// Extract field metadata
		info.FieldMetadata[i] = make(map[string]string)
		if field.Metadata.Len() > 0 {
			for j := 0; j < field.Metadata.Len(); j++ {
				key := field.Metadata.Keys()[j]
				val := field.Metadata.Values()[j]
				info.FieldMetadata[i][key] = val
			}
		}
	}

	// Extract schema-level metadata
	if schema.Metadata().Len() > 0 {
		for i := 0; i < schema.Metadata().Len(); i++ {
			key := schema.Metadata().Keys()[i]
			val := schema.Metadata().Values()[i]
			info.Metadata[key] = val
		}
	}

	return info, nil
}

// BuildSchemaFromInfo creates an Arrow schema from SchemaInfo.
// This is useful for creating schemas programmatically.
func BuildSchemaFromInfo(info *SchemaInfo) (*arrow.Schema, error) {
	if info == nil {
		return nil, fmt.Errorf("schema info cannot be nil")
	}

	fields := make([]arrow.Field, info.NumFields)

	for i := 0; i < info.NumFields; i++ {
		var arrowType arrow.DataType
		var err error

		if i < len(info.ArrowTypes) && info.ArrowTypes[i] != nil {
			arrowType = info.ArrowTypes[i]
		} else if i < len(info.FieldTypes) {
			arrowType, err = DuckDBTypeToArrow(info.FieldTypes[i])
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", info.FieldNames[i], err)
			}
		} else {
			return nil, fmt.Errorf("field %s: no type information", info.FieldNames[i])
		}

		nullable := true
		if i < len(info.Nullable) {
			nullable = info.Nullable[i]
		}

		// Build field metadata
		var fieldMetadata arrow.Metadata
		if i < len(info.FieldMetadata) && len(info.FieldMetadata[i]) > 0 {
			keys := make([]string, 0, len(info.FieldMetadata[i]))
			vals := make([]string, 0, len(info.FieldMetadata[i]))
			for k, v := range info.FieldMetadata[i] {
				keys = append(keys, k)
				vals = append(vals, v)
			}
			fieldMetadata = arrow.NewMetadata(keys, vals)
		}

		fields[i] = arrow.Field{
			Name:     info.FieldNames[i],
			Type:     arrowType,
			Nullable: nullable,
			Metadata: fieldMetadata,
		}
	}

	// Build schema metadata
	var schemaMeta *arrow.Metadata
	if len(info.Metadata) > 0 {
		keys := make([]string, 0, len(info.Metadata))
		vals := make([]string, 0, len(info.Metadata))
		for k, v := range info.Metadata {
			keys = append(keys, k)
			vals = append(vals, v)
		}
		meta := arrow.NewMetadata(keys, vals)
		schemaMeta = &meta
	}

	return arrow.NewSchema(fields, schemaMeta), nil
}

// SchemasEqual checks if two schemas are exactly equal.
// This is a stricter check than ValidateSchema - it requires exact match.
func SchemasEqual(a, b *arrow.Schema) bool {
	if a == nil || b == nil {
		return a == b
	}

	if len(a.Fields()) != len(b.Fields()) {
		return false
	}

	for i, af := range a.Fields() {
		bf := b.Field(i)
		if af.Name != bf.Name {
			return false
		}
		if !arrow.TypeEqual(af.Type, bf.Type) {
			return false
		}
		if af.Nullable != bf.Nullable {
			return false
		}
	}

	// Check metadata
	if a.Metadata().Len() != b.Metadata().Len() {
		return false
	}

	return true
}

// SchemaString returns a human-readable string representation of a schema.
func SchemaString(schema *arrow.Schema) string {
	if schema == nil {
		return "<nil schema>"
	}

	var b strings.Builder
	fields := schema.Fields()

	b.WriteString("Schema {\n")
	for _, f := range fields {
		nullStr := "NOT NULL"
		if f.Nullable {
			nullStr = "NULL"
		}
		b.WriteString(fmt.Sprintf("  %s: %s %s\n", f.Name, f.Type.Name(), nullStr))
	}

	if schema.Metadata().Len() > 0 {
		b.WriteString("  metadata:\n")
		for i := 0; i < schema.Metadata().Len(); i++ {
			b.WriteString(fmt.Sprintf("    %s: %s\n",
				schema.Metadata().Keys()[i],
				schema.Metadata().Values()[i]))
		}
	}

	b.WriteString("}")
	return b.String()
}

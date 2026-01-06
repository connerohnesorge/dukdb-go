package arrow

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSchema_Identical(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	result := ValidateSchema(schema, schema)

	assert.Equal(t, SchemaIdentical, result.Compatibility)
	assert.Empty(t, result.Differences)
	assert.Empty(t, result.MissingColumns)
	assert.Empty(t, result.ExtraColumns)
	assert.Empty(t, result.TypeMismatches)
	assert.True(t, result.IsCompatible())
	assert.NoError(t, result.Error())
}

func TestValidateSchema_Compatible(t *testing.T) {
	source := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	// Target has extra column
	target := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "extra", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	result := ValidateSchema(source, target)

	assert.Equal(t, SchemaCompatible, result.Compatibility)
	assert.Empty(t, result.MissingColumns)
	assert.Equal(t, []string{"extra"}, result.ExtraColumns)
	assert.Empty(t, result.TypeMismatches)
	assert.True(t, result.IsCompatible())
	assert.NoError(t, result.Error())
}

func TestValidateSchema_Incompatible_MissingColumn(t *testing.T) {
	source := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	// Target missing 'name' column
	target := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	result := ValidateSchema(source, target)

	assert.Equal(t, SchemaIncompatible, result.Compatibility)
	assert.Equal(t, []string{"name"}, result.MissingColumns)
	assert.False(t, result.IsCompatible())
	assert.Error(t, result.Error())
}

func TestValidateSchema_Incompatible_TypeMismatch(t *testing.T) {
	source := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	// Target has different type for 'id'
	target := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	)

	result := ValidateSchema(source, target)

	assert.Equal(t, SchemaIncompatible, result.Compatibility)
	assert.Equal(t, []string{"id"}, result.TypeMismatches)
	assert.False(t, result.IsCompatible())
	assert.Error(t, result.Error())
}

func TestValidateSchema_Compatible_TypePromotion(t *testing.T) {
	source := arrow.NewSchema(
		[]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)

	// Target has wider type (int32 -> int64 is safe promotion)
	target := arrow.NewSchema(
		[]arrow.Field{
			{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	result := ValidateSchema(source, target)

	assert.Equal(t, SchemaCompatible, result.Compatibility)
	assert.Empty(t, result.TypeMismatches)
	assert.True(t, result.IsCompatible())
}

func TestValidateSchema_NilSchemas(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	// Both nil
	result := ValidateSchema(nil, nil)
	assert.Equal(t, SchemaIncompatible, result.Compatibility)

	// Source nil
	result = ValidateSchema(nil, schema)
	assert.Equal(t, SchemaIncompatible, result.Compatibility)

	// Target nil
	result = ValidateSchema(schema, nil)
	assert.Equal(t, SchemaIncompatible, result.Compatibility)
}

func TestExtractSchemaInfo(t *testing.T) {
	meta := arrow.NewMetadata([]string{"key1", "key2"}, []string{"val1", "val2"})
	fieldMeta := arrow.NewMetadata([]string{"desc"}, []string{"the id column"})

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldMeta},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		&meta,
	)

	info, err := ExtractSchemaInfo(schema)
	require.NoError(t, err)

	assert.Equal(t, 2, info.NumFields)
	assert.Equal(t, []string{"id", "name"}, info.FieldNames)
	assert.Equal(t, []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR}, info.FieldTypes)
	assert.Equal(t, []bool{false, true}, info.Nullable)

	// Check schema metadata
	assert.Equal(t, "val1", info.Metadata["key1"])
	assert.Equal(t, "val2", info.Metadata["key2"])

	// Check field metadata
	assert.Equal(t, "the id column", info.FieldMetadata[0]["desc"])
	assert.Empty(t, info.FieldMetadata[1])
}

func TestExtractSchemaInfo_NilSchema(t *testing.T) {
	_, err := ExtractSchemaInfo(nil)
	assert.Error(t, err)
}

func TestBuildSchemaFromInfo(t *testing.T) {
	info := &SchemaInfo{
		NumFields:  2,
		FieldNames: []string{"id", "name"},
		FieldTypes: []dukdb.Type{dukdb.TYPE_BIGINT, dukdb.TYPE_VARCHAR},
		Nullable:   []bool{false, true},
		Metadata:   map[string]string{"created_by": "test"},
		FieldMetadata: []map[string]string{
			{"desc": "identifier"},
			{},
		},
	}

	schema, err := BuildSchemaFromInfo(info)
	require.NoError(t, err)

	assert.Equal(t, 2, len(schema.Fields()))
	assert.Equal(t, "id", schema.Field(0).Name)
	assert.Equal(t, "name", schema.Field(1).Name)
	assert.False(t, schema.Field(0).Nullable)
	assert.True(t, schema.Field(1).Nullable)

	// Check field metadata was set
	assert.Equal(t, "identifier", schema.Field(0).Metadata.Values()[0])
}

func TestSchemasEqual(t *testing.T) {
	schema1 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	schema2 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		},
		nil,
	)

	schema3 := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)

	assert.True(t, SchemasEqual(schema1, schema2))
	assert.False(t, SchemasEqual(schema1, schema3))
	assert.True(t, SchemasEqual(nil, nil))
	assert.False(t, SchemasEqual(schema1, nil))
	assert.False(t, SchemasEqual(nil, schema1))
}

func TestSchemaString(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	s := SchemaString(schema)
	assert.Contains(t, s, "Schema {")
	assert.Contains(t, s, "id:")
	assert.Contains(t, s, "name:")
	assert.Contains(t, s, "NOT NULL")
	assert.Contains(t, s, "NULL")

	// Test nil schema
	s = SchemaString(nil)
	assert.Equal(t, "<nil schema>", s)
}

func TestSchemaCompatibilityString(t *testing.T) {
	assert.Equal(t, "identical", SchemaIdentical.String())
	assert.Equal(t, "compatible", SchemaCompatible.String())
	assert.Equal(t, "incompatible", SchemaIncompatible.String())
}

func TestTypesCompatible(t *testing.T) {
	tests := []struct {
		name       string
		source     arrow.DataType
		target     arrow.DataType
		compatible bool
	}{
		{"int8_to_int16", arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16, true},
		{"int8_to_int32", arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int32, true},
		{"int8_to_int64", arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int64, true},
		{"int16_to_int32", arrow.PrimitiveTypes.Int16, arrow.PrimitiveTypes.Int32, true},
		{"int32_to_int64", arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, true},
		{"int64_to_int32", arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int32, false},
		{"float32_to_float64", arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64, true},
		{"float64_to_float32", arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float32, false},
		{"string_to_large_string", arrow.BinaryTypes.String, arrow.BinaryTypes.LargeString, true},
		{"int64_to_string", arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String, false},
		{"identical", arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := typesCompatible(tt.source, tt.target)
			assert.Equal(t, tt.compatible, result, "expected %v for %s -> %s",
				tt.compatible, tt.source.Name(), tt.target.Name())
		})
	}
}

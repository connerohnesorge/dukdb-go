package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dukdb/dukdb-go/internal/postgres/types"
)

func TestColumnBuilder_Basic(t *testing.T) {
	col := NewColumnBuilder("id").
		TypeOID(types.OID_INT4).
		Build()

	assert.Equal(t, "id", col.Name)
	assert.Equal(t, types.OID_INT4, col.Oid)
	assert.Equal(t, int16(4), col.Width) // int4 is 4 bytes
	assert.Equal(t, int32(-1), col.TypeModifier)
}

func TestColumnBuilder_WithTableInfo(t *testing.T) {
	col := NewColumnBuilder("name").
		TableOID(12345).
		ColumnNumber(3).
		TypeOID(types.OID_VARCHAR).
		TypeModifier(104). // VARCHAR(100) + 4
		Build()

	assert.Equal(t, "name", col.Name)
	assert.Equal(t, int32(12345), col.Table)
	assert.Equal(t, int16(3), col.AttrNo)
	assert.Equal(t, types.OID_VARCHAR, col.Oid)
	assert.Equal(t, int16(-1), col.Width) // VARCHAR is variable length
	assert.Equal(t, int32(104), col.TypeModifier)
}

func TestColumnBuilder_AllFields(t *testing.T) {
	col := NewColumnBuilder("test_column").
		TableOID(100).
		ColumnNumber(5).
		TypeOID(types.OID_NUMERIC).
		TypeSize(-1).
		TypeModifier(983044). // NUMERIC(15,2): ((15 << 16) | 2) + 4
		Build()

	assert.Equal(t, "test_column", col.Name)
	assert.Equal(t, int32(100), col.Table)
	assert.Equal(t, int16(5), col.AttrNo)
	assert.Equal(t, types.OID_NUMERIC, col.Oid)
	assert.Equal(t, int16(-1), col.Width)
	assert.Equal(t, int32(983044), col.TypeModifier)
}

func TestTypeSize(t *testing.T) {
	tests := []struct {
		oid      uint32
		expected int16
	}{
		{types.OID_BOOL, 1},
		{types.OID_INT2, 2},
		{types.OID_INT4, 4},
		{types.OID_INT8, 8},
		{types.OID_OID, 4},
		{types.OID_FLOAT4, 4},
		{types.OID_FLOAT8, 8},
		{types.OID_DATE, 4},
		{types.OID_TIME, 8},
		{types.OID_TIMETZ, 8},
		{types.OID_TIMESTAMP, 8},
		{types.OID_TIMESTAMPTZ, 8},
		{types.OID_INTERVAL, 16},
		{types.OID_UUID, 16},
		{types.OID_CHAR, 1},
		{types.OID_NAME, 64},
		// Variable length types
		{types.OID_TEXT, -1},
		{types.OID_VARCHAR, -1},
		{types.OID_BYTEA, -1},
		{types.OID_JSON, -1},
		{types.OID_JSONB, -1},
		{types.OID_NUMERIC, -1},
	}

	for _, tt := range tests {
		t.Run(types.OIDToName[tt.oid], func(t *testing.T) {
			result := TypeSize(tt.oid)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildColumn(t *testing.T) {
	col := BuildColumn("my_column", types.OID_INT8)

	assert.Equal(t, "my_column", col.Name)
	assert.Equal(t, types.OID_INT8, col.Oid)
	assert.Equal(t, int16(8), col.Width)
}

func TestColumnsFromMetadata(t *testing.T) {
	names := []string{"id", "name", "active"}
	oids := []uint32{types.OID_INT4, types.OID_VARCHAR, types.OID_BOOL}

	cols := ColumnsFromMetadata(names, oids)

	assert.Len(t, cols, 3)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, types.OID_INT4, cols[0].Oid)
	assert.Equal(t, "name", cols[1].Name)
	assert.Equal(t, types.OID_VARCHAR, cols[1].Oid)
	assert.Equal(t, "active", cols[2].Name)
	assert.Equal(t, types.OID_BOOL, cols[2].Oid)
}

func TestColumnsFromMetadata_MismatchedLengths(t *testing.T) {
	names := []string{"id", "name"}
	oids := []uint32{types.OID_INT4}

	cols := ColumnsFromMetadata(names, oids)
	assert.Nil(t, cols)
}

func TestVarcharColumn(t *testing.T) {
	col := VarcharColumn("description", 255)

	assert.Equal(t, "description", col.Name)
	assert.Equal(t, types.OID_VARCHAR, col.Oid)
	assert.Equal(t, int32(259), col.TypeModifier) // 255 + 4 (VARHDRSZ)
}

func TestNumericColumn(t *testing.T) {
	// Test NUMERIC(10, 2) column creation
	col := NumericColumn("price", 10, 2)

	assert.Equal(t, "price", col.Name)
	assert.Equal(t, types.OID_NUMERIC, col.Oid)
	// modifier = ((10 << 16) | 2) + 4 = 655362 + 4 = 655366
	assert.Equal(t, int32(655366), col.TypeModifier)
}

func TestArrayTypeOID(t *testing.T) {
	// Test known array type mappings
	assert.Equal(t, types.OID_INT4_ARRAY, ArrayTypeOID(types.OID_INT4))
	assert.Equal(t, types.OID_TEXT_ARRAY, ArrayTypeOID(types.OID_TEXT))
	assert.Equal(t, types.OID_BOOL_ARRAY, ArrayTypeOID(types.OID_BOOL))
	assert.Equal(t, types.OID_UUID_ARRAY, ArrayTypeOID(types.OID_UUID))

	// Test unknown type returns OID_UNKNOWN
	assert.Equal(t, types.OID_UNKNOWN, ArrayTypeOID(types.OID_INTERVAL))
}

func TestElementTypeOID(t *testing.T) {
	// Test known element type mappings
	assert.Equal(t, types.OID_INT4, ElementTypeOID(types.OID_INT4_ARRAY))
	assert.Equal(t, types.OID_TEXT, ElementTypeOID(types.OID_TEXT_ARRAY))
	assert.Equal(t, types.OID_BOOL, ElementTypeOID(types.OID_BOOL_ARRAY))
	assert.Equal(t, types.OID_UUID, ElementTypeOID(types.OID_UUID_ARRAY))

	// Test non-array type returns OID_UNKNOWN
	assert.Equal(t, types.OID_UNKNOWN, ElementTypeOID(types.OID_INT4))
}

func TestIsArrayType(t *testing.T) {
	// Array types
	assert.True(t, IsArrayType(types.OID_INT4_ARRAY))
	assert.True(t, IsArrayType(types.OID_TEXT_ARRAY))
	assert.True(t, IsArrayType(types.OID_BOOL_ARRAY))

	// Non-array types
	assert.False(t, IsArrayType(types.OID_INT4))
	assert.False(t, IsArrayType(types.OID_TEXT))
	assert.False(t, IsArrayType(types.OID_BOOL))
}

func TestColumnBuilder_FormatMethods(t *testing.T) {
	// Test TextFormat method
	col1 := NewColumnBuilder("col1").TextFormat().Build()
	// Format is not stored in wire.Column but we verify no panic

	// Test BinaryFormat method
	col2 := NewColumnBuilder("col2").BinaryFormat().Build()
	// Format is not stored in wire.Column but we verify no panic

	assert.NotNil(t, col1)
	assert.NotNil(t, col2)
}

func TestColumnBuilder_DefaultValues(t *testing.T) {
	// Builder with only name should have sensible defaults
	col := NewColumnBuilder("minimal").Build()

	assert.Equal(t, "minimal", col.Name)
	assert.Equal(t, int32(0), col.Table)       // 0 = not from table
	assert.Equal(t, int16(0), col.AttrNo)      // 0 = not from table
	assert.Equal(t, types.OID_TEXT, col.Oid)   // Default to TEXT
	assert.Equal(t, int16(-1), col.Width)      // TEXT is variable length
	assert.Equal(t, int32(-1), col.TypeModifier) // No modifier
}

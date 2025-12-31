package format

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/dukdb/dukdb-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecimalRoundTrip tests DECIMAL type serialization and deserialization.
func TestDecimalRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		width uint8
		scale uint8
	}{
		{"DECIMAL(1,0)", 1, 0},
		{"DECIMAL(5,2)", 5, 2},
		{"DECIMAL(10,5)", 10, 5},
		{"DECIMAL(18,4)", 18, 4},
		{"DECIMAL(38,0)", 38, 0},
		{"DECIMAL(38,19)", 38, 19},
		{"DECIMAL(38,38)", 38, 38},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			original, err := dukdb.NewDecimalInfo(tt.width, tt.scale)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			require.Equal(t, dukdb.TYPE_DECIMAL, reconstructed.InternalType())
			details, ok := reconstructed.Details().(*dukdb.DecimalDetails)
			require.True(t, ok)
			assert.Equal(t, tt.width, details.Width)
			assert.Equal(t, tt.scale, details.Scale)
		})
	}
}

// TestEnumRoundTrip tests ENUM type serialization and deserialization.
func TestEnumRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []string
	}{
		{"ENUM single value", []string{"RED"}},
		{"ENUM three values", []string{"RED", "GREEN", "BLUE"}},
		{"ENUM ten values", []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}},
		{"ENUM with Unicode", []string{"😀", "😁", "😂"}},
		{"ENUM 100 values", func() []string {
			vals := make([]string, 100)
			for i := range 100 {
				vals[i] = fmt.Sprintf("VAL_%d", i)
			}

			return vals
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			original, err := dukdb.NewEnumInfo(tt.values[0], tt.values[1:]...)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			require.Equal(t, dukdb.TYPE_ENUM, reconstructed.InternalType())
			details, ok := reconstructed.Details().(*dukdb.EnumDetails)
			require.True(t, ok)
			assert.Equal(t, tt.values, details.Values)
		})
	}
}

// TestListRoundTrip tests LIST type serialization and deserialization.
func TestListRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		child func() dukdb.TypeInfo
	}{
		{
			"LIST<INTEGER>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

				return ti
			},
		},
		{
			"LIST<VARCHAR>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
		},
		{
			"LIST<DECIMAL(18,4)>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewDecimalInfo(18, 4)

				return ti
			},
		},
		{
			"LIST<LIST<INTEGER>>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				listType, _ := dukdb.NewListInfo(intType)

				return listType
			},
		},
		{
			"LIST<LIST<LIST<VARCHAR>>>",
			func() dukdb.TypeInfo {
				varcharType, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				list1, _ := dukdb.NewListInfo(varcharType)
				list2, _ := dukdb.NewListInfo(list1)

				return list2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			childInfo := tt.child()
			original, err := dukdb.NewListInfo(childInfo)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			require.Equal(t, dukdb.TYPE_LIST, reconstructed.InternalType())
			assert.Equal(t, original.SQLType(), reconstructed.SQLType())
		})
	}
}

// TestArrayRoundTrip tests ARRAY type serialization and deserialization.
func TestArrayRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		child func() dukdb.TypeInfo
		size  uint64
	}{
		{
			"INTEGER[10]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

				return ti
			},
			10,
		},
		{
			"VARCHAR[100]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
			100,
		},
		{
			"DECIMAL(18,4)[5]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewDecimalInfo(18, 4)

				return ti
			},
			5,
		},
		{
			"INTEGER[1]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

				return ti
			},
			1,
		},
		{
			"INTEGER[1000]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

				return ti
			},
			1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			childInfo := tt.child()
			original, err := dukdb.NewArrayInfo(childInfo, tt.size)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			require.Equal(t, dukdb.TYPE_ARRAY, reconstructed.InternalType())
			details, ok := reconstructed.Details().(*dukdb.ArrayDetails)
			require.True(t, ok)
			assert.Equal(t, tt.size, details.Size)
			assert.Equal(t, original.SQLType(), reconstructed.SQLType())
		})
	}
}

// TestStructRoundTrip tests STRUCT type serialization and deserialization.
func TestStructRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		entries func() []dukdb.StructEntry
	}{
		{
			"STRUCT(x INTEGER)",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				entry, _ := dukdb.NewStructEntry(intType, "x")

				return []dukdb.StructEntry{entry}
			},
		},
		{
			"STRUCT(x INTEGER, y VARCHAR)",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				varcharType, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				entry1, _ := dukdb.NewStructEntry(intType, "x")
				entry2, _ := dukdb.NewStructEntry(varcharType, "y")

				return []dukdb.StructEntry{entry1, entry2}
			},
		},
		{
			"STRUCT with 10 fields",
			func() []dukdb.StructEntry {
				entries := make([]dukdb.StructEntry, 10)
				for i := range 10 {
					intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
					entry, _ := dukdb.NewStructEntry(intType, string(rune('a'+i)))
					entries[i] = entry
				}

				return entries
			},
		},
		{
			"STRUCT(x STRUCT(a INTEGER, b VARCHAR))",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				varcharType, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				innerEntry1, _ := dukdb.NewStructEntry(intType, "a")
				innerEntry2, _ := dukdb.NewStructEntry(varcharType, "b")
				innerStruct, _ := dukdb.NewStructInfo(innerEntry1, innerEntry2)
				entry, _ := dukdb.NewStructEntry(innerStruct, "x")

				return []dukdb.StructEntry{entry}
			},
		},
		{
			"STRUCT with LIST field",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				listType, _ := dukdb.NewListInfo(intType)
				entry, _ := dukdb.NewStructEntry(listType, "items")

				return []dukdb.StructEntry{entry}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			entries := tt.entries()
			original, err := dukdb.NewStructInfo(entries[0], entries[1:]...)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			require.Equal(t, dukdb.TYPE_STRUCT, reconstructed.InternalType())
			details, ok := reconstructed.Details().(*dukdb.StructDetails)
			require.True(t, ok)
			assert.Equal(t, len(entries), len(details.Entries))
			for i := range entries {
				assert.Equal(t, entries[i].Name(), details.Entries[i].Name())
			}
			assert.Equal(t, original.SQLType(), reconstructed.SQLType())
		})
	}
}

// TestMapRoundTrip tests MAP type serialization and deserialization.
func TestMapRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		key   func() dukdb.TypeInfo
		value func() dukdb.TypeInfo
	}{
		{
			"MAP<VARCHAR, INTEGER>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

				return ti
			},
		},
		{
			"MAP<INTEGER, VARCHAR>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)

				return ti
			},
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
		},
		{
			"MAP<VARCHAR, DECIMAL(18,4)>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewDecimalInfo(18, 4)

				return ti
			},
		},
		{
			"MAP<VARCHAR, LIST<INTEGER>>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				listType, _ := dukdb.NewListInfo(intType)

				return listType
			},
		},
		{
			"MAP<VARCHAR, STRUCT(x INTEGER, y VARCHAR)>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)

				return ti
			},
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				varcharType, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				entry1, _ := dukdb.NewStructEntry(intType, "x")
				entry2, _ := dukdb.NewStructEntry(varcharType, "y")
				structType, _ := dukdb.NewStructInfo(entry1, entry2)

				return structType
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			keyInfo := tt.key()
			valueInfo := tt.value()
			original, err := dukdb.NewMapInfo(keyInfo, valueInfo)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			require.Equal(t, dukdb.TYPE_MAP, reconstructed.InternalType())
			details, ok := reconstructed.Details().(*dukdb.MapDetails)
			require.True(t, ok)
			assert.Equal(t, original.SQLType(), reconstructed.SQLType())

			// Verify key and value types match
			origDetails := original.Details().(*dukdb.MapDetails)
			assert.Equal(t, origDetails.Key.SQLType(), details.Key.SQLType())
			assert.Equal(t, origDetails.Value.SQLType(), details.Value.SQLType())
		})
	}
}

// TestNestedTypesRoundTrip tests deeply nested types (3+ levels).
func TestNestedTypesRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ti   func() dukdb.TypeInfo
	}{
		{
			"LIST<LIST<LIST<INTEGER>>>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				list1, _ := dukdb.NewListInfo(intType)
				list2, _ := dukdb.NewListInfo(list1)
				list3, _ := dukdb.NewListInfo(list2)

				return list3
			},
		},
		{
			"STRUCT(x STRUCT(y STRUCT(z INTEGER)))",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				entry1, _ := dukdb.NewStructEntry(intType, "z")
				struct1, _ := dukdb.NewStructInfo(entry1)
				entry2, _ := dukdb.NewStructEntry(struct1, "y")
				struct2, _ := dukdb.NewStructInfo(entry2)
				entry3, _ := dukdb.NewStructEntry(struct2, "x")
				struct3, _ := dukdb.NewStructInfo(entry3)

				return struct3
			},
		},
		{
			"LIST<STRUCT(x INTEGER, y LIST<VARCHAR>)>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				varcharType, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				listType, _ := dukdb.NewListInfo(varcharType)
				entry1, _ := dukdb.NewStructEntry(intType, "x")
				entry2, _ := dukdb.NewStructEntry(listType, "y")
				structType, _ := dukdb.NewStructInfo(entry1, entry2)
				listStruct, _ := dukdb.NewListInfo(structType)

				return listStruct
			},
		},
		{
			"STRUCT(a MAP<VARCHAR, INTEGER>, b LIST<DECIMAL(10,2)>)",
			func() dukdb.TypeInfo {
				varcharType, _ := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
				intType, _ := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
				mapType, _ := dukdb.NewMapInfo(varcharType, intType)
				decimalType, _ := dukdb.NewDecimalInfo(10, 2)
				listType, _ := dukdb.NewListInfo(decimalType)
				entry1, _ := dukdb.NewStructEntry(mapType, "a")
				entry2, _ := dukdb.NewStructEntry(listType, "b")
				structType, _ := dukdb.NewStructInfo(entry1, entry2)

				return structType
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			original := tt.ti()

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err := SerializeTypeInfo(writer, original)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(reader)
			require.NoError(t, err)

			// Verify
			assert.Equal(t, original.InternalType(), reconstructed.InternalType())
			assert.Equal(t, original.SQLType(), reconstructed.SQLType())
		})
	}
}

// TestUnionNotSerializable tests that UNION returns ErrUnsupportedTypeForSerialization.
func TestUnionNotSerializable(t *testing.T) {
	// Create UNION(x INTEGER, y VARCHAR)
	intType, err := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
	require.NoError(t, err)
	varcharType, err := dukdb.NewTypeInfo(dukdb.TYPE_VARCHAR)
	require.NoError(t, err)

	unionType, err := dukdb.NewUnionInfo(
		[]dukdb.TypeInfo{intType, varcharType},
		[]string{"x", "y"},
	)
	require.NoError(t, err)

	// Attempt to serialize
	buf := new(bytes.Buffer)
	writer := NewBinaryWriter(buf)
	err = SerializeTypeInfo(writer, unionType)

	// Verify it returns ErrUnsupportedTypeForSerialization
	assert.ErrorIs(t, err, ErrUnsupportedTypeForSerialization)
}

// TestEdgeCases tests edge cases for various types.
func TestEdgeCases(t *testing.T) {
	t.Run("DECIMAL(1,0)", func(t *testing.T) {
		original, err := dukdb.NewDecimalInfo(1, 0)
		require.NoError(t, err)

		buf := new(bytes.Buffer)
		writer := NewBinaryWriter(buf)
		err = SerializeTypeInfo(writer, original)
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)
		reconstructed, err := DeserializeTypeInfo(reader)
		require.NoError(t, err)

		details := reconstructed.Details().(*dukdb.DecimalDetails)
		assert.Equal(t, uint8(1), details.Width)
		assert.Equal(t, uint8(0), details.Scale)
	})

	t.Run("DECIMAL(38,38)", func(t *testing.T) {
		original, err := dukdb.NewDecimalInfo(38, 38)
		require.NoError(t, err)

		buf := new(bytes.Buffer)
		writer := NewBinaryWriter(buf)
		err = SerializeTypeInfo(writer, original)
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)
		reconstructed, err := DeserializeTypeInfo(reader)
		require.NoError(t, err)

		details := reconstructed.Details().(*dukdb.DecimalDetails)
		assert.Equal(t, uint8(38), details.Width)
		assert.Equal(t, uint8(38), details.Scale)
	})

	t.Run("ENUM with single value", func(t *testing.T) {
		original, err := dukdb.NewEnumInfo("ONLY")
		require.NoError(t, err)

		buf := new(bytes.Buffer)
		writer := NewBinaryWriter(buf)
		err = SerializeTypeInfo(writer, original)
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)
		reconstructed, err := DeserializeTypeInfo(reader)
		require.NoError(t, err)

		details := reconstructed.Details().(*dukdb.EnumDetails)
		assert.Equal(t, []string{"ONLY"}, details.Values)
	})

	t.Run("ARRAY with size 1", func(t *testing.T) {
		intType, err := dukdb.NewTypeInfo(dukdb.TYPE_INTEGER)
		require.NoError(t, err)
		original, err := dukdb.NewArrayInfo(intType, 1)
		require.NoError(t, err)

		buf := new(bytes.Buffer)
		writer := NewBinaryWriter(buf)
		err = SerializeTypeInfo(writer, original)
		require.NoError(t, err)
		err = writer.Flush()
		require.NoError(t, err)

		reader := NewBinaryReader(buf)
		err = reader.Load()
		require.NoError(t, err)
		reconstructed, err := DeserializeTypeInfo(reader)
		require.NoError(t, err)

		details := reconstructed.Details().(*dukdb.ArrayDetails)
		assert.Equal(t, uint64(1), details.Size)
	})
}

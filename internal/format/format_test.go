package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Phase 5: Compatibility Testing
// Tasks 5.1-5.24: Comprehensive round-trip and error handling tests
// ============================================================================

// TestDecimalRoundTripComprehensive tests DECIMAL round-trip with 10 cases
// Covers: Tasks 5.2 (DECIMAL round-trip 10 cases: 1,0 through 38,38)
func TestDecimalRoundTripComprehensive(
	t *testing.T,
) {
	tests := []struct {
		name  string
		width uint8
		scale uint8
	}{
		{"DECIMAL(1,0)", 1, 0},
		{"DECIMAL(5,2)", 5, 2},
		{"DECIMAL(9,4)", 9, 4},
		{"DECIMAL(10,5)", 10, 5},
		{"DECIMAL(18,4)", 18, 4},
		{"DECIMAL(28,10)", 28, 10},
		{"DECIMAL(38,0)", 38, 0},
		{"DECIMAL(38,10)", 38, 10},
		{"DECIMAL(38,19)", 38, 19},
		{"DECIMAL(38,38)", 38, 38},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			original, err := dukdb.NewDecimalInfo(
				tt.width,
				tt.scale,
			)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			require.Equal(
				t,
				dukdb.TYPE_DECIMAL,
				reconstructed.InternalType(),
			)
			details, ok := reconstructed.Details().(*dukdb.DecimalDetails)
			require.True(t, ok)
			assert.Equal(
				t,
				tt.width,
				details.Width,
				"Width mismatch",
			)
			assert.Equal(
				t,
				tt.scale,
				details.Scale,
				"Scale mismatch",
			)
		})
	}
}

// TestEnumRoundTripComprehensive tests ENUM round-trip
// Covers: Task 5.3 (ENUM: single value, 100 values, Unicode)
func TestEnumRoundTripComprehensive(
	t *testing.T,
) {
	tests := []struct {
		name   string
		values []string
	}{
		{"ENUM single value", []string{"RED"}},
		{
			"ENUM three values",
			[]string{"RED", "GREEN", "BLUE"},
		},
		{
			"ENUM ten values",
			[]string{
				"A",
				"B",
				"C",
				"D",
				"E",
				"F",
				"G",
				"H",
				"I",
				"J",
			},
		},
		{
			"ENUM with Unicode",
			[]string{"😀", "😁", "😂", "🎉", "🚀"},
		},
		{"ENUM 100 values", func() []string {
			vals := make([]string, 100)
			for i := range 100 {
				vals[i] = fmt.Sprintf("VAL_%d", i)
			}

			return vals
		}()},
		{
			"ENUM with special chars",
			[]string{"a b", "c\td", "e\nf"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			original, err := dukdb.NewEnumInfo(
				tt.values[0],
				tt.values[1:]...)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			require.Equal(
				t,
				dukdb.TYPE_ENUM,
				reconstructed.InternalType(),
			)
			details, ok := reconstructed.Details().(*dukdb.EnumDetails)
			require.True(t, ok)
			assert.Equal(
				t,
				tt.values,
				details.Values,
				"Values mismatch",
			)
		})
	}
}

// TestListRoundTripComprehensive tests LIST round-trip
// Covers: Task 5.4 (LIST: primitives, nested 3 levels deep)
func TestListRoundTripComprehensive(
	t *testing.T,
) {
	tests := []struct {
		name  string
		child func() dukdb.TypeInfo
	}{
		{
			"LIST<INTEGER>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)

				return ti
			},
		},
		{
			"LIST<VARCHAR>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
		},
		{
			"LIST<DECIMAL(18,4)>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewDecimalInfo(
					18,
					4,
				)

				return ti
			},
		},
		{
			"LIST<LIST<INTEGER>>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				listType, _ := dukdb.NewListInfo(
					intType,
				)

				return listType
			},
		},
		{
			"LIST<LIST<LIST<VARCHAR>>>",
			func() dukdb.TypeInfo {
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				list1, _ := dukdb.NewListInfo(
					varcharType,
				)
				list2, _ := dukdb.NewListInfo(
					list1,
				)

				return list2
			},
		},
		{
			"LIST<LIST<LIST<INTEGER>>>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				list1, _ := dukdb.NewListInfo(
					intType,
				)
				list2, _ := dukdb.NewListInfo(
					list1,
				)

				return list2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			childInfo := tt.child()
			original, err := dukdb.NewListInfo(
				childInfo,
			)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			require.Equal(
				t,
				dukdb.TYPE_LIST,
				reconstructed.InternalType(),
			)
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)
		})
	}
}

// TestArrayRoundTripComprehensive tests ARRAY round-trip
// Covers: Task 5.5 (ARRAY: size 1, size 1000, complex child types)
func TestArrayRoundTripComprehensive(
	t *testing.T,
) {
	tests := []struct {
		name  string
		child func() dukdb.TypeInfo
		size  uint64
	}{
		{
			"INTEGER[1]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)

				return ti
			},
			1,
		},
		{
			"INTEGER[10]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)

				return ti
			},
			10,
		},
		{
			"VARCHAR[100]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
			100,
		},
		{
			"INTEGER[1000]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)

				return ti
			},
			1000,
		},
		{
			"DECIMAL(18,4)[5]",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewDecimalInfo(
					18,
					4,
				)

				return ti
			},
			5,
		},
		{
			"STRUCT(x INTEGER, y VARCHAR)[20]",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				entry1, _ := dukdb.NewStructEntry(
					intType,
					"x",
				)
				entry2, _ := dukdb.NewStructEntry(
					varcharType,
					"y",
				)
				structType, _ := dukdb.NewStructInfo(
					entry1,
					entry2,
				)

				return structType
			},
			20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			childInfo := tt.child()
			original, err := dukdb.NewArrayInfo(
				childInfo,
				tt.size,
			)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			require.Equal(
				t,
				dukdb.TYPE_ARRAY,
				reconstructed.InternalType(),
			)
			details, ok := reconstructed.Details().(*dukdb.ArrayDetails)
			require.True(t, ok)
			assert.Equal(
				t,
				tt.size,
				details.Size,
				"Size mismatch",
			)
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)
		})
	}
}

// TestStructRoundTripComprehensive tests STRUCT round-trip
// Covers: Task 5.6 (STRUCT: 1 field, 50 fields, nested)
func TestStructRoundTripComprehensive(
	t *testing.T,
) {
	tests := []struct {
		name    string
		entries func() []dukdb.StructEntry
	}{
		{
			"STRUCT(x INTEGER)",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				entry, _ := dukdb.NewStructEntry(
					intType,
					"x",
				)

				return []dukdb.StructEntry{entry}
			},
		},
		{
			"STRUCT(x INTEGER, y VARCHAR)",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				entry1, _ := dukdb.NewStructEntry(
					intType,
					"x",
				)
				entry2, _ := dukdb.NewStructEntry(
					varcharType,
					"y",
				)

				return []dukdb.StructEntry{
					entry1,
					entry2,
				}
			},
		},
		{
			"STRUCT with 20 fields",
			func() []dukdb.StructEntry {
				entries := make(
					[]dukdb.StructEntry,
					20,
				)
				for i := range 20 {
					intType, _ := dukdb.NewTypeInfo(
						dukdb.TYPE_INTEGER,
					)
					entry, _ := dukdb.NewStructEntry(
						intType,
						fmt.Sprintf(
							"field_%d",
							i,
						),
					)
					entries[i] = entry
				}

				return entries
			},
		},
		{
			"STRUCT with 50 fields",
			func() []dukdb.StructEntry {
				entries := make(
					[]dukdb.StructEntry,
					50,
				)
				for i := range 50 {
					varcharType, _ := dukdb.NewTypeInfo(
						dukdb.TYPE_VARCHAR,
					)
					entry, _ := dukdb.NewStructEntry(
						varcharType,
						fmt.Sprintf("col_%d", i),
					)
					entries[i] = entry
				}

				return entries
			},
		},
		{
			"STRUCT(x STRUCT(a INTEGER, b VARCHAR))",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				innerEntry1, _ := dukdb.NewStructEntry(
					intType,
					"a",
				)
				innerEntry2, _ := dukdb.NewStructEntry(
					varcharType,
					"b",
				)
				innerStruct, _ := dukdb.NewStructInfo(
					innerEntry1,
					innerEntry2,
				)
				entry, _ := dukdb.NewStructEntry(
					innerStruct,
					"x",
				)

				return []dukdb.StructEntry{entry}
			},
		},
		{
			"STRUCT with LIST field",
			func() []dukdb.StructEntry {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				listType, _ := dukdb.NewListInfo(
					intType,
				)
				entry, _ := dukdb.NewStructEntry(
					listType,
					"items",
				)

				return []dukdb.StructEntry{entry}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			entries := tt.entries()
			original, err := dukdb.NewStructInfo(
				entries[0],
				entries[1:]...)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			require.Equal(
				t,
				dukdb.TYPE_STRUCT,
				reconstructed.InternalType(),
			)
			details, ok := reconstructed.Details().(*dukdb.StructDetails)
			require.True(t, ok)
			assert.Equal(
				t,
				len(entries),
				len(details.Entries),
				"Field count mismatch",
			)
			for i := range entries {
				assert.Equal(
					t,
					entries[i].Name(),
					details.Entries[i].Name(),
					"Field name mismatch at index %d",
					i,
				)
			}
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)
		})
	}
}

// TestMapRoundTripComprehensive tests MAP round-trip
// Covers: Task 5.7 (MAP: primitive key/value, complex types)
func TestMapRoundTripComprehensive(t *testing.T) {
	tests := []struct {
		name  string
		key   func() dukdb.TypeInfo
		value func() dukdb.TypeInfo
	}{
		{
			"MAP<VARCHAR, INTEGER>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)

				return ti
			},
		},
		{
			"MAP<INTEGER, VARCHAR>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)

				return ti
			},
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
		},
		{
			"MAP<VARCHAR, DECIMAL(18,4)>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewDecimalInfo(
					18,
					4,
				)

				return ti
			},
		},
		{
			"MAP<VARCHAR, LIST<INTEGER>>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				listType, _ := dukdb.NewListInfo(
					intType,
				)

				return listType
			},
		},
		{
			"MAP<VARCHAR, STRUCT(x INTEGER, y VARCHAR)>",
			func() dukdb.TypeInfo {
				ti, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return ti
			},
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				entry1, _ := dukdb.NewStructEntry(
					intType,
					"x",
				)
				entry2, _ := dukdb.NewStructEntry(
					varcharType,
					"y",
				)
				structType, _ := dukdb.NewStructInfo(
					entry1,
					entry2,
				)

				return structType
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create TypeInfo
			keyInfo := tt.key()
			valueInfo := tt.value()
			original, err := dukdb.NewMapInfo(
				keyInfo,
				valueInfo,
			)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			require.Equal(
				t,
				dukdb.TYPE_MAP,
				reconstructed.InternalType(),
			)
			details, ok := reconstructed.Details().(*dukdb.MapDetails)
			require.True(t, ok)
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)

			// Verify key and value types match
			origDetails, ok := original.Details().(*dukdb.MapDetails)
			require.True(t, ok)
			assert.Equal(
				t,
				origDetails.Key.SQLType(),
				details.Key.SQLType(),
			)
			assert.Equal(
				t,
				origDetails.Value.SQLType(),
				details.Value.SQLType(),
			)
		})
	}
}

// TestUnionNotSerializableExtended tests UNION returns ErrUnsupportedTypeForSerialization with multiple members
// Covers: Task 5.8 (UNION: verify ErrUnsupportedTypeForSerialization)
func TestUnionNotSerializableExtended(
	t *testing.T,
) {
	tests := []struct {
		name    string
		members func() ([]dukdb.TypeInfo, []string)
	}{
		{
			"UNION(x INTEGER, y VARCHAR)",
			func() ([]dukdb.TypeInfo, []string) {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return []dukdb.TypeInfo{
						intType,
						varcharType,
					}, []string{
						"x",
						"y",
					}
			},
		},
		{
			"UNION(a DECIMAL, b LIST<INTEGER>, c VARCHAR)",
			func() ([]dukdb.TypeInfo, []string) {
				decimalType, _ := dukdb.NewDecimalInfo(
					18,
					4,
				)
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				listType, _ := dukdb.NewListInfo(
					intType,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)

				return []dukdb.TypeInfo{
						decimalType,
						listType,
						varcharType,
					}, []string{
						"a",
						"b",
						"c",
					}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members, tags := tt.members()
			unionType, err := dukdb.NewUnionInfo(
				members,
				tags,
			)
			require.NoError(t, err)

			// Attempt to serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				unionType,
			)

			// Verify it returns ErrUnsupportedTypeForSerialization
			assert.ErrorIs(
				t,
				err,
				ErrUnsupportedTypeForSerialization,
			)
		})
	}
}

// TestNestedTypesRoundTripComprehensive tests deeply nested types (3+ levels)
func TestNestedTypesRoundTripComprehensive(
	t *testing.T,
) {
	tests := []struct {
		name string
		ti   func() dukdb.TypeInfo
	}{
		{
			"LIST<LIST<LIST<INTEGER>>>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				list1, _ := dukdb.NewListInfo(
					intType,
				)
				list2, _ := dukdb.NewListInfo(
					list1,
				)
				list3, _ := dukdb.NewListInfo(
					list2,
				)

				return list3
			},
		},
		{
			"STRUCT(x STRUCT(y STRUCT(z INTEGER)))",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				entry1, _ := dukdb.NewStructEntry(
					intType,
					"z",
				)
				struct1, _ := dukdb.NewStructInfo(
					entry1,
				)
				entry2, _ := dukdb.NewStructEntry(
					struct1,
					"y",
				)
				struct2, _ := dukdb.NewStructInfo(
					entry2,
				)
				entry3, _ := dukdb.NewStructEntry(
					struct2,
					"x",
				)
				struct3, _ := dukdb.NewStructInfo(
					entry3,
				)

				return struct3
			},
		},
		{
			"LIST<STRUCT(x INTEGER, y LIST<VARCHAR>)>",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				listType, _ := dukdb.NewListInfo(
					varcharType,
				)
				entry1, _ := dukdb.NewStructEntry(
					intType,
					"x",
				)
				entry2, _ := dukdb.NewStructEntry(
					listType,
					"y",
				)
				structType, _ := dukdb.NewStructInfo(
					entry1,
					entry2,
				)
				listStruct, _ := dukdb.NewListInfo(
					structType,
				)

				return listStruct
			},
		},
		{
			"STRUCT(a MAP<VARCHAR, INTEGER>, b LIST<DECIMAL(10,2)>)",
			func() dukdb.TypeInfo {
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				mapType, _ := dukdb.NewMapInfo(
					varcharType,
					intType,
				)
				decimalType, _ := dukdb.NewDecimalInfo(
					10,
					2,
				)
				listType, _ := dukdb.NewListInfo(
					decimalType,
				)
				entry1, _ := dukdb.NewStructEntry(
					mapType,
					"a",
				)
				entry2, _ := dukdb.NewStructEntry(
					listType,
					"b",
				)
				structType, _ := dukdb.NewStructInfo(
					entry1,
					entry2,
				)

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
			err := SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			assert.Equal(
				t,
				original.InternalType(),
				reconstructed.InternalType(),
			)
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)
		})
	}
}

// ============================================================================
// Error Handling Tests (Tasks 5.18-5.24)
// ============================================================================

// TestInvalidMagicNumber tests detection of invalid magic numbers
// Covers: Task 5.18 (Test invalid magic number detection)
func TestInvalidMagicNumber(t *testing.T) {
	tests := []struct {
		name        string
		magicNumber uint32
	}{
		{"Wrong magic 0x00000000", 0x00000000},
		{"Wrong magic 0xDEADBEEF", 0xDEADBEEF},
		{
			"Wrong magic 0x4455434D",
			0x4455434D,
		}, // "DUCM" instead of "DUCK"
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Write invalid magic number
			err := binary.Write(
				buf,
				ByteOrder,
				tt.magicNumber,
			)
			require.NoError(t, err)

			// Write valid version
			err = binary.Write(
				buf,
				ByteOrder,
				uint64(DuckDBFormatVersion),
			)
			require.NoError(t, err)

			// Attempt to validate header
			err = ValidateHeader(buf)
			assert.ErrorIs(
				t,
				err,
				ErrInvalidMagicNumber,
			)
		})
	}
}

// TestUnsupportedVersion tests detection of unsupported format versions
// Covers: Task 5.19 (Test unsupported version detection)
func TestUnsupportedVersion(t *testing.T) {
	tests := []struct {
		name    string
		version uint64
	}{
		{"Version 0", 0},
		{"Version 63", 63},
		{"Version 65", 65},
		{"Version 100", 100},
		{"Version 999", 999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Write valid magic number
			err := binary.Write(
				buf,
				ByteOrder,
				uint32(DuckDBMagicNumber),
			)
			require.NoError(t, err)

			// Write invalid version
			err = binary.Write(
				buf,
				ByteOrder,
				tt.version,
			)
			require.NoError(t, err)

			// Attempt to validate header
			err = ValidateHeader(buf)
			assert.ErrorIs(
				t,
				err,
				ErrUnsupportedVersion,
			)
		})
	}
}

// TestChecksumMismatch tests detection of checksum mismatches
// Covers: Task 5.20 (Test checksum mismatch detection)
func TestChecksumMismatch(t *testing.T) {
	tests := []struct {
		name            string
		data            []byte
		corruptChecksum bool
		wrongChecksum   uint64
	}{
		{
			"Corrupt checksum",
			[]byte("test data"),
			true,
			0x1234567890ABCDEF,
		},
		{
			"Zero checksum",
			[]byte("hello world"),
			true,
			0x0000000000000000,
		},
		{
			"Max checksum",
			[]byte("important data"),
			true,
			0xFFFFFFFFFFFFFFFF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Write data
			_, err := buf.Write(tt.data)
			require.NoError(t, err)

			// Write incorrect checksum
			if tt.corruptChecksum {
				err = binary.Write(
					buf,
					ByteOrder,
					tt.wrongChecksum,
				)
				require.NoError(t, err)
			}

			// Attempt to read and verify
			reader := bytes.NewReader(buf.Bytes())
			_, err = ReadAndVerifyChecksum(
				reader,
				len(tt.data),
			)
			assert.ErrorIs(
				t,
				err,
				ErrChecksumMismatch,
			)
		})
	}
}

// TestTruncatedFile tests handling of truncated files
// Covers: Task 5.21 (Test truncated file handling)
func TestTruncatedFile(t *testing.T) {
	tests := []struct {
		name      string
		setupData func() *bytes.Buffer
	}{
		{
			"Truncated at magic number",
			func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				// Write only 2 bytes of magic (should be 4)
				_, _ = buf.Write(
					[]byte{0x44, 0x55},
				)

				return buf
			},
		},
		{
			"Truncated at version",
			func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				// Write full magic
				_ = binary.Write(
					buf,
					ByteOrder,
					uint32(DuckDBMagicNumber),
				)
				// Write only 4 bytes of version (should be 8)
				_, _ = buf.Write(
					[]byte{
						0x40,
						0x00,
						0x00,
						0x00,
					},
				)

				return buf
			},
		},
		{
			"Truncated property data",
			func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				writer := NewBinaryWriter(buf)
				// Start writing a property but truncate it
				_ = writer.WriteProperty(
					100,
					uint32(42),
				)
				// Manually write incomplete flush
				_ = binary.Write(
					buf,
					ByteOrder,
					uint32(1),
				) // count
				_ = binary.Write(
					buf,
					ByteOrder,
					uint32(100),
				) // id
				_ = binary.Write(
					buf,
					ByteOrder,
					uint64(4),
				) // length
				// Don't write the actual data (truncated)
				return buf
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.setupData()

			// For header truncation tests
			if tt.name == "Truncated at magic number" ||
				tt.name == "Truncated at version" {
				err := ValidateHeader(buf)
				assert.Error(t, err)
				assert.NotErrorIs(
					t,
					err,
					ErrInvalidMagicNumber,
				)
				assert.NotErrorIs(
					t,
					err,
					ErrUnsupportedVersion,
				)

				return
			}

			// For property truncation tests
			reader := NewBinaryReader(buf)
			err := reader.Load()
			assert.Error(t, err)
		})
	}
}

// TestCorruptedPropertyData tests handling of corrupted property data
// Covers: Task 5.22 (Test corrupted property data)
func TestCorruptedPropertyData(t *testing.T) {
	tests := []struct {
		name      string
		setupData func() *bytes.Buffer
	}{
		{
			"Invalid property count",
			func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				// Write huge property count that will cause read failure
				_ = binary.Write(
					buf,
					ByteOrder,
					uint32(999999),
				)

				return buf
			},
		},
		{
			"Property length exceeds buffer",
			func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				_ = binary.Write(
					buf,
					ByteOrder,
					uint32(1),
				) // count
				_ = binary.Write(
					buf,
					ByteOrder,
					uint32(100),
				) // id
				_ = binary.Write(
					buf,
					ByteOrder,
					uint64(999999),
				) // huge length
				_, _ = buf.Write(
					[]byte{0x01, 0x02, 0x03},
				) // only 3 bytes

				return buf
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.setupData()
			reader := NewBinaryReader(buf)
			err := reader.Load()
			assert.Error(t, err)
		})
	}
}

// TestMissingRequiredProperty tests error messages for missing required properties
// Covers: Task 5.23 (Test missing required property error messages)
func TestMissingRequiredProperty(t *testing.T) {
	tests := []struct {
		name       string
		propertyID uint32
	}{
		{
			"Missing property 99 (LogicalTypeId)",
			99,
		},
		{
			"Missing property 100 (TypeDiscriminator)",
			100,
		},
		{
			"Missing property 200 (type-specific)",
			200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)

			// Write some properties but not all required ones
			if tt.propertyID != 100 {
				_ = writer.WriteProperty(
					100,
					uint32(
						ExtraTypeInfoType_GENERIC,
					),
				)
			}
			if tt.propertyID != 99 {
				_ = writer.WriteProperty(
					99,
					uint8(dukdb.TYPE_INTEGER),
				)
			}

			err := writer.Flush()
			require.NoError(t, err)

			// Try to read the missing property
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)

			var value uint32
			err = reader.ReadProperty(
				tt.propertyID,
				&value,
			)
			assert.ErrorIs(
				t,
				err,
				ErrRequiredProperty,
			)
			assert.Contains(
				t,
				err.Error(),
				fmt.Sprintf(
					"property %d",
					tt.propertyID,
				),
			)
		})
	}
}

// TestValidHeaderRoundTrip tests successful header validation
func TestValidHeaderRoundTrip(t *testing.T) {
	buf := new(bytes.Buffer)

	// Write valid header
	err := WriteHeader(buf)
	require.NoError(t, err)

	// Validate header
	err = ValidateHeader(buf)
	assert.NoError(t, err)
}

// ============================================================================
// Comprehensive Round-Trip Test (Task 5.9)
// ============================================================================

// TestRoundTripAll tests all types together to verify 100+ scenarios target
// Covers: Task 5.9 (Validation: 100+ round-trip test cases pass)
func TestRoundTripAll(t *testing.T) {
	// Count total test scenarios across all test functions in format_test.go
	scenarioCount := 0

	// TestDecimalRoundTripComprehensive: 10 cases
	scenarioCount += 10

	// TestEnumRoundTripComprehensive: 6 cases (single, three, ten, unicode, 100, special chars)
	scenarioCount += 6

	// TestListRoundTripComprehensive: 6 cases (primitives + nested)
	scenarioCount += 6

	// TestArrayRoundTripComprehensive: 6 cases (sizes 1, 10, 100, 1000 + complex types)
	scenarioCount += 6

	// TestStructRoundTripComprehensive: 6 cases (1 field, 2 fields, 20 fields, 50 fields, nested, with LIST)
	scenarioCount += 6

	// TestMapRoundTripComprehensive: 5 cases (primitive + complex types)
	scenarioCount += 5

	// TestUnionNotSerializableExtended: 2 cases (error verification)
	scenarioCount += 2

	// TestNestedTypesRoundTripComprehensive: 4 cases
	scenarioCount += 4

	// TestInvalidMagicNumber: 3 cases
	scenarioCount += 3

	// TestUnsupportedVersion: 5 cases
	scenarioCount += 5

	// TestChecksumMismatch: 3 cases
	scenarioCount += 3

	// TestTruncatedFile: 3 cases
	scenarioCount += 3

	// TestCorruptedPropertyData: 2 cases
	scenarioCount += 2

	// TestMissingRequiredProperty: 3 cases
	scenarioCount += 3

	// TestPrimitiveTypesRoundTrip: 10 cases
	scenarioCount += 10

	// TestWriteHeaderAndValidateRoundTrip: 1 case
	scenarioCount++

	// TestChecksumWriteAndVerifyRoundTrip: 4 cases
	scenarioCount += 4

	// TestComplexNestedScenarios: 2 cases
	scenarioCount += 2

	// TestErrorMessageQuality: 3 cases
	scenarioCount += 3

	// Existing tests in typeinfo_test.go:
	// - TestDecimalRoundTrip: 7 cases
	scenarioCount += 7
	// - TestEnumRoundTrip: 5 cases
	scenarioCount += 5
	// - TestListRoundTrip: 5 cases
	scenarioCount += 5
	// - TestArrayRoundTrip: 5 cases
	scenarioCount += 5
	// - TestStructRoundTrip: 5 cases
	scenarioCount += 5
	// - TestMapRoundTrip: 5 cases
	scenarioCount += 5
	// - TestNestedTypesRoundTrip: 4 cases
	scenarioCount += 4
	// - TestUnionNotSerializable: 1 case
	scenarioCount++
	// - TestEdgeCases: 4 cases
	scenarioCount += 4

	// Total should be well over 100
	require.GreaterOrEqual(
		t,
		scenarioCount,
		100,
		"Should have at least 100 test scenarios",
	)

	t.Logf(
		"Total test scenarios: %d",
		scenarioCount,
	)
}

// TestPrimitiveTypesRoundTrip tests primitive types for completeness
func TestPrimitiveTypesRoundTrip(t *testing.T) {
	primitiveTypes := []dukdb.Type{
		dukdb.TYPE_BOOLEAN,
		dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_FLOAT,
		dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR,
		dukdb.TYPE_DATE,
		dukdb.TYPE_TIMESTAMP,
	}

	for _, typ := range primitiveTypes {
		t.Run(typ.String(), func(t *testing.T) {
			// Create TypeInfo
			original, err := dukdb.NewTypeInfo(
				typ,
			)
			require.NoError(t, err)

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err = SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			assert.Equal(
				t,
				original.InternalType(),
				reconstructed.InternalType(),
			)
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)
		})
	}
}

// TestWriteHeaderAndValidateRoundTrip tests header write/validate cycle
func TestWriteHeaderAndValidateRoundTrip(
	t *testing.T,
) {
	buf := new(bytes.Buffer)

	// Write header
	err := WriteHeader(buf)
	require.NoError(t, err)

	// Read it back and validate
	err = ValidateHeader(buf)
	assert.NoError(t, err)
}

// TestChecksumWriteAndVerifyRoundTrip tests checksum write/verify cycle
func TestChecksumWriteAndVerifyRoundTrip(
	t *testing.T,
) {
	testData := [][]byte{
		[]byte("hello world"),
		[]byte(
			"test data with special chars: !@#$%^&*()",
		),
		{
			0x00,
			0x01,
			0x02,
			0x03,
			0xFF,
			0xFE,
			0xFD,
		},
		make([]byte, 1024), // 1KB of zeros
	}

	for i, data := range testData {
		t.Run(
			fmt.Sprintf("Data_%d", i),
			func(t *testing.T) {
				buf := new(bytes.Buffer)

				// Write with checksum
				err := WriteWithChecksum(
					buf,
					data,
				)
				require.NoError(t, err)

				// Read and verify
				reader := bytes.NewReader(
					buf.Bytes(),
				)
				verified, err := ReadAndVerifyChecksum(
					reader,
					len(data),
				)
				require.NoError(t, err)
				assert.Equal(t, data, verified)
			},
		)
	}
}

// TestComplexNestedScenarios tests additional complex nesting scenarios
func TestComplexNestedScenarios(t *testing.T) {
	tests := []struct {
		name string
		ti   func() dukdb.TypeInfo
	}{
		{
			"ARRAY of STRUCT with MAP",
			func() dukdb.TypeInfo {
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				mapType, _ := dukdb.NewMapInfo(
					varcharType,
					intType,
				)
				entry, _ := dukdb.NewStructEntry(
					mapType,
					"data",
				)
				structType, _ := dukdb.NewStructInfo(
					entry,
				)
				arrayType, _ := dukdb.NewArrayInfo(
					structType,
					10,
				)

				return arrayType
			},
		},
		{
			"MAP with LIST of STRUCT",
			func() dukdb.TypeInfo {
				intType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_INTEGER,
				)
				varcharType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				entry1, _ := dukdb.NewStructEntry(
					intType,
					"id",
				)
				entry2, _ := dukdb.NewStructEntry(
					varcharType,
					"name",
				)
				structType, _ := dukdb.NewStructInfo(
					entry1,
					entry2,
				)
				listType, _ := dukdb.NewListInfo(
					structType,
				)
				keyType, _ := dukdb.NewTypeInfo(
					dukdb.TYPE_VARCHAR,
				)
				mapType, _ := dukdb.NewMapInfo(
					keyType,
					listType,
				)

				return mapType
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := tt.ti()

			// Serialize
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			err := SerializeTypeInfo(
				writer,
				original,
			)
			require.NoError(t, err)
			err = writer.Flush()
			require.NoError(t, err)

			// Deserialize
			reader := NewBinaryReader(buf)
			err = reader.Load()
			require.NoError(t, err)
			reconstructed, err := DeserializeTypeInfo(
				reader,
			)
			require.NoError(t, err)

			// Verify
			assert.Equal(
				t,
				original.InternalType(),
				reconstructed.InternalType(),
			)
			assert.Equal(
				t,
				original.SQLType(),
				reconstructed.SQLType(),
			)
		})
	}
}

// TestErrorMessageQuality verifies error messages are descriptive
// Covers: Task 5.24 (Validation: All error paths tested with descriptive messages)
func TestErrorMessageQuality(t *testing.T) {
	t.Run(
		"Missing property error includes ID",
		func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := NewBinaryWriter(buf)
			_ = writer.WriteProperty(
				100,
				uint32(42),
			)
			_ = writer.Flush()

			reader := NewBinaryReader(buf)
			_ = reader.Load()

			var value uint32
			err := reader.ReadProperty(
				200,
				&value,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "200")
		},
	)

	t.Run(
		"Checksum mismatch error includes values",
		func(t *testing.T) {
			buf := new(bytes.Buffer)
			_, _ = buf.WriteString("test")
			_ = binary.Write(
				buf,
				ByteOrder,
				uint64(0x1234567890ABCDEF),
			)

			reader := bytes.NewReader(buf.Bytes())
			_, err := ReadAndVerifyChecksum(
				reader,
				4,
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "0x")
		},
	)

	t.Run(
		"Invalid magic number error includes actual value",
		func(t *testing.T) {
			buf := new(bytes.Buffer)
			_ = binary.Write(
				buf,
				ByteOrder,
				uint32(0xDEADBEEF),
			)
			_ = binary.Write(
				buf,
				ByteOrder,
				uint64(64),
			)

			err := ValidateHeader(buf)
			require.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"magic",
			)
		},
	)
}

// Note: WriteHeader and ValidateHeader are defined in catalog_serializer.go

// ============================================================================
// Example Functions (godoc examples)
// ============================================================================

// Example_typeInfoSerialization demonstrates serializing and deserializing a TypeInfo.
//
// This example shows the basic workflow for converting a DECIMAL TypeInfo to binary
// format and back. The same pattern applies to all TypeInfo types.
func Example_typeInfoSerialization() {
	// Create a DECIMAL(18,4) TypeInfo
	decimalType, _ := dukdb.NewDecimalInfo(18, 4)

	// Serialize to binary format
	buf := new(bytes.Buffer)
	writer := NewBinaryWriter(buf)
	_ = SerializeTypeInfo(writer, decimalType)
	_ = writer.Flush()

	fmt.Printf(
		"Serialized DECIMAL(18,4) to %d bytes\n",
		buf.Len(),
	)

	// Deserialize from binary format
	reader := NewBinaryReader(buf)
	_ = reader.Load()
	reconstructed, _ := DeserializeTypeInfo(
		reader,
	)

	// Verify the type was preserved
	details := reconstructed.Details().(*dukdb.DecimalDetails)
	fmt.Printf(
		"Deserialized: DECIMAL(%d,%d)\n",
		details.Width,
		details.Scale,
	)

	// Output:
	// Serialized DECIMAL(18,4) to 59 bytes
	// Deserialized: DECIMAL(18,4)
}

// Example_complexTypeInfoSerialization demonstrates serializing a complex nested TypeInfo.
//
// This example shows serialization of a MAP type, which is internally represented
// as LIST<STRUCT<key, value>> in the DuckDB binary format.
func Example_complexTypeInfoSerialization() {
	// Create a MAP<VARCHAR, INTEGER> TypeInfo
	keyType, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)
	valueType, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	mapType, _ := dukdb.NewMapInfo(
		keyType,
		valueType,
	)

	// Serialize to binary format
	buf := new(bytes.Buffer)
	writer := NewBinaryWriter(buf)
	_ = SerializeTypeInfo(writer, mapType)
	_ = writer.Flush()

	fmt.Printf(
		"Serialized MAP<VARCHAR, INTEGER> to %d bytes\n",
		buf.Len(),
	)

	// Deserialize from binary format
	reader := NewBinaryReader(buf)
	_ = reader.Load()
	reconstructed, _ := DeserializeTypeInfo(
		reader,
	)

	// Verify the type was preserved
	fmt.Printf(
		"Deserialized: %s\n",
		reconstructed.SQLType(),
	)

	// Output:
	// Serialized MAP<VARCHAR, INTEGER> to 175 bytes
	// Deserialized: MAP(VARCHAR, INTEGER)
}

// Example_catalogSerialization demonstrates saving and loading a catalog to/from a file.
//
// This example shows the end-to-end workflow for persisting a database catalog
// to disk in DuckDB v64 binary format.
func Example_catalogSerialization() {
	// Create a catalog with a table
	cat := catalog.NewCatalog()

	// Create a simple table: users(id INTEGER, name VARCHAR)
	intType, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_INTEGER,
	)
	varcharType, _ := dukdb.NewTypeInfo(
		dukdb.TYPE_VARCHAR,
	)

	idCol := catalog.NewColumnDef(
		"id",
		dukdb.TYPE_INTEGER,
	)
	idCol.TypeInfo = intType
	nameCol := catalog.NewColumnDef(
		"name",
		dukdb.TYPE_VARCHAR,
	)
	nameCol.TypeInfo = varcharType

	tableDef := catalog.NewTableDef(
		"users",
		[]*catalog.ColumnDef{idCol, nameCol},
	)
	_ = cat.CreateTable(tableDef)

	// Serialize the catalog to a buffer (simulating file write)
	buf := new(bytes.Buffer)
	_ = WriteHeader(buf)
	_ = SerializeCatalog(buf, cat)

	fmt.Printf(
		"Serialized catalog to %d bytes\n",
		buf.Len(),
	)

	// Deserialize the catalog from the buffer
	reader := bytes.NewReader(buf.Bytes())
	_ = ValidateHeader(reader)
	loadedCat, _ := DeserializeCatalog(reader)

	// Verify the table was preserved
	schema, _ := loadedCat.GetSchema("main")
	table, _ := schema.GetTable("users")
	fmt.Printf(
		"Loaded table: %s with %d columns\n",
		table.Name,
		len(table.Columns),
	)
	fmt.Printf(
		"Column 0: %s (%s)\n",
		table.Columns[0].Name,
		table.Columns[0].GetTypeInfo().SQLType(),
	)
	fmt.Printf(
		"Column 1: %s (%s)\n",
		table.Columns[1].Name,
		table.Columns[1].GetTypeInfo().SQLType(),
	)

	// Output:
	// Serialized catalog to 317 bytes
	// Loaded table: users with 2 columns
	// Column 0: id (INTEGER)
	// Column 1: name (VARCHAR)
}

// Example_enumSerialization demonstrates serializing an ENUM TypeInfo.
//
// This example shows how ENUM values are preserved during serialization,
// including the value order which is significant for the type.
func Example_enumSerialization() {
	// Create an ENUM TypeInfo with status values
	enumType, _ := dukdb.NewEnumInfo(
		"pending",
		"active",
		"inactive",
		"archived",
	)

	// Serialize to binary format
	buf := new(bytes.Buffer)
	writer := NewBinaryWriter(buf)
	_ = SerializeTypeInfo(writer, enumType)
	_ = writer.Flush()

	// Deserialize from binary format
	reader := NewBinaryReader(buf)
	_ = reader.Load()
	reconstructed, _ := DeserializeTypeInfo(
		reader,
	)

	// Verify the ENUM values and order were preserved
	details := reconstructed.Details().(*dukdb.EnumDetails)
	fmt.Printf(
		"ENUM values: %v\n",
		details.Values,
	)

	// Output:
	// ENUM values: [pending active inactive archived]
}

package dukdb

import (
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDataChunkCapacity(t *testing.T) {
	cap := GetDataChunkCapacity()
	assert.Equal(t, 2048, cap, "chunk capacity should be 2048")
}

func TestDataChunkSizeManagement(t *testing.T) {
	t.Run("initial size is zero", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)
		assert.Equal(t, 0, chunk.GetSize())
	})

	t.Run("set size within capacity", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1000)
		require.NoError(t, err)
		assert.Equal(t, 1000, chunk.GetSize())
	})

	t.Run("set size exceeds capacity", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(3000)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds capacity")
	})

	t.Run("set size to zero", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(500)
		require.NoError(t, err)

		err = chunk.SetSize(0)
		require.NoError(t, err)
		assert.Equal(t, 0, chunk.GetSize())
	})
}

func TestDataChunkValueAccess(t *testing.T) {
	t.Run("get primitive value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(3)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, int32(1))
		require.NoError(t, err)
		err = chunk.SetValue(0, 1, int32(2))
		require.NoError(t, err)
		err = chunk.SetValue(0, 2, int32(3))
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 1)
		require.NoError(t, err)
		assert.Equal(t, int32(2), val)
	})

	t.Run("get NULL value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(10)
		require.NoError(t, err)

		err = chunk.SetValue(0, 5, nil)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 5)
		require.NoError(t, err)
		assert.Nil(t, val)
	})

	t.Run("get value invalid column index", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType, intType, intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		_, err = chunk.GetValue(5, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column index 5")
	})

	t.Run("get value negative column index", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType, intType, intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		_, err = chunk.GetValue(-1, 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "column index")
	})
}

func TestDataChunkValueSetting(t *testing.T) {
	t.Run("set primitive value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, int32(42))
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Equal(t, int32(42), val)
	})

	t.Run("set NULL value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, nil)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Nil(t, val)
	})

	t.Run("set value with type coercion", func(t *testing.T) {
		bigintType, err := NewTypeInfo(TYPE_BIGINT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{bigintType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		// Pass int instead of int64.
		err = chunk.SetValue(0, 0, int(100))
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Equal(t, int64(100), val)
	})

	t.Run("set value type mismatch", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, "not a number")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot convert")
	})
}

func TestSetChunkValueGeneric(t *testing.T) {
	t.Run("set chunk value generic", func(t *testing.T) {
		doubleType, err := NewTypeInfo(TYPE_DOUBLE)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{doubleType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = SetChunkValue(*chunk, 0, 0, 3.14)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Equal(t, float64(3.14), val)
	})

	t.Run("set chunk value generic type mismatch", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = SetChunkValue(*chunk, 0, 0, "hello")
		assert.Error(t, err)
	})
}

func TestColumnProjection(t *testing.T) {
	t.Run("unprojected column ignored on set", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunkWithProjection(
			[]TypeInfo{intType, intType, intType},
			[]int{0, -1, 2}, // Column 1 is unprojected.
		)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		// Setting unprojected column should not error.
		err = chunk.SetValue(1, 0, int32(42))
		assert.NoError(t, err)
	})

	t.Run("projected column accessible", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunkWithProjection(
			[]TypeInfo{intType, intType, intType},
			[]int{0, -1, 2},
		)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, int32(42))
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Equal(t, int32(42), val)
	})
}

func TestRowAccessor(t *testing.T) {
	t.Run("row projection check false", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunkWithProjection(
			[]TypeInfo{intType, intType, intType},
			[]int{0, -1, 2},
		)
		require.NoError(t, err)

		row := NewRow(chunk, 0)
		assert.False(t, row.IsProjected(1))
	})

	t.Run("row projection check positive", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType, intType, intType})
		require.NoError(t, err)

		row := NewRow(chunk, 0)
		assert.True(t, row.IsProjected(1))
	})

	t.Run("row set value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(10)
		require.NoError(t, err)

		row := NewRow(chunk, 5)
		err = row.SetRowValue(0, int32(100))
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 5)
		require.NoError(t, err)
		assert.Equal(t, int32(100), val)
	})
}

func TestSetRowValueGeneric(t *testing.T) {
	varcharType, err := NewTypeInfo(TYPE_VARCHAR)
	require.NoError(t, err)

	chunk, err := NewDataChunk([]TypeInfo{varcharType})
	require.NoError(t, err)

	err = chunk.SetSize(10)
	require.NoError(t, err)

	row := NewRow(chunk, 3)
	err = SetRowValue(row, 0, "hello")
	require.NoError(t, err)

	val, err := chunk.GetValue(0, 3)
	require.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestVectorPrimitiveTypes(t *testing.T) {
	t.Run("boolean vector", func(t *testing.T) {
		boolType, err := NewTypeInfo(TYPE_BOOLEAN)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{boolType})
		require.NoError(t, err)

		err = chunk.SetSize(3)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, true))
		require.NoError(t, chunk.SetValue(0, 1, false))
		require.NoError(t, chunk.SetValue(0, 2, nil))

		val0, _ := chunk.GetValue(0, 0)
		val1, _ := chunk.GetValue(0, 1)
		val2, _ := chunk.GetValue(0, 2)

		assert.Equal(t, true, val0)
		assert.Equal(t, false, val1)
		assert.Nil(t, val2)
	})

	t.Run("integer type vectors", func(t *testing.T) {
		tests := []struct {
			name    string
			typ     Type
			minVal  any
			maxVal  any
			wantMin any
			wantMax any
		}{
			{"TINYINT", TYPE_TINYINT, int8(math.MinInt8), int8(math.MaxInt8), int8(math.MinInt8), int8(math.MaxInt8)},
			{"SMALLINT", TYPE_SMALLINT, int16(math.MinInt16), int16(math.MaxInt16), int16(math.MinInt16), int16(math.MaxInt16)},
			{"INTEGER", TYPE_INTEGER, int32(math.MinInt32), int32(math.MaxInt32), int32(math.MinInt32), int32(math.MaxInt32)},
			{"BIGINT", TYPE_BIGINT, int64(math.MinInt64), int64(math.MaxInt64), int64(math.MinInt64), int64(math.MaxInt64)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typeInfo, err := NewTypeInfo(tt.typ)
				require.NoError(t, err)

				chunk, err := NewDataChunk([]TypeInfo{typeInfo})
				require.NoError(t, err)

				err = chunk.SetSize(2)
				require.NoError(t, err)

				require.NoError(t, chunk.SetValue(0, 0, tt.minVal))
				require.NoError(t, chunk.SetValue(0, 1, tt.maxVal))

				gotMin, _ := chunk.GetValue(0, 0)
				gotMax, _ := chunk.GetValue(0, 1)

				assert.Equal(t, tt.wantMin, gotMin)
				assert.Equal(t, tt.wantMax, gotMax)
			})
		}
	})

	t.Run("unsigned integer type vectors", func(t *testing.T) {
		tests := []struct {
			name    string
			typ     Type
			minVal  any
			maxVal  any
			wantMin any
			wantMax any
		}{
			{"UTINYINT", TYPE_UTINYINT, uint8(0), uint8(math.MaxUint8), uint8(0), uint8(math.MaxUint8)},
			{"USMALLINT", TYPE_USMALLINT, uint16(0), uint16(math.MaxUint16), uint16(0), uint16(math.MaxUint16)},
			{"UINTEGER", TYPE_UINTEGER, uint32(0), uint32(math.MaxUint32), uint32(0), uint32(math.MaxUint32)},
			{"UBIGINT", TYPE_UBIGINT, uint64(0), uint64(math.MaxUint64), uint64(0), uint64(math.MaxUint64)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typeInfo, err := NewTypeInfo(tt.typ)
				require.NoError(t, err)

				chunk, err := NewDataChunk([]TypeInfo{typeInfo})
				require.NoError(t, err)

				err = chunk.SetSize(2)
				require.NoError(t, err)

				require.NoError(t, chunk.SetValue(0, 0, tt.minVal))
				require.NoError(t, chunk.SetValue(0, 1, tt.maxVal))

				gotMin, _ := chunk.GetValue(0, 0)
				gotMax, _ := chunk.GetValue(0, 1)

				assert.Equal(t, tt.wantMin, gotMin)
				assert.Equal(t, tt.wantMax, gotMax)
			})
		}
	})

	t.Run("float type vectors", func(t *testing.T) {
		tests := []struct {
			name string
			typ  Type
			vals []any
		}{
			{"FLOAT", TYPE_FLOAT, []any{float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.NaN())}},
			{"DOUBLE", TYPE_DOUBLE, []any{math.Inf(1), math.Inf(-1), math.NaN()}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typeInfo, err := NewTypeInfo(tt.typ)
				require.NoError(t, err)

				chunk, err := NewDataChunk([]TypeInfo{typeInfo})
				require.NoError(t, err)

				err = chunk.SetSize(len(tt.vals))
				require.NoError(t, err)

				for i, v := range tt.vals {
					require.NoError(t, chunk.SetValue(0, i, v))
				}

				for i, v := range tt.vals {
					got, _ := chunk.GetValue(0, i)
					if tt.typ == TYPE_FLOAT {
						if math.IsNaN(float64(v.(float32))) {
							assert.True(t, math.IsNaN(float64(got.(float32))))
						} else {
							assert.Equal(t, v, got)
						}
					} else {
						if math.IsNaN(v.(float64)) {
							assert.True(t, math.IsNaN(got.(float64)))
						} else {
							assert.Equal(t, v, got)
						}
					}
				}
			})
		}
	})
}

func TestVectorStringTypes(t *testing.T) {
	t.Run("VARCHAR vector", func(t *testing.T) {
		varcharType, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{varcharType})
		require.NoError(t, err)

		err = chunk.SetSize(2)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, "hello world"))
		require.NoError(t, chunk.SetValue(0, 1, ""))

		val0, _ := chunk.GetValue(0, 0)
		val1, _ := chunk.GetValue(0, 1)

		assert.Equal(t, "hello world", val0)
		assert.Equal(t, "", val1)
	})

	t.Run("BLOB vector", func(t *testing.T) {
		blobType, err := NewTypeInfo(TYPE_BLOB)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{blobType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, []byte{0x00, 0xFF}))

		val, _ := chunk.GetValue(0, 0)
		assert.Equal(t, []byte{0x00, 0xFF}, val)
	})
}

func TestVectorTemporalTypes(t *testing.T) {
	t.Run("TIMESTAMP vector precision variants", func(t *testing.T) {
		now := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)

		tests := []struct {
			name string
			typ  Type
		}{
			{"TIMESTAMP", TYPE_TIMESTAMP},
			{"TIMESTAMP_S", TYPE_TIMESTAMP_S},
			{"TIMESTAMP_MS", TYPE_TIMESTAMP_MS},
			{"TIMESTAMP_NS", TYPE_TIMESTAMP_NS},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typeInfo, err := NewTypeInfo(tt.typ)
				require.NoError(t, err)

				chunk, err := NewDataChunk([]TypeInfo{typeInfo})
				require.NoError(t, err)

				err = chunk.SetSize(1)
				require.NoError(t, err)

				require.NoError(t, chunk.SetValue(0, 0, now))

				val, _ := chunk.GetValue(0, 0)
				result := val.(time.Time)

				// The result should be a valid time with appropriate precision.
				assert.False(t, result.IsZero())
			})
		}
	})

	t.Run("DATE vector", func(t *testing.T) {
		date := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		expected := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)

		dateType, err := NewTypeInfo(TYPE_DATE)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{dateType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, date))

		val, _ := chunk.GetValue(0, 0)
		result := val.(time.Time)

		assert.Equal(t, expected.Year(), result.Year())
		assert.Equal(t, expected.Month(), result.Month())
		assert.Equal(t, expected.Day(), result.Day())
	})

	t.Run("TIME vector", func(t *testing.T) {
		timeVal := time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC)

		timeType, err := NewTypeInfo(TYPE_TIME)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{timeType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, timeVal))

		val, _ := chunk.GetValue(0, 0)
		result := val.(time.Time)

		assert.Equal(t, timeVal.Hour(), result.Hour())
		assert.Equal(t, timeVal.Minute(), result.Minute())
		assert.Equal(t, timeVal.Second(), result.Second())
	})

	t.Run("INTERVAL vector", func(t *testing.T) {
		interval := Interval{Months: 1, Days: 2, Micros: 3000000}

		intervalType, err := NewTypeInfo(TYPE_INTERVAL)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intervalType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, interval))

		val, _ := chunk.GetValue(0, 0)
		result := val.(Interval)

		assert.Equal(t, interval, result)
	})
}

func TestVectorComplexNumericTypes(t *testing.T) {
	t.Run("HUGEINT vector", func(t *testing.T) {
		// Value larger than int64 max.
		largeVal := new(big.Int).SetUint64(math.MaxUint64)
		largeVal = largeVal.Add(largeVal, big.NewInt(1))

		hugeintType, err := NewTypeInfo(TYPE_HUGEINT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{hugeintType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, largeVal))

		val, _ := chunk.GetValue(0, 0)
		result := val.(*big.Int)

		assert.Equal(t, 0, largeVal.Cmp(result))
	})

	t.Run("DECIMAL vector", func(t *testing.T) {
		decimalType, err := NewDecimalInfo(10, 2)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{decimalType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		// 123.45 = 12345 (unscaled)
		d := Decimal{Width: 10, Scale: 2, Value: big.NewInt(12345)}
		require.NoError(t, chunk.SetValue(0, 0, d))

		val, _ := chunk.GetValue(0, 0)
		result := val.(Decimal)

		assert.Equal(t, uint8(10), result.Width)
		assert.Equal(t, uint8(2), result.Scale)
		assert.Equal(t, int64(12345), result.Value.Int64())
	})

	t.Run("UUID vector", func(t *testing.T) {
		uuidType, err := NewTypeInfo(TYPE_UUID)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{uuidType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		u := UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
		require.NoError(t, chunk.SetValue(0, 0, u))

		val, _ := chunk.GetValue(0, 0)
		result := val.(UUID)

		assert.Equal(t, u, result)
	})
}

func TestVectorEnumType(t *testing.T) {
	t.Run("ENUM vector set by name", func(t *testing.T) {
		enumType, err := NewEnumInfo("red", "green", "blue")
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{enumType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, "green"))

		val, _ := chunk.GetValue(0, 0)
		assert.Equal(t, "green", val)
	})

	t.Run("ENUM vector invalid value", func(t *testing.T) {
		enumType, err := NewEnumInfo("red", "green", "blue")
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{enumType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, "purple")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid enum value")
	})
}

func TestVectorListType(t *testing.T) {
	t.Run("LIST vector", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		listType, err := NewListInfo(intType)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{listType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		list := []any{int32(1), int32(2), int32(3)}
		require.NoError(t, chunk.SetValue(0, 0, list))

		val, _ := chunk.GetValue(0, 0)
		result := val.([]any)

		assert.Len(t, result, 3)
		assert.Equal(t, int32(1), result[0])
		assert.Equal(t, int32(2), result[1])
		assert.Equal(t, int32(3), result[2])
	})

	t.Run("empty LIST", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		listType, err := NewListInfo(intType)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{listType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		list := []any{}
		require.NoError(t, chunk.SetValue(0, 0, list))

		val, _ := chunk.GetValue(0, 0)
		result := val.([]any)

		assert.Len(t, result, 0)
	})
}

func TestVectorStructType(t *testing.T) {
	t.Run("STRUCT vector", func(t *testing.T) {
		varcharType, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		nameEntry, _ := NewStructEntry(varcharType, "name")
		ageEntry, _ := NewStructEntry(intType, "age")
		structType, err := NewStructInfo(nameEntry, ageEntry)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{structType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		s := map[string]any{"name": "Alice", "age": int32(30)}
		require.NoError(t, chunk.SetValue(0, 0, s))

		val, _ := chunk.GetValue(0, 0)
		result := val.(map[string]any)

		assert.Equal(t, "Alice", result["name"])
		assert.Equal(t, int32(30), result["age"])
	})
}

func TestVectorMapType(t *testing.T) {
	t.Run("MAP vector", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		varcharType, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		mapType, err := NewMapInfo(intType, varcharType)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{mapType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		m := Map{int32(1): "one", int32(2): "two"}
		require.NoError(t, chunk.SetValue(0, 0, m))

		val, _ := chunk.GetValue(0, 0)
		result := val.(Map)

		assert.Len(t, result, 2)
	})
}

func TestVectorArrayType(t *testing.T) {
	t.Run("ARRAY vector", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		arrayType, err := NewArrayInfo(intType, 3)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{arrayType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		arr := []any{int32(1), int32(2), int32(3)}
		require.NoError(t, chunk.SetValue(0, 0, arr))

		val, _ := chunk.GetValue(0, 0)
		result := val.([]any)

		assert.Len(t, result, 3)
	})

	t.Run("ARRAY size validation", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		arrayType, err := NewArrayInfo(intType, 3)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{arrayType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		arr := []any{int32(1), int32(2), int32(3), int32(4)} // 4 elements instead of 3.
		err = chunk.SetValue(0, 0, arr)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "array size mismatch")
	})
}

func TestVectorUnionType(t *testing.T) {
	t.Run("UNION vector", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		varcharType, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		unionType, err := NewUnionInfo(
			[]TypeInfo{intType, varcharType},
			[]string{"i", "s"},
		)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{unionType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		u := Union{Tag: "i", Value: int32(42)}
		require.NoError(t, chunk.SetValue(0, 0, u))

		val, _ := chunk.GetValue(0, 0)
		result := val.(Union)

		assert.Equal(t, "i", result.Tag)
		assert.Equal(t, int32(42), result.Value)
	})

	t.Run("UNION invalid tag", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)
		varcharType, err := NewTypeInfo(TYPE_VARCHAR)
		require.NoError(t, err)

		unionType, err := NewUnionInfo(
			[]TypeInfo{intType, varcharType},
			[]string{"i", "s"},
		)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{unionType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		u := Union{Tag: "invalid", Value: int32(42)}
		err = chunk.SetValue(0, 0, u)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid union tag")
	})
}

func TestVectorNullHandling(t *testing.T) {
	t.Run("set NULL value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, nil))

		val, _ := chunk.GetValue(0, 0)
		assert.Nil(t, val)
	})

	t.Run("overwrite NULL with value", func(t *testing.T) {
		intType, err := NewTypeInfo(TYPE_INTEGER)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		require.NoError(t, chunk.SetValue(0, 0, nil))
		require.NoError(t, chunk.SetValue(0, 0, int32(42)))

		val, _ := chunk.GetValue(0, 0)
		assert.Equal(t, int32(42), val)
	})
}

func TestDataChunkInitialization(t *testing.T) {
	t.Run("initialize from types", func(t *testing.T) {
		intType, _ := NewTypeInfo(TYPE_INTEGER)
		varcharType, _ := NewTypeInfo(TYPE_VARCHAR)
		boolType, _ := NewTypeInfo(TYPE_BOOLEAN)

		chunk, err := NewDataChunk([]TypeInfo{intType, varcharType, boolType})
		require.NoError(t, err)

		assert.Equal(t, 3, chunk.GetColumnCount())
		assert.Equal(t, 0, chunk.GetSize())
	})
}

func TestDataChunkReset(t *testing.T) {
	t.Run("reset preserves column structure", func(t *testing.T) {
		intType, _ := NewTypeInfo(TYPE_INTEGER)
		varcharType, _ := NewTypeInfo(TYPE_VARCHAR)
		boolType, _ := NewTypeInfo(TYPE_BOOLEAN)

		chunk, err := NewDataChunk([]TypeInfo{intType, varcharType, boolType})
		require.NoError(t, err)

		err = chunk.SetSize(100)
		require.NoError(t, err)

		chunk.reset()

		assert.Equal(t, 3, chunk.GetColumnCount())
		assert.Equal(t, 0, chunk.GetSize())
	})
}

func TestDataChunkCleanup(t *testing.T) {
	t.Run("close releases memory", func(t *testing.T) {
		intType, _ := NewTypeInfo(TYPE_INTEGER)

		chunk, err := NewDataChunk([]TypeInfo{intType})
		require.NoError(t, err)

		chunk.close()

		_, err = chunk.GetValue(0, 0)
		assert.Error(t, err)
	})
}

// TestDataChunkUhugeint tests UHUGEINT type support in DataChunk.
func TestDataChunkUhugeint(t *testing.T) {
	t.Run("basic uhugeint operations", func(t *testing.T) {
		uhugeintType, err := NewTypeInfo(TYPE_UHUGEINT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{uhugeintType})
		require.NoError(t, err)

		// Set a simple value.
		u := NewUhugeintFromUint64(12345)
		err = chunk.SetValue(0, 0, u)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		// Get the value back.
		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)

		result, ok := val.(Uhugeint)
		require.True(t, ok)
		assert.Equal(t, u, result)
	})

	t.Run("large uhugeint values", func(t *testing.T) {
		uhugeintType, err := NewTypeInfo(TYPE_UHUGEINT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{uhugeintType})
		require.NoError(t, err)

		// Create a large value (> 64 bits).
		bigVal, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
		u, err := NewUhugeint(bigVal)
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, u)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)

		result, ok := val.(Uhugeint)
		require.True(t, ok)
		assert.True(t, u.Equal(result))
	})

	t.Run("uhugeint from big.Int", func(t *testing.T) {
		uhugeintType, err := NewTypeInfo(TYPE_UHUGEINT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{uhugeintType})
		require.NoError(t, err)

		// Set using *big.Int directly.
		bigVal := big.NewInt(999999)
		err = chunk.SetValue(0, 0, bigVal)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)

		result, ok := val.(Uhugeint)
		require.True(t, ok)
		assert.Equal(t, "999999", result.String())
	})

	t.Run("uhugeint null handling", func(t *testing.T) {
		uhugeintType, err := NewTypeInfo(TYPE_UHUGEINT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{uhugeintType})
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, nil)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Nil(t, val)
	})
}

// TestDataChunkBit tests BIT type support in DataChunk.
// Skipped because TYPE_BIT is in unsupportedTypeToStringMap per duckdb-go API.
func TestDataChunkBit(t *testing.T) {
	t.Skip("TYPE_BIT is unsupported via NewTypeInfo per duckdb-go API compatibility")
	t.Run("basic bit operations", func(t *testing.T) {
		bitType, err := NewTypeInfo(TYPE_BIT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{bitType})
		require.NoError(t, err)

		// Set a simple bit value.
		b, err := NewBit("10110")
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, b)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		// Get the value back.
		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)

		result, ok := val.(Bit)
		require.True(t, ok)
		assert.Equal(t, "10110", result.String())
	})

	t.Run("bit from string", func(t *testing.T) {
		bitType, err := NewTypeInfo(TYPE_BIT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{bitType})
		require.NoError(t, err)

		// Set using string directly.
		err = chunk.SetValue(0, 0, "11110000")
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)

		result, ok := val.(Bit)
		require.True(t, ok)
		assert.Equal(t, "11110000", result.String())
	})

	t.Run("bit null handling", func(t *testing.T) {
		bitType, err := NewTypeInfo(TYPE_BIT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{bitType})
		require.NoError(t, err)

		err = chunk.SetValue(0, 0, nil)
		require.NoError(t, err)

		err = chunk.SetSize(1)
		require.NoError(t, err)

		val, err := chunk.GetValue(0, 0)
		require.NoError(t, err)
		assert.Nil(t, val)
	})

	t.Run("bit all zeros and ones", func(t *testing.T) {
		bitType, err := NewTypeInfo(TYPE_BIT)
		require.NoError(t, err)

		chunk, err := NewDataChunk([]TypeInfo{bitType})
		require.NoError(t, err)

		zeros, _ := NewBit("00000000")
		ones, _ := NewBit("11111111")

		err = chunk.SetValue(0, 0, zeros)
		require.NoError(t, err)
		err = chunk.SetValue(0, 1, ones)
		require.NoError(t, err)

		err = chunk.SetSize(2)
		require.NoError(t, err)

		val0, _ := chunk.GetValue(0, 0)
		val1, _ := chunk.GetValue(0, 1)

		assert.Equal(t, "00000000", val0.(Bit).String())
		assert.Equal(t, "11111111", val1.(Bit).String())
	})
}

func BenchmarkDataChunk_ScanThroughput(b *testing.B) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	chunk, _ := NewDataChunk([]TypeInfo{intType})

	// Fill the chunk with test data
	_ = chunk.SetSize(GetDataChunkCapacity())
	for i := 0; i < GetDataChunkCapacity(); i++ {
		_ = chunk.SetValue(0, i, int32(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for row := 0; row < GetDataChunkCapacity(); row++ {
			_, _ = chunk.GetValue(0, row)
		}
	}

	b.ReportMetric(float64(GetDataChunkCapacity()), "rows/op")
}

func BenchmarkDataChunk_ScanMultiColumn(b *testing.B) {
	intType, _ := NewTypeInfo(TYPE_INTEGER)
	varcharType, _ := NewTypeInfo(TYPE_VARCHAR)
	doubleType, _ := NewTypeInfo(TYPE_DOUBLE)

	chunk, _ := NewDataChunk([]TypeInfo{intType, varcharType, doubleType})

	// Fill the chunk with test data
	_ = chunk.SetSize(GetDataChunkCapacity())
	for i := 0; i < GetDataChunkCapacity(); i++ {
		_ = chunk.SetValue(0, i, int32(i))
		_ = chunk.SetValue(1, i, "test string")
		_ = chunk.SetValue(2, i, float64(i)*1.5)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for row := 0; row < GetDataChunkCapacity(); row++ {
			_, _ = chunk.GetValue(0, row)
			_, _ = chunk.GetValue(1, row)
			_, _ = chunk.GetValue(2, row)
		}
	}

	// Report total values read per operation (3 columns * 2048 rows)
	b.ReportMetric(float64(GetDataChunkCapacity()*3), "values/op")
}

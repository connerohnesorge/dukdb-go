package dukdb

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertToType_ExactMatch(t *testing.T) {
	// Test exact type matches (fast path)
	t.Run("int64", func(t *testing.T) {
		result, err := convertToType[int64](int64(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)
	})

	t.Run("string", func(t *testing.T) {
		result, err := convertToType[string]("hello")
		require.NoError(t, err)
		assert.Equal(t, "hello", result)
	})

	t.Run("float64", func(t *testing.T) {
		result, err := convertToType[float64](3.14)
		require.NoError(t, err)
		assert.Equal(t, 3.14, result)
	})

	t.Run("nil", func(t *testing.T) {
		result, err := convertToType[int64](nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result)
	})
}

func TestConvertToType_NumericConversions(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected int64
	}{
		{"int to int64", int(42), 42},
		{"int32 to int64", int32(42), 42},
		{"int16 to int64", int16(42), 42},
		{"int8 to int64", int8(42), 42},
		{"uint to int64", uint(42), 42},
		{"uint64 to int64", uint64(42), 42},
		{"uint32 to int64", uint32(42), 42},
		{"float64 to int64", float64(42.0), 42},
		{"float32 to int64", float32(42.0), 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToType[int64](tt.src)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToType_FloatConversions(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected float64
	}{
		{"int64 to float64", int64(42), 42.0},
		{"int32 to float64", int32(42), 42.0},
		{"float32 to float64", float32(3.14), float64(float32(3.14))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToType[float64](tt.src)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConvertToType_PointerTypes(t *testing.T) {
	t.Run("value to pointer", func(t *testing.T) {
		result, err := convertToType[*int64](int64(42))
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, int64(42), *result)
	})

	t.Run("nil to pointer", func(t *testing.T) {
		result, err := convertToType[*int64](nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestConvertToType_TimeConversion(t *testing.T) {
	now := time.Now()
	result, err := convertToType[time.Time](now)
	require.NoError(t, err)
	assert.Equal(t, now, result)
}

func TestConvertToType_BigIntConversion(t *testing.T) {
	bi := big.NewInt(12345)
	result, err := convertToType[*big.Int](bi)
	require.NoError(t, err)
	assert.Equal(t, bi, result)
}

func TestConvertToType_Errors(t *testing.T) {
	t.Run("incompatible types", func(t *testing.T) {
		_, err := convertToType[int64]("not a number that can be parsed")
		assert.Error(t, err)
	})

	t.Run("struct to int", func(t *testing.T) {
		type MyStruct struct{ X int }
		_, err := convertToType[int64](MyStruct{X: 1})
		assert.Error(t, err)
	})
}

func TestIsNumericKind(t *testing.T) {
	numericKinds := []reflect.Kind{
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
	}

	for _, k := range numericKinds {
		t.Run(k.String(), func(t *testing.T) {
			assert.True(t, isNumericKind(k))
		})
	}

	nonNumericKinds := []reflect.Kind{
		reflect.String, reflect.Bool, reflect.Struct, reflect.Slice,
		reflect.Map, reflect.Ptr, reflect.Interface,
	}

	for _, k := range nonNumericKinds {
		t.Run(k.String(), func(t *testing.T) {
			assert.False(t, isNumericKind(k))
		})
	}
}

func TestSetFieldValue(t *testing.T) {
	t.Run("nil to non-pointer field", func(t *testing.T) {
		var x int64
		v := reflect.ValueOf(&x).Elem()
		err := setFieldValue(v, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), x)
	})

	t.Run("nil to pointer field", func(t *testing.T) {
		var x *int64
		v := reflect.ValueOf(&x).Elem()
		err := setFieldValue(v, nil)
		require.NoError(t, err)
		assert.Nil(t, x)
	})

	t.Run("value to field", func(t *testing.T) {
		var x int64
		v := reflect.ValueOf(&x).Elem()
		err := setFieldValue(v, int64(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), x)
	})

	t.Run("convertible value to field", func(t *testing.T) {
		var x int64
		v := reflect.ValueOf(&x).Elem()
		err := setFieldValue(v, int32(42))
		require.NoError(t, err)
		assert.Equal(t, int64(42), x)
	})

	t.Run("nested struct field", func(t *testing.T) {
		type Inner struct {
			Name string `duckdb:"name"`
			Age  int    `duckdb:"age"`
		}
		var inner Inner
		v := reflect.ValueOf(&inner).Elem()
		err := setFieldValue(v, map[string]any{
			"name": "Alice",
			"age":  30,
		})
		require.NoError(t, err)
		assert.Equal(t, "Alice", inner.Name)
		assert.Equal(t, 30, inner.Age)
	})

	t.Run("nested slice field", func(t *testing.T) {
		var nums []int
		v := reflect.ValueOf(&nums).Elem()
		err := setFieldValue(v, []any{1, 2, 3})
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, nums)
	})

	t.Run("nested map field", func(t *testing.T) {
		var m map[string]int
		v := reflect.ValueOf(&m).Elem()
		err := setFieldValue(v, map[any]any{
			"one": 1,
			"two": 2,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, m["one"])
		assert.Equal(t, 2, m["two"])
	})
}

func TestToAnySlice(t *testing.T) {
	t.Run("int slice", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := toAnySlice(input)
		assert.Equal(t, []any{1, 2, 3}, result)
	})

	t.Run("string slice", func(t *testing.T) {
		input := []string{"a", "b", "c"}
		result := toAnySlice(input)
		assert.Equal(t, []any{"a", "b", "c"}, result)
	})

	t.Run("empty slice", func(t *testing.T) {
		input := []int{}
		result := toAnySlice(input)
		assert.Equal(t, []any{}, result)
	})
}

func TestParseUUID(t *testing.T) {
	t.Run("valid UUID with dashes", func(t *testing.T) {
		result, err := parseUUID("550e8400-e29b-41d4-a716-446655440000")
		require.NoError(t, err)
		expected := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("valid UUID without dashes", func(t *testing.T) {
		result, err := parseUUID("550e8400e29b41d4a716446655440000")
		require.NoError(t, err)
		expected := [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}
		assert.Equal(t, expected, result)
	})

	t.Run("invalid length", func(t *testing.T) {
		_, err := parseUUID("550e8400")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid UUID length")
	})

	t.Run("invalid hex", func(t *testing.T) {
		_, err := parseUUID("ZZZZ8400-e29b-41d4-a716-446655440000")
		assert.Error(t, err)
	})
}

func TestConvertToTime(t *testing.T) {
	t.Run("time.Time passthrough", func(t *testing.T) {
		now := time.Now()
		result, err := convertToTime(now)
		require.NoError(t, err)
		assert.Equal(t, now, result)
	})

	t.Run("int64 nanoseconds", func(t *testing.T) {
		ns := int64(1704067200000000000) // 2024-01-01 00:00:00 UTC
		result, err := convertToTime(ns)
		require.NoError(t, err)
		expected := time.Unix(0, ns)
		assert.Equal(t, expected, result)
	})

	t.Run("RFC3339 string", func(t *testing.T) {
		result, err := convertToTime("2024-01-01T12:00:00Z")
		require.NoError(t, err)
		expected := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		assert.Equal(t, expected, result)
	})

	t.Run("date only string", func(t *testing.T) {
		result, err := convertToTime("2024-01-01")
		require.NoError(t, err)
		expected := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, expected, result)
	})

	t.Run("invalid type", func(t *testing.T) {
		_, err := convertToTime(42)
		assert.Error(t, err)
	})
}

func TestConvertToBigInt(t *testing.T) {
	t.Run("big.Int passthrough", func(t *testing.T) {
		bi := big.NewInt(12345)
		result, err := convertToBigInt(bi)
		require.NoError(t, err)
		assert.Equal(t, bi, result)
	})

	t.Run("int64", func(t *testing.T) {
		result, err := convertToBigInt(int64(12345))
		require.NoError(t, err)
		assert.Equal(t, big.NewInt(12345), result)
	})

	t.Run("int", func(t *testing.T) {
		result, err := convertToBigInt(42)
		require.NoError(t, err)
		assert.Equal(t, big.NewInt(42), result)
	})

	t.Run("uint64", func(t *testing.T) {
		result, err := convertToBigInt(uint64(12345))
		require.NoError(t, err)
		assert.Equal(t, big.NewInt(12345), result)
	})

	t.Run("string", func(t *testing.T) {
		result, err := convertToBigInt("12345678901234567890")
		require.NoError(t, err)
		expected, _ := new(big.Int).SetString("12345678901234567890", 10)
		assert.Equal(t, expected, result)
	})

	t.Run("invalid string", func(t *testing.T) {
		_, err := convertToBigInt("not a number")
		assert.Error(t, err)
	})
}

func TestParseDecimal(t *testing.T) {
	t.Run("simple integer", func(t *testing.T) {
		result, err := ParseDecimal("123")
		require.NoError(t, err)
		assert.Equal(t, uint8(3), result.Width)
		assert.Equal(t, uint8(0), result.Scale)
		assert.Equal(t, big.NewInt(123), result.Value)
	})

	t.Run("decimal with fraction", func(t *testing.T) {
		result, err := ParseDecimal("123.45")
		require.NoError(t, err)
		assert.Equal(t, uint8(5), result.Width)
		assert.Equal(t, uint8(2), result.Scale)
		assert.Equal(t, big.NewInt(12345), result.Value)
	})

	t.Run("negative decimal", func(t *testing.T) {
		result, err := ParseDecimal("-123.45")
		require.NoError(t, err)
		assert.Equal(t, uint8(5), result.Width)
		assert.Equal(t, uint8(2), result.Scale)
		assert.Equal(t, big.NewInt(-12345), result.Value)
	})

	t.Run("leading zeros", func(t *testing.T) {
		result, err := ParseDecimal("0.123")
		require.NoError(t, err)
		assert.Equal(t, uint8(4), result.Width)
		assert.Equal(t, uint8(3), result.Scale)
		assert.Equal(t, big.NewInt(123), result.Value)
	})

	t.Run("empty string", func(t *testing.T) {
		_, err := ParseDecimal("")
		assert.Error(t, err)
	})

	t.Run("multiple decimal points", func(t *testing.T) {
		_, err := ParseDecimal("1.2.3")
		assert.Error(t, err)
	})
}

func TestParseInterval(t *testing.T) {
	t.Run("years and months", func(t *testing.T) {
		result, err := ParseInterval("1 year 2 months")
		require.NoError(t, err)
		assert.Equal(t, int32(14), result.Months) // 12 + 2
		assert.Equal(t, int32(0), result.Days)
		assert.Equal(t, int64(0), result.Micros)
	})

	t.Run("days", func(t *testing.T) {
		result, err := ParseInterval("5 days")
		require.NoError(t, err)
		assert.Equal(t, int32(0), result.Months)
		assert.Equal(t, int32(5), result.Days)
		assert.Equal(t, int64(0), result.Micros)
	})

	t.Run("hours and minutes", func(t *testing.T) {
		result, err := ParseInterval("2 hours 30 minutes")
		require.NoError(t, err)
		assert.Equal(t, int32(0), result.Months)
		assert.Equal(t, int32(0), result.Days)
		expectedMicros := int64(2*3600+30*60) * 1000000
		assert.Equal(t, expectedMicros, result.Micros)
	})

	t.Run("complex interval", func(t *testing.T) {
		result, err := ParseInterval("1 year 2 months 3 days 4 hours")
		require.NoError(t, err)
		assert.Equal(t, int32(14), result.Months)
		assert.Equal(t, int32(3), result.Days)
		assert.Equal(t, int64(4*3600*1000000), result.Micros)
	})

	t.Run("empty string", func(t *testing.T) {
		result, err := ParseInterval("")
		require.NoError(t, err)
		assert.Equal(t, Interval{}, result)
	})

	t.Run("plural units", func(t *testing.T) {
		result, err := ParseInterval("2 years 3 months 4 days")
		require.NoError(t, err)
		assert.Equal(t, int32(27), result.Months) // 24 + 3
		assert.Equal(t, int32(4), result.Days)
	})
}

func TestNewDecimalFromFloat(t *testing.T) {
	t.Run("positive float", func(t *testing.T) {
		result := NewDecimalFromFloat(123.45)
		assert.NotNil(t, result.Value)
		// The exact representation may vary, just ensure it's valid
		f := result.Float64()
		assert.InDelta(t, 123.45, f, 0.001)
	})

	t.Run("negative float", func(t *testing.T) {
		result := NewDecimalFromFloat(-123.45)
		assert.NotNil(t, result.Value)
		f := result.Float64()
		assert.InDelta(t, -123.45, f, 0.001)
	})

	t.Run("zero", func(t *testing.T) {
		result := NewDecimalFromFloat(0.0)
		assert.NotNil(t, result.Value)
		assert.Equal(t, 0.0, result.Float64())
	})
}

func TestConvertToDecimal(t *testing.T) {
	t.Run("Decimal passthrough", func(t *testing.T) {
		d := Decimal{Width: 5, Scale: 2, Value: big.NewInt(12345)}
		result, err := convertToDecimal(d)
		require.NoError(t, err)
		assert.Equal(t, d, result)
	})

	t.Run("float64", func(t *testing.T) {
		result, err := convertToDecimal(123.45)
		require.NoError(t, err)
		assert.InDelta(t, 123.45, result.Float64(), 0.001)
	})

	t.Run("string", func(t *testing.T) {
		result, err := convertToDecimal("123.45")
		require.NoError(t, err)
		assert.Equal(t, big.NewInt(12345), result.Value)
		assert.Equal(t, uint8(2), result.Scale)
	})
}

func TestConvertToInterval(t *testing.T) {
	t.Run("Interval passthrough", func(t *testing.T) {
		i := Interval{Months: 1, Days: 2, Micros: 3}
		result, err := convertToInterval(i)
		require.NoError(t, err)
		assert.Equal(t, i, result)
	})

	t.Run("string", func(t *testing.T) {
		result, err := convertToInterval("1 month 2 days")
		require.NoError(t, err)
		assert.Equal(t, int32(1), result.Months)
		assert.Equal(t, int32(2), result.Days)
	})
}

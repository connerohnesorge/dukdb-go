package dukdb

import (
	"database/sql"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test scanning primitive types

func TestScanBool(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected bool
	}{
		{"from bool true", true, true},
		{"from bool false", false, false},
		{"from int64 1", int64(1), true},
		{"from int64 0", int64(0), false},
		{"from float64 1.0", float64(1.0), true},
		{"from float64 0.0", float64(0.0), false},
		{"from string true", "true", true},
		{"from string false", "false", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest bool
			err := scanValue(tt.src, &dest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dest)
		})
	}
}

func TestScanInt(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected int64
	}{
		{"from int", int(42), 42},
		{"from int8", int8(42), 42},
		{"from int16", int16(42), 42},
		{"from int32", int32(42), 42},
		{"from int64", int64(42), 42},
		{"from uint8", uint8(42), 42},
		{"from uint16", uint16(42), 42},
		{"from uint32", uint32(42), 42},
		{"from uint64", uint64(42), 42},
		{"from float32", float32(42.5), 42},
		{"from float64", float64(42.7), 42},
		{"from string", "42", 42},
		{"from bool true", true, 1},
		{"from bool false", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest int64
			err := scanValue(tt.src, &dest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dest)
		})
	}
}

func TestScanIntTypes(t *testing.T) {
	// Test different int sizes
	t.Run("int8", func(t *testing.T) {
		var dest int8
		err := scanValue(int64(127), &dest)
		require.NoError(t, err)
		assert.Equal(t, int8(127), dest)
	})

	t.Run("int16", func(t *testing.T) {
		var dest int16
		err := scanValue(int64(32767), &dest)
		require.NoError(t, err)
		assert.Equal(t, int16(32767), dest)
	})

	t.Run("int32", func(t *testing.T) {
		var dest int32
		err := scanValue(int64(2147483647), &dest)
		require.NoError(t, err)
		assert.Equal(t, int32(2147483647), dest)
	})
}

func TestScanIntOverflow(t *testing.T) {
	t.Run("int8 overflow", func(t *testing.T) {
		var dest int8
		err := scanValue(int64(128), &dest)
		require.Error(t, err)

		var dukErr *Error
		require.True(t, errors.As(err, &dukErr))
		assert.Equal(
			t,
			ErrorTypeOutOfRange,
			dukErr.Type,
		)
	})

	t.Run("int16 overflow", func(t *testing.T) {
		var dest int16
		err := scanValue(int64(32768), &dest)
		require.Error(t, err)
	})

	t.Run("int32 overflow", func(t *testing.T) {
		var dest int32
		err := scanValue(int64(2147483648), &dest)
		require.Error(t, err)
	})
}

func TestScanUint(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected uint64
	}{
		{"from int", int(42), 42},
		{"from int64", int64(42), 42},
		{"from uint64", uint64(42), 42},
		{"from float64", float64(42.7), 42},
		{"from string", "42", 42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest uint64
			err := scanValue(tt.src, &dest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dest)
		})
	}
}

func TestScanUintNegative(t *testing.T) {
	var dest uint64
	err := scanValue(int64(-1), &dest)
	require.Error(t, err)

	var dukErr *Error
	require.True(t, errors.As(err, &dukErr))
	assert.Equal(
		t,
		ErrorTypeOutOfRange,
		dukErr.Type,
	)
}

func TestScanUintOverflow(t *testing.T) {
	t.Run("uint8 overflow", func(t *testing.T) {
		var dest uint8
		err := scanValue(uint64(256), &dest)
		require.Error(t, err)

		var dukErr *Error
		require.True(t, errors.As(err, &dukErr))
		assert.Equal(
			t,
			ErrorTypeOutOfRange,
			dukErr.Type,
		)
	})
}

func TestScanFloat(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected float64
	}{
		{
			"from float32",
			float32(3.14),
			float64(float32(3.14)),
		},
		{
			"from float64",
			float64(3.14159),
			3.14159,
		},
		{"from int", int(42), 42.0},
		{"from int64", int64(42), 42.0},
		{"from uint64", uint64(42), 42.0},
		{"from string", "3.14", 3.14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest float64
			err := scanValue(tt.src, &dest)
			require.NoError(t, err)
			assert.InDelta(
				t,
				tt.expected,
				dest,
				0.0001,
			)
		})
	}
}

func TestScanFloat32(t *testing.T) {
	var dest float32
	err := scanValue(float64(3.14), &dest)
	require.NoError(t, err)
	assert.InDelta(t, 3.14, float64(dest), 0.01)
}

func TestScanString(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected string
	}{
		{"from string", "hello", "hello"},
		{"from []byte", []byte("world"), "world"},
		{"from int", int(42), "42"},
		{"from int64", int64(42), "42"},
		{"from float64", float64(3.14), "3.14"},
		{"from bool", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest string
			err := scanValue(tt.src, &dest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dest)
		})
	}
}

func TestScanBytes(t *testing.T) {
	tests := []struct {
		name     string
		src      any
		expected []byte
	}{
		{
			"from []byte",
			[]byte{1, 2, 3},
			[]byte{1, 2, 3},
		},
		{"from string", "hello", []byte("hello")},
		{
			"from hex string",
			"\\x48656c6c6f",
			[]byte("Hello"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dest []byte
			err := scanValue(tt.src, &dest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, dest)
		})
	}
}

// Test scanning custom DuckDB types

func TestScanUUID(t *testing.T) {
	t.Run("from string", func(t *testing.T) {
		var dest UUID
		err := scanValue(
			"550e8400-e29b-41d4-a716-446655440000",
			&dest,
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			"550e8400-e29b-41d4-a716-446655440000",
			dest.String(),
		)
	})

	t.Run("from bytes", func(t *testing.T) {
		var dest UUID
		bytes := []byte{
			0x55,
			0x0e,
			0x84,
			0x00,
			0xe2,
			0x9b,
			0x41,
			0xd4,
			0xa7,
			0x16,
			0x44,
			0x66,
			0x55,
			0x44,
			0x00,
			0x00,
		}
		err := scanValue(bytes, &dest)
		require.NoError(t, err)
		assert.Equal(
			t,
			"550e8400-e29b-41d4-a716-446655440000",
			dest.String(),
		)
	})
}

func TestScanInterval(t *testing.T) {
	t.Run("from Interval", func(t *testing.T) {
		var dest Interval
		src := Interval{
			Months: 12,
			Days:   30,
			Micros: 1000000,
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, src, dest)
	})

	t.Run("from map", func(t *testing.T) {
		var dest Interval
		src := map[string]any{
			"months": float64(12),
			"days":   float64(30),
			"micros": float64(1000000),
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, int32(12), dest.Months)
		assert.Equal(t, int32(30), dest.Days)
		assert.Equal(
			t,
			int64(1000000),
			dest.Micros,
		)
	})
}

func TestScanDecimal(t *testing.T) {
	t.Run("from Decimal", func(t *testing.T) {
		var dest Decimal
		src := Decimal{
			Width: 10,
			Scale: 2,
			Value: big.NewInt(12345),
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, src.Width, dest.Width)
		assert.Equal(t, src.Scale, dest.Scale)
		assert.Equal(
			t,
			src.Value.String(),
			dest.Value.String(),
		)
	})

	t.Run("from string", func(t *testing.T) {
		var dest Decimal
		err := scanValue("123.45", &dest)
		require.NoError(t, err)
		assert.Equal(t, "123.45", dest.String())
	})

	t.Run(
		"from string negative",
		func(t *testing.T) {
			var dest Decimal
			err := scanValue("-987.654", &dest)
			require.NoError(t, err)
			assert.Equal(
				t,
				"-987.654",
				dest.String(),
			)
		},
	)
}

func TestScanBigInt(t *testing.T) {
	t.Run("from *big.Int", func(t *testing.T) {
		var dest *big.Int
		src := big.NewInt(9223372036854775807)
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(
			t,
			src.String(),
			dest.String(),
		)
	})

	t.Run(
		"to int64 from big.Int",
		func(t *testing.T) {
			var dest int64
			src := big.NewInt(12345)
			err := scanValue(src, &dest)
			require.NoError(t, err)
			assert.Equal(t, int64(12345), dest)
		},
	)
}

// Test scanning nested types

func TestScanList(t *testing.T) {
	t.Run("to []any", func(t *testing.T) {
		var dest []any
		src := []any{1, 2, 3}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, src, dest)
	})

	t.Run("to []int", func(t *testing.T) {
		var dest []int
		src := []any{int64(1), int64(2), int64(3)}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, dest)
	})
}

func TestScanStruct(t *testing.T) {
	t.Run(
		"to map[string]any",
		func(t *testing.T) {
			var dest map[string]any
			src := map[string]any{
				"name":  "test",
				"value": float64(42),
			}
			err := scanValue(src, &dest)
			require.NoError(t, err)
			assert.Equal(t, src, dest)
		},
	)

	t.Run("to custom struct", func(t *testing.T) {
		type Person struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}
		var dest Person
		src := map[string]any{
			"name": "Alice",
			"age":  int64(30),
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, "Alice", dest.Name)
		assert.Equal(t, 30, dest.Age)
	})
}

func TestScanMap(t *testing.T) {
	t.Run("to Map", func(t *testing.T) {
		var dest Map
		src := Map{
			"key1": "value1",
			"key2": "value2",
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, src, dest)
	})

	t.Run(
		"to map[string]string from Map",
		func(t *testing.T) {
			var dest map[string]string
			src := Map{
				"key1": "value1",
				"key2": "value2",
			}
			err := scanValue(src, &dest)
			require.NoError(t, err)
			assert.Equal(
				t,
				"value1",
				dest["key1"],
			)
			assert.Equal(
				t,
				"value2",
				dest["key2"],
			)
		},
	)
}

func TestScanUnion(t *testing.T) {
	t.Run("from Union", func(t *testing.T) {
		var dest Union
		src := Union{
			Tag:   "int_val",
			Value: int64(42),
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, src.Tag, dest.Tag)
		assert.Equal(t, src.Value, dest.Value)
	})

	t.Run("from map", func(t *testing.T) {
		var dest Union
		src := map[string]any{
			"tag":   "str_val",
			"value": "hello",
		}
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, "str_val", dest.Tag)
		assert.Equal(t, "hello", dest.Value)
	})
}

// Test NULL handling

func TestScanNullToPointer(t *testing.T) {
	t.Run("*int to nil", func(t *testing.T) {
		var dest *int
		dest = new(int)
		*dest = 42
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.Nil(t, dest)
	})

	t.Run("*string to nil", func(t *testing.T) {
		var dest *string
		dest = new(string)
		*dest = "hello"
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.Nil(t, dest)
	})
}

func TestScanNullToValue(t *testing.T) {
	t.Run("int to zero", func(t *testing.T) {
		var dest int = 42
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.Equal(t, 0, dest)
	})

	t.Run("string to empty", func(t *testing.T) {
		var dest string = "hello"
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.Equal(t, "", dest)
	})

	t.Run("bool to false", func(t *testing.T) {
		var dest bool = true
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.False(t, dest)
	})

	t.Run("float64 to zero", func(t *testing.T) {
		var dest float64 = 3.14
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.Equal(t, float64(0), dest)
	})
}

// Test sql.Scanner interface

type customScanner struct {
	value  any
	called bool
}

func (c *customScanner) Scan(src any) error {
	c.called = true
	c.value = src
	return nil
}

func TestScanSQLScanner(t *testing.T) {
	t.Run(
		"scanner receives value",
		func(t *testing.T) {
			scanner := &customScanner{}
			err := scanValue(
				"test value",
				scanner,
			)
			require.NoError(t, err)
			assert.True(t, scanner.called)
			assert.Equal(
				t,
				"test value",
				scanner.value,
			)
		},
	)

	t.Run(
		"scanner receives nil",
		func(t *testing.T) {
			scanner := &customScanner{}
			err := scanValue(nil, scanner)
			require.NoError(t, err)
			assert.True(t, scanner.called)
			assert.Nil(t, scanner.value)
		},
	)
}

// Verify customScanner implements sql.Scanner
var _ sql.Scanner = (*customScanner)(nil)

// Test error cases

func TestScanErrorCases(t *testing.T) {
	t.Run("dest not pointer", func(t *testing.T) {
		var dest int = 0
		err := scanValue(
			42,
			dest,
		) // Pass by value, not pointer
		require.Error(t, err)

		var dukErr *Error
		require.True(t, errors.As(err, &dukErr))
		assert.Equal(
			t,
			ErrorTypeInvalid,
			dukErr.Type,
		)
		assert.Contains(t, dukErr.Msg, "pointer")
	})

	t.Run(
		"dest is nil pointer",
		func(t *testing.T) {
			var dest *int = nil
			err := scanValue(42, dest)
			require.Error(t, err)

			var dukErr *Error
			require.True(
				t,
				errors.As(err, &dukErr),
			)
			assert.Equal(
				t,
				ErrorTypeInvalid,
				dukErr.Type,
			)
		},
	)

	t.Run("type mismatch", func(t *testing.T) {
		var dest int
		err := scanValue([]any{1, 2, 3}, &dest)
		require.Error(t, err)

		var dukErr *Error
		require.True(t, errors.As(err, &dukErr))
		assert.Equal(
			t,
			ErrorTypeInvalid,
			dukErr.Type,
		)
	})

	t.Run(
		"invalid string to bool",
		func(t *testing.T) {
			var dest bool
			err := scanValue("not-a-bool", &dest)
			require.Error(t, err)
		},
	)

	t.Run(
		"invalid string to int",
		func(t *testing.T) {
			var dest int
			err := scanValue(
				"not-a-number",
				&dest,
			)
			require.Error(t, err)
		},
	)
}

// Test time scanning

func TestScanTime(t *testing.T) {
	t.Run("from time.Time", func(t *testing.T) {
		var dest time.Time
		src := time.Date(
			2024,
			1,
			15,
			10,
			30,
			0,
			0,
			time.UTC,
		)
		err := scanValue(src, &dest)
		require.NoError(t, err)
		assert.Equal(t, src, dest)
	})

	t.Run(
		"from RFC3339 string",
		func(t *testing.T) {
			var dest time.Time
			err := scanValue(
				"2024-01-15T10:30:00Z",
				&dest,
			)
			require.NoError(t, err)
			assert.Equal(t, 2024, dest.Year())
			assert.Equal(
				t,
				time.January,
				dest.Month(),
			)
			assert.Equal(t, 15, dest.Day())
		},
	)

	t.Run("from date string", func(t *testing.T) {
		var dest time.Time
		err := scanValue("2024-01-15", &dest)
		require.NoError(t, err)
		assert.Equal(t, 2024, dest.Year())
		assert.Equal(
			t,
			time.January,
			dest.Month(),
		)
		assert.Equal(t, 15, dest.Day())
	})

	t.Run(
		"from datetime string",
		func(t *testing.T) {
			var dest time.Time
			err := scanValue(
				"2024-01-15 10:30:00",
				&dest,
			)
			require.NoError(t, err)
			assert.Equal(t, 10, dest.Hour())
			assert.Equal(t, 30, dest.Minute())
		},
	)

	t.Run(
		"from int64 microseconds",
		func(t *testing.T) {
			var dest time.Time
			// 2024-01-15 00:00:00 UTC in microseconds
			micros := int64(1705276800000000)
			err := scanValue(micros, &dest)
			require.NoError(t, err)
			assert.Equal(t, 2024, dest.Year())
		},
	)
}

// Test scanning into interface{}

func TestScanToAny(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		var dest any
		err := scanValue(int64(42), &dest)
		require.NoError(t, err)
		assert.Equal(t, int64(42), dest)
	})

	t.Run("string", func(t *testing.T) {
		var dest any
		err := scanValue("hello", &dest)
		require.NoError(t, err)
		assert.Equal(t, "hello", dest)
	})

	t.Run("nil", func(t *testing.T) {
		var dest any = "previous"
		err := scanValue(nil, &dest)
		require.NoError(t, err)
		assert.Nil(t, dest)
	})
}

// Test edge cases

func TestScanEdgeCases(t *testing.T) {
	t.Run("max int64", func(t *testing.T) {
		var dest int64
		err := scanValue(
			int64(math.MaxInt64),
			&dest,
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			int64(math.MaxInt64),
			dest,
		)
	})

	t.Run("max uint64", func(t *testing.T) {
		var dest uint64
		err := scanValue(
			uint64(math.MaxUint64),
			&dest,
		)
		require.NoError(t, err)
		assert.Equal(
			t,
			uint64(math.MaxUint64),
			dest,
		)
	})

	t.Run("empty string", func(t *testing.T) {
		var dest string
		err := scanValue("", &dest)
		require.NoError(t, err)
		assert.Equal(t, "", dest)
	})

	t.Run("empty slice", func(t *testing.T) {
		var dest []any
		err := scanValue([]any{}, &dest)
		require.NoError(t, err)
		assert.Empty(t, dest)
	})

	t.Run("empty map", func(t *testing.T) {
		var dest map[string]any
		err := scanValue(map[string]any{}, &dest)
		require.NoError(t, err)
		assert.Empty(t, dest)
	})
}

// Test pointer to pointer scanning

func TestScanPointerToPointer(t *testing.T) {
	t.Run("**int", func(t *testing.T) {
		var dest *int
		err := scanValue(int64(42), &dest)
		require.NoError(t, err)
		require.NotNil(t, dest)
		assert.Equal(t, 42, *dest)
	})

	t.Run("**string", func(t *testing.T) {
		var dest *string
		err := scanValue("hello", &dest)
		require.NoError(t, err)
		require.NotNil(t, dest)
		assert.Equal(t, "hello", *dest)
	})
}

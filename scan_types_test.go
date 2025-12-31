package dukdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListScanner tests the ListScanner type.
func TestListScanner(t *testing.T) {
	t.Run("scan int64 slice", func(t *testing.T) {
		var result []int64
		scanner := ScanList(&result)
		err := scanner.Scan(
			[]any{int64(1), int64(2), int64(3)},
		)
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3}, result)
	})

	t.Run(
		"scan string slice",
		func(t *testing.T) {
			var result []string
			scanner := ScanList(&result)
			err := scanner.Scan(
				[]any{"a", "b", "c"},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				[]string{"a", "b", "c"},
				result,
			)
		},
	)

	t.Run(
		"scan float64 slice",
		func(t *testing.T) {
			var result []float64
			scanner := ScanList(&result)
			err := scanner.Scan(
				[]any{1.1, 2.2, 3.3},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				[]float64{1.1, 2.2, 3.3},
				result,
			)
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		var result []int64
		scanner := ScanList(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("scan empty slice", func(t *testing.T) {
		var result []int64
		scanner := ScanList(&result)
		err := scanner.Scan([]any{})
		require.NoError(t, err)
		assert.Equal(t, []int64{}, result)
	})

	t.Run(
		"scan with type conversion",
		func(t *testing.T) {
			var result []int64
			scanner := ScanList(&result)
			// int32 should convert to int64
			err := scanner.Scan(
				[]any{
					int32(1),
					int32(2),
					int32(3),
				},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				[]int64{1, 2, 3},
				result,
			)
		},
	)

	t.Run(
		"scan invalid type",
		func(t *testing.T) {
			var result []int64
			scanner := ScanList(&result)
			err := scanner.Scan("not a slice")
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"cannot scan",
			)
		},
	)

	t.Run(
		"scan with conversion error",
		func(t *testing.T) {
			var result []int64
			scanner := ScanList(&result)
			// Can't convert struct to int64
			type dummy struct{}
			err := scanner.Scan([]any{dummy{}})
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"list element 0",
			)
		},
	)
}

// TestArrayScanner tests the ArrayScanner type with size validation.
func TestArrayScanner(t *testing.T) {
	t.Run(
		"scan with correct size",
		func(t *testing.T) {
			var result []int64
			scanner := ScanArray(&result, 3)
			err := scanner.Scan(
				[]any{
					int64(1),
					int64(2),
					int64(3),
				},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				[]int64{1, 2, 3},
				result,
			)
		},
	)

	t.Run(
		"scan with wrong size",
		func(t *testing.T) {
			var result []int64
			scanner := ScanArray(&result, 5)
			err := scanner.Scan(
				[]any{
					int64(1),
					int64(2),
					int64(3),
				},
			)
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"array size mismatch",
			)
		},
	)

	t.Run(
		"scan with any size",
		func(t *testing.T) {
			var result []int64
			scanner := ScanArray(&result, -1)
			err := scanner.Scan(
				[]any{int64(1), int64(2)},
			)
			require.NoError(t, err)
			assert.Equal(t, []int64{1, 2}, result)
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		var result []int64
		scanner := ScanArray(&result, 3)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

// TestStructScanner tests the StructScanner type.
func TestStructScanner(t *testing.T) {
	t.Run(
		"scan simple struct",
		func(t *testing.T) {
			type Person struct {
				Name string `duckdb:"name"`
				Age  int    `duckdb:"age"`
			}
			var result Person
			scanner := ScanStruct(&result)
			err := scanner.Scan(map[string]any{
				"name": "Alice",
				"age":  30,
			})
			require.NoError(t, err)
			assert.Equal(t, "Alice", result.Name)
			assert.Equal(t, 30, result.Age)
		},
	)

	t.Run(
		"scan with field name tag",
		func(t *testing.T) {
			type Record struct {
				ID    int64  `duckdb:"record_id"`
				Value string `duckdb:"record_value"`
			}
			var result Record
			scanner := ScanStruct(&result)
			err := scanner.Scan(map[string]any{
				"record_id":    int64(42),
				"record_value": "test",
			})
			require.NoError(t, err)
			assert.Equal(t, int64(42), result.ID)
			assert.Equal(t, "test", result.Value)
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		type Person struct {
			Name string
		}
		var result Person
		scanner := ScanStruct(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		// Result should be unchanged (zero value)
		assert.Equal(t, "", result.Name)
	})

	t.Run(
		"scan with missing fields",
		func(t *testing.T) {
			type Person struct {
				Name    string `duckdb:"name"`
				Age     int    `duckdb:"age"`
				Country string `duckdb:"country"`
			}
			var result Person
			scanner := ScanStruct(&result)
			err := scanner.Scan(map[string]any{
				"name": "Bob",
				// age and country missing
			})
			require.NoError(t, err)
			assert.Equal(t, "Bob", result.Name)
			assert.Equal(
				t,
				0,
				result.Age,
			) // zero value
			assert.Equal(
				t,
				"",
				result.Country,
			) // zero value
		},
	)

	t.Run(
		"scan with nested struct",
		func(t *testing.T) {
			type Address struct {
				City    string `duckdb:"city"`
				Country string `duckdb:"country"`
			}
			type Person struct {
				Name    string  `duckdb:"name"`
				Address Address `duckdb:"address"`
			}
			var result Person
			scanner := ScanStruct(&result)
			err := scanner.Scan(map[string]any{
				"name": "Charlie",
				"address": map[string]any{
					"city":    "NYC",
					"country": "USA",
				},
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"Charlie",
				result.Name,
			)
			assert.Equal(
				t,
				"NYC",
				result.Address.City,
			)
			assert.Equal(
				t,
				"USA",
				result.Address.Country,
			)
		},
	)

	t.Run(
		"scan invalid type",
		func(t *testing.T) {
			type Person struct {
				Name string
			}
			var result Person
			scanner := ScanStruct(&result)
			err := scanner.Scan("not a map")
			assert.Error(t, err)
		},
	)
}

// TestMapScanner tests the MapScanner type.
func TestMapScanner(t *testing.T) {
	t.Run(
		"scan string to int map",
		func(t *testing.T) {
			var result map[string]int
			scanner := ScanMap(&result)
			err := scanner.Scan(map[any]any{
				"one":   1,
				"two":   2,
				"three": 3,
			})
			require.NoError(t, err)
			assert.Equal(t, 1, result["one"])
			assert.Equal(t, 2, result["two"])
			assert.Equal(t, 3, result["three"])
		},
	)

	t.Run(
		"scan int to string map",
		func(t *testing.T) {
			var result map[int]string
			scanner := ScanMap(&result)
			err := scanner.Scan(map[any]any{
				1: "one",
				2: "two",
			})
			require.NoError(t, err)
			assert.Equal(t, "one", result[1])
			assert.Equal(t, "two", result[2])
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		var result map[string]int
		scanner := ScanMap(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("scan empty map", func(t *testing.T) {
		var result map[string]int
		scanner := ScanMap(&result)
		err := scanner.Scan(map[any]any{})
		require.NoError(t, err)
		assert.Equal(t, map[string]int{}, result)
	})

	t.Run(
		"scan with null key error",
		func(t *testing.T) {
			var result map[string]int
			scanner := ScanMap(&result)
			err := scanner.Scan(map[any]any{
				nil: 1, // NULL key should error
			})
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"map key cannot be NULL",
			)
		},
	)

	t.Run(
		"scan with null value",
		func(t *testing.T) {
			var result map[string]int
			scanner := ScanMap(&result)
			err := scanner.Scan(map[any]any{
				"key": nil, // NULL value should become zero value
			})
			require.NoError(t, err)
			assert.Equal(t, 0, result["key"])
		},
	)

	t.Run("scan Map type", func(t *testing.T) {
		var result map[string]int
		scanner := ScanMap(&result)
		err := scanner.Scan(Map{
			"one": 1,
			"two": 2,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, result["one"])
		assert.Equal(t, 2, result["two"])
	})
}

// TestUnionScanner tests the UnionScanner type.
func TestUnionScanner(t *testing.T) {
	t.Run("scan union value", func(t *testing.T) {
		var result UnionValue
		scanner := ScanUnion(&result)
		err := scanner.Scan(UnionValue{
			Tag:   "int_val",
			Index: 0,
			Value: int64(42),
		})
		require.NoError(t, err)
		assert.Equal(t, "int_val", result.Tag)
		assert.Equal(t, 0, result.Index)
		assert.Equal(t, int64(42), result.Value)
	})

	t.Run("scan Union type", func(t *testing.T) {
		var result UnionValue
		scanner := ScanUnion(&result)
		err := scanner.Scan(Union{
			Tag:   "str_val",
			Value: "hello",
		})
		require.NoError(t, err)
		assert.Equal(t, "str_val", result.Tag)
		assert.Equal(t, "hello", result.Value)
	})

	t.Run(
		"scan map representation",
		func(t *testing.T) {
			var result UnionValue
			scanner := ScanUnion(&result)
			err := scanner.Scan(map[string]any{
				"tag":   "float_val",
				"index": 2,
				"value": 3.14,
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"float_val",
				result.Tag,
			)
			assert.Equal(t, 2, result.Index)
			assert.Equal(t, 3.14, result.Value)
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		var result UnionValue
		scanner := ScanUnion(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Equal(t, "", result.Tag)
		assert.Equal(t, -1, result.Index)
		assert.Nil(t, result.Value)
	})

	t.Run("UnionValue.As", func(t *testing.T) {
		uv := UnionValue{
			Tag:   "int_val",
			Index: 0,
			Value: int64(42),
		}
		var result int64
		err := uv.As(&result)
		require.NoError(t, err)
		assert.Equal(t, int64(42), result)
	})
}

// TestEnumScanner tests the EnumScanner type.
func TestEnumScanner(t *testing.T) {
	type Status string

	const (
		StatusActive   Status = "active"
		StatusInactive Status = "inactive"
	)

	t.Run("scan enum value", func(t *testing.T) {
		var result Status
		scanner := ScanEnum(&result)
		err := scanner.Scan("active")
		require.NoError(t, err)
		assert.Equal(t, StatusActive, result)
	})

	t.Run(
		"scan enum from bytes",
		func(t *testing.T) {
			var result Status
			scanner := ScanEnum(&result)
			err := scanner.Scan(
				[]byte("inactive"),
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				StatusInactive,
				result,
			)
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		var result Status
		scanner := ScanEnum(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Equal(t, Status(""), result)
	})

	t.Run(
		"scan invalid type",
		func(t *testing.T) {
			var result Status
			scanner := ScanEnum(&result)
			err := scanner.Scan(123)
			assert.Error(t, err)
		},
	)
}

// TestJSONScanner tests the JSONScanner type.
func TestJSONScanner(t *testing.T) {
	type Config struct {
		Enabled bool   `json:"enabled"`
		Timeout int    `json:"timeout"`
		Name    string `json:"name"`
	}

	t.Run("scan JSON string", func(t *testing.T) {
		var result Config
		scanner := ScanJSON(&result)
		err := scanner.Scan(
			`{"enabled": true, "timeout": 30, "name": "test"}`,
		)
		require.NoError(t, err)
		assert.True(t, result.Enabled)
		assert.Equal(t, 30, result.Timeout)
		assert.Equal(t, "test", result.Name)
	})

	t.Run("scan JSON bytes", func(t *testing.T) {
		var result Config
		scanner := ScanJSON(&result)
		err := scanner.Scan(
			[]byte(
				`{"enabled": false, "timeout": 60}`,
			),
		)
		require.NoError(t, err)
		assert.False(t, result.Enabled)
		assert.Equal(t, 60, result.Timeout)
	})

	t.Run("scan nil", func(t *testing.T) {
		var result Config
		scanner := ScanJSON(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		// Result should be unchanged (zero value)
		assert.False(t, result.Enabled)
	})

	t.Run("scan to map", func(t *testing.T) {
		var result map[string]any
		scanner := ScanJSON(&result)
		err := scanner.Scan(
			`{"key": "value", "num": 42}`,
		)
		require.NoError(t, err)
		assert.Equal(t, "value", result["key"])
		assert.Equal(
			t,
			float64(42),
			result["num"],
		)
	})

	t.Run(
		"scan invalid JSON",
		func(t *testing.T) {
			var result Config
			scanner := ScanJSON(&result)
			err := scanner.Scan(`{invalid json}`)
			assert.Error(t, err)
		},
	)

	t.Run(
		"scan invalid type",
		func(t *testing.T) {
			var result Config
			scanner := ScanJSON(&result)
			err := scanner.Scan(123)
			assert.Error(t, err)
		},
	)
}

// TestUUIDScanner tests the UUIDScanner type.
func TestUUIDScanner(t *testing.T) {
	t.Run("scan from bytes", func(t *testing.T) {
		var result [16]byte
		scanner := ScanUUID(&result)
		input := [16]byte{
			1,
			2,
			3,
			4,
			5,
			6,
			7,
			8,
			9,
			10,
			11,
			12,
			13,
			14,
			15,
			16,
		}
		err := scanner.Scan(input)
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run(
		"scan from byte slice",
		func(t *testing.T) {
			var result [16]byte
			scanner := ScanUUID(&result)
			input := []byte{
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
			}
			err := scanner.Scan(input)
			require.NoError(t, err)
			expected := [16]byte{
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
			}
			assert.Equal(t, expected, result)
		},
	)

	t.Run("scan from string", func(t *testing.T) {
		var result [16]byte
		scanner := ScanUUID(&result)
		err := scanner.Scan(
			"550e8400-e29b-41d4-a716-446655440000",
		)
		require.NoError(t, err)
		expected := [16]byte{
			0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
			0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
		}
		assert.Equal(t, expected, result)
	})

	t.Run(
		"scan from UUID type",
		func(t *testing.T) {
			var result [16]byte
			scanner := ScanUUID(&result)
			input := UUID{
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
			}
			err := scanner.Scan(input)
			require.NoError(t, err)
			expected := [16]byte{
				1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10,
				11,
				12,
				13,
				14,
				15,
				16,
			}
			assert.Equal(t, expected, result)
		},
	)

	t.Run("scan nil", func(t *testing.T) {
		var result [16]byte
		scanner := ScanUUID(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Equal(t, [16]byte{}, result)
	})

	t.Run(
		"scan invalid byte slice length",
		func(t *testing.T) {
			var result [16]byte
			scanner := ScanUUID(&result)
			err := scanner.Scan(
				[]byte{1, 2, 3},
			) // too short
			assert.Error(t, err)
			assert.Contains(
				t,
				err.Error(),
				"UUID must be 16 bytes",
			)
		},
	)

	t.Run(
		"scan invalid string",
		func(t *testing.T) {
			var result [16]byte
			scanner := ScanUUID(&result)
			err := scanner.Scan("not-a-uuid")
			assert.Error(t, err)
		},
	)
}

// TestTimeNSScanner tests the TimeNSScanner type.
func TestTimeNSScanner(t *testing.T) {
	t.Run(
		"scan from time.Time",
		func(t *testing.T) {
			var result TimeNS
			scanner := ScanTimeNS(&result)
			// 12:30:45.123456789
			input := time.Date(
				2024,
				1,
				1,
				12,
				30,
				45,
				123456789,
				time.UTC,
			)
			err := scanner.Scan(input)
			require.NoError(t, err)
			h, m, s, ns := result.Components()
			assert.Equal(t, 12, h)
			assert.Equal(t, 30, m)
			assert.Equal(t, 45, s)
			assert.Equal(t, int64(123456789), ns)
		},
	)

	t.Run("scan from int64", func(t *testing.T) {
		var result TimeNS
		scanner := ScanTimeNS(&result)
		// 1 hour in nanoseconds
		input := int64(3600 * 1000000000)
		err := scanner.Scan(input)
		require.NoError(t, err)
		h, m, s, ns := result.Components()
		assert.Equal(t, 1, h)
		assert.Equal(t, 0, m)
		assert.Equal(t, 0, s)
		assert.Equal(t, int64(0), ns)
	})

	t.Run("scan from TimeNS", func(t *testing.T) {
		var result TimeNS
		scanner := ScanTimeNS(&result)
		input := NewTimeNS(14, 30, 0, 0)
		err := scanner.Scan(input)
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run("scan nil", func(t *testing.T) {
		var result TimeNS
		scanner := ScanTimeNS(&result)
		err := scanner.Scan(nil)
		require.NoError(t, err)
		assert.Equal(t, TimeNS(0), result)
	})

	t.Run(
		"scan invalid type",
		func(t *testing.T) {
			var result TimeNS
			scanner := ScanTimeNS(&result)
			err := scanner.Scan("not a time")
			assert.Error(t, err)
		},
	)
}

// TestNestedTypes tests scanning nested complex types.
func TestNestedTypes(t *testing.T) {
	t.Run(
		"struct with list field",
		func(t *testing.T) {
			type Order struct {
				ID    int64   `duckdb:"id"`
				Items []int64 `duckdb:"items"`
			}
			var result Order
			scanner := ScanStruct(&result)
			err := scanner.Scan(map[string]any{
				"id": int64(1),
				"items": []any{
					int64(10),
					int64(20),
					int64(30),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, int64(1), result.ID)
			assert.Equal(
				t,
				[]int64{10, 20, 30},
				result.Items,
			)
		},
	)

	t.Run(
		"struct with map field",
		func(t *testing.T) {
			type Record struct {
				Name       string            `duckdb:"name"`
				Properties map[string]string `duckdb:"properties"`
			}
			var result Record
			scanner := ScanStruct(&result)
			err := scanner.Scan(map[string]any{
				"name": "test",
				"properties": map[any]any{
					"key1": "value1",
					"key2": "value2",
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "test", result.Name)
			assert.Equal(
				t,
				"value1",
				result.Properties["key1"],
			)
			assert.Equal(
				t,
				"value2",
				result.Properties["key2"],
			)
		},
	)
}

package dukdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListValue(t *testing.T) {
	t.Run("int slice", func(t *testing.T) {
		lv := NewListValue([]int{1, 2, 3})
		val, err := lv.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{1, 2, 3}, val)
	})

	t.Run("string slice", func(t *testing.T) {
		lv := NewListValue(
			[]string{"a", "b", "c"},
		)
		val, err := lv.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{"a", "b", "c"}, val)
	})

	t.Run("empty slice", func(t *testing.T) {
		lv := NewListValue([]int{})
		val, err := lv.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{}, val)
	})

	t.Run("nil slice", func(t *testing.T) {
		lv := ListValue[int](nil)
		val, err := lv.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{}, val)
	})

	t.Run("float64 slice", func(t *testing.T) {
		lv := NewListValue(
			[]float64{1.1, 2.2, 3.3},
		)
		val, err := lv.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{1.1, 2.2, 3.3}, val)
	})
}

func TestStructValue(t *testing.T) {
	t.Run("simple struct", func(t *testing.T) {
		type Person struct {
			Name string `duckdb:"name"`
			Age  int    `duckdb:"age"`
		}
		sv := NewStructValue(
			Person{Name: "Alice", Age: 30},
		)
		val, err := sv.Value()
		require.NoError(t, err)
		m, ok := val.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Alice", m["name"])
		assert.Equal(t, 30, m["age"])
	})

	t.Run(
		"struct with custom tags",
		func(t *testing.T) {
			type Record struct {
				ID    int64  `duckdb:"record_id"`
				Value string `duckdb:"record_value"`
			}
			sv := NewStructValue(
				Record{ID: 42, Value: "test"},
			)
			val, err := sv.Value()
			require.NoError(t, err)
			m, ok := val.(map[string]any)
			require.True(t, ok)
			assert.Equal(
				t,
				int64(42),
				m["record_id"],
			)
			assert.Equal(
				t,
				"test",
				m["record_value"],
			)
		},
	)

	t.Run(
		"struct without tags uses field name",
		func(t *testing.T) {
			type Simple struct {
				FieldOne string
				FieldTwo int
			}
			sv := NewStructValue(
				Simple{
					FieldOne: "value",
					FieldTwo: 123,
				},
			)
			val, err := sv.Value()
			require.NoError(t, err)
			m, ok := val.(map[string]any)
			require.True(t, ok)
			// Should use lowercase field names
			assert.Equal(
				t,
				"value",
				m["fieldone"],
			)
			assert.Equal(t, 123, m["fieldtwo"])
		},
	)

	t.Run(
		"nil pointer to struct",
		func(t *testing.T) {
			type Person struct {
				Name string
			}
			sv := NewStructValue((*Person)(nil))
			val, err := sv.Value()
			require.NoError(t, err)
			assert.Nil(t, val)
		},
	)

	t.Run(
		"pointer to struct",
		func(t *testing.T) {
			type Person struct {
				Name string `duckdb:"name"`
			}
			p := &Person{Name: "Bob"}
			sv := NewStructValue(p)
			val, err := sv.Value()
			require.NoError(t, err)
			m, ok := val.(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "Bob", m["name"])
		},
	)
}

func TestMapValue(t *testing.T) {
	t.Run(
		"string to int map",
		func(t *testing.T) {
			mv := NewMapValue(map[string]int{
				"one":   1,
				"two":   2,
				"three": 3,
			})
			val, err := mv.Value()
			require.NoError(t, err)
			m, ok := val.(map[any]any)
			require.True(t, ok)
			assert.Equal(t, 1, m["one"])
			assert.Equal(t, 2, m["two"])
			assert.Equal(t, 3, m["three"])
		},
	)

	t.Run(
		"int to string map",
		func(t *testing.T) {
			mv := NewMapValue(map[int]string{
				1: "one",
				2: "two",
			})
			val, err := mv.Value()
			require.NoError(t, err)
			m, ok := val.(map[any]any)
			require.True(t, ok)
			assert.Equal(t, "one", m[1])
			assert.Equal(t, "two", m[2])
		},
	)

	t.Run("empty map", func(t *testing.T) {
		mv := NewMapValue(map[string]int{})
		val, err := mv.Value()
		require.NoError(t, err)
		m, ok := val.(map[any]any)
		require.True(t, ok)
		assert.Empty(t, m)
	})

	t.Run("nil map", func(t *testing.T) {
		mv := MapValue[string, int](nil)
		val, err := mv.Value()
		require.NoError(t, err)
		assert.Nil(t, val)
	})
}

func TestArrayValue(t *testing.T) {
	t.Run("int array", func(t *testing.T) {
		av := NewArrayValue([]int{1, 2, 3, 4, 5})
		val, err := av.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{1, 2, 3, 4, 5}, val)
	})

	t.Run("string array", func(t *testing.T) {
		av := NewArrayValue(
			[]string{"a", "b", "c"},
		)
		val, err := av.Value()
		require.NoError(t, err)
		assert.Equal(t, []any{"a", "b", "c"}, val)
	})

	t.Run(
		"fixed size validation in usage",
		func(t *testing.T) {
			// ArrayValue itself doesn't validate size, that's for the scanner
			av := NewArrayValue([]int{1, 2})
			val, err := av.Value()
			require.NoError(t, err)
			assert.Equal(t, []any{1, 2}, val)
		},
	)
}

func TestJSONValue(t *testing.T) {
	t.Run("struct value", func(t *testing.T) {
		type Config struct {
			Enabled bool   `json:"enabled"`
			Name    string `json:"name"`
		}
		jv := NewJSONValue(
			Config{Enabled: true, Name: "test"},
		)
		val, err := jv.Value()
		require.NoError(t, err)
		// JSONValue returns the value as-is for the driver to handle
		c, ok := val.(Config)
		require.True(t, ok)
		assert.True(t, c.Enabled)
		assert.Equal(t, "test", c.Name)
	})

	t.Run("map value", func(t *testing.T) {
		jv := NewJSONValue(map[string]any{
			"key": "value",
			"num": 42,
		})
		val, err := jv.Value()
		require.NoError(t, err)
		m, ok := val.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", m["key"])
		assert.Equal(t, 42, m["num"])
	})
}

// TestRoundTrip tests that values can be bound and then scanned back.
func TestRoundTrip(t *testing.T) {
	t.Run("list round trip", func(t *testing.T) {
		// Bind
		original := []int64{1, 2, 3}
		lv := NewListValue(original)
		val, err := lv.Value()
		require.NoError(t, err)

		// Scan
		var result []int64
		scanner := ScanList(&result)
		err = scanner.Scan(val)
		require.NoError(t, err)
		assert.Equal(t, original, result)
	})

	t.Run("map round trip", func(t *testing.T) {
		// Bind
		original := map[string]int64{
			"a": 1,
			"b": 2,
		}
		mv := NewMapValue(original)
		val, err := mv.Value()
		require.NoError(t, err)

		// Scan
		var result map[string]int64
		scanner := ScanMap(&result)
		err = scanner.Scan(val)
		require.NoError(t, err)
		assert.Equal(t, original, result)
	})

	t.Run(
		"struct round trip",
		func(t *testing.T) {
			type Person struct {
				Name string `duckdb:"name"`
				Age  int    `duckdb:"age"`
			}

			// Bind
			original := Person{
				Name: "Alice",
				Age:  30,
			}
			sv := NewStructValue(original)
			val, err := sv.Value()
			require.NoError(t, err)

			// Scan
			var result Person
			scanner := ScanStruct(&result)
			err = scanner.Scan(val)
			require.NoError(t, err)
			assert.Equal(
				t,
				original.Name,
				result.Name,
			)
			assert.Equal(
				t,
				original.Age,
				result.Age,
			)
		},
	)
}

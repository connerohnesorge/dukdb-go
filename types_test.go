package dukdb

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUUID_Scan_Bytes(t *testing.T) {
	// Test scanning 16-byte slice
	input := []byte{
		0x12,
		0x34,
		0x56,
		0x78,
		0x9a,
		0xbc,
		0xde,
		0xf0,
		0x12,
		0x34,
		0x56,
		0x78,
		0x9a,
		0xbc,
		0xde,
		0xf0,
	}
	var u UUID
	err := u.Scan(input)
	require.NoError(t, err)
	assert.Equal(t, input, u[:])
}

func TestUUID_Scan_String(t *testing.T) {
	// Test scanning string format
	input := "12345678-9abc-def0-1234-56789abcdef0"
	var u UUID
	err := u.Scan(input)
	require.NoError(t, err)
	assert.Equal(t, input, u.String())
}

func TestUUID_Scan_StringFromBytes(t *testing.T) {
	// Test scanning string format passed as bytes (not 16 bytes)
	input := []byte(
		"12345678-9abc-def0-1234-56789abcdef0",
	)
	var u UUID
	err := u.Scan(input)
	require.NoError(t, err)
	assert.Equal(
		t,
		"12345678-9abc-def0-1234-56789abcdef0",
		u.String(),
	)
}

func TestUUID_Scan_InvalidType(t *testing.T) {
	var u UUID
	err := u.Scan(12345)
	assert.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"invalid UUID value type",
	)
}

func TestUUID_Scan_InvalidString(t *testing.T) {
	var u UUID
	err := u.Scan("not-a-uuid")
	assert.Error(t, err)
}

func TestMap_Scan(t *testing.T) {
	input := Map{"key1": "value1", "key2": 42}
	var m Map
	err := m.Scan(input)
	require.NoError(t, err)
	assert.Equal(t, "value1", m["key1"])
	assert.Equal(t, 42, m["key2"])
}

func TestMap_Scan_InvalidType(t *testing.T) {
	var m Map
	err := m.Scan(
		map[string]string{"key": "value"},
	)
	assert.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"invalid type",
	)
}

func TestComposite_Scan(t *testing.T) {
	type Person struct {
		Name string `mapstructure:"name"`
		Age  int    `mapstructure:"age"`
	}

	input := map[string]any{
		"name": "John",
		"age":  30,
	}
	var c Composite[Person]
	err := c.Scan(input)
	require.NoError(t, err)

	p := c.Get()
	assert.Equal(t, "John", p.Name)
	assert.Equal(t, 30, p.Age)
}

func TestComposite_Scan_List(t *testing.T) {
	input := []any{1, 2, 3, 4, 5}
	var c Composite[[]int]
	err := c.Scan(input)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, c.Get())
}

func TestUUID_Value(t *testing.T) {
	u := UUID{
		0x12,
		0x34,
		0x56,
		0x78,
		0x9a,
		0xbc,
		0xde,
		0xf0,
		0x12,
		0x34,
		0x56,
		0x78,
		0x9a,
		0xbc,
		0xde,
		0xf0,
	}
	val, err := u.Value()
	require.NoError(t, err)
	assert.Equal(
		t,
		"12345678-9abc-def0-1234-56789abcdef0",
		val,
	)
}

func TestUUID_String(t *testing.T) {
	u := UUID{
		0x00,
		0x11,
		0x22,
		0x33,
		0x44,
		0x55,
		0x66,
		0x77,
		0x88,
		0x99,
		0xaa,
		0xbb,
		0xcc,
		0xdd,
		0xee,
		0xff,
	}
	assert.Equal(
		t,
		"00112233-4455-6677-8899-aabbccddeeff",
		u.String(),
	)
}

func TestDecimal_Float64(t *testing.T) {
	tests := []struct {
		name     string
		decimal  Decimal
		expected float64
	}{
		{
			name: "simple",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: big.NewInt(12345),
			},
			expected: 123.45,
		},
		{
			name: "zero",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: big.NewInt(0),
			},
			expected: 0.0,
		},
		{
			name: "negative",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: big.NewInt(-12345),
			},
			expected: -123.45,
		},
		{
			name: "nil value",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: nil,
			},
			expected: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.decimal.Float64()
			assert.InDelta(
				t,
				tt.expected,
				result,
				0.001,
			)
		})
	}
}

func TestDecimal_String(t *testing.T) {
	tests := []struct {
		name     string
		decimal  Decimal
		expected string
	}{
		{
			name: "simple",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: big.NewInt(12345),
			},
			expected: "123.45",
		},
		{
			name: "zero",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: big.NewInt(0),
			},
			expected: "0",
		},
		{
			name: "negative",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: big.NewInt(-12345),
			},
			expected: "-123.45",
		},
		{
			name: "trailing zeros trimmed",
			decimal: Decimal{
				Width: 10,
				Scale: 4,
				Value: big.NewInt(123400),
			},
			expected: "12.34",
		},
		{
			name: "leading zeros after decimal",
			decimal: Decimal{
				Width: 10,
				Scale: 4,
				Value: big.NewInt(1),
			},
			expected: "0.0001",
		},
		{
			name: "whole number",
			decimal: Decimal{
				Width: 10,
				Scale: 0,
				Value: big.NewInt(12345),
			},
			expected: "12345",
		},
		{
			name: "nil value",
			decimal: Decimal{
				Width: 10,
				Scale: 2,
				Value: nil,
			},
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.decimal.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseValue_Boolean(t *testing.T) {
	val, err := ParseValue(
		[]byte("true"),
		TYPE_BOOLEAN,
	)
	require.NoError(t, err)
	assert.Equal(t, true, val)

	val, err = ParseValue(
		[]byte("false"),
		TYPE_BOOLEAN,
	)
	require.NoError(t, err)
	assert.Equal(t, false, val)
}

func TestParseValue_Integers(t *testing.T) {
	tests := []struct {
		typ      Type
		input    string
		expected any
	}{
		{TYPE_TINYINT, "127", int8(127)},
		{TYPE_TINYINT, "-128", int8(-128)},
		{TYPE_SMALLINT, "32767", int16(32767)},
		{
			TYPE_INTEGER,
			"2147483647",
			int32(2147483647),
		},
		{
			TYPE_BIGINT,
			"9223372036854775807",
			int64(9223372036854775807),
		},
		{TYPE_UTINYINT, "255", uint8(255)},
		{TYPE_USMALLINT, "65535", uint16(65535)},
		{
			TYPE_UINTEGER,
			"4294967295",
			uint32(4294967295),
		},
		{
			TYPE_UBIGINT,
			"18446744073709551615",
			uint64(18446744073709551615),
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.typ.String(),
			func(t *testing.T) {
				val, err := ParseValue(
					[]byte(tt.input),
					tt.typ,
				)
				require.NoError(t, err)
				assert.Equal(t, tt.expected, val)
			},
		)
	}
}

func TestParseValue_Floats(t *testing.T) {
	val, err := ParseValue(
		[]byte("3.14159"),
		TYPE_DOUBLE,
	)
	require.NoError(t, err)
	assert.InDelta(t, 3.14159, val, 0.00001)
}

func TestParseValue_SpecialFloats(t *testing.T) {
	// Test Infinity
	val, err := ParseValue(
		[]byte(`"Infinity"`),
		TYPE_DOUBLE,
	)
	require.NoError(t, err)
	assert.True(t, math.IsInf(val.(float64), 1))

	// Test -Infinity
	val, err = ParseValue(
		[]byte(`"-Infinity"`),
		TYPE_DOUBLE,
	)
	require.NoError(t, err)
	assert.True(t, math.IsInf(val.(float64), -1))

	// Test NaN
	val, err = ParseValue(
		[]byte(`"NaN"`),
		TYPE_DOUBLE,
	)
	require.NoError(t, err)
	assert.True(t, math.IsNaN(val.(float64)))
}

func TestParseValue_HugeInt(t *testing.T) {
	// Test as string (for large values)
	val, err := ParseValue(
		[]byte(
			`"170141183460469231731687303715884105727"`,
		),
		TYPE_HUGEINT,
	)
	require.NoError(t, err)
	expected, _ := new(
		big.Int,
	).SetString("170141183460469231731687303715884105727", 10)
	assert.Equal(
		t,
		0,
		expected.Cmp(val.(*big.Int)),
	)

	// Test as number (for small values)
	val, err = ParseValue(
		[]byte(`12345`),
		TYPE_HUGEINT,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		0,
		big.NewInt(12345).Cmp(val.(*big.Int)),
	)
}

func TestParseValue_String(t *testing.T) {
	val, err := ParseValue(
		[]byte(`"hello world"`),
		TYPE_VARCHAR,
	)
	require.NoError(t, err)
	assert.Equal(t, "hello world", val)
}

func TestParseValue_BLOB(t *testing.T) {
	// Test hex format
	val, err := ParseValue(
		[]byte(`"\\x48454C4C4F"`),
		TYPE_BLOB,
	)
	require.NoError(t, err)
	assert.Equal(t, []byte("HELLO"), val)
}

func TestParseValue_UUID(t *testing.T) {
	val, err := ParseValue(
		[]byte(
			`"12345678-9abc-def0-1234-56789abcdef0"`,
		),
		TYPE_UUID,
	)
	require.NoError(t, err)
	u, ok := val.(UUID)
	require.True(t, ok)
	assert.Equal(
		t,
		"12345678-9abc-def0-1234-56789abcdef0",
		u.String(),
	)
}

func TestParseValue_Interval(t *testing.T) {
	input := `{"months": 12, "days": 30, "micros": 1000000}`
	val, err := ParseValue(
		[]byte(input),
		TYPE_INTERVAL,
	)
	require.NoError(t, err)
	interval, ok := val.(Interval)
	require.True(t, ok)
	assert.Equal(t, int32(12), interval.Months)
	assert.Equal(t, int32(30), interval.Days)
	assert.Equal(
		t,
		int64(1000000),
		interval.Micros,
	)
}

func TestParseValue_List(t *testing.T) {
	val, err := ParseValue(
		[]byte(`[1, 2, 3, 4, 5]`),
		TYPE_LIST,
	)
	require.NoError(t, err)
	list, ok := val.([]any)
	require.True(t, ok)
	assert.Len(t, list, 5)
	assert.Equal(
		t,
		float64(1),
		list[0],
	) // JSON numbers are float64
}

func TestParseValue_Struct(t *testing.T) {
	val, err := ParseValue(
		[]byte(`{"name": "John", "age": 30}`),
		TYPE_STRUCT,
	)
	require.NoError(t, err)
	m, ok := val.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "John", m["name"])
	assert.Equal(t, float64(30), m["age"])
}

func TestParseValue_Map(t *testing.T) {
	// DuckDB MAP format: array of {key, value} pairs
	input := `[{"key": "a", "value": 1}, {"key": "b", "value": 2}]`
	val, err := ParseValue(
		[]byte(input),
		TYPE_MAP,
	)
	require.NoError(t, err)
	m, ok := val.(Map)
	require.True(t, ok)
	assert.Equal(t, float64(1), m["a"])
	assert.Equal(t, float64(2), m["b"])
}

func TestParseValue_Union(t *testing.T) {
	input := `{"tag": "string_member", "value": "hello"}`
	val, err := ParseValue(
		[]byte(input),
		TYPE_UNION,
	)
	require.NoError(t, err)
	u, ok := val.(Union)
	require.True(t, ok)
	assert.Equal(t, "string_member", u.Tag)
	assert.Equal(t, "hello", u.Value)
}

func TestParseValue_Null(t *testing.T) {
	val, err := ParseValue(
		[]byte("null"),
		TYPE_VARCHAR,
	)
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = ParseValue(
		[]byte("null"),
		TYPE_INTEGER,
	)
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestParseValue_Timestamp(t *testing.T) {
	val, err := ParseValue(
		[]byte(`"2024-01-15 10:30:00"`),
		TYPE_TIMESTAMP,
	)
	require.NoError(t, err)
	tm, ok := val.(time.Time)
	require.True(t, ok)
	assert.Equal(t, 2024, tm.Year())
	assert.Equal(t, time.January, tm.Month())
	assert.Equal(t, 15, tm.Day())
}

func TestParseValue_Date(t *testing.T) {
	val, err := ParseValue(
		[]byte(`"2024-01-15"`),
		TYPE_DATE,
	)
	require.NoError(t, err)
	tm, ok := val.(time.Time)
	require.True(t, ok)
	assert.Equal(t, 2024, tm.Year())
	assert.Equal(t, time.January, tm.Month())
	assert.Equal(t, 15, tm.Day())
}

func TestHugeIntConversion(t *testing.T) {
	// Test round-trip conversion
	original := hugeInt{
		lower: 12345678901234567890,
		upper: 123456789,
	}
	bi := hugeIntToBigInt(original)
	result, err := bigIntToHugeInt(bi)
	require.NoError(t, err)
	assert.Equal(t, original.lower, result.lower)
	assert.Equal(t, original.upper, result.upper)
}

func TestBigIntToHugeInt_Overflow(t *testing.T) {
	// Test value too large for HUGEINT (2^128)
	huge := new(big.Int).Lsh(big.NewInt(1), 128)
	_, err := bigIntToHugeInt(huge)
	assert.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"too big for HUGEINT",
	)
}

func TestBigIntToHugeInt_Nil(t *testing.T) {
	_, err := bigIntToHugeInt(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestBigIntToHugeInt_MaxValue(t *testing.T) {
	// Maximum positive HUGEINT: 2^127 - 1
	maxVal := new(
		big.Int,
	).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	_, err := bigIntToHugeInt(maxVal)
	assert.NoError(t, err)
}

func TestBigIntToHugeInt_MinValue(t *testing.T) {
	// Minimum negative HUGEINT: -2^127
	minVal := new(
		big.Int,
	).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	_, err := bigIntToHugeInt(minVal)
	assert.NoError(t, err)
}

func TestInferInterval(t *testing.T) {
	interval := Interval{
		Days:   10,
		Months: 2,
		Micros: 1000000,
	}
	result, err := inferInterval(interval)
	require.NoError(t, err)
	assert.Equal(t, interval, result)
}

func TestInferInterval_InvalidType(t *testing.T) {
	_, err := inferInterval("not an interval")
	assert.Error(t, err)
}

func TestCastToTime(t *testing.T) {
	now := time.Now()
	result, err := castToTime(now)
	require.NoError(t, err)
	assert.True(t, result.Equal(now.UTC()))
}

func TestCastToTime_InvalidType(t *testing.T) {
	_, err := castToTime("not a time")
	assert.Error(t, err)
}

func TestInferTimestamp(t *testing.T) {
	now := time.Now()
	result, err := inferTimestamp(now)
	require.NoError(t, err)
	assert.Equal(t, now.UTC().UnixMicro(), result)
}

func TestInferDate(t *testing.T) {
	// January 2, 1970 should be day 1
	date := time.Date(
		1970,
		time.January,
		2,
		0,
		0,
		0,
		0,
		time.UTC,
	)
	result, err := inferDate(date)
	require.NoError(t, err)
	assert.Equal(t, int32(1), result)
}

func TestInferTime(t *testing.T) {
	// 01:00:00 should be 3600 * 1000000 microseconds
	t1 := time.Date(
		2024,
		1,
		1,
		1,
		0,
		0,
		0,
		time.UTC,
	)
	result, err := inferTime(t1)
	require.NoError(t, err)
	assert.Equal(t, int64(3600*1000000), result)
}

// TestUUID_ImplementsScanner verifies UUID implements sql.Scanner
func TestUUID_ImplementsScanner(t *testing.T) {
	var _ sql.Scanner = (*UUID)(nil)
}

// TestUUID_ImplementsValuer verifies UUID implements driver.Valuer
func TestUUID_ImplementsValuer(t *testing.T) {
	var _ driver.Valuer = (*UUID)(nil)
}

// TestMap_ImplementsScanner verifies Map implements sql.Scanner
func TestMap_ImplementsScanner(t *testing.T) {
	var _ sql.Scanner = (*Map)(nil)
}

// TestType_String verifies all types have string representations
func TestType_String(t *testing.T) {
	types := []Type{
		TYPE_INVALID, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER,
		TYPE_BIGINT, TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT,
		TYPE_FLOAT, TYPE_DOUBLE, TYPE_TIMESTAMP, TYPE_DATE, TYPE_TIME,
		TYPE_INTERVAL, TYPE_HUGEINT, TYPE_UHUGEINT, TYPE_VARCHAR, TYPE_BLOB,
		TYPE_DECIMAL, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS,
		TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UUID,
		TYPE_UNION, TYPE_BIT, TYPE_TIME_TZ, TYPE_TIMESTAMP_TZ, TYPE_ANY,
		TYPE_BIGNUM, TYPE_SQLNULL,
	}

	for _, typ := range types {
		s := typ.String()
		assert.NotEmpty(
			t,
			s,
			"Type %d should have string representation",
			typ,
		)
		assert.NotEqual(
			t,
			"UNKNOWN",
			s,
			"Type %d should not be UNKNOWN",
			typ,
		)
	}
}

// TestType_Category verifies all types have valid categories
func TestType_Category(t *testing.T) {
	validCategories := map[string]bool{
		"numeric":  true,
		"temporal": true,
		"string":   true,
		"nested":   true,
		"other":    true,
	}

	types := []Type{
		TYPE_INVALID, TYPE_BOOLEAN, TYPE_TINYINT, TYPE_VARCHAR, TYPE_BLOB,
		TYPE_TIMESTAMP, TYPE_DATE, TYPE_LIST, TYPE_STRUCT, TYPE_UUID,
	}

	for _, typ := range types {
		cat := typ.Category()
		assert.True(
			t,
			validCategories[cat],
			"Type %s has invalid category: %s",
			typ.String(),
			cat,
		)
	}
}

// TestType_CategoryNumeric verifies numeric types return "numeric"
func TestType_CategoryNumeric(t *testing.T) {
	numericTypes := []Type{
		TYPE_BOOLEAN, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT,
		TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT,
		TYPE_FLOAT, TYPE_DOUBLE, TYPE_HUGEINT, TYPE_UHUGEINT,
		TYPE_DECIMAL, TYPE_BIGNUM,
	}

	for _, typ := range numericTypes {
		assert.Equal(
			t,
			"numeric",
			typ.Category(),
			"Type %s should be numeric",
			typ.String(),
		)
	}
}

// TestType_CategoryTemporal verifies temporal types return "temporal"
func TestType_CategoryTemporal(t *testing.T) {
	temporalTypes := []Type{
		TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS,
		TYPE_TIMESTAMP_TZ, TYPE_DATE, TYPE_TIME, TYPE_TIME_TZ, TYPE_INTERVAL,
	}

	for _, typ := range temporalTypes {
		assert.Equal(
			t,
			"temporal",
			typ.Category(),
			"Type %s should be temporal",
			typ.String(),
		)
	}
}

// TestType_CategoryNested verifies nested types return "nested"
func TestType_CategoryNested(t *testing.T) {
	nestedTypes := []Type{
		TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY, TYPE_UNION,
	}

	for _, typ := range nestedTypes {
		assert.Equal(
			t,
			"nested",
			typ.Category(),
			"Type %s should be nested",
			typ.String(),
		)
	}
}

// TestDecimal_API verifies Decimal has the expected methods
func TestDecimal_API(t *testing.T) {
	d := Decimal{
		Width: 10,
		Scale: 2,
		Value: big.NewInt(12345),
	}

	// Float64 method exists and returns correct type
	f := d.Float64()
	assert.IsType(t, float64(0), f)

	// String method exists and returns correct type
	s := d.String()
	assert.IsType(t, "", s)
}

// TestInterval_API verifies Interval has the expected fields
func TestInterval_API(t *testing.T) {
	i := Interval{Days: 1, Months: 2, Micros: 3}
	assert.Equal(t, int32(1), i.Days)
	assert.Equal(t, int32(2), i.Months)
	assert.Equal(t, int64(3), i.Micros)

	// Verify JSON tags work
	data, err := json.Marshal(i)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"days"`)
	assert.Contains(t, string(data), `"months"`)
	assert.Contains(t, string(data), `"micros"`)
}

// TestUnion_API verifies Union has the expected fields
func TestUnion_API(t *testing.T) {
	u := Union{Tag: "member", Value: "value"}
	assert.Equal(t, "member", u.Tag)
	assert.Equal(t, "value", u.Value)
}

// TestComposite_API verifies Composite has Get and Scan methods
func TestComposite_API(t *testing.T) {
	var c Composite[string]

	// Scan method exists
	err := c.Scan("test")
	require.NoError(t, err)

	// Get method exists and returns correct type
	val := c.Get()
	assert.Equal(t, "test", val)
}

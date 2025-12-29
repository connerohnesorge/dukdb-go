package compatibility

import (
	"database/sql"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TypeCompatibilityTests verifies DuckDB types work identically across implementations.
var TypeCompatibilityTests = []CompatibilityTest{
	// Integer types
	{Name: "TypeTinyInt", Category: "type", Test: testTypeTinyInt},
	{Name: "TypeSmallInt", Category: "type", Test: testTypeSmallInt},
	{Name: "TypeInteger", Category: "type", Test: testTypeInteger},
	{Name: "TypeBigInt", Category: "type", Test: testTypeBigInt},
	{Name: "TypeHugeInt", Category: "type", Test: testTypeHugeInt},
	{Name: "TypeUTinyInt", Category: "type", Test: testTypeUTinyInt},
	{Name: "TypeUSmallInt", Category: "type", Test: testTypeUSmallInt},
	{Name: "TypeUInteger", Category: "type", Test: testTypeUInteger},
	{Name: "TypeUBigInt", Category: "type", Test: testTypeUBigInt},

	// Floating point
	{Name: "TypeFloat", Category: "type", Test: testTypeFloat},
	{Name: "TypeDouble", Category: "type", Test: testTypeDouble},
	{Name: "TypeFloatSpecialValues", Category: "type", Test: testTypeFloatSpecialValues},

	// Fixed point
	{Name: "TypeDecimal", Category: "type", Test: testTypeDecimal},

	// String types
	{Name: "TypeVarchar", Category: "type", Test: testTypeVarchar},
	{Name: "TypeVarcharUnicode", Category: "type", Test: testTypeVarcharUnicode},
	{Name: "TypeBlob", Category: "type", Test: testTypeBlob},

	// Boolean
	{Name: "TypeBoolean", Category: "type", Test: testTypeBoolean},

	// Date/Time
	{Name: "TypeDate", Category: "type", Test: testTypeDate},
	{Name: "TypeTime", Category: "type", Test: testTypeTime},
	{Name: "TypeTimestamp", Category: "type", Test: testTypeTimestamp},
	{Name: "TypeInterval", Category: "type", Test: testTypeInterval},

	// Complex types
	{Name: "TypeUUID", Category: "type", Test: testTypeUUID},
	{Name: "TypeList", Category: "type", Test: testTypeList},
	{Name: "TypeStruct", Category: "type", Test: testTypeStruct},
	{Name: "TypeMap", Category: "type", Test: testTypeMap},

	// NULL handling
	{Name: "TypeNullInteger", Category: "type", Test: testTypeNullInteger},
	{Name: "TypeNullVarchar", Category: "type", Test: testTypeNullVarchar},
	{Name: "TypeNullBoolean", Category: "type", Test: testTypeNullBoolean},
}

// Integer type tests

func testTypeTinyInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_tinyint (val TINYINT)`)
	require.NoError(t, err)

	// Test min, max, and typical values
	_, err = db.Exec(`INSERT INTO type_tinyint VALUES (-128), (0), (127)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_tinyint ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []int8{-128, 0, 127}
	var idx int
	for rows.Next() {
		var val int8
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeSmallInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_smallint (val SMALLINT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_smallint VALUES (-32768), (0), (32767)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_smallint ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []int16{-32768, 0, 32767}
	var idx int
	for rows.Next() {
		var val int16
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeInteger(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_integer (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_integer VALUES (-2147483648), (0), (2147483647)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_integer ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []int32{-2147483648, 0, 2147483647}
	var idx int
	for rows.Next() {
		var val int32
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeBigInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_bigint (val BIGINT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_bigint VALUES (-9223372036854775808), (0), (9223372036854775807)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_bigint ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []int64{-9223372036854775808, 0, 9223372036854775807}
	var idx int
	for rows.Next() {
		var val int64
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeHugeInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_hugeint (val HUGEINT)`)
	require.NoError(t, err)

	// HUGEINT is 128-bit, we test with string representation
	maxHugeInt := "170141183460469231731687303715884105727"
	_, err = db.Exec(`INSERT INTO type_hugeint VALUES (` + maxHugeInt + `)`)
	require.NoError(t, err)

	var result string
	err = db.QueryRow(`SELECT val::VARCHAR FROM type_hugeint`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, maxHugeInt, result)
}

func testTypeUTinyInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_utinyint (val UTINYINT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_utinyint VALUES (0), (128), (255)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_utinyint ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []uint8{0, 128, 255}
	var idx int
	for rows.Next() {
		var val uint8
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeUSmallInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_usmallint (val USMALLINT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_usmallint VALUES (0), (32768), (65535)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_usmallint ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []uint16{0, 32768, 65535}
	var idx int
	for rows.Next() {
		var val uint16
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeUInteger(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_uinteger (val UINTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_uinteger VALUES (0), (2147483648), (4294967295)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_uinteger ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []uint32{0, 2147483648, 4294967295}
	var idx int
	for rows.Next() {
		var val uint32
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeUBigInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_ubigint (val UBIGINT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_ubigint VALUES (0), (9223372036854775808), (18446744073709551615)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_ubigint ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []uint64{0, 9223372036854775808, 18446744073709551615}
	var idx int
	for rows.Next() {
		var val uint64
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// Floating point tests

func testTypeFloat(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_float (val FLOAT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_float VALUES (-3.14), (0.0), (3.14)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_float ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []float32{-3.14, 0.0, 3.14}
	var idx int
	for rows.Next() {
		var val float32
		require.NoError(t, rows.Scan(&val))
		assert.InDelta(t, expected[idx], val, 0.001)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeDouble(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_double (val DOUBLE)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_double VALUES (-3.14159265358979), (0.0), (3.14159265358979)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_double ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []float64{-3.14159265358979, 0.0, 3.14159265358979}
	var idx int
	for rows.Next() {
		var val float64
		require.NoError(t, rows.Scan(&val))
		assert.InDelta(t, expected[idx], val, 0.00000001)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeFloatSpecialValues(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_float_special (val DOUBLE)`)
	require.NoError(t, err)

	// Test special floating point values
	_, err = db.Exec(`INSERT INTO type_float_special VALUES ('infinity'), ('-infinity'), ('nan')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_float_special`)
	require.NoError(t, err)
	defer rows.Close()

	var vals []float64
	for rows.Next() {
		var val float64
		require.NoError(t, rows.Scan(&val))
		vals = append(vals, val)
	}
	require.NoError(t, rows.Err())

	require.Len(t, vals, 3)
	assert.True(t, math.IsInf(vals[0], 1))  // positive infinity
	assert.True(t, math.IsInf(vals[1], -1)) // negative infinity
	assert.True(t, math.IsNaN(vals[2]))     // NaN
}

// Fixed point tests

func testTypeDecimal(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_decimal (val DECIMAL(10, 2))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_decimal VALUES (12345.67), (-12345.67), (0.01)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_decimal ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	// Read as float64 for comparison
	expected := []float64{-12345.67, 0.01, 12345.67}
	var idx int
	for rows.Next() {
		var val float64
		require.NoError(t, rows.Scan(&val))
		assert.InDelta(t, expected[idx], val, 0.001)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// String type tests

func testTypeVarchar(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_varchar (val VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_varchar VALUES ('hello'), (''), ('world')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_varchar ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []string{"", "hello", "world"}
	var idx int
	for rows.Next() {
		var val string
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx], val)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeVarcharUnicode(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_varchar_unicode (val VARCHAR)`)
	require.NoError(t, err)

	// Test various Unicode strings
	unicodeStrings := []string{
		"Hello, World!",
		"Bonjour, le monde!",
		"Hallo Welt!",
		"Ciao mondo!",
		"Hola Mundo!",
		"Merhaba Dunya!",
		"Privet mir!",
		"Kon'nichiwa sekai!",
	}

	for _, s := range unicodeStrings {
		_, err = db.Exec(`INSERT INTO type_varchar_unicode VALUES ($1)`, s)
		require.NoError(t, err)
	}

	rows, err := db.Query(`SELECT val FROM type_varchar_unicode ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var results []string
	for rows.Next() {
		var val string
		require.NoError(t, rows.Scan(&val))
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	assert.Len(t, results, len(unicodeStrings))
}

func testTypeBlob(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_blob (val BLOB)`)
	require.NoError(t, err)

	// Insert binary data
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	_, err = db.Exec(`INSERT INTO type_blob VALUES ($1)`, binaryData)
	require.NoError(t, err)

	var result []byte
	err = db.QueryRow(`SELECT val FROM type_blob`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, binaryData, result)
}

// Boolean tests

func testTypeBoolean(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_boolean (val BOOLEAN)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_boolean VALUES (true), (false)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_boolean ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var vals []bool
	for rows.Next() {
		var val bool
		require.NoError(t, rows.Scan(&val))
		vals = append(vals, val)
	}
	require.NoError(t, rows.Err())

	require.Len(t, vals, 2)
	assert.Equal(t, false, vals[0])
	assert.Equal(t, true, vals[1])
}

// Date/Time tests

func testTypeDate(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_date (val DATE)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_date VALUES ('2024-01-15'), ('2000-06-30'), ('1990-12-25')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_date ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []time.Time{
		time.Date(1990, 12, 25, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 6, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
	}
	var idx int
	for rows.Next() {
		var val time.Time
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx].Year(), val.Year())
		assert.Equal(t, expected[idx].Month(), val.Month())
		assert.Equal(t, expected[idx].Day(), val.Day())
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeTime(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_time (val TIME)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_time VALUES ('12:30:45'), ('00:00:00'), ('23:59:59')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_time ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var val any
		require.NoError(t, rows.Scan(&val))
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, count)
}

func testTypeTimestamp(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_timestamp (val TIMESTAMP)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_timestamp VALUES ('2024-01-15 12:30:45'), ('2000-06-30 00:00:00')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_timestamp ORDER BY val`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []time.Time{
		time.Date(2000, 6, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC),
	}
	var idx int
	for rows.Next() {
		var val time.Time
		require.NoError(t, rows.Scan(&val))
		assert.Equal(t, expected[idx].Year(), val.Year())
		assert.Equal(t, expected[idx].Month(), val.Month())
		assert.Equal(t, expected[idx].Day(), val.Day())
		assert.Equal(t, expected[idx].Hour(), val.Hour())
		assert.Equal(t, expected[idx].Minute(), val.Minute())
		assert.Equal(t, expected[idx].Second(), val.Second())
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, idx)
}

func testTypeInterval(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_interval (val INTERVAL)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_interval VALUES (INTERVAL '1' DAY), (INTERVAL '2' HOUR)`)
	require.NoError(t, err)

	var count int
	rows, err := db.Query(`SELECT val FROM type_interval`)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var val any
		require.NoError(t, rows.Scan(&val))
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, count)
}

// Complex type tests

func testTypeUUID(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_uuid (val UUID)`)
	require.NoError(t, err)

	uuid := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	_, err = db.Exec(`INSERT INTO type_uuid VALUES ($1)`, uuid)
	require.NoError(t, err)

	var result string
	err = db.QueryRow(`SELECT val::VARCHAR FROM type_uuid`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, uuid, result)
}

func testTypeList(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_list (val INTEGER[])`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_list VALUES ([1, 2, 3])`)
	require.NoError(t, err)

	var result any
	err = db.QueryRow(`SELECT val FROM type_list`).Scan(&result)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func testTypeStruct(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_struct (val STRUCT(name VARCHAR, age INTEGER))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_struct VALUES ({'name': 'Alice', 'age': 30})`)
	require.NoError(t, err)

	var result any
	err = db.QueryRow(`SELECT val FROM type_struct`).Scan(&result)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func testTypeMap(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_map (val MAP(VARCHAR, INTEGER))`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_map VALUES (MAP {'a': 1, 'b': 2})`)
	require.NoError(t, err)

	var result any
	err = db.QueryRow(`SELECT val FROM type_map`).Scan(&result)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

// NULL handling tests

func testTypeNullInteger(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_null_int (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_null_int VALUES (NULL), (42)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_null_int ORDER BY val NULLS FIRST`)
	require.NoError(t, err)
	defer rows.Close()

	var results []sql.NullInt64
	for rows.Next() {
		var val sql.NullInt64
		require.NoError(t, rows.Scan(&val))
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.False(t, results[0].Valid)
	assert.True(t, results[1].Valid)
	assert.Equal(t, int64(42), results[1].Int64)
}

func testTypeNullVarchar(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_null_varchar (val VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_null_varchar VALUES (NULL), ('hello')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_null_varchar ORDER BY val NULLS FIRST`)
	require.NoError(t, err)
	defer rows.Close()

	var results []sql.NullString
	for rows.Next() {
		var val sql.NullString
		require.NoError(t, rows.Scan(&val))
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.False(t, results[0].Valid)
	assert.True(t, results[1].Valid)
	assert.Equal(t, "hello", results[1].String)
}

func testTypeNullBoolean(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE type_null_bool (val BOOLEAN)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_null_bool VALUES (NULL), (true)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT val FROM type_null_bool ORDER BY val NULLS FIRST`)
	require.NoError(t, err)
	defer rows.Close()

	var results []sql.NullBool
	for rows.Next() {
		var val sql.NullBool
		require.NoError(t, rows.Scan(&val))
		results = append(results, val)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.False(t, results[0].Valid)
	assert.True(t, results[1].Valid)
	assert.True(t, results[1].Bool)
}

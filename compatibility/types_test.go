package compatibility

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TypeCompatibilityTests verifies DuckDB types work identically across implementations.
var TypeCompatibilityTests = []CompatibilityTest{
	// Integer types
	{
		Name:     "TypeTinyInt",
		Category: "type",
		Test:     testTypeTinyInt,
	},
	{
		Name:     "TypeSmallInt",
		Category: "type",
		Test:     testTypeSmallInt,
	},
	{
		Name:     "TypeInteger",
		Category: "type",
		Test:     testTypeInteger,
	},

	// Floating point
	{
		Name:     "TypeFloat",
		Category: "type",
		Test:     testTypeFloat,
	},
	{
		Name:     "TypeDouble",
		Category: "type",
		Test:     testTypeDouble,
	},

	// String types
	{
		Name:     "TypeVarchar",
		Category: "type",
		Test:     testTypeVarchar,
	},
	{
		Name:     "TypeVarcharUnicode",
		Category: "type",
		Test:     testTypeVarcharUnicode,
	},
	{
		Name:     "TypeBlob",
		Category: "type",
		Test:     testTypeBlob,
	},

	// Boolean
	{
		Name:     "TypeBoolean",
		Category: "type",
		Test:     testTypeBoolean,
	},

	// NULL handling
	{
		Name:     "TypeNullInteger",
		Category: "type",
		Test:     testTypeNullInteger,
	},
	{
		Name:     "TypeNullVarchar",
		Category: "type",
		Test:     testTypeNullVarchar,
	},
	{
		Name:     "TypeNullBoolean",
		Category: "type",
		Test:     testTypeNullBoolean,
	},
}

// Integer type tests

func testTypeTinyInt(t *testing.T, db *sql.DB) {
	_, err := db.Exec(
		`CREATE TABLE type_tinyint (val TINYINT)`,
	)
	require.NoError(t, err)

	// Test min, max, and typical values
	_, err = db.Exec(
		`INSERT INTO type_tinyint VALUES (-128), (0), (127)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_tinyint ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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
	_, err := db.Exec(
		`CREATE TABLE type_smallint (val SMALLINT)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_smallint VALUES (-32768), (0), (32767)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_smallint ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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
	_, err := db.Exec(
		`CREATE TABLE type_integer (val INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_integer VALUES (-2147483648), (0), (2147483647)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_integer ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expected := []int32{
		-2147483648,
		0,
		2147483647,
	}
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

// Floating point tests

func testTypeFloat(t *testing.T, db *sql.DB) {
	_, err := db.Exec(
		`CREATE TABLE type_float (val FLOAT)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_float VALUES (-3.14), (0.0), (3.14)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_float ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expected := []float32{-3.14, 0.0, 3.14}
	var idx int
	for rows.Next() {
		var val float32
		require.NoError(t, rows.Scan(&val))
		assert.InDelta(
			t,
			expected[idx],
			val,
			0.001,
		)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testTypeDouble(t *testing.T, db *sql.DB) {
	_, err := db.Exec(
		`CREATE TABLE type_double (val DOUBLE)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_double VALUES (-3.14159265358979), (0.0), (3.14159265358979)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_double ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expected := []float64{
		-3.14159265358979,
		0.0,
		3.14159265358979,
	}
	var idx int
	for rows.Next() {
		var val float64
		require.NoError(t, rows.Scan(&val))
		assert.InDelta(
			t,
			expected[idx],
			val,
			0.00000001,
		)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// String type tests

func testTypeVarchar(t *testing.T, db *sql.DB) {
	_, err := db.Exec(
		`CREATE TABLE type_varchar (val VARCHAR)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_varchar VALUES ('hello'), (''), ('world')`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_varchar ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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

func testTypeVarcharUnicode(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE type_varchar_unicode (val VARCHAR)`,
	)
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
		_, err = db.Exec(
			`INSERT INTO type_varchar_unicode VALUES ($1)`,
			s,
		)
		require.NoError(t, err)
	}

	rows, err := db.Query(
		`SELECT val FROM type_varchar_unicode ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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
	_, err := db.Exec(
		`CREATE TABLE type_blob (val BLOB)`,
	)
	require.NoError(t, err)

	// Insert binary data
	binaryData := []byte{
		0x00,
		0x01,
		0x02,
		0xFF,
		0xFE,
		0xFD,
	}
	_, err = db.Exec(
		`INSERT INTO type_blob VALUES ($1)`,
		binaryData,
	)
	require.NoError(t, err)

	var result []byte
	err = db.QueryRow(`SELECT val FROM type_blob`).
		Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, binaryData, result)
}

// Boolean tests

func testTypeBoolean(t *testing.T, db *sql.DB) {
	_, err := db.Exec(
		`CREATE TABLE type_boolean (val BOOLEAN)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_boolean VALUES (true), (false)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_boolean ORDER BY val`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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

// NULL handling tests

func testTypeNullInteger(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE type_null_int (val INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_null_int VALUES (NULL), (42)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_null_int ORDER BY val NULLS FIRST`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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

func testTypeNullVarchar(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE type_null_varchar (val VARCHAR)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_null_varchar VALUES (NULL), ('hello')`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_null_varchar ORDER BY val NULLS FIRST`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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

func testTypeNullBoolean(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE type_null_bool (val BOOLEAN)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO type_null_bool VALUES (NULL), (true)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT val FROM type_null_bool ORDER BY val NULLS FIRST`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

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

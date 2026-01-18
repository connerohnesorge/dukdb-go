package tests

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createWKBPoint creates a WKB-encoded POINT geometry for testing.
// WKB POINT: byte_order + type(1) + x(8 bytes) + y(8 bytes)
func createWKBPoint(x, y float64) []byte {
	buf := new(bytes.Buffer)
	// Byte order: 1 = little endian
	buf.WriteByte(0x01)
	// Type: 1 = Point (little endian)
	_ = binary.Write(buf, binary.LittleEndian, uint32(1))
	// X coordinate
	_ = binary.Write(buf, binary.LittleEndian, x)
	// Y coordinate
	_ = binary.Write(buf, binary.LittleEndian, y)

	return buf.Bytes()
}

// TestJSONColumnScanning verifies JSON columns scan to expected Go types.
func TestJSONColumnScanning(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with JSON column
	_, err = db.Exec(`CREATE TABLE json_scan_test (id INTEGER, data JSON)`)
	require.NoError(t, err)

	t.Run("Scan JSON to string", func(t *testing.T) {
		jsonData := `{"name":"Alice","age":30,"active":true}`
		_, err := db.Exec(`INSERT INTO json_scan_test VALUES (1, $1)`, jsonData)
		require.NoError(t, err)

		var data string
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 1`).Scan(&data)
		require.NoError(t, err)

		// Verify the string contains valid JSON
		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "Alice", parsed["name"])
		assert.Equal(t, float64(30), parsed["age"])
		assert.Equal(t, true, parsed["active"])
	})

	t.Run("Scan JSON array to string and unmarshal", func(t *testing.T) {
		jsonData := `[1,2,3,4,5]`
		_, err := db.Exec(`INSERT INTO json_scan_test VALUES (2, $1)`, jsonData)
		require.NoError(t, err)

		var data string
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 2`).Scan(&data)
		require.NoError(t, err)

		var parsed []any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		require.Len(t, parsed, 5)
		assert.Equal(t, float64(1), parsed[0])
		assert.Equal(t, float64(5), parsed[4])
	})

	t.Run("Scan JSON to NullString for NULL handling", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO json_scan_test VALUES (3, NULL)`)
		require.NoError(t, err)

		var data sql.NullString
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 3`).Scan(&data)
		require.NoError(t, err)
		assert.False(t, data.Valid, "NULL JSON should scan to invalid NullString")
	})

	t.Run("Scan JSON primitive types", func(t *testing.T) {
		// Insert various primitive types
		_, err := db.Exec(`INSERT INTO json_scan_test VALUES (4, $1)`, `"hello"`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO json_scan_test VALUES (5, $1)`, `42`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO json_scan_test VALUES (6, $1)`, `true`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO json_scan_test VALUES (7, $1)`, `null`)
		require.NoError(t, err)

		// Verify string primitive
		var strData string
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 4`).Scan(&strData)
		require.NoError(t, err)
		var strParsed string
		err = json.Unmarshal([]byte(strData), &strParsed)
		require.NoError(t, err)
		assert.Equal(t, "hello", strParsed)

		// Verify number primitive
		var numData string
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 5`).Scan(&numData)
		require.NoError(t, err)
		var numParsed float64
		err = json.Unmarshal([]byte(numData), &numParsed)
		require.NoError(t, err)
		assert.Equal(t, float64(42), numParsed)

		// Verify boolean primitive
		var boolData string
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 6`).Scan(&boolData)
		require.NoError(t, err)
		var boolParsed bool
		err = json.Unmarshal([]byte(boolData), &boolParsed)
		require.NoError(t, err)
		assert.True(t, boolParsed)

		// Verify null primitive (JSON null, not SQL NULL)
		var nullData string
		err = db.QueryRow(`SELECT data FROM json_scan_test WHERE id = 7`).Scan(&nullData)
		require.NoError(t, err)
		var nullParsed any
		err = json.Unmarshal([]byte(nullData), &nullParsed)
		require.NoError(t, err)
		assert.Nil(t, nullParsed)
	})
}

// TestGeometryColumnScanning verifies GEOMETRY columns scan to expected types.
func TestGeometryColumnScanning(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	t.Run("Scan geometry created with ST_Point", func(t *testing.T) {
		// Create a point using ST_Point function
		var wkt string
		err := db.QueryRow(`SELECT ST_AsText(ST_Point(3.0, 4.0))`).Scan(&wkt)
		require.NoError(t, err)
		assert.Equal(t, "POINT(3 4)", wkt)
	})

	t.Run("Scan geometry coordinates with ST_X and ST_Y", func(t *testing.T) {
		var x, y float64
		err := db.QueryRow(`SELECT ST_X(ST_Point(5.5, 6.5)), ST_Y(ST_Point(5.5, 6.5))`).Scan(&x, &y)
		require.NoError(t, err)
		assert.InDelta(t, 5.5, x, 0.0001)
		assert.InDelta(t, 6.5, y, 0.0001)
	})

	t.Run("Scan geometry type with ST_GeometryType", func(t *testing.T) {
		var geomType string
		err := db.QueryRow(`SELECT ST_GeometryType(ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'))`).
			Scan(&geomType)
		require.NoError(t, err)
		assert.Equal(t, "POLYGON", geomType)
	})

	t.Run("Scan NULL geometry", func(t *testing.T) {
		var result sql.NullFloat64
		err := db.QueryRow(`SELECT ST_X(ST_GeomFromText(NULL))`).Scan(&result)
		require.NoError(t, err)
		assert.False(t, result.Valid, "NULL geometry should scan to invalid NullFloat64")
	})

	t.Run("Scan geometry from WKB stored in BLOB column", func(t *testing.T) {
		// Create table with BLOB column
		_, err := db.Exec(`CREATE TABLE geom_blob_test (id INTEGER, geom BLOB)`)
		require.NoError(t, err)

		// Create WKB for a point (1.0, 2.0)
		pointWKB := createWKBPoint(1.0, 2.0)
		_, err = db.Exec(`INSERT INTO geom_blob_test VALUES (1, $1)`, pointWKB)
		require.NoError(t, err)

		// Scan back to []byte
		var geomData []byte
		err = db.QueryRow(`SELECT geom FROM geom_blob_test WHERE id = 1`).Scan(&geomData)
		require.NoError(t, err)
		assert.Equal(t, pointWKB, geomData)

		// Verify it's valid WKB (byte order + type)
		assert.Equal(t, byte(0x01), geomData[0]) // Little endian
	})
}

// TestBignumColumnScanning verifies BIGNUM columns scan correctly.
func TestBignumColumnScanning(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with BIGNUM column
	_, err = db.Exec(`CREATE TABLE bignum_scan_test (id INTEGER, value BIGNUM)`)
	require.NoError(t, err)

	t.Run("Scan BIGNUM to string and convert to big.Int", func(t *testing.T) {
		// Insert a number larger than int64
		largeNum := "123456789012345678901234567890"
		_, err := db.Exec(`INSERT INTO bignum_scan_test VALUES (1, $1)`, largeNum)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_scan_test WHERE id = 1`).Scan(&value)
		require.NoError(t, err)

		// Convert to big.Int and verify
		expected, ok := new(big.Int).SetString(largeNum, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Scan negative BIGNUM", func(t *testing.T) {
		negNum := "-98765432109876543210"
		_, err := db.Exec(`INSERT INTO bignum_scan_test VALUES (2, $1)`, negNum)
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_scan_test WHERE id = 2`).Scan(&value)
		require.NoError(t, err)

		expected, ok := new(big.Int).SetString(negNum, 10)
		require.True(t, ok)
		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))
	})

	t.Run("Scan BIGNUM to NullString for NULL handling", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO bignum_scan_test VALUES (3, NULL)`)
		require.NoError(t, err)

		var value sql.NullString
		err = db.QueryRow(`SELECT value FROM bignum_scan_test WHERE id = 3`).Scan(&value)
		require.NoError(t, err)
		assert.False(t, value.Valid, "NULL BIGNUM should scan to invalid NullString")
	})

	t.Run("Scan very large BIGNUM (beyond int64 range)", func(t *testing.T) {
		// 2^100 is way beyond int64 max
		hugeValue := new(big.Int).Exp(big.NewInt(2), big.NewInt(100), nil)
		_, err := db.Exec(`INSERT INTO bignum_scan_test VALUES (4, $1)`, hugeValue.String())
		require.NoError(t, err)

		var value string
		err = db.QueryRow(`SELECT value FROM bignum_scan_test WHERE id = 4`).Scan(&value)
		require.NoError(t, err)

		actual, ok := new(big.Int).SetString(value, 10)
		require.True(t, ok)
		assert.Equal(t, 0, hugeValue.Cmp(actual))
	})
}

// TestVariantColumnScanning verifies VARIANT columns scan correctly.
func TestVariantColumnScanning(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with VARIANT column
	_, err = db.Exec(`CREATE TABLE variant_scan_test (id INTEGER, data VARIANT)`)
	require.NoError(t, err)

	t.Run("Scan VARIANT object to string and unmarshal", func(t *testing.T) {
		variantData := `{"name":"Bob","score":95}`
		_, err := db.Exec(`INSERT INTO variant_scan_test VALUES (1, $1)`, variantData)
		require.NoError(t, err)

		var data string
		err = db.QueryRow(`SELECT data FROM variant_scan_test WHERE id = 1`).Scan(&data)
		require.NoError(t, err)

		var parsed map[string]any
		err = json.Unmarshal([]byte(data), &parsed)
		require.NoError(t, err)
		assert.Equal(t, "Bob", parsed["name"])
		assert.Equal(t, float64(95), parsed["score"])
	})

	t.Run("Scan VARIANT primitives", func(t *testing.T) {
		// Insert various types
		_, err := db.Exec(`INSERT INTO variant_scan_test VALUES (2, $1)`, `42`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO variant_scan_test VALUES (3, $1)`, `"hello"`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO variant_scan_test VALUES (4, $1)`, `true`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO variant_scan_test VALUES (5, $1)`, `[1,2,3]`)
		require.NoError(t, err)

		// Verify number
		var numStr string
		err = db.QueryRow(`SELECT data FROM variant_scan_test WHERE id = 2`).Scan(&numStr)
		require.NoError(t, err)
		var num float64
		err = json.Unmarshal([]byte(numStr), &num)
		require.NoError(t, err)
		assert.Equal(t, float64(42), num)

		// Verify string
		var strStr string
		err = db.QueryRow(`SELECT data FROM variant_scan_test WHERE id = 3`).Scan(&strStr)
		require.NoError(t, err)
		var str string
		err = json.Unmarshal([]byte(strStr), &str)
		require.NoError(t, err)
		assert.Equal(t, "hello", str)

		// Verify boolean
		var boolStr string
		err = db.QueryRow(`SELECT data FROM variant_scan_test WHERE id = 4`).Scan(&boolStr)
		require.NoError(t, err)
		var b bool
		err = json.Unmarshal([]byte(boolStr), &b)
		require.NoError(t, err)
		assert.True(t, b)

		// Verify array
		var arrStr string
		err = db.QueryRow(`SELECT data FROM variant_scan_test WHERE id = 5`).Scan(&arrStr)
		require.NoError(t, err)
		var arr []any
		err = json.Unmarshal([]byte(arrStr), &arr)
		require.NoError(t, err)
		assert.Len(t, arr, 3)
	})

	t.Run("Scan VARIANT to NullString for NULL handling", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO variant_scan_test VALUES (6, NULL)`)
		require.NoError(t, err)

		var data sql.NullString
		err = db.QueryRow(`SELECT data FROM variant_scan_test WHERE id = 6`).Scan(&data)
		require.NoError(t, err)
		assert.False(t, data.Valid, "NULL VARIANT should scan to invalid NullString")
	})
}

// TestLambdaColumnScanning verifies LAMBDA columns scan correctly.
func TestLambdaColumnScanning(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with LAMBDA column
	_, err = db.Exec(`CREATE TABLE lambda_scan_test (id INTEGER, expr LAMBDA)`)
	require.NoError(t, err)

	t.Run("Scan LAMBDA expression to string", func(t *testing.T) {
		lambdaExpr := "x -> x + 1"
		_, err := db.Exec(`INSERT INTO lambda_scan_test VALUES (1, $1)`, lambdaExpr)
		require.NoError(t, err)

		var expr string
		err = db.QueryRow(`SELECT expr FROM lambda_scan_test WHERE id = 1`).Scan(&expr)
		require.NoError(t, err)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Scan multi-parameter LAMBDA", func(t *testing.T) {
		lambdaExpr := "(x, y) -> x * y"
		_, err := db.Exec(`INSERT INTO lambda_scan_test VALUES (2, $1)`, lambdaExpr)
		require.NoError(t, err)

		var expr string
		err = db.QueryRow(`SELECT expr FROM lambda_scan_test WHERE id = 2`).Scan(&expr)
		require.NoError(t, err)
		assert.Equal(t, lambdaExpr, expr)
	})

	t.Run("Scan LAMBDA to NullString for NULL handling", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO lambda_scan_test VALUES (3, NULL)`)
		require.NoError(t, err)

		var expr sql.NullString
		err = db.QueryRow(`SELECT expr FROM lambda_scan_test WHERE id = 3`).Scan(&expr)
		require.NoError(t, err)
		assert.False(t, expr.Valid, "NULL LAMBDA should scan to invalid NullString")
	})

	t.Run("Scan complex LAMBDA expressions", func(t *testing.T) {
		testCases := []string{
			"(a, b, c) -> a * b + c",
			"x -> upper(trim(x))",
			"n -> CASE WHEN n > 0 THEN n ELSE -n END",
		}

		for i, lambdaExpr := range testCases {
			id := i + 10
			_, err := db.Exec(`INSERT INTO lambda_scan_test VALUES ($1, $2)`, id, lambdaExpr)
			require.NoError(t, err)

			var expr string
			err = db.QueryRow(`SELECT expr FROM lambda_scan_test WHERE id = $1`, id).Scan(&expr)
			require.NoError(t, err)
			assert.Equal(t, lambdaExpr, expr)
		}
	})
}

// TestExtendedTypesInQueries tests using extended types in complex queries.
func TestExtendedTypesInQueries(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	t.Run("JSON operators in WHERE clause", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE json_where_test (id INTEGER, data VARCHAR)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO json_where_test VALUES
			(1, '{"name": "Alice", "age": 30}'),
			(2, '{"name": "Bob", "age": 25}'),
			(3, '{"name": "Charlie", "age": 35}')`)
		require.NoError(t, err)

		// Query using JSON operator in WHERE clause
		rows, err := db.Query(
			`SELECT id, data->>'name' FROM json_where_test WHERE data->'name' = '"Alice"'`,
		)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()

		var ids []int
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1}, ids)
	})

	t.Run("Geometry functions in SELECT", func(t *testing.T) {
		// Test distance calculation
		var distance float64
		err := db.QueryRow(`SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))`).Scan(&distance)
		require.NoError(t, err)
		assert.InDelta(t, 5.0, distance, 0.0001)

		// Test spatial predicate
		var contains bool
		err = db.QueryRow(`SELECT ST_Contains(
			ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),
			ST_Point(5, 5)
		)`).Scan(&contains)
		require.NoError(t, err)
		assert.True(t, contains)
	})

	t.Run("Combining multiple extended types in one query", func(t *testing.T) {
		// Create a table with mixed types
		_, err := db.Exec(`CREATE TABLE mixed_types (
			id INTEGER,
			json_data JSON,
			bignum_val BIGNUM,
			variant_data VARIANT,
			lambda_expr LAMBDA
		)`)
		require.NoError(t, err)

		// Insert data with all types
		_, err = db.Exec(`INSERT INTO mixed_types VALUES ($1, $2, $3, $4, $5)`,
			1,
			`{"key":"value"}`,
			"123456789012345678901234567890",
			`{"type":"variant"}`,
			"x -> x + 1",
		)
		require.NoError(t, err)

		// Query all columns
		var id int
		var jsonData, bignumVal, variantData, lambdaExpr string
		err = db.QueryRow(`SELECT id, json_data, bignum_val, variant_data, lambda_expr FROM mixed_types WHERE id = 1`).
			Scan(&id, &jsonData, &bignumVal, &variantData, &lambdaExpr)
		require.NoError(t, err)

		assert.Equal(t, 1, id)

		// Verify JSON
		var jsonParsed map[string]any
		err = json.Unmarshal([]byte(jsonData), &jsonParsed)
		require.NoError(t, err)
		assert.Equal(t, "value", jsonParsed["key"])

		// Verify BIGNUM
		expected, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
		actual, ok := new(big.Int).SetString(bignumVal, 10)
		require.True(t, ok)
		assert.Equal(t, 0, expected.Cmp(actual))

		// Verify VARIANT
		var variantParsed map[string]any
		err = json.Unmarshal([]byte(variantData), &variantParsed)
		require.NoError(t, err)
		assert.Equal(t, "variant", variantParsed["type"])

		// Verify LAMBDA
		assert.Equal(t, "x -> x + 1", lambdaExpr)
	})

	t.Run("JSON extraction from JSON column type", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE json_extract_test (id INTEGER, data JSON)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO json_extract_test VALUES (1, $1)`,
			`{"users":[{"name":"Alice"},{"name":"Bob"}],"count":2}`)
		require.NoError(t, err)

		// Test json_extract function
		var result string
		err = db.QueryRow(`SELECT json_extract(data, 'count') FROM json_extract_test WHERE id = 1`).
			Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, "2", result)
	})
}

// TestTypeConversionCompatibility tests type conversions similar to go-duckdb behavior.
func TestTypeConversionCompatibility(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	t.Run("JSON to string implicit conversion", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE json_conv_test (id INTEGER, data JSON)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO json_conv_test VALUES (1, $1)`, `{"test":true}`)
		require.NoError(t, err)

		// Should be able to scan to string
		var str string
		err = db.QueryRow(`SELECT data FROM json_conv_test WHERE id = 1`).Scan(&str)
		require.NoError(t, err)
		assert.Contains(t, str, "test")
		assert.Contains(t, str, "true")
	})

	t.Run("BIGNUM string representation", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE bignum_conv_test (id INTEGER, val BIGNUM)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO bignum_conv_test VALUES (1, $1)`, "999")
		require.NoError(t, err)

		// Should scan to string
		var str string
		err = db.QueryRow(`SELECT val FROM bignum_conv_test WHERE id = 1`).Scan(&str)
		require.NoError(t, err)
		assert.Equal(t, "999", str)
	})

	t.Run("Geometry WKT conversion", func(t *testing.T) {
		// ST_AsText should return WKT string
		var wkt string
		err := db.QueryRow(`SELECT ST_AsText(ST_Point(1, 2))`).Scan(&wkt)
		require.NoError(t, err)
		assert.Equal(t, "POINT(1 2)", wkt)
	})
}

// TestRowsColumnTypes tests that column type information is available.
func TestRowsColumnTypes(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Create table with various extended types
	_, err = db.Exec(`CREATE TABLE type_info_test (
		json_col JSON,
		bignum_col BIGNUM,
		variant_col VARIANT,
		lambda_col LAMBDA
	)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO type_info_test VALUES ($1, $2, $3, $4)`,
		`{}`, "123", `{}`, "x -> x")
	require.NoError(t, err)

	rows, err := db.Query(`SELECT * FROM type_info_test`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	cols, err := rows.ColumnTypes()
	require.NoError(t, err)
	require.Len(t, cols, 4)

	// Verify column names
	assert.Equal(t, "json_col", cols[0].Name())
	assert.Equal(t, "bignum_col", cols[1].Name())
	assert.Equal(t, "variant_col", cols[2].Name())
	assert.Equal(t, "lambda_col", cols[3].Name())
}

// TestBatchInsertExtendedTypes tests batch inserts with extended types.
func TestBatchInsertExtendedTypes(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`CREATE TABLE batch_test (
		id INTEGER,
		json_data JSON,
		bignum_val BIGNUM
	)`)
	require.NoError(t, err)

	// Prepare statement for batch insert
	stmt, err := db.Prepare(`INSERT INTO batch_test VALUES ($1, $2, $3)`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, stmt.Close())
	}()

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		jsonData := `{"id":` + string(rune('0'+i)) + `}`
		bignumVal := "1234567890123456789" + string(rune('0'+i))
		_, err = stmt.Exec(i, jsonData, bignumVal)
		require.NoError(t, err)
	}

	// Verify count
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM batch_test`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	// Verify data integrity
	rows, err := db.Query(`SELECT id, json_data, bignum_val FROM batch_test ORDER BY id`)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	rowCount := 0
	for rows.Next() {
		var id int
		var jsonData, bignumVal string
		require.NoError(t, rows.Scan(&id, &jsonData, &bignumVal))
		rowCount++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 10, rowCount)
}

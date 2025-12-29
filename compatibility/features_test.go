package compatibility

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FeatureCompatibilityTests verifies advanced features work correctly.
var FeatureCompatibilityTests = []CompatibilityTest{
	// Appender functionality (basic via SQL)
	{Name: "AppenderStyleInsert", Category: "feature", Test: testAppenderStyleInsert},
	{Name: "AppenderStyleBatch", Category: "feature", Test: testAppenderStyleBatch},

	// Advanced SQL features
	{Name: "CTERecursive", Category: "feature", Test: testCTERecursive},
	{Name: "GenerateSeries", Category: "feature", Test: testGenerateSeries},
	{Name: "ArrayAgg", Category: "feature", Test: testArrayAgg},
	{Name: "StringAgg", Category: "feature", Test: testStringAgg},
	{Name: "Coalesce", Category: "feature", Test: testCoalesce},
	{Name: "NullIf", Category: "feature", Test: testNullIf},
	{Name: "Case", Category: "feature", Test: testCase},
	{Name: "Cast", Category: "feature", Test: testCast},

	// String functions
	{Name: "StringConcat", Category: "feature", Test: testStringConcat},
	{Name: "StringLength", Category: "feature", Test: testStringLength},
	{Name: "StringUpper", Category: "feature", Test: testStringUpper},
	{Name: "StringLower", Category: "feature", Test: testStringLower},
	{Name: "StringTrim", Category: "feature", Test: testStringTrim},
	{Name: "StringSubstring", Category: "feature", Test: testStringSubstring},
	{Name: "StringReplace", Category: "feature", Test: testStringReplace},

	// Numeric functions
	{Name: "MathAbs", Category: "feature", Test: testMathAbs},
	{Name: "MathRound", Category: "feature", Test: testMathRound},
	{Name: "MathFloor", Category: "feature", Test: testMathFloor},
	{Name: "MathCeil", Category: "feature", Test: testMathCeil},
	{Name: "MathPower", Category: "feature", Test: testMathPower},
	{Name: "MathSqrt", Category: "feature", Test: testMathSqrt},

	// Date/Time functions
	{Name: "DateCurrent", Category: "feature", Test: testDateCurrent},
	{Name: "DateExtract", Category: "feature", Test: testDateExtract},
	{Name: "DateTrunc", Category: "feature", Test: testDateTrunc},
	{Name: "DateDiff", Category: "feature", Test: testDateDiff},

	// JSON functions
	{Name: "JSONExtract", Category: "feature", Test: testJSONExtract},
	{Name: "JSONArray", Category: "feature", Test: testJSONArray},
	{Name: "JSONObject", Category: "feature", Test: testJSONObject},

	// Sequences and ranges
	{Name: "Sequence", Category: "feature", Test: testSequence},
	{Name: "Range", Category: "feature", Test: testRange},
}

// Appender-style tests (using SQL patterns similar to bulk inserts)

func testAppenderStyleInsert(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE appender_insert (id INTEGER, value VARCHAR)`)
	require.NoError(t, err)

	// Simulate appender pattern with multi-row insert
	_, err = db.Exec(`INSERT INTO appender_insert VALUES
		(1, 'first'),
		(2, 'second'),
		(3, 'third')`)
	require.NoError(t, err)

	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM appender_insert`).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func testAppenderStyleBatch(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE appender_batch (id INTEGER, value INTEGER)`)
	require.NoError(t, err)

	// Begin transaction for batched inserts
	tx, err := db.Begin()
	require.NoError(t, err)

	stmt, err := tx.Prepare(`INSERT INTO appender_batch VALUES ($1, $2)`)
	require.NoError(t, err)

	// Insert many rows
	for i := 1; i <= 100; i++ {
		_, err = stmt.Exec(i, i*10)
		require.NoError(t, err)
	}

	stmt.Close()
	require.NoError(t, tx.Commit())

	var count int
	var sum int64
	err = db.QueryRow(`SELECT COUNT(*), SUM(value) FROM appender_batch`).Scan(&count, &sum)
	require.NoError(t, err)
	assert.Equal(t, 100, count)
	assert.Equal(t, int64(50500), sum) // sum of 10+20+...+1000
}

// Advanced SQL features

func testCTERecursive(t *testing.T, db *sql.DB) {
	// Generate numbers 1-10 using recursive CTE
	rows, err := db.Query(`
		WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x+1 FROM cnt WHERE x < 10
		)
		SELECT x FROM cnt
	`)
	require.NoError(t, err)
	defer rows.Close()

	var numbers []int
	for rows.Next() {
		var x int
		require.NoError(t, rows.Scan(&x))
		numbers = append(numbers, x)
	}
	require.NoError(t, rows.Err())

	require.Len(t, numbers, 10)
	for i, n := range numbers {
		assert.Equal(t, i+1, n)
	}
}

func testGenerateSeries(t *testing.T, db *sql.DB) {
	rows, err := db.Query(`SELECT * FROM generate_series(1, 5)`)
	require.NoError(t, err)
	defer rows.Close()

	var numbers []int64
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		numbers = append(numbers, n)
	}
	require.NoError(t, rows.Err())

	require.Len(t, numbers, 5)
	assert.Equal(t, []int64{1, 2, 3, 4, 5}, numbers)
}

func testArrayAgg(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE array_agg_test (category VARCHAR, val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO array_agg_test VALUES ('A', 1), ('A', 2), ('B', 3)`)
	require.NoError(t, err)

	// DuckDB returns array as a special type, we verify it works
	rows, err := db.Query(`SELECT category, ARRAY_AGG(val ORDER BY val) as vals FROM array_agg_test GROUP BY category ORDER BY category`)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		var category string
		var vals any
		require.NoError(t, rows.Scan(&category, &vals))
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, count)
}

func testStringAgg(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE string_agg_test (category VARCHAR, name VARCHAR)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO string_agg_test VALUES ('A', 'Alice'), ('A', 'Bob'), ('B', 'Charlie')`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT category, STRING_AGG(name, ', ' ORDER BY name) as names FROM string_agg_test GROUP BY category ORDER BY category`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		category string
		names    string
	}
	for rows.Next() {
		var r struct {
			category string
			names    string
		}
		require.NoError(t, rows.Scan(&r.category, &r.names))
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	require.Len(t, results, 2)
	assert.Equal(t, "A", results[0].category)
	assert.Equal(t, "Alice, Bob", results[0].names)
	assert.Equal(t, "B", results[1].category)
	assert.Equal(t, "Charlie", results[1].names)
}

func testCoalesce(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT COALESCE(NULL, NULL, 'default', 'other')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "default", result)
}

func testNullIf(t *testing.T, db *sql.DB) {
	// NULLIF returns NULL if both arguments are equal
	var result sql.NullInt64
	err := db.QueryRow(`SELECT NULLIF(5, 5)`).Scan(&result)
	require.NoError(t, err)
	assert.False(t, result.Valid)

	// NULLIF returns first argument if they differ
	err = db.QueryRow(`SELECT NULLIF(5, 10)`).Scan(&result)
	require.NoError(t, err)
	assert.True(t, result.Valid)
	assert.Equal(t, int64(5), result.Int64)
}

func testCase(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`CREATE TABLE case_test (val INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO case_test VALUES (1), (2), (3)`)
	require.NoError(t, err)

	rows, err := db.Query(`
		SELECT val, CASE
			WHEN val = 1 THEN 'one'
			WHEN val = 2 THEN 'two'
			ELSE 'other'
		END as text
		FROM case_test ORDER BY val
	`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []string{"one", "two", "other"}
	var idx int
	for rows.Next() {
		var val int
		var text string
		require.NoError(t, rows.Scan(&val, &text))
		assert.Equal(t, expected[idx], text)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testCast(t *testing.T, db *sql.DB) {
	// Integer to string
	var strResult string
	err := db.QueryRow(`SELECT CAST(42 AS VARCHAR)`).Scan(&strResult)
	require.NoError(t, err)
	assert.Equal(t, "42", strResult)

	// String to integer
	var intResult int
	err = db.QueryRow(`SELECT CAST('123' AS INTEGER)`).Scan(&intResult)
	require.NoError(t, err)
	assert.Equal(t, 123, intResult)

	// Float to integer (truncation)
	err = db.QueryRow(`SELECT CAST(3.9 AS INTEGER)`).Scan(&intResult)
	require.NoError(t, err)
	assert.Equal(t, 3, intResult)
}

// String functions

func testStringConcat(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT 'Hello' || ' ' || 'World'`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello World", result)
}

func testStringLength(t *testing.T, db *sql.DB) {
	var length int
	err := db.QueryRow(`SELECT LENGTH('Hello')`).Scan(&length)
	require.NoError(t, err)
	assert.Equal(t, 5, length)
}

func testStringUpper(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT UPPER('hello')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "HELLO", result)
}

func testStringLower(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT LOWER('HELLO')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func testStringTrim(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT TRIM('  hello  ')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func testStringSubstring(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT SUBSTRING('Hello World', 1, 5)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello", result)
}

func testStringReplace(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT REPLACE('Hello World', 'World', 'DuckDB')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "Hello DuckDB", result)
}

// Numeric functions

func testMathAbs(t *testing.T, db *sql.DB) {
	var result int
	err := db.QueryRow(`SELECT ABS(-42)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}

func testMathRound(t *testing.T, db *sql.DB) {
	var result float64
	err := db.QueryRow(`SELECT ROUND(3.7)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 4.0, result)

	err = db.QueryRow(`SELECT ROUND(3.2)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 3.0, result)
}

func testMathFloor(t *testing.T, db *sql.DB) {
	var result float64
	err := db.QueryRow(`SELECT FLOOR(3.7)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 3.0, result)
}

func testMathCeil(t *testing.T, db *sql.DB) {
	var result float64
	err := db.QueryRow(`SELECT CEIL(3.2)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 4.0, result)
}

func testMathPower(t *testing.T, db *sql.DB) {
	var result float64
	err := db.QueryRow(`SELECT POWER(2, 10)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1024.0, result)
}

func testMathSqrt(t *testing.T, db *sql.DB) {
	var result float64
	err := db.QueryRow(`SELECT SQRT(16)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 4.0, result)
}

// Date/Time functions

func testDateCurrent(t *testing.T, db *sql.DB) {
	// Just verify the functions return something
	var date, timestamp any
	err := db.QueryRow(`SELECT CURRENT_DATE, CURRENT_TIMESTAMP`).Scan(&date, &timestamp)
	require.NoError(t, err)
	assert.NotNil(t, date)
	assert.NotNil(t, timestamp)
}

func testDateExtract(t *testing.T, db *sql.DB) {
	var year, month, day int
	err := db.QueryRow(`SELECT EXTRACT(YEAR FROM DATE '2024-06-15'),
		EXTRACT(MONTH FROM DATE '2024-06-15'),
		EXTRACT(DAY FROM DATE '2024-06-15')`).Scan(&year, &month, &day)
	require.NoError(t, err)
	assert.Equal(t, 2024, year)
	assert.Equal(t, 6, month)
	assert.Equal(t, 15, day)
}

func testDateTrunc(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT DATE_TRUNC('month', DATE '2024-06-15')::VARCHAR`).Scan(&result)
	require.NoError(t, err)
	assert.Contains(t, result, "2024-06-01")
}

func testDateDiff(t *testing.T, db *sql.DB) {
	var diff int
	err := db.QueryRow(`SELECT DATE_DIFF('day', DATE '2024-01-01', DATE '2024-01-10')`).Scan(&diff)
	require.NoError(t, err)
	assert.Equal(t, 9, diff)
}

// JSON functions

func testJSONExtract(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT JSON_EXTRACT('{"name": "Alice", "age": 30}', '$.name')`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, `"Alice"`, result)
}

func testJSONArray(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT JSON_ARRAY(1, 2, 3)`).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, "[1,2,3]", result)
}

func testJSONObject(t *testing.T, db *sql.DB) {
	var result string
	err := db.QueryRow(`SELECT JSON_OBJECT('name', 'Alice', 'age', 30)`).Scan(&result)
	require.NoError(t, err)
	assert.Contains(t, result, `"name"`)
	assert.Contains(t, result, `"Alice"`)
}

// Sequence and range

func testSequence(t *testing.T, db *sql.DB) {
	// Create a sequence
	_, err := db.Exec(`CREATE SEQUENCE test_seq START 1`)
	require.NoError(t, err)

	var val1, val2 int64
	err = db.QueryRow(`SELECT NEXTVAL('test_seq')`).Scan(&val1)
	require.NoError(t, err)

	err = db.QueryRow(`SELECT NEXTVAL('test_seq')`).Scan(&val2)
	require.NoError(t, err)

	assert.Equal(t, int64(1), val1)
	assert.Equal(t, int64(2), val2)
}

func testRange(t *testing.T, db *sql.DB) {
	rows, err := db.Query(`SELECT * FROM range(0, 5)`)
	require.NoError(t, err)
	defer rows.Close()

	var numbers []int64
	for rows.Next() {
		var n int64
		require.NoError(t, rows.Scan(&n))
		numbers = append(numbers, n)
	}
	require.NoError(t, rows.Err())

	require.Len(t, numbers, 5)
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, numbers)
}

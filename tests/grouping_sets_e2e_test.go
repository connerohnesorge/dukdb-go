package tests

import (
	"database/sql"
	"sort"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupGSSalesTable creates and populates the gs_sales test table.
func setupGSSalesTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE gs_sales(region VARCHAR, product VARCHAR, amount INTEGER)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO gs_sales VALUES
		('East', 'A', 100),
		('East', 'B', 200),
		('West', 'A', 150),
		('West', 'B', 250)`)
	require.NoError(t, err)
}

// gsRow represents a result row for grouping sets queries.
type gsRow struct {
	Region  sql.NullString
	Product sql.NullString
	Total   sql.NullInt64
}

// gsRowKey returns a string key for sorting/comparison.
func gsRowKey(r gsRow) string {
	region := "NULL"
	if r.Region.Valid {
		region = r.Region.String
	}
	product := "NULL"
	if r.Product.Valid {
		product = r.Product.String
	}
	total := "NULL"
	if r.Total.Valid {
		total = string(rune(r.Total.Int64 + '0'))
	}
	return region + "|" + product + "|" + total
}

// scanGSRows scans rows into gsRow slices.
func scanGSRows(t *testing.T, rows *sql.Rows) []gsRow {
	t.Helper()
	var result []gsRow
	for rows.Next() {
		var r gsRow
		err := rows.Scan(&r.Region, &r.Product, &r.Total)
		require.NoError(t, err)
		result = append(result, r)
	}
	require.NoError(t, rows.Err())
	return result
}

// assertGSRowsMatch asserts that actual rows match expected rows (order independent).
func assertGSRowsMatch(t *testing.T, expected, actual []gsRow) {
	t.Helper()
	require.Equal(t, len(expected), len(actual), "row count mismatch")

	sortRows := func(rows []gsRow) {
		sort.Slice(rows, func(i, j int) bool {
			return gsRowKey(rows[i]) < gsRowKey(rows[j])
		})
	}

	sortRows(expected)
	sortRows(actual)

	for i := range expected {
		assert.Equal(t, expected[i].Region.Valid, actual[i].Region.Valid, "row %d region valid", i)
		if expected[i].Region.Valid {
			assert.Equal(t, expected[i].Region.String, actual[i].Region.String, "row %d region", i)
		}
		assert.Equal(t, expected[i].Product.Valid, actual[i].Product.Valid, "row %d product valid", i)
		if expected[i].Product.Valid {
			assert.Equal(t, expected[i].Product.String, actual[i].Product.String, "row %d product", i)
		}
		assert.Equal(t, expected[i].Total.Valid, actual[i].Total.Valid, "row %d total valid", i)
		if expected[i].Total.Valid {
			assert.Equal(t, expected[i].Total.Int64, actual[i].Total.Int64, "row %d total", i)
		}
	}
}

func ns(s string) sql.NullString {
	return sql.NullString{String: s, Valid: true}
}

func ni(v int64) sql.NullInt64 {
	return sql.NullInt64{Int64: v, Valid: true}
}

var nullStr sql.NullString
var nullInt sql.NullInt64

// TestGroupingSetsBasic tests explicit GROUPING SETS with SUM.
func TestGroupingSetsBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupGSSalesTable(t, db)

	rows, err := db.Query(`SELECT region, product, SUM(amount) AS total FROM gs_sales GROUP BY GROUPING SETS ((region, product), (region), ())`)
	require.NoError(t, err)
	defer rows.Close()

	actual := scanGSRows(t, rows)

	expected := []gsRow{
		{ns("East"), ns("A"), ni(100)},
		{ns("East"), ns("B"), ni(200)},
		{ns("West"), ns("A"), ni(150)},
		{ns("West"), ns("B"), ni(250)},
		{ns("East"), nullStr, ni(300)},
		{ns("West"), nullStr, ni(400)},
		{nullStr, nullStr, ni(700)},
	}

	assertGSRowsMatch(t, expected, actual)
}

// TestRollupBasic tests ROLLUP produces same as equivalent GROUPING SETS.
func TestRollupBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupGSSalesTable(t, db)

	rows, err := db.Query(`SELECT region, product, SUM(amount) AS total FROM gs_sales GROUP BY ROLLUP(region, product)`)
	require.NoError(t, err)
	defer rows.Close()

	actual := scanGSRows(t, rows)

	expected := []gsRow{
		{ns("East"), ns("A"), ni(100)},
		{ns("East"), ns("B"), ni(200)},
		{ns("West"), ns("A"), ni(150)},
		{ns("West"), ns("B"), ni(250)},
		{ns("East"), nullStr, ni(300)},
		{ns("West"), nullStr, ni(400)},
		{nullStr, nullStr, ni(700)},
	}

	assertGSRowsMatch(t, expected, actual)
}

// TestCubeBasic tests CUBE produces all 2^n combinations.
func TestCubeBasic(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupGSSalesTable(t, db)

	rows, err := db.Query(`SELECT region, product, SUM(amount) AS total FROM gs_sales GROUP BY CUBE(region, product)`)
	require.NoError(t, err)
	defer rows.Close()

	actual := scanGSRows(t, rows)

	expected := []gsRow{
		// (region, product)
		{ns("East"), ns("A"), ni(100)},
		{ns("East"), ns("B"), ni(200)},
		{ns("West"), ns("A"), ni(150)},
		{ns("West"), ns("B"), ni(250)},
		// (region)
		{ns("East"), nullStr, ni(300)},
		{ns("West"), nullStr, ni(400)},
		// (product)
		{nullStr, ns("A"), ni(250)},
		{nullStr, ns("B"), ni(450)},
		// ()
		{nullStr, nullStr, ni(700)},
	}

	assertGSRowsMatch(t, expected, actual)
}

// TestGroupingFunction tests GROUPING() returns correct bitmask.
func TestGroupingFunction(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupGSSalesTable(t, db)

	rows, err := db.Query(`SELECT region, product, GROUPING(region) AS gr, GROUPING(product) AS gp, SUM(amount) AS total FROM gs_sales GROUP BY ROLLUP(region, product)`)
	require.NoError(t, err)
	defer rows.Close()

	type groupingRow struct {
		Region  sql.NullString
		Product sql.NullString
		GR      int64
		GP      int64
		Total   sql.NullInt64
	}

	var actual []groupingRow
	for rows.Next() {
		var r groupingRow
		err := rows.Scan(&r.Region, &r.Product, &r.GR, &r.GP, &r.Total)
		require.NoError(t, err)
		actual = append(actual, r)
	}
	require.NoError(t, rows.Err())

	// Verify grouping function values
	for _, r := range actual {
		if r.Region.Valid && r.Product.Valid {
			// Detail row: both grouped
			assert.Equal(t, int64(0), r.GR, "detail row gr should be 0")
			assert.Equal(t, int64(0), r.GP, "detail row gp should be 0")
		} else if r.Region.Valid && !r.Product.Valid {
			// Region subtotal: product not grouped
			assert.Equal(t, int64(0), r.GR, "region subtotal gr should be 0")
			assert.Equal(t, int64(1), r.GP, "region subtotal gp should be 1")
		} else if !r.Region.Valid && !r.Product.Valid {
			// Grand total: neither grouped
			assert.Equal(t, int64(1), r.GR, "grand total gr should be 1")
			assert.Equal(t, int64(1), r.GP, "grand total gp should be 1")
		}
	}

	// Should have 7 rows total
	assert.Equal(t, 7, len(actual))
}

// TestRollupSingleColumn tests ROLLUP with a single column.
func TestRollupSingleColumn(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupGSSalesTable(t, db)

	rows, err := db.Query(`SELECT region, SUM(amount) AS total FROM gs_sales GROUP BY ROLLUP(region)`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		Region sql.NullString
		Total  sql.NullInt64
	}
	var actual []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.Region, &r.Total)
		require.NoError(t, err)
		actual = append(actual, r)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, 3, len(actual), "expected 3 rows: East, West, grand total")

	// Sort by total for deterministic comparison
	sort.Slice(actual, func(i, j int) bool {
		if !actual[i].Total.Valid {
			return false
		}
		if !actual[j].Total.Valid {
			return true
		}
		return actual[i].Total.Int64 < actual[j].Total.Int64
	})

	assert.True(t, actual[0].Region.Valid)
	assert.Equal(t, "East", actual[0].Region.String)
	assert.Equal(t, int64(300), actual[0].Total.Int64)

	assert.True(t, actual[1].Region.Valid)
	assert.Equal(t, "West", actual[1].Region.String)
	assert.Equal(t, int64(400), actual[1].Total.Int64)

	assert.False(t, actual[2].Region.Valid, "grand total region should be NULL")
	assert.Equal(t, int64(700), actual[2].Total.Int64)
}

// TestGroupingSetsEmptyTable tests grouping sets on an empty table.
func TestGroupingSetsEmptyTable(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE gs_empty(a VARCHAR, b INTEGER)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT a, SUM(b) AS total FROM gs_empty GROUP BY ROLLUP(a)`)
	require.NoError(t, err)
	defer rows.Close()

	var actual []gsRow
	for rows.Next() {
		var r gsRow
		err := rows.Scan(&r.Region, &r.Total)
		require.NoError(t, err)
		actual = append(actual, r)
	}
	require.NoError(t, rows.Err())

	// Should have at least the grand total row
	require.GreaterOrEqual(t, len(actual), 1, "should have at least the grand total row")

	// The grand total should have a=NULL
	found := false
	for _, r := range actual {
		if !r.Region.Valid {
			found = true
			// SUM of no rows is NULL
			break
		}
	}
	assert.True(t, found, "should have a grand total row with a=NULL")
}

// TestGroupingSetsCountStar tests COUNT(*) with grouping sets.
func TestGroupingSetsCountStar(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	setupGSSalesTable(t, db)

	rows, err := db.Query(`SELECT region, COUNT(*) AS cnt FROM gs_sales GROUP BY ROLLUP(region)`)
	require.NoError(t, err)
	defer rows.Close()

	type row struct {
		Region sql.NullString
		Count  int64
	}
	var actual []row
	for rows.Next() {
		var r row
		err := rows.Scan(&r.Region, &r.Count)
		require.NoError(t, err)
		actual = append(actual, r)
	}
	require.NoError(t, rows.Err())

	require.Equal(t, 3, len(actual), "expected 3 rows")

	// Sort by count for deterministic comparison
	sort.Slice(actual, func(i, j int) bool {
		if actual[i].Count != actual[j].Count {
			return actual[i].Count < actual[j].Count
		}
		return actual[i].Region.String < actual[j].Region.String
	})

	// East: 2 rows, West: 2 rows, grand total: 4 rows
	assert.Equal(t, int64(2), actual[0].Count)
	assert.Equal(t, int64(2), actual[1].Count)
	assert.Equal(t, int64(4), actual[2].Count)
	assert.False(t, actual[2].Region.Valid, "grand total region should be NULL")
}

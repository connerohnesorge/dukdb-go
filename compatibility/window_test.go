package compatibility

import (
	"database/sql"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WindowCompatibilityTests covers window function operations.
// This includes ranking functions, value functions, aggregate window functions,
// and various frame specifications.
var WindowCompatibilityTests = []CompatibilityTest{
	// Ranking Functions (Tasks 5.1-5.5)
	{
		Name:     "WindowRowNumberWithoutPartition",
		Category: "window",
		Test:     testWindowRowNumberWithoutPartition,
	},
	{
		Name:     "WindowRowNumberWithPartition",
		Category: "window",
		Test:     testWindowRowNumberWithPartition,
	},
	{
		Name:     "WindowRankWithTies",
		Category: "window",
		Test:     testWindowRankWithTies,
	},
	{
		Name:     "WindowDenseRankWithTies",
		Category: "window",
		Test:     testWindowDenseRankWithTies,
	},
	{
		Name:     "WindowNtileVariousBuckets",
		Category: "window",
		Test:     testWindowNtileVariousBuckets,
	},

	// Value Functions (Tasks 5.6-5.10)
	{
		Name:     "WindowLagWithOffset",
		Category: "window",
		Test:     testWindowLagWithOffset,
	},
	{
		Name:     "WindowLagWithDefault",
		Category: "window",
		Test:     testWindowLagWithDefault,
	},
	{
		Name:     "WindowLeadWithOffset",
		Category: "window",
		Test:     testWindowLeadWithOffset,
	},
	{
		Name:     "WindowLeadWithDefault",
		Category: "window",
		Test:     testWindowLeadWithDefault,
	},
	{
		Name:     "WindowFirstValueWithFrame",
		Category: "window",
		Test:     testWindowFirstValueWithFrame,
	},
	{
		Name:     "WindowLastValueWithFrame",
		Category: "window",
		Test:     testWindowLastValueWithFrame,
	},
	{
		Name:     "WindowNthValueValid",
		Category: "window",
		Test:     testWindowNthValueValid,
	},
	{
		Name:     "WindowNthValueOutOfBounds",
		Category: "window",
		Test:     testWindowNthValueOutOfBounds,
	},

	// Distribution Functions (Tasks 5.11-5.13)
	{
		Name:     "WindowPercentRankDistribution",
		Category: "window",
		Test:     testWindowPercentRankDistribution,
	},
	{
		Name:     "WindowCumeDistWithTies",
		Category: "window",
		Test:     testWindowCumeDistWithTies,
	},
	{
		Name:     "WindowEdgeCaseSingleRow",
		Category: "window",
		Test:     testWindowEdgeCaseSingleRow,
	},
	{
		Name:     "WindowEdgeCaseAllTies",
		Category: "window",
		Test:     testWindowEdgeCaseAllTies,
	},

	// Aggregate Window Functions (Tasks 5.14-5.18)
	{
		Name:     "WindowSumRunningTotal",
		Category: "window",
		Test:     testWindowSumRunningTotal,
	},
	{
		Name:     "WindowCountFullPartition",
		Category: "window",
		Test:     testWindowCountFullPartition,
	},
	{
		Name:     "WindowAvgSlidingFrame",
		Category: "window",
		Test:     testWindowAvgSlidingFrame,
	},
	{
		Name:     "WindowMinMaxOver",
		Category: "window",
		Test:     testWindowMinMaxOver,
	},
	{
		Name:     "WindowMultipleAggregates",
		Category: "window",
		Test:     testWindowMultipleAggregates,
	},

	// Frame Specifications (Tasks 5.19-5.23)
	{
		Name:     "WindowRowsBetweenBounds",
		Category: "window",
		Test:     testWindowRowsBetweenBounds,
	},
	{
		Name:     "WindowRangeBetweenNumeric",
		Category: "window",
		Test:     testWindowRangeBetweenNumeric,
	},
	{
		Name:     "WindowGroupsBetweenPeers",
		Category: "window",
		Test:     testWindowGroupsBetweenPeers,
	},
	{
		Name:     "WindowDefaultFrameWithOrderBy",
		Category: "window",
		Test:     testWindowDefaultFrameWithOrderBy,
	},
	{
		Name:     "WindowDefaultFrameWithoutOrderBy",
		Category: "window",
		Test:     testWindowDefaultFrameWithoutOrderBy,
	},
	{
		Name:     "WindowFrameSingleBoundShorthand",
		Category: "window",
		Test:     testWindowFrameSingleBoundShorthand,
	},

	// EXCLUDE Clause (Tasks 5.24-5.27)
	{
		Name:     "WindowExcludeNoOthers",
		Category: "window",
		Test:     testWindowExcludeNoOthers,
	},
	{
		Name:     "WindowExcludeCurrentRow",
		Category: "window",
		Test:     testWindowExcludeCurrentRow,
	},
	{
		Name:     "WindowExcludeGroup",
		Category: "window",
		Test:     testWindowExcludeGroup,
	},
	{
		Name:     "WindowExcludeTies",
		Category: "window",
		Test:     testWindowExcludeTies,
	},

	// NULL Ordering (Tasks 5.28-5.31)
	{
		Name:     "WindowOrderByNullsFirst",
		Category: "window",
		Test:     testWindowOrderByNullsFirst,
	},
	{
		Name:     "WindowOrderByNullsLast",
		Category: "window",
		Test:     testWindowOrderByNullsLast,
	},
	{
		Name:     "WindowOrderByDescNullsFirst",
		Category: "window",
		Test:     testWindowOrderByDescNullsFirst,
	},
	{
		Name:     "WindowOrderByDescNullsLast",
		Category: "window",
		Test:     testWindowOrderByDescNullsLast,
	},

	// IGNORE NULLS (Tasks 5.32-5.36)
	{
		Name:     "WindowLagIgnoreNulls",
		Category: "window",
		Test:     testWindowLagIgnoreNulls,
	},
	{
		Name:     "WindowLeadIgnoreNulls",
		Category: "window",
		Test:     testWindowLeadIgnoreNulls,
	},
	{
		Name:     "WindowFirstValueIgnoreNulls",
		Category: "window",
		Test:     testWindowFirstValueIgnoreNulls,
	},
	{
		Name:     "WindowLastValueIgnoreNulls",
		Category: "window",
		Test:     testWindowLastValueIgnoreNulls,
	},
	{
		Name:     "WindowNthValueIgnoreNulls",
		Category: "window",
		Test:     testWindowNthValueIgnoreNulls,
	},

	// FILTER Clause (Tasks 5.37-5.39)
	{
		Name:     "WindowCountFilterWhere",
		Category: "window",
		Test:     testWindowCountFilterWhere,
	},
	{
		Name:     "WindowSumFilterWhere",
		Category: "window",
		Test:     testWindowSumFilterWhere,
	},
	{
		Name:     "WindowFilterWithPartitionBy",
		Category: "window",
		Test:     testWindowFilterWithPartitionBy,
	},

	// DISTINCT in Aggregates (Tasks 5.40-5.42)
	{
		Name:     "WindowCountDistinct",
		Category: "window",
		Test:     testWindowCountDistinct,
	},
	{
		Name:     "WindowSumDistinct",
		Category: "window",
		Test:     testWindowSumDistinct,
	},
	{
		Name:     "WindowDistinctWithNulls",
		Category: "window",
		Test:     testWindowDistinctWithNulls,
	},

	// NULL Handling (Tasks 5.43-5.46)
	{
		Name:     "WindowNullInPartitionBy",
		Category: "window",
		Test:     testWindowNullInPartitionBy,
	},
	{
		Name:     "WindowNullInOrderBy",
		Category: "window",
		Test:     testWindowNullInOrderBy,
	},
	{
		Name:     "WindowNullInAggregate",
		Category: "window",
		Test:     testWindowNullInAggregate,
	},
	{
		Name:     "WindowNullInLagLead",
		Category: "window",
		Test:     testWindowNullInLagLead,
	},
}

// ============================================================================
// Ranking Functions Tests (Tasks 5.1-5.5)
// ============================================================================

func testWindowRowNumberWithoutPartition(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE rownumber_test (id INTEGER, name VARCHAR)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO rownumber_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as rn FROM rownumber_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expected := []int64{1, 2, 3}
	idx := 0
	for rows.Next() {
		var id int
		var name string
		var rn int64
		require.NoError(t, rows.Scan(&id, &name, &rn))
		assert.Equal(t, expected[idx], rn, "row %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testWindowRowNumberWithPartition(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE rownumber_part (dept VARCHAR, name VARCHAR, salary INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO rownumber_part VALUES
		('Sales', 'Alice', 50000),
		('Sales', 'Bob', 60000),
		('IT', 'Charlie', 70000),
		('IT', 'Diana', 80000),
		('IT', 'Eve', 75000)`,
	)
	require.NoError(t, err)

	// Query without relying on alias-based ORDER BY
	// Order by dept and salary (which determines row_number order within partition)
	rows, err := db.Query(
		`SELECT dept, name, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
		FROM rownumber_part ORDER BY dept, salary DESC`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Expected results when ordered by dept, salary DESC:
	// IT: Diana(80000)->rn=1, Eve(75000)->rn=2, Charlie(70000)->rn=3
	// Sales: Bob(60000)->rn=1, Alice(50000)->rn=2
	type result struct {
		dept   string
		name   string
		salary int
		rn     int64
	}
	expectedResults := []result{
		{"IT", "Diana", 80000, 1},
		{"IT", "Eve", 75000, 2},
		{"IT", "Charlie", 70000, 3},
		{"Sales", "Bob", 60000, 1},
		{"Sales", "Alice", 50000, 2},
	}

	idx := 0
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.dept, &r.name, &r.salary, &r.rn))
		assert.Equal(t, expectedResults[idx].dept, r.dept, "dept at row %d", idx)
		assert.Equal(t, expectedResults[idx].rn, r.rn, "rn at row %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5, idx)
}

func testWindowRankWithTies(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE rank_test (name VARCHAR, score INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO rank_test VALUES
		('Alice', 100), ('Bob', 90), ('Charlie', 90), ('Diana', 80)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT name, score, RANK() OVER (ORDER BY score DESC) as rnk
		FROM rank_test ORDER BY score DESC, name`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Expected: Alice=1, Bob=2, Charlie=2, Diana=4 (with gap)
	expectedRanks := []int64{1, 2, 2, 4}
	idx := 0
	for rows.Next() {
		var name string
		var score int
		var rnk int64
		require.NoError(t, rows.Scan(&name, &score, &rnk))
		assert.Equal(t, expectedRanks[idx], rnk, "rank at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowDenseRankWithTies(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE denserank_test (name VARCHAR, score INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO denserank_test VALUES
		('Alice', 100), ('Bob', 90), ('Charlie', 90), ('Diana', 80)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk
		FROM denserank_test ORDER BY score DESC, name`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Expected: Alice=1, Bob=2, Charlie=2, Diana=3 (no gap)
	expectedRanks := []int64{1, 2, 2, 3}
	idx := 0
	for rows.Next() {
		var name string
		var score int
		var drnk int64
		require.NoError(t, rows.Scan(&name, &score, &drnk))
		assert.Equal(t, expectedRanks[idx], drnk, "dense_rank at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowNtileVariousBuckets(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE ntile_test (id INTEGER)`,
	)
	require.NoError(t, err)

	// Insert 10 rows
	_, err = db.Exec(
		`INSERT INTO ntile_test VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, NTILE(3) OVER (ORDER BY id) as bucket FROM ntile_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// 10 rows into 3 buckets: buckets 1,2 get 4 rows, bucket 3 gets 2 rows
	// Expected: 1,1,1,1,2,2,2,3,3,3 (with 10 rows, first 4 in bucket 1, next 3 in bucket 2, last 3 in bucket 3)
	// Actually: 10/3 = 3 with remainder 1, so bucket 1 gets 4, buckets 2,3 get 3
	expectedBuckets := []int64{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
	idx := 0
	for rows.Next() {
		var id int
		var bucket int64
		require.NoError(t, rows.Scan(&id, &bucket))
		assert.Equal(t, expectedBuckets[idx], bucket, "bucket at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 10, idx)
}

// ============================================================================
// Value Functions Tests (Tasks 5.6-5.10)
// ============================================================================

func testWindowLagWithOffset(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE lag_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO lag_test VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, LAG(value, 2) OVER (ORDER BY id) as lagged
		FROM lag_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	idx := 0
	for rows.Next() {
		var id, value int
		var lagged sql.NullInt64
		require.NoError(t, rows.Scan(&id, &value, &lagged))
		if idx < 2 {
			assert.False(t, lagged.Valid, "lagged should be NULL for first 2 rows")
		} else {
			assert.True(t, lagged.Valid)
			assert.Equal(t, int64((idx-1)*10), lagged.Int64, "lagged at position %d", idx)
		}
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowLagWithDefault(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE lag_default (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO lag_default VALUES (1, 10), (2, 20), (3, 30)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, LAG(value, 1, -1) OVER (ORDER BY id) as lagged
		FROM lag_default ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedLagged := []int64{-1, 10, 20}
	idx := 0
	for rows.Next() {
		var id, value int
		var lagged int64
		require.NoError(t, rows.Scan(&id, &value, &lagged))
		assert.Equal(t, expectedLagged[idx], lagged, "lagged at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testWindowLeadWithOffset(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE lead_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO lead_test VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, LEAD(value, 2) OVER (ORDER BY id) as led
		FROM lead_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	idx := 0
	for rows.Next() {
		var id, value int
		var led sql.NullInt64
		require.NoError(t, rows.Scan(&id, &value, &led))
		if idx >= 2 {
			assert.False(t, led.Valid, "led should be NULL for last 2 rows")
		} else {
			assert.True(t, led.Valid)
			assert.Equal(t, int64((idx+3)*10), led.Int64, "led at position %d", idx)
		}
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowLeadWithDefault(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE lead_default (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO lead_default VALUES (1, 10), (2, 20), (3, 30)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, LEAD(value, 1, 999) OVER (ORDER BY id) as led
		FROM lead_default ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedLed := []int64{20, 30, 999}
	idx := 0
	for rows.Next() {
		var id, value int
		var led int64
		require.NoError(t, rows.Scan(&id, &value, &led))
		assert.Equal(t, expectedLed[idx], led, "led at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testWindowFirstValueWithFrame(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE first_value_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO first_value_test VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		FIRST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as fv
		FROM first_value_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Frame for each row: row 0: [0,1], row 1: [0,2], row 2: [1,3], row 3: [2,3]
	// First values: 10, 10, 20, 30
	expectedFV := []int64{10, 10, 20, 30}
	idx := 0
	for rows.Next() {
		var id, value int
		var fv int64
		require.NoError(t, rows.Scan(&id, &value, &fv))
		assert.Equal(t, expectedFV[idx], fv, "first_value at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowLastValueWithFrame(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE last_value_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO last_value_test VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as lv
		FROM last_value_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Frame is entire partition, last value is always 40
	idx := 0
	for rows.Next() {
		var id, value int
		var lv int64
		require.NoError(t, rows.Scan(&id, &value, &lv))
		assert.Equal(t, int64(40), lv, "last_value at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowNthValueValid(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE nth_value_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO nth_value_test VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		NTH_VALUE(value, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as nv
		FROM nth_value_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// 2nd value in entire partition is 20
	idx := 0
	for rows.Next() {
		var id, value int
		var nv int64
		require.NoError(t, rows.Scan(&id, &value, &nv))
		assert.Equal(t, int64(20), nv, "nth_value at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowNthValueOutOfBounds(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE nth_value_oob (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO nth_value_oob VALUES (1, 10), (2, 20)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		NTH_VALUE(value, 5) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as nv
		FROM nth_value_oob ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// 5th value doesn't exist, should be NULL
	idx := 0
	for rows.Next() {
		var id, value int
		var nv sql.NullInt64
		require.NoError(t, rows.Scan(&id, &value, &nv))
		assert.False(t, nv.Valid, "nth_value should be NULL at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, idx)
}

// ============================================================================
// Distribution Functions Tests (Tasks 5.11-5.13)
// ============================================================================

func testWindowPercentRankDistribution(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE percent_rank_test (id INTEGER, score INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO percent_rank_test VALUES (1, 100), (2, 90), (3, 90), (4, 80), (5, 70)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, score, PERCENT_RANK() OVER (ORDER BY score DESC) as pr
		FROM percent_rank_test ORDER BY score DESC, id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Ranks: 1,2,2,4,5
	// PERCENT_RANK = (rank - 1) / (n - 1) = (rank - 1) / 4
	// Expected: 0, 0.25, 0.25, 0.75, 1.0
	expectedPR := []float64{0.0, 0.25, 0.25, 0.75, 1.0}
	idx := 0
	for rows.Next() {
		var id, score int
		var pr float64
		require.NoError(t, rows.Scan(&id, &score, &pr))
		assert.InDelta(t, expectedPR[idx], pr, 0.001, "percent_rank at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5, idx)
}

func testWindowCumeDistWithTies(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE cume_dist_test (id INTEGER, score INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO cume_dist_test VALUES (1, 100), (2, 90), (3, 90), (4, 80), (5, 70)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, score, CUME_DIST() OVER (ORDER BY score DESC) as cd
		FROM cume_dist_test ORDER BY score DESC, id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// CUME_DIST = (rows at or before) / n
	// For ties, all tied rows get the same CUME_DIST (the end of their peer group)
	// Expected: 1/5=0.2, 3/5=0.6, 3/5=0.6, 4/5=0.8, 5/5=1.0
	expectedCD := []float64{0.2, 0.6, 0.6, 0.8, 1.0}
	idx := 0
	for rows.Next() {
		var id, score int
		var cd float64
		require.NoError(t, rows.Scan(&id, &score, &cd))
		assert.InDelta(t, expectedCD[idx], cd, 0.001, "cume_dist at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5, idx)
}

func testWindowEdgeCaseSingleRow(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE single_row_test (id INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO single_row_test VALUES (1)`,
	)
	require.NoError(t, err)

	var pr float64
	var cd float64
	err = db.QueryRow(
		`SELECT PERCENT_RANK() OVER (ORDER BY id), CUME_DIST() OVER (ORDER BY id)
		FROM single_row_test`,
	).Scan(&pr, &cd)
	require.NoError(t, err)

	// Single row: PERCENT_RANK = 0, CUME_DIST = 1
	assert.Equal(t, 0.0, pr)
	assert.Equal(t, 1.0, cd)
}

func testWindowEdgeCaseAllTies(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE all_ties_test (id INTEGER, val INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO all_ties_test VALUES (1, 100), (2, 100), (3, 100)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, RANK() OVER (ORDER BY val) as rnk,
		DENSE_RANK() OVER (ORDER BY val) as drnk,
		PERCENT_RANK() OVER (ORDER BY val) as pr,
		CUME_DIST() OVER (ORDER BY val) as cd
		FROM all_ties_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// All ties: RANK=1, DENSE_RANK=1, PERCENT_RANK=0, CUME_DIST=1
	idx := 0
	for rows.Next() {
		var id int
		var rnk, drnk int64
		var pr, cd float64
		require.NoError(t, rows.Scan(&id, &rnk, &drnk, &pr, &cd))
		assert.Equal(t, int64(1), rnk)
		assert.Equal(t, int64(1), drnk)
		assert.Equal(t, 0.0, pr)
		assert.Equal(t, 1.0, cd)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// ============================================================================
// Aggregate Window Functions Tests (Tasks 5.14-5.18)
// ============================================================================

func testWindowSumRunningTotal(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE sum_running (id INTEGER, amount INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO sum_running VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, amount, SUM(amount) OVER (ORDER BY id) as running_total
		FROM sum_running ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Running total: 10, 30, 60, 100
	expectedRT := []float64{10, 30, 60, 100}
	idx := 0
	for rows.Next() {
		var id, amount int
		var rt float64
		require.NoError(t, rows.Scan(&id, &amount, &rt))
		assert.InDelta(t, expectedRT[idx], rt, 0.001, "running total at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowCountFullPartition(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE count_part (dept VARCHAR, name VARCHAR)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO count_part VALUES
		('Sales', 'Alice'), ('Sales', 'Bob'),
		('IT', 'Charlie'), ('IT', 'Diana'), ('IT', 'Eve')`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT dept, name, COUNT(*) OVER (PARTITION BY dept) as dept_count
		FROM count_part ORDER BY dept, name`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedCounts := map[string]int64{"IT": 3, "Sales": 2}
	for rows.Next() {
		var dept, name string
		var cnt int64
		require.NoError(t, rows.Scan(&dept, &name, &cnt))
		assert.Equal(t, expectedCounts[dept], cnt)
	}
	require.NoError(t, rows.Err())
}

func testWindowAvgSlidingFrame(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE avg_sliding (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO avg_sliding VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		AVG(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg
		FROM avg_sliding ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Moving averages:
	// Row 0: (10+20)/2 = 15
	// Row 1: (10+20+30)/3 = 20
	// Row 2: (20+30+40)/3 = 30
	// Row 3: (30+40+50)/3 = 40
	// Row 4: (40+50)/2 = 45
	expectedAvg := []float64{15, 20, 30, 40, 45}
	idx := 0
	for rows.Next() {
		var id, value int
		var avg float64
		require.NoError(t, rows.Scan(&id, &value, &avg))
		assert.InDelta(t, expectedAvg[idx], avg, 0.001, "moving avg at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5, idx)
}

func testWindowMinMaxOver(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE minmax_test (dept VARCHAR, salary INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO minmax_test VALUES
		('Sales', 50000), ('Sales', 60000),
		('IT', 70000), ('IT', 80000), ('IT', 75000)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT dept, salary,
		MIN(salary) OVER (PARTITION BY dept) as min_sal,
		MAX(salary) OVER (PARTITION BY dept) as max_sal
		FROM minmax_test ORDER BY dept, salary`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedMin := map[string]int{"IT": 70000, "Sales": 50000}
	expectedMax := map[string]int{"IT": 80000, "Sales": 60000}
	for rows.Next() {
		var dept string
		var salary, minSal, maxSal int
		require.NoError(t, rows.Scan(&dept, &salary, &minSal, &maxSal))
		assert.Equal(t, expectedMin[dept], minSal)
		assert.Equal(t, expectedMax[dept], maxSal)
	}
	require.NoError(t, rows.Err())
}

func testWindowMultipleAggregates(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE multi_agg (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO multi_agg VALUES (1, 10), (2, 20), (3, 30)`,
	)
	require.NoError(t, err)

	var sumVal float64
	var countVal int64
	var avgVal float64
	err = db.QueryRow(
		`SELECT
		SUM(value) OVER () as s,
		COUNT(*) OVER () as c,
		AVG(value) OVER () as a
		FROM multi_agg LIMIT 1`,
	).Scan(&sumVal, &countVal, &avgVal)
	require.NoError(t, err)

	assert.InDelta(t, 60.0, sumVal, 0.001)
	assert.Equal(t, int64(3), countVal)
	assert.InDelta(t, 20.0, avgVal, 0.001)
}

// ============================================================================
// Frame Specifications Tests (Tasks 5.19-5.23)
// ============================================================================

func testWindowRowsBetweenBounds(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE rows_between (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO rows_between VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		SUM(value) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as sum3
		FROM rows_between ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Sum of current and 2 preceding:
	// Row 0: 10
	// Row 1: 10+20 = 30
	// Row 2: 10+20+30 = 60
	// Row 3: 20+30+40 = 90
	// Row 4: 30+40+50 = 120
	expectedSum := []float64{10, 30, 60, 90, 120}
	idx := 0
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001, "sum at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5, idx)
}

func testWindowRangeBetweenNumeric(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE range_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO range_test VALUES (1, 10), (2, 20), (5, 50), (6, 60)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		SUM(value) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_sum
		FROM range_test ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Cumulative sum: 10, 30, 80, 140
	expectedSum := []float64{10, 30, 80, 140}
	idx := 0
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001, "cum_sum at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowGroupsBetweenPeers(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE groups_test (id INTEGER, score INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO groups_test VALUES
		(1, 100, 10), (2, 100, 20), (3, 90, 30), (4, 90, 40), (5, 80, 50)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, score, value,
		SUM(value) OVER (ORDER BY score DESC GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grp_sum
		FROM groups_test ORDER BY score DESC, id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Groups: {100: [10,20]}, {90: [30,40]}, {80: [50]}
	// Group sums: first group=30, first two groups=100, all groups=150
	expectedSum := []float64{30, 30, 100, 100, 150}
	idx := 0
	for rows.Next() {
		var id, score, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &score, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001, "grp_sum at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 5, idx)
}

func testWindowDefaultFrameWithOrderBy(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE default_frame_order (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO default_frame_order VALUES (1, 10), (2, 20), (3, 30)`,
	)
	require.NoError(t, err)

	// With ORDER BY, default frame is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
	rows, err := db.Query(
		`SELECT id, value, SUM(value) OVER (ORDER BY id) as running_sum
		FROM default_frame_order ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedSum := []float64{10, 30, 60}
	idx := 0
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testWindowDefaultFrameWithoutOrderBy(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE default_frame_no_order (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO default_frame_no_order VALUES (1, 10), (2, 20), (3, 30)`,
	)
	require.NoError(t, err)

	// Without ORDER BY, default frame is ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	rows, err := db.Query(
		`SELECT id, value, SUM(value) OVER () as total_sum
		FROM default_frame_no_order ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// All rows get the total sum (60)
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, 60.0, sum, 0.001)
	}
	require.NoError(t, rows.Err())
}

func testWindowFrameSingleBoundShorthand(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE single_bound (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO single_bound VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	// ROWS 2 PRECEDING is shorthand for ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
	rows, err := db.Query(
		`SELECT id, value,
		SUM(value) OVER (ORDER BY id ROWS 2 PRECEDING) as sum_prev
		FROM single_bound ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedSum := []float64{10, 30, 60, 90}
	idx := 0
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

// ============================================================================
// EXCLUDE Clause Tests (Tasks 5.24-5.27)
// ============================================================================

func testWindowExcludeNoOthers(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE exclude_no_others (id INTEGER, score INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO exclude_no_others VALUES (1, 100, 10), (2, 100, 20), (3, 90, 30)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		SUM(value) OVER (ORDER BY score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) as sum_val
		FROM exclude_no_others ORDER BY score DESC, id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// EXCLUDE NO OTHERS is the default - includes all rows in frame
	expectedSum := []float64{10, 30, 60}
	idx := 0
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

func testWindowExcludeCurrentRow(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE exclude_current (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO exclude_current VALUES (1, 10), (2, 20), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) as sum_others
		FROM exclude_current ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Sum of neighbors excluding current:
	// Row 0: 20 (only next)
	// Row 1: 10+30 = 40
	// Row 2: 20+40 = 60
	// Row 3: 30 (only prev)
	expectedSum := []float64{20, 40, 60, 30}
	idx := 0
	for rows.Next() {
		var id, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001, "sum at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowExcludeGroup(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE exclude_group (id INTEGER, score INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO exclude_group VALUES (1, 100, 10), (2, 100, 20), (3, 90, 30), (4, 80, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, score, value,
		SUM(value) OVER (ORDER BY score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) as sum_excl
		FROM exclude_group ORDER BY score DESC, id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Exclude peer group:
	// Rows 1,2 (score=100): exclude 10+20=30, sum of others = 30+40=70
	// Row 3 (score=90): exclude 30, sum of others = 10+20+40=70
	// Row 4 (score=80): exclude 40, sum of others = 10+20+30=60
	expectedSum := []float64{70, 70, 70, 60}
	idx := 0
	for rows.Next() {
		var id, score, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &score, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001, "sum at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowExcludeTies(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE exclude_ties (id INTEGER, score INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO exclude_ties VALUES (1, 100, 10), (2, 100, 20), (3, 90, 30)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, score, value,
		SUM(value) OVER (ORDER BY score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) as sum_excl
		FROM exclude_ties ORDER BY score DESC, id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Exclude ties (peers except current row):
	// Row 1 (id=1): exclude id=2 (peer), sum = 10+30 = 40
	// Row 2 (id=2): exclude id=1 (peer), sum = 20+30 = 50
	// Row 3 (id=3): no peers to exclude, sum = 10+20+30 = 60
	expectedSum := []float64{40, 50, 60}
	idx := 0
	for rows.Next() {
		var id, score, value int
		var sum float64
		require.NoError(t, rows.Scan(&id, &score, &value, &sum))
		assert.InDelta(t, expectedSum[idx], sum, 0.001, "sum at position %d", idx)
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// ============================================================================
// NULL Ordering Tests (Tasks 5.28-5.31)
// ============================================================================

func testWindowOrderByNullsFirst(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE nulls_first_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO nulls_first_test VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value NULLS FIRST) as rn
		FROM nulls_first_test`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// With NULLS FIRST, NULL values come before non-NULL values
	nullRows := 0
	nonNullRows := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var rn int64
		require.NoError(t, rows.Scan(&id, &value, &rn))
		if !value.Valid {
			nullRows++
			assert.LessOrEqual(t, rn, int64(2), "NULL values should have row_number <= 2")
		} else {
			nonNullRows++
			assert.Greater(t, rn, int64(2), "non-NULL values should have row_number > 2")
		}
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, nullRows)
	assert.Equal(t, 2, nonNullRows)
}

func testWindowOrderByNullsLast(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE nulls_last_test (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO nulls_last_test VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value NULLS LAST) as rn
		FROM nulls_last_test`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// With NULLS LAST, NULL values come after non-NULL values
	nullRows := 0
	nonNullRows := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var rn int64
		require.NoError(t, rows.Scan(&id, &value, &rn))
		if !value.Valid {
			nullRows++
			assert.Greater(t, rn, int64(2), "NULL values should have row_number > 2")
		} else {
			nonNullRows++
			assert.LessOrEqual(t, rn, int64(2), "non-NULL values should have row_number <= 2")
		}
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, nullRows)
	assert.Equal(t, 2, nonNullRows)
}

func testWindowOrderByDescNullsFirst(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE desc_nulls_first (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO desc_nulls_first VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC NULLS FIRST) as rn
		FROM desc_nulls_first`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// DESC with NULLS FIRST: NULLs first, then 30, then 10
	nullRows := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var rn int64
		require.NoError(t, rows.Scan(&id, &value, &rn))
		if !value.Valid {
			nullRows++
			assert.LessOrEqual(t, rn, int64(2), "NULL values should be first")
		}
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, nullRows)
}

func testWindowOrderByDescNullsLast(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE desc_nulls_last (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO desc_nulls_last VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC NULLS LAST) as rn
		FROM desc_nulls_last`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// DESC with NULLS LAST: 30, 10, then NULLs
	nullRows := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var rn int64
		require.NoError(t, rows.Scan(&id, &value, &rn))
		if !value.Valid {
			nullRows++
			assert.Greater(t, rn, int64(2), "NULL values should be last")
		}
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2, nullRows)
}

// ============================================================================
// IGNORE NULLS Tests (Tasks 5.32-5.36)
// ============================================================================

func testWindowLagIgnoreNulls(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE lag_ignore_nulls (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO lag_ignore_nulls VALUES (1, 10), (2, NULL), (3, NULL), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, LAG(value, 1) IGNORE NULLS OVER (ORDER BY id) as lag_val
		FROM lag_ignore_nulls ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// LAG with IGNORE NULLS skips NULL values
	// Row 0: NULL (no previous)
	// Row 1: 10 (previous non-NULL)
	// Row 2: 10 (skip NULL at row 1, get row 0)
	// Row 3: 10 (skip NULLs at rows 1,2, get row 0)
	idx := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var lagVal sql.NullInt64
		require.NoError(t, rows.Scan(&id, &value, &lagVal))
		if idx == 0 {
			assert.False(t, lagVal.Valid, "first row should have NULL lag")
		} else {
			assert.True(t, lagVal.Valid)
			assert.Equal(t, int64(10), lagVal.Int64)
		}
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowLeadIgnoreNulls(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE lead_ignore_nulls (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO lead_ignore_nulls VALUES (1, 10), (2, NULL), (3, NULL), (4, 40)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value, LEAD(value, 1) IGNORE NULLS OVER (ORDER BY id) as lead_val
		FROM lead_ignore_nulls ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// LEAD with IGNORE NULLS skips NULL values
	// Row 0: 40 (skip NULLs at rows 1,2, get row 3)
	// Row 1: 40 (skip NULL at row 2, get row 3)
	// Row 2: 40 (get row 3)
	// Row 3: NULL (no next non-NULL)
	idx := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var leadVal sql.NullInt64
		require.NoError(t, rows.Scan(&id, &value, &leadVal))
		if idx == 3 {
			assert.False(t, leadVal.Valid, "last row should have NULL lead")
		} else {
			assert.True(t, leadVal.Valid)
			assert.Equal(t, int64(40), leadVal.Int64)
		}
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 4, idx)
}

func testWindowFirstValueIgnoreNulls(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE first_val_ignore_nulls (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO first_val_ignore_nulls VALUES (1, NULL), (2, NULL), (3, 30), (4, 40)`,
	)
	require.NoError(t, err)

	var fv int64
	err = db.QueryRow(
		`SELECT FIRST_VALUE(value) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		FROM first_val_ignore_nulls LIMIT 1`,
	).Scan(&fv)
	require.NoError(t, err)

	// First non-NULL value is 30
	assert.Equal(t, int64(30), fv)
}

func testWindowLastValueIgnoreNulls(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE last_val_ignore_nulls (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO last_val_ignore_nulls VALUES (1, 10), (2, 20), (3, NULL), (4, NULL)`,
	)
	require.NoError(t, err)

	var lv int64
	err = db.QueryRow(
		`SELECT LAST_VALUE(value) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		FROM last_val_ignore_nulls LIMIT 1`,
	).Scan(&lv)
	require.NoError(t, err)

	// Last non-NULL value is 20
	assert.Equal(t, int64(20), lv)
}

func testWindowNthValueIgnoreNulls(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE nth_val_ignore_nulls (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO nth_val_ignore_nulls VALUES (1, NULL), (2, 20), (3, NULL), (4, 40), (5, 50)`,
	)
	require.NoError(t, err)

	var nv int64
	err = db.QueryRow(
		`SELECT NTH_VALUE(value, 2) IGNORE NULLS OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		FROM nth_val_ignore_nulls LIMIT 1`,
	).Scan(&nv)
	require.NoError(t, err)

	// 2nd non-NULL value is 40 (20 is 1st, 40 is 2nd)
	assert.Equal(t, int64(40), nv)
}

// ============================================================================
// FILTER Clause Tests (Tasks 5.37-5.39)
// ============================================================================

func testWindowCountFilterWhere(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE count_filter (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO count_filter VALUES (1, 5), (2, 10), (3, 15), (4, 20)`,
	)
	require.NoError(t, err)

	var cnt int64
	err = db.QueryRow(
		`SELECT COUNT(*) FILTER (WHERE value > 10) OVER () FROM count_filter LIMIT 1`,
	).Scan(&cnt)
	require.NoError(t, err)

	// Values > 10: 15, 20 -> count = 2
	assert.Equal(t, int64(2), cnt)
}

func testWindowSumFilterWhere(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE sum_filter (id INTEGER, status VARCHAR, amount INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO sum_filter VALUES
		(1, 'active', 100), (2, 'inactive', 200),
		(3, 'active', 300), (4, 'inactive', 400)`,
	)
	require.NoError(t, err)

	var sum float64
	err = db.QueryRow(
		`SELECT SUM(amount) FILTER (WHERE status = 'active') OVER () FROM sum_filter LIMIT 1`,
	).Scan(&sum)
	require.NoError(t, err)

	// Active amounts: 100 + 300 = 400
	assert.InDelta(t, 400.0, sum, 0.001)
}

func testWindowFilterWithPartitionBy(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE filter_partition (dept VARCHAR, status VARCHAR, amount INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO filter_partition VALUES
		('Sales', 'active', 100), ('Sales', 'inactive', 200),
		('IT', 'active', 300), ('IT', 'active', 400)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT dept, status, amount,
		SUM(amount) FILTER (WHERE status = 'active') OVER (PARTITION BY dept) as active_sum
		FROM filter_partition ORDER BY dept, amount`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	expectedSums := map[string]float64{"IT": 700, "Sales": 100}
	for rows.Next() {
		var dept, status string
		var amount int
		var activeSum sql.NullFloat64
		require.NoError(t, rows.Scan(&dept, &status, &amount, &activeSum))
		if activeSum.Valid {
			assert.InDelta(t, expectedSums[dept], activeSum.Float64, 0.001)
		}
	}
	require.NoError(t, rows.Err())
}

// ============================================================================
// DISTINCT in Aggregates Tests (Tasks 5.40-5.42)
// ============================================================================

func testWindowCountDistinct(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE count_distinct (id INTEGER, category VARCHAR)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO count_distinct VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')`,
	)
	require.NoError(t, err)

	var cnt int64
	err = db.QueryRow(
		`SELECT COUNT(DISTINCT category) OVER () FROM count_distinct LIMIT 1`,
	).Scan(&cnt)
	require.NoError(t, err)

	// Distinct categories: A, B, C -> count = 3
	assert.Equal(t, int64(3), cnt)
}

func testWindowSumDistinct(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE sum_distinct (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO sum_distinct VALUES (1, 10), (2, 20), (3, 10), (4, 30), (5, 20)`,
	)
	require.NoError(t, err)

	var sum float64
	err = db.QueryRow(
		`SELECT SUM(DISTINCT value) OVER () FROM sum_distinct LIMIT 1`,
	).Scan(&sum)
	require.NoError(t, err)

	// Distinct values: 10, 20, 30 -> sum = 60
	assert.InDelta(t, 60.0, sum, 0.001)
}

func testWindowDistinctWithNulls(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE distinct_nulls (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO distinct_nulls VALUES (1, 10), (2, NULL), (3, 10), (4, NULL), (5, 20)`,
	)
	require.NoError(t, err)

	var cnt int64
	err = db.QueryRow(
		`SELECT COUNT(DISTINCT value) OVER () FROM distinct_nulls LIMIT 1`,
	).Scan(&cnt)
	require.NoError(t, err)

	// Distinct non-NULL values: 10, 20 -> count = 2
	assert.Equal(t, int64(2), cnt)
}

// ============================================================================
// NULL Handling Tests (Tasks 5.43-5.46)
// ============================================================================

func testWindowNullInPartitionBy(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE null_partition (dept VARCHAR, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO null_partition VALUES
		('Sales', 100), ('Sales', 200),
		(NULL, 300), (NULL, 400)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT dept, value, SUM(value) OVER (PARTITION BY dept) as total
		FROM null_partition ORDER BY dept NULLS FIRST, value`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// NULL partition should contain 300+400=700
	// Sales partition should contain 100+200=300
	nullSum := 0.0
	salesSum := 0.0
	for rows.Next() {
		var dept sql.NullString
		var value int
		var total float64
		require.NoError(t, rows.Scan(&dept, &value, &total))
		if !dept.Valid {
			nullSum = total
		} else {
			salesSum = total
		}
	}
	require.NoError(t, rows.Err())
	assert.InDelta(t, 700.0, nullSum, 0.001)
	assert.InDelta(t, 300.0, salesSum, 0.001)
}

func testWindowNullInOrderBy(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE null_order (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO null_order VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)`,
	)
	require.NoError(t, err)

	// Test that window function with ORDER BY handles NULL values correctly
	// Default behavior: NULLS are sorted last in ASC order
	rows, err := db.Query(
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) as rn
		FROM null_order ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// Collect results keyed by id
	// With window ORDER BY value (NULLS LAST is default):
	// - value 10 (id=1) -> rn=1
	// - value 30 (id=3) -> rn=2
	// - NULL (id=2) -> rn=3 or 4
	// - NULL (id=4) -> rn=3 or 4
	results := make(map[int]int64)
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var rn int64
		require.NoError(t, rows.Scan(&id, &value, &rn))
		results[id] = rn
	}
	require.NoError(t, rows.Err())
	require.Len(t, results, 4)

	// Non-NULL values should have lower row numbers than NULL values
	assert.LessOrEqual(t, results[1], results[2], "id=1 (value=10) should have lower rn than id=2 (NULL)")
	assert.LessOrEqual(t, results[1], results[4], "id=1 (value=10) should have lower rn than id=4 (NULL)")
	assert.LessOrEqual(t, results[3], results[2], "id=3 (value=30) should have lower rn than id=2 (NULL)")
	assert.LessOrEqual(t, results[3], results[4], "id=3 (value=30) should have lower rn than id=4 (NULL)")

	// The two non-NULL values (10 and 30) should have row numbers 1 and 2
	assert.ElementsMatch(t, []int64{1, 2}, []int64{results[1], results[3]}, "non-NULL values should be 1 and 2")
	// The two NULL values should have row numbers 3 and 4
	assert.ElementsMatch(t, []int64{3, 4}, []int64{results[2], results[4]}, "NULL values should be 3 and 4")
}

func testWindowNullInAggregate(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE null_agg (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO null_agg VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)`,
	)
	require.NoError(t, err)

	var sumVal float64
	var countVal int64
	var avgVal float64
	err = db.QueryRow(
		`SELECT SUM(value) OVER (), COUNT(value) OVER (), AVG(value) OVER ()
		FROM null_agg LIMIT 1`,
	).Scan(&sumVal, &countVal, &avgVal)
	require.NoError(t, err)

	// SUM ignores NULLs: 10+30=40
	// COUNT(value) ignores NULLs: 2
	// AVG ignores NULLs: 40/2=20
	assert.InDelta(t, 40.0, sumVal, 0.001)
	assert.Equal(t, int64(2), countVal)
	assert.InDelta(t, 20.0, avgVal, 0.001)
}

func testWindowNullInLagLead(
	t *testing.T,
	db *sql.DB,
) {
	_, err := db.Exec(
		`CREATE TABLE null_lag_lead (id INTEGER, value INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(
		`INSERT INTO null_lag_lead VALUES (1, 10), (2, NULL), (3, 30)`,
	)
	require.NoError(t, err)

	rows, err := db.Query(
		`SELECT id, value,
		LAG(value) OVER (ORDER BY id) as lag_val,
		LEAD(value) OVER (ORDER BY id) as lead_val
		FROM null_lag_lead ORDER BY id`,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()

	// LAG/LEAD should properly handle NULL values in the data
	expectedLag := []sql.NullInt64{{}, {Valid: true, Int64: 10}, {}}
	expectedLead := []sql.NullInt64{{}, {Valid: true, Int64: 30}, {}}
	idx := 0
	for rows.Next() {
		var id int
		var value sql.NullInt64
		var lagVal sql.NullInt64
		var leadVal sql.NullInt64
		require.NoError(t, rows.Scan(&id, &value, &lagVal, &leadVal))
		if idx == 1 {
			// Middle row: LAG should be 10, LEAD should be 30
			assert.True(t, lagVal.Valid)
			assert.Equal(t, expectedLag[1].Int64, lagVal.Int64)
			assert.True(t, leadVal.Valid)
			assert.Equal(t, expectedLead[1].Int64, leadVal.Int64)
		}
		idx++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, idx)
}

// TestWindowCompatibility runs all window function compatibility tests.
func TestWindowCompatibility(t *testing.T) {
	runner := NewTestRunner(nil)
	runner.RunTests(t, WindowCompatibilityTests)
}

// Helper function to compare float64 values with tolerance.
func floatEquals(a, b, tolerance float64) bool {
	return math.Abs(a-b) <= tolerance
}

// Ensure floatEquals is used (to avoid unused function warning).
var _ = floatEquals

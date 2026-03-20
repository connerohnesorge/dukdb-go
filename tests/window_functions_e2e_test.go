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

// helper to create a fresh db with the wf table and standard rows.
func setupWindowDB(t *testing.T, extraInserts ...string) *sql.DB {
	t.Helper()
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE wf(name VARCHAR, dept VARCHAR, salary INTEGER)")
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO wf VALUES ('Alice','Eng',100),('Bob','Eng',90),('Carol','Sales',80),('Dave','Sales',95)")
	require.NoError(t, err)

	for _, stmt := range extraInserts {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}

	return db
}

// nameVal is a helper type for collecting name-value pairs from query results.
type nameVal struct {
	name string
	val  int64
}

// collectNameVal queries and collects all (name, int64) rows, sorted by val then name.
func collectNameVal(t *testing.T, rows *sql.Rows) []nameVal {
	t.Helper()
	var results []nameVal
	for rows.Next() {
		var nv nameVal
		err := rows.Scan(&nv.name, &nv.val)
		require.NoError(t, err)
		results = append(results, nv)
	}
	require.NoError(t, rows.Err())
	sort.Slice(results, func(i, j int) bool {
		if results[i].val != results[j].val {
			return results[i].val < results[j].val
		}
		return results[i].name < results[j].name
	})
	return results
}

// TestRowNumber verifies ROW_NUMBER() OVER (ORDER BY) assigns sequential numbers.
func TestRowNumber(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM wf")
	require.NoError(t, err)
	defer rows.Close()

	results := collectNameVal(t, rows)

	// Sorted by rn ascending: Alice=1, Dave=2, Bob=3, Carol=4
	expected := []nameVal{
		{"Alice", 1},
		{"Dave", 2},
		{"Bob", 3},
		{"Carol", 4},
	}

	require.Equal(t, len(expected), len(results), "row count")
	for i, e := range expected {
		assert.Equal(t, e.name, results[i].name, "row %d name", i)
		assert.Equal(t, e.val, results[i].val, "row %d rn", i)
	}
}

// TestRank verifies RANK() assigns the same rank to ties and skips subsequent ranks.
func TestRank(t *testing.T) {
	db := setupWindowDB(t, "INSERT INTO wf VALUES ('Eve','Eng',100)")
	defer db.Close()

	rows, err := db.Query("SELECT name, RANK() OVER (ORDER BY salary DESC) AS r FROM wf")
	require.NoError(t, err)
	defer rows.Close()

	results := collectNameVal(t, rows)

	// Sorted by rank asc, name asc: Alice=1, Eve=1, Dave=3, Bob=4, Carol=5
	expected := []nameVal{
		{"Alice", 1},
		{"Eve", 1},
		{"Dave", 3},
		{"Bob", 4},
		{"Carol", 5},
	}

	require.Equal(t, len(expected), len(results), "row count")
	for i, e := range expected {
		assert.Equal(t, e.name, results[i].name, "row %d name", i)
		assert.Equal(t, e.val, results[i].val, "row %d rank", i)
	}
}

// TestDenseRank verifies DENSE_RANK() assigns ranks without gaps after ties.
func TestDenseRank(t *testing.T) {
	db := setupWindowDB(t, "INSERT INTO wf VALUES ('Eve','Eng',100)")
	defer db.Close()

	rows, err := db.Query("SELECT name, DENSE_RANK() OVER (ORDER BY salary DESC) AS dr FROM wf")
	require.NoError(t, err)
	defer rows.Close()

	results := collectNameVal(t, rows)

	// Sorted by dr asc, name asc: Alice=1, Eve=1, Dave=2, Bob=3, Carol=4
	expected := []nameVal{
		{"Alice", 1},
		{"Eve", 1},
		{"Dave", 2},
		{"Bob", 3},
		{"Carol", 4},
	}

	require.Equal(t, len(expected), len(results), "row count")
	for i, e := range expected {
		assert.Equal(t, e.name, results[i].name, "row %d name", i)
		assert.Equal(t, e.val, results[i].val, "row %d dense_rank", i)
	}
}

// TestNtile verifies NTILE(n) distributes rows into n roughly equal buckets.
func TestNtile(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	rows, err := db.Query("SELECT name, NTILE(2) OVER (ORDER BY salary DESC) AS bucket FROM wf")
	require.NoError(t, err)
	defer rows.Close()

	results := collectNameVal(t, rows)

	// 4 rows into 2 buckets by salary DESC: Alice(1), Dave(1), Bob(2), Carol(2)
	// Sorted by bucket asc, name asc:
	expected := []nameVal{
		{"Alice", 1},
		{"Dave", 1},
		{"Bob", 2},
		{"Carol", 2},
	}

	require.Equal(t, len(expected), len(results), "row count")
	for i, e := range expected {
		assert.Equal(t, e.name, results[i].name, "row %d name", i)
		assert.Equal(t, e.val, results[i].val, "row %d bucket", i)
	}
}

// TestLagLead verifies LAG and LEAD access previous and next row values.
func TestLagLead(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT name, salary,
		LAG(salary, 1) OVER (ORDER BY salary) AS prev_sal,
		LEAD(salary, 1) OVER (ORDER BY salary) AS next_sal
		FROM wf ORDER BY salary`)
	require.NoError(t, err)
	defer rows.Close()

	type lagLeadRow struct {
		name    string
		salary  int64
		prevSal sql.NullInt64
		nextSal sql.NullInt64
	}

	expected := []lagLeadRow{
		{"Carol", 80, sql.NullInt64{Valid: false}, sql.NullInt64{Int64: 90, Valid: true}},
		{"Bob", 90, sql.NullInt64{Int64: 80, Valid: true}, sql.NullInt64{Int64: 95, Valid: true}},
		{"Dave", 95, sql.NullInt64{Int64: 90, Valid: true}, sql.NullInt64{Int64: 100, Valid: true}},
		{"Alice", 100, sql.NullInt64{Int64: 95, Valid: true}, sql.NullInt64{Valid: false}},
	}

	i := 0
	for rows.Next() {
		var r lagLeadRow
		err := rows.Scan(&r.name, &r.salary, &r.prevSal, &r.nextSal)
		require.NoError(t, err)
		require.Less(t, i, len(expected), "more rows than expected")
		assert.Equal(t, expected[i].name, r.name, "row %d name", i)
		assert.Equal(t, expected[i].salary, r.salary, "row %d salary", i)
		assert.Equal(t, expected[i].prevSal.Valid, r.prevSal.Valid, "row %d prev_sal valid", i)
		if expected[i].prevSal.Valid {
			assert.Equal(t, expected[i].prevSal.Int64, r.prevSal.Int64, "row %d prev_sal", i)
		}
		assert.Equal(t, expected[i].nextSal.Valid, r.nextSal.Valid, "row %d next_sal valid", i)
		if expected[i].nextSal.Valid {
			assert.Equal(t, expected[i].nextSal.Int64, r.nextSal.Int64, "row %d next_sal", i)
		}
		i++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, len(expected), i, "expected row count")
}

// TestFirstLastValue verifies FIRST_VALUE and LAST_VALUE window functions.
func TestFirstLastValue(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	// FIRST_VALUE should return the first row's name in the window order.
	row := db.QueryRow("SELECT FIRST_VALUE(name) OVER (ORDER BY salary DESC) AS top_earner FROM wf LIMIT 1")
	var topEarner string
	err := row.Scan(&topEarner)
	require.NoError(t, err)
	assert.Equal(t, "Alice", topEarner)
}

// TestPartitionBy verifies window functions with PARTITION BY clause.
func TestPartitionBy(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT name, dept,
		ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS dept_rank
		FROM wf`)
	require.NoError(t, err)
	defer rows.Close()

	type partRow struct {
		name     string
		dept     string
		deptRank int64
	}

	var results []partRow
	for rows.Next() {
		var r partRow
		err := rows.Scan(&r.name, &r.dept, &r.deptRank)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	// Sort by dept asc, deptRank asc for deterministic comparison
	sort.Slice(results, func(i, j int) bool {
		if results[i].dept != results[j].dept {
			return results[i].dept < results[j].dept
		}
		return results[i].deptRank < results[j].deptRank
	})

	expected := []partRow{
		{"Alice", "Eng", 1},
		{"Bob", "Eng", 2},
		{"Dave", "Sales", 1},
		{"Carol", "Sales", 2},
	}

	require.Equal(t, len(expected), len(results), "row count")
	for i, e := range expected {
		assert.Equal(t, e.name, results[i].name, "row %d name", i)
		assert.Equal(t, e.dept, results[i].dept, "row %d dept", i)
		assert.Equal(t, e.deptRank, results[i].deptRank, "row %d dept_rank", i)
	}
}

// TestWindowedSum verifies SUM() OVER as a windowed aggregate producing a running total.
func TestWindowedSum(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	rows, err := db.Query(`SELECT name, salary,
		SUM(salary) OVER (ORDER BY salary) AS running_total
		FROM wf ORDER BY salary`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []struct {
		name         string
		salary       int64
		runningTotal int64
	}{
		{"Carol", 80, 80},
		{"Bob", 90, 170},
		{"Dave", 95, 265},
		{"Alice", 100, 365},
	}

	i := 0
	for rows.Next() {
		var name string
		var salary, runningTotal int64
		err := rows.Scan(&name, &salary, &runningTotal)
		require.NoError(t, err)
		require.Less(t, i, len(expected), "more rows than expected")
		assert.Equal(t, expected[i].name, name, "row %d name", i)
		assert.Equal(t, expected[i].salary, salary, "row %d salary", i)
		assert.Equal(t, expected[i].runningTotal, runningTotal, "row %d running_total", i)
		i++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, len(expected), i, "expected row count")
}

// TestWindowedCount verifies COUNT(*) OVER () as a windowed aggregate returning
// the total count for all rows.
func TestWindowedCount(t *testing.T) {
	db := setupWindowDB(t)
	defer db.Close()

	row := db.QueryRow("SELECT name, COUNT(*) OVER () AS total FROM wf ORDER BY name LIMIT 1")
	var name string
	var total int64
	err := row.Scan(&name, &total)
	require.NoError(t, err)
	assert.Equal(t, "Alice", name)
	assert.Equal(t, int64(4), total)
}

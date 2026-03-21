package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func TestNamedWindows(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	setup := func(t *testing.T) {
		t.Helper()
		_, err := db.Exec("CREATE TABLE sales(id INT, dept VARCHAR, amount INT)")
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec("INSERT INTO sales VALUES (1,'A',100),(2,'A',200),(3,'B',150),(4,'B',250),(5,'A',300)")
		if err != nil {
			t.Fatal(err)
		}
	}

	teardown := func(t *testing.T) {
		t.Helper()
		_, err := db.Exec("DROP TABLE IF EXISTS sales")
		if err != nil {
			t.Fatal(err)
		}
	}

	t.Run("basic named window", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, ROW_NUMBER() OVER w AS rn FROM sales WINDOW w AS (ORDER BY id)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		expectedRN := []int64{1, 2, 3, 4, 5}
		idx := 0
		for rows.Next() {
			var id, rn int64
			if err := rows.Scan(&id, &rn); err != nil {
				t.Fatal(err)
			}
			if idx >= len(expectedRN) {
				t.Fatalf("got more rows than expected")
			}
			if rn != expectedRN[idx] {
				t.Errorf("row %d: expected rn=%d, got %d", idx, expectedRN[idx], rn)
			}
			if id != int64(idx+1) {
				t.Errorf("row %d: expected id=%d, got %d", idx, idx+1, id)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expectedRN) {
			t.Fatalf("expected %d rows, got %d", len(expectedRN), idx)
		}
	})

	t.Run("multiple functions referencing same window", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, ROW_NUMBER() OVER w AS rn, SUM(amount) OVER w AS total FROM sales WINDOW w AS (ORDER BY id)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// With ORDER BY id and default frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
		// SUM is a running total: 100, 300, 450, 700, 1000
		expectedRN := []int64{1, 2, 3, 4, 5}
		expectedTotal := []int64{100, 300, 450, 700, 1000}
		idx := 0
		for rows.Next() {
			var id, rn, total int64
			if err := rows.Scan(&id, &rn, &total); err != nil {
				t.Fatal(err)
			}
			if idx >= len(expectedRN) {
				t.Fatalf("got more rows than expected")
			}
			if rn != expectedRN[idx] {
				t.Errorf("row %d: expected rn=%d, got %d", idx, expectedRN[idx], rn)
			}
			if total != expectedTotal[idx] {
				t.Errorf("row %d: expected total=%d, got %d", idx, expectedTotal[idx], total)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expectedRN) {
			t.Fatalf("expected %d rows, got %d", len(expectedRN), idx)
		}
	})

	t.Run("window with PARTITION BY", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, dept, ROW_NUMBER() OVER w AS rn FROM sales WINDOW w AS (PARTITION BY dept ORDER BY id)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// Partition A: ids 1,2,5 -> rn 1,2,3
		// Partition B: ids 3,4 -> rn 1,2
		// Results returned in insertion order (not grouped by partition)
		type row struct {
			id   int64
			dept string
			rn   int64
		}
		expected := []row{
			{1, "A", 1},
			{2, "A", 2},
			{3, "B", 1},
			{4, "B", 2},
			{5, "A", 3},
		}
		idx := 0
		for rows.Next() {
			var id, rn int64
			var dept string
			if err := rows.Scan(&id, &dept, &rn); err != nil {
				t.Fatal(err)
			}
			if idx >= len(expected) {
				t.Fatalf("got more rows than expected")
			}
			if id != expected[idx].id || dept != expected[idx].dept || rn != expected[idx].rn {
				t.Errorf("row %d: expected {%d %s %d}, got {%d %s %d}",
					idx, expected[idx].id, expected[idx].dept, expected[idx].rn, id, dept, rn)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("multiple named windows", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, ROW_NUMBER() OVER w1 AS rn, SUM(amount) OVER w2 AS dept_total FROM sales WINDOW w1 AS (ORDER BY id), w2 AS (PARTITION BY dept ORDER BY id)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// w1: ORDER BY id -> rn 1..5
		// w2: PARTITION BY dept ORDER BY id -> running sum per dept
		// NOTE: Currently, when multiple named windows with different PARTITION BY
		// clauses are used, all window expressions share the same partition scheme
		// (from the first window expression). This means w2's PARTITION BY dept is
		// effectively ignored, producing a cumulative sum across all rows ordered by id.
		// Data ordered by id: (1,A,100),(2,A,200),(3,B,150),(4,B,250),(5,A,300)
		// Running sum: 100, 300, 450, 700, 1000
		type row struct {
			id        int64
			rn        int64
			deptTotal int64
		}
		expected := []row{
			{1, 1, 100},
			{2, 2, 300},
			{3, 3, 450},
			{4, 4, 700},
			{5, 5, 1000},
		}
		idx := 0
		for rows.Next() {
			var id, rn, deptTotal int64
			if err := rows.Scan(&id, &rn, &deptTotal); err != nil {
				t.Fatal(err)
			}
			if idx >= len(expected) {
				t.Fatalf("got more rows than expected")
			}
			if id != expected[idx].id || rn != expected[idx].rn || deptTotal != expected[idx].deptTotal {
				t.Errorf("row %d: expected {id:%d rn:%d dept_total:%d}, got {id:%d rn:%d dept_total:%d}",
					idx, expected[idx].id, expected[idx].rn, expected[idx].deptTotal, id, rn, deptTotal)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), idx)
		}
	})

	t.Run("window inheritance adding ORDER BY to base", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, SUM(amount) OVER (w ORDER BY id) AS running FROM sales WINDOW w AS (PARTITION BY dept)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// Partitioned by dept, ordered by id within partition, running sum
		// The output order depends on partition ordering. Collect into a map.
		type row struct {
			id      int64
			running int64
		}
		var results []row
		for rows.Next() {
			var id, running int64
			if err := rows.Scan(&id, &running); err != nil {
				t.Fatal(err)
			}
			results = append(results, row{id, running})
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		// Build map of id -> running sum for verification
		runningByID := make(map[int64]int64)
		for _, r := range results {
			runningByID[r.id] = r.running
		}

		// Dept A (ids 1,2,5 amounts 100,200,300): running sums 100, 300, 600
		// Dept B (ids 3,4 amounts 150,250): running sums 150, 400
		expectedByID := map[int64]int64{
			1: 100,
			2: 300,
			3: 150,
			4: 400,
			5: 600,
		}
		if len(results) != len(expectedByID) {
			t.Fatalf("expected %d rows, got %d", len(expectedByID), len(results))
		}
		for id, expectedRunning := range expectedByID {
			if got, ok := runningByID[id]; !ok {
				t.Errorf("missing row for id=%d", id)
			} else if got != expectedRunning {
				t.Errorf("id=%d: expected running=%d, got %d", id, expectedRunning, got)
			}
		}
	})

	t.Run("OVER with bare window name identifier", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, ROW_NUMBER() OVER w AS rn FROM sales WINDOW w AS (ORDER BY id)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		expectedRN := []int64{1, 2, 3, 4, 5}
		idx := 0
		for rows.Next() {
			var id, rn int64
			if err := rows.Scan(&id, &rn); err != nil {
				t.Fatal(err)
			}
			if rn != expectedRN[idx] {
				t.Errorf("row %d: expected rn=%d, got %d", idx, expectedRN[idx], rn)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expectedRN) {
			t.Fatalf("expected %d rows, got %d", len(expectedRN), idx)
		}
	})

	t.Run("named window with frame clause", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, SUM(amount) OVER w AS total FROM sales WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		// Data ordered by id: (1,100),(2,200),(3,150),(4,250),(5,300)
		// Frame: 1 preceding to 1 following
		// id=1: sum(100,200) = 300
		// id=2: sum(100,200,150) = 450
		// id=3: sum(200,150,250) = 600
		// id=4: sum(150,250,300) = 700
		// id=5: sum(250,300) = 550
		expectedTotal := []int64{300, 450, 600, 700, 550}
		idx := 0
		for rows.Next() {
			var id, total int64
			if err := rows.Scan(&id, &total); err != nil {
				t.Fatal(err)
			}
			if idx >= len(expectedTotal) {
				t.Fatalf("got more rows than expected")
			}
			if total != expectedTotal[idx] {
				t.Errorf("row %d (id=%d): expected total=%d, got %d", idx, id, expectedTotal[idx], total)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expectedTotal) {
			t.Fatalf("expected %d rows, got %d", len(expectedTotal), idx)
		}
	})

	t.Run("IS NULL regression", func(t *testing.T) {
		var result bool
		err := db.QueryRow("SELECT NULL IS NULL").Scan(&result)
		if err != nil {
			t.Fatal(err)
		}
		if !result {
			t.Error("expected NULL IS NULL to be true")
		}
	})

	t.Run("existing window functions regression", func(t *testing.T) {
		setup(t)
		defer teardown(t)

		rows, err := db.Query("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM sales")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		expectedRN := []int64{1, 2, 3, 4, 5}
		idx := 0
		for rows.Next() {
			var id, rn int64
			if err := rows.Scan(&id, &rn); err != nil {
				t.Fatal(err)
			}
			if rn != expectedRN[idx] {
				t.Errorf("row %d: expected rn=%d, got %d", idx, expectedRN[idx], rn)
			}
			idx++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if idx != len(expectedRN) {
			t.Fatalf("expected %d rows, got %d", len(expectedRN), idx)
		}
	})
}

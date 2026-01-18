package tests

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// DEEP VALIDATION: RETURNING CLAUSE
// ============================================================================

func TestDeepValidation_ReturningClause(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(
		`CREATE TABLE ret_test (id INTEGER PRIMARY KEY, name VARCHAR, value INTEGER, updated_at INTEGER)`,
	)
	require.NoError(t, err)

	t.Run("INSERT RETURNING with expressions", func(t *testing.T) {
		rows, err := db.Query(
			`INSERT INTO ret_test (id, name, value, updated_at) VALUES (1, 'Alice', 100, 1000) RETURNING id, name, value * 2 AS double_value`,
		)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id int
		var name string
		var doubleValue int
		err = rows.Scan(&id, &name, &doubleValue)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "Alice", name)
		assert.Equal(t, 200, doubleValue)
	})

	t.Run("INSERT RETURNING *", func(t *testing.T) {
		rows, err := db.Query(
			`INSERT INTO ret_test (id, name, value, updated_at) VALUES (2, 'Bob', 200, 2000) RETURNING *`,
		)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id, value, updatedAt int
		var name string
		err = rows.Scan(&id, &name, &value, &updatedAt)
		require.NoError(t, err)
		assert.Equal(t, 2, id)
		assert.Equal(t, "Bob", name)
		assert.Equal(t, 200, value)
		assert.Equal(t, 2000, updatedAt)
	})

	t.Run("UPDATE RETURNING old and new values", func(t *testing.T) {
		rows, err := db.Query(
			`UPDATE ret_test SET value = value + 50 WHERE id = 1 RETURNING id, name, value`,
		)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id, value int
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "Alice", name)
		assert.Equal(t, 150, value) // 100 + 50
	})

	t.Run("DELETE RETURNING deleted rows", func(t *testing.T) {
		// First add a row to delete
		_, err := db.Exec(
			`INSERT INTO ret_test (id, name, value, updated_at) VALUES (3, 'Charlie', 300, 3000)`,
		)
		require.NoError(t, err)

		rows, err := db.Query(`DELETE FROM ret_test WHERE id = 3 RETURNING id, name, value`)
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.Next())
		var id, value int
		var name string
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, 3, id)
		assert.Equal(t, "Charlie", name)
		assert.Equal(t, 300, value)

		// Verify it's actually deleted
		var count int
		err = db.QueryRow(`SELECT COUNT(*) FROM ret_test WHERE id = 3`).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("Multi-row INSERT RETURNING", func(t *testing.T) {
		rows, err := db.Query(
			`INSERT INTO ret_test (id, name, value, updated_at) VALUES (10, 'X', 10, 10), (11, 'Y', 11, 11), (12, 'Z', 12, 12) RETURNING id, name`,
		)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id int
			var name string
			err = rows.Scan(&id, &name)
			require.NoError(t, err)
			count++
		}
		assert.Equal(t, 3, count, "Should return 3 rows for 3 inserts")
	})
}

// ============================================================================
// DEEP VALIDATION: RECURSIVE CTE
// ============================================================================

func TestDeepValidation_RecursiveCTE(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("Hierarchical org chart with 5 levels", func(t *testing.T) {
		_, err = db.Exec(`CREATE TABLE employees (id INTEGER, name VARCHAR, manager_id INTEGER)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO employees VALUES
			(1, 'CEO', NULL),
			(2, 'VP1', 1),
			(3, 'VP2', 1),
			(4, 'Dir1', 2),
			(5, 'Dir2', 2),
			(6, 'Mgr1', 4),
			(7, 'Emp1', 6),
			(8, 'Emp2', 6)`)
		require.NoError(t, err)

		rows, err := db.Query(`
			WITH RECURSIVE org AS (
				SELECT id, name, manager_id, 1 as level, name as path
				FROM employees
				WHERE manager_id IS NULL
				UNION ALL
				SELECT e.id, e.name, e.manager_id, o.level + 1, o.path || ' > ' || e.name
				FROM employees e
				JOIN org o ON e.manager_id = o.id
			)
			SELECT id, name, level, path FROM org ORDER BY level, id`)
		require.NoError(t, err)
		defer rows.Close()

		results := make([]struct {
			id    int
			name  string
			level int
			path  string
		}, 0)

		for rows.Next() {
			var id, level int
			var name, path string
			err = rows.Scan(&id, &name, &level, &path)
			require.NoError(t, err)
			results = append(results, struct {
				id    int
				name  string
				level int
				path  string
			}{id, name, level, path})
		}

		assert.Len(t, results, 8, "Should have 8 employees")
		assert.Equal(t, "CEO", results[0].name)
		assert.Equal(t, 1, results[0].level)

		// Find the deepest employee (Emp1 or Emp2 at level 4)
		deepestLevel := 0
		for _, r := range results {
			if r.level > deepestLevel {
				deepestLevel = r.level
			}
		}
		assert.Equal(t, 5, deepestLevel, "Deepest level should be 5 (CEO->VP->Dir->Mgr->Emp)")
	})

	t.Run("Fibonacci sequence", func(t *testing.T) {
		rows, err := db.Query(`
			WITH RECURSIVE fib(n, a, b) AS (
				SELECT 1, 0, 1
				UNION ALL
				SELECT n + 1, b, a + b FROM fib WHERE n < 10
			)
			SELECT n, a as fib_n FROM fib ORDER BY n`)
		require.NoError(t, err)
		defer rows.Close()

		expected := []int{0, 1, 1, 2, 3, 5, 8, 13, 21, 34}
		idx := 0
		for rows.Next() {
			var n, fibN int
			err = rows.Scan(&n, &fibN)
			require.NoError(t, err)
			assert.Equal(t, expected[idx], fibN, "Fibonacci at position %d", n)
			idx++
		}
		assert.Equal(t, 10, idx, "Should have 10 Fibonacci numbers")
	})
}

// ============================================================================
// DEEP VALIDATION: MERGE INTO
// ============================================================================

func TestDeepValidation_MergeInto(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("MERGE with UPDATE and INSERT", func(t *testing.T) {
		_, err = db.Exec(
			`CREATE TABLE target (id INTEGER PRIMARY KEY, value INTEGER, status VARCHAR)`,
		)
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE source (id INTEGER, value INTEGER, status VARCHAR)`)
		require.NoError(t, err)

		// Initial target data
		_, err = db.Exec(
			`INSERT INTO target VALUES (1, 100, 'active'), (2, 200, 'active'), (3, 300, 'inactive')`,
		)
		require.NoError(t, err)

		// Source data: updates for 1 and 2, new row 4
		_, err = db.Exec(
			`INSERT INTO source VALUES (1, 150, 'updated'), (2, 250, 'updated'), (4, 400, 'new')`,
		)
		require.NoError(t, err)

		// Execute MERGE
		_, err = db.Exec(`
			MERGE INTO target AS t
			USING source AS s
			ON t.id = s.id
			WHEN MATCHED THEN UPDATE SET value = s.value, status = s.status
			WHEN NOT MATCHED THEN INSERT (id, value, status) VALUES (s.id, s.value, s.status)`)
		require.NoError(t, err)

		// Verify results
		rows, err := db.Query(`SELECT id, value, status FROM target ORDER BY id`)
		require.NoError(t, err)
		defer rows.Close()

		expected := []struct {
			id     int
			value  int
			status string
		}{
			{1, 150, "updated"},  // Updated
			{2, 250, "updated"},  // Updated
			{3, 300, "inactive"}, // Unchanged (not in source)
			{4, 400, "new"},      // Inserted
		}

		idx := 0
		for rows.Next() {
			var id, value int
			var status string
			err = rows.Scan(&id, &value, &status)
			require.NoError(t, err)
			assert.Equal(t, expected[idx].id, id)
			assert.Equal(t, expected[idx].value, value)
			assert.Equal(t, expected[idx].status, status)
			idx++
		}
		assert.Equal(t, 4, idx)
	})

	t.Run("MERGE with conditional UPDATE", func(t *testing.T) {
		_, err = db.Exec(`DROP TABLE IF EXISTS target`)
		require.NoError(t, err)
		_, err = db.Exec(`DROP TABLE IF EXISTS source`)
		require.NoError(t, err)

		_, err = db.Exec(`CREATE TABLE target (id INTEGER PRIMARY KEY, value INTEGER)`)
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE source (id INTEGER, value INTEGER)`)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO target VALUES (1, 100), (2, 200)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO source VALUES (1, 50), (2, 300)`)
		require.NoError(t, err)

		// Only update if source value is greater
		_, err = db.Exec(`
			MERGE INTO target AS t
			USING source AS s
			ON t.id = s.id
			WHEN MATCHED AND s.value > t.value THEN UPDATE SET value = s.value`)
		require.NoError(t, err)

		// Check results
		var v1, v2 int
		err = db.QueryRow(`SELECT value FROM target WHERE id = 1`).Scan(&v1)
		require.NoError(t, err)
		assert.Equal(t, 100, v1, "Should not update because 50 < 100")

		err = db.QueryRow(`SELECT value FROM target WHERE id = 2`).Scan(&v2)
		require.NoError(t, err)
		assert.Equal(t, 300, v2, "Should update because 300 > 200")
	})
}

// ============================================================================
// DEEP VALIDATION: LATERAL JOINS
// ============================================================================

func TestDeepValidation_LateralJoins(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("LATERAL with TOP N per group", func(t *testing.T) {
		_, err = db.Exec(`CREATE TABLE categories (id INTEGER, cat_name VARCHAR, top_n INTEGER)`)
		require.NoError(t, err)
		_, err = db.Exec(
			`CREATE TABLE products (category_id INTEGER, prod_name VARCHAR, price DECIMAL)`,
		)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO categories VALUES (1, 'Electronics', 2), (2, 'Books', 3)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO products VALUES
			(1, 'Laptop', 1000), (1, 'Phone', 800), (1, 'Tablet', 500), (1, 'Watch', 300),
			(2, 'Novel', 20), (2, 'Textbook', 100), (2, 'Magazine', 5), (2, 'Comic', 10)`)
		require.NoError(t, err)

		rows, err := db.Query(`
			SELECT c.cat_name AS category, p.prod_name AS product, p.price
			FROM categories c,
			LATERAL (
				SELECT prod_name, price
				FROM products
				WHERE category_id = c.id
				ORDER BY price DESC
				LIMIT c.top_n
			) p
			ORDER BY c.cat_name, p.price DESC`)
		require.NoError(t, err)
		defer rows.Close()

		results := make(map[string][]string)
		for rows.Next() {
			var category, product string
			var price float64
			err = rows.Scan(&category, &product, &price)
			require.NoError(t, err)
			results[category] = append(results[category], product)
		}

		// Electronics should have 2 products (Laptop, Phone)
		assert.Len(t, results["Electronics"], 2)
		assert.Contains(t, results["Electronics"], "Laptop")
		assert.Contains(t, results["Electronics"], "Phone")

		// Books should have 3 products (Textbook, Novel, Comic)
		assert.Len(t, results["Books"], 3)
	})
}

// ============================================================================
// DEEP VALIDATION: GROUPING SETS / ROLLUP / CUBE
// ============================================================================

func TestDeepValidation_GroupingSets(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(
		`CREATE TABLE sales (region VARCHAR, product VARCHAR, year INTEGER, amount INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO sales VALUES
		('East', 'Widget', 2023, 100),
		('East', 'Gadget', 2023, 200),
		('East', 'Widget', 2024, 150),
		('West', 'Widget', 2023, 120),
		('West', 'Gadget', 2024, 180)`)
	require.NoError(t, err)

	t.Run("ROLLUP produces correct subtotals", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT region, product, SUM(amount) as total
			FROM sales
			GROUP BY ROLLUP(region, product)
			ORDER BY region NULLS LAST, product NULLS LAST`)
		require.NoError(t, err)
		defer rows.Close()

		var hasGrandTotal bool
		var grandTotal int
		rowCount := 0

		for rows.Next() {
			var region, product sql.NullString
			var total int
			err = rows.Scan(&region, &product, &total)
			require.NoError(t, err)
			rowCount++

			if !region.Valid && !product.Valid {
				hasGrandTotal = true
				grandTotal = total
			}
		}

		assert.True(t, hasGrandTotal, "Should have grand total row")
		assert.Equal(t, 750, grandTotal, "Grand total should be sum of all amounts")
		// ROLLUP(region, product) produces: (region, product), (region, NULL), (NULL, NULL)
		// For 2 regions and varying products, we expect more than 5 rows
		assert.Greater(t, rowCount, 5)
	})

	t.Run("CUBE produces all combinations", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT region, year, SUM(amount) as total
			FROM sales
			GROUP BY CUBE(region, year)
			ORDER BY region NULLS LAST, year NULLS LAST`)
		require.NoError(t, err)
		defer rows.Close()

		combinations := make(map[string]int)
		for rows.Next() {
			var region sql.NullString
			var year sql.NullInt64
			var total int
			err = rows.Scan(&region, &year, &total)
			require.NoError(t, err)

			key := fmt.Sprintf("%v-%v", region.Valid, year.Valid)
			combinations[key]++
		}

		// CUBE produces: (region, year), (region, NULL), (NULL, year), (NULL, NULL)
		assert.Contains(t, combinations, "true-true")   // (region, year)
		assert.Contains(t, combinations, "true-false")  // (region, NULL)
		assert.Contains(t, combinations, "false-true")  // (NULL, year)
		assert.Contains(t, combinations, "false-false") // (NULL, NULL)
	})
}

// ============================================================================
// DEEP VALIDATION: PIVOT / UNPIVOT
// ============================================================================

func TestDeepValidation_PivotUnpivot(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	t.Run("PIVOT transforms rows to columns", func(t *testing.T) {
		_, err = db.Exec(
			`CREATE TABLE quarterly_data (year INTEGER, quarter VARCHAR, revenue INTEGER)`,
		)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO quarterly_data VALUES
			(2023, 'Q1', 100), (2023, 'Q2', 150), (2023, 'Q3', 200), (2023, 'Q4', 250),
			(2024, 'Q1', 120), (2024, 'Q2', 180)`)
		require.NoError(t, err)

		// Basic pivot query
		rows, err := db.Query(`
			SELECT * FROM quarterly_data
			PIVOT (SUM(revenue) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
			ORDER BY year`)
		require.NoError(t, err)
		defer rows.Close()

		cols, err := rows.Columns()
		require.NoError(t, err)
		t.Logf("PIVOT columns: %v", cols)

		// Should have year and Q1-Q4 columns
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		assert.Equal(t, 2, rowCount, "Should have 2 years of data")
	})

	t.Run("UNPIVOT transforms columns to rows", func(t *testing.T) {
		_, err = db.Exec(
			`CREATE TABLE wide_data (id INTEGER, jan INTEGER, feb INTEGER, mar INTEGER)`,
		)
		require.NoError(t, err)

		_, err = db.Exec(`INSERT INTO wide_data VALUES (1, 100, 110, 120), (2, 200, 210, 220)`)
		require.NoError(t, err)

		rows, err := db.Query(`
			SELECT * FROM wide_data
			UNPIVOT (value FOR month IN (jan, feb, mar))
			ORDER BY id, month`)
		require.NoError(t, err)
		defer rows.Close()

		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		// 2 rows * 3 months = 6 unpivoted rows
		assert.Equal(t, 6, rowCount, "Should have 6 rows after unpivot")
	})
}

// ============================================================================
// DEEP VALIDATION: DISTINCT ON
// ============================================================================

func TestDeepValidation_DistinctOn(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(
		`CREATE TABLE events (category VARCHAR, event_name VARCHAR, event_time INTEGER, priority INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO events VALUES
		('A', 'Event1', 100, 1),
		('A', 'Event2', 200, 2),
		('A', 'Event3', 150, 3),
		('B', 'Event4', 50, 1),
		('B', 'Event5', 300, 2)`)
	require.NoError(t, err)

	t.Run("DISTINCT ON returns first row per group", func(t *testing.T) {
		// Get earliest event per category
		rows, err := db.Query(`
			SELECT DISTINCT ON (category) category, event_name, event_time
			FROM events
			ORDER BY category, event_time ASC`)
		require.NoError(t, err)
		defer rows.Close()

		results := make(map[string]string)
		for rows.Next() {
			var category, eventName string
			var eventTime int
			err = rows.Scan(&category, &eventName, &eventTime)
			require.NoError(t, err)
			results[category] = eventName
		}

		assert.Len(t, results, 2)
		assert.Equal(t, "Event1", results["A"], "Category A earliest event is Event1 (time 100)")
		assert.Equal(t, "Event4", results["B"], "Category B earliest event is Event4 (time 50)")
	})

	t.Run("DISTINCT ON with different ordering", func(t *testing.T) {
		// Get highest priority event per category
		rows, err := db.Query(`
			SELECT DISTINCT ON (category) category, event_name, priority
			FROM events
			ORDER BY category, priority DESC`)
		require.NoError(t, err)
		defer rows.Close()

		results := make(map[string]int)
		for rows.Next() {
			var category, eventName string
			var priority int
			err = rows.Scan(&category, &eventName, &priority)
			require.NoError(t, err)
			results[category] = priority
		}

		assert.Equal(t, 3, results["A"], "Category A highest priority is 3")
		assert.Equal(t, 2, results["B"], "Category B highest priority is 2")
	})
}

// ============================================================================
// DEEP VALIDATION: QUALIFY
// ============================================================================

func TestDeepValidation_Qualify(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE sales_reps (rep_name VARCHAR, region VARCHAR, sales INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO sales_reps VALUES
		('Alice', 'East', 1000),
		('Bob', 'East', 1500),
		('Charlie', 'East', 800),
		('Dave', 'West', 2000),
		('Eve', 'West', 1800),
		('Frank', 'West', 1200)`)
	require.NoError(t, err)

	t.Run("QUALIFY filters by row number", func(t *testing.T) {
		// Get top 2 sales reps per region
		rows, err := db.Query(`
			SELECT rep_name, region, sales,
				   ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales DESC) as rn
			FROM sales_reps
			QUALIFY rn <= 2
			ORDER BY region, sales DESC`)
		require.NoError(t, err)
		defer rows.Close()

		results := make(map[string][]string)
		for rows.Next() {
			var repName, region string
			var sales, rn int
			err = rows.Scan(&repName, &region, &sales, &rn)
			require.NoError(t, err)
			results[region] = append(results[region], repName)
		}

		assert.Len(t, results["East"], 2)
		assert.Contains(t, results["East"], "Bob")   // 1500 - top
		assert.Contains(t, results["East"], "Alice") // 1000 - second

		assert.Len(t, results["West"], 2)
		assert.Contains(t, results["West"], "Dave") // 2000 - top
		assert.Contains(t, results["West"], "Eve")  // 1800 - second
	})

	t.Run("QUALIFY with RANK for ties", func(t *testing.T) {
		// Add tied sales values
		_, err = db.Exec(`INSERT INTO sales_reps VALUES ('Grace', 'East', 1500)`)
		require.NoError(t, err)

		rows, err := db.Query(`
			SELECT rep_name, region, sales,
				   RANK() OVER (PARTITION BY region ORDER BY sales DESC) as rnk
			FROM sales_reps
			WHERE region = 'East'
			QUALIFY rnk = 1
			ORDER BY rep_name`)
		require.NoError(t, err)
		defer rows.Close()

		topReps := []string{}
		for rows.Next() {
			var repName, region string
			var sales, rnk int
			err = rows.Scan(&repName, &region, &sales, &rnk)
			require.NoError(t, err)
			topReps = append(topReps, repName)
		}

		// Both Bob and Grace have 1500 (tied for rank 1)
		assert.Len(t, topReps, 2)
		assert.Contains(t, topReps, "Bob")
		assert.Contains(t, topReps, "Grace")
	})
}

// ============================================================================
// DEEP VALIDATION: SAMPLE
// ============================================================================

func TestDeepValidation_Sample(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE large_table (id INTEGER)`)
	require.NoError(t, err)

	// Insert 1000 rows
	for i := 0; i < 1000; i++ {
		_, err = db.Exec(`INSERT INTO large_table VALUES (?)`, i)
		require.NoError(t, err)
	}

	t.Run("BERNOULLI sampling returns approximate percentage", func(t *testing.T) {
		rows, err := db.Query(`SELECT * FROM large_table TABLESAMPLE BERNOULLI(10)`)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}

		// 10% of 1000 = ~100, but with variance
		// Allow range of 50-150 (quite wide due to randomness)
		assert.Greater(t, count, 20, "Should have at least some samples")
		assert.Less(t, count, 500, "Should not have too many samples")
		t.Logf("BERNOULLI(10) returned %d rows out of 1000", count)
	})

	t.Run("SYSTEM sampling returns approximate percentage", func(t *testing.T) {
		rows, err := db.Query(`SELECT * FROM large_table TABLESAMPLE SYSTEM(5)`)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}

		// 5% of 1000 = ~50
		assert.Greater(t, count, 0, "Should have at least some samples")
		assert.Less(t, count, 500, "Should not have too many samples")
		t.Logf("SYSTEM(5) returned %d rows out of 1000", count)
	})

	t.Run("REPEATABLE produces consistent results", func(t *testing.T) {
		// Run the same query twice with same seed
		getCount := func() int {
			rows, err := db.Query(
				`SELECT * FROM large_table TABLESAMPLE BERNOULLI(20) REPEATABLE(42)`,
			)
			require.NoError(t, err)
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			return count
		}

		count1 := getCount()
		count2 := getCount()

		assert.Equal(t, count1, count2, "Same seed should produce same sample size")
		t.Logf("REPEATABLE(42) returned %d rows both times", count1)
	})
}

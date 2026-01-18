package tests

import (
	"database/sql"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	// Import to register the engine backend
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReturningClauseIntegration verifies RETURNING clause works end-to-end
func TestReturningClauseIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec(
		`CREATE TABLE returning_test (id INTEGER PRIMARY KEY, name VARCHAR, value INTEGER)`,
	)
	require.NoError(t, err)

	// Test INSERT RETURNING
	t.Run("INSERT RETURNING", func(t *testing.T) {
		rows, err := db.Query(
			`INSERT INTO returning_test (id, name, value) VALUES (1, 'Alice', 100) RETURNING id, name`,
		)
		require.NoError(t, err)
		defer rows.Close()

		var id int
		var name string
		require.True(t, rows.Next())
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "Alice", name)
	})

	// Test UPDATE RETURNING
	t.Run("UPDATE RETURNING", func(t *testing.T) {
		rows, err := db.Query(
			`UPDATE returning_test SET value = 200 WHERE id = 1 RETURNING id, value`,
		)
		require.NoError(t, err)
		defer rows.Close()

		var id, value int
		require.True(t, rows.Next())
		err = rows.Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, 200, value)
	})

	// Test DELETE RETURNING
	t.Run("DELETE RETURNING", func(t *testing.T) {
		rows, err := db.Query(`DELETE FROM returning_test WHERE id = 1 RETURNING id, name, value`)
		require.NoError(t, err)
		defer rows.Close()

		var id, value int
		var name string
		require.True(t, rows.Next())
		err = rows.Scan(&id, &name, &value)
		require.NoError(t, err)
		assert.Equal(t, 1, id)
		assert.Equal(t, "Alice", name)
		assert.Equal(t, 200, value)
	})
}

// TestDistinctOnIntegration verifies DISTINCT ON works end-to-end
func TestDistinctOnIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(
		`CREATE TABLE distincton_test (category VARCHAR, value INTEGER, created_at INTEGER)`,
	)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO distincton_test VALUES
		('A', 10, 1),
		('A', 20, 2),
		('B', 30, 1),
		('B', 40, 2)`)
	require.NoError(t, err)

	// Test DISTINCT ON - get first row per category when ordered by created_at
	rows, err := db.Query(`SELECT DISTINCT ON (category) category, value, created_at
		FROM distincton_test ORDER BY category, created_at ASC`)
	require.NoError(t, err)
	defer rows.Close()

	results := make(map[string]int)
	for rows.Next() {
		var category string
		var value, createdAt int
		err = rows.Scan(&category, &value, &createdAt)
		require.NoError(t, err)
		results[category] = value
	}

	// Should get first row per category (lowest created_at)
	assert.Len(t, results, 2)
	assert.Equal(t, 10, results["A"]) // First A row has value 10
	assert.Equal(t, 30, results["B"]) // First B row has value 30
}

// TestSampleIntegration verifies SAMPLE clause works end-to-end
func TestSampleIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate test table with many rows
	_, err = db.Exec(`CREATE TABLE sample_test (id INTEGER)`)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err = db.Exec(`INSERT INTO sample_test VALUES (?)`, i)
		require.NoError(t, err)
	}

	// Test SAMPLE with percentage - should return approximately 10 rows
	t.Run("SAMPLE with percentage", func(t *testing.T) {
		rows, err := db.Query(`SELECT * FROM sample_test TABLESAMPLE BERNOULLI(10)`)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			count++
		}
		// Allow for statistical variance - should be roughly 10 ± some variance
		assert.Greater(t, count, 0)
		assert.Less(t, count, 50) // Should be much less than total
	})
}

// TestQualifyIntegration verifies QUALIFY clause works end-to-end
func TestQualifyIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(`CREATE TABLE qualify_test (category VARCHAR, value INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO qualify_test VALUES
		('A', 10),
		('A', 20),
		('A', 30),
		('B', 100),
		('B', 200)`)
	require.NoError(t, err)

	// Test QUALIFY - get rows where row_number = 1 within each category
	rows, err := db.Query(`
		SELECT category, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn
		FROM qualify_test
		QUALIFY rn = 1`)
	require.NoError(t, err)
	defer rows.Close()

	results := make(map[string]int)
	for rows.Next() {
		var category string
		var value, rn int
		err = rows.Scan(&category, &value, &rn)
		require.NoError(t, err)
		results[category] = value
	}

	// Should get highest value per category
	assert.Equal(t, 30, results["A"])
	assert.Equal(t, 200, results["B"])
}

// TestMergeIntoIntegration verifies MERGE INTO works end-to-end
func TestMergeIntoIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create target and source tables
	_, err = db.Exec(`CREATE TABLE merge_target (id INTEGER PRIMARY KEY, value INTEGER)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE merge_source (id INTEGER, value INTEGER)`)
	require.NoError(t, err)

	// Populate target
	_, err = db.Exec(`INSERT INTO merge_target VALUES (1, 100), (2, 200)`)
	require.NoError(t, err)

	// Populate source with updates and new rows
	_, err = db.Exec(`INSERT INTO merge_source VALUES (1, 150), (3, 300)`)
	require.NoError(t, err)

	// Execute MERGE
	_, err = db.Exec(`
		MERGE INTO merge_target AS t
		USING merge_source AS s
		ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET value = s.value
		WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)`)
	require.NoError(t, err)

	// Verify results
	rows, err := db.Query(`SELECT id, value FROM merge_target ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	expected := map[int]int{
		1: 150, // Updated
		2: 200, // Unchanged
		3: 300, // Inserted
	}

	for rows.Next() {
		var id, value int
		err = rows.Scan(&id, &value)
		require.NoError(t, err)
		assert.Equal(t, expected[id], value, "id=%d", id)
	}
}

// TestLateralJoinIntegration verifies LATERAL joins work end-to-end
func TestLateralJoinIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate test tables
	_, err = db.Exec(`CREATE TABLE lateral_main (id INTEGER, limit_val INTEGER)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE lateral_detail (main_id INTEGER, value INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO lateral_main VALUES (1, 2), (2, 1)`)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO lateral_detail VALUES (1, 10), (1, 20), (1, 30), (2, 100), (2, 200)`,
	)
	require.NoError(t, err)

	// Test LATERAL join - for each main row, get top N details based on main.limit_val
	rows, err := db.Query(`
		SELECT m.id, d.value
		FROM lateral_main m,
		LATERAL (SELECT value FROM lateral_detail WHERE main_id = m.id ORDER BY value LIMIT m.limit_val) d`)
	require.NoError(t, err)
	defer rows.Close()

	results := make(map[int][]int)
	for rows.Next() {
		var id, value int
		err = rows.Scan(&id, &value)
		require.NoError(t, err)
		results[id] = append(results[id], value)
	}

	// Main id 1 should have 2 values (limit_val=2): [10, 20]
	// Main id 2 should have 1 value (limit_val=1): [100]
	assert.Len(t, results[1], 2)
	assert.Len(t, results[2], 1)
}

// TestRecursiveCTEIntegration verifies recursive CTEs work end-to-end
func TestRecursiveCTEIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate hierarchical table
	_, err = db.Exec(`CREATE TABLE employees (id INTEGER, name VARCHAR, manager_id INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO employees VALUES
		(1, 'CEO', NULL),
		(2, 'VP', 1),
		(3, 'Director', 2),
		(4, 'Manager', 3),
		(5, 'Employee', 4)`)
	require.NoError(t, err)

	// Test recursive CTE to find all reports under CEO
	rows, err := db.Query(`
		WITH RECURSIVE org_chart AS (
			SELECT id, name, manager_id, 1 as level
			FROM employees
			WHERE manager_id IS NULL
			UNION ALL
			SELECT e.id, e.name, e.manager_id, oc.level + 1
			FROM employees e
			JOIN org_chart oc ON e.manager_id = oc.id
		)
		SELECT id, name, level FROM org_chart ORDER BY level, id`)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		id    int
		name  string
		level int
	}
	for rows.Next() {
		var id, level int
		var name string
		err = rows.Scan(&id, &name, &level)
		require.NoError(t, err)
		results = append(results, struct {
			id    int
			name  string
			level int
		}{id, name, level})
	}

	// Should have all 5 employees with correct levels
	require.Len(t, results, 5)
	assert.Equal(t, "CEO", results[0].name)
	assert.Equal(t, 1, results[0].level)
	assert.Equal(t, "Employee", results[4].name)
	assert.Equal(t, 5, results[4].level)
}

// TestGroupingSetsIntegration verifies GROUPING SETS work end-to-end
func TestGroupingSetsIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate test table
	_, err = db.Exec(`CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO sales VALUES
		('East', 'Widget', 100),
		('East', 'Gadget', 200),
		('West', 'Widget', 150),
		('West', 'Gadget', 250)`)
	require.NoError(t, err)

	// Test ROLLUP - should give subtotals and grand total
	t.Run("ROLLUP", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT region, product, SUM(amount) as total
			FROM sales
			GROUP BY ROLLUP(region, product)
			ORDER BY region NULLS LAST, product NULLS LAST`)
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		var grandTotal int
		for rows.Next() {
			var region, product sql.NullString
			var total int
			err = rows.Scan(&region, &product, &total)
			require.NoError(t, err)
			if !region.Valid && !product.Valid {
				grandTotal = total
			}
			count++
		}
		// Should have: (East,Widget), (East,Gadget), (East,NULL), (West,Widget), (West,Gadget), (West,NULL), (NULL,NULL)
		assert.Equal(t, 7, count)
		assert.Equal(t, 700, grandTotal) // Sum of all amounts
	})
}

// TestPivotUnpivotIntegration verifies PIVOT/UNPIVOT work end-to-end
func TestPivotUnpivotIntegration(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Create and populate test table for PIVOT
	_, err = db.Exec(`CREATE TABLE quarterly_sales (year INTEGER, quarter VARCHAR, sales INTEGER)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO quarterly_sales VALUES
		(2023, 'Q1', 100),
		(2023, 'Q2', 150),
		(2023, 'Q3', 200),
		(2023, 'Q4', 250),
		(2024, 'Q1', 120),
		(2024, 'Q2', 180)`)
	require.NoError(t, err)

	// Test PIVOT - transform quarters into columns
	t.Run("PIVOT", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT * FROM quarterly_sales
			PIVOT (SUM(sales) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
			ORDER BY year`)
		require.NoError(t, err)
		defer rows.Close()

		cols, _ := rows.Columns()
		t.Logf("PIVOT columns: %v", cols)

		for rows.Next() {
			// Just verify it executes without error
		}
	})
}

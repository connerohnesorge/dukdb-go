package test_probe

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

// logRows scans all rows and logs them. Returns the number of rows read.
func logRows(t *testing.T, rows *sql.Rows) int {
	t.Helper()
	cols, err := rows.Columns()
	if err != nil {
		t.Errorf("failed to get columns: %v", err)
		return 0
	}
	t.Logf("Columns: %v", cols)

	count := 0
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Errorf("scan error on row %d: %v", count, err)
			continue
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			parts[i] = fmt.Sprintf("%s=%v", cols[i], v)
		}
		t.Logf("  Row %d: %s", count, strings.Join(parts, ", "))
		count++
	}
	if err := rows.Err(); err != nil {
		t.Errorf("rows iteration error: %v", err)
	}
	return count
}

// execLog executes a statement and logs the result.
func execLog(t *testing.T, db *sql.DB, query string, args ...interface{}) sql.Result {
	t.Helper()
	res, err := db.Exec(query, args...)
	if err != nil {
		t.Errorf("exec failed for [%s]: %v", truncateSQL(query), err)
		return nil
	}
	if ra, err := res.RowsAffected(); err == nil {
		t.Logf("exec [%s] => rows affected: %d", truncateSQL(query), ra)
	} else {
		t.Logf("exec [%s] => ok", truncateSQL(query))
	}
	return res
}

// queryLog runs a query and logs all result rows.
func queryLog(t *testing.T, db *sql.DB, query string, args ...interface{}) int {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Errorf("query failed for [%s]: %v", truncateSQL(query), err)
		return 0
	}
	defer rows.Close()
	n := logRows(t, rows)
	t.Logf("query [%s] returned %d rows", truncateSQL(query), n)
	return n
}

func truncateSQL(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}

func TestDMLAndQueries(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Setup base tables used across sub-tests.
	setup := []string{
		`CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			dept VARCHAR,
			salary DOUBLE,
			manager_id INTEGER,
			hire_date DATE DEFAULT CURRENT_DATE
		)`,
		`CREATE TABLE departments (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			budget DOUBLE
		)`,
		`INSERT INTO departments VALUES (1, 'Engineering', 500000), (2, 'Sales', 300000), (3, 'HR', 200000), (4, 'Marketing', 150000)`,
	}
	for _, s := range setup {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup failed [%s]: %v", truncateSQL(s), err)
		}
	}

	// ---------------------------------------------------------------
	// 1. INSERT single row, INSERT multiple rows
	// ---------------------------------------------------------------
	t.Run("INSERT_single_and_multiple_rows", func(t *testing.T) {
		execLog(t, db, `INSERT INTO employees (id, name, dept, salary, manager_id) VALUES (1, 'Alice', 'Engineering', 120000, NULL)`)
		execLog(t, db, `INSERT INTO employees (id, name, dept, salary, manager_id) VALUES
			(2, 'Bob', 'Engineering', 110000, 1),
			(3, 'Carol', 'Sales', 90000, 1),
			(4, 'Dave', 'Sales', 85000, 3),
			(5, 'Eve', 'HR', 95000, 1)`)
		n := queryLog(t, db, `SELECT * FROM employees ORDER BY id`)
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		}
	})

	// ---------------------------------------------------------------
	// 2. INSERT with DEFAULT values
	// ---------------------------------------------------------------
	t.Run("INSERT_with_DEFAULT", func(t *testing.T) {
		execLog(t, db, `INSERT INTO employees (id, name, dept, salary, manager_id) VALUES (6, 'Frank', 'HR', 70000, 5)`)
		queryLog(t, db, `SELECT id, name, hire_date FROM employees WHERE id = 6`)
	})

	// ---------------------------------------------------------------
	// 3. INSERT ... SELECT
	// ---------------------------------------------------------------
	t.Run("INSERT_SELECT", func(t *testing.T) {
		execLog(t, db, `CREATE TABLE engineers (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			dept VARCHAR,
			salary DOUBLE,
			manager_id INTEGER,
			hire_date DATE
		)`)
		execLog(t, db, `INSERT INTO engineers SELECT * FROM employees WHERE dept = 'Engineering'`)
		n := queryLog(t, db, `SELECT * FROM engineers`)
		if n != 2 {
			t.Errorf("expected 2 engineers, got %d", n)
		}
	})

	// ---------------------------------------------------------------
	// 4. UPDATE with WHERE clause
	// ---------------------------------------------------------------
	t.Run("UPDATE_with_WHERE", func(t *testing.T) {
		execLog(t, db, `UPDATE employees SET salary = salary * 1.10 WHERE dept = 'Engineering'`)
		queryLog(t, db, `SELECT id, name, salary FROM employees WHERE dept = 'Engineering'`)
	})

	// ---------------------------------------------------------------
	// 5. UPDATE with subquery
	// ---------------------------------------------------------------
	t.Run("UPDATE_with_subquery", func(t *testing.T) {
		execLog(t, db, `UPDATE employees SET salary = salary + 5000 WHERE dept IN (SELECT name FROM departments WHERE budget > 400000)`)
		queryLog(t, db, `SELECT id, name, salary, dept FROM employees WHERE dept = 'Engineering'`)
	})

	// ---------------------------------------------------------------
	// 6. DELETE with WHERE clause
	// ---------------------------------------------------------------
	t.Run("DELETE_with_WHERE", func(t *testing.T) {
		execLog(t, db, `INSERT INTO employees (id, name, dept, salary, manager_id) VALUES (99, 'Temp', 'HR', 10000, NULL)`)
		execLog(t, db, `DELETE FROM employees WHERE id = 99`)
		var cnt int
		if err := db.QueryRow(`SELECT COUNT(*) FROM employees WHERE id = 99`).Scan(&cnt); err != nil {
			t.Error(err)
		} else if cnt != 0 {
			t.Errorf("expected 0 rows after delete, got %d", cnt)
		}
	})

	// ---------------------------------------------------------------
	// 7. DELETE all rows (no WHERE)
	// ---------------------------------------------------------------
	t.Run("DELETE_all_rows", func(t *testing.T) {
		execLog(t, db, `CREATE TABLE temp_del (x INT)`)
		execLog(t, db, `INSERT INTO temp_del VALUES (1),(2),(3)`)
		execLog(t, db, `DELETE FROM temp_del`)
		var cnt int
		if err := db.QueryRow(`SELECT COUNT(*) FROM temp_del`).Scan(&cnt); err != nil {
			t.Error(err)
		} else if cnt != 0 {
			t.Errorf("expected 0 rows after delete all, got %d", cnt)
		}
	})

	// ---------------------------------------------------------------
	// 8. UPSERT / INSERT OR REPLACE / ON CONFLICT
	// ---------------------------------------------------------------
	t.Run("INSERT_OR_REPLACE", func(t *testing.T) {
		execLog(t, db, `CREATE TABLE kv (k VARCHAR PRIMARY KEY, v INTEGER)`)
		execLog(t, db, `INSERT INTO kv VALUES ('a', 1), ('b', 2)`)
		execLog(t, db, `INSERT OR REPLACE INTO kv VALUES ('a', 10), ('c', 3)`)
		queryLog(t, db, `SELECT * FROM kv ORDER BY k`)
	})

	t.Run("INSERT_ON_CONFLICT", func(t *testing.T) {
		execLog(t, db, `CREATE TABLE kv2 (k VARCHAR PRIMARY KEY, v INTEGER)`)
		execLog(t, db, `INSERT INTO kv2 VALUES ('x', 100)`)
		execLog(t, db, `INSERT INTO kv2 VALUES ('x', 200) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`)
		var v int
		if err := db.QueryRow(`SELECT v FROM kv2 WHERE k = 'x'`).Scan(&v); err != nil {
			t.Error(err)
		} else {
			t.Logf("ON CONFLICT result: k=x, v=%d", v)
		}
	})

	// ---------------------------------------------------------------
	// 9. SELECT with WHERE, ORDER BY, LIMIT, OFFSET
	// ---------------------------------------------------------------
	t.Run("SELECT_WHERE_ORDER_LIMIT_OFFSET", func(t *testing.T) {
		queryLog(t, db, `SELECT id, name, salary FROM employees WHERE salary > 90000 ORDER BY salary DESC LIMIT 3 OFFSET 1`)
	})

	// ---------------------------------------------------------------
	// 10. SELECT DISTINCT
	// ---------------------------------------------------------------
	t.Run("SELECT_DISTINCT", func(t *testing.T) {
		queryLog(t, db, `SELECT DISTINCT dept FROM employees ORDER BY dept`)
	})

	// ---------------------------------------------------------------
	// 11. GROUP BY with HAVING
	// ---------------------------------------------------------------
	t.Run("GROUP_BY_HAVING", func(t *testing.T) {
		queryLog(t, db, `SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY dept HAVING COUNT(*) >= 2 ORDER BY dept`)
	})

	// ---------------------------------------------------------------
	// 12. Aggregate functions
	// ---------------------------------------------------------------
	t.Run("Aggregate_functions", func(t *testing.T) {
		queryLog(t, db, `SELECT
			COUNT(*) AS total,
			SUM(salary) AS sum_sal,
			AVG(salary) AS avg_sal,
			MIN(salary) AS min_sal,
			MAX(salary) AS max_sal,
			COUNT(DISTINCT dept) AS distinct_depts
		FROM employees`)

		queryLog(t, db, `SELECT STRING_AGG(name, ', ' ORDER BY name) AS all_names FROM employees`)
	})

	// ---------------------------------------------------------------
	// 13. JOINs
	// ---------------------------------------------------------------
	t.Run("INNER_JOIN", func(t *testing.T) {
		queryLog(t, db, `SELECT e.name, d.name AS dept_name, d.budget
			FROM employees e INNER JOIN departments d ON e.dept = d.name
			ORDER BY e.name`)
	})

	t.Run("LEFT_JOIN", func(t *testing.T) {
		queryLog(t, db, `SELECT d.name AS dept_name, e.name AS emp_name
			FROM departments d LEFT JOIN employees e ON d.name = e.dept
			ORDER BY d.name, e.name`)
	})

	t.Run("RIGHT_JOIN", func(t *testing.T) {
		queryLog(t, db, `SELECT e.name AS emp_name, d.name AS dept_name
			FROM employees e RIGHT JOIN departments d ON e.dept = d.name
			ORDER BY d.name, e.name`)
	})

	t.Run("FULL_OUTER_JOIN", func(t *testing.T) {
		execLog(t, db, `CREATE TABLE t_left (id INT, val VARCHAR)`)
		execLog(t, db, `CREATE TABLE t_right (id INT, val VARCHAR)`)
		execLog(t, db, `INSERT INTO t_left VALUES (1,'a'),(2,'b'),(3,'c')`)
		execLog(t, db, `INSERT INTO t_right VALUES (2,'x'),(3,'y'),(4,'z')`)
		queryLog(t, db, `SELECT l.id AS l_id, l.val AS l_val, r.id AS r_id, r.val AS r_val
			FROM t_left l FULL OUTER JOIN t_right r ON l.id = r.id
			ORDER BY COALESCE(l.id, r.id)`)
	})

	t.Run("CROSS_JOIN", func(t *testing.T) {
		execLog(t, db, `CREATE TABLE cross_a (v INT)`)
		execLog(t, db, `INSERT INTO cross_a VALUES (1),(2)`)
		execLog(t, db, `CREATE TABLE cross_b (v VARCHAR)`)
		execLog(t, db, `INSERT INTO cross_b VALUES ('a'),('b')`)
		queryLog(t, db, `SELECT a.v AS x, b.v AS y FROM cross_a a CROSS JOIN cross_b b ORDER BY x, y`)
	})

	// ---------------------------------------------------------------
	// 14. Subqueries
	// ---------------------------------------------------------------
	t.Run("Scalar_subquery", func(t *testing.T) {
		queryLog(t, db, `SELECT name, salary, (SELECT AVG(salary) FROM employees) AS company_avg FROM employees ORDER BY name`)
	})

	t.Run("IN_subquery", func(t *testing.T) {
		queryLog(t, db, `SELECT name FROM employees WHERE dept IN (SELECT name FROM departments WHERE budget > 200000) ORDER BY name`)
	})

	t.Run("EXISTS_subquery", func(t *testing.T) {
		queryLog(t, db, `SELECT d.name FROM departments d WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept = d.name) ORDER BY d.name`)
	})

	t.Run("Correlated_subquery", func(t *testing.T) {
		queryLog(t, db, `SELECT e.name, e.salary,
			(SELECT COUNT(*) FROM employees e2 WHERE e2.dept = e.dept) AS dept_size
			FROM employees e ORDER BY e.name`)
	})

	// ---------------------------------------------------------------
	// 15. CTEs (WITH ... AS)
	// ---------------------------------------------------------------
	t.Run("CTE", func(t *testing.T) {
		queryLog(t, db, `WITH dept_stats AS (
			SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept
		)
		SELECT e.name, e.salary, ds.avg_sal
		FROM employees e JOIN dept_stats ds ON e.dept = ds.dept
		ORDER BY e.name`)
	})

	// ---------------------------------------------------------------
	// 16. Recursive CTEs
	// ---------------------------------------------------------------
	t.Run("Recursive_CTE", func(t *testing.T) {
		queryLog(t, db, `WITH RECURSIVE cnt(x) AS (
			SELECT 1
			UNION ALL
			SELECT x + 1 FROM cnt WHERE x < 10
		)
		SELECT x FROM cnt`)
	})

	t.Run("Recursive_CTE_hierarchy", func(t *testing.T) {
		queryLog(t, db, `WITH RECURSIVE org(id, name, manager_id, lvl) AS (
			SELECT id, name, manager_id, 0 FROM employees WHERE manager_id IS NULL
			UNION ALL
			SELECT e.id, e.name, e.manager_id, o.lvl + 1
			FROM employees e JOIN org o ON e.manager_id = o.id
		)
		SELECT id, name, lvl FROM org ORDER BY lvl, name`)
	})

	// ---------------------------------------------------------------
	// 17. UNION, UNION ALL, INTERSECT, EXCEPT
	// ---------------------------------------------------------------
	t.Run("UNION_ALL", func(t *testing.T) {
		queryLog(t, db, `SELECT name FROM employees WHERE dept = 'Engineering'
			UNION ALL
			SELECT name FROM employees WHERE salary > 100000
			ORDER BY name`)
	})

	t.Run("UNION", func(t *testing.T) {
		queryLog(t, db, `SELECT name FROM employees WHERE dept = 'Engineering'
			UNION
			SELECT name FROM employees WHERE salary > 100000
			ORDER BY name`)
	})

	t.Run("INTERSECT", func(t *testing.T) {
		queryLog(t, db, `SELECT dept FROM employees WHERE salary > 80000
			INTERSECT
			SELECT dept FROM employees WHERE salary < 100000
			ORDER BY dept`)
	})

	t.Run("EXCEPT", func(t *testing.T) {
		queryLog(t, db, `SELECT dept FROM employees
			EXCEPT
			SELECT name FROM departments WHERE budget < 250000`)
	})

	// ---------------------------------------------------------------
	// 18. CASE WHEN expressions
	// ---------------------------------------------------------------
	t.Run("CASE_WHEN", func(t *testing.T) {
		queryLog(t, db, `SELECT name, salary,
			CASE
				WHEN salary > 120000 THEN 'high'
				WHEN salary > 90000 THEN 'mid'
				ELSE 'low'
			END AS band
		FROM employees ORDER BY name`)
	})

	// ---------------------------------------------------------------
	// 19. Window functions
	// ---------------------------------------------------------------
	t.Run("Window_ROW_NUMBER", func(t *testing.T) {
		queryLog(t, db, `SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees ORDER BY dept, rn`)
	})

	t.Run("Window_RANK_DENSE_RANK", func(t *testing.T) {
		queryLog(t, db, `SELECT name, salary,
			RANK() OVER (ORDER BY salary DESC) AS rnk,
			DENSE_RANK() OVER (ORDER BY salary DESC) AS drnk
		FROM employees ORDER BY salary DESC`)
	})

	t.Run("Window_LAG_LEAD", func(t *testing.T) {
		queryLog(t, db, `SELECT name, salary,
			LAG(salary, 1) OVER (ORDER BY salary) AS prev_sal,
			LEAD(salary, 1) OVER (ORDER BY salary) AS next_sal
		FROM employees ORDER BY salary`)
	})

	t.Run("Window_NTILE", func(t *testing.T) {
		queryLog(t, db, `SELECT name, salary, NTILE(3) OVER (ORDER BY salary DESC) AS tile FROM employees ORDER BY salary DESC`)
	})

	t.Run("Window_SUM_AVG_OVER", func(t *testing.T) {
		queryLog(t, db, `SELECT name, dept, salary,
			SUM(salary) OVER (PARTITION BY dept) AS dept_total,
			AVG(salary) OVER (PARTITION BY dept) AS dept_avg
		FROM employees ORDER BY dept, name`)
	})

	// ---------------------------------------------------------------
	// 20. BETWEEN, LIKE, ILIKE, IN list
	// ---------------------------------------------------------------
	t.Run("BETWEEN", func(t *testing.T) {
		queryLog(t, db, `SELECT name, salary FROM employees WHERE salary BETWEEN 85000 AND 100000 ORDER BY salary`)
	})

	t.Run("LIKE", func(t *testing.T) {
		queryLog(t, db, `SELECT name FROM employees WHERE name LIKE 'A%' ORDER BY name`)
	})

	t.Run("ILIKE", func(t *testing.T) {
		queryLog(t, db, `SELECT name FROM employees WHERE name ILIKE 'a%' ORDER BY name`)
	})

	t.Run("IN_list", func(t *testing.T) {
		queryLog(t, db, `SELECT name, dept FROM employees WHERE dept IN ('Engineering', 'HR') ORDER BY name`)
	})

	// ---------------------------------------------------------------
	// 21. IS NULL, IS NOT NULL, COALESCE, NULLIF
	// ---------------------------------------------------------------
	t.Run("IS_NULL_IS_NOT_NULL", func(t *testing.T) {
		queryLog(t, db, `SELECT name, manager_id FROM employees WHERE manager_id IS NULL`)
		queryLog(t, db, `SELECT name, manager_id FROM employees WHERE manager_id IS NOT NULL ORDER BY name`)
	})

	t.Run("COALESCE", func(t *testing.T) {
		queryLog(t, db, `SELECT name, COALESCE(manager_id, -1) AS mgr FROM employees ORDER BY name`)
	})

	t.Run("NULLIF", func(t *testing.T) {
		queryLog(t, db, `SELECT NULLIF(1, 1) AS nil_val, NULLIF(1, 2) AS one_val`)
	})

	// ---------------------------------------------------------------
	// 22. String functions
	// ---------------------------------------------------------------
	t.Run("String_functions", func(t *testing.T) {
		queryLog(t, db, `SELECT
			UPPER('hello') AS up,
			LOWER('WORLD') AS lo,
			LENGTH('test') AS len,
			SUBSTRING('abcdef', 2, 3) AS sub,
			REPLACE('foobar', 'bar', 'baz') AS rep,
			TRIM('  x  ') AS trimmed,
			CONCAT('a', 'b', 'c') AS cat`)

		queryLog(t, db, `SELECT
			SUBSTRING('abcdef', 1, 3) AS lft,
			SUBSTRING('abcdef', 5, 2) AS rgt,
			REVERSE('abc') AS rev,
			REPEAT('ab', 3) AS rpt,
			INSTR('abcdef', 'cd') AS pos`)
	})

	// ---------------------------------------------------------------
	// 23. Math functions
	// ---------------------------------------------------------------
	t.Run("Math_functions", func(t *testing.T) {
		queryLog(t, db, `SELECT
			ABS(-42) AS abs_v,
			CEIL(3.2) AS ceil_v,
			FLOOR(3.8) AS floor_v,
			ROUND(3.456, 2) AS round_v,
			(10 % 3) AS mod_v,
			POWER(2, 10) AS pow_v,
			SQRT(144) AS sqrt_v`)

		queryLog(t, db, `SELECT
			LOG10(1000) AS log10_v,
			LN(2.718281828) AS ln_v,
			SIGN(-5) AS sign_neg,
			SIGN(5) AS sign_pos,
			GREATEST(1, 5, 3) AS grt,
			LEAST(1, 5, 3) AS lst`)
	})

	// ---------------------------------------------------------------
	// 24. Date/time functions
	// ---------------------------------------------------------------
	t.Run("Date_time_functions", func(t *testing.T) {
		queryLog(t, db, `SELECT CURRENT_DATE AS today`)
		queryLog(t, db, `SELECT CURRENT_TIMESTAMP AS now_ts`)
		queryLog(t, db, `SELECT EXTRACT(YEAR FROM DATE '2024-06-15') AS yr,
			EXTRACT(MONTH FROM DATE '2024-06-15') AS mo,
			EXTRACT(DAY FROM DATE '2024-06-15') AS dy`)
		queryLog(t, db, `SELECT DATE_PART('year', DATE '2024-06-15') AS yr`)
		queryLog(t, db, `SELECT DATE_TRUNC('month', TIMESTAMP '2024-06-15 10:30:00') AS truncated`)
		queryLog(t, db, `SELECT DATE_DIFF('day', DATE '2024-01-01', DATE '2024-12-31') AS diff_days`)
		queryLog(t, db, `SELECT AGE(DATE '2024-06-15', DATE '2020-01-01') AS age_val`)
	})

	// ---------------------------------------------------------------
	// 25. Type casting with CAST and ::
	// ---------------------------------------------------------------
	t.Run("Type_casting", func(t *testing.T) {
		queryLog(t, db, `SELECT CAST(42 AS VARCHAR) AS str_val, CAST('123' AS INTEGER) AS int_val`)
		queryLog(t, db, `SELECT 42::VARCHAR AS str_val, '123'::INTEGER AS int_val`)
		queryLog(t, db, `SELECT CAST(3.14 AS INTEGER) AS trunc_val, CAST('2024-01-15' AS DATE) AS date_val`)
	})

	// ---------------------------------------------------------------
	// 26. EXPLAIN
	// ---------------------------------------------------------------
	t.Run("EXPLAIN", func(t *testing.T) {
		rows, err := db.Query(`EXPLAIN SELECT * FROM employees WHERE dept = 'Engineering'`)
		if err != nil {
			t.Logf("EXPLAIN not supported or failed: %v", err)
		} else {
			defer rows.Close()
			logRows(t, rows)
		}
	})

	// ---------------------------------------------------------------
	// 27. Nested aggregation and complex expressions
	// ---------------------------------------------------------------
	t.Run("Nested_aggregation_complex_expressions", func(t *testing.T) {
		queryLog(t, db, `SELECT dept,
			ROUND(AVG(salary), 2) AS avg_sal,
			ROUND(AVG(salary) / (SELECT AVG(salary) FROM employees) * 100, 1) AS pct_of_company_avg
		FROM employees GROUP BY dept ORDER BY avg_sal DESC`)

		queryLog(t, db, `SELECT
			dept,
			COUNT(*) AS cnt,
			SUM(CASE WHEN salary > 100000 THEN 1 ELSE 0 END) AS high_earners,
			ROUND(SUM(CASE WHEN salary > 100000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_high
		FROM employees GROUP BY dept ORDER BY dept`)
	})

	// ---------------------------------------------------------------
	// 28. Multiple ORDER BY columns with ASC/DESC/NULLS FIRST/NULLS LAST
	// ---------------------------------------------------------------
	t.Run("ORDER_BY_multi_col", func(t *testing.T) {
		queryLog(t, db, `SELECT name, dept, salary FROM employees ORDER BY dept ASC, salary DESC`)
		queryLog(t, db, `SELECT name, manager_id FROM employees ORDER BY manager_id ASC NULLS FIRST, name ASC`)
		queryLog(t, db, `SELECT name, manager_id FROM employees ORDER BY manager_id DESC NULLS LAST, name DESC`)
	})

	// ---------------------------------------------------------------
	// 29. GENERATE_SERIES
	// ---------------------------------------------------------------
	t.Run("GENERATE_SERIES", func(t *testing.T) {
		queryLog(t, db, `SELECT * FROM GENERATE_SERIES(1, 5)`)
		queryLog(t, db, `SELECT * FROM GENERATE_SERIES(0, 20, 5)`)
	})

	// ---------------------------------------------------------------
	// 30. Parameterized queries with ? placeholders
	// ---------------------------------------------------------------
	t.Run("Parameterized_queries", func(t *testing.T) {
		// Parameterized SELECT
		rows, err := db.Query(`SELECT name, salary FROM employees WHERE dept = ? AND salary > ? ORDER BY name`, "Engineering", 100000)
		if err != nil {
			t.Errorf("parameterized query failed: %v", err)
		} else {
			defer rows.Close()
			n := logRows(t, rows)
			t.Logf("parameterized query returned %d rows", n)
		}

		// Parameterized INSERT
		res, err := db.Exec(`INSERT INTO employees (id, name, dept, salary, manager_id) VALUES (?, ?, ?, ?, ?)`, 100, "Zara", "Marketing", 88000, 1)
		if err != nil {
			t.Errorf("parameterized insert failed: %v", err)
		} else {
			if ra, err := res.RowsAffected(); err == nil {
				t.Logf("parameterized insert rows affected: %d", ra)
			}
		}

		// Parameterized UPDATE
		res, err = db.Exec(`UPDATE employees SET salary = ? WHERE id = ?`, 90000, 100)
		if err != nil {
			t.Errorf("parameterized update failed: %v", err)
		} else {
			if ra, err := res.RowsAffected(); err == nil {
				t.Logf("parameterized update rows affected: %d", ra)
			}
		}

		// Parameterized DELETE
		res, err = db.Exec(`DELETE FROM employees WHERE id = ?`, 100)
		if err != nil {
			t.Errorf("parameterized delete failed: %v", err)
		} else {
			if ra, err := res.RowsAffected(); err == nil {
				t.Logf("parameterized delete rows affected: %d", ra)
			}
		}

		// QueryRow with parameter
		var cnt int
		err = db.QueryRow(`SELECT COUNT(*) FROM employees WHERE dept = ?`, "Engineering").Scan(&cnt)
		if err != nil {
			t.Errorf("parameterized QueryRow failed: %v", err)
		} else {
			t.Logf("parameterized QueryRow: Engineering count = %d", cnt)
		}
	})
}

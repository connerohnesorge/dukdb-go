package dukdb

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

// Helper: execute ignoring panics, return error
func probeExec(db *sql.DB, q string, args ...any) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("PANIC: %v", r)
		}
	}()
	_, err := db.Exec(q, args...)
	return err
}

// Helper: query single value ignoring panics
func probeQueryVal(db *sql.DB, q string, args ...any) (val any, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("PANIC: %v", r)
		}
	}()
	err := db.QueryRow(q, args...).Scan(&val)
	return val, err
}

// Helper: query multiple rows, return all values
func probeQueryAll(db *sql.DB, q string) (cols []string, rows [][]any, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("PANIC: %v", r)
		}
	}()
	r, err := db.Query(q)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	cols, _ = r.Columns()
	for r.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := r.Scan(ptrs...); err != nil {
			return cols, rows, err
		}
		row := make([]any, len(vals))
		copy(row, vals)
		rows = append(rows, row)
	}
	return cols, rows, r.Err()
}

// ============================================================
// TEST 1: Complex DDL - tables, views, indexes, sequences, schemas
// ============================================================
func TestProbe_ComplexDDL(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type ddlCase struct {
		name string
		sql  string
	}

	cases := []ddlCase{
		// Schema management
		{"create schema", "CREATE SCHEMA analytics"},
		{"create table in schema", `CREATE TABLE analytics.events (
			id INTEGER PRIMARY KEY,
			event_type VARCHAR NOT NULL,
			payload VARCHAR,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			CHECK (length(event_type) > 0)
		)`},

		// Complex table with multiple constraints
		{"create orders table", `CREATE TABLE orders (
			order_id INTEGER PRIMARY KEY,
			customer_name VARCHAR NOT NULL,
			amount DECIMAL(10,2) NOT NULL CHECK (amount > 0),
			status VARCHAR DEFAULT 'pending',
			region VARCHAR,
			order_date DATE DEFAULT CURRENT_DATE,
			UNIQUE(customer_name, order_date)
		)`},

		// Table with foreign key
		{"create order_items", `CREATE TABLE order_items (
			item_id INTEGER PRIMARY KEY,
			order_id INTEGER REFERENCES orders(order_id),
			product VARCHAR NOT NULL,
			quantity INTEGER NOT NULL CHECK (quantity > 0),
			unit_price DECIMAL(10,2) NOT NULL
		)`},

		// Sequences
		{"create sequence", "CREATE SEQUENCE invoice_seq START WITH 1000 INCREMENT BY 1"},
		{"create cycling sequence", "CREATE SEQUENCE color_seq START WITH 1 INCREMENT BY 1 MAXVALUE 3 CYCLE"},

		// Indexes
		{"create index", "CREATE INDEX idx_orders_region ON orders(region)"},
		{"create unique index", "CREATE UNIQUE INDEX idx_orders_name_date ON orders(customer_name, order_date)"},
		{"create index on items", "CREATE INDEX idx_items_product ON order_items(product)"},

		// Views
		{"create view", `CREATE VIEW order_summary AS
			SELECT o.order_id, o.customer_name, o.amount, COUNT(oi.item_id) as item_count
			FROM orders o LEFT JOIN order_items oi ON o.order_id = oi.order_id
			GROUP BY o.order_id, o.customer_name, o.amount`},

		// ALTER TABLE
		{"add column", "ALTER TABLE orders ADD COLUMN priority INTEGER DEFAULT 0"},
		{"rename column", "ALTER TABLE orders RENAME COLUMN region TO sales_region"},
	}

	results := make(map[string]string)
	for _, c := range cases {
		err := probeExec(db, c.sql)
		if err != nil {
			results[c.name] = fmt.Sprintf("FAIL: %v", err)
		} else {
			results[c.name] = "OK"
		}
	}

	// Report
	t.Log("=== Complex DDL Results ===")
	for _, c := range cases {
		t.Logf("  %-30s %s", c.name, results[c.name])
	}

	// Verify sequence works
	val, err := probeQueryVal(db, "SELECT NEXTVAL('invoice_seq')")
	t.Logf("  %-30s %v (err: %v)", "nextval(invoice_seq)", val, err)

	val, err = probeQueryVal(db, "SELECT NEXTVAL('invoice_seq')")
	t.Logf("  %-30s %v (err: %v)", "nextval again", val, err)

	// Verify cycling sequence
	for i := 0; i < 5; i++ {
		val, err = probeQueryVal(db, "SELECT NEXTVAL('color_seq')")
		t.Logf("  %-30s %v (err: %v)", fmt.Sprintf("color_seq iteration %d", i), val, err)
	}

	// Drop operations
	drops := []ddlCase{
		{"drop index", "DROP INDEX idx_orders_region"},
		{"drop view", "DROP VIEW order_summary"},
		{"drop sequence", "DROP SEQUENCE invoice_seq"},
		{"drop table cascade", "DROP TABLE order_items"},
		{"drop schema", "DROP SCHEMA analytics"},
	}
	for _, c := range drops {
		err := probeExec(db, c.sql)
		if err != nil {
			t.Logf("  %-30s FAIL: %v", c.name, err)
		} else {
			t.Logf("  %-30s OK", c.name)
		}
	}
}

// ============================================================
// TEST 2: Complex queries - JOINs, subqueries, CTEs, window functions
// ============================================================
func TestProbe_ComplexQueries(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Setup data
	setup := []string{
		`CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			dept VARCHAR,
			salary DECIMAL(10,2),
			hire_date DATE,
			manager_id INTEGER
		)`,
		`INSERT INTO employees VALUES
			(1, 'Alice', 'Engineering', 120000, '2020-01-15', NULL),
			(2, 'Bob', 'Engineering', 110000, '2020-03-20', 1),
			(3, 'Carol', 'Sales', 95000, '2019-06-01', NULL),
			(4, 'Dave', 'Sales', 85000, '2021-01-10', 3),
			(5, 'Eve', 'Engineering', 130000, '2018-09-01', 1),
			(6, 'Frank', 'Marketing', 90000, '2022-02-15', NULL),
			(7, 'Grace', 'Marketing', 88000, '2022-05-20', 6),
			(8, 'Heidi', 'Engineering', 115000, '2021-07-01', 1),
			(9, 'Ivan', 'Sales', 92000, '2020-11-15', 3),
			(10, 'Judy', 'Engineering', 125000, '2019-03-01', 1)`,
		`CREATE TABLE departments (
			name VARCHAR PRIMARY KEY,
			budget DECIMAL(12,2),
			head_count INTEGER
		)`,
		`INSERT INTO departments VALUES
			('Engineering', 500000, 5),
			('Sales', 300000, 3),
			('Marketing', 200000, 2)`,
		`CREATE TABLE projects (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			dept VARCHAR,
			start_date DATE,
			end_date DATE
		)`,
		`INSERT INTO projects VALUES
			(1, 'Project Alpha', 'Engineering', '2023-01-01', '2023-06-30'),
			(2, 'Project Beta', 'Engineering', '2023-03-15', '2023-12-31'),
			(3, 'Campaign X', 'Marketing', '2023-02-01', '2023-04-30'),
			(4, 'Sales Push', 'Sales', '2023-01-15', '2023-03-31')`,
		`CREATE TABLE assignments (
			emp_id INTEGER,
			project_id INTEGER,
			hours_allocated INTEGER,
			PRIMARY KEY(emp_id, project_id)
		)`,
		`INSERT INTO assignments VALUES
			(1, 1, 40), (2, 1, 30), (5, 1, 35),
			(1, 2, 20), (8, 2, 40), (10, 2, 30),
			(6, 3, 25), (7, 3, 35),
			(3, 4, 30), (4, 4, 40), (9, 4, 20)`,
	}
	for _, s := range setup {
		if err := probeExec(db, s); err != nil {
			t.Fatalf("Setup failed: %s: %v", s[:50], err)
		}
	}

	type queryCase struct {
		name string
		sql  string
	}

	queries := []queryCase{
		// Self-join: employees with their managers
		{"self join", `SELECT e.name as employee, m.name as manager
			FROM employees e LEFT JOIN employees m ON e.manager_id = m.id
			ORDER BY e.name`},

		// Correlated subquery: above average salary per dept
		{"correlated subquery", `SELECT name, dept, salary FROM employees e
			WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept = e.dept)
			ORDER BY dept, salary DESC`},

		// CTE with aggregation
		{"CTE aggregation", `WITH dept_stats AS (
			SELECT dept, AVG(salary) as avg_sal, COUNT(*) as cnt
			FROM employees GROUP BY dept
		)
		SELECT e.name, e.salary, ds.avg_sal, e.salary - ds.avg_sal as diff
		FROM employees e JOIN dept_stats ds ON e.dept = ds.dept
		ORDER BY diff DESC`},

		// Window functions: rank within department
		{"window rank", `SELECT name, dept, salary,
			RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank,
			DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as dense_rank,
			ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as row_num
		FROM employees ORDER BY dept, salary DESC`},

		// Window functions: running totals
		{"window running total", `SELECT name, dept, salary,
			SUM(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS UNBOUNDED PRECEDING) as running_total,
			AVG(salary) OVER (PARTITION BY dept ORDER BY hire_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_avg
		FROM employees ORDER BY dept, hire_date`},

		// LAG/LEAD
		{"window lag/lead", `SELECT name, salary,
			LAG(salary, 1) OVER (ORDER BY salary) as prev_salary,
			LEAD(salary, 1) OVER (ORDER BY salary) as next_salary,
			salary - LAG(salary, 1) OVER (ORDER BY salary) as gap
		FROM employees ORDER BY salary`},

		// NTILE
		{"window ntile", `SELECT name, salary,
			NTILE(4) OVER (ORDER BY salary DESC) as quartile
		FROM employees ORDER BY salary DESC`},

		// Multiple JOINs with aggregation
		{"multi join agg", `SELECT e.name, COUNT(DISTINCT a.project_id) as project_count,
			SUM(a.hours_allocated) as total_hours
		FROM employees e
		JOIN assignments a ON e.id = a.emp_id
		JOIN projects p ON a.project_id = p.id
		GROUP BY e.name
		HAVING COUNT(DISTINCT a.project_id) > 1
		ORDER BY total_hours DESC`},

		// EXISTS subquery
		{"exists subquery", `SELECT name FROM employees e
			WHERE EXISTS (SELECT 1 FROM assignments a WHERE a.emp_id = e.id AND a.hours_allocated >= 35)
			ORDER BY name`},

		// NOT EXISTS
		{"not exists", `SELECT name FROM employees e
			WHERE NOT EXISTS (SELECT 1 FROM assignments WHERE emp_id = e.id)
			ORDER BY name`},

		// IN subquery
		{"in subquery", `SELECT name FROM employees
			WHERE dept IN (SELECT dept FROM departments WHERE budget > 250000)
			ORDER BY name`},

		// UNION / INTERSECT / EXCEPT
		{"union", `SELECT name, 'high_salary' as category FROM employees WHERE salary > 110000
			UNION ALL
			SELECT name, 'senior' as category FROM employees WHERE hire_date < '2020-01-01'
			ORDER BY name, category`},

		{"except", `SELECT name FROM employees WHERE dept = 'Engineering'
			EXCEPT
			SELECT e.name FROM employees e JOIN assignments a ON e.id = a.emp_id WHERE a.hours_allocated < 35
			ORDER BY name`},

		// CASE expression
		{"case expression", `SELECT name, salary,
			CASE
				WHEN salary >= 120000 THEN 'Senior'
				WHEN salary >= 100000 THEN 'Mid'
				ELSE 'Junior'
			END as level,
			CASE dept
				WHEN 'Engineering' THEN 'Tech'
				WHEN 'Sales' THEN 'Revenue'
				ELSE 'Support'
			END as division
		FROM employees ORDER BY salary DESC`},

		// Recursive CTE: org chart
		{"recursive CTE", `WITH RECURSIVE org_chart AS (
			SELECT id, name, manager_id, 0 as depth, name as path
			FROM employees WHERE manager_id IS NULL
			UNION ALL
			SELECT e.id, e.name, e.manager_id, oc.depth + 1,
				oc.path || ' -> ' || e.name
			FROM employees e JOIN org_chart oc ON e.manager_id = oc.id
		)
		SELECT name, depth, path FROM org_chart ORDER BY path`},

		// GROUP BY with HAVING and ORDER BY expression
		{"group by having", `SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal,
			MIN(salary) as min_sal, MAX(salary) as max_sal
		FROM employees
		GROUP BY dept
		HAVING AVG(salary) > 90000
		ORDER BY avg_sal DESC`},

		// BETWEEN and LIKE
		{"between and like", `SELECT name, salary, hire_date FROM employees
			WHERE salary BETWEEN 90000 AND 120000
			AND name LIKE '%a%'
			ORDER BY salary`},

		// COALESCE and NULL handling
		{"null handling", `SELECT name,
			COALESCE(manager_id, -1) as mgr,
			NULLIF(dept, 'Marketing') as dept_or_null,
			CASE WHEN manager_id IS NULL THEN 'Top' ELSE 'Report' END as type
		FROM employees ORDER BY name`},

		// Scalar subquery in SELECT
		{"scalar subquery", `SELECT name, salary,
			(SELECT MAX(salary) FROM employees) as max_salary,
			salary * 100.0 / (SELECT SUM(salary) FROM employees) as pct_of_total
		FROM employees ORDER BY pct_of_total DESC`},

		// DISTINCT ON equivalent
		{"distinct aggregation", `SELECT dept, name, salary FROM employees e
			WHERE salary = (SELECT MAX(salary) FROM employees WHERE dept = e.dept)
			ORDER BY dept`},
	}

	t.Log("=== Complex Query Results ===")
	for _, q := range queries {
		cols, rows, err := probeQueryAll(db, q.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", q.name, err)
		} else {
			t.Logf("  %-25s OK (%d rows, cols: %s)", q.name, len(rows), strings.Join(cols, ","))
			// Print first 3 rows for verification
			for i, row := range rows {
				if i >= 3 {
					t.Logf("    ... and %d more rows", len(rows)-3)
					break
				}
				t.Logf("    %v", row)
			}
		}
	}
}

// ============================================================
// TEST 3: Aggregate functions - comprehensive coverage
// ============================================================
func TestProbe_AggregateFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, `CREATE TABLE nums (id INTEGER, val DOUBLE, grp VARCHAR)`)
	probeExec(db, `INSERT INTO nums VALUES
		(1, 10.5, 'A'), (2, 20.3, 'A'), (3, 15.7, 'B'),
		(4, 30.1, 'A'), (5, 25.8, 'B'), (6, 5.2, 'B'),
		(7, 40.0, 'A'), (8, 12.9, 'B'), (9, 18.6, 'A'),
		(10, NULL, 'A'), (11, 0.0, 'B'), (12, -5.5, 'B')`)

	type aggCase struct {
		name string
		sql  string
	}

	aggs := []aggCase{
		// Basic
		{"COUNT", "SELECT COUNT(*), COUNT(val) FROM nums"},
		{"SUM/AVG", "SELECT SUM(val), AVG(val) FROM nums"},
		{"MIN/MAX", "SELECT MIN(val), MAX(val) FROM nums"},
		{"COUNT DISTINCT", "SELECT COUNT(DISTINCT grp) FROM nums"},

		// Statistical
		{"STDDEV", "SELECT STDDEV_POP(val), STDDEV_SAMP(val) FROM nums WHERE val IS NOT NULL"},
		{"VARIANCE", "SELECT VAR_POP(val), VAR_SAMP(val) FROM nums WHERE val IS NOT NULL"},
		{"MEDIAN", "SELECT MEDIAN(val) FROM nums"},
		{"MODE", "SELECT MODE(grp) FROM nums"},
		{"SKEWNESS", "SELECT SKEWNESS(val) FROM nums WHERE val IS NOT NULL"},
		{"KURTOSIS", "SELECT KURTOSIS(val) FROM nums WHERE val IS NOT NULL"},
		{"ENTROPY", "SELECT ENTROPY(grp) FROM nums"},

		// Approximate
		{"APPROX_COUNT_DISTINCT", "SELECT APPROX_COUNT_DISTINCT(grp) FROM nums"},

		// Boolean
		{"BOOL_AND/OR", "SELECT BOOL_AND(val > 0), BOOL_OR(val < 0) FROM nums WHERE val IS NOT NULL"},

		// String agg
		{"STRING_AGG", "SELECT STRING_AGG(grp, ',') FROM nums"},
		{"LIST_AGG", "SELECT LIST(val) FROM nums WHERE val IS NOT NULL"},

		// Conditional
		{"COUNT_IF", "SELECT COUNT_IF(val > 20) FROM nums"},
		{"SUM_IF", "SELECT SUM_IF(val, val > 10) FROM nums"},
		{"AVG_IF", "SELECT AVG_IF(val, grp = 'A') FROM nums"},

		// ARGMIN/ARGMAX
		{"ARGMIN", "SELECT ARGMIN(grp, val) FROM nums WHERE val IS NOT NULL"},
		{"ARGMAX", "SELECT ARGMAX(grp, val) FROM nums WHERE val IS NOT NULL"},

		// MIN_BY/MAX_BY
		{"MIN_BY", "SELECT MIN_BY(grp, val) FROM nums WHERE val IS NOT NULL"},
		{"MAX_BY", "SELECT MAX_BY(grp, val) FROM nums WHERE val IS NOT NULL"},

		// FIRST/LAST
		{"FIRST", "SELECT FIRST(val) FROM nums WHERE val IS NOT NULL"},
		{"ANY_VALUE", "SELECT ANY_VALUE(grp) FROM nums"},

		// Regression
		{"CORR", "SELECT CORR(id, val) FROM nums WHERE val IS NOT NULL"},
		{"COVAR", "SELECT COVAR_POP(id, val), COVAR_SAMP(id, val) FROM nums WHERE val IS NOT NULL"},
		{"REGR", "SELECT REGR_SLOPE(val, id), REGR_INTERCEPT(val, id) FROM nums WHERE val IS NOT NULL"},

		// Grouped
		{"GROUP BY aggs", `SELECT grp, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val)
			FROM nums GROUP BY grp ORDER BY grp`},

		// HAVING with aggregate
		{"HAVING", "SELECT grp, AVG(val) FROM nums GROUP BY grp HAVING AVG(val) > 15"},

		// Histogram
		{"HISTOGRAM", "SELECT HISTOGRAM(grp) FROM nums"},

		// PRODUCT
		{"PRODUCT", "SELECT PRODUCT(val) FROM nums WHERE val > 0 AND val < 30"},

		// BIT aggregates
		{"BIT_AND/OR/XOR", "SELECT BIT_AND(id), BIT_OR(id), BIT_XOR(id) FROM nums"},
	}

	t.Log("=== Aggregate Function Results ===")
	for _, a := range aggs {
		cols, rows, err := probeQueryAll(db, a.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", a.name, err)
		} else {
			if len(rows) > 0 {
				t.Logf("  %-25s OK (cols: %s, first: %v)", a.name, strings.Join(cols, ","), rows[0])
			} else {
				t.Logf("  %-25s OK (empty)", a.name)
			}
		}
	}
}

// ============================================================
// TEST 4: Scalar functions - math, string, date/time
// ============================================================
func TestProbe_ScalarFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type scalarCase struct {
		name string
		sql  string
	}

	scalars := []scalarCase{
		// Math
		{"ABS", "SELECT ABS(-42), ABS(42)"},
		{"ROUND", "SELECT ROUND(3.14159, 2), ROUND(3.5), ROUND(-2.5)"},
		{"CEIL/FLOOR", "SELECT CEIL(3.2), FLOOR(3.8), CEIL(-3.2), FLOOR(-3.8)"},
		{"POWER/SQRT", "SELECT POWER(2, 10), SQRT(144), CBRT(27)"},
		{"LOG", "SELECT LN(2.718281828), LOG10(1000), LOG2(1024)"},
		{"TRIG", "SELECT SIN(0), COS(0), TAN(0), PI()"},
		{"MOD", "SELECT MOD(17, 5), 17 % 5"},
		{"SIGN", "SELECT SIGN(-5), SIGN(0), SIGN(5)"},
		{"GCD/LCM", "SELECT GCD(12, 18), LCM(4, 6)"},
		{"FACTORIAL", "SELECT FACTORIAL(5), FACTORIAL(0)"},
		{"EVEN", "SELECT EVEN(3), EVEN(4), EVEN(-3)"},
		{"E()", "SELECT E()"},

		// String
		{"UPPER/LOWER", "SELECT UPPER('hello'), LOWER('WORLD')"},
		{"LENGTH", "SELECT LENGTH('hello'), LENGTH('')"},
		{"SUBSTRING", "SELECT SUBSTRING('hello world', 7), SUBSTRING('hello', 1, 3)"},
		{"REPLACE", "SELECT REPLACE('hello world', 'world', 'there')"},
		{"TRIM", "SELECT TRIM('  hello  '), LTRIM('  hi'), RTRIM('hi  ')"},
		{"LPAD/RPAD", "SELECT LPAD('42', 5, '0'), RPAD('hi', 6, '!')"},
		{"REVERSE", "SELECT REVERSE('hello')"},
		{"REPEAT", "SELECT REPEAT('ab', 3)"},
		{"CONCAT", "SELECT CONCAT('a', 'b', 'c'), 'x' || 'y' || 'z'"},
		{"CONCAT_WS", "SELECT CONCAT_WS(', ', 'a', 'b', 'c')"},
		{"POSITION", "SELECT POSITION('lo' IN 'hello')"},
		{"SPLIT_PART", "SELECT SPLIT_PART('a.b.c', '.', 2)"},
		{"STARTS_WITH", "SELECT STARTS_WITH('hello', 'hel'), STARTS_WITH('hello', 'xyz')"},
		{"CONTAINS", "SELECT CONTAINS('hello world', 'world'), CONTAINS('hello', 'xyz')"},
		{"LEFT/RIGHT", "SELECT LEFT('hello', 3), RIGHT('hello', 3)"},
		{"INITCAP", "SELECT INITCAP('hello world')"},
		{"ASCII", "SELECT ASCII('A'), ASCII('a')"},
		{"REGEXP_MATCHES", "SELECT REGEXP_MATCHES('hello123', '[0-9]+')"},
		{"REGEXP_REPLACE", "SELECT REGEXP_REPLACE('hello 123 world 456', '[0-9]+', 'NUM')"},
		{"REGEXP_EXTRACT", "SELECT REGEXP_EXTRACT('hello123world', '([0-9]+)', 1)"},

		// Date/Time
		{"CURRENT_DATE", "SELECT CURRENT_DATE"},
		{"CURRENT_TIMESTAMP", "SELECT CURRENT_TIMESTAMP"},
		{"NOW()", "SELECT NOW()"},
		{"DATE_TRUNC", "SELECT DATE_TRUNC('month', TIMESTAMP '2023-07-15 14:30:00')"},
		{"DATE_DIFF", "SELECT DATE_DIFF('day', DATE '2023-01-01', DATE '2023-12-31')"},
		{"EXTRACT", "SELECT EXTRACT(YEAR FROM TIMESTAMP '2023-07-15 14:30:00'), EXTRACT(MONTH FROM DATE '2023-07-15')"},
		{"STRFTIME", "SELECT STRFTIME(TIMESTAMP '2023-07-15 14:30:00', '%Y-%m-%d')"},
		{"DAYNAME", "SELECT DAYNAME(DATE '2023-07-15')"},
		{"MONTHNAME", "SELECT MONTHNAME(DATE '2023-07-15')"},

		// Type casting
		{"CAST int", "SELECT CAST('42' AS INTEGER), CAST(3.14 AS INTEGER)"},
		{"CAST varchar", "SELECT CAST(42 AS VARCHAR), CAST(TRUE AS VARCHAR)"},
		{"CAST decimal", "SELECT CAST(3.14159 AS DECIMAL(5,2))"},
		{"TRY_CAST", "SELECT TRY_CAST('not_a_number' AS INTEGER), TRY_CAST('42' AS INTEGER)"},

		// NULL functions
		{"COALESCE", "SELECT COALESCE(NULL, NULL, 42, 99)"},
		{"NULLIF", "SELECT NULLIF(1, 1), NULLIF(1, 2)"},
		{"IFNULL", "SELECT IFNULL(NULL, 'default'), IFNULL('value', 'default')"},
		{"GREATEST/LEAST", "SELECT GREATEST(1, 5, 3), LEAST(1, 5, 3)"},

		// Bitwise
		{"BIT ops", "SELECT 5 & 3, 5 | 3, 5 # 3, ~5"},
		{"BIT_COUNT", "SELECT BIT_COUNT(255)"},

		// Encoding
		{"BASE64", "SELECT BASE64('hello')"},
	}

	t.Log("=== Scalar Function Results ===")
	for _, s := range scalars {
		cols, rows, err := probeQueryAll(db, s.sql)
		if err != nil {
			t.Logf("  %-20s FAIL: %v", s.name, err)
		} else if len(rows) > 0 {
			t.Logf("  %-20s OK (cols: %s, vals: %v)", s.name, strings.Join(cols, ","), rows[0])
		}
	}
}

// ============================================================
// TEST 5: Data types - complex types, nested structures
// ============================================================
func TestProbe_DataTypes(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type typeCase struct {
		name string
		sql  string
	}

	types := []typeCase{
		// Basic types
		{"BOOLEAN", "SELECT TRUE, FALSE, TRUE AND FALSE, TRUE OR FALSE, NOT TRUE"},
		{"TINYINT", "SELECT CAST(127 AS TINYINT), CAST(-128 AS TINYINT)"},
		{"SMALLINT", "SELECT CAST(32767 AS SMALLINT)"},
		{"INTEGER", "SELECT CAST(2147483647 AS INTEGER)"},
		{"BIGINT", "SELECT CAST(9223372036854775807 AS BIGINT)"},
		{"HUGEINT", "SELECT CAST(170141183460469231731687303715884105727 AS HUGEINT)"},
		{"FLOAT", "SELECT CAST(3.14 AS FLOAT)"},
		{"DOUBLE", "SELECT CAST(3.141592653589793 AS DOUBLE)"},
		{"DECIMAL", "SELECT CAST(12345.67 AS DECIMAL(10,2))"},
		{"VARCHAR", "SELECT CAST('hello world' AS VARCHAR)"},
		{"BLOB", "SELECT '\\x48656C6C6F'::BLOB"},

		// Date/Time types
		{"DATE", "SELECT DATE '2023-07-15'"},
		{"TIME", "SELECT TIME '14:30:00'"},
		{"TIMESTAMP", "SELECT TIMESTAMP '2023-07-15 14:30:00'"},
		{"INTERVAL", "SELECT INTERVAL '1' YEAR + INTERVAL '6' MONTH"},

		// Complex types
		{"LIST literal", "SELECT [1, 2, 3, 4, 5]"},
		{"LIST varchar", "SELECT ['hello', 'world']"},
		{"NESTED LIST", "SELECT [[1,2],[3,4]]"},
		{"LIST ops", "SELECT LIST_VALUE(1,2,3), [1,2,3][2]"},
		{"STRUCT", "SELECT {'name': 'Alice', 'age': 30}"},
		{"NESTED STRUCT", "SELECT {'person': {'name': 'Bob', 'scores': [90, 85, 92]}}"},
		{"MAP", "SELECT MAP {'a': 1, 'b': 2, 'c': 3}"},

		// NULL handling
		{"NULL types", "SELECT NULL::INTEGER, NULL::VARCHAR, NULL::BOOLEAN"},

		// Type checking
		{"TYPEOF", "SELECT TYPEOF(42), TYPEOF('hello'), TYPEOF(3.14), TYPEOF(TRUE)"},

		// Enum type
		{"ENUM", `SELECT * FROM (
			SELECT CAST('red' AS VARCHAR) as color
			UNION ALL SELECT 'green'
			UNION ALL SELECT 'blue'
		)`},

		// JSON type
		{"JSON", "SELECT '{\"name\": \"Alice\", \"age\": 30}'::JSON"},

		// UUID
		{"UUID generate", "SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID"},

		// Array with specific type
		{"ARRAY typed", "SELECT ARRAY[1, 2, 3]"},
	}

	t.Log("=== Data Type Results ===")
	for _, tc := range types {
		cols, rows, err := probeQueryAll(db, tc.sql)
		if err != nil {
			t.Logf("  %-20s FAIL: %v", tc.name, err)
		} else if len(rows) > 0 {
			t.Logf("  %-20s OK (cols: %s, vals: %v)", tc.name, strings.Join(cols, ","), rows[0])
		}
	}
}

// ============================================================
// TEST 6: Transactions - isolation levels, savepoints, conflicts
// ============================================================
func TestProbe_Transactions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance DECIMAL(10,2))")
	probeExec(db, "INSERT INTO accounts VALUES (1, 1000.00), (2, 2000.00), (3, 3000.00)")

	t.Log("=== Transaction Results ===")

	// Test 1: Basic commit
	{
		tx, err := db.Begin()
		if err != nil {
			t.Logf("  %-30s FAIL: %v", "begin tx", err)
		} else {
			_, err = tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
			t.Logf("  %-30s err=%v", "update in tx", err)
			_, err = tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
			t.Logf("  %-30s err=%v", "update 2 in tx", err)
			err = tx.Commit()
			t.Logf("  %-30s err=%v", "commit", err)

			val, _ := probeQueryVal(db, "SELECT balance FROM accounts WHERE id = 1")
			t.Logf("  %-30s balance=%v", "after commit acct 1", val)
			val, _ = probeQueryVal(db, "SELECT balance FROM accounts WHERE id = 2")
			t.Logf("  %-30s balance=%v", "after commit acct 2", val)
		}
	}

	// Test 2: Rollback
	{
		tx, err := db.Begin()
		if err == nil {
			tx.Exec("UPDATE accounts SET balance = 0 WHERE id = 1")
			err = tx.Rollback()
			t.Logf("  %-30s err=%v", "rollback", err)

			val, _ := probeQueryVal(db, "SELECT balance FROM accounts WHERE id = 1")
			t.Logf("  %-30s balance=%v (should be 900)", "after rollback acct 1", val)
		}
	}

	// Test 3: Savepoints
	{
		tx, err := db.Begin()
		if err == nil {
			tx.Exec("UPDATE accounts SET balance = 500 WHERE id = 1")
			_, err = tx.Exec("SAVEPOINT sp1")
			t.Logf("  %-30s err=%v", "savepoint sp1", err)

			tx.Exec("UPDATE accounts SET balance = 0 WHERE id = 1")
			_, err = tx.Exec("ROLLBACK TO SAVEPOINT sp1")
			t.Logf("  %-30s err=%v", "rollback to sp1", err)

			// Should be 500, not 0
			var bal any
			tx.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&bal)
			t.Logf("  %-30s balance=%v (should be 500)", "in tx after sp rollback", bal)

			err = tx.Commit()
			t.Logf("  %-30s err=%v", "commit after savepoint", err)

			val, _ := probeQueryVal(db, "SELECT balance FROM accounts WHERE id = 1")
			t.Logf("  %-30s balance=%v", "final acct 1", val)
		}
	}

	// Test 4: Isolation levels via raw SQL
	isolationLevels := []string{
		"READ UNCOMMITTED",
		"READ COMMITTED",
		"REPEATABLE READ",
		"SERIALIZABLE",
	}
	for _, level := range isolationLevels {
		err := probeExec(db, fmt.Sprintf("BEGIN TRANSACTION ISOLATION LEVEL %s", level))
		if err != nil {
			t.Logf("  %-30s FAIL: %v", "isolation "+level, err)
		} else {
			t.Logf("  %-30s OK", "isolation "+level)
			probeExec(db, "COMMIT")
		}
	}

	// Test 5: Show isolation level
	val, err := probeQueryVal(db, "SHOW transaction_isolation")
	t.Logf("  %-30s val=%v err=%v", "show isolation", val, err)
}

// ============================================================
// TEST 7: Concurrent access
// ============================================================
func TestProbe_Concurrency(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, "CREATE TABLE counter (id INTEGER PRIMARY KEY, val INTEGER)")
	probeExec(db, "INSERT INTO counter VALUES (1, 0)")

	t.Log("=== Concurrency Results ===")

	// Concurrent reads
	var wg sync.WaitGroup
	errors := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errors[idx] = probeQueryVal(db, "SELECT val FROM counter WHERE id = 1")
		}(i)
	}
	wg.Wait()

	readErrors := 0
	for _, e := range errors {
		if e != nil {
			readErrors++
		}
	}
	t.Logf("  %-30s %d/10 succeeded", "concurrent reads", 10-readErrors)

	// Concurrent writes (sequential via transactions)
	writeErrors := 0
	for i := 0; i < 10; i++ {
		err := probeExec(db, "UPDATE counter SET val = val + 1 WHERE id = 1")
		if err != nil {
			writeErrors++
		}
	}
	val, _ := probeQueryVal(db, "SELECT val FROM counter WHERE id = 1")
	t.Logf("  %-30s %d/10 succeeded, final val=%v", "sequential writes", 10-writeErrors, val)

	// Concurrent writes with goroutines
	probeExec(db, "UPDATE counter SET val = 0 WHERE id = 1")
	var mu sync.Mutex
	concWriteErrors := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := probeExec(db, "UPDATE counter SET val = val + 1 WHERE id = 1")
			if err != nil {
				mu.Lock()
				concWriteErrors++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	val, _ = probeQueryVal(db, "SELECT val FROM counter WHERE id = 1")
	t.Logf("  %-30s %d/10 succeeded, final val=%v", "concurrent writes", 10-concWriteErrors, val)
}

// ============================================================
// TEST 8: Complex real-world analytical queries
// ============================================================
func TestProbe_AnalyticalQueries(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create a realistic sales dataset
	setup := []string{
		`CREATE TABLE sales (
			id INTEGER,
			product VARCHAR,
			category VARCHAR,
			region VARCHAR,
			amount DECIMAL(10,2),
			quantity INTEGER,
			sale_date DATE,
			customer_id INTEGER
		)`,
		`INSERT INTO sales VALUES
			(1, 'Widget A', 'Electronics', 'North', 299.99, 2, '2023-01-15', 101),
			(2, 'Gadget B', 'Electronics', 'South', 149.50, 5, '2023-01-20', 102),
			(3, 'Widget A', 'Electronics', 'East', 299.99, 1, '2023-02-10', 103),
			(4, 'Doohickey', 'Hardware', 'North', 49.99, 10, '2023-02-15', 101),
			(5, 'Thingamajig', 'Hardware', 'West', 79.99, 3, '2023-03-01', 104),
			(6, 'Widget A', 'Electronics', 'North', 299.99, 4, '2023-03-15', 105),
			(7, 'Gadget B', 'Electronics', 'East', 149.50, 2, '2023-04-01', 102),
			(8, 'Whatchamacallit', 'Accessories', 'South', 19.99, 20, '2023-04-10', 106),
			(9, 'Doohickey', 'Hardware', 'North', 49.99, 7, '2023-05-01', 107),
			(10, 'Widget A', 'Electronics', 'South', 299.99, 3, '2023-05-15', 108),
			(11, 'Thingamajig', 'Hardware', 'East', 79.99, 5, '2023-06-01', 103),
			(12, 'Gadget B', 'Electronics', 'West', 149.50, 8, '2023-06-15', 109),
			(13, 'Widget A', 'Electronics', 'North', 299.99, 1, '2023-07-01', 110),
			(14, 'Whatchamacallit', 'Accessories', 'North', 19.99, 15, '2023-07-15', 101),
			(15, 'Doohickey', 'Hardware', 'South', 49.99, 12, '2023-08-01', 111),
			(16, 'Widget A', 'Electronics', 'East', 299.99, 6, '2023-08-15', 112),
			(17, 'Thingamajig', 'Hardware', 'North', 79.99, 4, '2023-09-01', 113),
			(18, 'Gadget B', 'Electronics', 'South', 149.50, 3, '2023-09-15', 102),
			(19, 'Widget A', 'Electronics', 'West', 299.99, 2, '2023-10-01', 114),
			(20, 'Doohickey', 'Hardware', 'East', 49.99, 8, '2023-10-15', 115)`,
	}
	for _, s := range setup {
		if err := probeExec(db, s); err != nil {
			t.Fatalf("Setup: %v", err)
		}
	}

	type analytCase struct {
		name string
		sql  string
	}

	analytics := []analytCase{
		// Revenue by category with running total
		{"revenue by category", `
			WITH monthly AS (
				SELECT DATE_TRUNC('month', sale_date) as month,
					category,
					SUM(amount * quantity) as revenue
				FROM sales GROUP BY 1, 2
			)
			SELECT month, category, revenue,
				SUM(revenue) OVER (PARTITION BY category ORDER BY month) as cumulative
			FROM monthly ORDER BY category, month`},

		// Top products by region
		{"top products by region", `
			SELECT region, product,
				SUM(amount * quantity) as total_rev,
				RANK() OVER (PARTITION BY region ORDER BY SUM(amount * quantity) DESC) as rank
			FROM sales
			GROUP BY region, product
			ORDER BY region, rank`},

		// Customer RFM analysis (Recency, Frequency, Monetary)
		{"customer RFM", `
			SELECT customer_id,
				MAX(sale_date) as last_purchase,
				COUNT(*) as frequency,
				SUM(amount * quantity) as monetary,
				NTILE(3) OVER (ORDER BY MAX(sale_date) DESC) as recency_score,
				NTILE(3) OVER (ORDER BY COUNT(*) DESC) as frequency_score,
				NTILE(3) OVER (ORDER BY SUM(amount * quantity) DESC) as monetary_score
			FROM sales
			GROUP BY customer_id
			ORDER BY monetary DESC`},

		// Month-over-month growth
		{"MoM growth", `
			WITH monthly_rev AS (
				SELECT DATE_TRUNC('month', sale_date) as month,
					SUM(amount * quantity) as revenue
				FROM sales GROUP BY 1
			)
			SELECT month, revenue,
				LAG(revenue) OVER (ORDER BY month) as prev_month,
				CASE WHEN LAG(revenue) OVER (ORDER BY month) > 0
					THEN (revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 / LAG(revenue) OVER (ORDER BY month)
					ELSE NULL
				END as growth_pct
			FROM monthly_rev ORDER BY month`},

		// Pareto analysis (80/20 rule)
		{"pareto analysis", `
			WITH product_rev AS (
				SELECT product, SUM(amount * quantity) as rev
				FROM sales GROUP BY product
			),
			ranked AS (
				SELECT product, rev,
					SUM(rev) OVER (ORDER BY rev DESC) as cumulative,
					SUM(rev) OVER () as total
				FROM product_rev
			)
			SELECT product, rev,
				cumulative,
				ROUND(cumulative * 100.0 / total, 1) as cumulative_pct
			FROM ranked ORDER BY rev DESC`},

		// Cohort analysis
		{"cohort analysis", `
			WITH first_purchase AS (
				SELECT customer_id, MIN(sale_date) as cohort_date
				FROM sales GROUP BY customer_id
			),
			cohort_data AS (
				SELECT fp.cohort_date,
					DATE_TRUNC('month', s.sale_date) as activity_month,
					COUNT(DISTINCT s.customer_id) as active_customers
				FROM sales s
				JOIN first_purchase fp ON s.customer_id = fp.customer_id
				GROUP BY 1, 2
			)
			SELECT * FROM cohort_data ORDER BY cohort_date, activity_month`},

		// Moving average with window frame
		{"7-day moving avg", `
			SELECT sale_date, amount * quantity as daily_rev,
				AVG(amount * quantity) OVER (
					ORDER BY sale_date
					ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
				) as moving_avg,
				SUM(amount * quantity) OVER (
					ORDER BY sale_date
					ROWS UNBOUNDED PRECEDING
				) as running_total
			FROM sales ORDER BY sale_date`},

		// Pivot-like query
		{"pivot by region", `
			SELECT category,
				SUM(CASE WHEN region = 'North' THEN amount * quantity ELSE 0 END) as north_rev,
				SUM(CASE WHEN region = 'South' THEN amount * quantity ELSE 0 END) as south_rev,
				SUM(CASE WHEN region = 'East' THEN amount * quantity ELSE 0 END) as east_rev,
				SUM(CASE WHEN region = 'West' THEN amount * quantity ELSE 0 END) as west_rev,
				SUM(amount * quantity) as total
			FROM sales GROUP BY category ORDER BY total DESC`},

		// Year-to-date vs same period last year (simulated)
		{"YTD comparison", `
			WITH monthly AS (
				SELECT EXTRACT(MONTH FROM sale_date) as month_num,
					SUM(amount * quantity) as revenue
				FROM sales GROUP BY 1
			)
			SELECT month_num, revenue,
				SUM(revenue) OVER (ORDER BY month_num) as ytd,
				revenue - LAG(revenue) OVER (ORDER BY month_num) as vs_prev_month
			FROM monthly ORDER BY month_num`},

		// Top N per group
		{"top 2 per category", `
			WITH ranked_products AS (
				SELECT category, product,
					SUM(amount * quantity) as revenue,
					ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(amount * quantity) DESC) as rn
				FROM sales GROUP BY category, product
			)
			SELECT category, product, revenue
			FROM ranked_products WHERE rn <= 2
			ORDER BY category, revenue DESC`},
	}

	t.Log("=== Analytical Query Results ===")
	for _, a := range analytics {
		cols, rows, err := probeQueryAll(db, a.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", a.name, err)
		} else {
			t.Logf("  %-25s OK (%d rows, cols: %s)", a.name, len(rows), strings.Join(cols, ","))
			for i, row := range rows {
				if i >= 2 {
					break
				}
				t.Logf("    %v", row)
			}
		}
	}
}

// ============================================================
// TEST 9: Edge cases and error handling
// ============================================================
func TestProbe_EdgeCases(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, "CREATE TABLE test_edge (id INTEGER PRIMARY KEY, val VARCHAR, num DOUBLE)")
	probeExec(db, "INSERT INTO test_edge VALUES (1, 'hello', 3.14), (2, NULL, NULL), (3, '', 0.0)")

	type edgeCase struct {
		name      string
		sql       string
		expectErr bool
	}

	edges := []edgeCase{
		// Division by zero
		{"div by zero", "SELECT 1/0", false},
		{"div by zero float", "SELECT 1.0/0.0", false},

		// NaN/Infinity
		{"nan check", "SELECT ISNAN(0.0/0.0)", false},
		{"infinity", "SELECT 1.0/0.0, ISINF(1.0/0.0)", false},

		// Very large numbers
		{"large int", "SELECT 9223372036854775807", false},
		{"large calc", "SELECT POWER(2, 62)", false},

		// Empty string vs NULL
		{"empty vs null", "SELECT '' = '', '' IS NULL, NULL IS NULL, '' = NULL", false},

		// Unicode
		{"unicode", "SELECT LENGTH('héllo'), UPPER('café'), LOWER('MÜNCHEN')", false},
		{"emoji", "SELECT LENGTH('🎉'), '🎉' || '🎊'", false},

		// Type coercion
		{"int + float", "SELECT 1 + 1.5", false},
		{"string concat", "SELECT 'num: ' || 42", false},

		// Duplicate primary key
		{"dup pk", "INSERT INTO test_edge VALUES (1, 'dup', 0)", true},

		// NULL in aggregation
		{"null agg", "SELECT COUNT(*), COUNT(val), COUNT(num), SUM(num), AVG(num) FROM test_edge", false},

		// Empty result set
		{"empty result", "SELECT * FROM test_edge WHERE 1 = 0", false},

		// Very long string
		{"long string", fmt.Sprintf("SELECT LENGTH('%s')", strings.Repeat("x", 10000)), false},

		// Nested function calls
		{"nested funcs", "SELECT UPPER(REVERSE(TRIM('  hello  ')))", false},

		// Multiple NULLs in ORDER BY
		{"null ordering", "SELECT val FROM test_edge ORDER BY val NULLS FIRST", false},
		{"null ordering last", "SELECT val FROM test_edge ORDER BY val NULLS LAST", false},

		// Boolean expressions
		{"bool expr", "SELECT TRUE AND NOT FALSE, FALSE OR TRUE, NOT NOT TRUE", false},

		// Overflow detection
		{"overflow", "SELECT CAST(9999999999999999999999 AS BIGINT)", true},

		// LIMIT 0
		{"limit 0", "SELECT * FROM test_edge LIMIT 0", false},

		// Self-referencing subquery
		{"self ref", "SELECT (SELECT MAX(id) FROM test_edge) - id as diff FROM test_edge", false},

		// Multiple CTEs
		{"multi CTE", `WITH a AS (SELECT 1 as x), b AS (SELECT x + 1 as y FROM a), c AS (SELECT y + 1 as z FROM b) SELECT * FROM c`, false},

		// GENERATE_SERIES
		{"generate_series", "SELECT * FROM generate_series(1, 10)", false},
		{"generate_series step", "SELECT * FROM generate_series(0, 100, 10)", false},

		// String escaping
		{"string escape", "SELECT 'it''s a test'", false},

		// Math edge cases
		{"sqrt negative", "SELECT SQRT(-1)", false},
		{"log zero", "SELECT LN(0)", false},
		{"log negative", "SELECT LN(-1)", false},

		// Aggregate on empty set
		{"agg empty", "SELECT SUM(num), AVG(num), MIN(num), MAX(num) FROM test_edge WHERE 1=0", false},
	}

	t.Log("=== Edge Case Results ===")
	for _, e := range edges {
		_, rows, err := probeQueryAll(db, e.sql)
		if err != nil {
			if e.expectErr {
				t.Logf("  %-25s EXPECTED ERROR: %v", e.name, err)
			} else {
				t.Logf("  %-25s UNEXPECTED FAIL: %v", e.name, err)
			}
		} else {
			if e.expectErr {
				t.Logf("  %-25s SHOULD HAVE FAILED (got %d rows)", e.name, len(rows))
			} else {
				val := ""
				if len(rows) > 0 {
					val = fmt.Sprintf("first: %v", rows[0])
				} else {
					val = "empty"
				}
				t.Logf("  %-25s OK (%s)", e.name, val)
			}
		}
	}
}

// ============================================================
// TEST 10: Prepared statements and parameterized queries
// ============================================================
func TestProbe_PreparedStatements(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, `CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER, active BOOLEAN, score DOUBLE)`)
	probeExec(db, `INSERT INTO users VALUES
		(1, 'Alice', 30, TRUE, 95.5),
		(2, 'Bob', 25, TRUE, 87.3),
		(3, 'Carol', 35, FALSE, 92.1),
		(4, 'Dave', 28, TRUE, 78.9)`)

	t.Log("=== Prepared Statement Results ===")

	// Test various parameter types
	paramTests := []struct {
		name   string
		query  string
		args   []any
	}{
		{"int param", "SELECT name FROM users WHERE age > ?", []any{28}},
		{"string param", "SELECT age FROM users WHERE name = ?", []any{"Alice"}},
		{"bool param", "SELECT name FROM users WHERE active = ?", []any{true}},
		{"float param", "SELECT name FROM users WHERE score > ?", []any{90.0}},
		{"multi params", "SELECT name FROM users WHERE age > ? AND score > ?", []any{25, 80.0}},
		{"null param", "SELECT COALESCE(?, 'default')", []any{nil}},
	}

	for _, pt := range paramTests {
		cols, rows, err := probeQueryAll(db, pt.query)
		if err != nil {
			// Try with Prepare
			stmt, err2 := db.Prepare(pt.query)
			if err2 != nil {
				t.Logf("  %-20s FAIL prepare: %v", pt.name, err2)
				continue
			}
			r, err2 := stmt.Query(pt.args...)
			stmt.Close()
			if err2 != nil {
				t.Logf("  %-20s FAIL query: %v (orig: %v)", pt.name, err2, err)
				continue
			}
			r.Close()
		}
		_ = cols
		_ = rows

		// Now try with prepared statement explicitly
		stmt, err := db.Prepare(pt.query)
		if err != nil {
			t.Logf("  %-20s FAIL prepare: %v", pt.name, err)
			continue
		}
		r, err := stmt.Query(pt.args...)
		if err != nil {
			t.Logf("  %-20s FAIL exec: %v", pt.name, err)
			stmt.Close()
			continue
		}
		count := 0
		for r.Next() {
			count++
		}
		r.Close()
		stmt.Close()
		t.Logf("  %-20s OK (%d rows)", pt.name, count)
	}

	// Test Exec with params
	_, err = db.Exec("INSERT INTO users VALUES (?, ?, ?, ?, ?)", 5, "Eve", 32, true, 88.0)
	t.Logf("  %-20s err=%v", "exec with params", err)

	// Verify
	val, err := probeQueryVal(db, "SELECT COUNT(*) FROM users")
	t.Logf("  %-20s count=%v err=%v", "count after insert", val, err)
}

// ============================================================
// TEST 11: System tables and information schema
// ============================================================
func TestProbe_SystemTables(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, "CREATE TABLE test_sys (id INTEGER PRIMARY KEY, val VARCHAR)")
	probeExec(db, "CREATE INDEX idx_sys ON test_sys(val)")
	probeExec(db, "CREATE VIEW test_view AS SELECT * FROM test_sys")

	queries := []struct {
		name string
		sql  string
	}{
		{"duckdb_tables()", "SELECT * FROM duckdb_tables()"},
		{"duckdb_columns()", "SELECT * FROM duckdb_columns() WHERE table_name = 'test_sys'"},
		{"duckdb_views()", "SELECT * FROM duckdb_views()"},
		{"duckdb_indexes()", "SELECT * FROM duckdb_indexes()"},
		{"duckdb_schemas()", "SELECT * FROM duckdb_schemas()"},
		{"duckdb_types()", "SELECT * FROM duckdb_types() LIMIT 5"},
		{"duckdb_settings()", "SELECT * FROM duckdb_settings() LIMIT 5"},
		{"duckdb_functions()", "SELECT * FROM duckdb_functions() LIMIT 5"},
		{"info_schema tables", "SELECT * FROM information_schema.tables"},
		{"info_schema columns", "SELECT * FROM information_schema.columns WHERE table_name = 'test_sys'"},
		{"info_schema schemata", "SELECT * FROM information_schema.schemata"},
		{"pg_catalog tables", "SELECT * FROM pg_catalog.pg_tables"},
		{"pg_catalog namespace", "SELECT * FROM pg_catalog.pg_namespace"},
		{"version()", "SELECT VERSION()"},
		{"current_database()", "SELECT CURRENT_DATABASE()"},
		{"current_schema()", "SELECT CURRENT_SCHEMA"},
	}

	t.Log("=== System Table Results ===")
	for _, q := range queries {
		cols, rows, err := probeQueryAll(db, q.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", q.name, err)
		} else {
			t.Logf("  %-25s OK (%d rows, cols: %s)", q.name, len(rows), strings.Join(cols, ","))
		}
	}
}

// ============================================================
// TEST 12: generate_series and table-generating functions
// ============================================================
func TestProbe_TableFunctions(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	queries := []struct {
		name string
		sql  string
	}{
		{"generate_series int", "SELECT * FROM generate_series(1, 5)"},
		{"generate_series step", "SELECT * FROM generate_series(0, 100, 25)"},
		{"range", "SELECT * FROM range(1, 6)"},
		{"unnest list", "SELECT unnest([1, 2, 3, 4, 5])"},
		{"unnest in query", "SELECT u.* FROM (SELECT unnest(['a','b','c']) as val) u"},
		{"generate + join", `
			SELECT g.generate_series as n, g.generate_series * g.generate_series as square
			FROM generate_series(1, 10) g`},
		{"fibonacci CTE", `
			WITH RECURSIVE fib(n, a, b) AS (
				SELECT 1, 0, 1
				UNION ALL
				SELECT n+1, b, a+b FROM fib WHERE n < 15
			)
			SELECT n, a as fib_n FROM fib`},
		{"date series", `
			SELECT * FROM generate_series(DATE '2023-01-01', DATE '2023-01-10', INTERVAL '1' DAY)`},
	}

	t.Log("=== Table Function Results ===")
	for _, q := range queries {
		cols, rows, err := probeQueryAll(db, q.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", q.name, err)
		} else {
			t.Logf("  %-25s OK (%d rows, cols: %s)", q.name, len(rows), strings.Join(cols, ","))
			for i, row := range rows {
				if i >= 3 {
					break
				}
				t.Logf("    %v", row)
			}
		}
	}
}

// ============================================================
// TEST 13: COPY operations and data import/export
// ============================================================
func TestProbe_CopyOperations(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, "CREATE TABLE export_test (id INTEGER, name VARCHAR, val DOUBLE)")
	probeExec(db, `INSERT INTO export_test VALUES
		(1, 'Alice', 3.14), (2, 'Bob', 2.72), (3, 'Carol', 1.41)`)

	tmpDir := t.TempDir()

	copies := []struct {
		name string
		sql  string
	}{
		{"COPY TO CSV", fmt.Sprintf("COPY export_test TO '%s/test.csv' (FORMAT CSV, HEADER)", tmpDir)},
		{"COPY TO JSON", fmt.Sprintf("COPY export_test TO '%s/test.json' (FORMAT JSON)", tmpDir)},
		{"COPY TO PARQUET", fmt.Sprintf("COPY export_test TO '%s/test.parquet' (FORMAT PARQUET)", tmpDir)},
		{"COPY query TO CSV", fmt.Sprintf("COPY (SELECT * FROM export_test WHERE val > 2) TO '%s/filtered.csv' (FORMAT CSV, HEADER)", tmpDir)},
	}

	t.Log("=== COPY Operation Results ===")
	for _, c := range copies {
		err := probeExec(db, c.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", c.name, err)
		} else {
			t.Logf("  %-25s OK", c.name)
		}
	}

	// Try reading them back
	reads := []struct {
		name string
		sql  string
	}{
		{"read_csv", fmt.Sprintf("SELECT * FROM read_csv('%s/test.csv', header=true)", tmpDir)},
		{"read_csv_auto", fmt.Sprintf("SELECT * FROM read_csv_auto('%s/test.csv')", tmpDir)},
		{"read_json", fmt.Sprintf("SELECT * FROM read_json('%s/test.json')", tmpDir)},
		{"read_json_auto", fmt.Sprintf("SELECT * FROM read_json_auto('%s/test.json')", tmpDir)},
		{"read_parquet", fmt.Sprintf("SELECT * FROM read_parquet('%s/test.parquet')", tmpDir)},
		{"COPY FROM CSV", fmt.Sprintf(`CREATE TABLE import_csv (id INTEGER, name VARCHAR, val DOUBLE);
			COPY import_csv FROM '%s/test.csv' (FORMAT CSV, HEADER)`, tmpDir)},
	}

	for _, r := range reads {
		_, rows, err := probeQueryAll(db, r.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", r.name, err)
		} else {
			t.Logf("  %-25s OK (%d rows)", r.name, len(rows))
		}
	}
}

// ============================================================
// TEST 14: INSERT variations, UPDATE, DELETE, MERGE
// ============================================================
func TestProbe_DMLOperations(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	probeExec(db, `CREATE TABLE products (
		id INTEGER PRIMARY KEY,
		name VARCHAR NOT NULL,
		price DECIMAL(10,2),
		stock INTEGER DEFAULT 0
	)`)

	t.Log("=== DML Operation Results ===")

	dmls := []struct {
		name string
		sql  string
	}{
		// INSERT variations
		{"basic insert", "INSERT INTO products VALUES (1, 'Widget', 29.99, 100)"},
		{"insert columns", "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 49.99)"},
		{"insert multi", "INSERT INTO products VALUES (3, 'Doohickey', 9.99, 50), (4, 'Thingamajig', 19.99, 75)"},
		{"insert from select", "INSERT INTO products SELECT id + 10, name || ' v2', price * 1.1, stock / 2 FROM products WHERE id <= 2"},
		{"insert default", "INSERT INTO products (id, name) VALUES (5, 'Basic')"},

		// UPDATE
		{"update single", "UPDATE products SET price = 34.99 WHERE id = 1"},
		{"update multi col", "UPDATE products SET price = price * 1.1, stock = stock + 10 WHERE stock > 0"},
		{"update with subquery", "UPDATE products SET price = (SELECT AVG(price) FROM products) WHERE id = 5"},
		{"update all", "UPDATE products SET stock = COALESCE(stock, 0)"},

		// DELETE
		{"delete where", "DELETE FROM products WHERE id > 10"},
		{"delete subquery", "DELETE FROM products WHERE price < (SELECT AVG(price) FROM products WHERE id <= 4)"},
	}

	for _, d := range dmls {
		err := probeExec(db, d.sql)
		if err != nil {
			t.Logf("  %-25s FAIL: %v", d.name, err)
		} else {
			val, _ := probeQueryVal(db, "SELECT COUNT(*) FROM products")
			t.Logf("  %-25s OK (count=%v)", d.name, val)
		}
	}

	// Final state
	_, rows, _ := probeQueryAll(db, "SELECT * FROM products ORDER BY id")
	t.Logf("  Final state: %d products", len(rows))
	for _, r := range rows {
		t.Logf("    %v", r)
	}
}

// ============================================================
// TEST 15: Performance / stress test with larger dataset
// ============================================================
func TestProbe_Performance(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Log("=== Performance Results ===")

	// Bulk insert
	probeExec(db, "CREATE TABLE perf (id INTEGER, val DOUBLE, cat VARCHAR)")

	start := time.Now()
	tx, _ := db.Begin()
	stmt, err := tx.Prepare("INSERT INTO perf VALUES (?, ?, ?)")
	if err != nil {
		t.Logf("  prepare: FAIL: %v", err)
		tx.Rollback()
	} else {
		categories := []string{"A", "B", "C", "D", "E"}
		for i := 0; i < 10000; i++ {
			stmt.Exec(i, float64(i)*math.Pi, categories[i%5])
		}
		stmt.Close()
		tx.Commit()
		elapsed := time.Since(start)
		t.Logf("  %-30s %v", "insert 10k rows", elapsed)
	}

	// Count
	start = time.Now()
	val, _ := probeQueryVal(db, "SELECT COUNT(*) FROM perf")
	t.Logf("  %-30s %v (count=%v)", "count 10k", time.Since(start), val)

	// Full scan aggregation
	start = time.Now()
	_, rows, err := probeQueryAll(db, "SELECT cat, COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM perf GROUP BY cat ORDER BY cat")
	t.Logf("  %-30s %v (rows=%d, err=%v)", "group by agg 10k", time.Since(start), len(rows), err)

	// Sort
	start = time.Now()
	_, _, err = probeQueryAll(db, "SELECT * FROM perf ORDER BY val DESC LIMIT 10")
	t.Logf("  %-30s %v (err=%v)", "sort 10k limit 10", time.Since(start), err)

	// Window function on 10k
	start = time.Now()
	_, _, err = probeQueryAll(db, `SELECT id, val,
		ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val DESC) as rn
		FROM perf ORDER BY cat, rn LIMIT 25`)
	t.Logf("  %-30s %v (err=%v)", "window fn 10k", time.Since(start), err)

	// Join
	probeExec(db, "CREATE TABLE perf2 AS SELECT * FROM perf WHERE id < 1000")
	start = time.Now()
	val, _ = probeQueryVal(db, "SELECT COUNT(*) FROM perf p1 JOIN perf2 p2 ON p1.cat = p2.cat AND p1.id = p2.id")
	t.Logf("  %-30s %v (count=%v)", "join 10k x 1k", time.Since(start), val)

	// Subquery
	start = time.Now()
	_, _, err = probeQueryAll(db, "SELECT * FROM perf WHERE val > (SELECT AVG(val) FROM perf) LIMIT 10")
	t.Logf("  %-30s %v (err=%v)", "subquery filter 10k", time.Since(start), err)

	// Index performance
	probeExec(db, "CREATE INDEX idx_perf_cat ON perf(cat)")
	start = time.Now()
	val, _ = probeQueryVal(db, "SELECT COUNT(*) FROM perf WHERE cat = 'A'")
	t.Logf("  %-30s %v (count=%v)", "index lookup", time.Since(start), val)

	// CTE on large data
	start = time.Now()
	_, _, err = probeQueryAll(db, `
		WITH ranked AS (
			SELECT *, RANK() OVER (PARTITION BY cat ORDER BY val DESC) as rk FROM perf
		)
		SELECT cat, COUNT(*) FROM ranked WHERE rk <= 10 GROUP BY cat ORDER BY cat`)
	t.Logf("  %-30s %v (err=%v)", "CTE + window 10k", time.Since(start), err)
}

// ============================================================
// TEST 16: database/sql interface completeness
// ============================================================
func TestProbe_DatabaseSQLInterface(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Log("=== database/sql Interface Results ===")

	// Ping
	err = db.Ping()
	t.Logf("  %-25s err=%v", "Ping()", err)

	// Stats
	stats := db.Stats()
	t.Logf("  %-25s open=%d, idle=%d, inUse=%d", "Stats()", stats.OpenConnections, stats.Idle, stats.InUse)

	// SetMaxOpenConns
	db.SetMaxOpenConns(5)
	t.Logf("  %-25s OK", "SetMaxOpenConns(5)")

	// SetMaxIdleConns
	db.SetMaxIdleConns(2)
	t.Logf("  %-25s OK", "SetMaxIdleConns(2)")

	// ExecContext
	probeExec(db, "CREATE TABLE iface_test (id INTEGER, val VARCHAR)")
	_, err = db.Exec("INSERT INTO iface_test VALUES (1, 'test')")
	t.Logf("  %-25s err=%v", "Exec()", err)

	// QueryRow
	var id int
	var val string
	err = db.QueryRow("SELECT id, val FROM iface_test WHERE id = 1").Scan(&id, &val)
	t.Logf("  %-25s id=%d val=%s err=%v", "QueryRow().Scan()", id, val, err)

	// Columns metadata
	rows, err := db.Query("SELECT * FROM iface_test")
	if err == nil {
		cols, _ := rows.Columns()
		types, _ := rows.ColumnTypes()
		t.Logf("  %-25s cols=%v", "Columns()", cols)
		for _, ct := range types {
			t.Logf("    col=%s dbType=%s", ct.Name(), ct.DatabaseTypeName())
		}
		rows.Close()
	}

	// Named return values from Exec
	res, err := db.Exec("INSERT INTO iface_test VALUES (2, 'test2')")
	if err == nil {
		lastID, _ := res.LastInsertId()
		affected, _ := res.RowsAffected()
		t.Logf("  %-25s lastID=%d affected=%d", "Result", lastID, affected)
	}

	// Multiple result sets (probably not supported)
	rows, err = db.Query("SELECT 1; SELECT 2")
	if err != nil {
		t.Logf("  %-25s err=%v", "multi statement", err)
	} else {
		t.Logf("  %-25s OK", "multi statement")
		rows.Close()
	}
}

// ============================================================
// SUMMARY: Run all and produce a final report
// ============================================================
func TestProbe_FinalSummary(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Quick feature probe - just check if things work at all
	features := []struct {
		category string
		name     string
		sql      string
	}{
		// DDL
		{"DDL", "CREATE TABLE", "CREATE TABLE s1 (id INT PRIMARY KEY, val VARCHAR)"},
		{"DDL", "CREATE VIEW", "CREATE VIEW v1 AS SELECT 1 as x"},
		{"DDL", "CREATE INDEX", "CREATE INDEX i1 ON s1(val)"},
		{"DDL", "CREATE SEQUENCE", "CREATE SEQUENCE seq1"},
		{"DDL", "CREATE SCHEMA", "CREATE SCHEMA myschema"},
		{"DDL", "ALTER TABLE", "ALTER TABLE s1 ADD COLUMN extra INT"},

		// DML
		{"DML", "INSERT", "INSERT INTO s1 VALUES (1, 'a', NULL)"},
		{"DML", "UPDATE", "UPDATE s1 SET val = 'b' WHERE id = 1"},
		{"DML", "DELETE", "DELETE FROM s1 WHERE id = 1"},

		// Queries
		{"Query", "Basic SELECT", "SELECT 1 + 1"},
		{"Query", "JOIN", "SELECT * FROM s1 CROSS JOIN v1"},
		{"Query", "Subquery", "SELECT (SELECT 42)"},
		{"Query", "CTE", "WITH t AS (SELECT 1 x) SELECT * FROM t"},
		{"Query", "Recursive CTE", "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT * FROM r"},
		{"Query", "Window Function", "SELECT ROW_NUMBER() OVER () FROM v1"},
		{"Query", "UNION", "SELECT 1 UNION ALL SELECT 2"},
		{"Query", "EXCEPT", "SELECT 1 EXCEPT SELECT 2"},
		{"Query", "INTERSECT", "SELECT 1 INTERSECT SELECT 1"},
		{"Query", "CASE", "SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END"},
		{"Query", "BETWEEN", "SELECT 5 BETWEEN 1 AND 10"},
		{"Query", "LIKE", "SELECT 'hello' LIKE 'hel%'"},
		{"Query", "IN list", "SELECT 1 IN (1, 2, 3)"},
		{"Query", "EXISTS", "SELECT EXISTS(SELECT 1)"},
		{"Query", "GENERATE_SERIES", "SELECT * FROM generate_series(1,5)"},
		{"Query", "UNNEST", "SELECT unnest([1,2,3])"},

		// Aggregates
		{"Aggregate", "COUNT/SUM/AVG/MIN/MAX", "SELECT COUNT(1), SUM(1), AVG(1), MIN(1), MAX(1)"},
		{"Aggregate", "STRING_AGG", "SELECT STRING_AGG('a', ',')"},
		{"Aggregate", "MEDIAN", "SELECT MEDIAN(x) FROM (SELECT 1 x UNION ALL SELECT 2 UNION ALL SELECT 3)"},
		{"Aggregate", "HISTOGRAM", "SELECT HISTOGRAM(x) FROM (SELECT 1 x UNION ALL SELECT 1 UNION ALL SELECT 2)"},

		// Functions
		{"Function", "Math", "SELECT ABS(-1), SQRT(4), POWER(2,3), LOG2(8)"},
		{"Function", "String", "SELECT UPPER('hi'), REVERSE('abc'), LENGTH('test')"},
		{"Function", "Date", "SELECT CURRENT_DATE, NOW()"},
		{"Function", "CAST", "SELECT CAST(42 AS VARCHAR)"},
		{"Function", "TRY_CAST", "SELECT TRY_CAST('bad' AS INTEGER)"},
		{"Function", "COALESCE", "SELECT COALESCE(NULL, 42)"},
		{"Function", "TYPEOF", "SELECT TYPEOF(42)"},

		// Types
		{"Type", "LIST", "SELECT [1,2,3]"},
		{"Type", "STRUCT", "SELECT {'a': 1, 'b': 2}"},
		{"Type", "MAP", "SELECT MAP {'x': 10}"},
		{"Type", "DECIMAL", "SELECT CAST(3.14 AS DECIMAL(5,2))"},
		{"Type", "INTERVAL", "SELECT INTERVAL '1' DAY"},
		{"Type", "UUID", "SELECT '550e8400-e29b-41d4-a716-446655440000'::UUID"},
		{"Type", "BOOLEAN", "SELECT TRUE, FALSE"},
		{"Type", "BLOB", "SELECT '\\xDEADBEEF'::BLOB"},

		// Transactions
		{"Transaction", "BEGIN/COMMIT", "BEGIN; SELECT 1; COMMIT"},
		{"Transaction", "Isolation Level", "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE; COMMIT"},

		// System
		{"System", "VERSION()", "SELECT VERSION()"},
		{"System", "duckdb_tables()", "SELECT * FROM duckdb_tables()"},
		{"System", "info_schema", "SELECT * FROM information_schema.tables"},
		{"System", "pg_catalog", "SELECT * FROM pg_catalog.pg_tables"},
	}

	t.Log("╔══════════════════════════════════════════════════════════════╗")
	t.Log("║           dukdb-go IMPLEMENTATION COMPLETENESS              ║")
	t.Log("╠══════════════════════════════════════════════════════════════╣")

	currentCat := ""
	pass, fail, total := 0, 0, 0
	catPass := make(map[string]int)
	catTotal := make(map[string]int)

	for _, f := range features {
		if f.category != currentCat {
			if currentCat != "" {
				t.Logf("║  %-20s %d/%d passed                         ║", currentCat, catPass[currentCat], catTotal[currentCat])
				t.Log("╠──────────────────────────────────────────────────────────────╣")
			}
			currentCat = f.category
			t.Logf("║ %-60s ║", f.category)
		}

		total++
		catTotal[f.category]++

		err := probeExec(db, f.sql)
		if err != nil {
			// Try as query
			_, _, err = probeQueryAll(db, f.sql)
		}

		status := "✓"
		if err != nil {
			status = "✗"
			fail++
		} else {
			pass++
			catPass[f.category]++
		}
		t.Logf("║  %s %-56s ║", status, f.name)
	}
	// Last category
	t.Logf("║  %-20s %d/%d passed                         ║", currentCat, catPass[currentCat], catTotal[currentCat])
	t.Log("╠══════════════════════════════════════════════════════════════╣")
	t.Logf("║  TOTAL: %d/%d features working (%d%%)                       ║", pass, total, pass*100/total)
	t.Logf("║  PASS: %d  FAIL: %d                                       ║", pass, fail)
	t.Log("╚══════════════════════════════════════════════════════════════╝")
}

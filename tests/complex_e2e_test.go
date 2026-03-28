package tests

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
)

func freshDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func queryRows(t *testing.T, db *sql.DB, q string, args ...any) [][]any {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		row := make([]any, len(cols))
		copy(row, vals)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return out
}

// ── 1. Window Functions ───────────────────────────────────────────────────────

func TestCE_WindowFunctions(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE sales (dept TEXT, rep TEXT, amount INT)`)
	salesData := [][]any{
		{"eng", "alice", 500}, {"eng", "bob", 300}, {"eng", "carol", 700},
		{"sales", "dave", 900}, {"sales", "eve", 200}, {"sales", "frank", 600},
		{"hr", "grace", 400}, {"hr", "henry", 350},
	}
	for _, r := range salesData {
		mustExec(t, db, `INSERT INTO sales VALUES (?, ?, ?)`, r...)
	}

	t.Run("rank_within_dept", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT rep, dept, amount, RANK() OVER (PARTITION BY dept ORDER BY amount DESC) AS rnk FROM sales ORDER BY dept, rnk`)
		if len(rows) != 8 {
			t.Errorf("expected 8 rows, got %d", len(rows))
		}
		t.Logf("rank results: %d rows", len(rows))
		for _, r := range rows {
			t.Logf("  %v", r)
		}
	})

	t.Run("running_total", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT dept, amount, SUM(amount) OVER (PARTITION BY dept ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM sales ORDER BY dept, amount`)
		prevDept := ""
		prevRunning := int64(0)
		for _, r := range rows {
			dept := fmt.Sprintf("%v", r[0])
			running := ceToInt64(r[2])
			if dept != prevDept {
				prevDept = dept
				prevRunning = 0
			}
			if running < prevRunning {
				t.Errorf("running total went backwards in dept %s: %d < %d", dept, running, prevRunning)
			}
			prevRunning = running
		}
		t.Logf("running_total: %d rows OK", len(rows))
	})

	t.Run("lag_lead", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT rep, amount, LAG(amount,1,0) OVER (ORDER BY amount) AS lag_v, LEAD(amount,1,0) OVER (ORDER BY amount) AS lead_v FROM sales ORDER BY amount`)
		prev := int64(0)
		for _, r := range rows {
			lagV := ceToInt64(r[2])
			if lagV != prev {
				t.Errorf("lag mismatch: want %d got %d", prev, lagV)
			}
			prev = ceToInt64(r[1])
		}
		t.Logf("lag_lead: %d rows OK", len(rows))
	})

	t.Run("ntile", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT rep, amount, NTILE(3) OVER (ORDER BY amount) AS tile FROM sales ORDER BY amount`)
		for _, r := range rows {
			tile := ceToInt64(r[2])
			if tile < 1 || tile > 3 {
				t.Errorf("ntile out of range: %d", tile)
			}
		}
		t.Logf("ntile: %d rows OK", len(rows))
	})

	t.Run("percent_rank", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT rep, amount, PERCENT_RANK() OVER (ORDER BY amount) AS pct FROM sales ORDER BY amount`)
		for _, r := range rows {
			pct := ceToFloat64(r[2])
			if pct < 0 || pct > 1 {
				t.Errorf("percent_rank out of [0,1]: %f", pct)
			}
		}
		t.Logf("percent_rank: %d rows OK", len(rows))
	})

	t.Run("dense_rank_and_cume_dist", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT rep, amount, DENSE_RANK() OVER (PARTITION BY dept ORDER BY amount) AS dr, CUME_DIST() OVER (ORDER BY amount) AS cd FROM sales ORDER BY dept, amount`)
		for _, r := range rows {
			cd := ceToFloat64(r[3])
			if cd <= 0 || cd > 1 {
				t.Errorf("cume_dist out of (0,1]: %f", cd)
			}
		}
		t.Logf("dense_rank+cume_dist: %d rows OK", len(rows))
	})

	t.Run("first_last_value", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT dept, rep, amount, FIRST_VALUE(amount) OVER (PARTITION BY dept ORDER BY amount) AS first_a, LAST_VALUE(amount) OVER (PARTITION BY dept ORDER BY amount ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_a FROM sales ORDER BY dept, amount`)
		t.Logf("first/last_value: %d rows", len(rows))
		for _, r := range rows {
			t.Logf("  %v", r)
		}
	})
}

// ── 2. Recursive CTEs ─────────────────────────────────────────────────────────

func TestCE_RecursiveCTE(t *testing.T) {
	db := freshDB(t)

	t.Run("fibonacci_10", func(t *testing.T) {
		rows := queryRows(t, db, `WITH RECURSIVE fib(n,a,b) AS (SELECT 1,0,1 UNION ALL SELECT n+1,b,a+b FROM fib WHERE n<10) SELECT n,a FROM fib ORDER BY n`)
		expected := []int64{0, 1, 1, 2, 3, 5, 8, 13, 21, 34}
		if len(rows) != 10 {
			t.Fatalf("expected 10 rows, got %d", len(rows))
		}
		for i, r := range rows {
			if ceToInt64(r[1]) != expected[i] {
				t.Errorf("fib[%d]: want %d got %d", i, expected[i], ceToInt64(r[1]))
			}
		}
		t.Log("fibonacci OK")
	})

	t.Run("org_hierarchy_depth", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE org (id INT, name TEXT, manager_id INT)`)
		mustExec(t, db, `INSERT INTO org VALUES (1,'CEO',NULL),(2,'VP Eng',1),(3,'VP Sales',1),(4,'Lead Dev',2),(5,'Dev',4),(6,'Sales Mgr',3)`)
		rows := queryRows(t, db, `WITH RECURSIVE h(id,name,lvl) AS (SELECT id,name,0 FROM org WHERE manager_id IS NULL UNION ALL SELECT o.id,o.name,h.lvl+1 FROM org o JOIN h ON o.manager_id=h.id) SELECT id,name,lvl FROM h ORDER BY lvl,id`)
		if len(rows) != 6 {
			t.Errorf("expected 6 nodes, got %d", len(rows))
		}
		// CEO at level 0
		if ceToInt64(rows[0][2]) != 0 {
			t.Errorf("CEO should be level 0, got %v", rows[0][2])
		}
		t.Logf("org hierarchy: %d nodes", len(rows))
		for _, r := range rows {
			indent := strings.Repeat("  ", int(ceToInt64(r[2])))
			t.Logf("%s%v (level %v)", indent, r[1], r[2])
		}
	})

	t.Run("path_traversal", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE edges (src INT, dst INT, cost INT)`)
		mustExec(t, db, `INSERT INTO edges VALUES (1,2,1),(2,3,2),(3,4,1),(1,3,5),(2,4,4)`)
		rows := queryRows(t, db, `WITH RECURSIVE paths(src,dst,cost,path) AS (SELECT src,dst,cost,CAST(src AS TEXT)||'->'||CAST(dst AS TEXT) FROM edges WHERE src=1 UNION ALL SELECT p.src,e.dst,p.cost+e.cost,p.path||'->'||CAST(e.dst AS TEXT) FROM paths p JOIN edges e ON p.dst=e.src WHERE p.cost+e.cost<20 AND LENGTH(p.path)<30) SELECT dst,MIN(cost) AS min_cost FROM paths WHERE dst=4 GROUP BY dst`)
		t.Logf("paths to node 4: %v", rows)
		if len(rows) == 0 {
			t.Error("no paths found")
		}
	})
}

// ── 3. Complex Aggregates ─────────────────────────────────────────────────────

func TestCE_Aggregates(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE meas (sensor TEXT, ts INT, value DOUBLE)`)
	for i := 0; i < 100; i++ {
		mustExec(t, db, `INSERT INTO meas VALUES (?,?,?)`, fmt.Sprintf("s%d", i%5), i, math.Sin(float64(i))*100)
	}

	tests := []struct {
		name  string
		query string
	}{
		{"stddev_var", `SELECT sensor, ROUND(AVG(value),4) AS avg, ROUND(STDDEV(value),4) AS sd, ROUND(VARIANCE(value),4) AS var FROM meas GROUP BY sensor ORDER BY sensor`},
		{"percentile_cont", `SELECT sensor, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) AS median, PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY value) AS p90 FROM meas GROUP BY sensor ORDER BY sensor`},
		{"percentile_disc", `SELECT sensor, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY value) AS median_disc FROM meas GROUP BY sensor ORDER BY sensor`},
		{"bool_agg", `SELECT sensor, BOOL_AND(value>-200) AS all_ok, BOOL_OR(value>0) AS some_pos FROM meas GROUP BY sensor ORDER BY sensor`},
		{"string_agg", `SELECT sensor, STRING_AGG(CAST(ts AS TEXT),',') AS ts_list FROM meas WHERE ts<10 GROUP BY sensor ORDER BY sensor`},
		{"filter_agg", `SELECT sensor, COUNT(*) FILTER(WHERE value>0) AS pos_cnt, AVG(value) FILTER(WHERE value>0) AS pos_avg FROM meas GROUP BY sensor ORDER BY sensor`},
		{"approx_distinct", `SELECT COUNT(DISTINCT sensor) AS exact, APPROX_COUNT_DISTINCT(sensor) AS approx FROM meas`},
		{"corr", `SELECT ROUND(CORR(ts,value)::DOUBLE,6) AS correlation FROM meas`},
		{"regr_slope", `SELECT ROUND(REGR_SLOPE(value,ts)::DOUBLE,6) AS slope, ROUND(REGR_INTERCEPT(value,ts)::DOUBLE,6) AS intercept FROM meas`},
		{"mode", `SELECT MODE() WITHIN GROUP (ORDER BY sensor) AS most_common FROM meas`},
		{"entropy", `SELECT ROUND(ENTROPY(sensor)::DOUBLE,4) AS h FROM meas`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %d rows", tt.name, len(rows))
			for _, r := range rows {
				t.Logf("  %v", r)
			}
		})
	}
}

// ── 4. Join Patterns ──────────────────────────────────────────────────────────

func TestCE_Joins(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE customers (id INT, name TEXT, city TEXT)`)
	mustExec(t, db, `CREATE TABLE orders (id INT, customer_id INT, product TEXT, qty INT)`)
	mustExec(t, db, `CREATE TABLE products (name TEXT, price DOUBLE, category TEXT)`)

	for i := 1; i <= 10; i++ {
		mustExec(t, db, `INSERT INTO customers VALUES (?,?,?)`, i, fmt.Sprintf("cust%d", i), []string{"NYC", "LA", "Chicago"}[i%3])
	}
	for _, p := range [][]any{{"widget", 9.99, "hardware"}, {"gadget", 24.99, "hardware"}, {"donut", 1.99, "food"}, {"coffee", 3.49, "food"}, {"book", 12.99, "media"}} {
		mustExec(t, db, `INSERT INTO products VALUES (?,?,?)`, p...)
	}
	prods := []string{"widget", "gadget", "donut", "coffee", "book"}
	for i := 1; i <= 30; i++ {
		mustExec(t, db, `INSERT INTO orders VALUES (?,?,?,?)`, i, (i%10)+1, prods[i%5], (i%5)+1)
	}

	tests := []struct {
		name  string
		query string
		check func([][]any)
	}{
		{
			"multi_join_revenue",
			`SELECT c.city, p.category, ROUND(SUM(o.qty*p.price),2) AS revenue FROM customers c JOIN orders o ON c.id=o.customer_id JOIN products p ON o.product=p.name GROUP BY c.city,p.category ORDER BY revenue DESC`,
			func(rows [][]any) {
				if len(rows) == 0 {
					t.Error("expected revenue rows")
				}
			},
		},
		{
			"left_join_nulls",
			`SELECT c.name, COUNT(o.id) AS cnt FROM customers c LEFT JOIN orders o ON c.id=o.customer_id GROUP BY c.name ORDER BY cnt DESC, c.name`,
			func(rows [][]any) {
				if len(rows) != 10 {
					t.Errorf("expected 10 customers, got %d", len(rows))
				}
			},
		},
		{
			"exists_subquery",
			`SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o JOIN products p ON o.product=p.name WHERE o.customer_id=c.id AND p.category='food') ORDER BY c.name`,
			nil,
		},
		{
			"not_exists",
			`SELECT c.name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id=c.id) ORDER BY c.name`,
			nil,
		},
		{
			"in_subquery",
			`SELECT name FROM products WHERE name IN (SELECT product FROM orders GROUP BY product HAVING SUM(qty)>5) ORDER BY name`,
			nil,
		},
		{
			"correlated_subquery",
			`SELECT c.name, (SELECT ROUND(SUM(o.qty*p.price),2) FROM orders o JOIN products p ON o.product=p.name WHERE o.customer_id=c.id) AS spend FROM customers c ORDER BY spend DESC NULLS LAST`,
			nil,
		},
		{
			"cross_join",
			`SELECT a.name, b.name FROM customers a CROSS JOIN customers b WHERE a.id < b.id AND a.city = b.city ORDER BY a.name, b.name LIMIT 10`,
			nil,
		},
		{
			"full_outer_join",
			`SELECT c.name, o.product FROM customers c FULL OUTER JOIN orders o ON c.id=o.customer_id WHERE c.id IS NULL OR o.id IS NULL LIMIT 10`,
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			if tt.check != nil {
				tt.check(rows)
			}
			t.Logf("%s: %d rows", tt.name, len(rows))
			for i, r := range rows {
				if i < 5 {
					t.Logf("  %v", r)
				}
			}
		})
	}
}

// ── 5. String Functions ───────────────────────────────────────────────────────

func TestCE_StringFunctions(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE texts (id INT, val TEXT)`)
	for i, s := range []string{"Hello, World!", "  spaces  ", "camelCaseText", "snake_case", "UPPERCASE", "mixed123", "café naïve", "tab\there"} {
		mustExec(t, db, `INSERT INTO texts VALUES (?,?)`, i, s)
	}

	tests := []struct{ name, query string }{
		{"trim", `SELECT id, TRIM(val) AS t, LTRIM(val) AS l, RTRIM(val) AS r FROM texts ORDER BY id`},
		{"upper_lower", `SELECT id, UPPER(val) AS up, LOWER(val) AS lo FROM texts ORDER BY id`},
		{"regexp_extract", `SELECT val, REGEXP_EXTRACT(val,'[A-Z][a-z]+') AS cap FROM texts WHERE REGEXP_MATCHES(val,'[A-Z]') = true`},
		{"regexp_replace", `SELECT val, REGEXP_REPLACE(val,'[0-9]+','#') AS cleaned FROM texts`},
		{"string_split", `SELECT val, STRING_SPLIT(val,' ') AS words FROM texts WHERE val LIKE '% %'`},
		{"substr_position", `SELECT val, INSTR(val,'e') AS pos_e, SUBSTR(val,1,5) AS pfx FROM texts`},
		{"lpad_rpad", `SELECT LPAD(CAST(id AS TEXT),4,'0') AS lid, RPAD(TRIM(val),15,'.') AS padded FROM texts`},
		{"levenshtein", `SELECT a.val, b.val, LEVENSHTEIN(a.val,b.val) AS dist FROM texts a JOIN texts b ON a.id<b.id WHERE a.id<3 AND b.id<3`},
		{"starts_with_contains", `SELECT val, STARTS_WITH(val,'H') AS sh, CONTAINS(val,'e') AS ce FROM texts`},
		{"repeat_reverse", `SELECT REPEAT('ab',3) AS rep, REVERSE('hello') AS rev`},
		{"ascii_chr", `SELECT ASCII('A') AS a_val, CHR(65) AS chr_val`},
		{"printf", `SELECT PRINTF('%05d: %s', id, UPPER(TRIM(val))) AS fmt FROM texts LIMIT 3`},
		{"instr", `SELECT val, INSTR(val,'e') AS pos FROM texts`},
		{"split_part", `SELECT SPLIT_PART('a,b,c,d',',',2) AS part`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %d rows", tt.name, len(rows))
			for _, r := range rows {
				t.Logf("  %v", r)
			}
		})
	}
}

// ── 6. Date/Time Functions ────────────────────────────────────────────────────

func TestCE_DateTimeFunctions(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE evts (id INT, ts TIMESTAMP, dt DATE)`)
	for i := 0; i < 20; i++ {
		mustExec(t, db, `INSERT INTO evts VALUES (?,?,?)`, i,
			fmt.Sprintf("2024-%02d-%02d %02d:30:00", (i%12)+1, (i%28)+1, i%24),
			fmt.Sprintf("2024-%02d-%02d", (i%12)+1, (i%28)+1))
	}

	tests := []struct{ name, query string }{
		{"date_trunc_month", `SELECT DATE_TRUNC('month',ts) AS mo, COUNT(*) FROM evts GROUP BY mo ORDER BY mo`},
		{"extract", `SELECT id, EXTRACT(YEAR FROM dt) AS yr, EXTRACT(MONTH FROM dt) AS mo, EXTRACT(DOW FROM ts) AS dow FROM evts LIMIT 5`},
		{"datediff", `SELECT id, DATEDIFF(dt, DATE '2025-01-01') AS diff FROM evts ORDER BY id LIMIT 5`},
		{"date_add_interval", `SELECT id, dt + INTERVAL '30' DAY AS p30, dt - INTERVAL '1' MONTH AS m1m FROM evts LIMIT 5`},
		{"strftime", `SELECT id, STRFTIME(ts,'%Y-%m-%d %H:%M') AS fmt FROM evts LIMIT 5`},
		{"epoch", `SELECT id, EPOCH(ts) AS ep FROM evts LIMIT 5`},
		{"time_bucket", `SELECT TIME_BUCKET(INTERVAL '3 months',dt) AS bkt, COUNT(*) FROM evts GROUP BY bkt ORDER BY bkt`},
		{"last_day", `SELECT id, LAST_DAY(dt) AS ld FROM evts LIMIT 5`},
		{"make_date", `SELECT MAKE_DATE(2024,3,15) AS d`},
		{"year_month_day", `SELECT id, YEAR(dt) AS y, MONTH(dt) AS m, DAY(dt) AS d FROM evts LIMIT 3`},
		{"weekday_quarter", `SELECT id, EXTRACT(DOW FROM dt) AS wd, QUARTER(dt) AS q FROM evts LIMIT 3`},
		{"date_diff_months", `SELECT id, DATEDIFF(DATE '2024-01-01', dt) AS months_since FROM evts LIMIT 5`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %d rows", tt.name, len(rows))
			for _, r := range rows {
				t.Logf("  %v", r)
			}
		})
	}
}

// ── 7. JSON ───────────────────────────────────────────────────────────────────

func TestCE_JSONFunctions(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE jdata (id INT, payload JSON)`)
	for i, j := range []string{
		`{"name":"alice","age":30,"tags":["go","sql"],"addr":{"city":"NYC"}}`,
		`{"name":"bob","age":25,"tags":["python","ml"],"addr":{"city":"LA"}}`,
		`{"name":"carol","age":35,"scores":[95,87,92],"active":true}`,
	} {
		mustExec(t, db, `INSERT INTO jdata VALUES (?,?)`, i, j)
	}

	tests := []struct{ name, query string }{
		{"extract_scalar", `SELECT id, JSON_EXTRACT(payload,'$.name') AS name, JSON_EXTRACT(payload,'$.age') AS age FROM jdata`},
		{"extract_nested", `SELECT id, JSON_EXTRACT(payload,'$.addr.city') AS city FROM jdata WHERE JSON_EXTRACT(payload,'$.addr') IS NOT NULL`},
		{"json_type", `SELECT id, JSON_TYPE(payload) AS rt, JSON_TYPE(payload,'$.age') AS age_t FROM jdata`},
		{"array_length", `SELECT id, JSON_ARRAY_LENGTH(payload,'$.tags') AS tlen FROM jdata WHERE JSON_TYPE(payload,'$.tags')='array'`},
		{"json_keys", `SELECT id, JSON_KEYS(payload) AS ks FROM jdata`},
		{"json_object_agg", `SELECT JSON_OBJECT('cnt',COUNT(*),'avg_age',AVG(CAST(JSON_EXTRACT(payload,'$.age') AS INT))) AS s FROM jdata`},
		{"json_arrow_op", `SELECT id, payload->>'$.name' AS name FROM jdata`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %d rows", tt.name, len(rows))
			for _, r := range rows {
				t.Logf("  %v", r)
			}
		})
	}
}

// ── 8. Array / List Functions ─────────────────────────────────────────────────

func TestCE_ArrayFunctions(t *testing.T) {
	db := freshDB(t)

	tests := []struct {
		name  string
		query string
		skip  string
	}{
		{"literal_array", `SELECT [1,2,3,4,5] AS arr, LIST_AGGREGATE([1,2,3],'count') AS len`, ""},
		{"array_slice", "", "array slice syntax not yet supported"},
		{"array_agg_order", `SELECT LIST_SORT(ARRAY_AGG(v)) AS sorted FROM (SELECT 3 AS v UNION ALL SELECT 1 UNION ALL SELECT 4 UNION ALL SELECT 1 UNION ALL SELECT 5 UNION ALL SELECT 9) t`, ""},
		{"unnest", `SELECT unnest AS v FROM UNNEST([10,20,30,40]) ORDER BY unnest`, ""},
		{"array_contains", `SELECT ARRAY_CONTAINS([1,2,3,4,5],3) AS has3, ARRAY_CONTAINS([1,2,3],9) AS has9`, ""},
		{"array_distinct", `SELECT ARRAY_DISTINCT([1,2,2,3,3,3]) AS dedup`, ""},
		{"array_flatten", `SELECT FLATTEN([[1,2],[3,4],[5]]) AS flat`, ""},
		{"list_sort", `SELECT LIST_SORT([5,3,1,4,2]) AS asc_v, LIST_REVERSE_SORT([5,3,1,4,2]) AS desc_v`, ""},
		{"array_position", `SELECT ARRAY_POSITION([10,20,30,20],20) AS pos`, ""},
		{"list_filter_lambda", `SELECT LIST_FILTER([1,2,3,4,5,6], x -> x%2=0) AS evens`, ""},
		{"list_transform_lambda", `SELECT LIST_TRANSFORM([1,2,3,4,5], x -> x*x) AS squares`, ""},
		{"generate_series", `SELECT * FROM GENERATE_SERIES(1,10,2) ORDER BY 1`, ""},
		{"range", `SELECT * FROM RANGE(0,5) ORDER BY 1`, ""},
		{"array_concat", `SELECT ARRAY_CAT([1,2],[3,4]) AS cat`, ""},
		{"list_reduce", `SELECT LIST_AGGREGATE([1,2,3,4,5],'sum') AS total`, ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %v", tt.name, rows)
		})
	}
}

// ── 9. Struct / Map Types ─────────────────────────────────────────────────────

func TestCE_StructMapTypes(t *testing.T) {
	db := freshDB(t)

	t.Run("struct_literal", func(t *testing.T) {
		t.Skip("struct literal syntax not yet supported")
	})
	t.Run("struct_field_access", func(t *testing.T) {
		t.Skip("struct literal syntax not yet supported")
	})
	t.Run("struct_pack", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT STRUCT_PACK(x:=1,y:=2,label:='point') AS pt`)
		t.Logf("struct_pack: %v", rows)
	})
	t.Run("map_literal", func(t *testing.T) {
		t.Skip("MAP literal syntax not yet supported")
	})
	t.Run("map_access", func(t *testing.T) {
		t.Skip("MAP subscript syntax not yet supported")
	})
	t.Run("map_keys_vals", func(t *testing.T) {
		t.Skip("MAP function syntax needs MAP literal")
	})
	t.Run("row_constructor", func(t *testing.T) {
		t.Skip("ROW() constructor not implemented")
	})
}

// ── 10. PIVOT / UNPIVOT ───────────────────────────────────────────────────────

func TestCE_PivotUnpivot(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE qrev (product TEXT, q TEXT, revenue DOUBLE)`)
	for _, p := range []string{"widgetA", "widgetB"} {
		for i, q := range []string{"Q1", "Q2", "Q3", "Q4"} {
			mustExec(t, db, `INSERT INTO qrev VALUES (?,?,?)`, p, q, float64((i+1)*100))
		}
	}

	t.Run("pivot", func(t *testing.T) {
		t.Skip("PIVOT not yet implemented")
	})

	t.Run("unpivot", func(t *testing.T) {
		t.Skip("UNPIVOT not yet implemented")
	})
}

// ── 11. Transactions & Savepoints ────────────────────────────────────────────

func TestCE_Transactions(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE accts (id INT PRIMARY KEY, bal DOUBLE)`)
	mustExec(t, db, `INSERT INTO accts VALUES (1,1000),(2,500),(3,750)`)

	t.Run("savepoint_partial_rollback", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := tx.Exec(`UPDATE accts SET bal=bal-100 WHERE id=1`); err != nil {
			tx.Rollback()
			t.Fatalf("update1: %v", err)
		}
		if _, err := tx.Exec(`SAVEPOINT sp1`); err != nil {
			tx.Rollback()
			t.Fatalf("savepoint: %v", err)
		}
		if _, err := tx.Exec(`UPDATE accts SET bal=bal-999 WHERE id=3`); err != nil {
			tx.Rollback()
			t.Fatalf("update3: %v", err)
		}
		if _, err := tx.Exec(`ROLLBACK TO SAVEPOINT sp1`); err != nil {
			tx.Rollback()
			t.Fatalf("rollback to: %v", err)
		}
		if _, err := tx.Exec(`UPDATE accts SET bal=bal+50 WHERE id=2`); err != nil {
			tx.Rollback()
			t.Fatalf("update2: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		rows := queryRows(t, db, `SELECT id, bal FROM accts ORDER BY id`)
		// id=1: 1000-100=900, id=2: 500+50=550, id=3: 750 (rolled back)
		expect := map[int64]float64{1: 900, 2: 550, 3: 750}
		for _, r := range rows {
			id := ceToInt64(r[0])
			bal := ceToFloat64(r[1])
			if exp, ok := expect[id]; ok && math.Abs(bal-exp) > 0.001 {
				t.Errorf("acct %d: want %.0f got %.2f", id, exp, bal)
			}
			t.Logf("  id=%d bal=%.2f", id, bal)
		}
	})

	t.Run("rollback_reverts", func(t *testing.T) {
		var before float64
		db.QueryRow(`SELECT bal FROM accts WHERE id=1`).Scan(&before)
		tx, _ := db.Begin()
		tx.Exec(`UPDATE accts SET bal=bal-9999 WHERE id=1`)
		tx.Rollback()
		var after float64
		db.QueryRow(`SELECT bal FROM accts WHERE id=1`).Scan(&after)
		if before != after {
			t.Errorf("rollback failed: %v -> %v", before, after)
		}
		t.Logf("rollback OK: bal=%v", after)
	})

	t.Run("read_committed", func(t *testing.T) {
		if _, err := db.Exec(`BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED`); err != nil {
			t.Fatalf("begin read committed: %v", err)
		}
		defer db.Exec(`ROLLBACK`)
		var bal float64
		db.QueryRow(`SELECT bal FROM accts WHERE id=1`).Scan(&bal)
		t.Logf("read committed bal=%.2f", bal)
	})

	t.Run("serializable", func(t *testing.T) {
		if _, err := db.Exec(`BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE`); err != nil {
			t.Fatalf("begin serializable: %v", err)
		}
		defer db.Exec(`ROLLBACK`)
		var bal float64
		db.QueryRow(`SELECT bal FROM accts WHERE id=2`).Scan(&bal)
		t.Logf("serializable bal=%.2f", bal)
	})
}

// ── 12. COPY / File I/O ───────────────────────────────────────────────────────

func TestCE_CopyIO(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE export_src (id INT, name TEXT, val DOUBLE)`)
	for i := 0; i < 20; i++ {
		mustExec(t, db, `INSERT INTO export_src VALUES (?,?,?)`, i, fmt.Sprintf("item_%d", i), float64(i)*1.5)
	}

	t.Run("copy_to_csv", func(t *testing.T) {
		if _, err := db.Exec(`COPY export_src TO '/tmp/ce_test.csv' (FORMAT CSV, HEADER TRUE)`); err != nil {
			t.Fatalf("COPY TO CSV: %v", err)
		}
		t.Log("COPY TO CSV OK")
	})

	t.Run("copy_from_csv", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE import_csv (id INT, name TEXT, val DOUBLE)`)
		if _, err := db.Exec(`COPY import_csv FROM '/tmp/ce_test.csv' (FORMAT CSV, HEADER TRUE)`); err != nil {
			t.Fatalf("COPY FROM CSV: %v", err)
		}
		rows := queryRows(t, db, `SELECT COUNT(*), SUM(val) FROM import_csv`)
		cnt := ceToInt64(rows[0][0])
		sum := ceToFloat64(rows[0][1])
		if cnt != 20 {
			t.Errorf("expected 20 rows, got %d", cnt)
		}
		t.Logf("imported %d rows, sum=%.2f", cnt, sum)
	})

	t.Run("read_csv_function", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT COUNT(*) FROM read_csv('/tmp/ce_test.csv', header=true)`)
		t.Logf("read_csv count: %v", rows[0][0])
	})

	t.Run("copy_to_parquet", func(t *testing.T) {
		os.Remove("/tmp/ce_test.parquet")
		if _, err := db.Exec(`COPY export_src TO '/tmp/ce_test.parquet' (FORMAT PARQUET)`); err != nil {
			t.Fatalf("COPY TO PARQUET: %v", err)
		}
		t.Log("COPY TO PARQUET OK")
	})

	t.Run("read_parquet_function", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT COUNT(*), SUM(val) FROM read_parquet('/tmp/ce_test.parquet')`)
		cnt := ceToInt64(rows[0][0])
		if cnt != 20 {
			t.Errorf("expected 20 rows from parquet, got %d", cnt)
		}
		t.Logf("read_parquet: count=%d sum=%.2f", cnt, ceToFloat64(rows[0][1]))
	})

	t.Run("copy_query_filtered", func(t *testing.T) {
		if _, err := db.Exec(`COPY (SELECT * FROM export_src WHERE val > 15) TO '/tmp/ce_filtered.csv' (FORMAT CSV, HEADER TRUE)`); err != nil {
			t.Fatalf("COPY query: %v", err)
		}
		rows := queryRows(t, db, `SELECT COUNT(*) FROM read_csv('/tmp/ce_filtered.csv', header=true)`)
		t.Logf("filtered copy: %v rows", rows[0][0])
	})

	t.Run("copy_to_json", func(t *testing.T) {
		if _, err := db.Exec(`COPY export_src TO '/tmp/ce_test.json' (FORMAT JSON)`); err != nil {
			t.Fatalf("COPY TO JSON: %v", err)
		}
		rows := queryRows(t, db, `SELECT COUNT(*) FROM read_json('/tmp/ce_test.json')`)
		t.Logf("read_json count: %v", rows[0][0])
	})
}

// ── 13. Full-Text Search ──────────────────────────────────────────────────────

func TestCE_FullTextSearch(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE docs (id INT, title TEXT, body TEXT)`)
	for i, row := range [][]any{
		{1, "Go Programming", "Go is a statically typed compiled language designed at Google"},
		{2, "Rust Safety", "Rust provides memory safety without garbage collection"},
		{3, "Python Data Science", "Python is popular for data analysis and machine learning"},
		{4, "Database Design", "Good database design requires understanding of normalization"},
		{5, "SQL Optimization", "Understanding query plans helps optimize SQL performance"},
		{6, "Go Concurrency", "Go goroutines and channels make concurrency simple and efficient"},
	} {
		_ = i
		mustExec(t, db, `INSERT INTO docs VALUES (?,?,?)`, row...)
	}

	t.Run("create_fts", func(t *testing.T) {
		if _, err := db.Exec(`PRAGMA create_fts_index('docs','id','title','body')`); err != nil {
			t.Fatalf("create_fts_index: %v", err)
		}
		t.Log("FTS index created")
	})

	t.Run("bm25_search", func(t *testing.T) {
		t.Skip("fts dotted function call syntax not yet supported by parser")
	})

	t.Run("bm25_go_specific", func(t *testing.T) {
		t.Skip("fts dotted function call syntax not yet supported by parser")
	})
}

// ── 14. Math Functions ────────────────────────────────────────────────────────

func TestCE_MathFunctions(t *testing.T) {
	db := freshDB(t)

	tests := []struct {
		name  string
		query string
	}{
		{"trig", `SELECT ROUND(SIN(PI()/6)::DOUBLE,6) AS sin30, ROUND(COS(PI()/3)::DOUBLE,6) AS cos60, ROUND(TAN(PI()/4)::DOUBLE,6) AS tan45`},
		{"log_exp", `SELECT ROUND(LOG(100)::DOUBLE,6) AS log10, ROUND(LOG2(8)::DOUBLE,6) AS log2_8, ROUND(LN(EXP(1))::DOUBLE,6) AS ln_e`},
		{"power_sqrt", `SELECT POWER(2,10) AS p2_10, SQRT(144) AS r12, CBRT(27) AS c3`},
		{"ceil_floor_round", `SELECT CEIL(3.2) AS c, FLOOR(3.8) AS f, ROUND(3.567,2) AS r, TRUNC(3.999) AS tr`},
		{"bitwise", `SELECT 5 & 3 AS band, 5|3 AS bor, ~5 AS bnot`},
		{"greatest_least", `SELECT GREATEST(3,1,4,1,5,9) AS g, LEAST(3,1,4,1,5,9) AS l`},
		{"isnan_isinf", `SELECT ISNAN('NaN'::DOUBLE) AS nan_check, ISINF('Infinity'::DOUBLE) AS inf_check`},
		{"abs_sign", `SELECT ABS(-42) AS a, SIGN(-5) AS sn, SIGN(0) AS sz, SIGN(7) AS sp`},
		{"factorial", `SELECT FACTORIAL(5) AS f5, FACTORIAL(10) AS f10`},
		{"gcd_lcm", `SELECT GCD(12,8) AS gcd, LCM(4,6) AS lcm`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %v", tt.name, rows)
		})
	}
}

// ── 15. ENUM Type ─────────────────────────────────────────────────────────────

func TestCE_EnumType(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TYPE status AS ENUM ('active','inactive','pending')`)
	mustExec(t, db, `CREATE TABLE users (id INT, name TEXT, st status)`)
	mustExec(t, db, `INSERT INTO users VALUES (1,'alice','active'),(2,'bob','inactive'),(3,'carol','pending')`)

	t.Run("enum_filter", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT name, st FROM users WHERE st='active' ORDER BY name`)
		if len(rows) != 1 {
			t.Errorf("expected 1 active, got %d", len(rows))
		}
		t.Logf("active users: %v", rows)
	})

	t.Run("enum_group_by", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT st, COUNT(*) FROM users GROUP BY st ORDER BY st`)
		t.Logf("enum groupby: %v", rows)
	})

	t.Run("enum_ordering", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT name, st FROM users ORDER BY st, name`)
		t.Logf("ordered by enum: %v", rows)
	})
}

// ── 16. LATERAL Joins ─────────────────────────────────────────────────────────

func TestCE_LateralJoin(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE teams (id INT, name TEXT)`)
	mustExec(t, db, `INSERT INTO teams VALUES (1,'alpha'),(2,'beta'),(3,'gamma')`)
	mustExec(t, db, `CREATE TABLE scores (team_id INT, player TEXT, score INT)`)
	for i := 0; i < 30; i++ {
		mustExec(t, db, `INSERT INTO scores VALUES (?,?,?)`, (i%3)+1, fmt.Sprintf("p%d", i), (i*7+3)%100)
	}

	t.Run("lateral_top2_per_team", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT t.name, top.player, top.score FROM teams t, LATERAL (SELECT player, score FROM scores WHERE team_id=t.id ORDER BY score DESC LIMIT 2) top ORDER BY t.name, top.score DESC`)
		t.Logf("lateral top2: %d rows", len(rows))
		for _, r := range rows {
			t.Logf("  %v", r)
		}
	})

	t.Run("lateral_unnest_split", func(t *testing.T) {
		t.Skip("UNNEST as scalar function not yet supported")
	})
}

// ── 17. Generated Columns & Constraints ──────────────────────────────────────

func TestCE_GeneratedColumnsConstraints(t *testing.T) {
	db := freshDB(t)

	t.Run("virtual_generated", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE prices (id INT, price DOUBLE, tax DOUBLE DEFAULT 0.1, total DOUBLE GENERATED ALWAYS AS (price*(1+tax)) VIRTUAL, label TEXT, label_up TEXT GENERATED ALWAYS AS (UPPER(label)) VIRTUAL)`)
		mustExec(t, db, `INSERT INTO prices(id,price,label) VALUES (1,100,'widget')`)
		mustExec(t, db, `INSERT INTO prices(id,price,tax,label) VALUES (2,50,0.2,'gadget')`)
		rows := queryRows(t, db, `SELECT id, price, tax, total, label_up FROM prices ORDER BY id`)
		for _, r := range rows {
			price := ceToFloat64(r[1])
			tax := ceToFloat64(r[2])
			total := ceToFloat64(r[3])
			want := price * (1 + tax)
			if math.Abs(total-want) > 0.001 {
				t.Errorf("generated total: want %.4f got %.4f", want, total)
			}
			t.Logf("  id=%v price=%.2f tax=%.2f total=%.2f label=%v", r[0], price, tax, total, r[4])
		}
	})

	t.Run("check_constraint", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE bounded (id INT, pct DOUBLE CHECK(pct>=0 AND pct<=100))`)
		mustExec(t, db, `INSERT INTO bounded VALUES (1,50)`)
		mustExec(t, db, `INSERT INTO bounded VALUES (2,0)`)
		mustExec(t, db, `INSERT INTO bounded VALUES (3,100)`)
		if _, err := db.Exec(`INSERT INTO bounded VALUES (4,101)`); err == nil {
			t.Error("expected CHECK violation for 101")
		} else {
			t.Logf("CHECK correctly rejected 101: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO bounded VALUES (5,-1)`); err == nil {
			t.Error("expected CHECK violation for -1")
		} else {
			t.Logf("CHECK correctly rejected -1: %v", err)
		}
	})

	t.Run("unique_constraint", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE unique_test (id INT, code TEXT UNIQUE)`)
		mustExec(t, db, `INSERT INTO unique_test VALUES (1,'AAA')`)
		if _, err := db.Exec(`INSERT INTO unique_test VALUES (2,'AAA')`); err == nil {
			t.Error("expected UNIQUE violation")
		} else {
			t.Logf("UNIQUE correctly rejected duplicate: %v", err)
		}
	})

	t.Run("primary_key", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE pk_test (id INT PRIMARY KEY, name TEXT)`)
		mustExec(t, db, `INSERT INTO pk_test VALUES (1,'alice')`)
		if _, err := db.Exec(`INSERT INTO pk_test VALUES (1,'bob')`); err == nil {
			t.Error("expected PK violation")
		} else {
			t.Logf("PK correctly rejected duplicate: %v", err)
		}
	})
}

// ── 18. Prepared Statements & Type Params ────────────────────────────────────

func TestCE_PreparedStatements(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE prep (id INT, name TEXT, score DOUBLE)`)

	t.Run("batch_1000", func(t *testing.T) {
		stmt, err := db.Prepare(`INSERT INTO prep VALUES (?,?,?)`)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		defer stmt.Close()
		for i := 0; i < 1000; i++ {
			if _, err := stmt.Exec(i, fmt.Sprintf("n_%d", i), float64(i)*0.7); err != nil {
				t.Fatalf("exec[%d]: %v", i, err)
			}
		}
		rows := queryRows(t, db, `SELECT COUNT(*) FROM prep`)
		if ceToInt64(rows[0][0]) != 1000 {
			t.Errorf("expected 1000, got %v", rows[0][0])
		}
		t.Log("batch insert 1000 OK")
	})

	t.Run("parameterized_range_query", func(t *testing.T) {
		stmt, err := db.Prepare(`SELECT id, name, score FROM prep WHERE score>? AND score<? ORDER BY score LIMIT ?`)
		if err != nil {
			t.Fatalf("prepare select: %v", err)
		}
		defer stmt.Close()
		rows, err := stmt.Query(100.0, 200.0, 5)
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var id int
			var name string
			var score float64
			rows.Scan(&id, &name, &score)
			if score <= 100 || score >= 200 {
				t.Errorf("score out of range: %f", score)
			}
			count++
		}
		t.Logf("parameterized range: %d rows", count)
	})

	t.Run("all_types", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE typed (a BOOLEAN, b TINYINT, c SMALLINT, d INT, e BIGINT, f FLOAT, g DOUBLE, h TEXT, i BLOB)`)
		if _, err := db.Exec(`INSERT INTO typed VALUES (?,?,?,?,?,?,?,?,?)`,
			true, int8(127), int16(32767), int32(1<<20), int64(1<<40),
			float32(3.14), 2.71828, "hello", []byte{0xDE, 0xAD, 0xBE, 0xEF}); err != nil {
			t.Fatalf("all-types insert: %v", err)
		}
		rows := queryRows(t, db, `SELECT * FROM typed`)
		t.Logf("all types row: %v", rows[0])
	})
}

// ── 19. Schema / Catalog ──────────────────────────────────────────────────────

func TestCE_SchemaCatalog(t *testing.T) {
	db := freshDB(t)

	t.Run("multi_schema_query", func(t *testing.T) {
		mustExec(t, db, `CREATE SCHEMA app`)
		mustExec(t, db, `CREATE SCHEMA audit`)
		mustExec(t, db, `CREATE TABLE app.users (id INT, name TEXT)`)
		mustExec(t, db, `CREATE TABLE audit.events (ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, action TEXT)`)
		mustExec(t, db, `INSERT INTO app.users VALUES (1,'alice'),(2,'bob')`)
		mustExec(t, db, `INSERT INTO audit.events(action) VALUES ('login')`)
		rows := queryRows(t, db, `SELECT u.name FROM app.users u ORDER BY u.name`)
		if len(rows) != 2 {
			t.Errorf("expected 2 users, got %d", len(rows))
		}
		t.Logf("multi-schema: %v", rows)
	})

	t.Run("information_schema_tables", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog') ORDER BY table_schema, table_name`)
		t.Logf("tables in catalog: %d", len(rows))
		for _, r := range rows {
			t.Logf("  %v.%v (%v)", r[0], r[1], r[2])
		}
	})

	t.Run("information_schema_columns", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema='app' ORDER BY table_name, ordinal_position`)
		t.Logf("app schema columns: %v", rows)
	})

	t.Run("views", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE raw (id INT, x DOUBLE, y DOUBLE)`)
		for i := 0; i < 20; i++ {
			mustExec(t, db, `INSERT INTO raw VALUES (?,?,?)`, i, float64(i)*1.1, float64(i)*2.2)
		}
		mustExec(t, db, `CREATE VIEW derived AS SELECT id, x, y, x+y AS sum_xy, ROUND(SQRT(x*x+y*y),4) AS dist FROM raw WHERE id>5`)
		rows := queryRows(t, db, `SELECT COUNT(*) FROM derived WHERE dist>10`)
		t.Logf("view query result: %v", rows[0][0])
		mustExec(t, db, `CREATE OR REPLACE VIEW derived AS SELECT id, x+y AS sum_xy FROM raw WHERE id>10`)
		rows2 := queryRows(t, db, `SELECT COUNT(*) FROM derived`)
		t.Logf("replaced view result: %v", rows2[0][0])
	})

	t.Run("alter_table_ops", func(t *testing.T) {
		mustExec(t, db, `CREATE TABLE alterable (id INT, name TEXT, old_col INT)`)
		mustExec(t, db, `INSERT INTO alterable VALUES (1,'alice',42)`)
		mustExec(t, db, `ALTER TABLE alterable ADD COLUMN new_col TEXT DEFAULT 'default'`)
		mustExec(t, db, `ALTER TABLE alterable DROP COLUMN old_col`)
		mustExec(t, db, `ALTER TABLE alterable RENAME COLUMN name TO full_name`)
		rows := queryRows(t, db, `SELECT id, full_name, new_col FROM alterable`)
		t.Logf("after ALTER: %v", rows)
	})

	t.Run("sequences", func(t *testing.T) {
		mustExec(t, db, `CREATE SEQUENCE myseq START WITH 100 INCREMENT BY 5`)
		mustExec(t, db, `CREATE TABLE seqtbl (id INT DEFAULT NEXTVAL('myseq'), v TEXT)`)
		for _, v := range []string{"a", "b", "c", "d"} {
			mustExec(t, db, `INSERT INTO seqtbl(v) VALUES (?)`, v)
		}
		rows := queryRows(t, db, `SELECT id, v FROM seqtbl ORDER BY id`)
		prev := int64(95)
		for _, r := range rows {
			id := ceToInt64(r[0])
			if id != prev+5 {
				t.Errorf("seq: expected %d got %d", prev+5, id)
			}
			prev = id
		}
		t.Logf("sequence rows: %v", rows)
		var curr int64
		db.QueryRow(`SELECT CURRVAL('myseq')`).Scan(&curr)
		if curr != 115 {
			t.Errorf("currval: expected 115 got %d", curr)
		}
	})
}

// ── 20. GROUPING SETS / ROLLUP / CUBE ─────────────────────────────────────────

func TestCE_GroupingSetsRollupCube(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE gsdata (region TEXT, product TEXT, quarter TEXT, sales INT)`)
	for _, region := range []string{"North", "South", "East"} {
		for _, prod := range []string{"A", "B"} {
			for i, q := range []string{"Q1", "Q2", "Q3", "Q4"} {
				mustExec(t, db, `INSERT INTO gsdata VALUES (?,?,?,?)`, region, prod, q, (i+1)*100)
			}
		}
	}

	t.Run("rollup", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT region, product, SUM(sales) AS total FROM gsdata GROUP BY ROLLUP(region, product) ORDER BY region NULLS LAST, product NULLS LAST`)
		t.Logf("ROLLUP: %d rows", len(rows))
		for _, r := range rows {
			t.Logf("  %v", r)
		}
	})

	t.Run("cube", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT region, product, SUM(sales) AS total FROM gsdata GROUP BY CUBE(region, product) ORDER BY region NULLS LAST, product NULLS LAST`)
		t.Logf("CUBE: %d rows", len(rows))
	})

	t.Run("grouping_sets", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT region, product, SUM(sales) AS total, GROUPING(region) AS gr, GROUPING(product) AS gp FROM gsdata GROUP BY GROUPING SETS((region),(product),()) ORDER BY gr,gp,region NULLS LAST,product NULLS LAST`)
		t.Logf("GROUPING SETS: %d rows", len(rows))
	})
}

// ── 21. QUALIFY Clause ────────────────────────────────────────────────────────

func TestCE_QualifyClause(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE qdata (dept TEXT, name TEXT, salary INT)`)
	for _, r := range [][]any{{"eng", "alice", 100}, {"eng", "bob", 200}, {"eng", "carol", 150}, {"sales", "dave", 180}, {"sales", "eve", 120}} {
		mustExec(t, db, `INSERT INTO qdata VALUES (?,?,?)`, r...)
	}

	t.Run("top1_per_dept", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT dept, name, salary FROM qdata QUALIFY RANK() OVER (PARTITION BY dept ORDER BY salary DESC) = 1`)
		if len(rows) != 2 {
			t.Errorf("expected 2 top earners, got %d", len(rows))
		}
		t.Logf("QUALIFY top1: %v", rows)
	})

	t.Run("above_dept_avg", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT dept, name, salary FROM qdata QUALIFY salary > AVG(salary) OVER (PARTITION BY dept) ORDER BY dept, salary`)
		t.Logf("QUALIFY above avg: %d rows", len(rows))
		for _, r := range rows {
			t.Logf("  %v", r)
		}
	})
}

// ── 22. ATTACH / DETACH ───────────────────────────────────────────────────────

func TestCE_AttachDetach(t *testing.T) {
	db := freshDB(t)

	t.Run("attach_memory_db", func(t *testing.T) {
		if _, err := db.Exec(`ATTACH ':memory:' AS secondary`); err != nil {
			t.Fatalf("ATTACH: %v", err)
		}
		mustExec(t, db, `CREATE TABLE secondary.remote (id INT, val TEXT)`)
		mustExec(t, db, `INSERT INTO secondary.remote VALUES (1,'hello'),(2,'world')`)
		rows := queryRows(t, db, `SELECT id, val FROM secondary.remote ORDER BY id`)
		if len(rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(rows))
		}
		t.Logf("attached db rows: %v", rows)
		if _, err := db.Exec(`DETACH secondary`); err != nil {
			t.Fatalf("DETACH: %v", err)
		}
		t.Log("ATTACH/DETACH OK")
	})
}

// ── 23. Large-Scale Sort & Aggregation Correctness ────────────────────────────

func TestCE_LargeScaleCorrectness(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE big (id INT, grp INT, val DOUBLE)`)
	stmt, _ := db.Prepare(`INSERT INTO big VALUES (?,?,?)`)
	for i := 0; i < 10000; i++ {
		stmt.Exec(i, i%100, float64(i)*math.Pi)
	}
	stmt.Close()

	t.Run("group_by_agg_100_groups", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT grp, COUNT(*) AS cnt, MIN(val) AS lo, MAX(val) AS hi FROM big GROUP BY grp ORDER BY grp`)
		if len(rows) != 100 {
			t.Errorf("expected 100 groups, got %d", len(rows))
		}
		prevGrp := int64(-1)
		for _, r := range rows {
			grp := ceToInt64(r[0])
			cnt := ceToInt64(r[1])
			if grp <= prevGrp {
				t.Errorf("not sorted: %d after %d", grp, prevGrp)
			}
			if cnt != 100 {
				t.Errorf("grp %d: want 100 rows, got %d", grp, cnt)
			}
			prevGrp = grp
		}
		t.Log("100-group aggregation correct")
	})

	t.Run("distinct_large", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT DISTINCT grp FROM big ORDER BY grp`)
		if len(rows) != 100 {
			t.Errorf("expected 100 distinct, got %d", len(rows))
		}
		grps := make([]int, len(rows))
		for i, r := range rows {
			grps[i] = int(ceToInt64(r[0]))
		}
		if !sort.IntsAreSorted(grps) {
			t.Error("DISTINCT not sorted")
		}
		t.Log("DISTINCT large OK")
	})

	t.Run("top_10_percent", func(t *testing.T) {
		rows := queryRows(t, db, `SELECT id, val FROM big WHERE val > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY val) FROM big) ORDER BY val DESC LIMIT 20`)
		t.Logf("top 10%%: %d rows", len(rows))
	})
}

// ── 24. Complex Analytical CTEs ───────────────────────────────────────────────

func TestCE_AnalyticalCTEs(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE activity (user_id INT, event TEXT, ts TIMESTAMP, val DOUBLE)`)
	etypes := []string{"click", "view", "purchase", "share"}
	for i := 0; i < 500; i++ {
		mustExec(t, db, `INSERT INTO activity VALUES (?,?,?,?)`,
			(i%50)+1, etypes[i%4],
			fmt.Sprintf("2024-%02d-%02d %02d:00:00", (i%12)+1, (i%28)+1, i%24),
			float64(i%100)*0.5)
	}

	tests := []struct{ name, query string }{
		{
			"session_window_analysis",
			`WITH windowed AS (
				SELECT user_id, event, ts, val,
				       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS rn,
				       LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) AS prev_ts,
				       SUM(val) OVER (PARTITION BY user_id ORDER BY ts ROWS UNBOUNDED PRECEDING) AS cum_val
				FROM activity
			)
			SELECT user_id, COUNT(*) AS events, MAX(cum_val) AS total_val
			FROM windowed GROUP BY user_id ORDER BY total_val DESC LIMIT 10`,
		},
		{
			"funnel",
			`SELECT
				COUNT(DISTINCT CASE WHEN event='view' THEN user_id END) AS viewers,
				COUNT(DISTINCT CASE WHEN event='click' THEN user_id END) AS clickers,
				COUNT(DISTINCT CASE WHEN event='purchase' THEN user_id END) AS purchasers,
				ROUND(100.0*COUNT(DISTINCT CASE WHEN event='click' THEN user_id END)/NULLIF(COUNT(DISTINCT CASE WHEN event='view' THEN user_id END),0),1) AS ctr
			FROM activity`,
		},
		{
			"percentile_by_event",
			`SELECT event, COUNT(*) AS cnt,
			        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS med,
			        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY val) AS p95
			 FROM activity GROUP BY event ORDER BY event`,
		},
		{
			"moving_avg_7day",
			`WITH daily AS (SELECT DATE_TRUNC('day',ts) AS day, SUM(val) AS dv FROM activity GROUP BY day)
			 SELECT day, dv, AVG(dv) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma7
			 FROM daily ORDER BY day`,
		},
		{
			"top3_per_event",
			`WITH ranked AS (
				SELECT user_id, event, SUM(val) AS total,
				       RANK() OVER (PARTITION BY event ORDER BY SUM(val) DESC) AS rnk
				FROM activity GROUP BY user_id, event
			)
			SELECT event, user_id, total FROM ranked WHERE rnk<=3 ORDER BY event, rnk`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := queryRows(t, db, tt.query)
			t.Logf("%s: %d rows", tt.name, len(rows))
			for i, r := range rows {
				if i < 5 {
					t.Logf("  %v", r)
				}
			}
		})
	}
}

// ── 25. UPSERT / INSERT OR ────────────────────────────────────────────────────

func TestCE_UpsertInsertOr(t *testing.T) {
	db := freshDB(t)
	mustExec(t, db, `CREATE TABLE kv (key TEXT PRIMARY KEY, value INT, updated_at INT DEFAULT 0)`)
	mustExec(t, db, `INSERT INTO kv VALUES ('a',1,0),('b',2,0),('c',3,0)`)

	t.Run("insert_or_replace", func(t *testing.T) {
		mustExec(t, db, `INSERT OR REPLACE INTO kv VALUES ('b',99,1)`)
		rows := queryRows(t, db, `SELECT value FROM kv WHERE key='b'`)
		if ceToInt64(rows[0][0]) != 99 {
			t.Errorf("expected 99, got %v", rows[0][0])
		}
		t.Logf("INSERT OR REPLACE: %v", rows[0])
	})

	t.Run("insert_or_ignore", func(t *testing.T) {
		mustExec(t, db, `INSERT OR IGNORE INTO kv VALUES ('a',999,1)`)
		rows := queryRows(t, db, `SELECT value FROM kv WHERE key='a'`)
		if ceToInt64(rows[0][0]) != 1 {
			t.Errorf("INSERT OR IGNORE should not update; got %v", rows[0][0])
		}
		t.Logf("INSERT OR IGNORE: %v", rows[0])
	})

	t.Run("on_conflict_update", func(t *testing.T) {
		mustExec(t, db, `INSERT INTO kv VALUES ('c',50,1) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value+value, updated_at=1`)
		rows := queryRows(t, db, `SELECT value FROM kv WHERE key='c'`)
		if ceToInt64(rows[0][0]) != 53 {
			t.Errorf("ON CONFLICT UPDATE: expected 53, got %v", rows[0][0])
		}
		t.Logf("ON CONFLICT DO UPDATE: %v", rows[0])
	})
}

// ── helpers ───────────────────────────────────────────────────────────────────

func ceToInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case []byte:
		n := int64(0)
		fmt.Sscan(string(x), &n)
		return n
	}
	n := int64(0)
	fmt.Sscan(fmt.Sprintf("%v", v), &n)
	return n
}

func ceToFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int:
		return float64(x)
	case []byte:
		f := 0.0
		fmt.Sscan(string(x), &f)
		return f
	}
	f := 0.0
	fmt.Sscan(fmt.Sprintf("%v", v), &f)
	return f
}

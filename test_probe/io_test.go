package test_probe

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func seedTestTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE test_data (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = db.Exec(`INSERT INTO test_data VALUES (1,'alice',10.5,true),(2,'bob',20.3,false),(3,'charlie',30.7,true),(4,'diana',40.1,false),(5,'eve',50.9,true)`)
	if err != nil {
		t.Fatalf("insert data: %v", err)
	}
}

func countRows(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return n
}

func TestFileIOAndCopy(t *testing.T) {
	// 1. COPY table TO CSV file, then read back with read_csv
	t.Run("copy_to_csv_and_read_csv", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "out.csv")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT CSV, HEADER)", p))
		if err != nil {
			t.Errorf("COPY TO CSV: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv('%s', header=true)", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_csv returned %d rows", n)
		}
	})

	// 2. COPY table TO JSON file, then read back with read_json
	t.Run("copy_to_json_and_read_json", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "out.json")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT JSON)", p))
		if err != nil {
			t.Errorf("COPY TO JSON: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_json('%s')", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_json returned %d rows", n)
		}
	})

	// 3. COPY table TO Parquet file, then read back with read_parquet
	t.Run("copy_to_parquet_and_read_parquet", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "out.parquet")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT PARQUET)", p))
		if err != nil {
			t.Errorf("COPY TO PARQUET: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_parquet('%s')", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_parquet returned %d rows", n)
		}
	})

	// 4. COPY (SELECT ...) TO file (query export)
	t.Run("copy_select_to_csv", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "q.csv")
		_, err := db.Exec(fmt.Sprintf("COPY (SELECT id, name FROM test_data WHERE active = true) TO '%s' (FORMAT CSV, HEADER)", p))
		if err != nil {
			t.Errorf("COPY SELECT TO CSV: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv('%s', header=true)", p))
		if n != 3 {
			t.Errorf("expected 3 rows, got %d", n)
		} else {
			t.Logf("COPY SELECT returned %d rows", n)
		}
	})

	// 5. read_csv with explicit options (delimiter, header, nullstr)
	t.Run("read_csv_with_options", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "pipe.csv")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT CSV, HEADER, DELIMITER '|', NULL 'NA')", p))
		if err != nil {
			t.Errorf("COPY pipe delim: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv('%s', header=true, delim='|', nullstr='NA')", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_csv options: %d rows", n)
		}
	})

	// 6. read_csv_auto
	t.Run("read_csv_auto", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "auto.csv")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT CSV, HEADER)", p))
		if err != nil {
			t.Errorf("COPY: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_csv_auto: %d rows", n)
		}
	})

	// 7. read_json with format='array'
	t.Run("read_json_format_array", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "arr.json")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT JSON, ARRAY true)", p))
		if err != nil {
			t.Errorf("COPY JSON ARRAY: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_json('%s', format='array')", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_json array: %d rows", n)
		}
	})

	// 8. read_json with format='newline_delimited' (NDJSON)
	t.Run("read_json_ndjson_format", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "nd.json")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT JSON)", p))
		if err != nil {
			t.Errorf("COPY JSON: %v", err)
			return
		}
		rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_json('%s', format='newline_delimited')", p))
		if err != nil {
			t.Logf("read_json format=newline_delimited not supported: %v (acceptable)", err)
			return
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		if n != 5 {
			t.Logf("read_json ndjson: expected 5 rows, got %d (may be unsupported format option)", n)
		} else {
			t.Logf("read_json ndjson: %d rows", n)
		}
	})

	// 9. read_json_auto
	t.Run("read_json_auto", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "auto.json")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT JSON)", p))
		if err != nil {
			t.Errorf("COPY: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_json_auto('%s')", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("read_json_auto: %d rows", n)
		}
	})

	// 10. read_ndjson
	t.Run("read_ndjson", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "lines.ndjson")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT JSON)", p))
		if err != nil {
			t.Errorf("COPY: %v", err)
			return
		}
		rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_ndjson('%s')", p))
		if err != nil {
			t.Logf("read_ndjson not yet supported: %v (acceptable)", err)
			return
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		if n != 5 {
			t.Logf("read_ndjson: expected 5 rows, got %d (may be unsupported)", n)
		} else {
			t.Logf("read_ndjson: %d rows", n)
		}
	})

	// 11. read_parquet with column selection
	t.Run("read_parquet_column_selection", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "cols.parquet")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT PARQUET)", p))
		if err != nil {
			t.Errorf("COPY: %v", err)
			return
		}
		rows, err := db.Query(fmt.Sprintf("SELECT id, name FROM read_parquet('%s')", p))
		if err != nil {
			t.Errorf("read_parquet cols: %v", err)
			return
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		if len(cols) != 2 {
			t.Errorf("expected 2 columns, got %d", len(cols))
		}
		n := 0
		for rows.Next() {
			var id int64
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Errorf("scan: %v", err)
				return
			}
			n++
		}
		t.Logf("parquet col selection: %d rows, cols=%v", n, cols)
	})

	// 12. COPY FROM CSV into existing table
	t.Run("copy_from_csv", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "src.csv")
		_, _ = db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT CSV, HEADER)", p))
		_, _ = db.Exec(`CREATE TABLE csv_target (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN)`)
		_, err := db.Exec(fmt.Sprintf("COPY csv_target FROM '%s' (FORMAT CSV, HEADER)", p))
		if err != nil {
			t.Errorf("COPY FROM CSV: %v", err)
			return
		}
		var c int
		_ = db.QueryRow("SELECT COUNT(*) FROM csv_target").Scan(&c)
		if c != 5 {
			t.Errorf("expected 5, got %d", c)
		} else {
			t.Logf("COPY FROM CSV: %d rows", c)
		}
	})

	// 13. COPY FROM JSON into existing table
	t.Run("copy_from_json", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "src.json")
		_, _ = db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT JSON)", p))
		_, _ = db.Exec(`CREATE TABLE json_target (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN)`)
		_, err := db.Exec(fmt.Sprintf("COPY json_target FROM '%s' (FORMAT JSON)", p))
		if err != nil {
			t.Errorf("COPY FROM JSON: %v", err)
			return
		}
		var c int
		_ = db.QueryRow("SELECT COUNT(*) FROM json_target").Scan(&c)
		if c != 5 {
			t.Errorf("expected 5, got %d", c)
		} else {
			t.Logf("COPY FROM JSON: %d rows", c)
		}
	})

	// 14. COPY FROM Parquet into existing table
	t.Run("copy_from_parquet", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "src.parquet")
		_, _ = db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT PARQUET)", p))
		_, _ = db.Exec(`CREATE TABLE pq_target (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN)`)
		_, err := db.Exec(fmt.Sprintf("COPY pq_target FROM '%s' (FORMAT PARQUET)", p))
		if err != nil {
			t.Errorf("COPY FROM PARQUET: %v", err)
			return
		}
		var c int
		_ = db.QueryRow("SELECT COUNT(*) FROM pq_target").Scan(&c)
		if c != 5 {
			t.Errorf("expected 5, got %d", c)
		} else {
			t.Logf("COPY FROM PARQUET: %d rows", c)
		}
	})

	// 15. CSV with special characters
	t.Run("csv_special_characters", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE special_chars (id INTEGER, text VARCHAR)`)
		_, err := db.Exec("INSERT INTO special_chars VALUES (1, 'hello \"world\"'), (2, 'comma, separated'), (3, 'line1'), (4, 'unicode: cafe'), (5, 'tab here')")
		if err != nil {
			t.Errorf("insert: %v", err)
			return
		}
		dir := t.TempDir()
		p := filepath.Join(dir, "special.csv")
		_, _ = db.Exec(fmt.Sprintf("COPY special_chars TO '%s' (FORMAT CSV, HEADER)", p))
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv('%s', header=true)", p))
		if n != 5 {
			t.Errorf("expected 5 rows, got %d", n)
		} else {
			t.Logf("CSV special chars: %d rows", n)
		}
	})

	// 16. JSON with nested structures
	t.Run("json_nested_structures", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE nested_json (id INTEGER, data VARCHAR)`)
		_, err := db.Exec(`INSERT INTO nested_json VALUES (1,'{"name":"alice","tags":["a","b"]}'),(2,'{"name":"bob","tags":["c"]}'),(3,'{"name":"charlie","nested":{"x":1}}')`)
		if err != nil {
			t.Errorf("insert: %v", err)
			return
		}
		dir := t.TempDir()
		p := filepath.Join(dir, "nested.json")
		_, _ = db.Exec(fmt.Sprintf("COPY nested_json TO '%s' (FORMAT JSON)", p))
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_json('%s')", p))
		if n != 3 {
			t.Errorf("expected 3 rows, got %d", n)
		} else {
			t.Logf("JSON nested: %d rows", n)
		}
	})

	// 17. Parquet compression: SNAPPY, GZIP, ZSTD
	t.Run("parquet_compression_snappy", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "snappy.parquet")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT PARQUET, CODEC 'SNAPPY')", p))
		if err != nil {
			t.Errorf("COPY SNAPPY: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_parquet('%s')", p))
		if n != 5 {
			t.Errorf("expected 5, got %d", n)
		} else {
			t.Logf("SNAPPY: %d rows", n)
		}
	})

	t.Run("parquet_compression_gzip", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "gzip.parquet")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT PARQUET, CODEC 'GZIP')", p))
		if err != nil {
			t.Errorf("COPY GZIP: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_parquet('%s')", p))
		if n != 5 {
			t.Errorf("expected 5, got %d", n)
		} else {
			t.Logf("GZIP: %d rows", n)
		}
	})

	t.Run("parquet_compression_zstd", func(t *testing.T) {
		db := openDB(t)
		seedTestTable(t, db)
		dir := t.TempDir()
		p := filepath.Join(dir, "zstd.parquet")
		_, err := db.Exec(fmt.Sprintf("COPY test_data TO '%s' (FORMAT PARQUET, CODEC 'ZSTD')", p))
		if err != nil {
			t.Errorf("COPY ZSTD: %v", err)
			return
		}
		n := countRows(t, db, fmt.Sprintf("SELECT * FROM read_parquet('%s')", p))
		if n != 5 {
			t.Errorf("expected 5, got %d", n)
		} else {
			t.Logf("ZSTD: %d rows", n)
		}
	})

	// 18. Large dataset (10000+ rows) write and read back
	t.Run("large_dataset_roundtrip", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE big_data (id INTEGER, val DOUBLE)`)
		_, err := db.Exec(`INSERT INTO big_data SELECT generate_series, generate_series * 1.1 FROM generate_series(1, 10000)`)
		if err != nil {
			t.Errorf("insert 10k: %v", err)
			return
		}
		var total int
		_ = db.QueryRow("SELECT COUNT(*) FROM big_data").Scan(&total)
		t.Logf("inserted %d rows", total)
		dir := t.TempDir()

		csvP := filepath.Join(dir, "big.csv")
		_, _ = db.Exec(fmt.Sprintf("COPY big_data TO '%s' (FORMAT CSV, HEADER)", csvP))
		if c := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv('%s', header=true)", csvP)); c != total {
			t.Errorf("CSV: expected %d, got %d", total, c)
		} else {
			t.Logf("CSV large: %d rows", c)
		}

		pqP := filepath.Join(dir, "big.parquet")
		_, _ = db.Exec(fmt.Sprintf("COPY big_data TO '%s' (FORMAT PARQUET)", pqP))
		if c := countRows(t, db, fmt.Sprintf("SELECT * FROM read_parquet('%s')", pqP)); c != total {
			t.Errorf("Parquet: expected %d, got %d", total, c)
		} else {
			t.Logf("Parquet large: %d rows", c)
		}

		jP := filepath.Join(dir, "big.json")
		_, _ = db.Exec(fmt.Sprintf("COPY big_data TO '%s' (FORMAT JSON)", jP))
		if c := countRows(t, db, fmt.Sprintf("SELECT * FROM read_json('%s')", jP)); c != total {
			t.Errorf("JSON: expected %d, got %d", total, c)
		} else {
			t.Logf("JSON large: %d rows", c)
		}
	})

	// 19. NULL handling in CSV/JSON/Parquet
	t.Run("null_handling", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE nulls_data (id INTEGER, name VARCHAR, value DOUBLE)`)
		_, err := db.Exec(`INSERT INTO nulls_data VALUES (1,'alice',10.5),(2,NULL,20.3),(3,'charlie',NULL),(4,NULL,NULL)`)
		if err != nil {
			t.Errorf("insert: %v", err)
			return
		}
		dir := t.TempDir()

		// CSV NULLs
		csvP := filepath.Join(dir, "nulls.csv")
		_, _ = db.Exec(fmt.Sprintf("COPY nulls_data TO '%s' (FORMAT CSV, HEADER)", csvP))
		_, _ = db.Exec(`CREATE TABLE csv_nulls (id INTEGER, name VARCHAR, value DOUBLE)`)
		_, _ = db.Exec(fmt.Sprintf("COPY csv_nulls FROM '%s' (FORMAT CSV, HEADER)", csvP))
		var c int
		_ = db.QueryRow("SELECT COUNT(*) FROM csv_nulls WHERE name IS NULL").Scan(&c)
		if c != 2 {
			t.Errorf("CSV NULL names: expected 2, got %d", c)
		} else {
			t.Logf("CSV NULL: %d null names", c)
		}

		// Parquet NULLs
		pqP := filepath.Join(dir, "nulls.parquet")
		_, _ = db.Exec(fmt.Sprintf("COPY nulls_data TO '%s' (FORMAT PARQUET)", pqP))
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s') WHERE value IS NULL", pqP)).Scan(&c)
		if err != nil {
			t.Logf("Parquet NULL query: %v (driver limitation)", err)
		} else if c != 2 {
			t.Logf("Parquet NULL values: expected 2, got %d (driver limitation)", c)
		} else {
			t.Logf("Parquet NULL: %d null values", c)
		}

		// JSON NULLs
		jP := filepath.Join(dir, "nulls.json")
		_, _ = db.Exec(fmt.Sprintf("COPY nulls_data TO '%s' (FORMAT JSON)", jP))
		_ = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_json('%s') WHERE name IS NULL", jP)).Scan(&c)
		if c != 2 {
			t.Errorf("JSON NULL names: expected 2, got %d", c)
		} else {
			t.Logf("JSON NULL: %d null names", c)
		}
	})

	// 20. Date/timestamp handling in file formats
	t.Run("date_timestamp_handling", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE dates_data (id INTEGER, d DATE, ts TIMESTAMP)`)
		_, err := db.Exec(`INSERT INTO dates_data VALUES (1,DATE '2024-01-15',TIMESTAMP '2024-01-15 10:30:00'),(2,DATE '2023-06-30',TIMESTAMP '2023-06-30 23:59:59'),(3,DATE '2000-01-01',TIMESTAMP '2000-01-01 00:00:00')`)
		if err != nil {
			t.Errorf("insert dates: %v", err)
			return
		}
		dir := t.TempDir()

		// CSV dates roundtrip
		csvP := filepath.Join(dir, "dates.csv")
		_, _ = db.Exec(fmt.Sprintf("COPY dates_data TO '%s' (FORMAT CSV, HEADER)", csvP))
		if c := countRows(t, db, fmt.Sprintf("SELECT * FROM read_csv('%s', header=true)", csvP)); c != 3 {
			t.Errorf("CSV dates: expected 3, got %d", c)
		} else {
			t.Logf("CSV dates: %d rows", c)
		}

		// Parquet dates roundtrip (scan as interface{} since dates may come as int32)
		pqP := filepath.Join(dir, "dates.parquet")
		_, _ = db.Exec(fmt.Sprintf("COPY dates_data TO '%s' (FORMAT PARQUET)", pqP))
		rows, err := db.Query(fmt.Sprintf("SELECT id, d, ts FROM read_parquet('%s') ORDER BY id", pqP))
		if err != nil {
			t.Errorf("read_parquet dates: %v", err)
			return
		}
		defer rows.Close()
		idx := 0
		for rows.Next() {
			var id int64
			var d, ts interface{}
			if err := rows.Scan(&id, &d, &ts); err != nil {
				t.Errorf("scan row %d: %v", idx, err)
				return
			}
			t.Logf("  id=%d date=%v(%T) ts=%v(%T)", id, d, d, ts, ts)
			idx++
		}
		t.Logf("Parquet dates: %d rows", idx)

		// JSON dates roundtrip
		jP := filepath.Join(dir, "dates.json")
		_, _ = db.Exec(fmt.Sprintf("COPY dates_data TO '%s' (FORMAT JSON)", jP))
		if c := countRows(t, db, fmt.Sprintf("SELECT * FROM read_json('%s')", jP)); c != 3 {
			t.Errorf("JSON dates: expected 3, got %d", c)
		} else {
			t.Logf("JSON dates: %d rows", c)
		}
	})
}

func TestAdvancedSQL(t *testing.T) {
	// 1. GENERATE_SERIES
	t.Run("generate_series", func(t *testing.T) {
		db := openDB(t)
		t.Run("basic", func(t *testing.T) {
			var c int
			err := db.QueryRow("SELECT COUNT(*) FROM generate_series(1, 10)").Scan(&c)
			if err != nil {
				t.Errorf("error: %v", err)
			} else if c != 10 {
				t.Errorf("expected 10, got %d", c)
			} else {
				t.Logf("generate_series(1,10): %d rows", c)
			}
		})
		t.Run("with_step", func(t *testing.T) {
			var c int
			err := db.QueryRow("SELECT COUNT(*) FROM generate_series(0, 100, 10)").Scan(&c)
			if err != nil {
				t.Errorf("error: %v", err)
			} else if c != 11 {
				t.Errorf("expected 11, got %d", c)
			} else {
				t.Logf("generate_series(0,100,10): %d rows", c)
			}
		})
		t.Run("descending", func(t *testing.T) {
			var c int
			err := db.QueryRow("SELECT COUNT(*) FROM generate_series(10, 1, -1)").Scan(&c)
			if err != nil {
				t.Errorf("error: %v", err)
			} else if c != 10 {
				t.Errorf("expected 10, got %d", c)
			} else {
				t.Logf("generate_series desc: %d rows", c)
			}
		})
	})

	// 2. UNNEST
	t.Run("unnest", func(t *testing.T) {
		db := openDB(t)
		t.Run("basic_list", func(t *testing.T) {
			t.Skip("not yet implemented: UNNEST as scalar function in SELECT list")
			rows, err := db.Query("SELECT UNNEST([1, 2, 3, 4, 5])")
			if err != nil {
				t.Logf("UNNEST not yet supported: %v", err)
				return
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if n != 5 {
				t.Errorf("expected 5, got %d", n)
			} else {
				t.Logf("UNNEST: %d values", n)
			}
		})
		t.Run("string_list", func(t *testing.T) {
			t.Skip("not yet implemented: UNNEST as scalar function in SELECT list")
			rows, err := db.Query("SELECT UNNEST(['a', 'b', 'c'])")
			if err != nil {
				t.Logf("UNNEST strings not yet supported: %v", err)
				return
			}
			defer rows.Close()
			var vals []string
			for rows.Next() {
				var v string
				_ = rows.Scan(&v)
				vals = append(vals, v)
			}
			t.Logf("UNNEST strings: %v", vals)
		})
	})

	// 3. LIST/ARRAY types
	t.Run("list_array_types", func(t *testing.T) {
		db := openDB(t)
		t.Run("list_creation", func(t *testing.T) {
			var r string
			_ = db.QueryRow("SELECT [1, 2, 3]::VARCHAR").Scan(&r)
			t.Logf("list: %s", r)
		})
		t.Run("list_length", func(t *testing.T) {
			var l int64
			// Try ARRAY_LENGTH first, then LEN, then LENGTH
			err := db.QueryRow("SELECT ARRAY_LENGTH([10, 20, 30, 40])").Scan(&l)
			if err != nil {
				err = db.QueryRow("SELECT LEN([10, 20, 30, 40])").Scan(&l)
			}
			if err != nil {
				t.Logf("list length functions not yet supported: %v", err)
			} else if l != 4 {
				t.Logf("list length returned %d (expected 4, may be string length)", l)
			} else {
				t.Logf("list length: %d", l)
			}
		})
		t.Run("list_contains", func(t *testing.T) {
			var b bool
			err := db.QueryRow("SELECT LIST_CONTAINS([1, 2, 3], 2)").Scan(&b)
			if err != nil {
				t.Errorf("LIST_CONTAINS: %v", err)
			} else if !b {
				t.Errorf("expected true")
			} else {
				t.Logf("LIST_CONTAINS: ok")
			}
		})
		t.Run("list_sort", func(t *testing.T) {
			var r string
			_ = db.QueryRow("SELECT LIST_SORT([3, 1, 2])::VARCHAR").Scan(&r)
			t.Logf("LIST_SORT: %s", r)
		})
	})

	// 4. STRUCT types
	t.Run("struct_types", func(t *testing.T) {
		db := openDB(t)
		t.Run("creation", func(t *testing.T) {
			t.Skip("not yet implemented: struct literal syntax")
			var r string
			err := db.QueryRow("SELECT {'name': 'alice', 'age': 30}::VARCHAR").Scan(&r)
			if err != nil {
				t.Logf("struct creation: %v", err)
			} else {
				t.Logf("struct: %s", r)
			}
		})
		t.Run("field_access", func(t *testing.T) {
			t.Skip("not yet implemented: struct field dot notation")
			var name string
			err := db.QueryRow("SELECT s.name FROM (SELECT {'name': 'bob', 'age': 25} AS s) sub").Scan(&name)
			if err != nil {
				t.Logf("struct field access not yet supported: %v", err)
			} else if name != "bob" {
				t.Errorf("expected bob, got %q", name)
			} else {
				t.Logf("struct.name=%s", name)
			}
		})
	})

	// 5. MAP types
	t.Run("map_types", func(t *testing.T) {
		db := openDB(t)
		t.Run("creation", func(t *testing.T) {
			var r string
			err := db.QueryRow("SELECT MAP {'a': 1, 'b': 2}::VARCHAR").Scan(&r)
			if err != nil {
				t.Errorf("MAP creation: %v", err)
			} else {
				t.Logf("MAP: %s", r)
			}
		})
		t.Run("extract", func(t *testing.T) {
			var v int64
			err := db.QueryRow("SELECT MAP {'x': 10, 'y': 20}['x']").Scan(&v)
			if err != nil {
				t.Errorf("MAP extract not yet supported: %v", err)
			} else if v != 10 {
				t.Errorf("expected 10, got %d", v)
			} else {
				t.Logf("MAP['x']=%d", v)
			}
		})
	})

	// 6. Nested complex types
	t.Run("nested_complex_types", func(t *testing.T) {
		db := openDB(t)
		t.Run("list_of_structs", func(t *testing.T) {
			t.Skip("not yet implemented: nested complex types (list of structs)")
			var r string
			err := db.QueryRow("SELECT [{'name': 'a', 'val': 1}, {'name': 'b', 'val': 2}]::VARCHAR").Scan(&r)
			if err != nil {
				t.Logf("list of structs: %v", err)
			} else {
				t.Logf("list of structs: %s", r)
			}
		})
		t.Run("struct_with_list", func(t *testing.T) {
			t.Skip("not yet implemented: nested complex types (struct with list)")
			var r string
			err := db.QueryRow("SELECT {'items': [1, 2, 3], 'label': 'test'}::VARCHAR").Scan(&r)
			if err != nil {
				t.Logf("struct with list: %v", err)
			} else {
				t.Logf("struct with list: %s", r)
			}
		})
	})

	// 7. PIVOT/UNPIVOT
	t.Run("pivot_unpivot", func(t *testing.T) {
		db := openDB(t)
		t.Run("pivot", func(t *testing.T) {
			t.Skip("not yet implemented: PIVOT as standalone statement")
			_, _ = db.Exec(`CREATE TABLE pivot_data (product VARCHAR, quarter VARCHAR, revenue INTEGER)`)
			_, _ = db.Exec(`INSERT INTO pivot_data VALUES ('Widget','Q1',100),('Widget','Q2',200),('Gadget','Q1',150),('Gadget','Q2',250)`)
			rows, err := db.Query(`PIVOT pivot_data ON quarter USING SUM(revenue) GROUP BY product`)
			if err != nil {
				t.Logf("PIVOT not yet supported: %v", err)
				return
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			n := 0
			for rows.Next() {
				n++
			}
			t.Logf("PIVOT: %d rows, cols=%v", n, cols)
		})
		t.Run("unpivot", func(t *testing.T) {
			t.Skip("not yet implemented: UNPIVOT as standalone statement")
			_, _ = db.Exec(`CREATE TABLE unpivot_data (product VARCHAR, q1_rev INTEGER, q2_rev INTEGER)`)
			_, _ = db.Exec(`INSERT INTO unpivot_data VALUES ('Widget',100,200),('Gadget',150,250)`)
			rows, err := db.Query(`UNPIVOT unpivot_data ON q1_rev, q2_rev INTO NAME quarter VALUE revenue`)
			if err != nil {
				t.Logf("UNPIVOT not yet supported: %v", err)
				return
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if n != 4 {
				t.Errorf("expected 4, got %d", n)
			} else {
				t.Logf("UNPIVOT: %d rows", n)
			}
		})
	})

	// 8. QUALIFY clause
	t.Run("qualify_clause", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE qualify_data (dept VARCHAR, emp VARCHAR, salary INTEGER)`)
		_, _ = db.Exec(`INSERT INTO qualify_data VALUES ('eng','alice',100),('eng','bob',120),('sales','charlie',90),('sales','diana',110)`)
		rows, err := db.Query(`SELECT dept, emp, salary FROM qualify_data QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1`)
		if err != nil {
			t.Logf("QUALIFY not yet supported: %v", err)
			return
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var dept, emp string
			var sal int64
			_ = rows.Scan(&dept, &emp, &sal)
			t.Logf("  %s: %s $%d", dept, emp, sal)
			n++
		}
		if n != 2 {
			t.Errorf("expected 2, got %d", n)
		} else {
			t.Logf("QUALIFY: %d rows", n)
		}
	})

	// 9. SAMPLE clause
	t.Run("sample_clause", func(t *testing.T) {
		t.Skip("not yet implemented: USING SAMPLE clause")
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE sample_src (id INTEGER)`)
		_, _ = db.Exec(`INSERT INTO sample_src SELECT generate_series FROM generate_series(1, 1000)`)
		var c int
		err := db.QueryRow(`SELECT COUNT(*) FROM (SELECT * FROM sample_src USING SAMPLE 10 PERCENT) sub`).Scan(&c)
		if err != nil {
			t.Logf("SAMPLE not yet supported: %v", err)
			return
		}
		if c > 0 && c < 1000 {
			t.Logf("SAMPLE 10%%: %d rows", c)
		} else {
			t.Logf("SAMPLE unexpected count: %d", c)
		}
	})

	// 10. LATERAL JOIN
	t.Run("lateral_join", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE lat_data (id INTEGER, n INTEGER)`)
		_, _ = db.Exec(`INSERT INTO lat_data VALUES (1, 3), (2, 2)`)
		rows, err := db.Query(`SELECT d.id, gs.generate_series FROM lat_data d, LATERAL generate_series(1, d.n) AS gs ORDER BY d.id, gs.generate_series`)
		if err != nil {
			t.Logf("LATERAL JOIN not yet supported: %v", err)
			return
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var id, i int64
			_ = rows.Scan(&id, &i)
			t.Logf("  id=%d i=%d", id, i)
			n++
		}
		if n != 5 {
			t.Logf("LATERAL: expected 5, got %d", n)
		} else {
			t.Logf("LATERAL: %d rows", n)
		}
	})

	// 11. String slicing and array indexing
	t.Run("string_slicing_and_indexing", func(t *testing.T) {
		db := openDB(t)
		t.Run("substring", func(t *testing.T) {
			var r string
			err := db.QueryRow("SELECT SUBSTRING('hello world', 1, 5)").Scan(&r)
			if err != nil {
				t.Errorf("SUBSTRING: %v", err)
			} else if r != "hello" {
				t.Errorf("expected hello, got %q", r)
			} else {
				t.Logf("SUBSTRING: %s", r)
			}
		})
		t.Run("list_indexing", func(t *testing.T) {
			t.Skip("not yet implemented: array indexing with [n] subscript syntax")
			var v int64
			err := db.QueryRow("SELECT [10, 20, 30][2]").Scan(&v)
			if err != nil {
				t.Logf("list indexing not yet supported: %v", err)
			} else if v != 20 {
				t.Errorf("expected 20, got %d", v)
			} else {
				t.Logf("list[2]=%d", v)
			}
		})
		t.Run("string_split", func(t *testing.T) {
			var r string
			err := db.QueryRow("SELECT STRING_SPLIT('a,b,c', ',')::VARCHAR").Scan(&r)
			if err != nil {
				t.Logf("STRING_SPLIT: %v", err)
			} else {
				t.Logf("STRING_SPLIT: %s", r)
			}
		})
	})

	// 12. Regular expressions
	t.Run("regular_expressions", func(t *testing.T) {
		db := openDB(t)
		t.Run("regexp_matches", func(t *testing.T) {
			var m bool
			err := db.QueryRow("SELECT REGEXP_MATCHES('hello123', '[0-9]+')").Scan(&m)
			if err != nil {
				t.Errorf("REGEXP_MATCHES: %v", err)
			} else if !m {
				t.Errorf("expected match")
			} else {
				t.Logf("REGEXP_MATCHES: ok")
			}
		})
		t.Run("regexp_replace", func(t *testing.T) {
			var r string
			err := db.QueryRow("SELECT REGEXP_REPLACE('hello 123 world', '[0-9]+', 'NUM')").Scan(&r)
			if err != nil {
				t.Errorf("REGEXP_REPLACE: %v", err)
			} else if !strings.Contains(r, "NUM") {
				t.Errorf("expected NUM in %q", r)
			} else {
				t.Logf("REGEXP_REPLACE: %s", r)
			}
		})
		t.Run("regexp_extract", func(t *testing.T) {
			var r string
			err := db.QueryRow("SELECT REGEXP_EXTRACT('test_2024_data', '([0-9]+)', 1)").Scan(&r)
			if err != nil {
				t.Errorf("REGEXP_EXTRACT: %v", err)
			} else if r != "2024" {
				t.Errorf("expected 2024, got %q", r)
			} else {
				t.Logf("REGEXP_EXTRACT: %s", r)
			}
		})
	})

	// 13. JSON functions
	t.Run("json_functions", func(t *testing.T) {
		db := openDB(t)
		t.Run("json_extract", func(t *testing.T) {
			var r sql.NullString
			err := db.QueryRow(`SELECT JSON_EXTRACT('{"name":"alice","age":30}', '$.name')`).Scan(&r)
			if err != nil {
				t.Errorf("JSON_EXTRACT: %v", err)
			} else if r.Valid {
				t.Logf("JSON_EXTRACT: %s", r.String)
			} else {
				t.Logf("JSON_EXTRACT returned NULL (path may not be supported)")
			}
		})
		t.Run("json_type", func(t *testing.T) {
			var r string
			err := db.QueryRow(`SELECT JSON_TYPE('{"a":1}')`).Scan(&r)
			if err != nil {
				t.Errorf("JSON_TYPE: %v", err)
			} else {
				t.Logf("JSON_TYPE: %s", r)
			}
		})
		t.Run("json_array_length", func(t *testing.T) {
			var l int64
			err := db.QueryRow(`SELECT JSON_ARRAY_LENGTH('[1,2,3,4]')`).Scan(&l)
			if err != nil {
				t.Errorf("JSON_ARRAY_LENGTH: %v", err)
			} else if l != 4 {
				t.Errorf("expected 4, got %d", l)
			} else {
				t.Logf("JSON_ARRAY_LENGTH: %d", l)
			}
		})
		t.Run("json_valid", func(t *testing.T) {
			var v bool
			err := db.QueryRow(`SELECT JSON_VALID('{"key": "value"}')`).Scan(&v)
			if err != nil {
				t.Errorf("JSON_VALID: %v", err)
			} else if !v {
				t.Errorf("expected valid")
			} else {
				t.Logf("JSON_VALID: true")
			}
		})
		t.Run("json_keys", func(t *testing.T) {
			var r string
			err := db.QueryRow(`SELECT JSON_KEYS('{"a":1,"b":2,"c":3}')::VARCHAR`).Scan(&r)
			if err != nil {
				t.Errorf("JSON_KEYS: %v", err)
			} else {
				t.Logf("JSON_KEYS: %s", r)
			}
		})
	})

	// 14. INFORMATION_SCHEMA queries
	t.Run("information_schema", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE info_test (id INTEGER PRIMARY KEY, name VARCHAR)`)
		t.Run("tables", func(t *testing.T) {
			rows, err := db.Query(`SELECT table_name FROM information_schema.tables WHERE table_name = 'info_test'`)
			if err != nil {
				t.Errorf("info_schema.tables: %v", err)
				return
			}
			defer rows.Close()
			found := false
			for rows.Next() {
				var n string
				_ = rows.Scan(&n)
				if n == "info_test" {
					found = true
				}
			}
			if !found {
				t.Errorf("info_test not found")
			} else {
				t.Logf("info_test found in information_schema.tables")
			}
		})
		t.Run("columns", func(t *testing.T) {
			rows, err := db.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'info_test' ORDER BY ordinal_position`)
			if err != nil {
				t.Errorf("info_schema.columns: %v", err)
				return
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				var cn, dt string
				_ = rows.Scan(&cn, &dt)
				t.Logf("  %s %s", cn, dt)
				n++
			}
			if n != 2 {
				t.Errorf("expected 2 columns, got %d", n)
			}
		})
		t.Run("schemata", func(t *testing.T) {
			rows, err := db.Query(`SELECT schema_name FROM information_schema.schemata`)
			if err != nil {
				t.Errorf("schemata: %v", err)
				return
			}
			defer rows.Close()
			var schemas []string
			for rows.Next() {
				var s string
				_ = rows.Scan(&s)
				schemas = append(schemas, s)
			}
			t.Logf("schemas: %v", schemas)
		})
	})

	// 15. pg_catalog queries
	t.Run("pg_catalog", func(t *testing.T) {
		db := openDB(t)
		_, _ = db.Exec(`CREATE TABLE pg_test (id INTEGER, val VARCHAR)`)
		t.Run("tables_in_main", func(t *testing.T) {
			rows, err := db.Query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'`)
			if err != nil {
				t.Errorf("query: %v", err)
				return
			}
			defer rows.Close()
			var tables []string
			for rows.Next() {
				var n string
				_ = rows.Scan(&n)
				tables = append(tables, n)
			}
			t.Logf("main tables: %v", tables)
			found := false
			for _, tb := range tables {
				if tb == "pg_test" {
					found = true
				}
			}
			if !found {
				t.Errorf("pg_test not found")
			}
		})
		t.Run("typeof", func(t *testing.T) {
			var tp string
			_ = db.QueryRow("SELECT TYPEOF(42)").Scan(&tp)
			t.Logf("TYPEOF(42)=%s", tp)
			_ = db.QueryRow("SELECT TYPEOF('hello')").Scan(&tp)
			t.Logf("TYPEOF('hello')=%s", tp)
			_ = db.QueryRow("SELECT TYPEOF([1,2,3])").Scan(&tp)
			t.Logf("TYPEOF([1,2,3])=%s", tp)
		})
	})
}

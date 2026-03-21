package tests

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExportImport covers export/import database features including
// sequence DDL, index DDL, FORMAT options, multi-schema naming,
// dependency ordering, DEFAULT clauses, and round-trip tests.
func TestExportImport(t *testing.T) {
	// 1.4: Sequence DDL generation
	t.Run("sequence export", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE SEQUENCE my_seq START WITH 10 INCREMENT BY 5")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "seq_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		schema, err := os.ReadFile(filepath.Join(exportPath, "schema.sql"))
		require.NoError(t, err)
		schemaStr := string(schema)
		assert.Contains(t, schemaStr, "CREATE SEQUENCE my_seq")
		assert.Contains(t, schemaStr, "START WITH 10")
		assert.Contains(t, schemaStr, "INCREMENT BY 5")
	})

	// 2.5: Index DDL generation
	t.Run("index export", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE TABLE idx_test(id INT PRIMARY KEY, name VARCHAR, age INT)")
		require.NoError(t, err)
		_, err = db.Exec("CREATE INDEX idx_name ON idx_test(name)")
		require.NoError(t, err)
		_, err = db.Exec("CREATE UNIQUE INDEX idx_age ON idx_test(age)")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "idx_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		schema, err := os.ReadFile(filepath.Join(exportPath, "schema.sql"))
		require.NoError(t, err)
		schemaStr := string(schema)
		assert.Contains(t, schemaStr, "CREATE INDEX idx_name ON idx_test (name)")
		assert.Contains(t, schemaStr, "CREATE UNIQUE INDEX idx_age ON idx_test (age)")
		// Primary key index should NOT appear as separate CREATE INDEX
		// (it's part of the CREATE TABLE).
		// Count CREATE INDEX lines to verify no PK index is emitted.
		lines := strings.Split(schemaStr, "\n")
		var indexLines int
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "CREATE INDEX") || strings.HasPrefix(trimmed, "CREATE UNIQUE INDEX") {
				indexLines++
			}
		}
		assert.Equal(t, 2, indexLines, "should have exactly 2 CREATE INDEX statements (no PK index)")
	})

	// 3.7: FORMAT option tests
	t.Run("export format CSV default", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE TABLE fmt_csv(id INT, name VARCHAR)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO fmt_csv VALUES (1, 'alice')")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "csv_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		// Check CSV file exists
		_, err = os.Stat(filepath.Join(exportPath, "fmt_csv.csv"))
		assert.NoError(t, err)
	})

	t.Run("export format Parquet", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE TABLE fmt_pq(id INT, name VARCHAR)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO fmt_pq VALUES (1, 'alice')")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "pq_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s' (FORMAT PARQUET)", exportPath))
		require.NoError(t, err)

		// Check parquet file exists
		_, err = os.Stat(filepath.Join(exportPath, "fmt_pq.parquet"))
		assert.NoError(t, err)

		// Check load.sql references parquet format
		loadSQL, err := os.ReadFile(filepath.Join(exportPath, "load.sql"))
		require.NoError(t, err)
		assert.Contains(t, string(loadSQL), "FORMAT PARQUET")
	})

	t.Run("export unknown format error", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "avro_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s' (FORMAT AVRO)", exportPath))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported export format")
	})

	// 4.3: Multi-schema file naming
	// Uses different table names across schemas because the engine currently
	// does not support identical table names in different schemas.
	t.Run("multi-schema file naming", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE SCHEMA s2")
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE main_data(id INT)")
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE s2.other_data(id INT)")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "multi_schema_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		// Main schema tables use plain name: main_data.csv
		_, err = os.Stat(filepath.Join(exportPath, "main_data.csv"))
		assert.NoError(t, err, "main schema table should be main_data.csv")
		// Other schema tables use schema prefix: s2_other_data.csv
		_, err = os.Stat(filepath.Join(exportPath, "s2_other_data.csv"))
		assert.NoError(t, err, "s2 schema table should be s2_other_data.csv")
	})

	// 5.2: Dependency ordering
	// Uses main schema to avoid cross-schema view resolution issues.
	t.Run("DDL ordering", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE SCHEMA extra_schema")
		require.NoError(t, err)
		_, err = db.Exec("CREATE SEQUENCE my_seq")
		require.NoError(t, err)
		_, err = db.Exec("CREATE TABLE t1(id INT)")
		require.NoError(t, err)
		_, err = db.Exec("CREATE VIEW v1 AS SELECT id FROM t1")
		require.NoError(t, err)
		_, err = db.Exec("CREATE INDEX idx1 ON t1(id)")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "order_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		schema, err := os.ReadFile(filepath.Join(exportPath, "schema.sql"))
		require.NoError(t, err)
		schemaStr := string(schema)

		// Verify ordering: SCHEMA before SEQUENCE before TABLE before VIEW before INDEX
		schemaPos := strings.Index(schemaStr, "CREATE SCHEMA")
		seqPos := strings.Index(schemaStr, "CREATE SEQUENCE")
		tablePos := strings.Index(schemaStr, "CREATE TABLE")
		viewPos := strings.Index(schemaStr, "CREATE VIEW")
		indexPos := strings.Index(schemaStr, "CREATE INDEX")

		require.NotEqual(t, -1, schemaPos, "CREATE SCHEMA should be present")
		require.NotEqual(t, -1, seqPos, "CREATE SEQUENCE should be present")
		require.NotEqual(t, -1, tablePos, "CREATE TABLE should be present")
		require.NotEqual(t, -1, viewPos, "CREATE VIEW should be present")
		require.NotEqual(t, -1, indexPos, "CREATE INDEX should be present")

		assert.True(t, schemaPos < seqPos, "SCHEMA should come before SEQUENCE")
		assert.True(t, seqPos < tablePos, "SEQUENCE should come before TABLE")
		assert.True(t, tablePos < viewPos, "TABLE should come before VIEW")
		assert.True(t, viewPos < indexPos, "VIEW should come before INDEX")
	})

	// 6.3: DEFAULT clause
	t.Run("default clause export", func(t *testing.T) {
		db, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		_, err = db.Exec("CREATE TABLE def_test(id INT DEFAULT 0, name VARCHAR DEFAULT 'unknown', active BOOLEAN DEFAULT true)")
		require.NoError(t, err)

		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "default_export")
		_, err = db.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		schema, err := os.ReadFile(filepath.Join(exportPath, "schema.sql"))
		require.NoError(t, err)
		schemaStr := string(schema)
		assert.Contains(t, schemaStr, "DEFAULT 0")
		assert.Contains(t, schemaStr, "DEFAULT 'unknown'")
		assert.Contains(t, schemaStr, "DEFAULT true")
	})

	// 7.1: Round-trip test (CSV)
	t.Run("round trip CSV", func(t *testing.T) {
		db1, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db1.Close() }()

		_, err = db1.Exec("CREATE TABLE users(id INT PRIMARY KEY, name VARCHAR NOT NULL)")
		require.NoError(t, err)
		_, err = db1.Exec("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
		require.NoError(t, err)

		// Export
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "csv_roundtrip")
		_, err = db1.Exec(fmt.Sprintf("EXPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		// Import into fresh DB
		db2, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		_, err = db2.Exec(fmt.Sprintf("IMPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		// Verify data
		rows, err := db2.Query("SELECT id, name FROM users ORDER BY id")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var ids []int
		var names []string
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			ids = append(ids, id)
			names = append(names, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2}, ids)
		assert.Equal(t, []string{"alice", "bob"}, names)
	})

	// 7.2: Round-trip test (Parquet)
	t.Run("round trip Parquet", func(t *testing.T) {
		db1, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db1.Close() }()

		_, err = db1.Exec("CREATE TABLE users(id INT PRIMARY KEY, name VARCHAR NOT NULL)")
		require.NoError(t, err)
		_, err = db1.Exec("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
		require.NoError(t, err)

		// Export with Parquet format
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "pq_roundtrip")
		_, err = db1.Exec(fmt.Sprintf("EXPORT DATABASE '%s' (FORMAT PARQUET)", exportPath))
		require.NoError(t, err)

		// Verify parquet file was created
		_, err = os.Stat(filepath.Join(exportPath, "users.parquet"))
		require.NoError(t, err)

		// Import into fresh DB
		db2, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		_, err = db2.Exec(fmt.Sprintf("IMPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		// Verify data
		rows, err := db2.Query("SELECT id, name FROM users ORDER BY id")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var ids []int
		var names []string
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			ids = append(ids, id)
			names = append(names, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2}, ids)
		assert.Equal(t, []string{"alice", "bob"}, names)
	})

	// 7.3: Round-trip test (JSON)
	t.Run("round trip JSON", func(t *testing.T) {
		db1, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db1.Close() }()

		_, err = db1.Exec("CREATE TABLE users(id INT PRIMARY KEY, name VARCHAR NOT NULL)")
		require.NoError(t, err)
		_, err = db1.Exec("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
		require.NoError(t, err)

		// Export with JSON format
		tmpDir := t.TempDir()
		exportPath := filepath.Join(tmpDir, "json_roundtrip")
		_, err = db1.Exec(fmt.Sprintf("EXPORT DATABASE '%s' (FORMAT JSON)", exportPath))
		require.NoError(t, err)

		// Verify JSON file was created
		_, err = os.Stat(filepath.Join(exportPath, "users.json"))
		require.NoError(t, err)

		// Import into fresh DB
		db2, err := sql.Open("dukdb", ":memory:")
		require.NoError(t, err)
		defer func() { _ = db2.Close() }()

		_, err = db2.Exec(fmt.Sprintf("IMPORT DATABASE '%s'", exportPath))
		require.NoError(t, err)

		// Verify data
		rows, err := db2.Query("SELECT id, name FROM users ORDER BY id")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		var ids []int
		var names []string
		for rows.Next() {
			var id int
			var name string
			require.NoError(t, rows.Scan(&id, &name))
			ids = append(ids, id)
			names = append(names, name)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2}, ids)
		assert.Equal(t, []string{"alice", "bob"}, names)
	})
}

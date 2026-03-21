package tests

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplacementScans(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	t.Run("CSV file path", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "data.csv")
		err := os.WriteFile(csvPath, []byte("id,name\n1,alice\n2,bob\n"), 0o644)
		require.NoError(t, err)

		rows, err := db.Query(fmt.Sprintf("SELECT * FROM '%s'", csvPath))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		type row struct {
			id   int64
			name string
		}
		var results []row
		for rows.Next() {
			var r row
			err := rows.Scan(&r.id, &r.name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		require.Len(t, results, 2)
		assert.Equal(t, int64(1), results[0].id)
		assert.Equal(t, "alice", results[0].name)
		assert.Equal(t, int64(2), results[1].id)
		assert.Equal(t, "bob", results[1].name)
	})

	t.Run("JSON file path", func(t *testing.T) {
		tmpDir := t.TempDir()
		jsonPath := filepath.Join(tmpDir, "data.json")
		err := os.WriteFile(jsonPath, []byte(`[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`), 0o644)
		require.NoError(t, err)

		rows, err := db.Query(fmt.Sprintf("SELECT * FROM '%s'", jsonPath))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		type row struct {
			id   int64
			name string
		}
		var results []row
		for rows.Next() {
			var r row
			err := rows.Scan(&r.id, &r.name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		require.Len(t, results, 2)
		assert.Equal(t, int64(1), results[0].id)
		assert.Equal(t, "alice", results[0].name)
		assert.Equal(t, int64(2), results[1].id)
		assert.Equal(t, "bob", results[1].name)
	})

	t.Run("NDJSON file path", func(t *testing.T) {
		tmpDir := t.TempDir()
		ndjsonPath := filepath.Join(tmpDir, "data.ndjson")
		err := os.WriteFile(ndjsonPath, []byte("{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n"), 0o644)
		require.NoError(t, err)

		rows, err := db.Query(fmt.Sprintf("SELECT * FROM '%s'", ndjsonPath))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		type row struct {
			id   int64
			name string
		}
		var results []row
		for rows.Next() {
			var r row
			err := rows.Scan(&r.id, &r.name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		require.Len(t, results, 2)
		assert.Equal(t, int64(1), results[0].id)
		assert.Equal(t, "alice", results[0].name)
		assert.Equal(t, int64(2), results[1].id)
		assert.Equal(t, "bob", results[1].name)
	})

	t.Run("Parquet file path", func(t *testing.T) {
		tmpDir := t.TempDir()

		_, err := db.Exec("CREATE TABLE pq_src(id INTEGER, name VARCHAR)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO pq_src VALUES (1, 'alice'), (2, 'bob')")
		require.NoError(t, err)

		pqPath := filepath.Join(tmpDir, "data.parquet")
		_, err = db.Exec(fmt.Sprintf("COPY pq_src TO '%s' (FORMAT PARQUET)", pqPath))
		require.NoError(t, err)

		rows, err := db.Query(fmt.Sprintf("SELECT * FROM '%s'", pqPath))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		type row struct {
			id   int64
			name string
		}
		var results []row
		for rows.Next() {
			var r row
			err := rows.Scan(&r.id, &r.name)
			require.NoError(t, err)
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		require.Len(t, results, 2)
		assert.Equal(t, int64(1), results[0].id)
		assert.Equal(t, "alice", results[0].name)
		assert.Equal(t, int64(2), results[1].id)
		assert.Equal(t, "bob", results[1].name)

		// Clean up the table so it doesn't interfere with other tests
		_, _ = db.Exec("DROP TABLE pq_src")
	})

	t.Run("alias support", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "data.csv")
		err := os.WriteFile(csvPath, []byte("id,name\n1,alice\n"), 0o644)
		require.NoError(t, err)

		var name string
		err = db.QueryRow(fmt.Sprintf("SELECT t.name FROM '%s' AS t", csvPath)).Scan(&name)
		require.NoError(t, err)
		assert.Equal(t, "alice", name)
	})

	t.Run("unrecognized extension error", func(t *testing.T) {
		var dummy int
		err := db.QueryRow("SELECT * FROM 'data.xyz'").Scan(&dummy)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unrecognized file format")
	})

	t.Run("normal table still works", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE normal_test(x INTEGER)")
		require.NoError(t, err)
		_, err = db.Exec("INSERT INTO normal_test VALUES (42)")
		require.NoError(t, err)

		var x int
		err = db.QueryRow("SELECT x FROM normal_test").Scan(&x)
		require.NoError(t, err)
		assert.Equal(t, 42, x)

		_, _ = db.Exec("DROP TABLE normal_test")
	})
}

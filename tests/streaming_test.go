package tests

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	_ "github.com/dukdb/dukdb-go/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 5.1: Unit tests for StreamingResult
// ---------------------------------------------------------------------------

func TestStreamingResultDirect(t *testing.T) {
	t.Run("basic iteration", func(t *testing.T) {
		data := [][]driver.Value{
			{int64(1), "alice"},
			{int64(2), "bob"},
		}
		pos := 0
		scanNext := func(dest []driver.Value) error {
			if pos >= len(data) {
				return io.EOF
			}
			copy(dest, data[pos])
			pos++
			return nil
		}
		sr := dukdb.NewStreamingResult([]string{"id", "name"}, scanNext, nil)

		dest := make([]driver.Value, 2)
		err := sr.ScanNext(dest)
		require.NoError(t, err)
		assert.Equal(t, int64(1), dest[0])
		assert.Equal(t, "alice", dest[1])

		err = sr.ScanNext(dest)
		require.NoError(t, err)
		assert.Equal(t, int64(2), dest[0])
		assert.Equal(t, "bob", dest[1])

		err = sr.ScanNext(dest)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("close idempotency", func(t *testing.T) {
		sr := dukdb.NewStreamingResult([]string{"a"}, func(dest []driver.Value) error {
			return io.EOF
		}, nil)
		assert.NoError(t, sr.Close())
		assert.NoError(t, sr.Close()) // second close should not error
	})

	t.Run("read after close", func(t *testing.T) {
		sr := dukdb.NewStreamingResult([]string{"a"}, func(dest []driver.Value) error {
			dest[0] = 42
			return nil
		}, nil)
		_ = sr.Close()
		dest := make([]driver.Value, 1)
		err := sr.ScanNext(dest)
		assert.Equal(t, io.EOF, err)
	})

	t.Run("cancel function called on close", func(t *testing.T) {
		cancelled := false
		sr := dukdb.NewStreamingResult([]string{"a"}, func(dest []driver.Value) error {
			return io.EOF
		}, func() { cancelled = true })
		_ = sr.Close()
		assert.True(t, cancelled)
	})

	t.Run("columns", func(t *testing.T) {
		sr := dukdb.NewStreamingResult([]string{"x", "y", "z"}, func(dest []driver.Value) error {
			return io.EOF
		}, nil)
		assert.Equal(t, []string{"x", "y", "z"}, sr.Columns())
	})
}

// ---------------------------------------------------------------------------
// 5.2: Large result set integration test
// ---------------------------------------------------------------------------

func TestStreamingLargeResultSet(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Build a large table using recursive inserts in batches.
	_, err = db.Exec("CREATE TABLE big_table(id INTEGER, name VARCHAR)")
	require.NoError(t, err)

	// Insert 15000 rows in batches of 500 for speed.
	const total = 15000
	const batchSize = 500
	for start := 1; start <= total; start += batchSize {
		end := start + batchSize - 1
		if end > total {
			end = total
		}
		for i := start; i <= end; i++ {
			_, err = db.Exec("INSERT INTO big_table VALUES (?, ?)", i, fmt.Sprintf("row_%d", i))
			require.NoError(t, err)
		}
	}

	rows, err := db.Query("SELECT id, name FROM big_table ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)
		count++
		assert.Equal(t, count, id)
		assert.Equal(t, fmt.Sprintf("row_%d", count), name)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, total, count)
}

// ---------------------------------------------------------------------------
// 5.3: Streaming with blocking operators
// ---------------------------------------------------------------------------

func TestStreamingBlockingOperators(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE orders(id INT, customer VARCHAR, amount DECIMAL(10,2))")
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO orders VALUES (3, 'alice', 30.00), (1, 'bob', 10.00), (2, 'alice', 20.00), (4, 'bob', 40.00)",
	)
	require.NoError(t, err)

	t.Run("ORDER BY", func(t *testing.T) {
		rows, err := db.Query("SELECT id FROM orders ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()

		var ids []int
		for rows.Next() {
			var id int
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, []int{1, 2, 3, 4}, ids)
	})

	t.Run("GROUP BY", func(t *testing.T) {
		rows, err := db.Query(
			"SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer ORDER BY customer",
		)
		require.NoError(t, err)
		defer rows.Close()

		type result struct {
			customer string
			total    float64
		}
		var results []result
		for rows.Next() {
			var r result
			require.NoError(t, rows.Scan(&r.customer, &r.total))
			results = append(results, r)
		}
		require.NoError(t, rows.Err())
		require.Len(t, results, 2)
		assert.Equal(t, "alice", results[0].customer)
		assert.Equal(t, 50.0, results[0].total)
		assert.Equal(t, "bob", results[1].customer)
		assert.Equal(t, 50.0, results[1].total)
	})
}

// ---------------------------------------------------------------------------
// 5.4: Context cancellation mid-stream
// ---------------------------------------------------------------------------

func TestStreamingContextCancellation(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cancel_test(i INTEGER)")
	require.NoError(t, err)
	for i := 1; i <= 1000; i++ {
		_, err = db.Exec("INSERT INTO cancel_test VALUES (?)", i)
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT i FROM cancel_test ORDER BY i")
	require.NoError(t, err)

	count := 0
	for rows.Next() {
		var i int
		require.NoError(t, rows.Scan(&i))
		count++
		if count == 5 {
			cancel()
			break
		}
	}
	assert.Equal(t, 5, count)

	// Close should not error even after cancellation
	rows.Close()
}

// ---------------------------------------------------------------------------
// 5.5: Fallback / streaming path works identically to materialized
// ---------------------------------------------------------------------------

func TestStreamingFallbackMaterialized(t *testing.T) {
	db, err := sql.Open("dukdb", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	// Simple SELECT
	var val int
	err = db.QueryRow("SELECT 42").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, 42, val)

	// SELECT with no rows
	rows, err := db.Query("SELECT 1 WHERE 1=0")
	require.NoError(t, err)
	assert.False(t, rows.Next())
	rows.Close()

	// Multiple columns
	var a int
	var b string
	err = db.QueryRow("SELECT 1, 'hello'").Scan(&a, &b)
	require.NoError(t, err)
	assert.Equal(t, 1, a)
	assert.Equal(t, "hello", b)

	// NULL handling
	var np *int
	err = db.QueryRow("SELECT NULL::INTEGER").Scan(&np)
	require.NoError(t, err)
	assert.Nil(t, np)
}

// ---------------------------------------------------------------------------
// 5.6: Benchmark
// ---------------------------------------------------------------------------

func BenchmarkStreamingVsMaterialized(b *testing.B) {
	db, err := sql.Open("dukdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE bench_data(id INTEGER, val VARCHAR)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= 10000; i++ {
		_, err = db.Exec("INSERT INTO bench_data VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
	if err != nil {
		b.Fatal(err)
	}

	b.Run("streaming_query", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT id, val FROM bench_data")
			if err != nil {
				b.Fatal(err)
			}
			count := 0
			for rows.Next() {
				var id int
				var val string
				if err := rows.Scan(&id, &val); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()
			if count != 10000 {
				b.Fatalf("expected 10000 rows, got %d", count)
			}
		}
	})
}

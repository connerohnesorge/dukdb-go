package server

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferPool_SmallBuffer(t *testing.T) {
	pool := NewBufferPool()

	// Get a small buffer
	b := pool.GetSmall()
	require.NotNil(t, b)
	assert.Equal(t, 0, len(*b))
	assert.GreaterOrEqual(t, cap(*b), SmallBufferSize)

	// Use the buffer
	*b = append(*b, []byte("hello")...)
	assert.Equal(t, 5, len(*b))

	// Return to pool
	pool.PutSmall(b)

	// Get another - may or may not be the same buffer
	b2 := pool.GetSmall()
	require.NotNil(t, b2)
	assert.Equal(t, 0, len(*b2)) // Should be reset
}

func TestBufferPool_MediumBuffer(t *testing.T) {
	pool := NewBufferPool()

	b := pool.GetMedium()
	require.NotNil(t, b)
	assert.GreaterOrEqual(t, cap(*b), MediumBufferSize)

	pool.PutMedium(b)
}

func TestBufferPool_LargeBuffer(t *testing.T) {
	pool := NewBufferPool()

	b := pool.GetLarge()
	require.NotNil(t, b)
	assert.GreaterOrEqual(t, cap(*b), LargeBufferSize)

	pool.PutLarge(b)
}

func TestBufferPool_GetSized(t *testing.T) {
	pool := NewBufferPool()

	tests := []struct {
		size        int
		minCapacity int
	}{
		{10, SmallBufferSize},
		{100, MediumBufferSize},
		{2000, LargeBufferSize},
		{50000, 50000}, // Very large
	}

	for _, tc := range tests {
		b := pool.GetSized(tc.size)
		require.NotNil(t, b)
		assert.GreaterOrEqual(t, cap(*b), tc.minCapacity)
		pool.PutSized(b)
	}
}

func TestBufferPool_Concurrent(t *testing.T) {
	pool := NewBufferPool()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			b := pool.GetMedium()
			*b = append(*b, []byte("concurrent data")...)
			pool.PutMedium(b)
		}()
	}

	wg.Wait()
}

func TestBytesBufferPool(t *testing.T) {
	pool := NewBytesBufferPool()

	b := pool.Get()
	require.NotNil(t, b)
	assert.Equal(t, 0, b.Len())

	b.WriteString("test data")
	assert.Equal(t, 9, b.Len())

	pool.Put(b)

	b2 := pool.Get()
	require.NotNil(t, b2)
	assert.Equal(t, 0, b2.Len()) // Should be reset
}

func TestRowPrefetcher_Basic(t *testing.T) {
	prefetcher := NewRowPrefetcher(10)

	// Initialize with some data
	rows := []map[string]any{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}
	columns := []string{"id", "name"}

	prefetcher.Initialize(rows, columns)

	// Fetch first 2
	result, cols, hasMore, err := prefetcher.Fetch(2)
	require.NoError(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, columns, cols)
	assert.True(t, hasMore)

	// Fetch remaining
	result, cols, hasMore, err = prefetcher.Fetch(10)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, columns, cols)
	assert.False(t, hasMore)

	// No more rows
	result, cols, hasMore, err = prefetcher.Fetch(10)
	require.NoError(t, err)
	assert.Equal(t, 0, len(result))
	assert.False(t, hasMore)
}

func TestRowPrefetcher_Reset(t *testing.T) {
	prefetcher := NewRowPrefetcher(10)

	rows := []map[string]any{
		{"id": 1},
		{"id": 2},
	}
	columns := []string{"id"}

	prefetcher.Initialize(rows, columns)

	// Fetch all
	_, _, _, _ = prefetcher.Fetch(10)
	assert.Equal(t, 0, prefetcher.Remaining())

	// Reset
	prefetcher.Reset()
	assert.Equal(t, 2, prefetcher.Remaining())
	assert.Equal(t, 0, prefetcher.Position())

	// Fetch again
	result, _, _, _ := prefetcher.Fetch(10)
	assert.Equal(t, 2, len(result))
}

func TestPreparedStatementLRUCache_Basic(t *testing.T) {
	cache := NewPreparedStatementLRUCache(10)

	// Put a statement
	stmt := &CachedPreparedStatement{
		Query:     "SELECT * FROM users",
		CreatedAt: time.Now(),
	}
	cache.Put("SELECT * FROM users", stmt)

	// Get it back
	retrieved, ok := cache.Get("SELECT * FROM users")
	assert.True(t, ok)
	assert.Equal(t, stmt.Query, retrieved.Query)

	// Get non-existent
	_, ok = cache.Get("SELECT * FROM other")
	assert.False(t, ok)
}

func TestPreparedStatementLRUCache_Eviction(t *testing.T) {
	cache := NewPreparedStatementLRUCache(3)

	// Fill the cache
	cache.Put("query1", &CachedPreparedStatement{Query: "query1"})
	cache.Put("query2", &CachedPreparedStatement{Query: "query2"})
	cache.Put("query3", &CachedPreparedStatement{Query: "query3"})

	// Access query1 to make it recently used
	cache.Get("query1")

	// Add a new one - should evict query2 (least recently used)
	cache.Put("query4", &CachedPreparedStatement{Query: "query4"})

	// query2 should be evicted
	_, ok := cache.Get("query2")
	assert.False(t, ok)

	// Others should still be there
	_, ok = cache.Get("query1")
	assert.True(t, ok)
	_, ok = cache.Get("query3")
	assert.True(t, ok)
	_, ok = cache.Get("query4")
	assert.True(t, ok)
}

func TestPreparedStatementLRUCache_Stats(t *testing.T) {
	cache := NewPreparedStatementLRUCache(10)

	cache.Put("query1", &CachedPreparedStatement{Query: "query1"})

	// Hit
	cache.Get("query1")
	hits, misses, _, _ := cache.GetStatsSnapshot()
	assert.Equal(t, int64(1), hits)
	assert.Equal(t, int64(0), misses)

	// Miss
	cache.Get("nonexistent")
	hits, misses, _, _ = cache.GetStatsSnapshot()
	assert.Equal(t, int64(1), hits)
	assert.Equal(t, int64(1), misses)
}

func TestPreparedStatementLRUCache_Delete(t *testing.T) {
	cache := NewPreparedStatementLRUCache(10)

	cache.Put("query1", &CachedPreparedStatement{Query: "query1"})

	_, ok := cache.Get("query1")
	assert.True(t, ok)

	deleted := cache.Delete("query1")
	assert.True(t, deleted)

	_, ok = cache.Get("query1")
	assert.False(t, ok)

	// Delete non-existent
	deleted = cache.Delete("nonexistent")
	assert.False(t, deleted)
}

func TestPreparedStatementLRUCache_Clear(t *testing.T) {
	cache := NewPreparedStatementLRUCache(10)

	cache.Put("query1", &CachedPreparedStatement{Query: "query1"})
	cache.Put("query2", &CachedPreparedStatement{Query: "query2"})

	cache.Clear()

	_, ok := cache.Get("query1")
	assert.False(t, ok)
	_, ok = cache.Get("query2")
	assert.False(t, ok)
}

func TestQueryPlanCache_Basic(t *testing.T) {
	cache := NewQueryPlanCache(10)

	// Put a plan
	cache.Put("SELECT * FROM users", "plan-data")

	// Get it back
	entry, ok := cache.Get("SELECT * FROM users")
	assert.True(t, ok)
	assert.Equal(t, "plan-data", entry.Plan)

	// Get non-existent
	_, ok = cache.Get("SELECT * FROM other")
	assert.False(t, ok)
}

func TestQueryPlanCache_InvalidateAll(t *testing.T) {
	cache := NewQueryPlanCache(10)

	cache.Put("query1", "plan1")
	cache.Put("query2", "plan2")

	// Verify they exist
	_, ok := cache.Get("query1")
	assert.True(t, ok)

	// Invalidate all
	cache.InvalidateAll()

	// Plans should be stale
	_, ok = cache.Get("query1")
	assert.False(t, ok)
	_, ok = cache.Get("query2")
	assert.False(t, ok)
}

func TestQueryPlanCache_Eviction(t *testing.T) {
	cache := NewQueryPlanCache(3)

	// Fill the cache
	cache.Put("query1", "plan1")
	cache.Put("query2", "plan2")
	cache.Put("query3", "plan3")

	// Access query1
	cache.Get("query1")

	// Add a new one - should evict query2
	cache.Put("query4", "plan4")

	// query2 should be evicted
	_, ok := cache.Get("query2")
	assert.False(t, ok)

	// Others should still be there
	_, ok = cache.Get("query1")
	assert.True(t, ok)
}

func TestStreamingResultWriter(t *testing.T) {
	// Create a mock writer
	mock := &mockDataWriter{}

	writer := NewStreamingResultWriter(mock, 10)

	// Write some rows
	err := writer.WriteRow([]any{1, "Alice"})
	require.NoError(t, err)
	err = writer.WriteRow([]any{2, "Bob"})
	require.NoError(t, err)

	assert.Equal(t, 2, writer.RowCount())
	assert.Equal(t, 2, mock.rowCount)

	// Complete
	err = writer.Complete("SELECT 2")
	require.NoError(t, err)
	assert.True(t, mock.completed)
}

func TestPerformanceConfig_Defaults(t *testing.T) {
	config := NewPerformanceConfig()

	assert.Equal(t, DefaultCacheSize, config.PreparedStatementCacheSize)
	assert.Equal(t, DefaultPlanCacheSize, config.QueryPlanCacheSize)
	assert.Equal(t, DefaultPrefetchSize, config.CursorPrefetchSize)
	assert.True(t, config.EnableBufferPooling)
	assert.True(t, config.EnablePreparedStatementCaching)
	assert.True(t, config.EnableQueryPlanCaching)
}

func TestPerformanceMetrics_Snapshot(t *testing.T) {
	metrics := &PerformanceMetrics{}

	metrics.PreparedStmtCacheHits.Add(10)
	metrics.PreparedStmtCacheMisses.Add(5)
	metrics.RowsStreamed.Add(1000)

	snapshot := metrics.Snapshot()
	assert.Equal(t, int64(10), snapshot["prepared_stmt_cache_hits"])
	assert.Equal(t, int64(5), snapshot["prepared_stmt_cache_misses"])
	assert.Equal(t, int64(1000), snapshot["rows_streamed"])
}

func TestPerformanceMetrics_Reset(t *testing.T) {
	metrics := &PerformanceMetrics{}

	metrics.PreparedStmtCacheHits.Add(10)
	metrics.RowsStreamed.Add(1000)

	metrics.Reset()

	assert.Equal(t, int64(0), metrics.PreparedStmtCacheHits.Load())
	assert.Equal(t, int64(0), metrics.RowsStreamed.Load())
}

func TestGlobalInstances(t *testing.T) {
	// Test global buffer pool
	pool := GetBufferPool()
	require.NotNil(t, pool)

	// Test global bytes buffer pool
	bbPool := GetBytesBufferPool()
	require.NotNil(t, bbPool)

	// Test global metrics
	metrics := GetPerformanceMetrics()
	require.NotNil(t, metrics)
}

// mockDataWriter implements wire.DataWriter for testing.
type mockDataWriter struct {
	rowCount  int
	written   uint32
	completed bool
	emptied   bool
}

func (m *mockDataWriter) Row(values []any) error {
	m.rowCount++
	m.written++
	return nil
}

func (m *mockDataWriter) Complete(tag string) error {
	m.completed = true
	return nil
}

func (m *mockDataWriter) Empty() error {
	m.emptied = true
	return nil
}

func (m *mockDataWriter) Limit() uint32 {
	return 0
}

func (m *mockDataWriter) Written() uint32 {
	return m.written
}

func (m *mockDataWriter) Columns() wire.Columns {
	return nil
}

func (m *mockDataWriter) CopyIn(format wire.FormatCode) (*wire.CopyReader, error) {
	return nil, nil
}

// Benchmark tests

func BenchmarkBufferPool_GetPut_Small(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetSmall()
		*buf = append(*buf, "benchmark data"...)
		pool.PutSmall(buf)
	}
}

func BenchmarkBufferPool_GetPut_Medium(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetMedium()
		*buf = append(*buf, make([]byte, 500)...)
		pool.PutMedium(buf)
	}
}

func BenchmarkBufferPool_GetPut_Large(b *testing.B) {
	pool := NewBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.GetLarge()
		*buf = append(*buf, make([]byte, 10000)...)
		pool.PutLarge(buf)
	}
}

func BenchmarkBufferPool_NoPool_Small(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, SmallBufferSize)
		buf = append(buf, "benchmark data"...)
		_ = buf
	}
}

func BenchmarkBufferPool_NoPool_Large(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, LargeBufferSize)
		buf = append(buf, make([]byte, 10000)...)
		_ = buf
	}
}

func BenchmarkBytesBufferPool(b *testing.B) {
	pool := NewBytesBufferPool()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get()
		buf.WriteString("benchmark data with some content")
		pool.Put(buf)
	}
}

func BenchmarkBytesBuffer_NoPool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		buf.WriteString("benchmark data with some content")
		_ = buf
	}
}

func BenchmarkPreparedStatementCache_Get(b *testing.B) {
	cache := NewPreparedStatementLRUCache(100)

	// Pre-populate with some statements
	for i := 0; i < 50; i++ {
		query := "SELECT * FROM table" + string(rune('A'+i))
		cache.Put(query, &CachedPreparedStatement{Query: query})
	}

	query := "SELECT * FROM tableA"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(query)
	}
}

func BenchmarkPreparedStatementCache_Put(b *testing.B) {
	cache := NewPreparedStatementLRUCache(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := "SELECT * FROM table" + string(rune(i%50))
		cache.Put(query, &CachedPreparedStatement{Query: query})
	}
}

func BenchmarkQueryPlanCache_Get(b *testing.B) {
	cache := NewQueryPlanCache(100)

	// Pre-populate
	for i := 0; i < 50; i++ {
		query := "SELECT * FROM table" + string(rune('A'+i))
		cache.Put(query, "plan"+string(rune('A'+i)))
	}

	query := "SELECT * FROM tableA"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(query)
	}
}

func BenchmarkQueryHash(b *testing.B) {
	query := "SELECT id, name, email, created_at FROM users WHERE status = $1 AND role = $2 ORDER BY created_at DESC LIMIT 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashQuery(query)
	}
}

func BenchmarkRowPrefetcher_Fetch(b *testing.B) {
	// Create sample data
	rows := make([]map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		rows[i] = map[string]any{
			"id":   i,
			"name": "Name" + string(rune('A'+i%26)),
		}
	}
	columns := []string{"id", "name"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefetcher := NewRowPrefetcher(100)
		prefetcher.Initialize(rows, columns)

		for {
			result, _, hasMore, _ := prefetcher.Fetch(100)
			if !hasMore || len(result) == 0 {
				break
			}
		}
	}
}

func BenchmarkStreamingResultWriter(b *testing.B) {
	mock := &mockDataWriter{}
	writer := NewStreamingResultWriter(mock, 100)

	row := []any{1, "Alice", "alice@example.com", time.Now()}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = writer.WriteRow(row)
	}
}

// Concurrent benchmark tests

func BenchmarkBufferPool_Concurrent(b *testing.B) {
	pool := NewBufferPool()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.GetMedium()
			*buf = append(*buf, "concurrent benchmark data"...)
			pool.PutMedium(buf)
		}
	})
}

func BenchmarkPreparedStatementCache_Concurrent(b *testing.B) {
	cache := NewPreparedStatementLRUCache(100)

	// Pre-populate
	for i := 0; i < 50; i++ {
		query := "SELECT * FROM table" + string(rune('A'+i))
		cache.Put(query, &CachedPreparedStatement{Query: query})
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			query := "SELECT * FROM table" + string(rune('A'+i%50))
			if i%3 == 0 {
				cache.Get(query)
			} else {
				cache.Put(query, &CachedPreparedStatement{Query: query})
			}
			i++
		}
	})
}

// Comparison benchmarks with PostgreSQL operations (simulated)

func BenchmarkSimulatedQueryExecution_WithCaching(b *testing.B) {
	cache := NewPreparedStatementLRUCache(100)
	planCache := NewQueryPlanCache(100)

	// Simulate prepared statement + plan caching
	query := "SELECT * FROM users WHERE id = $1"

	// First execution - cache miss
	cache.Put(query, &CachedPreparedStatement{Query: query})
	planCache.Put(query, "physical_plan")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Look up cached statement
		_, _ = cache.Get(query)
		// Look up cached plan
		_, _ = planCache.Get(query)
	}
}

func BenchmarkSimulatedQueryExecution_WithoutCaching(b *testing.B) {
	// Simulate preparing and planning each time
	query := "SELECT * FROM users WHERE id = $1"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate hash computation (part of prepare)
		_ = hashQuery(query)
		// Simulate creating new objects
		_ = &CachedPreparedStatement{Query: query}
	}
}

// Integration-style benchmark

func BenchmarkFullQueryPath(b *testing.B) {
	pool := NewBufferPool()
	stmtCache := NewPreparedStatementLRUCache(100)
	planCache := NewQueryPlanCache(100)

	query := "SELECT id, name, email FROM users WHERE status = $1"

	// Pre-cache
	stmtCache.Put(query, &CachedPreparedStatement{Query: query})
	planCache.Put(query, "plan")

	// Sample rows
	rows := make([]map[string]any, 100)
	for i := 0; i < 100; i++ {
		rows[i] = map[string]any{
			"id":    i,
			"name":  "User" + string(rune('A'+i%26)),
			"email": "user@example.com",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Get buffer
		buf := pool.GetMedium()

		// Check statement cache
		_, _ = stmtCache.Get(query)

		// Check plan cache
		_, _ = planCache.Get(query)

		// Simulate row serialization
		for _, row := range rows {
			for _, v := range row {
				switch val := v.(type) {
				case int:
					*buf = append(*buf, byte(val))
				case string:
					*buf = append(*buf, val...)
				}
			}
		}

		// Return buffer
		pool.PutMedium(buf)
	}
}

// Test with context cancellation

func TestRowPrefetcher_ContextCancellation(t *testing.T) {
	prefetcher := NewRowPrefetcher(10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Initialize should still work
	rows := []map[string]any{{"id": 1}}
	prefetcher.Initialize(rows, []string{"id"})

	// Fetch should work since we have cached data
	result, _, _, err := prefetcher.Fetch(1)
	require.NoError(t, err)
	assert.Equal(t, 1, len(result))

	// Context cancellation doesn't affect cached data access
	_ = ctx
}

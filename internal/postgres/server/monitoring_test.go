package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pgcatalog "github.com/dukdb/dukdb-go/internal/postgres/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsCollector(t *testing.T) {
	mc := NewMetricsCollector()
	require.NotNil(t, mc)
	assert.True(t, mc.IsEnabled())

	// Test StartQuery and EndQuery
	mc.StartQuery(1, "SELECT * FROM users")
	time.Sleep(10 * time.Millisecond)
	mc.EndQuery(1, "SELECT * FROM users", 10*time.Millisecond, 5, nil)

	// Check global stats
	stats := mc.GetGlobalStats()
	assert.Equal(t, int64(1), stats.TotalQueries)
	assert.Equal(t, int64(0), stats.TotalErrors)

	// Test error tracking
	mc.StartQuery(2, "SELECT * FROM nonexistent")
	mc.EndQuery(2, "SELECT * FROM nonexistent", 5*time.Millisecond, 0, NewPgError("42P01", "table not found"))

	stats = mc.GetGlobalStats()
	assert.Equal(t, int64(2), stats.TotalQueries)
	assert.Equal(t, int64(1), stats.TotalErrors)

	// Check error counts
	errorCounts := mc.GetErrorCounts()
	assert.Equal(t, int64(1), errorCounts["42P01"])
}

func TestMetricsCollectorStatementStats(t *testing.T) {
	mc := NewMetricsCollector()

	// Execute the same query multiple times
	query := "SELECT id FROM users WHERE id = $1"
	for i := 0; i < 10; i++ {
		mc.StartQuery(uint64(i), query)
		mc.EndQuery(uint64(i), query, time.Duration(i+1)*time.Millisecond, int64(i), nil)
	}

	// Get statement stats
	stats := mc.GetStatementStats()
	require.Len(t, stats, 1)

	s := stats[0]
	assert.Equal(t, int64(10), s.Calls)
	assert.Equal(t, float64(1), s.MinTime) // 1ms
	assert.Equal(t, float64(10), s.MaxTime) // 10ms
	assert.Greater(t, s.MeanTime, float64(0))
}

func TestMetricsCollectorActiveQueries(t *testing.T) {
	mc := NewMetricsCollector()

	// Start multiple queries
	mc.StartQuery(1, "SELECT * FROM users")
	mc.StartQuery(2, "SELECT * FROM orders")
	mc.StartQuery(3, "SELECT * FROM products")

	// Check active queries
	active := mc.GetActiveQueries()
	assert.Len(t, active, 3)

	// End one query
	mc.EndQuery(2, "SELECT * FROM orders", 5*time.Millisecond, 10, nil)

	active = mc.GetActiveQueries()
	assert.Len(t, active, 2)
}

func TestMetricsCollectorHistogram(t *testing.T) {
	mc := NewMetricsCollector()

	// Record queries in different buckets
	mc.EndQuery(1, "fast query", 500*time.Microsecond, 1, nil)   // 0-1ms bucket (bucket 0)
	mc.EndQuery(2, "medium query", 3*time.Millisecond, 1, nil)   // 1-5ms bucket (bucket 1)
	mc.EndQuery(3, "slow query", 75*time.Millisecond, 1, nil)    // 50-100ms bucket (bucket 4)
	mc.EndQuery(4, "very slow query", 2*time.Second, 1, nil)     // 1-5s bucket (bucket 7)

	stats := mc.GetGlobalStats()
	histogram := stats.Histogram

	// Verify total queries
	assert.Equal(t, int64(4), stats.TotalQueries)

	// Verify buckets (cumulative check - at least one query in each expected bucket)
	// The histogram uses ms for bucketing, so:
	// 500us = 0ms -> bucket 0
	// 3ms -> bucket 1
	// 75ms -> bucket 4
	// 2000ms -> bucket 7
	assert.GreaterOrEqual(t, histogram[0], int64(1)) // 0-1ms
	assert.GreaterOrEqual(t, histogram[1], int64(1)) // 1-5ms
	assert.GreaterOrEqual(t, histogram[4], int64(1)) // 50-100ms
	assert.GreaterOrEqual(t, histogram[7], int64(1)) // 1-5s
}

func TestMetricsCollectorDisable(t *testing.T) {
	mc := NewMetricsCollector()
	mc.Disable()

	// Queries should not be tracked when disabled
	mc.StartQuery(1, "SELECT 1")
	mc.EndQuery(1, "SELECT 1", time.Millisecond, 1, nil)

	stats := mc.GetGlobalStats()
	assert.Equal(t, int64(0), stats.TotalQueries)

	// Re-enable
	mc.Enable()
	mc.StartQuery(2, "SELECT 2")
	mc.EndQuery(2, "SELECT 2", time.Millisecond, 1, nil)

	stats = mc.GetGlobalStats()
	assert.Equal(t, int64(1), stats.TotalQueries)
}

func TestMetricsCollectorReset(t *testing.T) {
	mc := NewMetricsCollector()

	// Add some metrics
	mc.EndQuery(1, "SELECT 1", time.Millisecond, 1, nil)
	mc.EndQuery(2, "SELECT 2", time.Millisecond, 1, NewPgError("XX000", "error"))

	// Reset
	mc.Reset()

	stats := mc.GetGlobalStats()
	assert.Equal(t, int64(0), stats.TotalQueries)
	assert.Equal(t, int64(0), stats.TotalErrors)
	assert.Len(t, mc.GetStatementStats(), 0)
	assert.Len(t, mc.GetErrorCounts(), 0)
}

func TestPrometheusMetrics(t *testing.T) {
	mc := NewMetricsCollector()

	// Add some metrics
	mc.EndQuery(1, "SELECT * FROM users", 5*time.Millisecond, 10, nil)
	mc.EndQuery(2, "INSERT INTO users VALUES (1)", 2*time.Millisecond, 1, nil)
	mc.EndQuery(3, "SELECT bad", time.Millisecond, 0, NewPgError("42601", "syntax error"))

	// Create Prometheus metrics handler
	pm := NewPrometheusMetrics(mc, nil)

	// Create test HTTP request
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	pm.ServeHTTP(rec, req)

	// Check response
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain; version=0.0.4", rec.Header().Get("Content-Type"))

	body := rec.Body.String()

	// Check that expected metrics are present
	assert.Contains(t, body, "dukdb_queries_total")
	assert.Contains(t, body, "dukdb_query_errors_total")
	assert.Contains(t, body, "dukdb_query_duration_ms_bucket")
	assert.Contains(t, body, "dukdb_query_errors_by_state")
	assert.Contains(t, body, "42601") // The error SQLSTATE
}

func TestObservabilityManager(t *testing.T) {
	om := NewObservabilityManager(nil, nil)
	require.NotNil(t, om)

	// Check that metrics collector is initialized
	mc := om.MetricsCollector()
	require.NotNil(t, mc)

	// Check that tracer is initialized (noop by default)
	tracer := om.Tracer()
	require.NotNil(t, tracer)

	// Record some metrics
	mc.EndQuery(1, "SELECT 1", time.Millisecond, 1, nil)

	stats := mc.GetGlobalStats()
	assert.Equal(t, int64(1), stats.TotalQueries)
}

func TestQueryTracer(t *testing.T) {
	// Test with noop tracer
	tracer := NewNoopTracer()
	qt := NewQueryTracer(tracer)

	// Execute traced query
	var executed bool
	err := qt.TraceQuery(context.Background(), "SELECT * FROM users", func(ctx context.Context) error {
		executed = true
		return nil
	})

	assert.NoError(t, err)
	assert.True(t, executed)

	// Test with error
	testErr := NewPgError("42601", "syntax error")
	err = qt.TraceQuery(context.Background(), "SELECT bad syntax", func(ctx context.Context) error {
		return testErr
	})

	assert.Equal(t, testErr, err)
}

func TestGetOperationType(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{"SELECT * FROM users", "SELECT"},
		{"INSERT INTO users VALUES (1)", "INSERT"},
		{"UPDATE users SET name = 'test'", "UPDATE"},
		{"DELETE FROM users WHERE id = 1", "DELETE"},
		{"CREATE TABLE test (id INT)", "CREATE"},
		{"DROP TABLE test", "DROP"},
		{"BEGIN", "BEGIN"},
		{"COMMIT", "COMMIT"},
		{"  select * from users", "SELECT"},
		{"", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := getOperationType(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExecutionTimeHistogramBuckets(t *testing.T) {
	h := &ExecutionTimeHistogram{}

	// Test bucket labels
	labels := h.BucketLabels()
	assert.Len(t, labels, 9)
	assert.Equal(t, "0-1ms", labels[0])
	assert.Equal(t, "5s+", labels[8])

	// Test bucket recording
	testCases := []struct {
		duration       time.Duration
		expectedBucket int
	}{
		{500 * time.Microsecond, 0}, // 0-1ms
		{2 * time.Millisecond, 1},   // 1-5ms
		{7 * time.Millisecond, 2},   // 5-10ms
		{25 * time.Millisecond, 3},  // 10-50ms
		{75 * time.Millisecond, 4},  // 50-100ms
		{250 * time.Millisecond, 5}, // 100-500ms
		{750 * time.Millisecond, 6}, // 500-1000ms
		{3 * time.Second, 7},        // 1-5s
		{10 * time.Second, 8},       // 5s+
	}

	for _, tc := range testCases {
		h.Record(tc.duration)
	}

	buckets := h.GetBuckets()
	for i, tc := range testCases {
		if buckets[tc.expectedBucket] == 0 {
			t.Errorf("test case %d: expected bucket %d to be non-zero for duration %v",
				i, tc.expectedBucket, tc.duration)
		}
	}
}

func TestServerStatementStatsProvider(t *testing.T) {
	mc := NewMetricsCollector()
	provider := NewServerStatementStatsProvider(mc)

	// Add some stats to the collector
	mc.EndQuery(1, "SELECT * FROM users", 5*time.Millisecond, 10, nil)
	mc.EndQuery(2, "SELECT * FROM users", 3*time.Millisecond, 5, nil)
	mc.EndQuery(3, "INSERT INTO log VALUES (1)", 2*time.Millisecond, 1, nil)

	// Get stats through provider
	stats := provider.GetStatementStats()
	assert.Len(t, stats, 2) // Two distinct queries

	// Find the SELECT query stats
	var selectStats *pgcatalog.StatementStatsEntry
	for _, s := range stats {
		if strings.Contains(s.Query, "SELECT") {
			selectStats = s
			break
		}
	}

	require.NotNil(t, selectStats)
	assert.Equal(t, int64(2), selectStats.Calls)
	assert.Equal(t, int64(15), selectStats.Rows) // 10 + 5
}

func TestServerLockProvider(t *testing.T) {
	provider := NewServerLockProvider(nil)

	// With nil server, should return empty
	locks := provider.GetLocks()
	assert.Empty(t, locks)
}

func TestFormatVirtualXID(t *testing.T) {
	xid := formatVirtualXID(12345)
	assert.Equal(t, "12345/1", xid)
}

// Ensure pgcatalog import is used
var _ pgcatalog.StatementStatsEntry

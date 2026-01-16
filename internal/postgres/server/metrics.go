// Package server provides PostgreSQL wire protocol compatibility for dukdb-go.
// This file implements query execution metrics and statistics collection.
package server

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsCollector collects query execution metrics for monitoring and observability.
// It tracks statement statistics compatible with pg_stat_statements and provides
// data for pg_stat_activity.
type MetricsCollector struct {
	mu sync.RWMutex

	// statements tracks per-query statistics (keyed by query fingerprint).
	statements map[uint64]*StatementStats

	// activeQueries tracks currently running queries.
	activeQueries map[uint64]*ActiveQuery

	// totalQueries is the total number of queries executed.
	totalQueries atomic.Int64

	// totalErrors is the total number of query errors.
	totalErrors atomic.Int64

	// totalRowsReturned is the total number of rows returned.
	totalRowsReturned atomic.Int64

	// totalRowsAffected is the total number of rows affected by DML.
	totalRowsAffected atomic.Int64

	// executionTimeHistogram tracks query execution time distribution.
	executionTimeHistogram *ExecutionTimeHistogram

	// errorCounts tracks errors by SQLSTATE code.
	errorCounts map[string]*atomic.Int64
	errorMu     sync.RWMutex

	// maxStatements is the maximum number of distinct statements to track.
	maxStatements int

	// enabled indicates whether metrics collection is enabled.
	enabled atomic.Bool

	// startTime is when the collector was started.
	startTime time.Time
}

// StatementStats holds statistics for a single statement type.
// This is compatible with pg_stat_statements.
type StatementStats struct {
	mu sync.RWMutex

	// QueryID is a unique identifier for this statement type (hash of normalized query).
	QueryID uint64

	// Query is the normalized query text (with parameters replaced by $1, $2, etc.).
	Query string

	// Calls is the number of times this statement was executed.
	Calls int64

	// TotalTime is the total time spent executing this statement (in milliseconds).
	TotalTime float64

	// MinTime is the minimum execution time (in milliseconds).
	MinTime float64

	// MaxTime is the maximum execution time (in milliseconds).
	MaxTime float64

	// MeanTime is the mean execution time (in milliseconds).
	MeanTime float64

	// StddevTime is the population standard deviation of execution time.
	StddevTime float64

	// Rows is the total number of rows returned or affected.
	Rows int64

	// SharedBlksHit is the total number of shared block cache hits (simulated).
	SharedBlksHit int64

	// SharedBlksRead is the total number of shared blocks read (simulated).
	SharedBlksRead int64

	// LocalBlksHit is the total number of local block cache hits (simulated).
	LocalBlksHit int64

	// LocalBlksRead is the total number of local blocks read (simulated).
	LocalBlksRead int64

	// BlkReadTime is the total time spent reading blocks (simulated, milliseconds).
	BlkReadTime float64

	// BlkWriteTime is the total time spent writing blocks (simulated, milliseconds).
	BlkWriteTime float64

	// sumSquares is used for stddev calculation.
	sumSquares float64
}

// ActiveQuery represents a currently executing query for pg_stat_activity.
type ActiveQuery struct {
	// SessionID is the session that is running this query.
	SessionID uint64

	// Query is the query text being executed.
	Query string

	// QueryStart is when the query started executing.
	QueryStart time.Time

	// State is the current state (active, idle, etc.).
	State string

	// WaitEvent is what the backend is waiting for, if any.
	WaitEvent string

	// WaitEventType is the type of wait event.
	WaitEventType string
}

// ExecutionTimeHistogram tracks the distribution of query execution times.
type ExecutionTimeHistogram struct {
	// Buckets in milliseconds: [0-1), [1-5), [5-10), [10-50), [50-100), [100-500), [500-1000), [1000-5000), [5000+)
	buckets [9]atomic.Int64
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	mc := &MetricsCollector{
		statements:             make(map[uint64]*StatementStats),
		activeQueries:          make(map[uint64]*ActiveQuery),
		executionTimeHistogram: &ExecutionTimeHistogram{},
		errorCounts:            make(map[string]*atomic.Int64),
		maxStatements:          5000, // Default max statements to track
		startTime:              time.Now(),
	}
	mc.enabled.Store(true)
	return mc
}

// Enable enables metrics collection.
func (mc *MetricsCollector) Enable() {
	mc.enabled.Store(true)
}

// Disable disables metrics collection.
func (mc *MetricsCollector) Disable() {
	mc.enabled.Store(false)
}

// IsEnabled returns whether metrics collection is enabled.
func (mc *MetricsCollector) IsEnabled() bool {
	return mc.enabled.Load()
}

// SetMaxStatements sets the maximum number of distinct statements to track.
func (mc *MetricsCollector) SetMaxStatements(max int) {
	mc.mu.Lock()
	mc.maxStatements = max
	mc.mu.Unlock()
}

// StartQuery records the start of a query execution.
func (mc *MetricsCollector) StartQuery(sessionID uint64, query string) {
	if !mc.enabled.Load() {
		return
	}

	mc.mu.Lock()
	mc.activeQueries[sessionID] = &ActiveQuery{
		SessionID:  sessionID,
		Query:      truncateQuery(query, 1024),
		QueryStart: time.Now(),
		State:      "active",
	}
	mc.mu.Unlock()
}

// EndQuery records the end of a query execution.
func (mc *MetricsCollector) EndQuery(sessionID uint64, query string, duration time.Duration, rowsAffected int64, err error) {
	if !mc.enabled.Load() {
		return
	}

	// Remove from active queries
	mc.mu.Lock()
	delete(mc.activeQueries, sessionID)
	mc.mu.Unlock()

	// Update global counters
	mc.totalQueries.Add(1)
	if rowsAffected > 0 {
		mc.totalRowsAffected.Add(rowsAffected)
	}

	// Track errors
	if err != nil {
		mc.totalErrors.Add(1)
		mc.recordError(err)
	}

	// Update histogram
	mc.executionTimeHistogram.Record(duration)

	// Update statement statistics
	mc.recordStatementStats(query, duration, rowsAffected)
}

// RecordRowsReturned records the number of rows returned by a query.
func (mc *MetricsCollector) RecordRowsReturned(count int64) {
	if !mc.enabled.Load() {
		return
	}
	mc.totalRowsReturned.Add(count)
}

// recordError records an error by SQLSTATE code.
func (mc *MetricsCollector) recordError(err error) {
	sqlState := "XX000" // Internal error default

	// Try to extract SQLSTATE from PgError
	if pgErr, ok := err.(*PgError); ok {
		sqlState = pgErr.Code
	}

	mc.errorMu.Lock()
	counter, ok := mc.errorCounts[sqlState]
	if !ok {
		counter = &atomic.Int64{}
		mc.errorCounts[sqlState] = counter
	}
	mc.errorMu.Unlock()

	counter.Add(1)
}

// recordStatementStats records statistics for a statement.
func (mc *MetricsCollector) recordStatementStats(query string, duration time.Duration, rows int64) {
	queryID := hashQueryForMetrics(query)
	durationMs := float64(duration.Microseconds()) / 1000.0

	mc.mu.Lock()

	stats, ok := mc.statements[queryID]
	if !ok {
		// Check if we're at the limit
		if len(mc.statements) >= mc.maxStatements {
			mc.mu.Unlock()
			return
		}
		stats = &StatementStats{
			QueryID: queryID,
			Query:   truncateQuery(normalizeQuery(query), 4096),
			MinTime: durationMs,
			MaxTime: durationMs,
		}
		mc.statements[queryID] = stats
	}

	mc.mu.Unlock()

	// Update stats (with its own lock)
	stats.mu.Lock()
	stats.Calls++
	stats.TotalTime += durationMs
	stats.Rows += rows
	stats.sumSquares += durationMs * durationMs

	if durationMs < stats.MinTime {
		stats.MinTime = durationMs
	}
	if durationMs > stats.MaxTime {
		stats.MaxTime = durationMs
	}
	stats.MeanTime = stats.TotalTime / float64(stats.Calls)
	if stats.Calls > 1 {
		// Calculate stddev using Welford's algorithm
		variance := (stats.sumSquares - (stats.TotalTime*stats.TotalTime)/float64(stats.Calls)) / float64(stats.Calls)
		if variance > 0 {
			stats.StddevTime = sqrt(variance)
		}
	}
	stats.mu.Unlock()
}

// GetActiveQueries returns all currently active queries.
func (mc *MetricsCollector) GetActiveQueries() []*ActiveQuery {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	queries := make([]*ActiveQuery, 0, len(mc.activeQueries))
	for _, q := range mc.activeQueries {
		queries = append(queries, &ActiveQuery{
			SessionID:     q.SessionID,
			Query:         q.Query,
			QueryStart:    q.QueryStart,
			State:         q.State,
			WaitEvent:     q.WaitEvent,
			WaitEventType: q.WaitEventType,
		})
	}
	return queries
}

// GetStatementStats returns all tracked statement statistics.
func (mc *MetricsCollector) GetStatementStats() []*StatementStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	stats := make([]*StatementStats, 0, len(mc.statements))
	for _, s := range mc.statements {
		s.mu.RLock()
		statsCopy := &StatementStats{
			QueryID:        s.QueryID,
			Query:          s.Query,
			Calls:          s.Calls,
			TotalTime:      s.TotalTime,
			MinTime:        s.MinTime,
			MaxTime:        s.MaxTime,
			MeanTime:       s.MeanTime,
			StddevTime:     s.StddevTime,
			Rows:           s.Rows,
			SharedBlksHit:  s.SharedBlksHit,
			SharedBlksRead: s.SharedBlksRead,
			LocalBlksHit:   s.LocalBlksHit,
			LocalBlksRead:  s.LocalBlksRead,
			BlkReadTime:    s.BlkReadTime,
			BlkWriteTime:   s.BlkWriteTime,
		}
		s.mu.RUnlock()
		stats = append(stats, statsCopy)
	}
	return stats
}

// GetGlobalStats returns global query statistics.
func (mc *MetricsCollector) GetGlobalStats() GlobalQueryStats {
	return GlobalQueryStats{
		TotalQueries:      mc.totalQueries.Load(),
		TotalErrors:       mc.totalErrors.Load(),
		TotalRowsReturned: mc.totalRowsReturned.Load(),
		TotalRowsAffected: mc.totalRowsAffected.Load(),
		Uptime:            time.Since(mc.startTime),
		Histogram:         mc.executionTimeHistogram.GetBuckets(),
	}
}

// GetErrorCounts returns error counts by SQLSTATE code.
func (mc *MetricsCollector) GetErrorCounts() map[string]int64 {
	mc.errorMu.RLock()
	defer mc.errorMu.RUnlock()

	result := make(map[string]int64, len(mc.errorCounts))
	for code, counter := range mc.errorCounts {
		result[code] = counter.Load()
	}
	return result
}

// Reset clears all collected metrics.
func (mc *MetricsCollector) Reset() {
	mc.mu.Lock()
	mc.statements = make(map[uint64]*StatementStats)
	mc.activeQueries = make(map[uint64]*ActiveQuery)
	mc.mu.Unlock()

	mc.errorMu.Lock()
	mc.errorCounts = make(map[string]*atomic.Int64)
	mc.errorMu.Unlock()

	mc.totalQueries.Store(0)
	mc.totalErrors.Store(0)
	mc.totalRowsReturned.Store(0)
	mc.totalRowsAffected.Store(0)
	mc.executionTimeHistogram = &ExecutionTimeHistogram{}
	mc.startTime = time.Now()
}

// GlobalQueryStats holds global query statistics.
type GlobalQueryStats struct {
	TotalQueries      int64
	TotalErrors       int64
	TotalRowsReturned int64
	TotalRowsAffected int64
	Uptime            time.Duration
	Histogram         [9]int64
}

// Record records an execution time in the appropriate bucket.
func (h *ExecutionTimeHistogram) Record(duration time.Duration) {
	ms := duration.Milliseconds()

	var bucket int
	switch {
	case ms < 1:
		bucket = 0
	case ms < 5:
		bucket = 1
	case ms < 10:
		bucket = 2
	case ms < 50:
		bucket = 3
	case ms < 100:
		bucket = 4
	case ms < 500:
		bucket = 5
	case ms < 1000:
		bucket = 6
	case ms < 5000:
		bucket = 7
	default:
		bucket = 8
	}

	h.buckets[bucket].Add(1)
}

// GetBuckets returns the current bucket counts.
func (h *ExecutionTimeHistogram) GetBuckets() [9]int64 {
	var result [9]int64
	for i := range h.buckets {
		result[i] = h.buckets[i].Load()
	}
	return result
}

// BucketLabels returns human-readable labels for histogram buckets.
func (h *ExecutionTimeHistogram) BucketLabels() []string {
	return []string{
		"0-1ms",
		"1-5ms",
		"5-10ms",
		"10-50ms",
		"50-100ms",
		"100-500ms",
		"500-1000ms",
		"1-5s",
		"5s+",
	}
}

// hashQueryForMetrics generates a hash for a query to use as a statement ID.
func hashQueryForMetrics(query string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(query))
	return h.Sum64()
}

// normalizeQuery normalizes a query by replacing literal values with placeholders.
// This is a simplified normalization - a full implementation would use the parser.
func normalizeQuery(query string) string {
	// For now, just return the query as-is
	// A full implementation would replace literals with $1, $2, etc.
	return query
}

// truncateQuery truncates a query to the specified maximum length.
func truncateQuery(query string, maxLen int) string {
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen-3] + "..."
}

// sqrt calculates the square root using Newton's method.
// This avoids importing math package.
func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 10; i++ {
		z = z - (z*z-x)/(2*z)
	}
	return z
}

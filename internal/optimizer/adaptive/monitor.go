package adaptive

import (
	"sync"
	"sync/atomic"
	"time"
)

// OperatorStats captures execution statistics for a single operator.
type OperatorStats struct {
	EstimatedRows int64
	ActualRows    int64
	EstimatedCost float64
	ActualCost    float64
	CPUTimeNanos  int64
	MemoryBytes   int64
	IOBytes       int64
	StartedAt     time.Time
	FinishedAt    time.Time
}

// QueryStats captures execution statistics for a query.
type QueryStats struct {
	QueryID    string
	StartedAt  time.Time
	FinishedAt time.Time
	Operators  map[string]OperatorStats
}

// Monitor collects runtime statistics with minimal overhead.
type Monitor struct {
	mu      sync.RWMutex
	queries map[string]*QueryStats
	enabled atomic.Bool
}

// NewMonitor creates a new execution monitor.
func NewMonitor(enabled bool) *Monitor {
	monitor := &Monitor{queries: make(map[string]*QueryStats)}
	monitor.enabled.Store(enabled)
	return monitor
}

// SetEnabled toggles monitoring.
func (m *Monitor) SetEnabled(enabled bool) {
	if m == nil {
		return
	}
	m.enabled.Store(enabled)
}

// StartQuery records query start.
func (m *Monitor) StartQuery(queryID string) {
	if m == nil || !m.enabled.Load() || queryID == "" {
		return
	}
	m.mu.Lock()
	m.queries[queryID] = &QueryStats{QueryID: queryID, StartedAt: time.Now(), Operators: make(map[string]OperatorStats)}
	m.mu.Unlock()
}

// StartOperator records operator start with estimated values.
func (m *Monitor) StartOperator(queryID, operator string, estimatedRows int64, estimatedCost float64) {
	if m == nil || !m.enabled.Load() || queryID == "" || operator == "" {
		return
	}

	m.mu.Lock()
	query := m.queries[queryID]
	if query == nil {
		query = &QueryStats{QueryID: queryID, StartedAt: time.Now(), Operators: make(map[string]OperatorStats)}
		m.queries[queryID] = query
	}
	query.Operators[operator] = OperatorStats{
		EstimatedRows: estimatedRows,
		EstimatedCost: estimatedCost,
		StartedAt:     time.Now(),
	}
	m.mu.Unlock()
}

// EndOperator records actual execution stats for an operator.
func (m *Monitor) EndOperator(queryID, operator string, actualRows int64, actualCost float64, cpuNanos, memoryBytes, ioBytes int64) {
	if m == nil || !m.enabled.Load() || queryID == "" || operator == "" {
		return
	}

	m.mu.Lock()
	query := m.queries[queryID]
	if query == nil {
		m.mu.Unlock()
		return
	}
	stats := query.Operators[operator]
	stats.ActualRows = actualRows
	stats.ActualCost = actualCost
	stats.CPUTimeNanos = cpuNanos
	stats.MemoryBytes = memoryBytes
	stats.IOBytes = ioBytes
	stats.FinishedAt = time.Now()
	query.Operators[operator] = stats
	m.mu.Unlock()
}

// FinishQuery marks query completion and returns a snapshot.
func (m *Monitor) FinishQuery(queryID string) QueryStats {
	if m == nil || queryID == "" {
		return QueryStats{}
	}

	m.mu.Lock()
	query := m.queries[queryID]
	if query == nil {
		m.mu.Unlock()
		return QueryStats{}
	}
	query.FinishedAt = time.Now()
	snapshot := copyQueryStats(query)
	delete(m.queries, queryID)
	m.mu.Unlock()
	return snapshot
}

// Snapshot returns a snapshot without removing it.
func (m *Monitor) Snapshot(queryID string) QueryStats {
	if m == nil || queryID == "" {
		return QueryStats{}
	}
	m.mu.RLock()
	query := m.queries[queryID]
	snapshot := copyQueryStats(query)
	m.mu.RUnlock()
	return snapshot
}

func copyQueryStats(stats *QueryStats) QueryStats {
	if stats == nil {
		return QueryStats{}
	}
	operators := make(map[string]OperatorStats, len(stats.Operators))
	for key, value := range stats.Operators {
		operators[key] = value
	}
	return QueryStats{
		QueryID:    stats.QueryID,
		StartedAt:  stats.StartedAt,
		FinishedAt: stats.FinishedAt,
		Operators:  operators,
	}
}

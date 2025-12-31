package dukdb

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coder/quartz"
)

// ProfilingInfo is a recursive type containing metrics for each node in the query plan.
// There are two types of nodes: the QUERY_ROOT and OPERATOR nodes.
// The QUERY_ROOT refers exclusively to the top-level node; its metrics are measured over the entire query.
// The OPERATOR nodes refer to the individual operators in the query plan.
type ProfilingInfo struct {
	// Metrics contains all key-value pairs of the current node.
	// The key represents the name and corresponds to the measured value.
	Metrics map[string]string
	// Children contains all children of the node and their respective metrics.
	Children []ProfilingInfo
}

// ProfilingContext captures timing and metrics with an injected clock for deterministic testing.
type ProfilingContext struct {
	mu        sync.Mutex
	clock     quartz.Clock
	startTime time.Time
	enabled   bool
	root      *ProfilingInfo
}

// NewProfilingContext creates a new ProfilingContext with the given clock.
// If clock is nil, the real system clock is used.
func NewProfilingContext(clock quartz.Clock) *ProfilingContext {
	if clock == nil {
		clock = quartz.NewReal()
	}

	return &ProfilingContext{
		clock:   clock,
		enabled: false,
	}
}

// WithClock returns a new ProfilingContext with the given clock.
func (p *ProfilingContext) WithClock(clock quartz.Clock) *ProfilingContext {
	return &ProfilingContext{
		clock:   clock,
		enabled: p.enabled,
	}
}

// Clock returns the clock used for timing measurements.
func (p *ProfilingContext) Clock() quartz.Clock {
	return p.clock
}

// Enable enables profiling.
func (p *ProfilingContext) Enable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = true
	p.root = nil
}

// Disable disables profiling.
func (p *ProfilingContext) Disable() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = false
	p.root = nil
}

// IsEnabled returns whether profiling is enabled.
func (p *ProfilingContext) IsEnabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.enabled
}

// Start begins timing measurement for a query.
func (p *ProfilingContext) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.enabled {
		return
	}
	p.startTime = p.clock.Now()
	p.root = &ProfilingInfo{
		Metrics:  make(map[string]string),
		Children: nil,
	}
}

// Elapsed returns the duration since Start was called.
func (p *ProfilingContext) Elapsed() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.startTime.IsZero() {
		return 0
	}

	return p.clock.Since(p.startTime)
}

// AddOperator adds an operator node to the profiling tree.
func (p *ProfilingContext) AddOperator(operatorType string, rows int, timing time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.enabled || p.root == nil {
		return
	}

	child := ProfilingInfo{
		Metrics: map[string]string{
			"OPERATOR_TYPE":   operatorType,
			"OPERATOR_TIMING": timing.String(),
			"ROWS":            fmt.Sprintf("%d", rows),
		},
		Children: nil,
	}
	p.root.Children = append(p.root.Children, child)
}

// Complete finalizes the profiling data with total metrics.
func (p *ProfilingContext) Complete(rowsReturned int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.enabled || p.root == nil {
		return
	}

	elapsed := p.clock.Since(p.startTime)
	p.root.Metrics["TOTAL_TIME"] = elapsed.String()
	p.root.Metrics["ROWS_RETURNED"] = fmt.Sprintf("%d", rowsReturned)
}

// GetInfo returns the current profiling info and clears it.
func (p *ProfilingContext) GetInfo() (*ProfilingInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.enabled {
		return nil, errProfilingNotEnabled
	}
	if p.root == nil {
		return nil, errProfilingInfoEmpty
	}

	info := p.root
	p.root = nil // Clear after retrieval
	p.startTime = time.Time{}

	return info, nil
}

// Error variables for profiling operations.
var (
	errProfilingNotEnabled = errors.New("profiling is not enabled")
	errProfilingInfoEmpty  = errors.New("profiling info is empty")
)

// GetProfilingInfo obtains all available metrics set by the current connection.
// Profiling must be enabled via PRAGMA enable_profiling before executing queries.
func GetProfilingInfo(c *sql.Conn) (ProfilingInfo, error) {
	info := ProfilingInfo{}
	err := c.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*Conn)
		if !ok {
			return errors.New("invalid driver connection type")
		}

		if conn.profiling == nil {
			return errProfilingNotEnabled
		}

		profInfo, profErr := conn.profiling.GetInfo()
		if profErr != nil {
			return profErr
		}

		info = *profInfo

		return nil
	})

	return info, err
}

// operatorMetrics tracks metrics for a single operator during execution.
type operatorMetrics struct {
	clock       quartz.Clock
	operatorTyp string
	startTime   time.Time
	endTime     time.Time
	rowCount    int
}

// newOperatorMetrics creates a new operatorMetrics with the given clock.
func newOperatorMetrics(clock quartz.Clock, operatorType string) *operatorMetrics {
	return &operatorMetrics{
		clock:       clock,
		operatorTyp: operatorType,
	}
}

// Start begins timing for this operator.
func (m *operatorMetrics) Start() {
	m.startTime = m.clock.Now()
}

// End finishes timing for this operator.
func (m *operatorMetrics) End() {
	m.endTime = m.clock.Now()
}

// AddRows increments the row count.
func (m *operatorMetrics) AddRows(count int) {
	m.rowCount += count
}

// Duration returns the time spent in this operator.
func (m *operatorMetrics) Duration() time.Duration {
	if m.startTime.IsZero() {
		return 0
	}
	if m.endTime.IsZero() {
		return m.clock.Since(m.startTime)
	}

	return m.endTime.Sub(m.startTime)
}

// RowCount returns the number of rows processed.
func (m *operatorMetrics) RowCount() int {
	return m.rowCount
}

// OperatorType returns the operator type.
func (m *operatorMetrics) OperatorType() string {
	return m.operatorTyp
}

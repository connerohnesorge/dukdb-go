package cardinality

import (
	"math"
	"sync"
)

// CorrelationTracker tracks Pearson correlation for paired observations.
type CorrelationTracker struct {
	mu    sync.Mutex
	n     float64
	meanX float64
	meanY float64
	cXY   float64
	m2X   float64
	m2Y   float64
}

// Observe adds a new pair observation.
func (t *CorrelationTracker) Observe(x, y float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.n++
	dx := x - t.meanX
	dy := y - t.meanY
	t.meanX += dx / t.n
	t.meanY += dy / t.n
	t.cXY += dx * (y - t.meanY)
	t.m2X += dx * (x - t.meanX)
	t.m2Y += dy * (y - t.meanY)
}

// Correlation returns the Pearson correlation coefficient.
func (t *CorrelationTracker) Correlation() float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.n < 2 || t.m2X == 0 || t.m2Y == 0 {
		return 0
	}
	return t.cXY / (math.Sqrt(t.m2X) * math.Sqrt(t.m2Y))
}

// CorrelationMatrix tracks correlations between column pairs.
type CorrelationMatrix struct {
	mu       sync.RWMutex
	trackers map[string]*CorrelationTracker
}

// NewCorrelationMatrix creates an empty correlation matrix.
func NewCorrelationMatrix() *CorrelationMatrix {
	return &CorrelationMatrix{trackers: make(map[string]*CorrelationTracker)}
}

// ObservePair records a pair of values for two columns.
func (m *CorrelationMatrix) ObservePair(left, right string, x, y float64) {
	if m == nil {
		return
	}
	key := pairKey(left, right)

	m.mu.Lock()
	tracker, ok := m.trackers[key]
	if !ok {
		tracker = &CorrelationTracker{}
		m.trackers[key] = tracker
	}
	m.mu.Unlock()

	tracker.Observe(x, y)
}

// Correlation returns correlation for a column pair.
func (m *CorrelationMatrix) Correlation(left, right string) float64 {
	if m == nil {
		return 0
	}
	key := pairKey(left, right)
	m.mu.RLock()
	tracker := m.trackers[key]
	m.mu.RUnlock()
	if tracker == nil {
		return 0
	}
	return tracker.Correlation()
}

func pairKey(left, right string) string {
	if left < right {
		return left + "|" + right
	}
	return right + "|" + left
}

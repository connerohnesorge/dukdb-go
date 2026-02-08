package cardinality

import (
	"testing"

	"github.com/dukdb/dukdb-go/internal/optimizer/stats"
)

func TestEstimatorSelectivity(t *testing.T) {
	collector := NewTestCollector()
	estimator := NewEstimator(collector)

	predicates := []Predicate{{Column: "age", Type: PredicateGreater, Value: 40}}
	selectivity := estimator.EstimateSelectivity("users", predicates)
	if selectivity <= 0 || selectivity > 1 {
		t.Fatalf("invalid selectivity %.2f", selectivity)
	}
}

func TestCorrelationTracker(t *testing.T) {
	matrix := NewCorrelationMatrix()
	for i := 0; i < 100; i++ {
		matrix.ObservePair("a", "b", float64(i), float64(i))
	}

	corr := matrix.Correlation("a", "b")
	if corr < 0.9 {
		t.Fatalf("expected strong correlation, got %.2f", corr)
	}
}

func NewTestCollector() *stats.Collector {
	collector := stats.NewCollector(stats.CollectorOptions{HistogramBuckets: 8, HLLPrecision: 10, Enabled: true})
	for i := 0; i < 100; i++ {
		collector.RecordRow("users", map[string]any{"age": i})
	}
	return collector
}

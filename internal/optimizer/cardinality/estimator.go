package cardinality

import "github.com/dukdb/dukdb-go/internal/optimizer/stats"

// Estimator provides selectivity estimates using runtime statistics.
type Estimator struct {
	stats        *stats.Collector
	correlations *CorrelationMatrix
}

// NewEstimator creates a new estimator using the provided collector.
func NewEstimator(collector *stats.Collector) *Estimator {
	return &Estimator{stats: collector}
}

// WithCorrelations attaches a correlation matrix for selectivity adjustment.
func (e *Estimator) WithCorrelations(matrix *CorrelationMatrix) *Estimator {
	e.correlations = matrix
	return e
}

// EstimateSelectivity estimates combined selectivity for predicates on a table.
func (e *Estimator) EstimateSelectivity(table string, predicates []Predicate) float64 {
	if e == nil || e.stats == nil {
		return 0.2
	}

	snapshot := e.stats.Snapshot(table)
	if len(predicates) == 0 {
		return 1.0
	}

	selectivity := 1.0
	for _, predicate := range predicates {
		col, ok := snapshot.Columns[predicate.Column]
		if !ok {
			selectivity *= 0.2
			continue
		}

		sel := SelectivityFromStats(col, predicate)
		selectivity = combineAnd(selectivity, sel)
	}

	if e.correlations != nil && len(predicates) > 1 {
		selectivity = e.adjustForCorrelations(selectivity, predicates)
	}

	return clamp(selectivity)
}

func (e *Estimator) adjustForCorrelations(base float64, predicates []Predicate) float64 {
	if e.correlations == nil {
		return base
	}

	adjustment := 1.0
	for i := 0; i < len(predicates); i++ {
		for j := i + 1; j < len(predicates); j++ {
			corr := e.correlations.Correlation(predicates[i].Column, predicates[j].Column)
			if corr == 0 {
				continue
			}
			adjustment *= 1 + 0.25*corr
		}
	}

	return clamp(base * adjustment)
}

func combineAnd(left, right float64) float64 {
	return clamp(left * right)
}

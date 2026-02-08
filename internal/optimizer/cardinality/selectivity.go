package cardinality

import (
	"math"

	"github.com/dukdb/dukdb-go/internal/optimizer/stats"
)

// PredicateType defines supported predicate kinds.
type PredicateType int

const (
	PredicateEqual PredicateType = iota
	PredicateNotEqual
	PredicateLess
	PredicateLessEqual
	PredicateGreater
	PredicateGreaterEqual
	PredicateInList
	PredicateBetween
	PredicateIsNull
	PredicateIsNotNull
)

// Predicate represents a simplified predicate for selectivity estimation.
type Predicate struct {
	Column    string
	Type      PredicateType
	Value     any
	Values    []any
	RangeLow  any
	RangeHigh any
}

// SelectivityFromStats estimates predicate selectivity using column statistics.
func SelectivityFromStats(col stats.ColumnSnapshot, pred Predicate) float64 {
	switch pred.Type {
	case PredicateEqual:
		return selectivityEqual(col)
	case PredicateNotEqual:
		return clamp(1.0 - selectivityEqual(col))
	case PredicateLess, PredicateLessEqual:
		return selectivityLess(col, pred.Value)
	case PredicateGreater, PredicateGreaterEqual:
		return clamp(1.0 - selectivityLess(col, pred.Value))
	case PredicateBetween:
		return selectivityBetween(col, pred.RangeLow, pred.RangeHigh)
	case PredicateInList:
		return selectivityInList(col, pred.Values)
	case PredicateIsNull:
		return clamp(col.NullFraction)
	case PredicateIsNotNull:
		return clamp(1.0 - col.NullFraction)
	default:
		return 0.2
	}
}

func selectivityEqual(col stats.ColumnSnapshot) float64 {
	if col.Distinct > 0 {
		return clamp(1.0 / float64(col.Distinct))
	}
	return 0.1
}

func selectivityInList(col stats.ColumnSnapshot, values []any) float64 {
	if len(values) == 0 {
		return 0.0
	}
	if col.Distinct > 0 {
		return clamp(float64(len(values)) / float64(col.Distinct))
	}
	return clamp(float64(len(values)) * 0.1)
}

func selectivityLess(col stats.ColumnSnapshot, value any) float64 {
	numeric, ok := toFloat64(value)
	if !ok {
		return 0.2
	}

	if col.Histogram != nil {
		return col.Histogram.EstimateLessThan(numeric)
	}

	if col.HasMinMax {
		if col.Max == col.Min {
			return 0.5
		}
		position := (numeric - col.Min) / (col.Max - col.Min)
		return clamp(position)
	}

	return 0.2
}

func selectivityBetween(col stats.ColumnSnapshot, low, high any) float64 {
	lowVal, okLow := toFloat64(low)
	highVal, okHigh := toFloat64(high)
	if !okLow || !okHigh {
		return 0.2
	}

	if col.Histogram != nil {
		return clamp(col.Histogram.EstimateBetween(lowVal, highVal))
	}

	if col.HasMinMax {
		min := math.Min(lowVal, highVal)
		max := math.Max(lowVal, highVal)
		if col.Max == col.Min {
			return 0.2
		}
		fraction := (max - min) / (col.Max - col.Min)
		return clamp(fraction)
	}

	return 0.2
}

func clamp(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

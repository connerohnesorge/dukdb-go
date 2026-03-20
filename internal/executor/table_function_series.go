package executor

import (
	"fmt"
	"math"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeGenerateSeries implements both generate_series (inclusive=true) and range (inclusive=false).
func (e *Executor) executeGenerateSeries(
	ctx *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
	inclusive bool,
) (*ExecutionResult, error) {
	// Extract bound expressions from plan.Options
	startExprVal, ok := plan.Options["start"]
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("%s: missing start expression", plan.FunctionName),
		}
	}
	stopExprVal, ok := plan.Options["stop"]
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("%s: missing stop expression", plan.FunctionName),
		}
	}

	startExpr, ok := startExprVal.(binder.BoundExpr)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("%s: invalid start expression type", plan.FunctionName),
		}
	}
	stopExpr, ok := stopExprVal.(binder.BoundExpr)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("%s: invalid stop expression type", plan.FunctionName),
		}
	}

	// Evaluate start and stop
	startVal, err := e.evaluateExpr(ctx, startExpr, nil)
	if err != nil {
		return nil, err
	}
	stopVal, err := e.evaluateExpr(ctx, stopExpr, nil)
	if err != nil {
		return nil, err
	}

	// NULL arguments produce empty result
	if startVal == nil || stopVal == nil {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{plan.FunctionName},
		}, nil
	}

	// Evaluate optional step
	var stepVal any
	if stepExprVal, ok := plan.Options["step"]; ok {
		stepExpr, ok := stepExprVal.(binder.BoundExpr)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("%s: invalid step expression type", plan.FunctionName),
			}
		}
		stepVal, err = e.evaluateExpr(ctx, stepExpr, nil)
		if err != nil {
			return nil, err
		}
		if stepVal == nil {
			return &ExecutionResult{
				Rows:    make([]map[string]any, 0),
				Columns: []string{plan.FunctionName},
			}, nil
		}
	}

	colName := plan.FunctionName

	// Try date series first (dates stored as int32 days since epoch).
	// This must be checked before the generic toInt64 path, because
	// toInt64 would also accept int32 and route dates into the integer
	// series, which cannot handle an INTERVAL step.
	startDate, startIsDate := startVal.(int32)
	stopDate, stopIsDate := stopVal.(int32)
	if startIsDate && stopIsDate {
		return e.generateDateSeries(startDate, stopDate, stepVal, inclusive, colName)
	}

	// If start/stop are int64 and the step is an Interval, treat as timestamp
	// series. This must be checked before the generic toInt64 path because
	// timestamps and plain integers are both represented as int64.
	if stepVal != nil {
		_, isInterval := stepVal.(Interval)
		_, isDukdbInterval := stepVal.(dukdb.Interval)
		if isInterval || isDukdbInterval {
			startTS, startIsTS := startVal.(int64)
			stopTS, stopIsTS := stopVal.(int64)
			if startIsTS && stopIsTS {
				return e.generateTimestampSeries(startTS, stopTS, stepVal, inclusive, colName)
			}
		}
	}

	// Try numeric series (int, int64, etc.)
	startInt, startOk := toInt64(startVal)
	stopInt, stopOk := toInt64(stopVal)
	if startOk && stopOk {
		return e.generateIntSeries(startInt, stopInt, stepVal, inclusive, colName)
	}

	return nil, &dukdb.Error{
		Type: dukdb.ErrorTypeExecutor,
		Msg:  fmt.Sprintf("%s: unsupported argument types: start=%T, stop=%T", plan.FunctionName, startVal, stopVal),
	}
}

// generateIntSeries generates an integer series.
func (e *Executor) generateIntSeries(start, stop int64, stepVal any, inclusive bool, colName string) (*ExecutionResult, error) {
	var step int64 = 1
	if stepVal != nil {
		s, ok := toInt64(stepVal)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "step must be an integer for integer series",
			}
		}
		step = s
	} else if start > stop {
		step = -1
	}

	if step == 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "step size cannot be zero",
		}
	}

	// Empty range due to direction mismatch
	if step > 0 && start > stop {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{colName},
		}, nil
	}
	if step < 0 && start < stop {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{colName},
		}, nil
	}

	rows := make([]map[string]any, 0)
	current := start
	for {
		if step > 0 {
			if inclusive {
				if current > stop {
					break
				}
			} else {
				if current >= stop {
					break
				}
			}
		} else {
			if inclusive {
				if current < stop {
					break
				}
			} else {
				if current <= stop {
					break
				}
			}
		}

		rows = append(rows, map[string]any{colName: current})

		// Overflow protection
		if step > 0 && current > math.MaxInt64-step {
			break
		}
		if step < 0 && current < math.MinInt64-step {
			break
		}
		current += step
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: []string{colName},
	}, nil
}

// generateDateSeries generates a date series with an interval step.
func (e *Executor) generateDateSeries(start, stop int32, stepVal any, inclusive bool, colName string) (*ExecutionResult, error) {
	var step Interval
	if stepVal != nil {
		interval, ok := stepVal.(Interval)
		if !ok {
			if di, ok2 := stepVal.(dukdb.Interval); ok2 {
				step = Interval{Months: di.Months, Days: di.Days, Micros: di.Micros}
			} else {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  "step must be an INTERVAL for date series",
				}
			}
		} else {
			step = interval
		}
	} else {
		step = Interval{Days: 1}
	}

	if step.Months == 0 && step.Days == 0 && step.Micros == 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "step size cannot be zero",
		}
	}

	positive := isPositiveInterval(step)

	if positive && start > stop {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{colName},
		}, nil
	}
	if !positive && start < stop {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{colName},
		}, nil
	}

	rows := make([]map[string]any, 0)
	currentTime := dateToTime(start)

	const maxRows = 10_000_000
	for i := 0; i < maxRows; i++ {
		currentDate := timeToDate(currentTime)

		if positive {
			if inclusive {
				if currentDate > stop {
					break
				}
			} else {
				if currentDate >= stop {
					break
				}
			}
		} else {
			if inclusive {
				if currentDate < stop {
					break
				}
			} else {
				if currentDate <= stop {
					break
				}
			}
		}

		rows = append(rows, map[string]any{colName: currentDate})
		nextTime := addInterval(currentTime, step)
		if nextTime.Equal(currentTime) {
			break
		}
		currentTime = nextTime
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: []string{colName},
	}, nil
}

// generateTimestampSeries generates a timestamp series with an interval step.
func (e *Executor) generateTimestampSeries(start, stop int64, stepVal any, inclusive bool, colName string) (*ExecutionResult, error) {
	var step Interval
	if stepVal != nil {
		interval, ok := stepVal.(Interval)
		if !ok {
			if di, ok2 := stepVal.(dukdb.Interval); ok2 {
				step = Interval{Months: di.Months, Days: di.Days, Micros: di.Micros}
			} else {
				return nil, &dukdb.Error{
					Type: dukdb.ErrorTypeExecutor,
					Msg:  "step must be an INTERVAL for timestamp series",
				}
			}
		} else {
			step = interval
		}
	} else {
		step = Interval{Days: 1}
	}

	if step.Months == 0 && step.Days == 0 && step.Micros == 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "step size cannot be zero",
		}
	}

	positive := isPositiveInterval(step)

	if positive && start > stop {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{colName},
		}, nil
	}
	if !positive && start < stop {
		return &ExecutionResult{
			Rows:    make([]map[string]any, 0),
			Columns: []string{colName},
		}, nil
	}

	rows := make([]map[string]any, 0)
	currentTime := timestampToTime(start)
	stopTime := timestampToTime(stop)

	const maxRows = 10_000_000
	for i := 0; i < maxRows; i++ {
		currentTS := timeToTimestamp(currentTime)

		if positive {
			if inclusive {
				if currentTime.After(stopTime) {
					break
				}
			} else {
				if !currentTime.Before(stopTime) {
					break
				}
			}
		} else {
			if inclusive {
				if currentTime.Before(stopTime) {
					break
				}
			} else {
				if !currentTime.After(stopTime) {
					break
				}
			}
		}

		rows = append(rows, map[string]any{colName: currentTS})
		nextTime := addInterval(currentTime, step)
		if nextTime.Equal(currentTime) {
			break
		}
		currentTime = nextTime
	}

	return &ExecutionResult{
		Rows:    rows,
		Columns: []string{colName},
	}, nil
}

// isPositiveInterval returns true if the interval represents a positive duration.
func isPositiveInterval(i Interval) bool {
	totalMicros := int64(i.Months)*30*24*3600*1_000_000 +
		int64(i.Days)*24*3600*1_000_000 +
		i.Micros
	return totalMicros > 0
}

package executor

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"sync"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// parallelGroupThreshold is the minimum number of groups needed to use parallel processing.
// Below this threshold, sequential processing has less overhead.
const parallelGroupThreshold = 100

// PhysicalAggregateOperator implements the PhysicalOperator interface for aggregation.
// It consumes all input from the child operator, groups by the GROUP BY expressions,
// computes aggregate functions, and produces one output chunk with one row per group.
type PhysicalAggregateOperator struct {
	child        PhysicalOperator
	childColumns []planner.ColumnBinding
	groupBy      []binder.BoundExpr
	aggregates   []binder.BoundExpr
	executor     *Executor
	ctx          *ExecutionContext
	types        []dukdb.TypeInfo
	finished     bool
	resultChunk  *storage.DataChunk
}

// NewPhysicalAggregateOperator creates a new PhysicalAggregateOperator.
func NewPhysicalAggregateOperator(
	child PhysicalOperator,
	childColumns []planner.ColumnBinding,
	groupBy []binder.BoundExpr,
	aggregates []binder.BoundExpr,
	executor *Executor,
	ctx *ExecutionContext,
) (*PhysicalAggregateOperator, error) {
	// Build output types: GROUP BY columns followed by aggregate columns
	numGroupBy := len(groupBy)
	numAgg := len(aggregates)
	types := make(
		[]dukdb.TypeInfo,
		numGroupBy+numAgg,
	)

	// Group by column types
	for i, expr := range groupBy {
		typ := expr.ResultType()
		info, err := dukdb.NewTypeInfo(typ)
		if err != nil {
			types[i] = &basicTypeInfo{typ: typ}
		} else {
			types[i] = info
		}
	}

	// Aggregate column types
	for i, expr := range aggregates {
		typ := expr.ResultType()
		info, err := dukdb.NewTypeInfo(typ)
		if err != nil {
			types[numGroupBy+i] = &basicTypeInfo{
				typ: typ,
			}
		} else {
			types[numGroupBy+i] = info
		}
	}

	return &PhysicalAggregateOperator{
		child:        child,
		childColumns: childColumns,
		groupBy:      groupBy,
		aggregates:   aggregates,
		executor:     executor,
		ctx:          ctx,
		types:        types,
		finished:     false,
	}, nil
}

// Next returns the aggregated results as a single DataChunk, or nil if already called.
// The operator consumes all input from the child, builds hash groups, and returns
// one chunk with all group results.
func (op *PhysicalAggregateOperator) Next() (*storage.DataChunk, error) {
	// Aggregation is a blocking operator - we only produce output once
	if op.finished {
		return nil, nil
	}
	op.finished = true

	// Hash table: groupKey -> list of rows belonging to that group
	type groupKey string
	groups := make(map[groupKey][]map[string]any)
	groupOrder := make(
		[]groupKey,
		0,
	) // Preserve insertion order

	// Consume all input from child operator
	for {
		inputChunk, err := op.child.Next()
		if err != nil {
			return nil, err
		}
		if inputChunk == nil {
			// No more input
			break
		}

		// Process each row in the chunk
		for rowIdx := 0; rowIdx < inputChunk.Count(); rowIdx++ {
			// Build row map for expression evaluation
			rowMap := make(map[string]any)

			if len(op.childColumns) > 0 {
				// Use column bindings if available
				for colIdx := 0; colIdx < inputChunk.ColumnCount() && colIdx < len(op.childColumns); colIdx++ {
					value := inputChunk.GetValue(
						rowIdx,
						colIdx,
					)
					col := op.childColumns[colIdx]

					// Add column by simple name
					if col.Column != "" {
						rowMap[col.Column] = value
					}

					// Add column by table-qualified name
					if col.Table != "" &&
						col.Column != "" {
						qualifiedName := col.Table + "." + col.Column
						rowMap[qualifiedName] = value
					}
				}
			} else {
				// Fallback: use column indices as string keys
				for colIdx := 0; colIdx < inputChunk.ColumnCount(); colIdx++ {
					value := inputChunk.GetValue(rowIdx, colIdx)
					rowMap[string(rune('0'+colIdx))] = value
				}
			}

			// Compute group key from GROUP BY expressions
			var key groupKey
			if len(op.groupBy) > 0 {
				keyParts := make(
					[]any,
					len(op.groupBy),
				)
				for i, expr := range op.groupBy {
					val, err := op.executor.evaluateExpr(
						op.ctx,
						expr,
						rowMap,
					)
					if err != nil {
						return nil, err
					}
					keyParts[i] = val
				}
				key = groupKey(
					formatGroupKey(keyParts),
				)
			} else {
				// No GROUP BY - single group for all rows
				key = ""
			}

			// Add row to group
			if _, exists := groups[key]; !exists {
				groupOrder = append(
					groupOrder,
					key,
				)
				groups[key] = make(
					[]map[string]any,
					0,
				)
			}
			groups[key] = append(
				groups[key],
				rowMap,
			)
		}
	}

	// If no groups and no GROUP BY, create a single empty group for aggregates
	// (e.g., SELECT COUNT(*) FROM empty_table should return 0, not no rows)
	if len(groups) == 0 && len(op.groupBy) == 0 {
		groupOrder = append(groupOrder, "")
		groups[""] = []map[string]any{}
	}

	// Build output chunk
	numGroupBy := len(op.groupBy)
	numAgg := len(op.aggregates)
	outputTypes := make(
		[]dukdb.Type,
		numGroupBy+numAgg,
	)

	for i, expr := range op.groupBy {
		outputTypes[i] = expr.ResultType()
	}
	for i, expr := range op.aggregates {
		outputTypes[numGroupBy+i] = expr.ResultType()
	}

	outputChunk := storage.NewDataChunkWithCapacity(
		outputTypes,
		len(groupOrder),
	)

	// Use parallel processing for large number of groups
	if len(groupOrder) >= parallelGroupThreshold {
		// Convert groupKey to string for the parallel function
		groupOrderStr := make([]string, len(groupOrder))
		groupsStr := make(map[string][]map[string]any)
		for i, key := range groupOrder {
			groupOrderStr[i] = string(key)
			groupsStr[string(key)] = groups[key]
		}

		results, err := op.computeAggregatesParallel(groupsStr, groupOrderStr, numGroupBy, numAgg)
		if err != nil {
			return nil, err
		}

		// Add results to output chunk in order
		for _, res := range results {
			outputChunk.AppendRow(res.rowValues)
		}
	} else {
		// Sequential processing for small number of groups
		for _, key := range groupOrder {
			groupRows := groups[key]
			rowValues := make(
				[]any,
				numGroupBy+numAgg,
			)

			// Add GROUP BY column values
			if len(groupRows) > 0 &&
				len(op.groupBy) > 0 {
				for i, expr := range op.groupBy {
					val, _ := op.executor.evaluateExpr(
						op.ctx,
						expr,
						groupRows[0],
					)
					rowValues[i] = val
				}
			} else if len(op.groupBy) > 0 {
				// Empty group - use NULLs for group by columns
				for i := range op.groupBy {
					rowValues[i] = nil
				}
			}

			// Compute aggregate values
			for i, expr := range op.aggregates {
				val, err := op.computeAggregate(
					expr,
					groupRows,
				)
				if err != nil {
					return nil, err
				}
				rowValues[numGroupBy+i] = val
			}

			outputChunk.AppendRow(rowValues)
		}
	}

	op.resultChunk = outputChunk

	return outputChunk, nil
}

// GetTypes returns the TypeInfo for each column produced by this operator.
func (op *PhysicalAggregateOperator) GetTypes() []dukdb.TypeInfo {
	return op.types
}

// computeAggregate computes an aggregate function over a set of rows.
func (op *PhysicalAggregateOperator) computeAggregate(
	expr binder.BoundExpr,
	rows []map[string]any,
) (any, error) {
	fn, ok := expr.(*binder.BoundFunctionCall)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "expected aggregate function",
		}
	}

	switch fn.Name {
	case "COUNT":
		// COUNT(*) - count all rows
		if fn.Star {
			return int64(len(rows)), nil
		}
		// COUNT(expr) - count non-NULL values
		count := int64(0)
		seen := make(map[string]bool)
		for _, row := range rows {
			if len(fn.Args) > 0 {
				val, err := op.executor.evaluateExpr(
					op.ctx,
					fn.Args[0],
					row,
				)
				if err != nil {
					return nil, err
				}
				if val != nil {
					if fn.Distinct {
						key := formatValue(val)
						if !seen[key] {
							seen[key] = true
							count++
						}
					} else {
						count++
					}
				}
			}
		}

		return count, nil

	case "SUM":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var sum float64
		hasValue := false
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(
				op.ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat64Value(val)
				hasValue = true
			}
		}
		if !hasValue {
			return nil, nil
		}
		// Return as int64 if all inputs were integers
		return sum, nil

	case "AVG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var sum float64
		count := 0
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(
				op.ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				sum += toFloat64Value(val)
				count++
			}
		}
		if count == 0 {
			return nil, nil
		}

		return sum / float64(count), nil

	case "MIN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var minVal any
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(
				op.ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				if minVal == nil ||
					compareValues(
						val,
						minVal,
					) < 0 {
					minVal = val
				}
			}
		}

		return minVal, nil

	case "MAX":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		var maxVal any
		for _, row := range rows {
			val, err := op.executor.evaluateExpr(
				op.ctx,
				fn.Args[0],
				row,
			)
			if err != nil {
				return nil, err
			}
			if val != nil {
				if maxVal == nil ||
					compareValues(
						val,
						maxVal,
					) > 0 {
					maxVal = val
				}
			}
		}

		return maxVal, nil

	// Statistical aggregate functions
	case "MEDIAN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeMedian(values)

	case "QUANTILE":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		// Evaluate the quantile position (second argument)
		qVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		// Check if the quantile argument is an array/slice
		if qSlice, ok := toFloat64Slice(qVal); ok {
			return computeQuantileArray(values, qSlice)
		}
		// Single quantile value
		q := toFloat64Value(qVal)
		return computeQuantile(values, q)

	case "PERCENTILE_CONT":
		// WITHIN GROUP syntax: PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col)
		if len(fn.OrderBy) > 0 && len(fn.Args) >= 1 {
			values, err := op.collectValues(fn.OrderBy[0].Expr, rows)
			if err != nil {
				return nil, err
			}
			pVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], nil)
			if err != nil {
				return nil, err
			}
			p := toFloat64Value(pVal)
			return computePercentileCont(values, p)
		}
		// Traditional syntax: PERCENTILE_CONT(col, p)
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		pVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		p := toFloat64Value(pVal)
		return computePercentileCont(values, p)

	case "PERCENTILE_DISC":
		// WITHIN GROUP syntax: PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY col)
		if len(fn.OrderBy) > 0 && len(fn.Args) >= 1 {
			values, err := op.collectValues(fn.OrderBy[0].Expr, rows)
			if err != nil {
				return nil, err
			}
			pVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[0], nil)
			if err != nil {
				return nil, err
			}
			p := toFloat64Value(pVal)
			return computePercentileDisc(values, p)
		}
		// Traditional syntax: PERCENTILE_DISC(col, p)
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		pVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		p := toFloat64Value(pVal)
		return computePercentileDisc(values, p)

	case "MODE":
		// WITHIN GROUP syntax: MODE() WITHIN GROUP (ORDER BY col)
		if len(fn.OrderBy) > 0 {
			values, err := op.collectValues(fn.OrderBy[0].Expr, rows)
			if err != nil {
				return nil, err
			}
			return computeMode(values)
		}
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeMode(values)

	case "ENTROPY":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeEntropy(values)

	case "SKEWNESS":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeSkewness(values)

	case "KURTOSIS":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeKurtosis(values)

	case "VAR_POP":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeVarPop(values)

	case "VAR_SAMP", "VARIANCE":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeVarSamp(values)

	case "STDDEV_POP":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeStddevPop(values)

	case "STDDEV_SAMP", "STDDEV":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeStddevSamp(values)

	// Approximate aggregate functions
	case "APPROX_COUNT_DISTINCT":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeApproxCountDistinct(values)

	case "APPROX_QUANTILE":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		// Evaluate the quantile position (second argument)
		qVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
		if err != nil {
			return nil, err
		}
		q := toFloat64Value(qVal)
		return computeApproxQuantile(values, q)

	case "APPROX_MEDIAN":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeApproxMedian(values)

	// String aggregate functions
	case "STRING_AGG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		// Get delimiter from second argument, default to comma
		delimiter := ","
		if len(fn.Args) >= 2 {
			delimVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
			if err != nil {
				return nil, err
			}
			if delimVal != nil {
				delimiter = toString(delimVal)
			}
		}
		return computeStringAgg(values, delimiter)

	case "LISTAGG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		delimiter := "" // LISTAGG defaults to empty string (unlike STRING_AGG's comma)
		if len(fn.Args) >= 2 {
			delimVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
			if err != nil {
				return nil, err
			}
			if delimVal != nil {
				delimiter = toString(delimVal)
			}
		}
		return computeStringAgg(values, delimiter)

	case "GROUP_CONCAT":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		// Get delimiter from second argument, default to comma
		delimiter := ","
		if len(fn.Args) >= 2 {
			delimVal, err := op.executor.evaluateExpr(op.ctx, fn.Args[1], nil)
			if err != nil {
				return nil, err
			}
			if delimVal != nil {
				delimiter = toString(delimVal)
			}
		}
		return computeGroupConcat(values, delimiter)

	case "LIST", "ARRAY_AGG":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		// Handle DISTINCT modifier
		if fn.Distinct {
			return computeListDistinct(values)
		}
		return computeList(values)

	case "LIST_DISTINCT":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		return computeListDistinct(values)

	// Time series aggregate functions
	case "COUNT_IF":
		if len(fn.Args) == 0 {
			return int64(0), nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeCountIf(values)

	case "FIRST", "ANY_VALUE":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeFirst(values)

	case "LAST":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeLast(values)

	case "HISTOGRAM":
		if len(fn.Args) != 1 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeHistogram(values)

	case "ARGMIN", "ARG_MIN", "MIN_BY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		// ARGMIN/MIN_BY takes two arguments: arg (value to return) and val (value to compare)
		argValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		valValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeArgmin(argValues, valValues)

	case "ARGMAX", "ARG_MAX", "MAX_BY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		// ARGMAX/MAX_BY takes two arguments: arg (value to return) and val (value to compare)
		argValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		valValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeArgmax(argValues, valValues)

	// Regression and correlation aggregate functions
	// Note: These functions take (Y, X) where Y is dependent and X is independent
	case "COVAR_POP":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeCovarPop(yValues, xValues)

	case "COVAR_SAMP":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeCovarSamp(yValues, xValues)

	case "CORR":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeCorr(yValues, xValues)

	case "REGR_SLOPE":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSlope(yValues, xValues)

	case "REGR_INTERCEPT":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrIntercept(yValues, xValues)

	case "REGR_R2":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrR2(yValues, xValues)

	case "REGR_COUNT":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrCount(yValues, xValues)

	case "REGR_AVGX":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrAvgX(yValues, xValues)

	case "REGR_AVGY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrAvgY(yValues, xValues)

	case "REGR_SXX":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSXX(yValues, xValues)

	case "REGR_SYY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSYY(yValues, xValues)

	case "REGR_SXY":
		if len(fn.Args) < 2 {
			return nil, nil
		}
		yValues, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		xValues, err := op.collectValues(fn.Args[1], rows)
		if err != nil {
			return nil, err
		}
		return computeRegrSXY(yValues, xValues)

	// Boolean aggregate functions
	case "BOOL_AND":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBoolAnd(values)

	case "BOOL_OR":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBoolOr(values)

	case "EVERY":
		// EVERY is an alias for BOOL_AND
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBoolAnd(values)

	// Bitwise aggregate functions
	case "BIT_AND":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBitAnd(values)

	case "BIT_OR":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBitOr(values)

	case "BIT_XOR":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValues(fn.Args[0], rows)
		if err != nil {
			return nil, err
		}
		return computeBitXor(values)

	case "JSON_GROUP_ARRAY":
		if len(fn.Args) == 0 {
			return nil, nil
		}
		values, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		return computeJSONGroupArray(values)

	case "JSON_GROUP_OBJECT":
		if len(fn.Args) < 2 {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  "JSON_GROUP_OBJECT requires 2 arguments (key, value)",
			}
		}
		keys, err := op.collectValuesWithOrderBy(fn.Args[0], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		vals, err := op.collectValuesWithOrderBy(fn.Args[1], fn.OrderBy, rows)
		if err != nil {
			return nil, err
		}
		return computeJSONGroupObject(keys, vals)

	default:
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg: fmt.Sprintf(
				"unknown aggregate function: %s",
				fn.Name,
			),
		}
	}
}

// collectValues collects all values from an expression across rows.
// Returns a slice of values (including NULLs as nil).
func (op *PhysicalAggregateOperator) collectValues(
	expr binder.BoundExpr,
	rows []map[string]any,
) ([]any, error) {
	values := make([]any, 0, len(rows))
	for _, row := range rows {
		val, err := op.executor.evaluateExpr(op.ctx, expr, row)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	return values, nil
}

// collectValuesWithOrderBy collects values along with their ORDER BY sorting keys.
// Returns the values sorted according to the ORDER BY clause.
func (op *PhysicalAggregateOperator) collectValuesWithOrderBy(
	valueExpr binder.BoundExpr,
	orderBy []binder.BoundOrderByExpr,
	rows []map[string]any,
) ([]any, error) {
	if len(orderBy) == 0 {
		return op.collectValues(valueExpr, rows)
	}

	// Collect values with their sorting keys
	type valueWithKey struct {
		value    any
		orderKey []any
	}
	items := make([]valueWithKey, 0, len(rows))

	for _, row := range rows {
		val, err := op.executor.evaluateExpr(op.ctx, valueExpr, row)
		if err != nil {
			return nil, err
		}

		// Collect ORDER BY key values
		keys := make([]any, len(orderBy))
		for i, ob := range orderBy {
			keyVal, err := op.executor.evaluateExpr(op.ctx, ob.Expr, row)
			if err != nil {
				return nil, err
			}
			keys[i] = keyVal
		}

		items = append(items, valueWithKey{value: val, orderKey: keys})
	}

	// Sort items based on ORDER BY keys
	sort.SliceStable(items, func(i, j int) bool {
		for k, ob := range orderBy {
			cmp := compareValues(items[i].orderKey[k], items[j].orderKey[k])
			if cmp != 0 {
				if ob.Desc {
					return cmp > 0 // descending
				}
				return cmp < 0 // ascending
			}
		}
		return false // equal, preserve order
	})

	// Extract sorted values
	result := make([]any, len(items))
	for i, item := range items {
		result[i] = item.value
	}
	return result, nil
}

// aggregateResult holds the result of computing aggregates for one group.
type aggregateResult struct {
	index     int   // Original position in groupOrder
	rowValues []any // Computed row values (GROUP BY + aggregates)
	err       error // Error if computation failed
}

// computeAggregatesParallel computes aggregates for multiple groups in parallel.
// It uses a worker pool pattern with configurable concurrency.
// Returns results in the same order as groupOrder.
func (op *PhysicalAggregateOperator) computeAggregatesParallel(
	groups map[string][]map[string]any,
	groupOrder []string,
	numGroupBy int,
	numAgg int,
) ([]aggregateResult, error) {
	numGroups := len(groupOrder)
	numWorkers := runtime.NumCPU()
	if numWorkers > numGroups {
		numWorkers = numGroups
	}

	// Channel for work distribution and results collection
	jobs := make(chan int, numGroups)
	results := make(chan aggregateResult, numGroups)

	// Start worker goroutines
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				key := groupOrder[idx]
				groupRows := groups[key]
				rowValues := make([]any, numGroupBy+numAgg)

				var err error

				// Add GROUP BY column values
				if len(groupRows) > 0 && len(op.groupBy) > 0 {
					for i, expr := range op.groupBy {
						val, e := op.executor.evaluateExpr(op.ctx, expr, groupRows[0])
						if e != nil {
							err = e
							break
						}
						rowValues[i] = val
					}
				} else if len(op.groupBy) > 0 {
					for i := range op.groupBy {
						rowValues[i] = nil
					}
				}

				// Compute aggregate values if no error yet
				if err == nil {
					for i, expr := range op.aggregates {
						val, e := op.computeAggregate(expr, groupRows)
						if e != nil {
							err = e
							break
						}
						rowValues[numGroupBy+i] = val
					}
				}

				results <- aggregateResult{
					index:     idx,
					rowValues: rowValues,
					err:       err,
				}
			}
		}()
	}

	// Send jobs to workers
	for i := range groupOrder {
		jobs <- i
	}
	close(jobs)

	// Wait for all workers to finish in a separate goroutine
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	collected := make([]aggregateResult, numGroups)
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		collected[res.index] = res
	}

	return collected, nil
}

// computeJSONGroupArray builds a JSON array string from the given values, filtering out NULLs.
func computeJSONGroupArray(values []any) (string, error) {
	var filtered []any
	for _, v := range values {
		if v != nil {
			filtered = append(filtered, v)
		}
	}
	if filtered == nil {
		filtered = []any{}
	}
	b, err := json.Marshal(filtered)
	if err != nil {
		return "", &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("JSON_GROUP_ARRAY: marshal error: %v", err),
		}
	}
	return string(b), nil
}

// computeJSONGroupObject builds a JSON object string from key-value pairs, skipping NULL keys.
func computeJSONGroupObject(keys, vals []any) (string, error) {
	result := make(map[string]any)
	for i := 0; i < len(keys) && i < len(vals); i++ {
		if keys[i] == nil {
			continue
		}
		key := toString(keys[i])
		result[key] = vals[i]
	}
	b, err := json.Marshal(result)
	if err != nil {
		return "", &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("JSON_GROUP_OBJECT: marshal error: %v", err),
		}
	}
	return string(b), nil
}

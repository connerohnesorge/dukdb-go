package executor

import (
	"fmt"
	"math"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeSummarize executes a SUMMARIZE statement.
func (e *Executor) executeSummarize(
	ctx *ExecutionContext,
	plan *planner.PhysicalSummarize,
) (*ExecutionResult, error) {
	var columns []string
	var colTypes []dukdb.Type
	var dataRows [][]any

	if plan.Query != nil {
		// SUMMARIZE SELECT ... - execute inner query
		result, err := e.executeWithContext(ctx, plan.Query)
		if err != nil {
			return nil, err
		}
		columns = result.Columns
		// Infer column types from the inner query's physical plan output columns
		outputCols := plan.Query.OutputColumns()
		colTypes = make([]dukdb.Type, len(columns))
		for i := range columns {
			if i < len(outputCols) {
				colTypes[i] = outputCols[i].Type
			} else {
				colTypes[i] = dukdb.TYPE_VARCHAR
			}
		}
		for _, row := range result.Rows {
			vals := make([]any, len(columns))
			for i, col := range columns {
				vals[i] = row[col]
			}
			dataRows = append(dataRows, vals)
		}
	} else {
		// SUMMARIZE table - read all data from storage
		tableDef := plan.TableDef
		columns = make([]string, len(tableDef.Columns))
		colTypes = make([]dukdb.Type, len(tableDef.Columns))
		for i, col := range tableDef.Columns {
			columns[i] = col.Name
			colTypes[i] = col.Type
		}

		// Get table from storage and read all rows via scanner
		storageTable, ok := e.storage.GetTable(plan.TableName)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("table %q not found in storage", plan.TableName),
			}
		}
		scanner := storageTable.Scan()
		for {
			chunk := scanner.Next()
			if chunk == nil {
				break
			}
			for i := 0; i < chunk.Count(); i++ {
				row := make([]any, len(columns))
				for colIdx := range columns {
					row[colIdx] = chunk.GetValue(i, colIdx)
				}
				dataRows = append(dataRows, row)
			}
		}
	}

	// Compute per-column statistics
	resultCols := []string{
		"column_name", "column_type",
		"min", "max",
		"approx_unique",
		"avg", "std",
		"q25", "q50", "q75",
		"count",
		"null_percentage",
	}
	resultColTypes := []dukdb.Type{
		dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR,
		dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_DOUBLE, dukdb.TYPE_DOUBLE,
		dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR, dukdb.TYPE_VARCHAR,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_DOUBLE,
	}
	// Silence unused variable warning for resultColTypes.
	_ = resultColTypes

	resultRows := make([]map[string]any, 0, len(columns))

	for colIdx, colName := range columns {
		colType := colTypes[colIdx]
		isNumeric := summarizeIsNumericType(colType)

		var (
			minVal       any
			maxVal       any
			nullCount    int
			nonNullCount int
			uniqueSet    = make(map[any]struct{})
			sum          float64
			numericVals  []float64
		)

		for _, row := range dataRows {
			val := row[colIdx]
			if val == nil {
				nullCount++
				continue
			}
			nonNullCount++
			uniqueSet[val] = struct{}{}

			// Type-aware min/max
			if minVal == nil {
				minVal = val
				maxVal = val
			} else {
				if summarizeCompareValues(val, minVal) < 0 {
					minVal = val
				}
				if summarizeCompareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}

			if isNumeric {
				if fv, ok := summarizeToFloat64(val); ok {
					sum += fv
					numericVals = append(numericVals, fv)
				}
			}
		}

		totalRows := len(dataRows)
		row := map[string]any{
			"column_name":     colName,
			"column_type":     colType.String(),
			"min":             summarizeFormatValue(minVal),
			"max":             summarizeFormatValue(maxVal),
			"approx_unique":   int64(len(uniqueSet)),
			"avg":             nil,
			"std":             nil,
			"q25":             nil,
			"q50":             nil,
			"q75":             nil,
			"count":           int64(nonNullCount),
			"null_percentage": float64(0),
		}

		if totalRows > 0 {
			row["null_percentage"] = float64(nullCount) / float64(totalRows) * 100.0
		}

		if isNumeric && len(numericVals) > 0 {
			avg := sum / float64(len(numericVals))
			row["avg"] = avg

			if len(numericVals) > 1 {
				// Sample standard deviation (N-1)
				sumDiffSq := 0.0
				for _, v := range numericVals {
					diff := v - avg
					sumDiffSq += diff * diff
				}
				row["std"] = math.Sqrt(sumDiffSq / float64(len(numericVals)-1))
			}

			// Percentiles
			sort.Float64s(numericVals)
			row["q25"] = summarizeFormatValue(summarizePercentile(numericVals, 0.25))
			row["q50"] = summarizeFormatValue(summarizePercentile(numericVals, 0.50))
			row["q75"] = summarizeFormatValue(summarizePercentile(numericVals, 0.75))
		}

		resultRows = append(resultRows, row)
	}

	return &ExecutionResult{
		Columns: resultCols,
		Rows:    resultRows,
	}, nil
}

func summarizeIsNumericType(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER, dukdb.TYPE_BIGINT,
		dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER, dukdb.TYPE_UBIGINT,
		dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE, dukdb.TYPE_HUGEINT, dukdb.TYPE_DECIMAL:
		return true
	}
	return false
}

func summarizeToFloat64(val any) (float64, bool) {
	switch v := val.(type) {
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
	}
	return 0, false
}

func summarizeCompareValues(a, b any) int {
	// Try numeric comparison first
	fa, okA := summarizeToFloat64(a)
	fb, okB := summarizeToFloat64(b)
	if okA && okB {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}
	// Fall back to string comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	return strings.Compare(sa, sb)
}

func summarizeFormatValue(val any) any {
	if val == nil {
		return nil
	}
	return fmt.Sprintf("%v", val)
}

func summarizePercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	// Linear interpolation
	idx := p * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

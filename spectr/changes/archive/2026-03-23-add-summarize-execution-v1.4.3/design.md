# Design: SUMMARIZE Statement Execution

## Existing Partial Implementation (Must Refactor)

There is already a partial `handleSummarize()` implementation in `internal/engine/conn.go` (lines ~811-949). This implementation handles `SUMMARIZE table_name` directly inside the connection layer, bypassing the planner/binder/executor architecture. It must be **refactored** into the proper architecture rather than built from scratch.

### What the existing implementation gets wrong

1. **Wrong column names**: Uses `unique_count` instead of `approx_unique`, includes `null_count` instead of `null_percentage`.
2. **Missing columns**: Does not compute `q25`, `q50`, or `q75` percentiles at all.
3. **`count` semantics**: Uses total row count (`len(dataRows)`) for `count`, but DuckDB uses **non-NULL count** per column.
4. **`SUMMARIZE SELECT` not implemented**: Returns an error for `SUMMARIZE SELECT ...` queries.
5. **Min/max uses string comparison**: Converts values with `fmt.Sprintf("%v", val)` and compares strings, which is wrong for numeric types (e.g., `"100" < "20"` as strings). Must use type-aware comparison.
6. **Standard deviation uses biased estimator**: Divides by N (population variance), but DuckDB uses N-1 (Bessel's correction / sample variance).
7. **Bypasses planner/executor**: The implementation lives in `conn.go` and calls `queryInnerStmt` directly. It must be moved into the planner/executor pipeline.

### Refactoring plan

- Remove `handleSummarize()` from `internal/engine/conn.go`.
- Route `SummarizeStmt` through the binder, planner, and executor like all other statements.
- Reuse the `toFloat64()` helper (already in `conn.go`) in the new executor code.

## Implementation Details

### 1. DuckDB SUMMARIZE Output Format

DuckDB's `SUMMARIZE table` returns a result set with these columns:

| Column | Type | Description |
|--------|------|-------------|
| column_name | VARCHAR | Name of the column |
| column_type | VARCHAR | Data type of the column |
| min | VARCHAR | Minimum value (cast to string) |
| max | VARCHAR | Maximum value (cast to string) |
| approx_unique | BIGINT | Approximate number of distinct values |
| avg | DOUBLE | Average value (numeric columns only, NULL for non-numeric) |
| std | DOUBLE | Standard deviation (numeric columns only, NULL for non-numeric) |
| q25 | VARCHAR | 25th percentile (cast to string) |
| q50 | VARCHAR | 50th percentile / median (cast to string) |
| q75 | VARCHAR | 75th percentile (cast to string) |
| count | BIGINT | Total non-NULL count |
| null_percentage | DOUBLE | Percentage of NULL values (0.0 to 100.0) |

### 2. Physical Plan Node (internal/planner/physical.go)

```go
type PhysicalSummarize struct {
    // For SUMMARIZE table_name:
    Schema    string
    TableName string
    // For SUMMARIZE SELECT ...:
    Query     PhysicalPlan // Inner query plan (nil if table mode)
}

func (*PhysicalSummarize) planNode()              {}
func (*PhysicalSummarize) Type() PhysicalPlanType  { return PhysicalPlanSummarize }
```

### 3. Executor (internal/executor/physical_summarize.go)

The executor:
1. Retrieves all rows (via table scan or inner query execution)
2. For each column, computes:
   - min/max using comparison operators
   - approx_unique using a HyperLogLog or simple distinct counter
   - avg/std using running mean + Welford's algorithm (numeric types only)
   - q25/q50/q75 using sorted samples or T-Digest approximation
   - count (non-NULL values)
   - null_percentage = (total_rows - count) / total_rows * 100

```go
type columnStats struct {
    name       string
    typeName   string
    min        any
    max        any
    distinctSet map[any]struct{} // For approx_unique (or HyperLogLog for large sets)
    sum        float64          // For avg
    sumSq      float64          // For std (Welford's)
    count      int64            // Non-NULL count
    nullCount  int64
    totalRows  int64
    values     []any            // Sampled values for percentiles
}

func (e *Executor) executeSummarize(ctx *ExecContext, plan *planner.PhysicalSummarize) (*ResultSet, error) {
    // 1. Get column metadata
    var columns []catalog.ColumnMeta
    if plan.TableName != "" {
        table, err := e.catalog.GetTable(plan.Schema, plan.TableName)
        columns = table.Columns
    }

    // 2. Initialize per-column stats
    stats := make([]columnStats, len(columns))
    for i, col := range columns {
        stats[i] = columnStats{
            name:        col.Name,
            typeName:    col.Type.String(),
            distinctSet: make(map[any]struct{}),
        }
    }

    // 3. Scan all rows and accumulate statistics
    // Use existing table scan or execute inner query
    scanner := e.createTableScanner(plan.Schema, plan.TableName)
    for scanner.Next() {
        row := scanner.Row()
        for i, val := range row {
            stats[i].totalRows++
            if val == nil {
                stats[i].nullCount++
                continue
            }
            stats[i].count++
            stats[i].distinctSet[val] = struct{}{}
            // Update min/max using type-aware comparison (NOT string comparison).
            // For numeric types, compare as float64. For strings, use lexicographic.
            // For dates/timestamps, compare underlying integer representation.
            updateMinMax(&stats[i], val)
            // Update running mean/variance (numeric types)
            if isNumeric(columns[i].Type) {
                f := toFloat64(val)
                stats[i].sum += f
                stats[i].sumSq += f * f
            }
            // Sample values for percentiles (reservoir sampling)
            if len(stats[i].values) < 10000 {
                stats[i].values = append(stats[i].values, val)
            }
        }
    }

    // 4. Compute final statistics
    result := make([][]any, len(columns))
    for i, s := range stats {
        avg := interface{}(nil)
        std := interface{}(nil)
        if isNumeric(columns[i].Type) && s.count > 0 {
            avgVal := s.sum / float64(s.count)
            avg = avgVal
            if s.count > 1 {
                // Use Bessel's correction (N-1) for sample standard deviation,
                // matching DuckDB's behavior
                variance := (s.sumSq - float64(s.count)*avgVal*avgVal) / float64(s.count-1)
                if variance >= 0 {
                    std = math.Sqrt(variance)
                }
            } else {
                std = nil // Single value: std is NULL (division by zero with N-1)
            }
        }
        nullPct := 0.0
        if s.totalRows > 0 {
            nullPct = float64(s.nullCount) / float64(s.totalRows) * 100.0
        }
        // Compute percentiles from sampled values
        sort.Slice(s.values, func(a, b int) bool { return compare(s.values[a], s.values[b]) < 0 })
        q25 := percentile(s.values, 0.25)
        q50 := percentile(s.values, 0.50)
        q75 := percentile(s.values, 0.75)

        result[i] = []any{
            s.name,                        // column_name
            s.typeName,                    // column_type
            fmt.Sprint(s.min),             // min
            fmt.Sprint(s.max),             // max
            int64(len(s.distinctSet)),     // approx_unique
            avg,                           // avg
            std,                           // std
            fmt.Sprint(q25),               // q25
            fmt.Sprint(q50),               // q50
            fmt.Sprint(q75),               // q75
            s.count,                       // count
            nullPct,                       // null_percentage
        }
    }

    return newResultSet(summarizeColumns(), result), nil
}
```

### 4. SUMMARIZE SELECT ... Mode

For `SUMMARIZE SELECT ...`:
1. Execute the inner query to get a result set
2. Use the result set's column metadata and data to compute the same statistics
3. Return the SUMMARIZE result set

This requires running the inner query to completion, collecting all results, then computing statistics.

### 5. Percentile Computation

For simplicity and correctness:
- Collect all non-NULL values per column (up to a sampling limit for very large tables)
- Sort the values using type-aware comparison (not string comparison)
- Use linear interpolation for percentile calculation (matching DuckDB's default behavior)

For large tables (>10,000 rows), use reservoir sampling to keep memory bounded.

#### Detailed algorithm

1. **Collection phase**: During the row scan, append each non-NULL value to the column's `values` slice. If the slice exceeds 10,000 entries, use reservoir sampling (replace a random existing entry with probability `10000/rowsSeen`).

2. **Sort phase**: After the scan completes, sort the collected values using a type-aware comparator. For numeric types, compare as float64. For strings, use lexicographic comparison. For dates/timestamps, compare the underlying integer representation. Do NOT use `fmt.Sprintf` for comparison.

3. **Interpolation phase**: Compute percentile `p` (where p is 0.25, 0.50, or 0.75) using linear interpolation:
   ```go
   func percentile(sorted []any, p float64) any {
       n := len(sorted)
       if n == 0 {
           return nil
       }
       if n == 1 {
           return sorted[0]
       }
       // Virtual index (0-based)
       idx := p * float64(n-1)
       lower := int(math.Floor(idx))
       upper := int(math.Ceil(idx))
       if lower == upper {
           return sorted[lower]
       }
       // For numeric types, interpolate between lower and upper
       // For non-numeric types, return the nearest value (round down)
       frac := idx - float64(lower)
       lf, lok := toFloat(sorted[lower])
       uf, uok := toFloat(sorted[upper])
       if lok && uok {
           return lf + frac*(uf-lf)
       }
       return sorted[lower] // Non-numeric: return lower bound
   }
   ```

4. **Output**: Cast the percentile result to VARCHAR using `fmt.Sprint()` for the final output row.

## Context

SUMMARIZE is a DuckDB-specific feature that provides a quick overview of data distribution. It's commonly used in data exploration, data quality checks, and notebook-style analysis. It combines multiple aggregate queries into a single convenient output.

## Goals / Non-Goals

- **Goals**: SUMMARIZE for tables and queries, all 12 output columns matching DuckDB format, correct statistics for numeric and non-numeric types
- **Non-Goals**: Streaming/incremental statistics, parallel column computation, persistent statistics caching (ANALYZE handles that separately)

## Decisions

- **Reservoir sampling**: For tables with >10,000 rows, use reservoir sampling for percentile estimation to bound memory usage
- **HyperLogLog vs. exact distinct**: Start with exact distinct counting (hash set). Switch to HyperLogLog if memory becomes an issue for wide tables with high cardinality.
- **String representation**: min, max, q25, q50, q75 are returned as VARCHAR (matching DuckDB), since they must work for all data types

## Risks / Trade-offs

- **Risk**: Full table scan required → Mitigation: This is expected behavior; SUMMARIZE is inherently a full-scan operation
- **Risk**: Memory usage for percentile computation on large tables → Mitigation: Reservoir sampling limits memory to O(sample_size * num_columns)

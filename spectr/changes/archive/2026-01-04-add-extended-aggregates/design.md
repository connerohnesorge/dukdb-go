## Context

This design document provides detailed technical specifications for implementing extended aggregate functions in dukdb-go. The implementation must maintain:

- Pure Go implementation (no CGO)
- API compatibility with duckdb-go
- Vectorized execution model
- Memory efficiency for analytical workloads
- Compatibility with DuckDB v1.4.3 semantics

## Goals / Non-Goals

### Goals
1. Implement statistical aggregates (MEDIAN, MODE, QUANTILE, ENTROPY, SKEWNESS, KURTOSIS)
2. Implement ordered set aggregates (STRING_AGG, LIST, ARRAY_AGG)
3. Implement boolean/bitwise aggregates (BOOL_AND, BOOL_OR, BIT_AND, BIT_OR, BIT_XOR)
4. Implement approximate aggregates (APPROX_COUNT_DISTINCT, APPROX_QUANTILE, APPROX_MEDIAN)
5. Implement time series aggregates (COUNT_IF, FIRST, LAST, ARGMIN, ARGMAX)
6. Implement regression and correlation functions (COVAR_*, CORR, REGR_*)
7. Implement list operations (LIST, LIST_DISTINCT, LIST_REMOVE, LIST_APPEND)

### Non-Goals
- Window function frame extensions (already partially implemented)
- User-defined aggregate functions (separate proposal)
- Distributed aggregate execution
- Parallel aggregation across partitions

## Decisions

### 1. Aggregate Function Architecture

**Decision**: Create specialized aggregate implementation files organized by function category

**Rationale**: Separation of concerns, easier testing, maintainable codebase

**File Structure**:
```
internal/executor/
├── physical_aggregate.go       # Core aggregate operator
├── aggregate_stats.go          # Statistical functions
├── aggregate_approx.go         # Approximate algorithms
├── aggregate_string.go         # String/list aggregation
├── aggregate_time.go           # Time series aggregates
├── aggregate_regr.go           # Regression/correlation
└── aggregate_hypothetical.go   # Hypothetical set aggregates
```

### 2. Statistical Aggregate Implementation Strategy

**Decision**: Use sorted array for median/quantile, hash map for mode

**Rationale**: Balanced performance for typical data sizes

**Implementation** (`internal/executor/aggregate_stats.go`):

```go
package executor

import (
    "math"
    "sort"
)

// MedianState holds state for median computation
type MedianState struct {
    Values []float64
    Sorted bool
}

// ComputeMedian calculates median value
func ComputeMedian(values []float64) float64 {
    if len(values) == 0 {
        return 0
    }
    
    sorted := make([]float64, len(values))
    copy(sorted, values)
    sort.Float64s(sorted)
    
    n := len(sorted)
    mid := n / 2
    
    if n%2 == 0 {
        return (sorted[mid-1] + sorted[mid]) / 2
    }
    return sorted[mid]
}

// ComputeQuantile calculates value at percentile q (0-1)
func ComputeQuantile(values []float64, q float64) float64 {
    if len(values) == 0 || q < 0 || q > 1 {
        return 0
    }
    
    sorted := make([]float64, len(values))
    copy(sorted, values)
    sort.Float64s(sorted)
    
    // Linear interpolation
    pos := float64(len(sorted)-1) * q
    idx := int(pos)
    frac := pos - float64(idx)
    
    if idx+1 < len(sorted) {
        return sorted[idx]*(1-frac) + sorted[idx+1]*frac
    }
    return sorted[idx]
}

// ModeState holds state for mode computation
type ModeState struct {
    Counts map[string]int64
    MaxCount int64
    ModeValue string
}

// ComputeMode finds most frequent value
func ComputeMode(values []any) any {
    if len(values) == 0 {
        return nil
    }
    
    counts := make(map[string]int64)
    var maxCount int64
    var modeVal any
    
    for _, v := range values {
        if v == nil {
            continue
        }
        key := fmt.Sprintf("%v", v)
        counts[key]++
        if counts[key] > maxCount {
            maxCount = counts[key]
            modeVal = v
        }
    }
    
    return modeVal
}

// ComputeEntropy calculates Shannon entropy
// H = -sum(p * log(p)) where p = frequency / total
func ComputeEntropy(values []any) float64 {
    if len(values) == 0 {
        return 0
    }
    
    total := int64(len(values))
    counts := make(map[string]int64)
    
    for _, v := range values {
        if v == nil {
            total--
            continue
        }
        key := fmt.Sprintf("%v", v)
        counts[key]++
    }
    
    if total == 0 {
        return 0
    }
    
    var entropy float64
    for _, count := range counts {
        p := float64(count) / float64(total)
        if p > 0 {
            entropy -= p * math.Log(p)
        }
    }
    
    return entropy
}

// ComputeSkewness calculates Fisher-Pearson coefficient of skewness
// gamma1 = sum((x - mean)^3) / (n * stddev^3)
func ComputeSkewness(values []float64) float64 {
    if len(values) < 3 {
        return 0
    }
    
    // Calculate mean
    var sum float64
    for _, v := range values {
        sum += v
    }
    mean := sum / float64(len(values))
    
    // Calculate stddev
    var sqDiff float64
    for _, v := range values {
        diff := v - mean
        sqDiff += diff * diff
    }
    stddev := math.Sqrt(sqDiff / float64(len(values)))
    
    if stddev == 0 {
        return 0
    }
    
    // Calculate skewness
    var sumCubedDiff float64
    for _, v := range values {
        diff := v - mean
        sumCubedDiff += diff * diff * diff
    }
    
    n := float64(len(values))
    return (n / ((n - 1) * (n - 2))) * (sumCubedDiff / (stddev * stddev * stddev))
}

// ComputeKurtosis calculates excess kurtosis (Fisher's definition)
// gamma2 = [n(n+1) / ((n-1)(n-2)(n-3))] * sum((x-mean)^4)/stddev^4 - 3(n-1)^2/((n-2)(n-3))
func ComputeKurtosis(values []float64) float64 {
    if len(values) < 4 {
        return 0
    }
    
    // Calculate mean
    var sum float64
    for _, v := range values {
        sum += v
    }
    mean := sum / float64(len(values))
    
    // Calculate stddev
    var sqDiff float64
    for _, v := range values {
        diff := v - mean
        sqDiff += diff * diff
    }
    stddev := math.Sqrt(sqDiff / float64(len(values)))
    
    if stddev == 0 {
        return 0
    }
    
    // Calculate kurtosis
    var sumFourthDiff float64
    for _, v := range values {
        diff := v - mean
        sumFourthDiff += diff * diff * diff * diff
    }
    
    n := float64(len(values))
    numerator := n * (n + 1)
    denominator := (n - 1) * (n - 2) * (n - 3)
    correction := 3 * (n - 1) * (n - 1) / ((n - 2) * (n - 3))
    
    return (numerator / denominator) * (sumFourthDiff / math.Pow(stddev, 4)) - correction
}

// VarianceState holds running state for variance computation
type VarianceState struct {
    N int64
    Mean float64
    M2 float64 // Sum of squared differences
}

// UpdateVariance updates running variance state
func (s *VarianceState) Update(x float64) {
    s.N++
    delta := x - s.Mean
    s.Mean += delta / float64(s.N)
    delta2 := x - s.Mean
    s.M2 += delta * delta2
}

// VariancePop returns population variance
func (s *VarianceState) VariancePop() float64 {
    if s.N == 0 {
        return 0
    }
    return s.M2 / float64(s.N)
}

// VarianceSamp returns sample variance
func (s *VarianceState) VarianceSamp() float64 {
    if s.N <= 1 {
        return 0
    }
    return s.M2 / float64(s.N-1)
}

// StdDevPop returns population standard deviation
func (s *VarianceState) StdDevPop() float64 {
    return math.Sqrt(s.VariancePop())
}

// StdDevSamp returns sample standard deviation
func (s *VarianceState) StdDevSamp() float64 {
    return math.Sqrt(s.VarianceSamp())
}
```

### 3. Approximate Aggregate Implementation Strategy

**Decision**: Implement HyperLogLog for APPROX_COUNT_DISTINCT and t-digest for APPROX_QUANTILE

**Rationale**: Industry-standard algorithms with good accuracy/space tradeoffs

**Implementation** (`internal/executor/aggregate_approx.go`):

```go
package executor

import (
    "encoding/binary"
    "hash"
    "hash/fnv"
    "math"
    "sort"
)

// HyperLogLog implements the HLL algorithm for approximate distinct count
type HyperLogLog struct {
    registers []uint8
    m         int       // Number of registers (2^p)
    p         int       // Precision parameter
    alpha     float64   // Bias correction constant
}

// NewHyperLogLog creates a new HLL with specified precision
func NewHyperLogLog(p int) *HyperLogLog {
    m := 1 << p
    alpha := 1.0
    switch m {
    case 16:
        alpha = 0.673
    case 32:
        alpha = 0.697
    case 64:
        alpha = 0.709
    default:
        alpha = 0.7213 / (1.0 + 1.079/float64(m))
    }
    
    return &HyperLogLog{
        registers: make([]uint8, m),
        m:         m,
        p:         p,
        alpha:     alpha,
    }
}

// Add inserts a value into the HLL
func (h *HyperLogLog) Add(value any) {
    hsh := hashValue(value)
    idx := hsh >> (64 - h.p)
    rho := countLeadingZeros(hsh << h.p)
    
    if rho > int(h.registers[idx]) {
        h.registers[idx] = uint8(rho)
    }
}

// Estimate returns the approximate distinct count
func (h *HyperLogLog) Estimate() float64 {
    var sum float64
    for _, reg := range h.registers {
        sum += 1.0 / float64(1<<reg)
    }
    
    estimate := h.alpha * float64(h.m) * float64(h.m) / sum
    zeros := countZeros(h.registers)
    
    // Small number correction
    if estimate < 2.5*float64(h.m) {
        if zeros > 0 {
            return float64(h.m) * math.Log(float64(h.m)/float64(zeros))
        }
    }
    
    // Large number correction
    if estimate > 1/30.0*float64(1<<32) {
        return -float64(1<<32) * math.Log(1-estimate/float64(1<<32))
    }
    
    return estimate
}

func hashValue(value any) uint64 {
    h := fnv.New64a()
    h.Write([]byte(fmt.Sprintf("%v", value)))
    return h.Sum64()
}

func countLeadingZeros(n uint64) int {
    if n == 0 {
        return 64
    }
    count := 0
    for n&0x8000000000000000 == 0 {
        count++
        n <<= 1
    }
    return count
}

func countZeros(registers []uint8) int {
    count := 0
    for _, r := range registers {
        if r == 0 {
            count++
        }
    }
    return count
}

// TDigest implements t-digest for approximate quantiles
type TDigest struct {
    centroids   []Centroid
    compression float64
    delta       float64
    count       float64
}

// Centroid represents a summary point
type Centroid struct {
    Mean   float64
    Weight float64
}

// NewTDigest creates a new t-digest
func NewTDigest(compression float64) *TDigest {
    return &TDigest{
        centroids:   make([]Centroid, 0),
        compression: compression,
    }
}

// Add inserts a value into the t-digest
func (t *TDigest) Add(x float64) {
    t.addCentroid(Centroid{Mean: x, Weight: 1.0})
}

// AddWeighted inserts a weighted value
func (t *TDigest) AddWeighted(x float64, weight float64) {
    t.addCentroid(Centroid{Mean: x, Weight: weight})
}

func (t *TDigest) addCentroid(c Centroid) {
    t.centroids = append(t.centroids, c)
    t.count += c.Weight
    t.compress()
}

func (t *TDigest) compress() {
    t.delta = t.compression / float64(len(t.centroids))
    
    // Sort centroids by mean
    sort.Slice(t.centroids, func(i, j int) bool {
        return t.centroids[i].Mean < t.centroids[j].Mean
    })
    
    // Merge nearby centroids
    var merged []Centroid
    for _, c := range t.centroids {
        if len(merged) == 0 {
            merged = append(merged, c)
            continue
        }
        
        last := &merged[len(merged)-1]
        k := t.count * t.delta
        minWeight := math.Min(k, float64(last.Weight))
        
        if c.Weight <= minWeight || last.Weight < minWeight {
            merged = append(merged, c)
        } else {
            totalWeight := last.Weight + c.Weight
            last.Mean = (last.Mean*last.Weight + c.Mean*c.Weight) / totalWeight
            last.Weight = totalWeight
        }
    }
    
    t.centroids = merged
}

// Quantile returns the approximate value at quantile q
func (t *TDigest) Quantile(q float64) float64 {
    if len(t.centroids) == 0 || q <= 0 || q >= 1 {
        return 0
    }
    
    // Find cumulative weight
    rank := q * (t.count - 1)
    weightSoFar := 0.0
    
    for i, c := range t.centroids {
        weightSoFar += c.Weight
        if weightSoFar >= rank {
            // Interpolate with next centroid if available
            if i+1 < len(t.centroids) {
                nextWeightSoFar := weightSoFar + t.centroids[i+1].Weight
                if nextWeightSoFar > rank {
                    ratio := (rank - (weightSoFar - c.Weight)) / c.Weight
                    return c.Mean + ratio*(t.centroids[i+1].Mean-c.Mean)
                }
            }
            return c.Mean
        }
    }
    
    return t.centroids[len(t.centroids)-1].Mean
}

// Median returns the approximate median
func (t *TDigest) Median() float64 {
    return t.Quantile(0.5)
}
```

### 4. String Aggregation Implementation Strategy

**Decision**: Use string builder with delimiter handling

**Implementation** (`internal/executor/aggregate_string.go`):

```go
package executor

import (
    "strings"
)

// StringAggState holds state for STRING_AGG
type StringAggState struct {
    Builder strings.Builder
    Delimiter string
    First bool
}

// NewStringAggState creates a new string aggregation state
func NewStringAggState(delimiter string) *StringAggState {
    return &StringAggState{
        Delimiter: delimiter,
        First:     true,
    }
}

// Add appends a value to the aggregation
func (s *StringAggState) Add(value any) {
    if value == nil {
        return
    }
    
    str := toString(value)
    if s.First {
        s.Builder.WriteString(str)
        s.First = false
    } else {
        s.Builder.WriteString(s.Delimiter)
        s.Builder.WriteString(str)
    }
}

// Result returns the aggregated string
func (s *StringAggState) Result() string {
    return s.Builder.String()
}

// ListAggState holds state for LIST aggregation
type ListAggState struct {
    Values []any
}

// NewListAggState creates a new list aggregation state
func NewListAggState() *ListAggState {
    return &ListAggState{
        Values: make([]any, 0),
    }
}

// Add appends a value to the list
func (s *ListAggState) Add(value any) {
    s.Values = append(s.Values, value)
}

// Result returns the aggregated list
func (s *ListAggState) Result() []any {
    return s.Values
}

// ListDistinctAggState holds state for LIST with distinct
type ListDistinctAggState struct {
    Seen  map[string]any
    Order []string
    Values []any
}

// NewListDistinctAggState creates a new distinct list aggregation state
func NewListDistinctAggState() *ListDistinctAggState {
    return &ListDistinctAggState{
        Seen:  make(map[string]any),
        Order: make([]string, 0),
        Values: make([]any, 0),
    }
}

// Add appends a value if not already present
func (s *ListDistinctAggState) Add(value any) {
    if value == nil {
        return
    }
    
    key := fmt.Sprintf("%v", value)
    if _, exists := s.Seen[key]; !exists {
        s.Seen[key] = value
        s.Order = append(s.Order, key)
        s.Values = append(s.Values, value)
    }
}

// Result returns the distinct list
func (s *ListDistinctAggState) Result() []any {
    return s.Values
}
```

### 5. Time Series Aggregate Implementation Strategy

**Decision**: State-based aggregation with streaming updates

**Implementation** (`internal/executor/aggregate_time.go`):

```go
package executor

// CountIfState holds state for COUNT_IF
type CountIfState struct {
    Count int64
}

// NewCountIfState creates a new COUNT_IF state
func NewCountIfState() *CountIfState {
    return &CountIfState{Count: 0}
}

// Add increments count if condition is true
func (s *CountIfState) Add(condition bool) {
    if condition {
        s.Count++
    }
}

// FirstState holds state for FIRST aggregation
type FirstState struct {
    Set bool
    Value any
}

// NewFirstState creates a new FIRST state
func NewFirstState() *FirstState {
    return &FirstState{Set: false}
}

// Add sets value if not already set
func (s *FirstState) Add(value any) {
    if !s.Set && value != nil {
        s.Value = value
        s.Set = true
    }
}

// LastState holds state for LAST aggregation
type LastState struct {
    Value any
}

// NewLastState creates a new LAST state
func NewLastState() *LastState {
    return &LastState{}
}

// Add updates the last value
func (s *LastState) Add(value any) {
    if value != nil {
        s.Value = value
    }
}

// ArgMinState holds state for ARGMIN/ARGMAX
type ArgMinState struct {
    MinVal   any
    MinArg   any
    MaxVal   any
    MaxArg   any
    MinSet   bool
    MaxSet   bool
}

// NewArgMinState creates a new ARGMIN/ARGMAX state
func NewArgMinState() *ArgMinState {
    return &ArgMinState{}
}

// Add updates min/max based on value
func (s *ArgMinState) Add(arg, val any) {
    if val == nil {
        return
    }
    
    if !s.MinSet || compareValues(val, s.MinVal) < 0 {
        s.MinVal = val
        s.MinArg = arg
        s.MinSet = true
    }
    
    if !s.MaxSet || compareValues(val, s.MaxVal) > 0 {
        s.MaxVal = val
        s.MaxArg = arg
        s.MaxSet = true
    }
}

// MinResult returns the argument with minimum value
func (s *ArgMinState) MinResult() any {
    return s.MinArg
}

// MaxResult returns the argument with maximum value
func (s *ArgMinState) MaxResult() any {
    return s.MaxArg
}

// MinByState holds state for MIN_BY/MAX_BY
type MinByState struct {
    MinPair [2]any
    MaxPair [2]any
    MinSet  bool
    MaxSet  bool
}

// NewMinByState creates a new MIN_BY/MAX_BY state
func NewMinByState() *MinByState {
    return &MinByState{}
}

// Add updates min/max based on comparison value
func (s *MinByState) Add(val, cmp any) {
    if val == nil || cmp == nil {
        return
    }
    
    if !s.MinSet || compareValues(cmp, s.MinPair[1]) < 0 {
        s.MinPair = [2]any{val, cmp}
        s.MinSet = true
    }
    
    if !s.MaxSet || compareValues(cmp, s.MaxPair[1]) > 0 {
        s.MaxPair = [2]any{val, cmp}
        s.MaxSet = true
    }
}

// MinResult returns the value with minimum comparison
func (s *MinByState) MinResult() any {
    return s.MinPair[0]
}

// MaxResult returns the value with maximum comparison
func (s *MinByState) MaxResult() any {
    return s.MaxPair[0]
}
```

### 6. Regression and Correlation Implementation Strategy

**Decision**: Single-pass computation with running state

**Implementation** (`internal/executor/aggregate_regr.go`):

```go
package executor

// RegressionState holds running state for regression computation
type RegressionState struct {
    N   int64
    SumX float64
    SumY float64
    SumXX float64
    SumYY float64
    SumXY float64
}

// NewRegressionState creates a new regression state
func NewRegressionState() *RegressionState {
    return &RegressionState{}
}

// Update adds a data point
func (s *RegressionState) Update(x, y float64) {
    s.N++
    s.SumX += x
    s.SumY += y
    s.SumXX += x * x
    s.SumYY += y * y
    s.SumXY += x * y
}

// CovarPop returns population covariance
func (s *RegressionState) CovarPop() float64 {
    if s.N == 0 {
        return 0
    }
    return s.SumXY/s.SumX - (s.SumX/s.SumY)*(s.SumY/s.N)
}

// CovarSamp returns sample covariance
func (s *RegressionState) CovarSamp() float64 {
    if s.N <= 1 {
        return 0
    }
    return (s.SumXY - s.SumX*s.SumY/float64(s.N)) / float64(s.N-1)
}

// Corr returns Pearson correlation coefficient
func (s *RegressionState) Corr() float64 {
    if s.N < 2 {
        return 0
    }
    
    numerator := float64(s.N)*s.SumXY - s.SumX*s.SumY
    denominator := math.Sqrt((float64(s.N)*s.SumXX - s.SumX*s.SumX) *
                              (float64(s.N)*s.SumYY - s.SumY*s.SumY))
    
    if denominator == 0 {
        return 0
    }
    return numerator / denominator
}

// RegrIntercept returns the intercept of the regression line
func (s *RegressionState) RegrIntercept() float64 {
    if s.N < 2 {
        return 0
    }
    
    denominator := float64(s.N)*s.SumXX - s.SumX*s.SumX
    if denominator == 0 {
        return 0
    }
    
    return (s.SumY - s.SumXY*s.SumX/s.SumXX) / float64(s.N)
}

// RegrSlope returns the slope of the regression line
func (s *RegressionState) RegrSlope() float64 {
    if s.N < 2 {
        return 0
    }
    
    denominator := float64(s.N)*s.SumXX - s.SumX*s.SumX
    if denominator == 0 {
        return 0
    }
    
    return (float64(s.N)*s.SumXY - s.SumX*s.SumY) / denominator
}

// RegrR2 returns the coefficient of determination (R-squared)
func (s *RegressionState) RegrR2() float64 {
    if s.N < 2 {
        return 0
    }
    
    yMean := s.SumY / float64(s.N)
    ssTot := 0.0
    
    // Calculate sum of squares total (SS_tot)
    // We need to track sum of y values during update for this
    // For now, use correlation squared as approximation
    corr := s.Corr()
    return corr * corr
}
```

### 7. Extended Aggregate Integration

**Decision**: Extend existing `computeAggregate` function in `physical_aggregate.go`

**Implementation Pattern** (modification to existing code):

```go
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
    // Existing aggregates (COUNT, SUM, AVG, MIN, MAX)...
    
    // Statistical aggregates
    case "MEDIAN":
        return op.computeMedian(fn.Args, rows)
    case "QUANTILE":
        return op.computeQuantile(fn.Args, rows)
    case "MODE":
        return op.computeMode(fn.Args, rows)
    case "ENTROPY":
        return op.computeEntropy(fn.Args, rows)
    case "SKEWNESS":
        return op.computeSkewness(fn.Args, rows)
    case "KURTOSIS":
        return op.computeKurtosis(fn.Args, rows)
    case "VAR_POP":
        return op.computeVarPop(fn.Args, rows)
    case "VAR_SAMP":
        return op.computeVarSamp(fn.Args, rows)
    case "STDDEV_POP":
        return op.computeStddevPop(fn.Args, rows)
    case "STDDEV_SAMP":
        return op.computeStddevSamp(fn.Args, rows)
    
    // Approximate aggregates
    case "APPROX_COUNT_DISTINCT":
        return op.computeApproxCountDistinct(fn.Args, rows)
    case "APPROX_QUANTILE":
        return op.computeApproxQuantile(fn.Args, rows)
    case "APPROX_MEDIAN":
        return op.computeApproxMedian(fn.Args, rows)
    
    // Boolean/bitwise aggregates
    case "BOOL_AND":
        return op.computeBoolAnd(fn.Args, rows)
    case "BOOL_OR":
        return op.computeBoolOr(fn.Args, rows)
    case "BIT_AND":
        return op.computeBitAnd(fn.Args, rows)
    case "BIT_OR":
        return op.computeBitOr(fn.Args, rows)
    case "BIT_XOR":
        return op.computeBitXor(fn.Args, rows)
    
    // String/list aggregates
    case "STRING_AGG":
        return op.computeStringAgg(fn.Args, rows)
    case "GROUP_CONCAT":
        return op.computeGroupConcat(fn.Args, rows)
    case "LIST":
        return op.computeList(fn.Args, rows)
    case "LIST_DISTINCT":
        return op.computeListDistinct(fn.Args, rows)
    
    // Time series aggregates
    case "COUNT_IF":
        return op.computeCountIf(fn.Args, rows)
    case "FIRST":
        return op.computeFirst(fn.Args, rows)
    case "LAST":
        return op.computeLast(fn.Args, rows)
    case "ARGMIN":
        return op.computeArgmin(fn.Args, rows)
    case "ARGMAX":
        return op.computeArgmax(fn.Args, rows)
    case "MIN_BY":
        return op.computeMinBy(fn.Args, rows)
    case "MAX_BY":
        return op.computeMaxBy(fn.Args, rows)
    
    // Regression/correlation
    case "COVAR_POP":
        return op.computeCovarPop(fn.Args, rows)
    case "COVAR_SAMP":
        return op.computeCovarSamp(fn.Args, rows)
    case "CORR":
        return op.computeCorr(fn.Args, rows)
    case "REGR_INTERCEPT":
        return op.computeRegrIntercept(fn.Args, rows)
    case "REGR_SLOPE":
        return op.computeRegrSlope(fn.Args, rows)
    case "REGR_R2":
        return op.computeRegrR2(fn.Args, rows)
    
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
```

### 8. Binder Integration

**Decision**: Extend `bindFunctionCall` in `internal/binder/bind_expr.go`

**Implementation Pattern**:

```go
func (b *Binder) bindFunctionCall(fn *parser.FunctionCall) (binder.BoundExpr, error) {
    // Validate argument counts for new aggregates
    switch strings.ToUpper(fn.Name) {
    case "MEDIAN":
        if len(fn.Args) != 1 {
            return nil, &dukdb.Error{
                Type:   dukdb.ErrorTypeBinder,
                Msg:    "MEDIAN requires exactly 1 argument",
                Detail: "",
            }
        }
    case "QUANTILE":
        if len(fn.Args) != 2 {
            return nil, &dukdb.Error{
                Type:   dukdb.ErrorTypeBinder,
                Msg:    "QUANTILE requires exactly 2 arguments",
                Detail: "",
            }
        }
        // Validate quantile value is between 0 and 1
        // ...
    case "APPROX_COUNT_DISTINCT":
        // Allow any argument type (handled at execution)
        // ...
    // ... similar validation for other functions
    }
    
    // ... existing binding logic
}
```

## Risks / Trade-offs

| Risk | Mitigation |
|------|------------|
| Memory usage for sorting in MEDIAN | Implement streaming algorithm for large datasets |
| Approximate algorithm accuracy | Make precision configurable, default to DuckDB-compatible settings |
| Numeric overflow in variance | Use Kahan summation or double-double arithmetic for large N |
| String concatenation performance | Use string builder with pre-allocation |

## Migration Plan

1. Add new aggregate function implementations in specialized files (non-breaking)
2. Extend computeAggregate switch statement with new cases
3. Update binder validation for new function signatures
4. Add unit tests for each function category
5. Add integration tests against DuckDB reference
6. Update compatibility test suite

## Open Questions

1. Should APPROX_COUNT_DISTINCT precision be configurable via session parameter?
2. How to handle DISTINCT modifier for new aggregate functions consistently?
3. Should we support FILTER clause for aggregate functions (SQL:2003)?

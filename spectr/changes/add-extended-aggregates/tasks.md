## 1. Statistical Aggregate Implementation

### 1.1 Median and Quantile Functions
- [x] 1.1.1 Create `internal/executor/aggregate_stats.go` with ComputeMedian function
- [x] 1.1.2 Add ComputeQuantile function for single quantile
- [ ] 1.1.3 Add support for QUANTILE with array of quantiles (returns array)
- [x] 1.1.4 Add PERCENTILE_CONT and PERCENTILE_DISC functions with interpolation
- [x] 1.1.5 Implement computeMedian method in PhysicalAggregateOperator
- [x] 1.1.6 Implement computeQuantile method in PhysicalAggregateOperator
- [x] 1.1.7 Handle NULL values and empty inputs correctly
- [x] 1.1.8 Add unit tests for median with even/odd number of values
- [x] 1.1.9 Add unit tests for quantile interpolation

### 1.2 Mode and Entropy Functions
- [x] 1.2.1 Implement ComputeMode function using hash map
- [x] 1.2.2 Implement ComputeEntropy function for Shannon entropy
- [x] 1.2.3 Add computeMode method in PhysicalAggregateOperator
- [x] 1.2.4 Add computeEntropy method in PhysicalAggregateOperator
- [x] 1.2.5 Handle multiple modes (return first encountered or error)
- [x] 1.2.6 Add unit tests for mode with various data types
- [x] 1.2.7 Add unit tests for entropy calculation

### 1.3 Skewness and Kurtosis Functions
- [x] 1.3.1 Implement ComputeSkewness function (Fisher-Pearson coefficient)
- [x] 1.3.2 Implement ComputeKurtosis function (excess kurtosis)
- [x] 1.3.3 Add computeSkewness method in PhysicalAggregateOperator
- [x] 1.3.4 Add computeKurtosis method in PhysicalAggregateOperator
- [x] 1.3.5 Handle edge cases (N < 3 for skewness, N < 4 for kurtosis)
- [x] 1.3.6 Add unit tests with known values for verification

### 1.4 Variance and Standard Deviation Functions
- [ ] 1.4.1 Implement VarianceState struct with running state
- [ ] 1.4.2 Add UpdateVariance method for streaming computation
- [x] 1.4.3 Implement VariancePop, VarianceSamp methods
- [x] 1.4.4 Implement StdDevPop, StdDevSamp methods
- [x] 1.4.5 Add computeVarPop, computeVarSamp methods
- [x] 1.4.6 Add computeStddevPop, computeStddevSamp methods
- [x] 1.4.7 Add unit tests comparing with numpy/scipy reference values

## 2. Approximate Aggregate Implementation

### 2.1 HyperLogLog for APPROX_COUNT_DISTINCT
- [x] 2.1.1 Create HyperLogLog struct in `aggregate_approx.go`
- [x] 2.1.2 Implement NewHyperLogLog with configurable precision
- [x] 2.1.3 Implement Add method with hash computation
- [x] 2.1.4 Implement Estimate method with bias correction
- [x] 2.1.5 Add small/large number corrections
- [x] 2.1.6 Implement computeApproxCountDistinct method
- [x] 2.1.7 Add unit tests comparing with exact COUNT(DISTINCT)
- [ ] 2.1.8 Test accuracy at various data sizes (1K, 100K, 1M)

### 2.2 T-Digest for APPROX_QUANTILE
- [x] 2.2.1 Create TDigest struct in `aggregate_approx.go`
- [x] 2.2.2 Implement Centroid struct with mean and weight
- [x] 2.2.3 Implement Add and AddWeighted methods
- [x] 2.2.4 Implement compress method for merging centroids
- [x] 2.2.5 Implement Quantile method with interpolation
- [x] 2.2.6 Implement Median method
- [x] 2.2.7 Add computeApproxQuantile method
- [x] 2.2.8 Add computeApproxMedian method
- [x] 2.2.9 Add unit tests comparing with exact QUANTILE

## 3. Boolean and Bitwise Aggregate Implementation

### 3.1 Boolean Aggregates
- [x] 3.1.1 Implement computeBoolAnd method (AND of all values)
- [x] 3.1.2 Implement computeBoolOr method (OR of all values)
- [x] 3.1.3 Handle NULL propagation (ignore NULL values)
- [x] 3.1.4 Add unit tests for BOOL_AND with TRUE/FALSE/NULL
- [x] 3.1.5 Add unit tests for BOOL_OR with TRUE/FALSE/NULL

### 3.2 Bitwise Aggregates
- [x] 3.2.1 Implement computeBitAnd method (bitwise AND)
- [x] 3.2.2 Implement computeBitOr method (bitwise OR)
- [x] 3.2.3 Implement computeBitXor method (bitwise XOR)
- [x] 3.2.4 Handle NULL propagation correctly
- [x] 3.2.5 Add unit tests for bitwise operations

## 4. String and List Aggregate Implementation

### 4.1 String Aggregation
- [x] 4.1.1 Create StringAggState struct in `aggregate_string.go`
- [x] 4.1.2 Implement NewStringAggState with delimiter
- [x] 4.1.3 Implement Add method for string concatenation
- [x] 4.1.4 Implement Result method
- [x] 4.1.5 Add computeStringAgg method
- [x] 4.1.6 Add computeGroupConcat method (MySQL compatibility)
- [ ] 4.1.7 Handle ORDER BY within aggregate if specified
- [x] 4.1.8 Add unit tests with various delimiters

### 4.2 List Aggregation
- [x] 4.2.1 Create ListAggState struct
- [x] 4.2.2 Implement Add and Result methods
- [x] 4.2.3 Add computeList method
- [x] 4.2.4 Create ListDistinctAggState struct
- [x] 4.2.5 Add computeListDistinct method
- [ ] 4.2.6 Handle ORDER BY for list aggregation
- [x] 4.2.7 Add unit tests for LIST aggregation
- [x] 4.2.8 Add unit tests for LIST with DISTINCT

## 5. Time Series Aggregate Implementation

### 5.1 Conditional and Positional Aggregates
- [x] 5.1.1 Create CountIfState struct in `aggregate_time.go`
- [x] 5.1.2 Add computeCountIf method
- [x] 5.1.3 Create FirstState and LastState structs
- [x] 5.1.4 Add computeFirst method
- [x] 5.1.5 Add computeLast method
- [x] 5.1.6 Handle NULL correctly in FIRST/LAST
- [x] 5.1.7 Add unit tests for COUNT_IF
- [x] 5.1.8 Add unit tests for FIRST/LAST

### 5.2 ArgMin/ArgMax Functions
- [x] 5.2.1 Create ArgMinState struct
- [x] 5.2.2 Implement Add method for dual-column operation
- [x] 5.2.3 Implement MinResult and MaxResult methods
- [x] 5.2.4 Add computeArgmin method
- [x] 5.2.5 Add computeArgmax method
- [x] 5.2.6 Create MinByState struct for MIN_BY/MAX_BY
- [x] 5.2.7 Add computeMinBy method
- [x] 5.2.8 Add computeMaxBy method
- [x] 5.2.9 Add unit tests for ARGMIN/ARGMAX

## 6. Regression and Correlation Implementation

### 6.1 Covariance and Correlation
- [x] 6.1.1 Create RegressionState struct in `aggregate_regr.go`
- [x] 6.1.2 Implement Update method for two-variable streaming
- [x] 6.1.3 Implement CovarPop method
- [x] 6.1.4 Implement CovarSamp method
- [x] 6.1.5 Implement Corr method (Pearson correlation)
- [x] 6.1.6 Add computeCovarPop method
- [x] 6.1.7 Add computeCovarSamp method
- [x] 6.1.8 Add computeCorr method
- [x] 6.1.9 Add unit tests with known correlation values

### 6.2 Linear Regression
- [x] 6.2.1 Implement RegrIntercept method
- [x] 6.2.2 Implement RegrSlope method
- [x] 6.2.3 Implement RegrR2 method (R-squared)
- [x] 6.2.4 Add computeRegrIntercept method
- [x] 6.2.5 Add computeRegrSlope method
- [x] 6.2.6 Add computeRegrR2 method
- [x] 6.2.7 Add unit tests with simple linear data

## 7. Binder Integration

### 7.1 Function Validation
- [x] 7.1.1 Update bindFunctionCall in `bind_expr.go` for MEDIAN validation
- [x] 7.1.2 Add validation for QUANTILE (1-2 args, quantile in [0,1])
- [x] 7.1.3 Add validation for APPROX_COUNT_DISTINCT
- [x] 7.1.4 Add validation for APPROX_QUANTILE
- [x] 7.1.5 Add validation for two-argument functions (COVAR_*, CORR, REGR_*)
- [x] 7.1.6 Add validation for ARGMIN/ARGMAX (2 arguments)
- [x] 7.1.7 Add validation for MIN_BY/MAX_BY (2 arguments)
- [x] 7.1.8 Add validation for STRING_AGG (1-2 arguments)

### 7.2 Type Resolution
- [x] 7.2.1 Ensure MEDIAN returns same type as input
- [x] 7.2.2 Ensure QUANTILE returns DOUBLE
- [x] 7.2.3 Ensure MODE returns same type as input
- [x] 7.2.4 Ensure APPROX_COUNT_DISTINCT returns BIGINT
- [x] 7.2.5 Ensure VAR_*/STDDEV_* return DOUBLE
- [x] 7.2.6 Ensure CORR returns DOUBLE
- [x] 7.2.7 Ensure LIST returns LIST type

## 8. Integration Testing

### 8.1 Unit Tests
- [x] 8.1.1 Add comprehensive unit tests for each function category
- [x] 8.1.2 Test edge cases (empty input, all NULL, mixed NULL)
- [x] 8.1.3 Test with various data types (numeric, string, temporal)
- [x] 8.1.4 Test DISTINCT modifier where applicable
- [ ] 8.1.5 Test FILTER clause where applicable

### 8.2 Compatibility Tests
- [ ] 8.2.1 Add compatibility tests comparing with DuckDB reference
- [ ] 8.2.2 Test MEDIAN against DuckDB output
- [ ] 8.2.3 Test QUANTILE against DuckDB output
- [ ] 8.2.4 Test MODE against DuckDB output
- [ ] 8.2.5 Test APPROX_* against DuckDB (within error tolerance)
- [ ] 8.2.6 Test all regression functions against DuckDB

### 8.3 Integration Tests
- [ ] 8.3.1 Test aggregates in GROUP BY context
- [ ] 8.3.2 Test with ORDER BY on aggregates
- [ ] 8.3.3 Test with HAVING clause
- [ ] 8.3.4 Test in subqueries
- [ ] 8.3.5 Test with window functions

## 9. Performance Testing

### 9.1 Benchmark Tests
- [ ] 9.1.1 Add benchmark for MEDIAN on large datasets
- [ ] 9.1.2 Add benchmark for APPROX_COUNT_DISTINCT vs COUNT(DISTINCT)
- [ ] 9.1.3 Add benchmark for variance/stddev vs naive computation
- [ ] 9.1.4 Profile memory usage for string aggregation

### 9.2 Optimization Opportunities
- [ ] 9.2.1 Optimize median computation for streaming data
- [ ] 9.2.2 Optimize memory for HyperLogLog (reduce register size)
- [ ] 9.2.3 Optimize t-digest compression frequency
- [ ] 9.2.4 Consider parallel aggregation for large groups

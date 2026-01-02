## 1. Statistical Aggregate Implementation

### 1.1 Median and Quantile Functions
- [ ] 1.1.1 Create `internal/executor/aggregate_stats.go` with ComputeMedian function
- [ ] 1.1.2 Add ComputeQuantile function for single quantile
- [ ] 1.1.3 Add support for QUANTILE with array of quantiles (returns array)
- [ ] 1.1.4 Add PERCENTILE_CONT and PERCENTILE_DISC functions with interpolation
- [ ] 1.1.5 Implement computeMedian method in PhysicalAggregateOperator
- [ ] 1.1.6 Implement computeQuantile method in PhysicalAggregateOperator
- [ ] 1.1.7 Handle NULL values and empty inputs correctly
- [ ] 1.1.8 Add unit tests for median with even/odd number of values
- [ ] 1.1.9 Add unit tests for quantile interpolation

### 1.2 Mode and Entropy Functions
- [ ] 1.2.1 Implement ComputeMode function using hash map
- [ ] 1.2.2 Implement ComputeEntropy function for Shannon entropy
- [ ] 1.2.3 Add computeMode method in PhysicalAggregateOperator
- [ ] 1.2.4 Add computeEntropy method in PhysicalAggregateOperator
- [ ] 1.2.5 Handle multiple modes (return first encountered or error)
- [ ] 1.2.6 Add unit tests for mode with various data types
- [ ] 1.2.7 Add unit tests for entropy calculation

### 1.3 Skewness and Kurtosis Functions
- [ ] 1.3.1 Implement ComputeSkewness function (Fisher-Pearson coefficient)
- [ ] 1.3.2 Implement ComputeKurtosis function (excess kurtosis)
- [ ] 1.3.3 Add computeSkewness method in PhysicalAggregateOperator
- [ ] 1.3.4 Add computeKurtosis method in PhysicalAggregateOperator
- [ ] 1.3.5 Handle edge cases (N < 3 for skewness, N < 4 for kurtosis)
- [ ] 1.3.6 Add unit tests with known values for verification

### 1.4 Variance and Standard Deviation Functions
- [ ] 1.4.1 Implement VarianceState struct with running state
- [ ] 1.4.2 Add UpdateVariance method for streaming computation
- [ ] 1.4.3 Implement VariancePop, VarianceSamp methods
- [ ] 1.4.4 Implement StdDevPop, StdDevSamp methods
- [ ] 1.4.5 Add computeVarPop, computeVarSamp methods
- [ ] 1.4.6 Add computeStddevPop, computeStddevSamp methods
- [ ] 1.4.7 Add unit tests comparing with numpy/scipy reference values

## 2. Approximate Aggregate Implementation

### 2.1 HyperLogLog for APPROX_COUNT_DISTINCT
- [ ] 2.1.1 Create HyperLogLog struct in `aggregate_approx.go`
- [ ] 2.1.2 Implement NewHyperLogLog with configurable precision
- [ ] 2.1.3 Implement Add method with hash computation
- [ ] 2.1.4 Implement Estimate method with bias correction
- [ ] 2.1.5 Add small/large number corrections
- [ ] 2.1.6 Implement computeApproxCountDistinct method
- [ ] 2.1.7 Add unit tests comparing with exact COUNT(DISTINCT)
- [ ] 2.1.8 Test accuracy at various data sizes (1K, 100K, 1M)

### 2.2 T-Digest for APPROX_QUANTILE
- [ ] 2.2.1 Create TDigest struct in `aggregate_approx.go`
- [ ] 2.2.2 Implement Centroid struct with mean and weight
- [ ] 2.2.3 Implement Add and AddWeighted methods
- [ ] 2.2.4 Implement compress method for merging centroids
- [ ] 2.2.5 Implement Quantile method with interpolation
- [ ] 2.2.6 Implement Median method
- [ ] 2.2.7 Add computeApproxQuantile method
- [ ] 2.2.8 Add computeApproxMedian method
- [ ] 2.2.9 Add unit tests comparing with exact QUANTILE

## 3. Boolean and Bitwise Aggregate Implementation

### 3.1 Boolean Aggregates
- [ ] 3.1.1 Implement computeBoolAnd method (AND of all values)
- [ ] 3.1.2 Implement computeBoolOr method (OR of all values)
- [ ] 3.1.3 Handle NULL propagation (ignore NULL values)
- [ ] 3.1.4 Add unit tests for BOOL_AND with TRUE/FALSE/NULL
- [ ] 3.1.5 Add unit tests for BOOL_OR with TRUE/FALSE/NULL

### 3.2 Bitwise Aggregates
- [ ] 3.2.1 Implement computeBitAnd method (bitwise AND)
- [ ] 3.2.2 Implement computeBitOr method (bitwise OR)
- [ ] 3.2.3 Implement computeBitXor method (bitwise XOR)
- [ ] 3.2.4 Handle NULL propagation correctly
- [ ] 3.2.5 Add unit tests for bitwise operations

## 4. String and List Aggregate Implementation

### 4.1 String Aggregation
- [ ] 4.1.1 Create StringAggState struct in `aggregate_string.go`
- [ ] 4.1.2 Implement NewStringAggState with delimiter
- [ ] 4.1.3 Implement Add method for string concatenation
- [ ] 4.1.4 Implement Result method
- [ ] 4.1.5 Add computeStringAgg method
- [ ] 4.1.6 Add computeGroupConcat method (MySQL compatibility)
- [ ] 4.1.7 Handle ORDER BY within aggregate if specified
- [ ] 4.1.8 Add unit tests with various delimiters

### 4.2 List Aggregation
- [ ] 4.2.1 Create ListAggState struct
- [ ] 4.2.2 Implement Add and Result methods
- [ ] 4.2.3 Add computeList method
- [ ] 4.2.4 Create ListDistinctAggState struct
- [ ] 4.2.5 Add computeListDistinct method
- [ ] 4.2.6 Handle ORDER BY for list aggregation
- [ ] 4.2.7 Add unit tests for LIST aggregation
- [ ] 4.2.8 Add unit tests for LIST with DISTINCT

## 5. Time Series Aggregate Implementation

### 5.1 Conditional and Positional Aggregates
- [ ] 5.1.1 Create CountIfState struct in `aggregate_time.go`
- [ ] 5.1.2 Add computeCountIf method
- [ ] 5.1.3 Create FirstState and LastState structs
- [ ] 5.1.4 Add computeFirst method
- [ ] 5.1.5 Add computeLast method
- [ ] 5.1.6 Handle NULL correctly in FIRST/LAST
- [ ] 5.1.7 Add unit tests for COUNT_IF
- [ ] 5.1.8 Add unit tests for FIRST/LAST

### 5.2 ArgMin/ArgMax Functions
- [ ] 5.2.1 Create ArgMinState struct
- [ ] 5.2.2 Implement Add method for dual-column operation
- [ ] 5.2.3 Implement MinResult and MaxResult methods
- [ ] 5.2.4 Add computeArgmin method
- [ ] 5.2.5 Add computeArgmax method
- [ ] 5.2.6 Create MinByState struct for MIN_BY/MAX_BY
- [ ] 5.2.7 Add computeMinBy method
- [ ] 5.2.8 Add computeMaxBy method
- [ ] 5.2.9 Add unit tests for ARGMIN/ARGMAX

## 6. Regression and Correlation Implementation

### 6.1 Covariance and Correlation
- [ ] 6.1.1 Create RegressionState struct in `aggregate_regr.go`
- [ ] 6.1.2 Implement Update method for two-variable streaming
- [ ] 6.1.3 Implement CovarPop method
- [ ] 6.1.4 Implement CovarSamp method
- [ ] 6.1.5 Implement Corr method (Pearson correlation)
- [ ] 6.1.6 Add computeCovarPop method
- [ ] 6.1.7 Add computeCovarSamp method
- [ ] 6.1.8 Add computeCorr method
- [ ] 6.1.9 Add unit tests with known correlation values

### 6.2 Linear Regression
- [ ] 6.2.1 Implement RegrIntercept method
- [ ] 6.2.2 Implement RegrSlope method
- [ ] 6.2.3 Implement RegrR2 method (R-squared)
- [ ] 6.2.4 Add computeRegrIntercept method
- [ ] 6.2.5 Add computeRegrSlope method
- [ ] 6.2.6 Add computeRegrR2 method
- [ ] 6.2.7 Add unit tests with simple linear data

## 7. Binder Integration

### 7.1 Function Validation
- [ ] 7.1.1 Update bindFunctionCall in `bind_expr.go` for MEDIAN validation
- [ ] 7.1.2 Add validation for QUANTILE (1-2 args, quantile in [0,1])
- [ ] 7.1.3 Add validation for APPROX_COUNT_DISTINCT
- [ ] 7.1.4 Add validation for APPROX_QUANTILE
- [ ] 7.1.5 Add validation for two-argument functions (COVAR_*, CORR, REGR_*)
- [ ] 7.1.6 Add validation for ARGMIN/ARGMAX (2 arguments)
- [ ] 7.1.7 Add validation for MIN_BY/MAX_BY (2 arguments)
- [ ] 7.1.8 Add validation for STRING_AGG (1-2 arguments)

### 7.2 Type Resolution
- [ ] 7.2.1 Ensure MEDIAN returns same type as input
- [ ] 7.2.2 Ensure QUANTILE returns DOUBLE
- [ ] 7.2.3 Ensure MODE returns same type as input
- [ ] 7.2.4 Ensure APPROX_COUNT_DISTINCT returns BIGINT
- [ ] 7.2.5 Ensure VAR_*/STDDEV_* return DOUBLE
- [ ] 7.2.6 Ensure CORR returns DOUBLE
- [ ] 7.2.7 Ensure LIST returns LIST type

## 8. Integration Testing

### 8.1 Unit Tests
- [ ] 8.1.1 Add comprehensive unit tests for each function category
- [ ] 8.1.2 Test edge cases (empty input, all NULL, mixed NULL)
- [ ] 8.1.3 Test with various data types (numeric, string, temporal)
- [ ] 8.1.4 Test DISTINCT modifier where applicable
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

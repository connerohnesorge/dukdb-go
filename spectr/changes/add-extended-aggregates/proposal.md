# Change: Add Extended Aggregate Functions

## Why

DuckDB v1.4.3 includes a rich set of aggregate functions essential for statistical analysis, including median/quantile estimation, mode, entropy, skewness, kurtosis, regression, correlation, and time series aggregates. The current dukdb-go implementation only supports basic aggregates (COUNT, SUM, AVG, MIN, MAX), severely limiting its utility for analytical workloads.

Without these extended aggregates:
1. Statistical analysis workloads cannot be expressed natively
2. Data science queries requiring mode, quantiles, or distributions fail
3. Time series analysis lacks COUNT_IF, FIRST, LAST, ARGMIN/ARGMAX
4. Regression and correlation analysis (COVAR, CORR, REGR_*) are unavailable
5. Approximate aggregates (HyperLogLog, t-digest) for large-scale data are missing
6. String aggregation (STRING_AGG, LIST) is not supported
7. API compatibility gaps exist with the official duckdb-go driver

## What Changes

### Breaking Changes

- None (purely additive functionality)

### New Features

**1. Statistical Aggregates**
- **Median and Quantiles**: MEDIAN(expr), QUANTILE(expr, q), QUANTILE(expr, [q1, q2]), PERCENTILE_CONT(expr, q), PERCENTILE_DISC(expr, q)
- **Distribution Measures**: MODE(expr), ENTROPY(expr), SKEWNESS(expr), KURTOSIS(expr)
- **Variance/StdDev**: VAR_POP(expr), VAR_SAMP(expr), STDDEV_POP(expr), STDDEV_SAMP(expr)

**2. Ordered Set Aggregates**
- **String Aggregation**: STRING_AGG(expr, delimiter), GROUP_CONCAT(expr, delimiter)
- **List Aggregation**: LIST(expr), ARRAY_AGG(expr ORDER BY ...)
- **Hypothetical Set**: RANK(expr, ...) WITHIN GROUP, DENSE_RANK(...) WITHIN GROUP, PERCENT_RANK(...) WITHIN GROUP, CUME_DIST(...) WITHIN GROUP

**3. Bitwise and Boolean Aggregates**
- **Boolean**: BOOL_AND(expr), BOOL_OR(expr)
- **Bitwise**: BIT_AND(expr), BIT_OR(expr), BIT_XOR(expr)

**4. Approximate Aggregates**
- **Count Distinct**: APPROX_COUNT_DISTINCT(expr) - HyperLogLog based
- **Quantiles**: APPROX_QUANTILE(expr, q), APPROX_MEDIAN(expr) - T-digest based

**5. Time Series Aggregates**
- **Conditional Count**: COUNT_IF(expr)
- **First/Last**: FIRST(expr), LAST(expr)
- **Extremes**: ARGMIN(arg, val), ARGMAX(arg, val), MIN_BY(arg, val), MAX_BY(arg, val)

**6. Regression and Correlation**
- **Covariance**: COVAR_POP(expr1, expr2), COVAR_SAMP(expr1, expr2)
- **Correlation**: CORR(expr1, expr2)
- **Regression**: REGR_INTERCEPT(y, x), REGR_SLOPE(y, x), REGR_R2(y, x)

**7. List Operations**
- **List Aggregation**: LIST(expr), LIST_DISTINCT(expr)
- **List Modification**: LIST_REMOVE(list, index), LIST_APPEND(list, val)

### Internal Changes

- New `internal/executor/physical_aggregate.go` extended aggregate implementations
- New `internal/executor/aggregate_approx.go` for approximate algorithms (HLL, t-digest)
- New `internal/executor/aggregate_stats.go` for statistical functions
- New `internal/executor/aggregate_string.go` for string aggregation
- New `internal/executor/aggregate_time.go` for time series aggregates
- Updated `internal/binder/bind_expr.go` for new aggregate function binding
- Updated `internal/parser/ast.go` with new function name constants

## Impact

### Affected Specs

- `execution-engine`: Extended aggregate operators and implementations
- `binder`: New aggregate function resolution and validation
- `expression-eval`: New aggregate function cases in expression evaluation
- `compatibility`: Compatibility tests against duckdb-go for new functions

### Affected Code

| File | Change Type | Description |
|------|-------------|-------------|
| `internal/executor/physical_aggregate.go` | MODIFIED | Extended aggregate switch cases |
| `internal/executor/expr.go` | MODIFIED | Aggregate function cases in computeAggregate |
| `internal/binder/bind_expr.go` | MODIFIED | Aggregate function binding and validation |
| `internal/executor/aggregate_stats.go` | ADDED | Statistical aggregates (MEDIAN, MODE, etc.) |
| `internal/executor/aggregate_approx.go` | ADDED | Approximate algorithms (HLL, t-digest) |
| `internal/executor/aggregate_string.go` | ADDED | String/list aggregation functions |
| `internal/executor/aggregate_time.go` | ADDED | Time series aggregate functions |
| `internal/executor/aggregate_regr.go` | ADDED | Regression and correlation functions |
| `internal/vector/vector.go` | MODIFIED | Vector operations for aggregations |

### Dependencies

- This proposal depends on: (none)
- This proposal blocks: (none)

### Compatibility

- Full compatibility with DuckDB v1.4.3 aggregate function behavior
- API-compatible with duckdb-go for all aggregate operations
- All new functions match DuckDB SQL syntax and semantics

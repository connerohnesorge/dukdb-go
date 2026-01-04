## ADDED Requirements

### Requirement: Statistical Aggregate Functions

The executor SHALL implement statistical aggregate functions including median, quantile, mode, entropy, skewness, and kurtosis.

#### Scenario: MEDIAN calculation with odd number of values
- WHEN executing `SELECT MEDIAN(val) FROM (VALUES (1), (3), (5), (2), (4)) AS t(val)`
- THEN the executor SHALL return 3.0
- AND the value at position (n+1)/2 after sorting is returned

#### Scenario: MEDIAN calculation with even number of values
- WHEN executing `SELECT MEDIAN(val) FROM (VALUES (1), (2), (3), (4)) AS t(val)`
- THEN the executor SHALL return 2.5
- AND the average of the two middle values is returned

#### Scenario: QUANTILE at specific percentile
- WHEN executing `SELECT QUANTILE(val, 0.75) FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS t(val)`
- THEN the executor SHALL return approximately 7.75
- AND linear interpolation is used between adjacent values

#### Scenario: QUANTILE with array of percentiles
- WHEN executing `SELECT QUANTILE(val, [0.25, 0.5, 0.75]) FROM t`
- THEN the executor SHALL return an array with three values
- AND each value corresponds to the respective percentile

#### Scenario: MODE with single mode
- WHEN executing `SELECT MODE(val) FROM (VALUES ('a'), ('b'), ('a'), ('c'), ('a')) AS t(val)`
- THEN the executor SHALL return 'a'
- AND 'a' has the highest frequency count

#### Scenario: MODE with multiple values having same max frequency
- WHEN executing `SELECT MODE(val) FROM (VALUES ('a'), ('b')) AS t(val)`
- THEN the executor SHALL return one of the values
- AND the behavior matches DuckDB for tie-breaking

#### Scenario: ENTROPY calculation
- WHEN executing `SELECT ENTROPY(val) FROM (VALUES ('a'), ('a'), ('b'), ('b'), ('c')) AS t(val)`
- THEN the executor SHALL return the Shannon entropy
- AND H = -sum(p * log(p)) where p is frequency/total

#### Scenario: SKEWNESS calculation
- WHEN executing `SELECT SKEWNESS(val) FROM t`
- THEN the executor SHALL return the Fisher-Pearson coefficient of skewness
- AND the formula matches DuckDB's implementation

#### Scenario: KURTOSIS calculation
- WHEN executing `SELECT KURTOSIS(val) FROM t`
- THEN the executor SHALL return the excess kurtosis (Fisher's definition)
- AND the formula matches DuckDB's implementation

#### Scenario: VAR_POP calculation
- WHEN executing `SELECT VAR_POP(val) FROM (VALUES (1), (2), (3), (4), (5)) AS t(val)`
- THEN the executor SHALL return 2.0
- AND population variance formula is used

#### Scenario: VAR_SAMP calculation
- WHEN executing `SELECT VAR_SAMP(val) FROM (VALUES (1), (2), (3), (4), (5)) AS t(val)`
- THEN the executor SHALL return 2.5
- AND sample variance formula is used

#### Scenario: STDDEV_POP calculation
- WHEN executing `SELECT STDDEV_POP(val) FROM t`
- THEN the executor SHALL return the square root of population variance
- AND matches SQL standard definition

#### Scenario: STDDEV_SAMP calculation
- WHEN executing `SELECT STDDEV_SAMP(val) FROM t`
- THEN the executor SHALL return the square root of sample variance
- AND matches SQL standard definition

### Requirement: Approximate Aggregate Functions

The executor SHALL implement approximate aggregate functions using HyperLogLog and t-digest algorithms.

#### Scenario: APPROX_COUNT_DISTINCT accuracy
- WHEN executing `SELECT APPROX_COUNT_DISTINCT(val) FROM generate_series(1, 1000000) AS t(val)`
- THEN the result SHALL be within 5% of the exact COUNT(DISTINCT)
- AND the error rate improves with larger datasets

#### Scenario: APPROX_COUNT_DISTINCT with many duplicates
- WHEN executing `SELECT APPROX_COUNT_DISTINCT(val) FROM (SELECT 1 AS val UNION ALL SELECT 1 AS val UNION ALL SELECT 2 AS val)`
- THEN the executor SHALL return approximately 2
- AND small number correction is applied

#### Scenario: APPROX_QUANTILE calculation
- WHEN executing `SELECT APPROX_QUANTILE(val, 0.5) FROM generate_series(1, 10000) AS t(val)`
- THEN the result SHALL be within 1% of exact QUANTILE(val, 0.5)
- AND t-digest compression parameter controls accuracy

#### Scenario: APPROX_MEDIAN calculation
- WHEN executing `SELECT APPROX_MEDIAN(val) FROM t`
- THEN the executor SHALL return an approximation of the median
- AND uses t-digest with default compression

#### Scenario: APPROX functions with NULL values
- WHEN executing `SELECT APPROX_COUNT_DISTINCT(val) FROM (VALUES (1), (NULL), (2), (NULL)) AS t(val)`
- THEN NULL values SHALL be ignored
- AND only non-NULL values are counted

### Requirement: Boolean and Bitwise Aggregate Functions

The executor SHALL implement boolean and bitwise aggregate functions.

#### Scenario: BOOL_AND returns TRUE only if all values are TRUE
- WHEN executing `SELECT BOOL_AND(val) FROM (VALUES (TRUE), (TRUE), (FALSE)) AS t(val)`
- THEN the executor SHALL return FALSE
- AND if any value is FALSE, result is FALSE

#### Scenario: BOOL_AND with all NULL values
- WHEN executing `SELECT BOOL_AND(val) FROM (VALUES (NULL), (NULL)) AS t(val)`
- THEN the executor SHALL return NULL
- AND NULL values are not considered

#### Scenario: BOOL_OR returns TRUE if any value is TRUE
- WHEN executing `SELECT BOOL_OR(val) FROM (VALUES (FALSE), (FALSE), (TRUE)) AS t(val)`
- THEN the executor SHALL return TRUE
- AND if any value is TRUE, result is TRUE

#### Scenario: BOOL_OR with all FALSE values
- WHEN executing `SELECT BOOL_OR(val) FROM (VALUES (FALSE), (FALSE)) AS t(val)`
- THEN the executor SHALL return FALSE
- AND NULL values are not considered

#### Scenario: BIT_AND across integer values
- WHEN executing `SELECT BIT_AND(val) FROM (VALUES (CAST(5 AS TINYINT)), (CAST(3 AS TINYINT)), (CAST(7 AS TINYINT))) AS t(val)`
- THEN the executor SHALL return 1 (binary 001)
- AND bitwise AND is performed on each bit position

#### Scenario: BIT_OR across integer values
- WHEN executing `SELECT BIT_OR(val) FROM (VALUES (CAST(5 AS TINYINT)), (CAST(3 AS TINYINT)), (CAST(1 AS TINYINT))) AS t(val)`
- THEN the executor SHALL return 7 (binary 111)
- AND bitwise OR is performed on each bit position

#### Scenario: BIT_XOR across integer values
- WHEN executing `SELECT BIT_XOR(val) FROM (VALUES (CAST(5 AS TINYINT)), (CAST(3 AS TINYINT))) AS t(val)`
- THEN the executor SHALL return 6 (binary 110)
- AND bitwise XOR is performed on each bit position

### Requirement: String and List Aggregate Functions

The executor SHALL implement string and list aggregation functions.

#### Scenario: STRING_AGG with delimiter
- WHEN executing `SELECT STRING_AGG(val, ',') FROM (VALUES ('a'), ('b'), ('c')) AS t(val)`
- THEN the executor SHALL return 'a,b,c'
- AND values are concatenated with the delimiter

#### Scenario: STRING_AGG with ORDER BY
- WHEN executing `SELECT STRING_AGG(val, ',' ORDER BY val DESC) FROM (VALUES ('a'), ('b'), ('c')) AS t(val)`
- THEN the executor SHALL return 'c,b,a'
- AND values are ordered before concatenation

#### Scenario: GROUP_CONCAT MySQL compatibility
- WHEN executing `SELECT GROUP_CONCAT(val) FROM (VALUES ('a'), ('b')) AS t(val)`
- THEN the executor SHALL return 'a,b'
- AND default comma delimiter is used

#### Scenario: LIST aggregation
- WHEN executing `SELECT LIST(val) FROM (VALUES (1), (2), (3)) AS t(val)`
- THEN the executor SHALL return [1, 2, 3]
- AND values are aggregated into a list

#### Scenario: LIST with ORDER BY
- WHEN executing `SELECT LIST(val ORDER BY val DESC) FROM (VALUES (1), (2), (3)) AS t(val)`
- THEN the executor SHALL return [3, 2, 1]
- AND values are ordered before aggregation

#### Scenario: LIST_DISTINCT aggregation
- WHEN executing `SELECT LIST_DISTINCT(val) FROM (VALUES (1), (1), (2), (2), (3)) AS t(val)`
- THEN the executor SHALL return [1, 2, 3]
- AND duplicate values are removed

### Requirement: Time Series Aggregate Functions

The executor SHALL implement time series aggregate functions for conditional and positional aggregation.

#### Scenario: COUNT_IF with condition
- WHEN executing `SELECT COUNT_IF(val > 5) FROM (VALUES (1), (6), (3), (7), (4)) AS t(val)`
- THEN the executor SHALL return 2
- AND only rows where val > 5 are counted

#### Scenario: COUNT_IF with no matching rows
- WHEN executing `SELECT COUNT_IF(val > 100) FROM (VALUES (1), (2), (3)) AS t(val)`
- THEN the executor SHALL return 0
- AND NULL is not returned

#### Scenario: FIRST aggregation
- WHEN executing `SELECT FIRST(val) FROM t`
- THEN the executor SHALL return the first non-NULL value in the group
- AND order is determined by input order

#### Scenario: LAST aggregation
- WHEN executing `SELECT LAST(val) FROM t`
- THEN the executor SHALL return the last non-NULL value in the group
- AND order is determined by input order

#### Scenario: ARGMIN returns argument with minimum value
- WHEN executing `SELECT ARGMIN(name, score) FROM (VALUES ('a', 10), ('b', 5), ('c', 8)) AS t(name, score)`
- THEN the executor SHALL return 'b'
- AND 'b' has the minimum score of 5

#### Scenario: ARGMAX returns argument with maximum value
- WHEN executing `SELECT ARGMAX(name, score) FROM t`
- THEN the executor SHALL return the name with the maximum score
- AND if multiple rows have same max, returns first encountered

#### Scenario: MIN_BY aggregation
- WHEN executing `SELECT MIN_BY(name, score) FROM t`
- THEN the executor SHALL return the name associated with the minimum score
- AND score determines which name is chosen

#### Scenario: MAX_BY aggregation
- WHEN executing `SELECT MAX_BY(name, score) FROM t`
- THEN the executor SHALL return the name associated with the maximum score
- AND score determines which name is chosen

### Requirement: Regression and Correlation Functions

The executor SHALL implement regression and correlation functions for statistical analysis.

#### Scenario: COVAR_POP population covariance
- WHEN executing `SELECT COVAR_POP(x, y) FROM (VALUES (1, 2), (2, 4), (3, 5)) AS t(x, y)`
- THEN the executor SHALL return the population covariance
- AND formula is E[(X - E[X])(Y - E[Y])]

#### Scenario: COVAR_SAMP sample covariance
- WHEN executing `SELECT COVAR_SAMP(x, y) FROM t`
- THEN the executor SHALL return sample covariance
- AND denominator is N-1

#### Scenario: CORR correlation coefficient
- WHEN executing `SELECT CORR(x, y) FROM t`
- THEN the executor SHALL return Pearson correlation coefficient
- AND value is between -1 and 1

#### Scenario: Perfect positive correlation
- WHEN executing `SELECT CORR(x, y) FROM (VALUES (1, 2), (2, 4), (3, 6)) AS t(x, y)`
- THEN the executor SHALL return 1.0
- AND y = 2*x produces perfect positive correlation

#### Scenario: Perfect negative correlation
- WHEN executing `SELECT CORR(x, y) FROM (VALUES (1, 6), (2, 4), (3, 2)) AS t(x, y)`
- THEN the executor SHALL return -1.0
- AND y = -  82*x produces perfect negative correlation

#### Scenario: REGR_INTERCEPT returns y-intercept
- WHEN executing `SELECT REGR_INTERCEPT(y, x) FROM t`
- THEN the executor SHALL return the intercept of linear regression
- AND y = intercept + slope * x

#### Scenario: REGR_SLOPE returns slope
- WHEN executing `SELECT REGR_SLOPE(y, x) FROM t`
- THEN the executor SHALL return the slope of linear regression
- AND uses least squares estimation

#### Scenario: REGR_R2 returns coefficient of determination
- WHEN executing `SELECT REGR_R2(y, x) FROM t`
- THEN the executor SHALL return R-squared value
- AND value is between 0 and 1

### Requirement: Aggregate Function Edge Cases

The executor SHALL handle edge cases correctly for all aggregate functions.

#### Scenario: All NULL input for aggregates
- WHEN executing `SELECT COUNT(*), AVG(x), MEDIAN(x), MODE(x) FROM (VALUES (NULL), (NULL)) AS t(x)`
- THEN COUNT SHALL return 2
- AND AVG, MEDIAN, MODE SHALL return NULL

#### Scenario: Empty group with aggregates
- WHEN executing `SELECT COUNT(x), AVG(x), MEDIAN(x) FROM t GROUP BY y HAVING COUNT(*) = 0`
- THEN aggregates SHALL return NULL for groups with no rows
- AND COUNT returns 0

#### Scenario: Single-row group
- WHEN executing `SELECT MEDIAN(x), AVG(x), STDDEV(x) FROM (VALUES (5)) AS t(x)`
- THEN MEDIAN and AVG return 5
- AND STDDEV returns NULL (undefined for single value)

#### Scenario: DISTINCT modifier with aggregates
- WHEN executing `SELECT COUNT(DISTINCT x), MEDIAN(DISTINCT x) FROM t`
- THEN duplicate values SHALL be removed before aggregation
- AND the behavior matches DuckDB

#### Scenario: FILTER clause with aggregates
- WHEN executing `SELECT COUNT(*) FILTER (WHERE x > 0) FROM t`
- THEN the filter SHALL be applied before aggregation
- AND only matching rows are counted

#### Scenario: Aggregate with ORDER BY
- WHEN executing `SELECT ARRAY_AGG(x ORDER BY x DESC) FROM t`
- THEN the ORDER BY SHALL determine the order of values in the result
- AND the aggregate output respects the ordering

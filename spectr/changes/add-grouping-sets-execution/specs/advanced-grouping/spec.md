## ADDED Requirements

### Requirement: GROUPING SETS Execution

The system SHALL execute queries with `GROUP BY GROUPING SETS ((...), (...), ...)` by computing aggregates for each specified grouping set and returning the UNION ALL of all results. Columns not present in a given grouping set SHALL be NULL in that set's output rows.

#### Scenario: Basic GROUPING SETS with two sets plus grand total

- GIVEN a table `sales` with columns `region`, `product`, `amount`
- WHEN the query `SELECT region, product, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region, product), (region), ())` is executed
- THEN the result contains one row per unique (region, product) combination with both columns populated
- AND one row per unique region with product as NULL
- AND one grand total row with both region and product as NULL

#### Scenario: GROUPING SETS with empty table

- GIVEN an empty table `sales` with columns `region`, `product`, `amount`
- WHEN the query `SELECT region, product, SUM(amount) AS total FROM sales GROUP BY GROUPING SETS ((region, product), (region), ())` is executed
- THEN the result contains one row for the empty grouping set with region=NULL, product=NULL, total=NULL

#### Scenario: GROUPING SETS with single-column sets

- GIVEN a table with columns `a`, `b`, `val`
- WHEN the query `SELECT a, b, COUNT(*) AS cnt FROM t GROUP BY GROUPING SETS ((a), (b))` is executed
- THEN the result contains one row per unique value of `a` (with `b` as NULL) and one row per unique value of `b` (with `a` as NULL)

### Requirement: ROLLUP Execution

The system SHALL execute `GROUP BY ROLLUP(a, b, ...)` by expanding it to the equivalent GROUPING SETS and executing accordingly. `ROLLUP(a, b, c)` SHALL produce grouping sets `(a, b, c), (a, b), (a), ()`.

#### Scenario: ROLLUP with two columns

- GIVEN a table `sales` with columns `region`, `product`, `amount`
- WHEN the query `SELECT region, product, SUM(amount) AS total FROM sales GROUP BY ROLLUP(region, product)` is executed
- THEN the result is equivalent to `GROUP BY GROUPING SETS ((region, product), (region), ())`
- AND the result includes detail rows, subtotals per region, and a grand total

#### Scenario: ROLLUP with three columns

- GIVEN a table with columns `year`, `quarter`, `month`, `sales`
- WHEN the query `SELECT year, quarter, month, SUM(sales) AS total FROM t GROUP BY ROLLUP(year, quarter, month)` is executed
- THEN the result contains grouping sets `(year, quarter, month), (year, quarter), (year), ()`

#### Scenario: ROLLUP with single column

- GIVEN a table with column `region` and `amount`
- WHEN the query `SELECT region, SUM(amount) FROM t GROUP BY ROLLUP(region)` is executed
- THEN the result is equivalent to `GROUP BY GROUPING SETS ((region), ())`

### Requirement: CUBE Execution

The system SHALL execute `GROUP BY CUBE(a, b, ...)` by expanding it to all 2^n possible grouping sets and executing accordingly. `CUBE(a, b)` SHALL produce grouping sets `(a, b), (a), (b), ()`.

#### Scenario: CUBE with two columns

- GIVEN a table `sales` with columns `region`, `product`, `amount`
- WHEN the query `SELECT region, product, SUM(amount) AS total FROM sales GROUP BY CUBE(region, product)` is executed
- THEN the result is equivalent to `GROUP BY GROUPING SETS ((region, product), (region), (product), ())`
- AND the result includes all four levels of aggregation

#### Scenario: CUBE with single column

- GIVEN a table with column `region` and `amount`
- WHEN the query `SELECT region, SUM(amount) FROM t GROUP BY CUBE(region)` is executed
- THEN the result is equivalent to `GROUP BY GROUPING SETS ((region), ())`

### Requirement: GROUPING Function

The system SHALL support the `GROUPING(col1, col2, ...)` function in SELECT clauses of queries with GROUPING SETS, ROLLUP, or CUBE. The function SHALL return an integer bitmask where bit N (MSB-first) is 1 if argument N is aggregated (NULL due to grouping) and 0 if argument N is grouped (present in the current grouping set).

#### Scenario: GROUPING with single argument

- GIVEN a table `sales` with columns `region`, `amount`
- WHEN the query `SELECT GROUPING(region) AS g, region, SUM(amount) FROM sales GROUP BY ROLLUP(region)` is executed
- THEN rows with a non-NULL region have g=0
- AND the grand total row (region IS NULL) has g=1

#### Scenario: GROUPING with two arguments

- GIVEN a table `sales` with columns `region`, `product`, `amount`
- WHEN the query `SELECT GROUPING(region, product) AS g, region, product, SUM(amount) FROM sales GROUP BY CUBE(region, product)` is executed
- THEN rows in grouping set (region, product) have g=0
- AND rows in grouping set (region) have g=1
- AND rows in grouping set (product) have g=2
- AND the grand total row has g=3

#### Scenario: Multiple GROUPING calls in same query

- GIVEN a table with columns `a`, `b`, `val`
- WHEN the query `SELECT GROUPING(a) AS ga, GROUPING(b) AS gb, a, b, SUM(val) FROM t GROUP BY CUBE(a, b)` is executed
- THEN ga and gb independently indicate whether their respective columns are grouped or aggregated

### Requirement: NULL Handling in Grouping Sets

The system SHALL correctly distinguish between NULL values that are present in the source data and NULL values introduced by the grouping set mechanism. The GROUPING() function SHALL be the mechanism to distinguish these cases.

#### Scenario: Source data contains NULL in grouping column

- GIVEN a table where column `region` has some NULL values
- WHEN the query `SELECT GROUPING(region) AS g, region, COUNT(*) FROM t GROUP BY ROLLUP(region)` is executed
- THEN rows where region is NULL from the source data have g=0
- AND the grand total row where region is NULL from the rollup has g=1

### Requirement: Multiple Aggregates with Grouping Sets

The system SHALL support multiple aggregate functions in the same query with GROUPING SETS, ROLLUP, or CUBE. Each aggregate SHALL be computed independently for each grouping set.

#### Scenario: SUM and COUNT with ROLLUP

- GIVEN a table `sales` with columns `region`, `amount`
- WHEN the query `SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY ROLLUP(region)` is executed
- THEN each row contains both the correct SUM and COUNT for its grouping set
- AND the grand total row contains the overall SUM and COUNT

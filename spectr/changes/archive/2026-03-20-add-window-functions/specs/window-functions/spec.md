# Window Functions Specification

## ADDED Requirements

### Requirement: Ranking Functions - ROW_NUMBER

The executor SHALL implement the ROW_NUMBER() window function, which assigns a unique sequential row number to each row within a partition (1-based indexing). Rows receive consecutive numbers regardless of ties in ORDER BY values.

#### Scenario: Basic row numbering
- GIVEN table with rows (id: 1,2,3), id DESC ORDER
- WHEN query executes `SELECT id, ROW_NUMBER() OVER (ORDER BY id DESC) AS rn`
- THEN results are: [(3, 1), (2, 2), (1, 3)]

#### Scenario: Row numbering with partition
- GIVEN table with rows (category: A,A,B,B) and (value: 10,20,30,40)
- WHEN query executes `SELECT category, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value)`
- THEN results are: [(A,10,1), (A,20,2), (B,30,1), (B,40,2)]

#### Scenario: Row numbering without ORDER BY
- GIVEN table with 5 rows in arbitrary order
- WHEN query executes `SELECT ROW_NUMBER() OVER ()`
- THEN results contain row numbers 1 through 5 in some deterministic order

#### Scenario: Row numbering respects partition boundaries
- GIVEN 2 partitions with 3 rows each
- WHEN query executes `SELECT ROW_NUMBER() OVER (PARTITION BY partition_id ORDER BY value)`
- THEN each partition gets row numbers 1,2,3 independently

### Requirement: Ranking Functions - RANK

The executor SHALL implement the RANK() window function, which assigns rank numbers based on ORDER BY values. Rows with equal ORDER BY values receive the same rank, and the next rank skips accordingly (rank with gaps).

#### Scenario: RANK with duplicate values
- GIVEN table with values [1,2,2,3,3,3,4]
- WHEN query executes `SELECT value, RANK() OVER (ORDER BY value)`
- THEN ranks are [1,2,2,4,4,4,7]

#### Scenario: RANK with partition
- GIVEN sales table with (salesperson: A,A,B,B) and (amount: 100,200,150,150)
- WHEN query executes `SELECT salesperson, RANK() OVER (PARTITION BY salesperson ORDER BY amount)`
- THEN results are: [(A,100,1), (A,200,2), (B,150,1), (B,150,1)]

#### Scenario: RANK without ORDER BY
- GIVEN table with 3 identical rows
- WHEN query executes `SELECT RANK() OVER ()`
- THEN all rows receive rank 1

#### Scenario: RANK with DESC ordering
- GIVEN values [1,2,2,3]
- WHEN query executes `SELECT value, RANK() OVER (ORDER BY value DESC)`
- THEN ranks are [4,2,2,1]

### Requirement: Ranking Functions - DENSE_RANK

The executor SHALL implement the DENSE_RANK() window function, which assigns consecutive rank numbers based on ORDER BY values. Unlike RANK, gaps do not appear; each distinct ORDER BY value gets a consecutive rank.

#### Scenario: DENSE_RANK with duplicate values
- GIVEN table with values [1,2,2,3,3,3,4]
- WHEN query executes `SELECT value, DENSE_RANK() OVER (ORDER BY value)`
- THEN dense ranks are [1,2,2,3,3,3,4]

#### Scenario: DENSE_RANK vs RANK comparison
- GIVEN same dataset with values [1,2,2,3]
- WHEN both RANK() and DENSE_RANK() are computed
- THEN RANK produces [1,2,2,4] and DENSE_RANK produces [1,2,2,3]

#### Scenario: DENSE_RANK with multiple ORDER BY columns
- GIVEN table with (col_a: 1,1,2,2), (col_b: A,B,A,B)
- WHEN query executes `SELECT DENSE_RANK() OVER (ORDER BY col_a, col_b)`
- THEN dense ranks are [1,2,3,4] (all unique combinations get consecutive ranks)

#### Scenario: DENSE_RANK with partition
- GIVEN sales table grouped by salesperson with duplicate amounts
- WHEN query executes `SELECT salesperson, amount, DENSE_RANK() OVER (PARTITION BY salesperson ORDER BY amount DESC)`
- THEN each partition gets consecutive ranks starting from 1

### Requirement: Ranking Functions - NTILE

The executor SHALL implement the NTILE(n) window function, which distributes rows into n buckets/quartiles. Each bucket contains approximately the same number of rows; if rows don't divide evenly, earlier buckets receive one extra row.

#### Scenario: NTILE basic distribution
- GIVEN 10 rows, NTILE(4)
- WHEN query executes `SELECT ROW_NUMBER() OVER (ORDER BY id), NTILE(4) OVER (ORDER BY id)`
- THEN buckets are: [1:1-1, 1:2-3, 1:3-5, 1:4-7, 2:1, 2:2-3, 2:4-5, 3:1-2, 3:3-4, 4:1] (some approximation of approximately equal-sized buckets)

#### Scenario: NTILE exact calculation
- GIVEN 10 rows, NTILE(4) - base size = 2, extra = 2
- WHEN executed
- THEN first 2 buckets get 3 rows each (2+1), last 2 buckets get 2 rows each
- RESULTS: rows 1-3 → bucket 1, rows 4-6 → bucket 2, rows 7-8 → bucket 3, rows 9-10 → bucket 4

#### Scenario: NTILE with partition
- GIVEN 2 partitions with 5 rows each, NTILE(2)
- WHEN query executes `SELECT PARTITION BY dept, NTILE(2) OVER (PARTITION BY dept ORDER BY salary)`
- THEN each partition gets independent bucket assignments 1 and 2

#### Scenario: NTILE with n=1
- GIVEN any number of rows, NTILE(1)
- WHEN executed
- THEN all rows are assigned to bucket 1

#### Scenario: NTILE argument is expression
- GIVEN table where @buckets is a bound parameter or expression
- WHEN query executes `SELECT NTILE(@buckets) OVER (ORDER BY value)`
- THEN expression is evaluated and result is correct distribution

### Requirement: Analytic Functions - LAG

The executor SHALL implement the LAG(expr, offset, default) window function, which returns the value of expr from a row that is offset rows before the current row within the partition. If no such row exists, default value is returned.

#### Scenario: LAG basic usage
- GIVEN values [10, 20, 30, 40] in order
- WHEN query executes `SELECT value, LAG(value) OVER (ORDER BY value)`
- THEN results are: [(10, NULL), (20, 10), (30, 20), (40, 30)]

#### Scenario: LAG with offset
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, LAG(value, 2) OVER (ORDER BY value)`
- THEN results are: [(10, NULL), (20, NULL), (30, 10), (40, 20)]

#### Scenario: LAG with default value
- GIVEN values [10, 20, 30]
- WHEN query executes `SELECT value, LAG(value, 1, -1) OVER (ORDER BY value)`
- THEN results are: [(10, -1), (20, 10), (30, 20)]

#### Scenario: LAG with partition
- GIVEN sales table (salesperson: A,A,B,B) with amounts (100, 200, 150, 250)
- WHEN query executes `SELECT salesperson, amount, LAG(amount) OVER (PARTITION BY salesperson ORDER BY date)`
- THEN A's results: [(100, NULL), (200, 100)], B's results: [(150, NULL), (250, 150)]

#### Scenario: LAG with IGNORE NULLS
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, LAG(value IGNORE NULLS) OVER (ORDER BY id)`
- THEN results are: [(10, NULL), (NULL, 10), (20, 10), (NULL, 20), (30, 20)]
- (The NULL values in results column are from input; comparison values skip NULLs)

#### Scenario: LAG with expression argument
- GIVEN table with (price: 100, qty: 2) rows
- WHEN query executes `SELECT LAG(price * qty, 1, 0) OVER (ORDER BY date)`
- THEN expression is evaluated on previous row's data

### Requirement: Analytic Functions - LEAD

The executor SHALL implement the LEAD(expr, offset, default) window function, which returns the value of expr from a row that is offset rows after the current row within the partition. If no such row exists, default value is returned. Semantically identical to LAG but looks forward instead of backward.

#### Scenario: LEAD basic usage
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, LEAD(value) OVER (ORDER BY value)`
- THEN results are: [(10, 20), (20, 30), (30, 40), (40, NULL)]

#### Scenario: LEAD with offset and default
- GIVEN values [10, 20, 30, 40, 50]
- WHEN query executes `SELECT value, LEAD(value, 2, 999) OVER (ORDER BY value)`
- THEN results are: [(10, 30), (20, 40), (30, 50), (40, 999), (50, 999)]

#### Scenario: LEAD with partition
- GIVEN time series data for multiple products
- WHEN query executes `SELECT product, date, price, LEAD(price) OVER (PARTITION BY product ORDER BY date)`
- THEN each product gets its own next-price values; last row in partition returns NULL or default

#### Scenario: LEAD with IGNORE NULLS
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, LEAD(value IGNORE NULLS) OVER (ORDER BY id)`
- THEN results skip NULL values when looking forward
- RESULTS: [(10, 20), (NULL, 20), (20, 30), (NULL, 30), (30, NULL)]

#### Scenario: LEAD detects partition boundaries
- GIVEN 2 partitions with 2 rows each
- WHEN query executes `SELECT LEAD(value) OVER (PARTITION BY part ORDER BY id)`
- THEN last row of each partition returns NULL (or default), doesn't cross partition boundary

### Requirement: Analytic Functions - FIRST_VALUE

The executor SHALL implement the FIRST_VALUE(expr) window function, which returns the value of expr from the first row in the window frame. Window frame defaults to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW when ORDER BY is specified.

#### Scenario: FIRST_VALUE basic usage
- GIVEN values [10, 20, 30, 40] in order
- WHEN query executes `SELECT value, FIRST_VALUE(value) OVER (ORDER BY value)`
- THEN with default frame (unbounded preceding to current), results are: [(10, 10), (20, 10), (30, 10), (40, 10)]

#### Scenario: FIRST_VALUE with explicit frame
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, FIRST_VALUE(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN each row's frame is 1 before and 1 after, so: [(10, 10), (20, 10), (30, 20), (40, 30)]

#### Scenario: FIRST_VALUE with partition
- GIVEN sales data partitioned by region
- WHEN query executes `SELECT region, date, revenue, FIRST_VALUE(revenue) OVER (PARTITION BY region ORDER BY date)`
- THEN results show first region's revenue for each region

#### Scenario: FIRST_VALUE with IGNORE NULLS
- GIVEN values [NULL, 10, NULL, 20, 30] with DEFAULT frame
- WHEN query executes `SELECT value, FIRST_VALUE(value IGNORE NULLS) OVER (ORDER BY id)`
- THEN skips NULL values to find first non-NULL: [(NULL, 10), (10, 10), (NULL, 10), (20, 10), (30, 10)]

#### Scenario: FIRST_VALUE with empty frame
- GIVEN values where ROWS 2 FOLLOWING creates empty frame at end
- WHEN query executes `SELECT value, FIRST_VALUE(value) OVER (ORDER BY id ROWS 2 FOLLOWING)`
- THEN last 2 rows return NULL (empty frame)

### Requirement: Analytic Functions - LAST_VALUE

The executor SHALL implement the LAST_VALUE(expr) window function, which returns the value of expr from the last row in the window frame. Important: when using ORDER BY without explicit frame, the frame defaults to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (not UNBOUNDED FOLLOWING).

#### Scenario: LAST_VALUE with default frame and ORDER BY
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, LAST_VALUE(value) OVER (ORDER BY value)`
- THEN with default frame (unbounded preceding to CURRENT), all rows' last value is themselves: [(10, 10), (20, 20), (30, 30), (40, 40)]

#### Scenario: LAST_VALUE with explicit UNBOUNDED FOLLOWING
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, LAST_VALUE(value) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- THEN frame includes entire partition, results are: [(10, 40), (20, 40), (30, 40), (40, 40)]

#### Scenario: LAST_VALUE with sliding window
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, LAST_VALUE(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN results are: [(10, 20), (20, 30), (30, 40), (40, 40)]

#### Scenario: LAST_VALUE with IGNORE NULLS
- GIVEN values [10, NULL, 20, NULL, 30] with sliding window
- WHEN query executes `SELECT value, LAST_VALUE(value IGNORE NULLS) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)`
- THEN skips NULL values to find last non-NULL in frame

#### Scenario: LAST_VALUE respects EXCLUDE clause
- GIVEN values [10, 20, 30] with EXCLUDE CURRENT ROW
- WHEN query executes `SELECT value, LAST_VALUE(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)`
- THEN current row is excluded from frame bounds calculation

### Requirement: Analytic Functions - NTH_VALUE

The executor SHALL implement the NTH_VALUE(expr, n) window function, which returns the value of expr from the nth row in the window frame (1-based indexing). If the frame contains fewer than n rows or n is NULL/invalid, NULL is returned.

#### Scenario: NTH_VALUE basic usage
- GIVEN values [10, 20, 30, 40] with default frame
- WHEN query executes `SELECT value, NTH_VALUE(value, 2) OVER (ORDER BY value)`
- THEN with frame up to current row: [(10, NULL), (20, 20), (30, 20), (40, 20)] (2nd value in frame [10], [10,20], [10,20,30], [10,20,30,40])

#### Scenario: NTH_VALUE with full partition frame
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, NTH_VALUE(value, 3) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- THEN all rows return 30 (the 3rd value in full frame)

#### Scenario: NTH_VALUE with n larger than frame
- GIVEN values [10, 20] with frame size 2
- WHEN query executes `SELECT value, NTH_VALUE(value, 5) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- THEN both rows return NULL (frame only has 2 rows)

#### Scenario: NTH_VALUE with n=1
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, NTH_VALUE(value, 1) OVER (ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- THEN all rows return 10 (first value in partition)

#### Scenario: NTH_VALUE with IGNORE NULLS
- GIVEN values [NULL, 10, NULL, 20, 30]
- WHEN query executes `SELECT value, NTH_VALUE(value IGNORE NULLS, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- THEN skips NULL values and returns 2nd non-NULL value (20)

#### Scenario: NTH_VALUE with expression for n
- GIVEN table where @n is a parameter
- WHEN query executes `SELECT NTH_VALUE(value, @n) OVER (ORDER BY id)`
- THEN n is evaluated as expression and correct nth value returned

### Requirement: Distribution Functions - PERCENT_RANK

The executor SHALL implement the PERCENT_RANK() window function, which returns the relative rank of the current row as a value between 0.0 and 1.0. Formula: (rank - 1) / (total_rows - 1). Returns 0.0 for single-row partitions.

#### Scenario: PERCENT_RANK basic usage
- GIVEN 5 rows with values [1, 2, 2, 3, 4]
- WHEN query executes `SELECT value, PERCENT_RANK() OVER (ORDER BY value)`
- THEN ranks are [1,2,2,4,5], percent_ranks are [0.0, 0.25, 0.25, 0.75, 1.0]

#### Scenario: PERCENT_RANK with ties
- GIVEN values [10, 20, 20, 30] (ranks: 1, 2, 2, 4)
- WHEN query executes `SELECT value, PERCENT_RANK() OVER (ORDER BY value)`
- THEN percent_ranks are [0.0, 0.333, 0.333, 1.0]

#### Scenario: PERCENT_RANK single row partition
- GIVEN partition with 1 row
- WHEN query executes `SELECT PERCENT_RANK() OVER (PARTITION BY id ORDER BY value)`
- THEN result is 0.0

#### Scenario: PERCENT_RANK with partition
- GIVEN 2 partitions with different sizes
- WHEN query executes `SELECT PERCENT_RANK() OVER (PARTITION BY partition_id ORDER BY value)`
- THEN each partition calculates independently; result is (rank-1)/(partition_size-1)

#### Scenario: PERCENT_RANK distributes evenly
- GIVEN 100 rows with unique values
- WHEN query executes `SELECT value, PERCENT_RANK() OVER (ORDER BY value)`
- THEN percent_ranks are approximately [0.0, 0.01, 0.02, ..., 0.99, 1.0]

### Requirement: Distribution Functions - CUME_DIST

The executor SHALL implement the CUME_DIST() window function, which returns the cumulative distribution of the current row as a value between 0.0 and 1.0. Formula: (rows at or before current in same peer group) / total_rows. With peer groups, multiple rows with the same ORDER BY value return the same cumulative distribution.

#### Scenario: CUME_DIST basic usage
- GIVEN values [10, 20, 20, 30, 30, 30] (3 peer groups)
- WHEN query executes `SELECT value, CUME_DIST() OVER (ORDER BY value)`
- THEN cume_dists are [0.1667, 0.3333, 0.3333, 1.0, 1.0, 1.0]

#### Scenario: CUME_DIST with single row
- GIVEN 1 row
- WHEN query executes `SELECT CUME_DIST() OVER (ORDER BY value)`
- THEN result is 1.0

#### Scenario: CUME_DIST with all equal rows
- GIVEN 5 identical values
- WHEN query executes `SELECT value, CUME_DIST() OVER (ORDER BY value)`
- THEN all rows return 1.0 (all in one peer group, all at or before end)

#### Scenario: CUME_DIST with partition
- GIVEN sales data grouped by salesperson
- WHEN query executes `SELECT salesperson, amount, CUME_DIST() OVER (PARTITION BY salesperson ORDER BY amount)`
- THEN each salesperson's CUME_DIST is calculated within their partition

#### Scenario: CUME_DIST shows cumulative progression
- GIVEN 10 rows with unique values
- WHEN query executes `SELECT value, CUME_DIST() OVER (ORDER BY value)`
- THEN cume_dists are [0.1, 0.2, 0.3, ..., 1.0] (monotonically increasing)

### Requirement: Aggregate Window Functions - SUM

The executor SHALL implement SUM(expr) OVER (window_spec) window function, which computes the sum of expr over the window frame. SUM returns NULL if the frame is empty or contains only NULLs. SUM supports FILTER clause for conditional aggregation and DISTINCT for unique value summation.

#### Scenario: SUM basic window
- GIVEN values [10, 20, 30, 40] with default frame (unbounded to current)
- WHEN query executes `SELECT value, SUM(value) OVER (ORDER BY value)`
- THEN sums are [10, 30, 60, 100]

#### Scenario: SUM with sliding window
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, SUM(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN sums are [30, 60, 90, 70] (current ± 1 row)

#### Scenario: SUM with partition
- GIVEN sales table (dept: A,A,B,B) with (amount: 100,200,150,250)
- WHEN query executes `SELECT dept, amount, SUM(amount) OVER (PARTITION BY dept ORDER BY amount)`
- THEN A: [100, 300], B: [150, 400]

#### Scenario: SUM with NULL values
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, SUM(value) OVER (ORDER BY id)`
- THEN sums treat NULL as 0: [10, 10, 30, 30, 60]

#### Scenario: SUM with empty frame
- GIVEN values [10, 20] with frame ROWS 5 FOLLOWING (empty)
- WHEN query executes `SELECT SUM(value) OVER (ORDER BY id ROWS 5 FOLLOWING)`
- THEN results are NULL for rows with empty frames

#### Scenario: SUM with FILTER clause
- GIVEN table (value: 10,20,30,40) and (flag: true,false,true,false)
- WHEN query executes `SELECT value, SUM(value) FILTER (WHERE flag) OVER (ORDER BY id)`
- THEN sums only include rows where flag=true: [10, 10, 40, 40]

#### Scenario: SUM with DISTINCT
- GIVEN values [10, 10, 20, 20, 30]
- WHEN query executes `SELECT value, SUM(DISTINCT value) OVER (ORDER BY id)`
- THEN results treat duplicates as single value: [10, 10, 30, 30, 60]

#### Scenario: SUM with expression argument
- GIVEN table (price: 100, qty: 2) rows
- WHEN query executes `SELECT SUM(price * qty) OVER (ORDER BY id)`
- THEN expression is evaluated per row before summing

### Requirement: Aggregate Window Functions - COUNT

The executor SHALL implement COUNT(expr) OVER (window_spec) and COUNT(*) OVER (window_spec) window functions. COUNT(expr) excludes NULLs; COUNT(*) counts all rows including NULLs. COUNT supports FILTER clause and DISTINCT keyword.

#### Scenario: COUNT basic window
- GIVEN values [10, 20, 30, 40] with default frame
- WHEN query executes `SELECT value, COUNT(value) OVER (ORDER BY value)`
- THEN counts are [1, 2, 3, 4]

#### Scenario: COUNT with NULL values
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, COUNT(value) OVER (ORDER BY id)`
- THEN counts are [1, 1, 2, 2, 3] (NULLs not counted)

#### Scenario: COUNT(*) includes NULL
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, COUNT(*) OVER (ORDER BY id)`
- THEN counts are [1, 2, 3, 4, 5] (all rows counted)

#### Scenario: COUNT with sliding window
- GIVEN values [10, 20, 30, 40, 50]
- WHEN query executes `SELECT value, COUNT(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN counts are [2, 3, 3, 3, 2]

#### Scenario: COUNT with partition
- GIVEN data with (dept: A,A,B,B) rows
- WHEN query executes `SELECT dept, COUNT(*) OVER (PARTITION BY dept ORDER BY id)`
- THEN each partition counts independently: A=[1,2], B=[1,2]

#### Scenario: COUNT with FILTER
- GIVEN table (value: 10,20,30,40) and (flag: true,false,true,false)
- WHEN query executes `SELECT COUNT(*) FILTER (WHERE flag) OVER (ORDER BY id)`
- THEN counts only flagged rows: [1, 1, 2, 2]

#### Scenario: COUNT with DISTINCT
- GIVEN values [10, 10, 20, 20, 30]
- WHEN query executes `SELECT COUNT(DISTINCT value) OVER (ORDER BY id)`
- THEN counts are [1, 1, 2, 2, 3] (duplicates counted once)

#### Scenario: COUNT(*) with empty frame
- GIVEN values with frame ROWS 2 FOLLOWING (empty at end)
- WHEN query executes `SELECT COUNT(*) OVER (ORDER BY id ROWS 2 FOLLOWING)`
- THEN last 2 rows return 0 (empty frame has 0 rows)

### Requirement: Aggregate Window Functions - AVG

The executor SHALL implement AVG(expr) OVER (window_spec) window function, which computes the average of expr over the window frame. AVG returns NULL if the frame is empty or contains only NULLs. AVG supports FILTER clause and DISTINCT for unique value averaging.

#### Scenario: AVG basic window
- GIVEN values [10, 20, 30, 40] with default frame (unbounded to current)
- WHEN query executes `SELECT value, AVG(value) OVER (ORDER BY value)`
- THEN avgs are [10.0, 15.0, 20.0, 25.0]

#### Scenario: AVG with sliding window
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, AVG(value) OVER (ORDER BY value ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN avgs are [15.0, 20.0, 30.0, 35.0]

#### Scenario: AVG with NULL values
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, AVG(value) OVER (ORDER BY id)`
- THEN avgs are [10.0, 10.0, 15.0, 15.0, 20.0] (NULLs excluded from average)

#### Scenario: AVG with partition
- GIVEN sales table (dept: A,A,B,B) with (amount: 100,200,100,400)
- WHEN query executes `SELECT dept, amount, AVG(amount) OVER (PARTITION BY dept ORDER BY amount)`
- THEN A: [100.0, 150.0], B: [100.0, 250.0]

#### Scenario: AVG with FILTER clause
- GIVEN table (value: 10,20,30,40) and (flag: true,false,true,false)
- WHEN query executes `SELECT AVG(value) FILTER (WHERE flag) OVER (ORDER BY id)`
- THEN avgs only include flagged values: [10.0, 10.0, 20.0, 20.0]

#### Scenario: AVG with DISTINCT
- GIVEN values [10, 10, 20, 20, 30]
- WHEN query executes `SELECT AVG(DISTINCT value) OVER (ORDER BY id)`
- THEN results average unique values: [10.0, 10.0, 15.0, 15.0, 20.0]

#### Scenario: AVG returns float
- GIVEN values [1, 2, 3] summing to 6
- WHEN query executes `SELECT AVG(value) OVER (ORDER BY id)`
- THEN all rows return 2.0 (float type), not 2 (integer)

### Requirement: Aggregate Window Functions - MIN

The executor SHALL implement MIN(expr) OVER (window_spec) window function, which returns the minimum value of expr within the window frame. MIN returns NULL if the frame is empty or contains only NULLs. MIN supports FILTER clause for conditional minimum. Note: MIN does not support DISTINCT (makes no semantic sense).

#### Scenario: MIN basic window
- GIVEN values [40, 10, 30, 20] with default frame
- WHEN query executes `SELECT value, MIN(value) OVER (ORDER BY id)`
- THEN mins are [40, 10, 10, 10]

#### Scenario: MIN with sliding window
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, MIN(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN mins are [10, 10, 20, 30]

#### Scenario: MIN with NULL values
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, MIN(value) OVER (ORDER BY id)`
- THEN mins are [10, 10, 10, 10, 10] (NULLs ignored)

#### Scenario: MIN with partition
- GIVEN sales table (region: North,North,South,South) with (amount: 100,200,150,250)
- WHEN query executes `SELECT region, MIN(amount) OVER (PARTITION BY region ORDER BY id)`
- THEN North: [100, 100], South: [150, 150]

#### Scenario: MIN with FILTER clause
- GIVEN table (value: 10,20,30,40) and (flag: true,false,true,false)
- WHEN query executes `SELECT MIN(value) FILTER (WHERE flag) OVER (ORDER BY id)`
- THEN mins only consider flagged values: [10, 10, 30, 30]

#### Scenario: MIN with empty frame
- GIVEN values with frame ROWS 10 FOLLOWING (likely empty)
- WHEN query executes `SELECT MIN(value) OVER (ORDER BY id ROWS 10 FOLLOWING)`
- THEN empty frame rows return NULL

#### Scenario: MIN with string values
- GIVEN values ['apple', 'banana', 'cherry']
- WHEN query executes `SELECT value, MIN(value) OVER (ORDER BY id)`
- THEN mins are ['apple', 'apple', 'apple'] (lexicographic comparison)

### Requirement: Aggregate Window Functions - MAX

The executor SHALL implement MAX(expr) OVER (window_spec) window function, which returns the maximum value of expr within the window frame. MAX returns NULL if the frame is empty or contains only NULLs. MAX supports FILTER clause for conditional maximum. Note: MAX does not support DISTINCT.

#### Scenario: MAX basic window
- GIVEN values [40, 10, 30, 20] with default frame
- WHEN query executes `SELECT value, MAX(value) OVER (ORDER BY id)`
- THEN maxs are [40, 40, 40, 40]

#### Scenario: MAX with sliding window
- GIVEN values [10, 20, 30, 40]
- WHEN query executes `SELECT value, MAX(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)`
- THEN maxs are [20, 30, 40, 40]

#### Scenario: MAX with NULL values
- GIVEN values [10, NULL, 20, NULL, 30]
- WHEN query executes `SELECT value, MAX(value) OVER (ORDER BY id)`
- THEN maxs are [10, 10, 20, 20, 30] (NULLs ignored)

#### Scenario: MAX with partition
- GIVEN sales table (region: East,East,West,West) with (amount: 100,200,150,250)
- WHEN query executes `SELECT region, MAX(amount) OVER (PARTITION BY region ORDER BY id)`
- THEN East: [200, 200], West: [250, 250]

#### Scenario: MAX with FILTER clause
- GIVEN table (value: 10,20,30,40) and (flag: true,false,true,false)
- WHEN query executes `SELECT MAX(value) FILTER (WHERE flag) OVER (ORDER BY id)`
- THEN maxs only consider flagged values: [10, 10, 30, 30]

#### Scenario: MAX with date values
- GIVEN dates [2024-01-01, 2024-01-15, 2024-02-01]
- WHEN query executes `SELECT date, MAX(date) OVER (ORDER BY seq)`
- THEN maxs are [2024-01-01, 2024-01-15, 2024-02-01] (temporal comparison)

#### Scenario: MAX returns same type as input
- GIVEN numeric values with various types (int, float, decimal)
- WHEN query executes `SELECT MAX(value) OVER (ORDER BY id)`
- THEN return type matches input type

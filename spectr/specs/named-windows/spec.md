# Named Windows Specification

## Requirements

### Requirement: Overview

The system MUST implement the following functionality.

Named windows allow defining window specifications once and reusing them multiple times in a query. This improves query readability and maintainability while ensuring consistent window definitions across multiple window functions.

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Syntax

The system MUST implement the following functionality.

```sql
-- Named window in WINDOW clause
SELECT
    function() OVER window_name,
    function() OVER (window_name [modification])
FROM table
WINDOW window_name AS (window_specification)

-- Multiple named windows
SELECT
    function() OVER w1,
    function() OVER w2
FROM table
WINDOW
    w1 AS (window_specification1),
    w2 AS (window_specification2)
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Window Specification Components

The system MUST implement the following functionality.

A window specification can include:
- PARTITION BY clause
- ORDER BY clause
- Frame clause

```sql
WINDOW w AS (
    [PARTITION BY expression [, ...]]
    [ORDER BY expression [ASC|DESC] [NULLS {FIRST|LAST}] [, ...]]
    [frame_clause]
)
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Basic Usage

The system MUST implement the following functionality.

#### Simple Named Window
```sql
-- Define and use a named window
SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER dept_window as row_num,
    RANK() OVER dept_window as rank,
    DENSE_RANK() OVER dept_window as dense_rank
FROM employees
WINDOW dept_window AS (PARTITION BY department ORDER BY salary DESC);
```

#### Reusing Window Specifications
```sql
-- Same window used for multiple functions
SELECT
    product_id,
    category,
    price,
    price - FIRST_VALUE(price) OVER price_window as price_diff_from_cheapest,
    price - LAST_VALUE(price) OVER price_window as price_diff_from_most_expensive,
    NTH_VALUE(price, 2) OVER price_window as second_cheapest_price
FROM products
WINDOW price_window AS (PARTITION BY category ORDER BY price);
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Window Inheritance

The system MUST implement the following functionality.

Named windows can be referenced and modified:

#### Adding ORDER BY
```sql
-- Base window with only PARTITION BY
SELECT
    employee_id,
    department,
    salary,
    hire_date,
    -- Window with just partitioning
    COUNT(*) OVER dept_partition as dept_count,
    -- Same window but with ORDER BY added
    ROW_NUMBER() OVER (dept_partition ORDER BY hire_date) as seniority_rank,
    -- Same window with different ORDER BY
    ROW_NUMBER() OVER (dept_partition ORDER BY salary DESC) as salary_rank
FROM employees
WINDOW dept_partition AS (PARTITION BY department);
```

#### Adding Frame Clause
```sql
-- Base window with PARTITION BY and ORDER BY
SELECT
    date,
    sales,
    -- Running total (default frame)
    SUM(sales) OVER sales_window as running_total,
    -- Moving average (modified frame)
    AVG(sales) OVER (sales_window ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_avg
FROM daily_sales
WINDOW sales_window AS (ORDER BY date);
```

#### Complete Example
```sql
-- Complex window inheritance
SELECT
    customer_id,
    order_date,
    order_amount,
    -- Total for customer (partition only)
    SUM(order_amount) OVER customer_window as customer_total,
    -- Running total with default frame
    SUM(order_amount) OVER (customer_window ORDER BY order_date) as customer_running_total,
    -- Moving average with custom frame
    AVG(order_amount) OVER (
        customer_window
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as customer_moving_avg
FROM orders
WINDOW customer_window AS (PARTITION BY customer_id);
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Multiple Named Windows

The system MUST implement the following functionality.

A query can define multiple named windows:

```sql
SELECT
    employee_id,
    department,
    salary,
    hire_date,
    -- Rank within department by salary
    ROW_NUMBER() OVER salary_window as salary_rank,
    -- Rank company-wide by seniority
    ROW_NUMBER() OVER seniority_window as company_seniority_rank,
    -- Rank within department by seniority
    ROW_NUMBER() OVER (dept_window ORDER BY hire_date) as dept_seniority_rank
FROM employees
WINDOW
    salary_window AS (PARTITION BY department ORDER BY salary DESC),
    seniority_window AS (ORDER BY hire_date),
    dept_window AS (PARTITION BY department);
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Window Resolution Rules

The system MUST implement the following functionality.

#### 1. Name Resolution
- Window names are query-scoped
- Must be unique within a query
- Case-insensitive resolution

#### 2. Inheritance Rules
- Can only add clauses, not override
- Cannot add PARTITION BY if base window has it
- Can add ORDER BY if base window doesn't have it
- Can add/modify frame clause

#### 3. Reference Validation
- All referenced windows must be defined
- No circular references allowed
- Forward references are allowed

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Valid Window Modifications

The system MUST implement the following functionality.

#### Allowed Modifications
```sql
-- Add ORDER BY to window with only PARTITION BY
WINDOW w AS (PARTITION BY x)
SELECT ... OVER (w ORDER BY y) ...

-- Add frame to window with PARTITION BY and ORDER BY
WINDOW w AS (PARTITION BY x ORDER BY y)
SELECT ... OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) ...

-- Modify frame clause
WINDOW w AS (ORDER BY x ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
SELECT ... OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ...
```

#### Invalid Modifications
```sql
-- Cannot add PARTITION BY to existing partitioned window
WINDOW w AS (PARTITION BY x)
SELECT ... OVER (PARTITION BY y) ...  -- ERROR

-- Cannot have conflicting ORDER BY
WINDOW w AS (ORDER BY x)
SELECT ... OVER (w ORDER BY y) ...  -- ERROR: conflicting ORDER BY
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Complex Examples

The system MUST implement the following functionality.

#### Financial Analysis
```sql
-- Complex financial calculations with named windows
SELECT
    symbol,
    date,
    close_price,
    volume,

    -- Price relative to 52-week high/low
    close_price / MAX(close_price) OVER year_window as pct_of_year_high,
    close_price / MIN(close_price) OVER year_window as pct_of_year_low,

    -- Moving averages
    AVG(close_price) OVER ma_20 as ma_20_day,
    AVG(close_price) OVER ma_50 as ma_50_day,

    -- Volume analysis
    volume / AVG(volume) OVER month_window as volume_ratio,

    -- Trend analysis
    close_price - LAG(close_price, 20) OVER date_window as price_change_20d,

    -- Ranking
    RANK() OVER (PARTITION BY date ORDER BY volume DESC) as volume_rank
FROM stock_data
WINDOW
    year_window AS (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 252 PRECEDING AND CURRENT ROW
    ),
    month_window AS (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 21 PRECEDING AND CURRENT ROW
    ),
    ma_20 AS (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ),
    ma_50 AS (
        PARTITION BY symbol
        ORDER BY date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ),
    date_window AS (
        PARTITION BY symbol
        ORDER BY date
    );
```

#### Time Series Analysis
```sql
-- Time-based analysis with multiple windows
SELECT
    sensor_id,
    reading_time,
    temperature,
    humidity,

    -- Hourly statistics
    AVG(temperature) OVER hour_window as hour_avg_temp,
    MIN(temperature) OVER hour_window as hour_min_temp,
    MAX(temperature) OVER hour_window as hour_max_temp,

    -- Daily comparison
    temperature - FIRST_VALUE(temperature) OVER day_window as temp_change_from_midnight,

    -- Anomaly detection (z-score)
    (temperature - AVG(temperature) OVER week_window) /
    STDDEV(temperature) OVER week_window as temp_zscore,

    -- Trend detection
    CASE
        WHEN temperature > AVG(temperature) OVER (day_window ORDER BY reading_time
                                                  ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
        THEN 'WARMING'
        WHEN temperature < AVG(temperature) OVER (day_window ORDER BY reading_time
                                                  ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
        THEN 'COOLING'
        ELSE 'STABLE'
    END as temp_trend
FROM sensor_readings
WINDOW
    hour_window AS (
        PARTITION BY sensor_id, DATE_TRUNC('hour', reading_time)
        ORDER BY reading_time
    ),
    day_window AS (
        PARTITION BY sensor_id, DATE(reading_time)
        ORDER BY reading_time
    ),
    week_window AS (
        PARTITION BY sensor_id
        ORDER BY reading_time
        ROWS BETWEEN 168 PRECEDING AND CURRENT ROW
    );
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Implementation Details

The system MUST implement the following functionality.

#### Window Registry

```go
type WindowRegistry struct {
    windows map[string]*WindowDefinition
}

type WindowDefinition struct {
    Name       string
    Partition  []Expression
    OrderBy    []SortSpecification
    Frame      *FrameClause
    Location   SourceLocation
}
```

#### Resolution Algorithm

```go
func resolveWindow(windowRef WindowReference, registry *WindowRegistry) (*WindowSpecification, error) {
    // Get base window definition
    baseDef, exists := registry.windows[windowRef.Name]
    if !exists {
        return nil, fmt.Errorf("window '%s' does not exist", windowRef.Name)
    }

    // Create copy of base definition
    resolved := &WindowSpecification{
        Partition: baseDef.Partition,
        OrderBy:   baseDef.OrderBy,
        Frame:     baseDef.Frame,
    }

    // Apply modifications
    if windowRef.Modification != nil {
        // Cannot add PARTITION BY if already exists
        if len(windowRef.Modification.Partition) > 0 && len(resolved.Partition) > 0 {
            return nil, fmt.Errorf("cannot add PARTITION BY to window '%s'", windowRef.Name)
        }

        // Can only add ORDER BY if not already exists
        if len(windowRef.Modification.OrderBy) > 0 {
            if len(resolved.OrderBy) > 0 {
                return nil, fmt.Errorf("window '%s' already has ORDER BY", windowRef.Name)
            }
            resolved.OrderBy = windowRef.Modification.OrderBy
        }

        // Always allow frame modification
        if windowRef.Modification.Frame != nil {
            resolved.Frame = windowRef.Modification.Frame
        }
    }

    return resolved, nil
}
```

#### Validation Rules

1. **No Circular References**
   ```sql
   -- Invalid
   WINDOW w1 AS (w2), w2 AS (w1)
   ```

2. **No Duplicate Names**
   ```sql
   -- Invalid
   WINDOW w AS (...), w AS (...)
   ```

3. **Reference Must Exist**
   ```sql
   -- Invalid
   SELECT ... OVER undefined_window ...
   ```

4. **Inheritance Rules**
   ```sql
   -- Cannot override PARTITION BY
   WINDOW w AS (PARTITION BY x)
   SELECT ... OVER (PARTITION BY y) ...  -- ERROR

   -- Cannot add conflicting ORDER BY
   WINDOW w AS (ORDER BY x)
   SELECT ... OVER (w ORDER BY y) ...  -- ERROR
   ```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Error Handling

The system MUST implement the following functionality.

#### Undefined Window Reference
```sql
SELECT ROW_NUMBER() OVER nonexistent_window;
-- ERROR: window 'nonexistent_window' does not exist
```

#### Duplicate Window Definition
```sql
SELECT * FROM t
WINDOW w AS (ORDER BY x), w AS (ORDER BY y);
-- ERROR: duplicate window name 'w'
```

#### Invalid Inheritance
```sql
WINDOW w AS (PARTITION BY x)
SELECT ... OVER (PARTITION BY y) ...;
-- ERROR: cannot add PARTITION BY to window 'w'
```

#### Circular Reference
```sql
WINDOW w1 AS (w2), w2 AS (w1)
-- ERROR: circular window reference
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Performance Considerations

The system MUST implement the following functionality.

#### 1. Window Reuse
Named windows enable optimization by reusing computed partitions:

```sql
-- All these can share the same partition computation
SELECT
    ROW_NUMBER() OVER w,
    RANK() OVER w,
    DENSE_RANK() OVER w,
    SUM(x) OVER (w ORDER BY y)
FROM t
WINDOW w AS (PARTITION BY category)
```

#### 2. Memory Efficiency
Shared window definitions reduce memory usage:

```go
// Implementation can cache partition boundaries
// and reuse across all functions using the same window
type SharedWindowState struct {
    PartitionKey   string
    PartitionRows  []int
    PeerBoundaries []int
}
```

#### 3. Optimization Opportunities

```go
type WindowOptimizer struct {
    // Merge windows with same PARTITION BY
    mergePartitions bool

    // Reuse ORDER BY sorting
    reuseSorting bool

    // Share frame computations
    shareFrames bool
}

func (o *WindowOptimizer) Optimize(windows []WindowDefinition) []WindowGroup {
    // Group windows by partition specification
    groups := make(map[string][]WindowDefinition)

    for _, w := range windows {
        key := w.PartitionKey()
        groups[key] = append(groups[key], w)
    }

    // Create optimized execution groups
    var result []WindowGroup
    for _, group := range groups {
        result = append(result, WindowGroup{
            SharedPartition: group[0].Partition,
            Windows:         group,
        })
    }

    return result
}
```

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Testing Scenarios

The system MUST implement the following functionality.

#### 1. Basic Named Windows
```sql
-- Single named window
SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (ORDER BY x);

-- Multiple uses
SELECT
    ROW_NUMBER() OVER w,
    RANK() OVER w
FROM t WINDOW w AS (PARTITION BY x ORDER BY y);
```

#### 2. Window Inheritance
```sql
-- Add ORDER BY
WINDOW w AS (PARTITION BY x)
SELECT ROW_NUMBER() OVER (w ORDER BY y) FROM t;

-- Add frame
WINDOW w AS (ORDER BY x)
SELECT SUM(y) OVER (w ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;
```

#### 3. Multiple Windows
```sql
SELECT
    ROW_NUMBER() OVER w1,
    ROW_NUMBER() OVER w2
FROM t
WINDOW
    w1 AS (PARTITION BY x),
    w2 AS (PARTITION BY y);
```

#### 4. Complex Inheritance
```sql
-- Multi-level inheritance
SELECT
    ROW_NUMBER() OVER w,
    SUM(amount) OVER (w ROWS UNBOUNDED PRECEDING),
    AVG(amount) OVER (w ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
FROM sales
WINDOW w AS (PARTITION BY customer_id ORDER BY date);
```

#### 5. Error Cases
```sql
-- Undefined window
SELECT ROW_NUMBER() OVER undefined;

-- Duplicate definition
WINDOW w AS (), w AS () SELECT * FROM t;

-- Invalid inheritance
WINDOW w AS (ORDER BY x) SELECT ROW_NUMBER() OVER (w ORDER BY y) FROM t;
```

This specification provides comprehensive coverage of named window functionality, ensuring flexible reuse of window definitions while maintaining clear semantics and good performance through optimization opportunities."}

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: WINDOW clause SHALL define reusable named window specifications

The WINDOW clause SHALL allow defining one or more named window specifications that can be referenced by window functions in the same SELECT statement via OVER.

#### Scenario: Single named window with multiple functions
```
Given a table "sales" with columns (dept TEXT, amount INT)
When the user executes:
  SELECT ROW_NUMBER() OVER w AS rn, SUM(amount) OVER w AS total
  FROM sales
  WINDOW w AS (PARTITION BY dept ORDER BY amount)
Then both window functions SHALL use the same window specification
And results SHALL be partitioned by dept and ordered by amount
```

#### Scenario: Multiple named windows in same query
```
Given a table "employees" with columns (dept TEXT, salary INT, hire_date DATE)
When the user executes:
  SELECT
    RANK() OVER salary_w AS salary_rank,
    RANK() OVER date_w AS date_rank
  FROM employees
  WINDOW salary_w AS (ORDER BY salary DESC),
         date_w AS (ORDER BY hire_date)
Then salary_rank SHALL rank by salary descending
And date_rank SHALL rank by hire_date ascending
```

### Requirement: OVER SHALL accept a bare window name reference

Window functions SHALL support `OVER name` syntax (without parentheses) to reference a named window definition.

#### Scenario: Bare name reference
```
Given WINDOW w AS (ORDER BY x)
When the user writes "SUM(x) OVER w"
Then the window function SHALL use the specification defined for w
```

### Requirement: OVER SHALL support window inheritance with additional clauses

When OVER references a named window inside parentheses, additional clauses (PARTITION BY, ORDER BY, frame) SHALL be merged with the base window. Overriding an existing clause in the base window SHALL produce an error.

#### Scenario: Inherit PARTITION BY and add ORDER BY
```
Given a table "t" with columns (dept TEXT, x INT, y INT)
When the user executes:
  SELECT SUM(x) OVER (w ORDER BY y)
  FROM t
  WINDOW w AS (PARTITION BY dept)
Then the effective window SHALL have PARTITION BY dept ORDER BY y
```

#### Scenario: Error on overriding base ORDER BY
```
Given WINDOW w AS (ORDER BY x)
When the user writes "SUM(x) OVER (w ORDER BY y)"
Then the system SHALL return an error indicating ORDER BY cannot be overridden
```

### Requirement: Named windows SHALL support transitive references

A named window definition SHALL be able to reference another named window as its base. Circular references SHALL produce an error.

#### Scenario: Transitive window reference
```
Given:
  WINDOW w1 AS (PARTITION BY dept),
         w2 AS (w1 ORDER BY salary)
When a function uses "OVER w2"
Then the effective window SHALL have PARTITION BY dept ORDER BY salary
```

#### Scenario: Circular reference error
```
Given:
  WINDOW w1 AS (w2),
         w2 AS (w1)
Then the system SHALL return an error indicating a circular window reference
```

### Requirement: Undefined window name references SHALL produce an error

Referencing a window name that is not defined in the WINDOW clause SHALL produce a clear error message.

#### Scenario: Undefined window name
```
When the user executes "SELECT SUM(x) OVER undefined_window FROM t"
And no WINDOW clause defines "undefined_window"
Then the system SHALL return an error indicating the window is not defined
```

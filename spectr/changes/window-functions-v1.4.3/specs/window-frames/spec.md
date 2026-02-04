# Window Frames Specification

## ADDED Requirements

### Requirement: Overview

The system MUST implement the following functionality.


Window frames define the set of rows used for computing window functions. This specification details the implementation of ROWS, RANGE, and GROUPS frame types with full boundary and exclusion support.


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Frame Types

The system MUST implement the following functionality.


#### 1. ROWS Frame

Physical row-based boundaries that count exact number of rows.

```sql
-- Syntax
ROWS BETWEEN frame_bound AND frame_bound

-- Examples
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
```

##### ROWS Boundaries

**UNBOUNDED PRECEDING**
- Includes all rows from the start of the partition
- Always valid as start bound
- Example: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`

**offset PRECEDING**
- Includes offset rows before the current row
- offset must be non-negative integer
- Example: `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`

**CURRENT ROW**
- The current row being processed
- Example: `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`

**offset FOLLOWING**
- Includes offset rows after the current row
- offset must be non-negative integer
- Example: `ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING`

**UNBOUNDED FOLLOWING**
- Includes all rows to the end of the partition
- Always valid as end bound
- Example: `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`

#### 2. RANGE Frame

Value-based boundaries that consider the ORDER BY values.

```sql
-- Syntax
RANGE BETWEEN frame_bound AND frame_bound

-- Examples
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
RANGE BETWEEN CURRENT ROW AND CURRENT ROW
```

##### RANGE Boundaries

**UNBOUNDED PRECEDING**
- All rows with ORDER BY values ≤ current row's ORDER BY value
- Equivalent to all rows from partition start

**offset PRECEDING**
- Rows where ORDER BY value ≥ (current value - offset)
- Only valid for numeric and temporal types
- Example: `RANGE BETWEEN 5 PRECEDING AND CURRENT ROW`

**CURRENT ROW**
- All rows with the same ORDER BY value as current row (peer group)
- Example: `RANGE BETWEEN CURRENT ROW AND CURRENT ROW`

**offset FOLLOWING**
- Rows where ORDER BY value ≤ (current value + offset)
- Only valid for numeric and temporal types
- Example: `RANGE BETWEEN CURRENT ROW AND 5 FOLLOWING`

**UNBOUNDED FOLLOWING**
- All rows with ORDER BY values ≥ current row's ORDER BY value
- Equivalent to all rows to partition end

##### RANGE with INTERVAL

For temporal types:

```sql
-- Time-based ranges
SELECT
    timestamp,
    value,
    AVG(value) OVER (
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
    ) as hourly_avg
FROM measurements;

-- Date ranges
SELECT
    date,
    sales,
    SUM(sales) OVER (
        ORDER BY date
        RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW
    ) as weekly_sales
FROM daily_sales;
```

#### 3. GROUPS Frame

Peer group-based boundaries that count groups of rows with equal ORDER BY values.

```sql
-- Syntax
GROUPS BETWEEN frame_bound AND frame_bound

-- Examples
GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING
GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
```

##### GROUPS Boundaries

**UNBOUNDED PRECEDING**
- All peer groups from partition start to current peer group

**offset PRECEDING**
- offset peer groups before current peer group
- Example: `GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW`

**CURRENT ROW**
- The entire current peer group
- Example: `GROUPS BETWEEN CURRENT ROW AND CURRENT ROW`

**offset FOLLOWING**
- offset peer groups after current peer group
- Example: `GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING`

**UNBOUNDED FOLLOWING**
- All peer groups from current peer group to partition end


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Default Frames

The system MUST implement the following functionality.


When no frame is explicitly specified:

#### With ORDER BY
```sql
-- This:
SELECT SUM(x) OVER (ORDER BY y) FROM t;

-- Is equivalent to:
SELECT SUM(x) OVER (
    ORDER BY y
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) FROM t;
```

#### Without ORDER BY
```sql
-- This:
SELECT SUM(x) OVER () FROM t;

-- Is equivalent to:
SELECT SUM(x) OVER (
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) FROM t;
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: EXCLUDE Clause

The system MUST implement the following functionality.


Fine-grained control over which rows to exclude from the frame.

#### EXCLUDE CURRENT ROW
Excludes the current row from the frame.

```sql
SELECT
    x,
    SUM(x) OVER (
        ORDER BY x
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        EXCLUDE CURRENT ROW
    ) as sum_without_current
FROM values;

-- For a row with x=5, the sum includes all other rows but not this row
```

#### EXCLUDE GROUP
Excludes all rows in the current peer group.

```sql
SELECT
    score,
    COUNT(*) OVER (
        ORDER BY score
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        EXCLUDE GROUP
    ) as count_other_scores
FROM test_scores;

-- For score=85, excludes all rows with score=85
```

#### EXCLUDE TIES
Excludes peer group rows except the current row.

```sql
SELECT
    score,
    AVG(score) OVER (
        ORDER BY score
        RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        EXCLUDE TIES
    ) as avg_other_scores
FROM test_scores;

-- For score=85, includes current row but excludes other rows with score=85
```

#### EXCLUDE NO OTHERS
Default behavior - no exclusions.

```sql
-- These are equivalent:
SELECT SUM(x) OVER (ORDER BY x) FROM t;
SELECT SUM(x) OVER (ORDER BY x EXCLUDE NO OTHERS) FROM t;
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Frame Validation Rules

The system MUST implement the following functionality.


#### Valid Frame Specifications

1. **Start bound must not come after end bound**
   ```sql
   -- Valid
   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING

   -- Invalid
   ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING
   ```

2. **UNBOUNDED PRECEDING can only be used as start bound**
   ```sql
   -- Valid
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

   -- Invalid
   ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING
   ```

3. **UNBOUNDED FOLLOWING can only be used as end bound**
   ```sql
   -- Valid
   ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING

   -- Invalid
   ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW
   ```

4. **RANGE offsets require numeric or temporal ORDER BY**
   ```sql
   -- Valid
   SELECT * FROM t ORDER BY num RANGE BETWEEN 5 PRECEDING AND CURRENT ROW;
   SELECT * FROM t ORDER BY date RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW;

   -- Invalid
   SELECT * FROM t ORDER BY name RANGE BETWEEN 5 PRECEDING AND CURRENT ROW;
   ```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Implementation Details

The system MUST implement the following functionality.


#### Frame Boundary Computation

##### ROWS Algorithm
```go
func computeRowsBounds(partition []Row, rowIdx int, startBound, endBound Bound) (int, int) {
    partitionSize := len(partition)

    // Compute start
    var start int
    switch startBound.Type {
    case UnboundedPreceding:
        start = 0
    case CurrentRow:
        start = rowIdx
    case Preceding:
        start = rowIdx - startBound.Offset
        if start < 0 {
            start = 0
        }
    case Following:
        start = rowIdx + startBound.Offset
        if start > partitionSize {
            start = partitionSize
        }
    }

    // Compute end
    var end int
    switch endBound.Type {
    case UnboundedFollowing:
        end = partitionSize - 1
    case CurrentRow:
        end = rowIdx
    case Preceding:
        end = rowIdx - endBound.Offset
        if end < 0 {
            end = -1 // Empty frame
        }
    case Following:
        end = rowIdx + endBound.Offset
        if end >= partitionSize {
            end = partitionSize - 1
        }
    }

    return start, end
}
```

##### RANGE Algorithm
```go
func computeRangeBounds(partition []Row, rowIdx int, startBound, endBound Bound,
                       orderByExpr Expression) (int, int) {

    // Get current row's ORDER BY value
    currentValue := evaluateExpression(orderByExpr, partition[rowIdx])

    // Find start bound
    var start int
    switch startBound.Type {
    case UnboundedPreceding:
        start = 0
    case CurrentRow:
        // Find first row in peer group
        start = findPeerGroupStart(partition, orderByExpr, rowIdx)
    case Preceding:
        targetValue := subtractOffset(currentValue, startBound.Offset)
        start = findFirstGEQ(partition, orderByExpr, targetValue)
    case Following:
        targetValue := addOffset(currentValue, startBound.Offset)
        start = findFirstGEQ(partition, orderByExpr, targetValue)
    }

    // Find end bound
    var end int
    switch endBound.Type {
    case UnboundedFollowing:
        end = len(partition) - 1
    case CurrentRow:
        // Find last row in peer group
        end = findPeerGroupEnd(partition, orderByExpr, rowIdx)
    case Preceding:
        targetValue := subtractOffset(currentValue, endBound.Offset)
        end = findLastLEQ(partition, orderByExpr, targetValue)
    case Following:
        targetValue := addOffset(currentValue, endBound.Offset)
        end = findLastLEQ(partition, orderByExpr, targetValue)
    }

    return start, end
}
```

##### GROUPS Algorithm
```go
func computeGroupsBounds(partition []Row, rowIdx int, startBound, endBound Bound,
                        peerBoundaries []int) (int, int) {

    // Find current peer group
    currentGroup := findPeerGroup(peerBoundaries, rowIdx)

    // Compute start group
    var startGroup int
    switch startBound.Type {
    case UnboundedPreceding:
        startGroup = 0
    case CurrentRow:
        startGroup = currentGroup
    case Preceding:
        startGroup = currentGroup - startBound.Offset
        if startGroup < 0 {
            startGroup = 0
        }
    case Following:
        startGroup = currentGroup + startBound.Offset
        if startGroup >= len(peerBoundaries) {
            return len(partition), -1 // Empty frame
        }
    }

    // Compute end group
    var endGroup int
    switch endBound.Type {
    case UnboundedFollowing:
        endGroup = len(peerBoundaries) - 1
    case CurrentRow:
        endGroup = currentGroup
    case Preceding:
        endGroup = currentGroup - endBound.Offset
        if endGroup < 0 {
            return 0, -1 // Empty frame
        }
    case Following:
        endGroup = currentGroup + endBound.Offset
        if endGroup >= len(peerBoundaries) {
            endGroup = len(peerBoundaries) - 1
        }
    }

    // Convert groups to row boundaries
    start := peerBoundaries[startGroup]
    end := peerBoundaries[endGroup + 1] - 1

    return start, end
}
```

#### EXCLUDE Implementation

```go
func applyExclude(frame FrameBounds, exclude ExcludeMode, partition []Row,
                  rowIdx int, peerBoundaries []int) []int {

    var excluded []int

    switch exclude {
    case ExcludeCurrentRow:
        excluded = []int{rowIdx}

    case ExcludeGroup:
        // Find peer group boundaries
        groupStart := findPeerGroupStart(partition, rowIdx)
        groupEnd := findPeerGroupEnd(partition, rowIdx)
        for i := groupStart; i <= groupEnd; i++ {
            excluded = append(excluded, i)
        }

    case ExcludeTies:
        // Exclude peer group except current row
        groupStart := findPeerGroupStart(partition, rowIdx)
        groupEnd := findPeerGroupEnd(partition, rowIdx)
        for i := groupStart; i <= groupEnd; i++ {
            if i != rowIdx {
                excluded = append(excluded, i)
            }
        }

    case ExcludeNoOthers:
        // No exclusions
        return nil
    }

    return excluded
}
```

#### Performance Optimizations

##### Incremental Frame Updates
For sliding windows:

```go
type SlidingFrameComputer struct {
    prevStart    int
    prevEnd      int

    // Cached values for incremental computation
    sum          float64
    values       *RingBuffer
}

func (s *SlidingFrameComputer) UpdateFrame(
    newStart, newEnd int,
    partition []Row,
    expr Expression) float64 {

    // Remove rows leaving the frame
    for s.prevStart < newStart {
        val := evaluateExpression(expr, partition[s.prevStart])
        s.sum -= toFloat64(val)
        s.prevStart++
    }

    // Add rows entering the frame
    for s.prevEnd < newEnd {
        s.prevEnd++
        val := evaluateExpression(expr, partition[s.prevEnd])
        s.sum += toFloat64(val)
    }

    return s.sum
}
```

##### Binary Search for RANGE
Pre-sort and binary search for efficiency:

```go
type RangeSearchIndex struct {
    sortedValues []any
    positions    []int
    valueIndex   map[any]int
}

func (r *RangeSearchIndex) FindFirstGEQ(target any) int {
    idx := sort.Search(len(r.sortedValues), func(i int) bool {
        return compareValues(r.sortedValues[i], target) >= 0
    })

    if idx < len(r.positions) {
        return r.positions[idx]
    }
    return len(r.positions)
}

func (r *RangeSearchIndex) FindLastLEQ(target any) int {
    idx := sort.Search(len(r.sortedValues), func(i int) bool {
        return compareValues(r.sortedValues[i], target) > 0
    })

    if idx > 0 {
        return r.positions[idx-1]
    }
    return -1
}
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Edge Cases

The system MUST implement the following functionality.


#### Empty Frames
Frames that contain no rows:

```sql
-- Empty frame examples
ROWS BETWEEN 5 FOLLOWING AND 10 FOLLOWING  -- Before any data
ROWS BETWEEN 10 PRECEDING AND 5 PRECEDING  -- After all data
RANGE BETWEEN 100 FOLLOWING AND 200 FOLLOWING  -- No matching values
```

#### Single Row Partitions
```sql
-- All frames are equivalent for single row
SELECT
    value,
    SUM(value) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as sum_all
FROM single_row_table;
-- Result: sum_all = value (only current row in frame)
```

#### NULL ORDER BY Values
```sql
-- NULL values in ORDER BY
SELECT
    category,
    value,
    ROW_NUMBER() OVER (ORDER BY nullable_col) as row_num
FROM data;

-- NULL ordering affects RANGE frames
-- Default is NULLS FIRST for ASC, NULLS LAST for DESC
```

#### Overflow in RANGE Offsets
```sql
-- Handle overflow gracefully
SELECT
    bigint_col,
    COUNT(*) OVER (
        ORDER BY bigint_col
        RANGE BETWEEN 9223372036854775807 FOLLOWING AND UNBOUNDED FOLLOWING
    ) as count_after
FROM big_int_table;
```


#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined

### Requirement: Testing Scenarios

The system MUST implement the following functionality.


#### 1. Basic Frame Tests
```sql
-- Test all frame types
SELECT
    id,
    value,
    SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as rows_sum,
    SUM(value) OVER (ORDER BY value RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) as range_sum,
    SUM(value) OVER (ORDER BY value GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as groups_sum
FROM test_data;
```

#### 2. Boundary Validation
```sql
-- Test boundary conditions
SELECT
    id,
    SUM(value) OVER (ORDER BY id ROWS BETWEEN 0 PRECEDING AND 0 FOLLOWING) as current_only,
    SUM(value) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as all_rows,
    SUM(value) OVER (ORDER BY id ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING) as big_window
FROM small_table;
```

#### 3. EXCLUDE Tests
```sql
-- Test all EXCLUDE modes
SELECT
    score,
    COUNT(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as count_all,
    COUNT(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) as exclude_current,
    COUNT(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) as exclude_group,
    COUNT(*) OVER (ORDER BY score RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) as exclude_ties
FROM test_scores;
```

#### 4. Performance Tests
```sql
-- Large sliding window
SELECT
    time,
    value,
    AVG(value) OVER (
        ORDER BY time
        ROWS BETWEEN 10000 PRECEDING AND 10000 FOLLOWING
    ) as moving_avg
FROM big_time_series;

-- Complex RANGE with duplicates
SELECT
    numeric_value,
    COUNT(*) OVER (
        ORDER BY numeric_value
        RANGE BETWEEN 100 PRECEDING AND 100 FOLLOWING
    ) as nearby_count
FROM table_with_many_duplicates;
```

This specification provides comprehensive coverage of window frame implementation, ensuring correct behavior for all frame types, boundaries, and edge cases while maintaining good performance through optimization strategies."}

#### Scenario: General Validation
- **Given** the system is operational
- **When** this feature is accessed
- **Then** it MUST function as defined


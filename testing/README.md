# Cost-Based Optimizer Testing Suite

This directory contains comprehensive test infrastructure for validating the enhanced cost-based optimizer implementation against DuckDB v1.4.3.

## Overview

The testing suite focuses on three primary validation methods:

1. **Correctness**: All queries produce identical results to DuckDB
2. **Plan Structure**: EXPLAIN output shows equivalent query plans
3. **Cardinality Accuracy**: Estimates within 2x of actual cardinalities

## Directory Structure

```
testing/
├── README.md                    # This file
├── testdata/
│   └── databases/              # Generated test databases
│       ├── comprehensive.db    # Full test suite database
│       ├── small_*.db          # Small (1K rows) datasets
│       ├── medium_*.db         # Medium (100K rows) datasets
│       └── large_*.db          # Large (1M+ rows) datasets
└── tools/
    ├── generate_test_databases.sql   # SQL schema + data generation
    └── generate_testdbs.go           # Go tool for database creation
```

## Test Database Generation

### Prerequisites

- DuckDB CLI (v1.4.3+): `duckdb` must be in PATH
- Go 1.21+ (for database generator tool)

### Generate Test Databases

```bash
# Generate all test databases
cd testing/tools
go run generate_testdbs.go -output ../testdata/databases

# Or use DuckDB directly
duckdb testing/testdata/databases/comprehensive.db < testing/tools/generate_test_databases.sql
```

### Database Specifications

#### Comprehensive Database (`comprehensive.db`)
- **Size**: ~100 MB
- **Contains**:
  - Small uniform table (1K rows)
  - Small skewed table (1K rows, Pareto 80/20)
  - Small clustered table (1K rows, time-series)
  - Small wide table (1K rows, 30 columns)
  - Medium uniform table (100K rows)
  - Medium skewed table (100K rows)
  - Medium clustered table (100K rows, time-series)
  - Large uniform table (1M+ rows)
  - Join tables: `orders`, `customers`, `products`, `order_items`
  - Correlation tables: `correlated_base`, `departments`
  - Indexes on all join and filter columns
  - Statistics (ANALYZE run)

#### Data Characteristics

**Uniform Distribution**
- Random values across full range
- No skew or clustering
- Representative of real-world randomly distributed data

**Skewed Distribution (Pareto 80/20)**
- 80% of data concentrated in 20% of values
- Models real-world hotspots (e.g., popular products, active customers)
- Tests cardinality estimator with skewed statistics

**Clustered Data**
- Time-series data with temporal clustering
- Tests optimizer on sequential scans and range filters
- Models real-world time-bucketed data

**Wide Tables (30+ columns)**
- Tests multi-column statistics and selectivity estimation
- Tests optimizer performance with many columns

## Test Categories

### Phase 1: Database Creation and Verification (Task 9.1)
- Validate test database structure
- Verify all tables exist with correct row counts
- Verify all indexes were created
- Check data characteristics (distributions)

### Phase 2: Correctness Tests (Task 9.2)
- Basic SELECT queries (with/without filters)
- JOIN operations (inner, left, right, full)
- Subqueries (scalar, EXISTS, IN, ANY, ALL)
- Aggregate functions with GROUP BY
- ORDER BY and LIMIT
- Common Table Expressions (CTEs)

**Methodology**:
1. Run same query against DuckDB and dukdb-go
2. Compare result sets (order-independent)
3. Verify identical results

**Expected**: 100% of queries produce identical results

### Phase 3: EXPLAIN Plan Comparison (Task 9.3)
- Parse EXPLAIN output from both systems
- Compare plan structure (not exact formatting)
- Verify:
  - Join order matches
  - Join types match (hash, nested loop, etc.)
  - Filter placement matches
  - Subquery handling matches

**Methodology**:
1. Extract plan structure from EXPLAIN
2. Traverse tree comparing operators
3. Report structural differences

**Expected**: Plans are structurally identical

### Phase 4: Cardinality Estimation (Task 9.4)
- Compare estimated vs actual row counts
- Track per-operator cardinality accuracy
- Measure estimation error as: `|Estimated - Actual| / Actual`

**Methodology**:
1. Run EXPLAIN ANALYZE on same query in both systems
2. Extract estimated cardinality at each operator
3. Execute query and count actual rows
4. Compare: `Estimated should be within [Actual/2, Actual*2]`

**Expected**: 95%+ of estimates within 2x of actual

### Phase 5: TPC-H Benchmark (Tasks 9.5-9.6)
- Run full 22 TPC-H queries
- Measure execution time
- Compare performance between DuckDB and dukdb-go

**Benchmarks**:
- Task 9.5: All queries within 10-20% of DuckDB performance
- Task 9.6: No query more than 2x slower than DuckDB

**Methodology**:
1. Load TPC-H dataset at specific scale
2. Run all 22 queries with warm cache
3. Measure execution time
4. Calculate performance ratio: `(dukdb_time / duckdb_time)`

**Expected**:
- 95%+ queries within 10-20%
- 100% queries within 2x

### Phase 6-8: Edge Case Testing (Tasks 9.7-9.9)

#### Subquery Edge Cases (Task 9.7)
- EXISTS with no matches
- Scalar subquery returning NULL
- Scalar subquery returning multiple rows (error case)
- IN with NULL values
- NOT IN with NULL values
- ANY/ALL with empty result sets
- Multiple levels of correlation
- Lateral joins with complex correlations

#### Filter Pushdown Edge Cases (Task 9.8)
- Complex AND/OR combinations
- Filters with function calls (e.g., UPPER, CAST)
- Filters on NULL values
- Outer join filter placement (ON vs WHERE)
- Aggregate filters (HAVING)

#### Statistics Persistence Edge Cases (Task 9.9)
- Save/load roundtrip
- Statistics format compatibility
- Migration from older versions
- Statistics with NULL values
- Statistics on wide tables

### Phase 9-11: Stress Testing (Tasks 9.10-9.12)

#### Large Database (Task 9.10)
- 1M+ row table
- GB-scale data
- Wide range of filters
- Complex joins
- Tests memory usage and performance at scale

#### Wide Tables (Task 9.11)
- 100+ column table
- Multi-column statistics accuracy
- Joint NDV collection
- Performance impact of many columns

#### Deep Correlation (Task 9.12)
- 3+ level nested subqueries
- Complex multi-level correlations
- Decorrelation complexity tests

## Testing Tools

### EXPLAIN Comparison Tool (Task 9.14)
Located in: `tools/explain_comparison.go`

Compares EXPLAIN output between DuckDB and dukdb-go:
- Parses EXPLAIN text format
- Extracts operator tree structure
- Compares operators, filters, and costs
- Reports structural differences

Usage:
```bash
go run tools/explain_comparison.go \
  --duckdb-explain "plan.duckdb.txt" \
  --dukdb-explain "plan.dukdb.txt"
```

### Cardinality Comparison Tool (Task 9.15)
Located in: `tools/cardinality_comparison.go`

Extracts and compares cardinality estimates:
- Parse EXPLAIN ANALYZE output
- Extract estimated cardinality at each operator
- Compare with actual cardinality
- Generate accuracy reports

Usage:
```bash
go run tools/cardinality_comparison.go \
  --database comprehensive.db \
  --queries test_queries.sql
```

### TPC-H Benchmark Runner (Task 9.16)
Located in: `tools/tpch_benchmark.go`

Runs TPC-H benchmark suite:
- Loads TPC-H dataset
- Executes all 22 queries
- Measures execution time
- Compares DuckDB vs dukdb-go
- Generates benchmark report

Usage:
```bash
go run tools/tpch_benchmark.go \
  --scale 10 \
  --duckdb-db duckdb.db \
  --dukdb-db dukdb.db
```

## Running Tests

### Run all optimizer comprehensive tests:
```bash
cd /path/to/dukdb-go
nix develop
go test -v ./internal/optimizer -run Comprehensive
```

### Run specific test category:
```bash
# Correctness tests only
go test -v ./internal/optimizer -run TestBasicSelectCorrectness

# Cardinality tests only
go test -v ./internal/optimizer -run TestCardinality

# EXPLAIN tests only
go test -v ./internal/optimizer -run TestExplain
```

### Run with test database path:
```bash
TEST_DB_PATH=/path/to/comprehensive.db go test -v ./internal/optimizer -run Comprehensive
```

## Expected Results

### Overall Targets
- **Correctness**: 100% of test queries match DuckDB results
- **EXPLAIN**: 100% of plans structurally match DuckDB
- **Cardinality**: 95%+ estimates within 2x of actual
- **Performance**: 95%+ of queries within 10-20% of DuckDB on TPC-H

### Specific Metrics
- Small tables: All tests pass
- Medium tables: All tests pass
- Large tables (1M+): Performance within 2x, accuracy 90%+
- Wide tables (30+ cols): Cardinality within 2x on 95%+ of queries
- Complex queries (3+ joins, subqueries): Within 2x TPC-H time

## Troubleshooting

### Test database not found
```bash
# Generate the database
cd testing/tools
go run generate_testdbs.go -output ../testdata/databases
```

### DuckDB not in PATH
```bash
# Find DuckDB installation
which duckdb

# Or set path explicitly
export PATH="/nix/store/.../bin:$PATH"
```

### Performance regression detected
1. Check if optimizer changes are reverted
2. Verify statistics are being used
3. Run TPC-H benchmark for detailed analysis
4. Check cardinality estimates for correctness
5. Profile query execution

## Documentation

### Test Methodology Document
- See `TESTING_METHODOLOGY.md` for detailed testing approach
- Describes comparison methods, validation criteria, reporting

### DuckDB Reference
- Code references: `references/duckdb-go/` directory
- Version: v1.4.3
- Key optimizer files:
  - `src/optimizer/optimizer.cpp` - Main optimizer
  - `src/optimizer/filter_pushdown.cpp` - Filter pushdown algorithm
  - `src/storage/statistics/` - Statistics format
  - `src/planner/` - Query planning

## Next Steps

1. **Generate Test Databases** (Task 9.1)
   - Run: `go run testing/tools/generate_testdbs.go`
   - Verify: `ls -lh testing/testdata/databases/`

2. **Implement Correctness Tests** (Task 9.2)
   - Compare query results against DuckDB
   - Run full test suite monthly

3. **Implement EXPLAIN Comparison** (Task 9.3)
   - Parse and compare plan structures
   - Track plan changes during optimization

4. **Implement Cardinality Validation** (Task 9.4)
   - Track estimate accuracy
   - Generate accuracy reports

5. **Run TPC-H Benchmark** (Tasks 9.5-9.6)
   - Measure performance regression
   - Identify slow queries

6. **Integration into CI/CD** (Task 9.17)
   - Automated test running
   - Performance regression detection
   - Automated reporting

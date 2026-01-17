# Comprehensive Test Databases for dukdb-go

This directory contains comprehensive test databases for testing dukdb-go. All databases are generated with deterministic, reproducible data.

## Test Databases

### 1. tpch_subset.db (4.1M)

TPC-H (Transaction Processing Performance Council) subset with 6 core tables:

- **nation** (25 rows): Reference table for nations with region keys
- **region** (5 rows): Geographic regions (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST)
- **supplier** (100 rows): Supplier information with nation associations
- **customer** (150 rows): Customer data with market segments and national affiliations
- **orders** (1,000 rows): Orders with dates, statuses, and priorities
- **lineitem** (5,000 rows): Line items from orders with shipping info
- **part** (100 rows): Product parts with types and pricing

**Use cases:**
- Join testing (multi-table relationships)
- Filtering and aggregation queries
- Date range queries
- Complex queries with multiple conditions

**Key characteristics:**
- Foreign key relationships between tables
- Indexes on foreign key columns
- Deterministic data generation
- Realistic TPC-H schema

### 2. correlation.db (6.8M)

Tables with known correlations for testing correlation-aware query optimization:

- **locations** (1,000 rows): Geographic locations with city-state correlation
  - Each city strictly maps to one state (perfect correlation)
  - Includes latitude/longitude and population data

- **customers_loc** (10,000 rows): Customers associated with locations
  - Correlated with location_id
  - Contains name and age fields

- **orders_loc** (50,000 rows): Orders from customers with location correlation
  - Location strongly correlates with product_category
  - Customers from the same location order similar products

- **transactions** (100,000 rows): Time-series data with daily patterns
  - Hour of day strongly correlates with transaction amount
  - Higher amounts during 12-18 (business hours)

**Use cases:**
- Testing correlation detection in statistics
- Testing decorrelation of subqueries
- Testing cost-based optimization with correlated columns
- Testing selectivity estimates for correlated predicates

**Key characteristics:**
- Explicit correlations between column pairs
- Indexes on frequently filtered columns
- Large enough for meaningful statistics
- Realistic business patterns

### 3. edge_cases.db (2.6M)

Edge cases and special scenarios for comprehensive testing:

- **nullable_data** (8 rows): Various NULL patterns
  - NULL in different columns
  - Complete rows with NULLs
  - Mixed NULL and non-NULL values

- **empty_table** (0 rows): Empty table with schema
  - Used to test handling of empty result sets
  - Tests aggregates on empty tables

- **special_values** (7 rows): Special characters and extreme values
  - Empty strings and whitespace
  - Tabs and newlines
  - Unicode characters
  - Very long strings (1000+ chars)
  - Extreme numeric values (1e100, 1e-100)
  - String concatenation edge cases

- **single_row** (1 row): Single row table
  - Tests LIMIT and pagination
  - Tests aggregates on single row

- **duplicates** (30 rows): Duplicate values
  - Multiple identical rows in categories
  - Tests GROUP BY and DISTINCT
  - Tests duplicate handling in joins

- **identical_values** (100 rows): All identical rows
  - Tests query optimization with identical data
  - Tests aggregates on uniform data

- **extreme_values** (3 rows): Integer and floating-point extremes
  - MIN/MAX integer values
  - Extreme floating point (1e308, -1e308)
  - Zero values (0.0, -0.0)

- **pattern_data** (8 rows): Binary patterns for string matching
  - All combinations of 'a' and 'b' (3 chars)
  - Tests LIKE and pattern matching
  - Tests string comparison

**Use cases:**
- NULL handling tests
- Edge case error conditions
- Special character handling
- Empty result set handling
- Unicode support testing
- Extreme value handling

**Key characteristics:**
- Covers many edge case scenarios
- Small enough for targeted testing
- Clear patterns for validation
- Tests error handling

### 4. performance.db (116M)

Large-scale dataset for performance and scalability testing:

- **performance_data** (1,050,000 rows): Main performance testing table
  - Timestamps (current time minus varying intervals)
  - 10 categories (Category_A through Category_J)
  - 3 subcategories (Sub_X, Sub_Y, Sub_Z)
  - Random metric values (0-10,000)
  - Random counts (0-1,000,000)
  - 3 statuses (Active, Inactive, Pending)
  - 5 regions (North, South, East, West, Central)
  - 10,000 unique users
  - Session IDs (100+ unique sessions)
  - Indexes on timestamp, category, region, user_id

- **performance_aggregates** (100,000 rows): Pre-aggregated daily data
  - Dates spanning 500 days
  - Aggregate statistics (total, average, min, max)
  - Row counts per aggregate
  - Indexes on date and category

**Use cases:**
- Performance regression testing
- Scalability testing (1M+ rows)
- Index usage evaluation
- Query optimization validation
- Memory usage profiling
- Large join testing
- Aggregation performance testing

**Key characteristics:**
- 1M+ rows for realistic performance testing
- Multiple indexes for different query patterns
- Realistic correlation patterns
- Time-series data with patterns
- Large file size (116M) for memory tests

## Generation

All databases are generated using the script `create_test_databases.sh`:

```bash
./test_data/create_test_databases.sh
```

This script:
1. Creates all four databases deterministically
2. Populates with consistent seed values
3. Creates appropriate indexes
4. Verifies row counts
5. Reports sizes and structure

The script uses DuckDB CLI and produces identical databases each time it's run.

## Regeneration

To regenerate the test databases:

```bash
rm /path/to/test_data/*.db
./test_data/create_test_databases.sh
```

## Data Characteristics

### Determinism

All databases use deterministic generation:
- Seed-based random values (reproducible)
- Consistent data distribution
- Same database generated multiple times is identical

### Correlations

- **tpch_subset.db**: Natural TPC-H relationships
- **correlation.db**: Explicit business correlations
  - City ↔ State (1:1 correlation)
  - Category ↔ Location (strong correlation)
  - Hour ↔ Transaction amount (strong correlation)
- **edge_cases.db**: Special cases, not correlated
- **performance.db**: Realistic patterns, weak correlation

### Indexes

Strategic indexes for realistic query patterns:
- Foreign key columns (tpch_subset)
- Filter columns (correlation)
- Primary keys and unique columns (edge_cases)
- Time and category columns (performance)

## Testing Guidelines

### Correctness Tests (9.2)

Use these databases to verify query results match DuckDB:
- tpch_subset.db for join and aggregation tests
- correlation.db for correlation-aware optimization tests
- edge_cases.db for NULL and special case handling
- performance.db for large dataset handling

### Cardinality Estimation (9.4)

Use correlation.db to test cardinality estimates:
- Correlations should be detected
- Estimates should account for correlation
- Multi-column statistics should be available

### Performance Tests (9.5-9.6)

Use performance.db for benchmarking:
- 1M+ rows for realistic performance
- Multiple query patterns
- Join and aggregation heavy workloads

### Edge Case Tests (9.7-9.12)

Use edge_cases.db for robustness testing:
- NULL handling across all operations
- Empty result sets
- Extreme values
- Unicode and special characters

## Files

- `create_test_databases.sh` - Script to generate all databases
- `README.md` - This documentation file
- `*.db` - Generated DuckDB database files (not checked into git)

## Size Information

Total: ~130MB of test data

- tpch_subset.db: 4.1M
- correlation.db: 6.8M
- edge_cases.db: 2.6M
- performance.db: 116M

## Notes

- Databases are regenerable, not stored in version control
- Use `create_test_databases.sh` to regenerate after cloning
- All data is non-sensitive test data
- Suitable for public repositories and CI/CD systems

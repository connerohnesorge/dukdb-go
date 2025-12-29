# Design: Compatibility Test Suite

## Context

The dukdb-go project aims to be a complete drop-in replacement for duckdb-go. To prove this claim, we need systematic compatibility testing that verifies identical behavior across both implementations.

**Stakeholders**:
- Users evaluating dukdb-go as a duckdb-go replacement
- Developers maintaining API compatibility
- CI/CD systems validating releases

**Constraints**:
- Must work without CGO (dukdb-go only mode)
- Must optionally compare with duckdb-go when CGO available
- Must use deterministic testing for reproducibility
- Must cover all public API surface

## Goals / Non-Goals

**Goals**:
1. Prove API compatibility with duckdb-go
2. Verify SQL query result equivalence
3. Test all 37 DuckDB data types
4. Validate UDF, Appender, and advanced features
5. Enable CI/CD compatibility gates

**Non-Goals**:
1. Performance benchmarking (separate concern)
2. Stress testing (separate concern)
3. Fuzz testing (separate concern)
4. Internal implementation testing

## Decisions

### Decision 1: Test Framework Architecture

**Options**:
A. Shared test functions with interface abstraction
B. Separate test files with code duplication
C. Test generation from specifications
D. Property-based testing

**Choice**: A - Shared test functions with interface abstraction

**Rationale**:
- Single source of truth for expected behavior
- DriverAdapter interface abstracts implementation differences
- Same test code runs against both backends
- Easy to add new tests without duplication

```go
type DriverAdapter interface {
    Open(dsn string) (*sql.DB, error)
    WithClock(clock quartz.Clock) DriverAdapter
    SupportsArrow() bool
    // ...
}
```

### Decision 2: Test Categories

**Options**:
A. Flat list of all tests
B. Hierarchical categories (sql/types/api/features)
C. Feature-based grouping
D. Complexity-based grouping

**Choice**: B - Hierarchical categories

**Rationale**:
- Clear organization by concern
- Easy to run subset of tests
- Maps to documentation structure
- Enables progressive validation

Categories:
- `sql` - SQL language compatibility
- `type` - Data type round-trip
- `api` - driver.Driver interface
- `appender` - Appender API
- `udf` - User-defined functions
- `profiling` - Query profiling
- `tpch` - TPC-H benchmark queries
- `deterministic` - Mock clock tests

### Decision 3: Result Comparison Strategy

**Options**:
A. Exact byte comparison
B. Semantic comparison with tolerance
C. Row-by-row value comparison
D. Schema + data comparison

**Choice**: C - Row-by-row value comparison with type-appropriate comparison

**Rationale**:
- Handles floating point tolerance
- Works with different internal representations
- Catches actual semantic differences
- Clear error messages on mismatch

```go
func CompareResults(t *testing.T, expected, actual []Row) {
    require.Len(t, actual, len(expected))
    for i := range expected {
        for j := range expected[i] {
            compareValues(t, expected[i][j], actual[i][j])
        }
    }
}

func compareValues(t *testing.T, expected, actual any) {
    switch e := expected.(type) {
    case float64:
        assert.InDelta(t, e, actual.(float64), 1e-10)
    case []byte:
        assert.Equal(t, e, actual.([]byte))
    default:
        assert.Equal(t, expected, actual)
    }
}
```

### Decision 4: CGO Toggle Strategy

**Options**:
A. Build tags only
B. Runtime detection
C. Environment variable
D. Build tags + runtime fallback

**Choice**: D - Build tags + runtime fallback

**Rationale**:
- Build tags prevent CGO linking when not wanted
- Runtime detection allows graceful degradation
- Environment variable for CI control
- Clear skip messages when reference unavailable

```go
//go:build duckdb_cgo

package compatibility

import duckdb "github.com/marcboeker/go-duckdb"

func newDuckDBAdapter() DriverAdapter {
    return &duckdbAdapter{}
}
```

### Decision 5: Test Data Strategy

**Options**:
A. External SQL files
B. Embedded Go strings
C. Generated test data
D. Reference data files

**Choice**: B + C - Embedded Go strings with generated data for complex tests

**Rationale**:
- Embedded strings keep tests self-contained
- Generated data (TPC-H) uses DuckDB's dbgen
- No external file dependencies
- Easy to read and modify

```go
const testQuery = `
    SELECT category, SUM(amount)
    FROM orders
    GROUP BY category
`

func setupTPCH(db *sql.DB) {
    db.Exec(`CALL dbgen(sf=0.01)`)
}
```

### Decision 6: Deterministic Testing Integration

**Options**:
A. Separate deterministic test suite
B. Clock injection in all tests
C. Deterministic tests as subset
D. No deterministic testing

**Choice**: C - Deterministic tests as focused subset

**Rationale**:
- Most tests don't need deterministic timing
- Dedicated category for time-sensitive tests
- Clear separation of concerns
- Uses existing quartz.Clock infrastructure

```go
var DeterministicTests = []CompatibilityTest{
    {Name: "MockClockProfiling", Category: "deterministic", ...},
    {Name: "MockClockTimeout", Category: "deterministic", ...},
}
```

### Decision 7: Skip/Feature Detection

**Options**:
A. Hard-coded skip flags
B. Runtime feature detection
C. Version-based skipping
D. Capability queries

**Choice**: B - Runtime feature detection via adapter

**Rationale**:
- Adapters know their capabilities
- Tests can be written ahead of implementation
- Clear skip reasons in test output
- Supports incremental feature parity

```go
type CompatibilityTest struct {
    Name      string
    SkipDukdb bool  // Not yet implemented in dukdb-go
    SkipDuckdb bool // CGO not available
}

func (a *dukdbAdapter) SupportsArrow() bool {
    return true  // After arrow implementation complete
}
```

### Decision 8: Error Comparison

**Options**:
A. Exact error string match
B. Error type match
C. Error category match
D. No error comparison

**Choice**: C - Error category match

**Rationale**:
- Error messages may differ between implementations
- Error types may not align exactly
- Category (syntax, constraint, not found) should match
- Enables meaningful compatibility assertion

```go
type ErrorCategory int

const (
    ErrSyntax ErrorCategory = iota
    ErrConstraint
    ErrNotFound
    ErrType
    ErrPermission
)

func categorizeError(err error) ErrorCategory {
    // Classify error by inspecting message/type
}
```

### Decision 9: Parallel Test Execution

**Options**:
A. All tests sequential
B. All tests parallel
C. Parallel by category
D. Parallel within category

**Choice**: C - Parallel by category

**Rationale**:
- Categories are independent
- Within category, tests share database state
- Maximizes parallelism safely
- `t.Parallel()` at category level

```go
func TestAllCompatibility(t *testing.T) {
    t.Run("SQL", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, SQLCompatibilityTests)
    })
    t.Run("Types", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, TypeCompatibilityTests)
    })
}
```

### Decision 10: Reporting

**Options**:
A. Standard Go test output
B. JUnit XML
C. Custom markdown report
D. All of the above

**Choice**: D - All formats supported

**Rationale**:
- Standard output for development
- JUnit for CI integration
- Markdown for documentation
- Environment variable selects format

```go
func TestGenerateCompatibilityReport(t *testing.T) {
    report := GenerateCompatibilityReport()

    switch os.Getenv("REPORT_FORMAT") {
    case "markdown":
        fmt.Println(report.Markdown())
    case "junit":
        fmt.Println(report.JUnit())
    default:
        fmt.Println(report.Text())
    }
}
```

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| CGO reference unavailable | Medium | Tests run dukdb-go only, skip comparison |
| Test maintenance burden | Medium | Well-organized categories, table-driven tests |
| False compatibility claims | High | TPC-H validates real workloads |
| Slow test execution | Low | Parallel execution, short mode skips TPC-H |
| Type comparison edge cases | Medium | Type-specific comparators with tolerance |

## Migration Plan

### Phase 1: Framework
1. Create `compatibility/` directory structure
2. Implement DriverAdapter interface
3. Implement TestRunner with category support

### Phase 2: Core Tests
1. SQL compatibility tests
2. Type compatibility tests
3. API compatibility tests

### Phase 3: Feature Tests
1. Appender tests
2. UDF tests
3. Profiling tests

### Phase 4: Benchmarks
1. TPC-H query tests
2. Deterministic tests
3. Report generation

### Phase 5: CI Integration
1. Add to CI pipeline
2. Generate compatibility reports
3. Gate releases on compatibility

## Open Questions (Resolved)

1. **What SQL coverage is needed?**
   - Answer: Core DDL, DML, aggregates, joins, window functions

2. **How to handle TPC-H data?**
   - Answer: Use DuckDB's built-in dbgen at sf=0.01

3. **How to compare floating point?**
   - Answer: InDelta with 1e-10 tolerance

4. **How to handle feature differences?**
   - Answer: SkipDukdb/SkipDuckdb flags on tests

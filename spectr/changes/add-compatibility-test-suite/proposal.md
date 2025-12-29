# Change: Add Compatibility Test Suite

## Why

To prove that dukdb-go is a complete drop-in replacement for duckdb-go, we need a comprehensive compatibility test suite that:

1. **Verifies API compatibility** - Same function signatures, same behavior
2. **Validates SQL compatibility** - Same query results across both implementations
3. **Tests feature parity** - All duckdb-go features work identically in dukdb-go
4. **Enables regression prevention** - Catch incompatibilities before release

**Current State**:
- dukdb-go has 35+ test files covering individual features
- duckdb-go has 14 test files that can serve as compatibility baselines
- No systematic comparison exists between implementations
- No shared test harness for running same tests against both backends

**Goal**: Prove dukdb-go has ALL functionality of DuckDB v1.4.3 without CGO.

## What Changes

### 1. Compatibility Test Framework (compatibility/framework.go - NEW)

```go
// DriverAdapter abstracts the difference between dukdb-go and duckdb-go
type DriverAdapter interface {
    // Database lifecycle
    Open(dsn string) (*sql.DB, error)
    OpenWithConfig(dsn string, config map[string]string) (*sql.DB, error)

    // Feature detection
    SupportsArrow() bool
    SupportsTableUDF() bool
    SupportsScalarUDF() bool
    SupportsAggregateUDF() bool

    // UDF registration
    RegisterScalarUDF(conn *sql.Conn, name string, fn any) error
    RegisterTableUDF(conn *sql.Conn, name string, fn any) error

    // Clock injection for deterministic testing
    WithClock(clock quartz.Clock) DriverAdapter
}

// dukdbAdapter implements DriverAdapter for dukdb-go
type dukdbAdapter struct {
    clock quartz.Clock
}

// duckdbAdapter implements DriverAdapter for duckdb-go (CGO)
// Only available with build tag: //go:build duckdb_cgo
type duckdbAdapter struct {
    clock quartz.Clock
}

// CompatibilityTest represents a single compatibility test case
type CompatibilityTest struct {
    Name        string
    Category    string  // "sql", "api", "type", "udf", "appender"
    Setup       func(db *sql.DB) error
    Test        func(t *testing.T, db *sql.DB)
    Teardown    func(db *sql.DB) error
    SkipDukdb   bool    // Skip for dukdb-go (not yet implemented)
    SkipDuckdb  bool    // Skip for duckdb-go (CGO not available)
}

// TestRunner executes tests against both implementations
type TestRunner struct {
    dukdbAdapter  DriverAdapter
    duckdbAdapter DriverAdapter
    clock         quartz.Clock
}

func (r *TestRunner) RunCompatibilityTests(t *testing.T, tests []CompatibilityTest) {
    for _, test := range tests {
        t.Run(test.Name, func(t *testing.T) {
            // Run against dukdb-go
            if !test.SkipDukdb {
                t.Run("dukdb", func(t *testing.T) {
                    r.runTestWithAdapter(t, r.dukdbAdapter, test)
                })
            }

            // Run against duckdb-go (if CGO available)
            if !test.SkipDuckdb && r.duckdbAdapter != nil {
                t.Run("duckdb", func(t *testing.T) {
                    r.runTestWithAdapter(t, r.duckdbAdapter, test)
                })
            }
        })
    }
}
```

### 2. SQL Compatibility Tests (compatibility/sql_test.go - NEW)

```go
// SQLCompatibilityTests covers basic SQL operations
var SQLCompatibilityTests = []CompatibilityTest{
    // DDL Tests
    {Name: "CreateTable", Category: "sql", Test: testCreateTable},
    {Name: "CreateTableIfNotExists", Category: "sql", Test: testCreateTableIfNotExists},
    {Name: "CreateIndex", Category: "sql", Test: testCreateIndex},
    {Name: "AlterTable", Category: "sql", Test: testAlterTable},
    {Name: "DropTable", Category: "sql", Test: testDropTable},

    // DML Tests
    {Name: "InsertValues", Category: "sql", Test: testInsertValues},
    {Name: "InsertSelect", Category: "sql", Test: testInsertSelect},
    {Name: "Update", Category: "sql", Test: testUpdate},
    {Name: "Delete", Category: "sql", Test: testDelete},

    // Query Tests
    {Name: "SelectStar", Category: "sql", Test: testSelectStar},
    {Name: "SelectColumns", Category: "sql", Test: testSelectColumns},
    {Name: "SelectWhere", Category: "sql", Test: testSelectWhere},
    {Name: "SelectOrderBy", Category: "sql", Test: testSelectOrderBy},
    {Name: "SelectLimit", Category: "sql", Test: testSelectLimit},
    {Name: "SelectGroupBy", Category: "sql", Test: testSelectGroupBy},
    {Name: "SelectHaving", Category: "sql", Test: testSelectHaving},
    {Name: "SelectDistinct", Category: "sql", Test: testSelectDistinct},
    {Name: "SelectJoin", Category: "sql", Test: testSelectJoin},
    {Name: "SelectSubquery", Category: "sql", Test: testSelectSubquery},
    {Name: "SelectCTE", Category: "sql", Test: testSelectCTE},
    {Name: "SelectUnion", Category: "sql", Test: testSelectUnion},

    // Aggregate Functions
    {Name: "AggCount", Category: "sql", Test: testAggCount},
    {Name: "AggSum", Category: "sql", Test: testAggSum},
    {Name: "AggAvg", Category: "sql", Test: testAggAvg},
    {Name: "AggMin", Category: "sql", Test: testAggMin},
    {Name: "AggMax", Category: "sql", Test: testAggMax},
    {Name: "AggCountDistinct", Category: "sql", Test: testAggCountDistinct},

    // Window Functions
    {Name: "WindowRowNumber", Category: "sql", Test: testWindowRowNumber},
    {Name: "WindowRank", Category: "sql", Test: testWindowRank},
    {Name: "WindowLag", Category: "sql", Test: testWindowLag},
    {Name: "WindowLead", Category: "sql", Test: testWindowLead},
}

func testSelectGroupBy(t *testing.T, db *sql.DB) {
    // Setup
    _, err := db.Exec(`CREATE TABLE orders (id INTEGER, category VARCHAR, amount DOUBLE)`)
    require.NoError(t, err)
    _, err = db.Exec(`INSERT INTO orders VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)`)
    require.NoError(t, err)

    // Test
    rows, err := db.Query(`SELECT category, SUM(amount) as total FROM orders GROUP BY category ORDER BY category`)
    require.NoError(t, err)
    defer rows.Close()

    // Verify - same results from both implementations
    var results []struct{ category string; total float64 }
    for rows.Next() {
        var r struct{ category string; total float64 }
        require.NoError(t, rows.Scan(&r.category, &r.total))
        results = append(results, r)
    }

    require.Len(t, results, 2)
    assert.Equal(t, "A", results[0].category)
    assert.Equal(t, 250.0, results[0].total)
    assert.Equal(t, "B", results[1].category)
    assert.Equal(t, 200.0, results[1].total)
}
```

### 3. Type Compatibility Tests (compatibility/types_test.go - NEW)

```go
// TypeCompatibilityTests verifies DuckDB types work identically
// Note: 29 supported types tested; 8 types are internal/unsupported (TYPE_ANY, TYPE_SQLNULL, etc.)
var TypeCompatibilityTests = []CompatibilityTest{
    // Integer types
    {Name: "TypeTinyInt", Category: "type", Test: testTypeTinyInt},
    {Name: "TypeSmallInt", Category: "type", Test: testTypeSmallInt},
    {Name: "TypeInteger", Category: "type", Test: testTypeInteger},
    {Name: "TypeBigInt", Category: "type", Test: testTypeBigInt},
    {Name: "TypeHugeInt", Category: "type", Test: testTypeHugeInt},
    {Name: "TypeUTinyInt", Category: "type", Test: testTypeUTinyInt},
    {Name: "TypeUSmallInt", Category: "type", Test: testTypeUSmallInt},
    {Name: "TypeUInteger", Category: "type", Test: testTypeUInteger},
    {Name: "TypeUBigInt", Category: "type", Test: testTypeUBigInt},
    {Name: "TypeUHugeInt", Category: "type", Test: testTypeUHugeInt},

    // Floating point (with special value tests)
    {Name: "TypeFloat", Category: "type", Test: testTypeFloat},
    {Name: "TypeDouble", Category: "type", Test: testTypeDouble},
    {Name: "TypeFloatNaN", Category: "type", Test: testTypeFloatNaN},
    {Name: "TypeFloatInf", Category: "type", Test: testTypeFloatInf},

    // Fixed point
    {Name: "TypeDecimal", Category: "type", Test: testTypeDecimal},
    {Name: "TypeDecimalPrecision", Category: "type", Test: testTypeDecimalPrecision},

    // String types
    {Name: "TypeVarchar", Category: "type", Test: testTypeVarchar},
    {Name: "TypeBlob", Category: "type", Test: testTypeBlob},

    // Boolean
    {Name: "TypeBoolean", Category: "type", Test: testTypeBoolean},

    // Date/Time
    {Name: "TypeDate", Category: "type", Test: testTypeDate},
    {Name: "TypeTime", Category: "type", Test: testTypeTime},
    {Name: "TypeTimeTZ", Category: "type", Test: testTypeTimeTZ},
    {Name: "TypeTimestamp", Category: "type", Test: testTypeTimestamp},
    {Name: "TypeTimestampTZ", Category: "type", Test: testTypeTimestampTZ},
    {Name: "TypeTimestampS", Category: "type", Test: testTypeTimestampS},
    {Name: "TypeTimestampMS", Category: "type", Test: testTypeTimestampMS},
    {Name: "TypeTimestampNS", Category: "type", Test: testTypeTimestampNS},
    {Name: "TypeInterval", Category: "type", Test: testTypeInterval},

    // Complex types
    {Name: "TypeUUID", Category: "type", Test: testTypeUUID},
    {Name: "TypeJSON", Category: "type", Test: testTypeJSON},
    {Name: "TypeJSONSemantic", Category: "type", Test: testTypeJSONSemantic},
    {Name: "TypeList", Category: "type", Test: testTypeList},
    {Name: "TypeListNested", Category: "type", Test: testTypeListNested},
    {Name: "TypeStruct", Category: "type", Test: testTypeStruct},
    {Name: "TypeStructNested", Category: "type", Test: testTypeStructNested},
    {Name: "TypeMap", Category: "type", Test: testTypeMap},
    {Name: "TypeMapNumericKey", Category: "type", Test: testTypeMapNumericKey},
    {Name: "TypeArray", Category: "type", Test: testTypeArray},
    {Name: "TypeUnion", Category: "type", Test: testTypeUnion},
    {Name: "TypeUnionNull", Category: "type", Test: testTypeUnionNull},
    {Name: "TypeEnum", Category: "type", Test: testTypeEnum},
    {Name: "TypeEnumCustom", Category: "type", Test: testTypeEnumCustom},
    {Name: "TypeBit", Category: "type", Test: testTypeBit},
}

func testTypeHugeInt(t *testing.T, db *sql.DB) {
    _, err := db.Exec(`CREATE TABLE hugeints (val HUGEINT)`)
    require.NoError(t, err)

    // Insert max HUGEINT value
    maxHugeInt := "170141183460469231731687303715884105727"
    _, err = db.Exec(`INSERT INTO hugeints VALUES ($1)`, maxHugeInt)
    require.NoError(t, err)

    // Read back and verify
    var result string
    err = db.QueryRow(`SELECT val::VARCHAR FROM hugeints`).Scan(&result)
    require.NoError(t, err)
    assert.Equal(t, maxHugeInt, result)
}
```

### 4. API Compatibility Tests (compatibility/api_test.go - NEW)

```go
// APICompatibilityTests verifies driver interface compatibility
var APICompatibilityTests = []CompatibilityTest{
    // Connection lifecycle
    {Name: "OpenClose", Category: "api", Test: testOpenClose},
    {Name: "Ping", Category: "api", Test: testPing},
    {Name: "ConnRaw", Category: "api", Test: testConnRaw},

    // Transactions
    {Name: "BeginCommit", Category: "api", Test: testBeginCommit},
    {Name: "BeginRollback", Category: "api", Test: testBeginRollback},
    {Name: "TxIsolation", Category: "api", Test: testTxIsolation},
    {Name: "TxSavepoint", Category: "api", Test: testTxSavepoint},
    {Name: "TxNestedSavepoint", Category: "api", Test: testTxNestedSavepoint},

    // Prepared statements
    {Name: "PrepareExec", Category: "api", Test: testPrepareExec},
    {Name: "PrepareQuery", Category: "api", Test: testPrepareQuery},
    {Name: "PrepareNumInput", Category: "api", Test: testPrepareNumInput},

    // Parameter binding
    {Name: "NamedParams", Category: "api", Test: testNamedParams},
    {Name: "PositionalParams", Category: "api", Test: testPositionalParams},
    {Name: "NullParams", Category: "api", Test: testNullParams},
    {Name: "ParamReuse", Category: "api", Test: testParamReuse},           // SELECT $1, $1
    {Name: "NullInWhere", Category: "api", Test: testNullInWhere},
    {Name: "NullInOrderBy", Category: "api", Test: testNullInOrderBy},

    // Result scanning
    {Name: "ScanPrimitives", Category: "api", Test: testScanPrimitives},
    {Name: "ScanNulls", Category: "api", Test: testScanNulls},
    {Name: "ScanComplexTypes", Category: "api", Test: testScanComplexTypes},

    // Row iteration
    {Name: "RowsColumns", Category: "api", Test: testRowsColumns},
    {Name: "RowsColumnTypes", Category: "api", Test: testRowsColumnTypes},
    {Name: "RowsColumnTypePrecision", Category: "api", Test: testRowsColumnTypePrecision},
    {Name: "RowsNext", Category: "api", Test: testRowsNext},
    {Name: "RowsClose", Category: "api", Test: testRowsClose},
}

// ErrorCompatibilityTests verifies error behavior matches
var ErrorCompatibilityTests = []CompatibilityTest{
    // Syntax errors
    {Name: "ErrSyntaxInvalid", Category: "error", Test: testErrSyntaxInvalid},
    {Name: "ErrSyntaxMissingTable", Category: "error", Test: testErrSyntaxMissingTable},

    // Type errors
    {Name: "ErrTypeMismatch", Category: "error", Test: testErrTypeMismatch},
    {Name: "ErrTypeOverflow", Category: "error", Test: testErrTypeOverflow},

    // Constraint errors
    {Name: "ErrConstraintPrimaryKey", Category: "error", Test: testErrConstraintPrimaryKey},
    {Name: "ErrConstraintUnique", Category: "error", Test: testErrConstraintUnique},
    {Name: "ErrConstraintNotNull", Category: "error", Test: testErrConstraintNotNull},
    {Name: "ErrConstraintCheck", Category: "error", Test: testErrConstraintCheck},
    {Name: "ErrConstraintForeignKey", Category: "error", Test: testErrConstraintForeignKey},

    // Not found errors
    {Name: "ErrTableNotFound", Category: "error", Test: testErrTableNotFound},
    {Name: "ErrColumnNotFound", Category: "error", Test: testErrColumnNotFound},
    {Name: "ErrFunctionNotFound", Category: "error", Test: testErrFunctionNotFound},
}

func testErrConstraintPrimaryKey(t *testing.T, db *sql.DB) {
    _, err := db.Exec(`CREATE TABLE pk_test (id INTEGER PRIMARY KEY)`)
    require.NoError(t, err)

    _, err = db.Exec(`INSERT INTO pk_test VALUES (1)`)
    require.NoError(t, err)

    // Duplicate key should fail
    _, err = db.Exec(`INSERT INTO pk_test VALUES (1)`)
    require.Error(t, err)

    // Error category should be constraint violation
    assert.True(t, isConstraintError(err))
}

func testTxSavepoint(t *testing.T, db *sql.DB) {
    _, err := db.Exec(`CREATE TABLE sp_test (val INTEGER)`)
    require.NoError(t, err)

    tx, err := db.Begin()
    require.NoError(t, err)

    _, err = tx.Exec(`INSERT INTO sp_test VALUES (1)`)
    require.NoError(t, err)

    _, err = tx.Exec(`SAVEPOINT sp1`)
    require.NoError(t, err)

    _, err = tx.Exec(`INSERT INTO sp_test VALUES (2)`)
    require.NoError(t, err)

    _, err = tx.Exec(`ROLLBACK TO sp1`)
    require.NoError(t, err)

    require.NoError(t, tx.Commit())

    // Only value 1 should exist
    var count int
    err = db.QueryRow(`SELECT COUNT(*) FROM sp_test`).Scan(&count)
    require.NoError(t, err)
    assert.Equal(t, 1, count)
}

func testConnRaw(t *testing.T, db *sql.DB) {
    conn, err := db.Conn(context.Background())
    require.NoError(t, err)
    defer conn.Close()

    // Test Raw() access to underlying driver connection
    err = conn.Raw(func(driverConn any) error {
        // Both implementations should expose a *Conn type
        assert.NotNil(t, driverConn)
        return nil
    })
    require.NoError(t, err)
}
```

### 5. Feature Compatibility Tests (compatibility/features_test.go - NEW)

```go
// FeatureCompatibilityTests verifies advanced features
var FeatureCompatibilityTests = []CompatibilityTest{
    // Appender
    {Name: "AppenderInsert", Category: "appender", Test: testAppenderInsert},
    {Name: "AppenderBatch", Category: "appender", Test: testAppenderBatch},
    {Name: "AppenderFlush", Category: "appender", Test: testAppenderFlush},
    {Name: "AppenderClose", Category: "appender", Test: testAppenderClose},

    // Scalar UDF
    {Name: "ScalarUDFSimple", Category: "udf", Test: testScalarUDFSimple},
    {Name: "ScalarUDFContext", Category: "udf", Test: testScalarUDFContext},
    {Name: "ScalarUDFOverload", Category: "udf", Test: testScalarUDFOverload},
    {Name: "ScalarUDFNullHandling", Category: "udf", Test: testScalarUDFNullHandling},

    // Table UDF
    {Name: "TableUDFSimple", Category: "udf", Test: testTableUDFSimple},
    {Name: "TableUDFChunk", Category: "udf", Test: testTableUDFChunk},
    {Name: "TableUDFRow", Category: "udf", Test: testTableUDFRow},

    // Profiling
    {Name: "ProfilingEnable", Category: "profiling", Test: testProfilingEnable},
    {Name: "ProfilingOperators", Category: "profiling", Test: testProfilingOperators},
    {Name: "ProfilingTiming", Category: "profiling", Test: testProfilingTiming},

    // DataChunk
    {Name: "DataChunkCreate", Category: "datachunk", Test: testDataChunkCreate},
    {Name: "DataChunkAppend", Category: "datachunk", Test: testDataChunkAppend},
    {Name: "DataChunkGetValue", Category: "datachunk", Test: testDataChunkGetValue},
}
```

### 6. TPC-H Benchmark Tests (compatibility/tpch_test.go - NEW)

```go
// TPCHTests runs TPC-H queries against both implementations
var TPCHTests = []CompatibilityTest{
    {Name: "TPCH_Q01", Category: "tpch", Test: testTPCHQ01},
    {Name: "TPCH_Q02", Category: "tpch", Test: testTPCHQ02},
    {Name: "TPCH_Q03", Category: "tpch", Test: testTPCHQ03},
    // ... all 22 TPC-H queries
    {Name: "TPCH_Q22", Category: "tpch", Test: testTPCHQ22},
}

// LoadTPCHData loads TPC-H schema and data at scale factor 0.01
func LoadTPCHData(t *testing.T, db *sql.DB) {
    // Create tables
    _, err := db.Exec(tpchSchema)
    require.NoError(t, err)

    // Generate data using DuckDB's built-in TPC-H extension
    _, err = db.Exec(`CALL dbgen(sf=0.01)`)
    require.NoError(t, err)
}

func testTPCHQ01(t *testing.T, db *sql.DB) {
    LoadTPCHData(t, db)

    rows, err := db.Query(tpchQ01)
    require.NoError(t, err)
    defer rows.Close()

    var results []Q01Result
    for rows.Next() {
        var r Q01Result
        require.NoError(t, rows.Scan(&r.ReturnFlag, &r.LineStatus,
            &r.SumQty, &r.SumBasePrice, &r.SumDiscPrice, &r.SumCharge,
            &r.AvgQty, &r.AvgPrice, &r.AvgDisc, &r.CountOrder))
        results = append(results, r)
    }

    // Rigorous validation: exact row count and value checks
    require.Len(t, results, 4) // TPC-H Q01 always returns exactly 4 rows at any scale factor

    // Verify expected return_flag/line_status combinations
    expectedCombos := map[string]bool{"A-F": false, "N-F": false, "N-O": false, "R-F": false}
    for _, r := range results {
        key := r.ReturnFlag + "-" + r.LineStatus
        _, ok := expectedCombos[key]
        assert.True(t, ok, "Unexpected return_flag/line_status: %s", key)
        expectedCombos[key] = true
    }

    // All combinations should be present
    for combo, found := range expectedCombos {
        assert.True(t, found, "Missing expected combination: %s", combo)
    }

    // Verify aggregate values are positive and reasonable
    for _, r := range results {
        assert.Greater(t, r.SumQty, 0.0)
        assert.Greater(t, r.SumBasePrice, 0.0)
        assert.Greater(t, r.CountOrder, int64(0))
        assert.InDelta(t, r.AvgQty, r.SumQty/float64(r.CountOrder), 0.01)
    }
}

// TPC-H Result Comparison verifies both implementations return identical results
func testTPCHResultComparison(t *testing.T, dukdbDB, duckdbDB *sql.DB) {
    queries := []string{tpchQ01, tpchQ03, tpchQ05, tpchQ06, tpchQ10}

    for i, query := range queries {
        t.Run(fmt.Sprintf("Q%02d", i+1), func(t *testing.T) {
            dukdbRows, err := dukdbDB.Query(query)
            require.NoError(t, err)
            dukdbResults := scanAllRows(t, dukdbRows)

            duckdbRows, err := duckdbDB.Query(query)
            require.NoError(t, err)
            duckdbResults := scanAllRows(t, duckdbRows)

            // Compare row counts
            require.Len(t, dukdbResults, len(duckdbResults),
                "Row count mismatch: dukdb=%d, duckdb=%d", len(dukdbResults), len(duckdbResults))

            // Compare each row with tolerance for floats
            for rowIdx := range dukdbResults {
                for colIdx := range dukdbResults[rowIdx] {
                    compareValues(t, duckdbResults[rowIdx][colIdx], dukdbResults[rowIdx][colIdx])
                }
            }
        })
    }
}

const tpchQ01 = `
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM lineitem
WHERE l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
`
```

### 7. Deterministic Test Support (compatibility/deterministic_test.go - NEW)

```go
// DeterministicTests verifies timing-dependent operations work with mock clock
var DeterministicTests = []CompatibilityTest{
    {Name: "MockClockProfiling", Category: "deterministic", Test: testMockClockProfiling},
    {Name: "MockClockTimeout", Category: "deterministic", Test: testMockClockTimeout},
    {Name: "MockClockTimestamp", Category: "deterministic", Test: testMockClockTimestamp},
}

func testMockClockProfiling(t *testing.T, db *sql.DB) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

    // Create profiling context with mock clock
    ctx := NewProfilingContext(mClock)
    ctx.Enable()
    ctx.Start()

    // Execute query
    rows, err := db.Query("SELECT 42")
    require.NoError(t, err)
    rows.Close()

    // Advance mock clock
    mClock.Advance(100 * time.Millisecond)

    // Verify elapsed time matches advanced time
    elapsed := ctx.Elapsed()
    assert.Equal(t, 100*time.Millisecond, elapsed)
}
```

### 8. Test Execution Entry Points (compatibility/all_test.go - NEW)

```go
func TestAllCompatibility(t *testing.T) {
    mClock := quartz.NewMock(t)
    runner := NewTestRunner(mClock)

    t.Run("SQL", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, SQLCompatibilityTests)
    })

    t.Run("Types", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, TypeCompatibilityTests)
    })

    t.Run("API", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, APICompatibilityTests)
    })

    t.Run("Errors", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, ErrorCompatibilityTests)
    })

    t.Run("Features", func(t *testing.T) {
        t.Parallel()
        runner.RunCompatibilityTests(t, FeatureCompatibilityTests)
    })

    t.Run("TPCH", func(t *testing.T) {
        if testing.Short() {
            t.Skip("Skipping TPC-H in short mode")
        }
        runner.RunCompatibilityTests(t, TPCHTests)
    })

    t.Run("Deterministic", func(t *testing.T) {
        runner.RunCompatibilityTests(t, DeterministicTests)
    })
}

// ResultOrderingNote: Tests that don't use ORDER BY should sort results before comparison
// to avoid false failures due to implementation-specific ordering.
func sortResultsForComparison(results [][]any) {
    sort.Slice(results, func(i, j int) bool {
        for col := 0; col < len(results[i]); col++ {
            cmp := compareAny(results[i][col], results[j][col])
            if cmp != 0 {
                return cmp < 0
            }
        }
        return false
    })
}

// Generate compatibility report
func TestGenerateCompatibilityReport(t *testing.T) {
    if os.Getenv("GENERATE_REPORT") == "" {
        t.Skip("Set GENERATE_REPORT=1 to generate report")
    }

    report := GenerateCompatibilityReport()
    fmt.Println(report.Markdown())
}
```

## Impact

- **Affected specs**: compatibility-testing (NEW)
- **Affected code**:
  - NEW: `compatibility/` directory (~2000 lines)
  - NEW: `compatibility/framework.go` (~200 lines)
  - NEW: `compatibility/sql_test.go` (~500 lines)
  - NEW: `compatibility/types_test.go` (~400 lines)
  - NEW: `compatibility/api_test.go` (~300 lines)
  - NEW: `compatibility/features_test.go` (~300 lines)
  - NEW: `compatibility/tpch_test.go` (~200 lines)
  - NEW: `compatibility/deterministic_test.go` (~100 lines)

- **Dependencies**:
  - `github.com/stretchr/testify` - Testing assertions
  - `github.com/coder/quartz` - Mock clock for deterministic tests
  - `duckdb-go` (optional, CGO) - Reference implementation

## Breaking Changes

None. All changes are additive test infrastructure.

## Deterministic Testing Requirements

All compatibility tests use quartz.Clock injection:
- Profiling tests use mock clock for timing verification
- Timeout tests use mock clock for deadline control
- Timestamp tests use mock clock for deterministic values

Test structure ensures reproducible results across runs.

## Running Tests

```bash
# Run all compatibility tests (dukdb-go only)
go test -v ./compatibility/...

# Run against both implementations (requires CGO)
CGO_ENABLED=1 go test -v -tags=duckdb_cgo ./compatibility/...

# Run TPC-H tests
go test -v -run TestAllCompatibility/TPCH ./compatibility/...

# Generate compatibility report
GENERATE_REPORT=1 go test -v -run TestGenerateCompatibilityReport ./compatibility/...
```

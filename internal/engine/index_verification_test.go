// Package engine provides the final verification test for index usage in query plans.
//
// =============================================================================
// Task 8.2: Verify index is actually used in queries (EXPLAIN)
// =============================================================================
//
// This test file provides a comprehensive verification that indexes are actually
// being used in query execution. It demonstrates the complete flow:
//
//  1. Create a table with data
//  2. Run query without index (EXPLAIN shows Scan)
//  3. CREATE INDEX on a column
//  4. Run same query (EXPLAIN shows IndexScan)
//  5. Verify results are identical
//
// The tests serve as both verification and documentation of the index usage
// feature implemented in the fix-index-usage change proposal.
//
// KEY VERIFICATION POINTS:
//   - PhysicalScan is used when no index exists
//   - PhysicalIndexScan is used when a matching index exists
//   - EXPLAIN output clearly indicates which scan type is used
//   - Query results are identical regardless of scan type
//   - Both point lookups (=) and range queries (<, >, BETWEEN) use indexes
package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// COMPREHENSIVE INDEX USAGE VERIFICATION
// =============================================================================

// TestVerifyIndexUsage_CompleteFlow is the master verification test that
// demonstrates the entire index usage flow from start to finish.
//
// This test proves that:
//  1. Without an index, queries use PhysicalScan (sequential scan)
//  2. After creating an index, the same query uses PhysicalIndexScan
//  3. Query results are identical in both cases
//  4. EXPLAIN output clearly shows the difference
func TestVerifyIndexUsage_CompleteFlow(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()
	ec := conn.(*EngineConn)

	// =========================================================================
	// STEP 1: Create table with test data
	// =========================================================================
	t.Log("STEP 1: Creating table with test data...")

	_, err = conn.Execute(ctx, `CREATE TABLE verification_demo (
		id INTEGER,
		name VARCHAR,
		category VARCHAR,
		price INTEGER
	)`, nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, `INSERT INTO verification_demo VALUES
		(1, 'Widget A', 'electronics', 100),
		(2, 'Widget B', 'electronics', 150),
		(3, 'Gadget C', 'tools', 200),
		(4, 'Gadget D', 'tools', 250),
		(5, 'Thing E', 'misc', 75),
		(6, 'Thing F', 'misc', 125),
		(7, 'Item G', 'electronics', 300),
		(8, 'Item H', 'tools', 175),
		(9, 'Product I', 'misc', 225),
		(10, 'Product J', 'electronics', 275)`, nil)
	require.NoError(t, err)

	t.Log("  Table created with 10 rows")

	// =========================================================================
	// STEP 2: Verify query WITHOUT index uses PhysicalScan
	// =========================================================================
	t.Log("STEP 2: Testing query WITHOUT index...")

	query := "SELECT * FROM verification_demo WHERE id = 5"
	planTypeBefore := verifyPlanType(t, ec, query)

	t.Logf("  Query: %s", query)
	t.Logf("  Plan type WITHOUT index: %s", planTypeBefore)
	assert.Equal(t, "PhysicalScan", planTypeBefore,
		"Query should use PhysicalScan when no index exists")

	// Run EXPLAIN before index
	explainBefore, _, err := conn.Query(ctx, "EXPLAIN "+query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, explainBefore)
	explainTextBefore := explainBefore[0]["explain_plan"].(string)
	t.Logf("  EXPLAIN output: %s", explainTextBefore)

	assert.Contains(t, explainTextBefore, "Scan:",
		"EXPLAIN should show 'Scan:' for sequential scan")
	assert.NotContains(t, explainTextBefore, "IndexScan:",
		"EXPLAIN should NOT show 'IndexScan:' before index creation")

	// Execute query and save results
	resultsBefore, _, err := conn.Query(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, resultsBefore, 1, "Should return exactly 1 row")
	t.Logf("  Result: id=%v, name=%v", resultsBefore[0]["id"], resultsBefore[0]["name"])

	// =========================================================================
	// STEP 3: Create index on the id column
	// =========================================================================
	t.Log("STEP 3: Creating index on id column...")

	_, err = conn.Execute(ctx, "CREATE INDEX idx_verification_id ON verification_demo(id)", nil)
	require.NoError(t, err)
	t.Log("  Index 'idx_verification_id' created")

	// =========================================================================
	// STEP 4: Verify same query NOW uses PhysicalIndexScan
	// =========================================================================
	t.Log("STEP 4: Testing same query WITH index...")

	planTypeAfter := verifyPlanType(t, ec, query)

	t.Logf("  Query: %s", query)
	t.Logf("  Plan type WITH index: %s", planTypeAfter)
	assert.Equal(t, "PhysicalIndexScan", planTypeAfter,
		"Query should use PhysicalIndexScan when index exists")

	// Run EXPLAIN after index
	explainAfter, _, err := conn.Query(ctx, "EXPLAIN "+query, nil)
	require.NoError(t, err)
	require.NotEmpty(t, explainAfter)
	explainTextAfter := explainAfter[0]["explain_plan"].(string)
	t.Logf("  EXPLAIN output: %s", explainTextAfter)

	assert.Contains(t, explainTextAfter, "IndexScan:",
		"EXPLAIN should show 'IndexScan:' after index creation")
	assert.Contains(t, explainTextAfter, "idx_verification_id",
		"EXPLAIN should show the index name")
	assert.Contains(t, explainTextAfter, "USING",
		"EXPLAIN should show 'USING' clause")

	// =========================================================================
	// STEP 5: Verify results are identical
	// =========================================================================
	t.Log("STEP 5: Verifying results are identical...")

	resultsAfter, _, err := conn.Query(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, resultsAfter, 1, "Should return exactly 1 row")
	t.Logf("  Result: id=%v, name=%v", resultsAfter[0]["id"], resultsAfter[0]["name"])

	// Compare results
	assert.Equal(t, resultsBefore[0]["id"], resultsAfter[0]["id"],
		"id should be identical")
	assert.Equal(t, resultsBefore[0]["name"], resultsAfter[0]["name"],
		"name should be identical")
	assert.Equal(t, resultsBefore[0]["category"], resultsAfter[0]["category"],
		"category should be identical")
	assert.Equal(t, resultsBefore[0]["price"], resultsAfter[0]["price"],
		"price should be identical")

	t.Log("VERIFICATION COMPLETE: Index is being used correctly!")
	t.Log("  - Before index: PhysicalScan (sequential scan)")
	t.Log("  - After index: PhysicalIndexScan (index scan)")
	t.Log("  - Results are identical")
}

// TestVerifyIndexUsage_RangeQueries verifies that range queries also use indexes.
func TestVerifyIndexUsage_RangeQueries(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()
	ec := conn.(*EngineConn)

	// Create table with numeric data
	_, err = conn.Execute(ctx, "CREATE TABLE range_verify (id INTEGER, value INTEGER)", nil)
	require.NoError(t, err)

	_, err = conn.Execute(ctx, `INSERT INTO range_verify VALUES
		(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
		(6, 60), (7, 70), (8, 80), (9, 90), (10, 100)`, nil)
	require.NoError(t, err)

	// Create index on value column
	_, err = conn.Execute(ctx, "CREATE INDEX idx_range_value ON range_verify(value)", nil)
	require.NoError(t, err)

	// =========================================================================
	// Test various range queries
	// =========================================================================

	rangeQueries := []struct {
		name     string
		query    string
		expected int // expected row count
	}{
		{
			name:     "Less Than (<)",
			query:    "SELECT * FROM range_verify WHERE value < 50",
			expected: 4,
		},
		{
			name:     "Greater Than (>)",
			query:    "SELECT * FROM range_verify WHERE value > 50",
			expected: 5,
		},
		{
			name:     "Less Or Equal (<=)",
			query:    "SELECT * FROM range_verify WHERE value <= 50",
			expected: 5,
		},
		{
			name:     "Greater Or Equal (>=)",
			query:    "SELECT * FROM range_verify WHERE value >= 50",
			expected: 6,
		},
		{
			name:     "Combined Range (AND)",
			query:    "SELECT * FROM range_verify WHERE value >= 30 AND value <= 70",
			expected: 5,
		},
	}

	for _, tc := range rangeQueries {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s", tc.query)

			// Check plan type
			planType := verifyPlanType(t, ec, tc.query)
			t.Logf("  Plan type: %s", planType)

			// Range queries should use IndexRangeScan (which shows as PhysicalIndexScan)
			assert.Equal(t, "PhysicalIndexScan", planType,
				"Range query should use PhysicalIndexScan")

			// Check EXPLAIN output
			explainRows, _, err := conn.Query(ctx, "EXPLAIN "+tc.query, nil)
			require.NoError(t, err)
			if len(explainRows) > 0 {
				explainText := explainRows[0]["explain_plan"].(string)
				t.Logf("  EXPLAIN: %s", explainText)
				assert.Contains(t, explainText, "IndexScan",
					"EXPLAIN should show IndexScan for range query")
			}

			// Verify results
			rows, _, err := conn.Query(ctx, tc.query, nil)
			require.NoError(t, err)
			assert.Len(t, rows, tc.expected,
				"Query should return %d rows", tc.expected)
			t.Logf("  Returned %d rows (expected %d)", len(rows), tc.expected)
		})
	}
}

// TestVerifyIndexUsage_MultipleIndexes verifies that the correct index is chosen
// when multiple indexes exist on a table.
func TestVerifyIndexUsage_MultipleIndexes(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()
	ec := conn.(*EngineConn)

	// Create table
	_, err = conn.Execute(ctx, `CREATE TABLE multi_index_verify (
		id INTEGER,
		email VARCHAR,
		status VARCHAR,
		created_at INTEGER
	)`, nil)
	require.NoError(t, err)

	// Create multiple indexes
	_, err = conn.Execute(ctx, "CREATE INDEX idx_id ON multi_index_verify(id)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_email ON multi_index_verify(email)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_status ON multi_index_verify(status)", nil)
	require.NoError(t, err)

	// Insert test data
	_, err = conn.Execute(ctx, `INSERT INTO multi_index_verify VALUES
		(1, 'alice@example.com', 'active', 20240101),
		(2, 'bob@example.com', 'inactive', 20240102),
		(3, 'charlie@example.com', 'active', 20240103)`, nil)
	require.NoError(t, err)

	t.Log("Testing correct index selection with multiple indexes...")

	// =========================================================================
	// Test 1: Query on id should use idx_id
	// =========================================================================
	t.Log("Test 1: Query on id column")
	query1 := "SELECT * FROM multi_index_verify WHERE id = 2"
	plan1 := verifyPlanType(t, ec, query1)
	assert.Equal(t, "PhysicalIndexScan", plan1, "Should use index for id query")

	explain1, _, _ := conn.Query(ctx, "EXPLAIN "+query1, nil)
	if len(explain1) > 0 {
		text1 := explain1[0]["explain_plan"].(string)
		t.Logf("  EXPLAIN: %s", text1)
		assert.Contains(t, text1, "idx_id",
			"Should use idx_id for id query")
	}

	// =========================================================================
	// Test 2: Query on email should use idx_email
	// =========================================================================
	t.Log("Test 2: Query on email column")
	query2 := "SELECT * FROM multi_index_verify WHERE email = 'alice@example.com'"
	plan2 := verifyPlanType(t, ec, query2)
	assert.Equal(t, "PhysicalIndexScan", plan2, "Should use index for email query")

	explain2, _, _ := conn.Query(ctx, "EXPLAIN "+query2, nil)
	if len(explain2) > 0 {
		text2 := explain2[0]["explain_plan"].(string)
		t.Logf("  EXPLAIN: %s", text2)
		assert.Contains(t, text2, "idx_email",
			"Should use idx_email for email query")
	}

	// =========================================================================
	// Test 3: Query on status should use idx_status
	// =========================================================================
	t.Log("Test 3: Query on status column")
	query3 := "SELECT * FROM multi_index_verify WHERE status = 'active'"
	plan3 := verifyPlanType(t, ec, query3)
	assert.Equal(t, "PhysicalIndexScan", plan3, "Should use index for status query")

	explain3, _, _ := conn.Query(ctx, "EXPLAIN "+query3, nil)
	if len(explain3) > 0 {
		text3 := explain3[0]["explain_plan"].(string)
		t.Logf("  EXPLAIN: %s", text3)
		assert.Contains(t, text3, "idx_status",
			"Should use idx_status for status query")
	}

	// =========================================================================
	// Test 4: Query on non-indexed column should use sequential scan
	// =========================================================================
	t.Log("Test 4: Query on non-indexed column (created_at)")
	query4 := "SELECT * FROM multi_index_verify WHERE created_at = 20240102"
	plan4 := verifyPlanType(t, ec, query4)
	assert.Equal(t, "PhysicalScan", plan4,
		"Should use sequential scan for non-indexed column")

	explain4, _, _ := conn.Query(ctx, "EXPLAIN "+query4, nil)
	if len(explain4) > 0 {
		text4 := explain4[0]["explain_plan"].(string)
		t.Logf("  EXPLAIN: %s", text4)
		assert.Contains(t, text4, "Scan:",
			"Should show sequential Scan for non-indexed query")
		assert.NotContains(t, text4, "IndexScan:",
			"Should NOT show IndexScan for non-indexed query")
	}

	t.Log("Multiple index verification complete!")
}

// TestVerifyIndexUsage_IndexDropRestoresSeqScan verifies that dropping an index
// causes queries to revert to sequential scan.
func TestVerifyIndexUsage_IndexDropRestoresSeqScan(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()
	ec := conn.(*EngineConn)

	// Setup
	_, err = conn.Execute(ctx, "CREATE TABLE drop_verify (id INTEGER, data VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "INSERT INTO drop_verify VALUES (1, 'a'), (2, 'b'), (3, 'c')", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "CREATE INDEX idx_drop_id ON drop_verify(id)", nil)
	require.NoError(t, err)

	query := "SELECT * FROM drop_verify WHERE id = 2"

	// =========================================================================
	// Before DROP: Should use IndexScan
	// =========================================================================
	t.Log("Before DROP INDEX:")
	planBefore := verifyPlanType(t, ec, query)
	t.Logf("  Plan type: %s", planBefore)
	assert.Equal(t, "PhysicalIndexScan", planBefore,
		"Should use PhysicalIndexScan before DROP")

	explainBefore, _, _ := conn.Query(ctx, "EXPLAIN "+query, nil)
	if len(explainBefore) > 0 {
		t.Logf("  EXPLAIN: %s", explainBefore[0]["explain_plan"])
	}

	// =========================================================================
	// DROP INDEX
	// =========================================================================
	t.Log("Dropping index...")
	_, err = conn.Execute(ctx, "DROP INDEX idx_drop_id", nil)
	require.NoError(t, err)

	// =========================================================================
	// After DROP: Should use SeqScan
	// =========================================================================
	t.Log("After DROP INDEX:")
	planAfter := verifyPlanType(t, ec, query)
	t.Logf("  Plan type: %s", planAfter)
	assert.Equal(t, "PhysicalScan", planAfter,
		"Should use PhysicalScan after DROP")

	explainAfter, _, _ := conn.Query(ctx, "EXPLAIN "+query, nil)
	if len(explainAfter) > 0 {
		text := explainAfter[0]["explain_plan"].(string)
		t.Logf("  EXPLAIN: %s", text)
		assert.NotContains(t, text, "IndexScan",
			"Should NOT show IndexScan after DROP")
	}

	// =========================================================================
	// Verify results still correct
	// =========================================================================
	rows, _, err := conn.Query(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1, "Should still return correct results")
	assert.Equal(t, "b", rows[0]["data"])

	t.Log("DROP INDEX verification complete!")
}

// TestVerifyIndexUsage_EXPLAIN_Format documents the expected EXPLAIN output format
// for both sequential scan and index scan.
func TestVerifyIndexUsage_EXPLAIN_Format(t *testing.T) {
	engine := NewEngine()
	defer func() {
		require.NoError(t, engine.Close())
	}()

	conn, err := engine.Open(":memory:", nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	ctx := context.Background()

	// Setup
	_, err = conn.Execute(ctx, "CREATE TABLE explain_format (id INTEGER, name VARCHAR)", nil)
	require.NoError(t, err)
	_, err = conn.Execute(ctx, "INSERT INTO explain_format VALUES (1, 'test'), (2, 'demo')", nil)
	require.NoError(t, err)

	t.Log("=== EXPLAIN OUTPUT FORMAT DOCUMENTATION ===")

	// =========================================================================
	// Sequential Scan EXPLAIN format
	// =========================================================================
	t.Log("")
	t.Log("--- Sequential Scan EXPLAIN Format ---")
	explainSeq, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM explain_format WHERE id = 1", nil)
	require.NoError(t, err)
	require.NotEmpty(t, explainSeq)

	seqText := explainSeq[0]["explain_plan"].(string)
	t.Logf("EXPLAIN SELECT * FROM explain_format WHERE id = 1:")
	t.Logf("  %s", seqText)
	t.Log("")
	t.Log("Expected format for Sequential Scan:")
	t.Log("  Scan: <table_name>")
	t.Log("    Filter: <predicate>")

	// Verify expected components
	assert.True(t, strings.Contains(seqText, "Scan:"),
		"Sequential scan EXPLAIN should contain 'Scan:'")
	assert.True(t, strings.Contains(seqText, "explain_format"),
		"Sequential scan EXPLAIN should contain table name")

	// =========================================================================
	// Index Scan EXPLAIN format
	// =========================================================================
	_, err = conn.Execute(ctx, "CREATE INDEX idx_explain_id ON explain_format(id)", nil)
	require.NoError(t, err)

	t.Log("")
	t.Log("--- Index Scan EXPLAIN Format ---")
	explainIdx, _, err := conn.Query(ctx, "EXPLAIN SELECT * FROM explain_format WHERE id = 1", nil)
	require.NoError(t, err)
	require.NotEmpty(t, explainIdx)

	idxText := explainIdx[0]["explain_plan"].(string)
	t.Logf("EXPLAIN SELECT * FROM explain_format WHERE id = 1:")
	t.Logf("  %s", idxText)
	t.Log("")
	t.Log("Expected format for Index Scan:")
	t.Log("  IndexScan: <table_name> USING <index_name>")
	t.Log("    Lookup: <column> = <value>")

	// Verify expected components
	assert.True(t, strings.Contains(idxText, "IndexScan:"),
		"Index scan EXPLAIN should contain 'IndexScan:'")
	assert.True(t, strings.Contains(idxText, "USING"),
		"Index scan EXPLAIN should contain 'USING'")
	assert.True(t, strings.Contains(idxText, "idx_explain_id"),
		"Index scan EXPLAIN should contain index name")

	// =========================================================================
	// EXPLAIN ANALYZE format
	// =========================================================================
	t.Log("")
	t.Log("--- EXPLAIN ANALYZE Format ---")
	explainAnalyze, _, err := conn.Query(
		ctx,
		"EXPLAIN ANALYZE SELECT * FROM explain_format WHERE id = 1",
		nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, explainAnalyze)

	analyzeText := explainAnalyze[0]["explain_plan"].(string)
	t.Logf("EXPLAIN ANALYZE SELECT * FROM explain_format WHERE id = 1:")
	t.Logf("  %s", analyzeText)
	t.Log("")
	t.Log("Expected format for EXPLAIN ANALYZE:")
	t.Log("  <operator>: <details> (actual rows=N, time=Xms)")

	// Verify expected components
	assert.True(t, strings.Contains(analyzeText, "actual rows="),
		"EXPLAIN ANALYZE should contain 'actual rows='")
	assert.True(t, strings.Contains(analyzeText, "time="),
		"EXPLAIN ANALYZE should contain 'time='")

	t.Log("")
	t.Log("=== END EXPLAIN OUTPUT FORMAT DOCUMENTATION ===")
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// verifyPlanType returns the scan type used in the physical plan for a query.
// Returns "PhysicalScan" for sequential scan or "PhysicalIndexScan" for index scan.
func verifyPlanType(t *testing.T, ec *EngineConn, query string) string {
	t.Helper()

	// Parse
	stmt, err := parser.Parse(query)
	require.NoError(t, err)

	// Bind
	b := binder.NewBinder(ec.engine.Catalog())
	boundStmt, err := b.Bind(stmt)
	require.NoError(t, err)

	// Get optimizer hints
	var hints *planner.OptimizationHints
	if opt := ec.engine.Optimizer(); opt != nil && opt.IsEnabled() {
		if _, isSelect := boundStmt.(*binder.BoundSelectStmt); isSelect {
			logicalPlan := createLogicalPlanAdapter(boundStmt)
			if logicalPlan != nil {
				optimizedPlan, _ := opt.Optimize(logicalPlan)
				if optimizedPlan != nil {
					hints = convertOptimizerHints(optimizedPlan)
				}
			}
		}
	}

	// Plan
	p := planner.NewPlanner(ec.engine.Catalog())
	if hints != nil {
		p.SetHints(hints)
	}

	plan, err := p.Plan(boundStmt)
	require.NoError(t, err)

	// Find scan node type
	return findScanType(plan)
}

// findScanType recursively finds the scan node type in a physical plan.
func findScanType(plan planner.PhysicalPlan) string {
	switch plan.(type) {
	case *planner.PhysicalScan:
		return "PhysicalScan"
	case *planner.PhysicalIndexScan:
		return "PhysicalIndexScan"
	}

	for _, child := range plan.Children() {
		if result := findScanType(child); result != "" {
			return result
		}
	}

	return ""
}

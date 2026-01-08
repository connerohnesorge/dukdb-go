// Package planner provides unit tests for optimization hints passing mechanism.
// These tests verify that hints flow correctly from the optimizer through to the planner.
package planner

import (
	"testing"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// 2.6.1: OptimizationHints.SetHints() Unit Tests
// =============================================================================

// TestSetHints_StoresHintsCorrectly verifies that SetHints properly stores hints on the Planner.
func TestSetHints_StoresHintsCorrectly(t *testing.T) {
	cat := catalog.NewCatalog()
	p := NewPlanner(cat)

	// Initially, hints should be nil
	assert.Nil(t, p.hints, "hints should initially be nil")

	// Create and set hints
	hints := NewOptimizationHints()
	hints.AccessHints["users"] = AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
	}
	hints.JoinHints["join_0"] = JoinHint{
		Method:    "HashJoin",
		BuildSide: "right",
	}

	p.SetHints(hints)

	// Verify hints are stored
	require.NotNil(t, p.hints, "hints should not be nil after SetHints")
	assert.Equal(t, hints, p.hints, "hints should be stored correctly")
}

// TestSetHints_NilHints verifies that SetHints can accept nil.
func TestSetHints_NilHints(t *testing.T) {
	cat := catalog.NewCatalog()
	p := NewPlanner(cat)

	// Set nil hints
	p.SetHints(nil)

	// Should be nil
	assert.Nil(t, p.hints, "hints should be nil")
}

// TestSetHints_OverwritesPreviousHints verifies that SetHints overwrites previous hints.
func TestSetHints_OverwritesPreviousHints(t *testing.T) {
	cat := catalog.NewCatalog()
	p := NewPlanner(cat)

	// Set first hints
	hints1 := NewOptimizationHints()
	hints1.AccessHints["table1"] = AccessHint{Method: "IndexScan", IndexName: "idx1"}
	p.SetHints(hints1)

	// Set second hints (should overwrite)
	hints2 := NewOptimizationHints()
	hints2.AccessHints["table2"] = AccessHint{Method: "SeqScan"}
	p.SetHints(hints2)

	// Verify second hints are stored
	require.NotNil(t, p.hints)
	_, hasTable1 := p.hints.GetAccessHint("table1")
	_, hasTable2 := p.hints.GetAccessHint("table2")
	assert.False(t, hasTable1, "table1 hint should be gone")
	assert.True(t, hasTable2, "table2 hint should exist")
}

// =============================================================================
// 2.6.2: GetAccessHint() Unit Tests
// =============================================================================

// TestGetAccessHint_ReturnsHintByTableName verifies that GetAccessHint retrieves hints by table name.
func TestGetAccessHint_ReturnsHintByTableName(t *testing.T) {
	hints := NewOptimizationHints()
	hints.AccessHints["users"] = AccessHint{
		Method:         "IndexScan",
		IndexName:      "idx_users_id",
		Selectivity:    0.05,
		MatchedColumns: 1,
		IsFullMatch:    true,
	}

	hint, ok := hints.GetAccessHint("users")
	require.True(t, ok, "should find hint for 'users'")
	assert.Equal(t, "IndexScan", hint.Method)
	assert.Equal(t, "idx_users_id", hint.IndexName)
	assert.Equal(t, 0.05, hint.Selectivity)
	assert.Equal(t, 1, hint.MatchedColumns)
	assert.True(t, hint.IsFullMatch)
}

// TestGetAccessHint_ReturnsHintByAlias verifies that GetAccessHint can retrieve hints by alias.
func TestGetAccessHint_ReturnsHintByAlias(t *testing.T) {
	hints := NewOptimizationHints()
	hints.AccessHints["u"] = AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_email",
	}

	hint, ok := hints.GetAccessHint("u")
	require.True(t, ok, "should find hint for alias 'u'")
	assert.Equal(t, "IndexScan", hint.Method)
	assert.Equal(t, "idx_users_email", hint.IndexName)
}

// TestGetAccessHint_ReturnsFalseForMissingHint verifies behavior when hint doesn't exist.
func TestGetAccessHint_ReturnsFalseForMissingHint(t *testing.T) {
	hints := NewOptimizationHints()
	hints.AccessHints["users"] = AccessHint{Method: "IndexScan"}

	hint, ok := hints.GetAccessHint("orders")
	assert.False(t, ok, "should not find hint for 'orders'")
	assert.Equal(t, AccessHint{}, hint, "returned hint should be zero value")
}

// TestGetAccessHint_NilHints verifies behavior when hints is nil.
func TestGetAccessHint_NilHints(t *testing.T) {
	var hints *OptimizationHints

	hint, ok := hints.GetAccessHint("users")
	assert.False(t, ok, "should return false for nil hints")
	assert.Equal(t, AccessHint{}, hint)
}

// TestGetAccessHint_NilAccessHintsMap verifies behavior when AccessHints map is nil.
func TestGetAccessHint_NilAccessHintsMap(t *testing.T) {
	hints := &OptimizationHints{
		JoinHints:   make(map[string]JoinHint),
		AccessHints: nil,
	}

	hint, ok := hints.GetAccessHint("users")
	assert.False(t, ok, "should return false when AccessHints map is nil")
	assert.Equal(t, AccessHint{}, hint)
}

// =============================================================================
// 2.6.3: GetJoinHint() Unit Tests
// =============================================================================

// TestGetJoinHint_ReturnsHintByKey verifies that GetJoinHint retrieves hints by key.
func TestGetJoinHint_ReturnsHintByKey(t *testing.T) {
	hints := NewOptimizationHints()
	hints.JoinHints["join_0"] = JoinHint{
		Method:    "HashJoin",
		BuildSide: "right",
	}
	hints.JoinHints["join_1"] = JoinHint{
		Method:    "NestedLoopJoin",
		BuildSide: "",
	}

	hint0, ok0 := hints.GetJoinHint("join_0")
	require.True(t, ok0)
	assert.Equal(t, "HashJoin", hint0.Method)
	assert.Equal(t, "right", hint0.BuildSide)

	hint1, ok1 := hints.GetJoinHint("join_1")
	require.True(t, ok1)
	assert.Equal(t, "NestedLoopJoin", hint1.Method)
}

// TestGetJoinHint_ReturnsFalseForMissingHint verifies behavior when hint doesn't exist.
func TestGetJoinHint_ReturnsFalseForMissingHint(t *testing.T) {
	hints := NewOptimizationHints()

	hint, ok := hints.GetJoinHint("join_99")
	assert.False(t, ok)
	assert.Equal(t, JoinHint{}, hint)
}

// TestGetJoinHint_NilHints verifies behavior when hints is nil.
func TestGetJoinHint_NilHints(t *testing.T) {
	var hints *OptimizationHints

	hint, ok := hints.GetJoinHint("join_0")
	assert.False(t, ok)
	assert.Equal(t, JoinHint{}, hint)
}

// =============================================================================
// 2.6.4: HasAccessHints() and HasJoinHints() Unit Tests
// =============================================================================

// TestHasAccessHints verifies the HasAccessHints method.
func TestHasAccessHints(t *testing.T) {
	tests := []struct {
		name     string
		hints    *OptimizationHints
		expected bool
	}{
		{
			name:     "nil hints",
			hints:    nil,
			expected: false,
		},
		{
			name:     "empty access hints",
			hints:    NewOptimizationHints(),
			expected: false,
		},
		{
			name: "has access hints",
			hints: &OptimizationHints{
				AccessHints: map[string]AccessHint{
					"users": {Method: "IndexScan"},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.hints.HasAccessHints()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestHasJoinHints verifies the HasJoinHints method.
func TestHasJoinHints(t *testing.T) {
	tests := []struct {
		name     string
		hints    *OptimizationHints
		expected bool
	}{
		{
			name:     "nil hints",
			hints:    nil,
			expected: false,
		},
		{
			name:     "empty join hints",
			hints:    NewOptimizationHints(),
			expected: false,
		},
		{
			name: "has join hints",
			hints: &OptimizationHints{
				JoinHints: map[string]JoinHint{
					"join_0": {Method: "HashJoin"},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.hints.HasJoinHints()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// 2.6.5: createPhysicalPlan() Tests - Hint Checking for LogicalScan
// =============================================================================

// TestCreatePhysicalPlan_NoHints_CreatesPhysicalScan verifies that without hints,
// createPhysicalPlan creates a PhysicalScan for LogicalScan nodes.
func TestCreatePhysicalPlan_NoHints_CreatesPhysicalScan(t *testing.T) {
	// Setup catalog with a table
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create planner without hints
	p := NewPlanner(cat)

	// Create a LogicalScan
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	// Convert to physical plan
	physPlan, err := p.createPhysicalPlan(logicalScan)
	require.NoError(t, err)

	// Should be PhysicalScan, not PhysicalIndexScan
	scan, ok := physPlan.(*PhysicalScan)
	require.True(t, ok, "should create PhysicalScan without hints")
	assert.Equal(t, "main", scan.Schema)
	assert.Equal(t, "users", scan.TableName)
}

// TestCreatePhysicalPlan_SeqScanHint_CreatesPhysicalScan verifies that a SeqScan hint
// results in PhysicalScan (not PhysicalIndexScan).
func TestCreatePhysicalPlan_SeqScanHint_CreatesPhysicalScan(t *testing.T) {
	// Setup catalog with a table and index
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create index in catalog
	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	// Create planner with SeqScan hint
	p := NewPlanner(cat)
	hints := NewOptimizationHints()
	hints.AccessHints["users"] = AccessHint{Method: "SeqScan"}
	p.SetHints(hints)

	// Create a LogicalScan
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	// Convert to physical plan
	physPlan, err := p.createPhysicalPlan(logicalScan)
	require.NoError(t, err)

	// Should be PhysicalScan since hint says SeqScan
	_, ok := physPlan.(*PhysicalScan)
	require.True(t, ok, "SeqScan hint should create PhysicalScan")
}

// TestCreatePhysicalPlan_IndexScanHint_CreatesPhysicalIndexScan verifies that
// an IndexScan hint results in PhysicalIndexScan.
func TestCreatePhysicalPlan_IndexScanHint_CreatesPhysicalIndexScan(t *testing.T) {
	// Setup catalog with a table and index
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	// Create index in catalog
	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	// Create planner with IndexScan hint
	p := NewPlanner(cat)
	hints := NewOptimizationHints()
	hints.AccessHints["users"] = AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
	}
	p.SetHints(hints)

	// Create a LogicalScan
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	// Convert to physical plan
	physPlan, err := p.createPhysicalPlan(logicalScan)
	require.NoError(t, err)

	// Should be PhysicalIndexScan
	indexScan, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok, "IndexScan hint should create PhysicalIndexScan")
	assert.Equal(t, "idx_users_id", indexScan.IndexName)
	assert.Equal(t, "users", indexScan.TableName)
	assert.NotNil(t, indexScan.IndexDef)
}

// TestCreatePhysicalPlan_IndexScanHint_UseAliasFirst verifies that hints are checked
// by alias first, then by table name.
func TestCreatePhysicalPlan_IndexScanHint_UseAliasFirst(t *testing.T) {
	// Setup catalog
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	// Create planner with hint on alias "u"
	p := NewPlanner(cat)
	hints := NewOptimizationHints()
	hints.AccessHints["u"] = AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
	}
	p.SetHints(hints)

	// Create a LogicalScan with alias "u"
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "u",
		TableDef:  tableDef,
	}

	// Convert to physical plan
	physPlan, err := p.createPhysicalPlan(logicalScan)
	require.NoError(t, err)

	// Should be PhysicalIndexScan since alias matches
	_, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok, "should use hint matched by alias")
}

// TestCreatePhysicalPlan_IndexScanHint_FallbackToTableName verifies that hints
// fall back to table name when alias doesn't match.
func TestCreatePhysicalPlan_IndexScanHint_FallbackToTableName(t *testing.T) {
	// Setup catalog
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	// Create planner with hint on table name "users"
	p := NewPlanner(cat)
	hints := NewOptimizationHints()
	hints.AccessHints["users"] = AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
	}
	p.SetHints(hints)

	// Create a LogicalScan with different alias
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "different_alias",
		TableDef:  tableDef,
	}

	// Convert to physical plan
	physPlan, err := p.createPhysicalPlan(logicalScan)
	require.NoError(t, err)

	// Should be PhysicalIndexScan since table name matches
	_, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok, "should use hint matched by table name")
}

// =============================================================================
// 2.6.6: createPhysicalIndexScan() Unit Tests
// =============================================================================

// TestCreatePhysicalIndexScan_Success verifies successful creation of PhysicalIndexScan.
func TestCreatePhysicalIndexScan_Success(t *testing.T) {
	// Setup catalog
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("orders", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "customer_id", Type: dukdb.TYPE_INTEGER},
		{Name: "total", Type: dukdb.TYPE_DOUBLE},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	indexDef := catalog.NewIndexDef("idx_orders_customer", "main", "orders", []string{"customer_id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	// Create planner
	p := NewPlanner(cat)

	// Create LogicalScan
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "orders",
		Alias:     "o",
		TableDef:  tableDef,
	}

	// Create hint
	hint := &AccessHint{
		Method:         "IndexScan",
		IndexName:      "idx_orders_customer",
		Selectivity:    0.1,
		MatchedColumns: 1,
		IsFullMatch:    true,
	}

	// Call createPhysicalIndexScan
	physPlan, err := p.createPhysicalIndexScan(logicalScan, hint)
	require.NoError(t, err)

	// Verify result
	indexScan, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok)
	assert.Equal(t, "main", indexScan.Schema)
	assert.Equal(t, "orders", indexScan.TableName)
	assert.Equal(t, "o", indexScan.Alias)
	assert.Equal(t, "idx_orders_customer", indexScan.IndexName)
	assert.NotNil(t, indexScan.IndexDef)
	assert.Equal(t, tableDef, indexScan.TableDef)
}

// TestCreatePhysicalIndexScan_EmptyIndexName verifies error when index name is empty.
func TestCreatePhysicalIndexScan_EmptyIndexName(t *testing.T) {
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	p := NewPlanner(cat)

	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	hint := &AccessHint{
		Method:    "IndexScan",
		IndexName: "", // Empty!
	}

	_, err = p.createPhysicalIndexScan(logicalScan, hint)
	require.Error(t, err)
	// Verify error message includes context
	assert.Contains(t, err.Error(), "empty index name")
	assert.Contains(t, err.Error(), "users") // table name
	assert.Contains(t, err.Error(), "main")  // schema name
}

// TestCreatePhysicalIndexScan_IndexNotFound verifies error when index doesn't exist.
func TestCreatePhysicalIndexScan_IndexNotFound(t *testing.T) {
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	p := NewPlanner(cat)

	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	hint := &AccessHint{
		Method:    "IndexScan",
		IndexName: "nonexistent_index",
	}

	_, err = p.createPhysicalIndexScan(logicalScan, hint)
	require.Error(t, err)
	// Verify error message includes all context: index name, schema, table name
	errMsg := err.Error()
	assert.Contains(t, errMsg, "not found")
	assert.Contains(t, errMsg, "nonexistent_index") // index name
	assert.Contains(t, errMsg, "main")              // schema name
	assert.Contains(t, errMsg, "users")             // table name
	// Verify the error includes actionable guidance
	assert.Contains(t, errMsg, "CREATE INDEX")
}

// TestCreatePhysicalIndexScan_SchemaNotFound verifies error when schema doesn't exist.
// This tests that the error message is clear even when the schema itself doesn't exist.
func TestCreatePhysicalIndexScan_SchemaNotFound(t *testing.T) {
	cat := catalog.NewCatalog()
	// Note: We don't create the schema "nonexistent_schema"
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})

	p := NewPlanner(cat)

	logicalScan := &LogicalScan{
		Schema:    "nonexistent_schema",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	hint := &AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
	}

	_, err := p.createPhysicalIndexScan(logicalScan, hint)
	require.Error(t, err)
	// Even when schema doesn't exist, error should include helpful context
	errMsg := err.Error()
	assert.Contains(t, errMsg, "not found")
	assert.Contains(t, errMsg, "idx_users_id")       // index name
	assert.Contains(t, errMsg, "nonexistent_schema") // schema name
	assert.Contains(t, errMsg, "users")              // table name
}

// TestCreatePhysicalIndexScan_WrongTable verifies error when index is for a different table.
func TestCreatePhysicalIndexScan_WrongTable(t *testing.T) {
	cat := catalog.NewCatalog()

	// Create two tables
	usersTable := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err := cat.CreateTable(usersTable)
	require.NoError(t, err)

	ordersTable := catalog.NewTableDef("orders", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err = cat.CreateTable(ordersTable)
	require.NoError(t, err)

	// Create index on orders table
	indexDef := catalog.NewIndexDef("idx_orders_id", "main", "orders", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	p := NewPlanner(cat)

	// Try to use orders index on users table
	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  usersTable,
	}

	hint := &AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_orders_id", // Index is on orders, not users!
	}

	_, err = p.createPhysicalIndexScan(logicalScan, hint)
	require.Error(t, err)
	// Verify error message includes all context: index name, both table names, schema
	errMsg := err.Error()
	assert.Contains(t, errMsg, "idx_orders_id") // index name
	assert.Contains(t, errMsg, "orders")        // table index is defined on
	assert.Contains(t, errMsg, "users")         // table being scanned
	assert.Contains(t, errMsg, "main")          // schema name
}

// TestCreatePhysicalIndexScan_WithLookupKeys verifies that lookup keys are passed through.
func TestCreatePhysicalIndexScan_WithLookupKeys(t *testing.T) {
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	p := NewPlanner(cat)

	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	// Create a bound literal for lookup key
	lookupKey := &binder.BoundLiteral{Value: int64(42), ValType: dukdb.TYPE_INTEGER}

	hint := &AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
		LookupKeys: []any{lookupKey},
	}

	physPlan, err := p.createPhysicalIndexScan(logicalScan, hint)
	require.NoError(t, err)

	indexScan, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok, "should be PhysicalIndexScan")
	require.Len(t, indexScan.LookupKeys, 1)
	assert.Equal(t, lookupKey, indexScan.LookupKeys[0])
}

// TestCreatePhysicalIndexScan_WithResidualFilter verifies residual filter is passed through.
func TestCreatePhysicalIndexScan_WithResidualFilter(t *testing.T) {
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "status", Type: dukdb.TYPE_VARCHAR},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	p := NewPlanner(cat)

	logicalScan := &LogicalScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "users",
		TableDef:  tableDef,
	}

	// Create a residual filter (e.g., status = 'active')
	residual := &binder.BoundBinaryExpr{
		Left:    &binder.BoundColumnRef{Column: "status", ColType: dukdb.TYPE_VARCHAR},
		Right:   &binder.BoundLiteral{Value: "active", ValType: dukdb.TYPE_VARCHAR},
		Op:      5, // OpEq
		ResType: dukdb.TYPE_BOOLEAN,
	}

	hint := &AccessHint{
		Method:         "IndexScan",
		IndexName:      "idx_users_id",
		ResidualFilter: residual,
	}

	physPlan, err := p.createPhysicalIndexScan(logicalScan, hint)
	require.NoError(t, err)

	indexScan, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok, "should be PhysicalIndexScan")
	assert.Equal(t, residual, indexScan.ResidualFilter)
}

// TestCreatePhysicalIndexScan_WithProjections verifies projections are passed through.
func TestCreatePhysicalIndexScan_WithProjections(t *testing.T) {
	cat := catalog.NewCatalog()
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
		{Name: "email", Type: dukdb.TYPE_VARCHAR},
	})
	err := cat.CreateTable(tableDef)
	require.NoError(t, err)

	indexDef := catalog.NewIndexDef("idx_users_id", "main", "users", []string{"id"}, false)
	err = cat.CreateIndex(indexDef)
	require.NoError(t, err)

	p := NewPlanner(cat)

	// Only select id and name columns
	logicalScan := &LogicalScan{
		Schema:      "main",
		TableName:   "users",
		Alias:       "users",
		TableDef:    tableDef,
		Projections: []int{0, 1}, // id and name
	}

	hint := &AccessHint{
		Method:    "IndexScan",
		IndexName: "idx_users_id",
	}

	physPlan, err := p.createPhysicalIndexScan(logicalScan, hint)
	require.NoError(t, err)

	indexScan, ok := physPlan.(*PhysicalIndexScan)
	require.True(t, ok, "should be PhysicalIndexScan")
	assert.Equal(t, []int{0, 1}, indexScan.Projections)
}

// =============================================================================
// 2.6.7: isIndexOnlyScan() Unit Tests
// =============================================================================

// TestIsIndexOnlyScan_CoveringIndex verifies index-only scan detection.
func TestIsIndexOnlyScan_CoveringIndex(t *testing.T) {
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
		{Name: "email", Type: dukdb.TYPE_VARCHAR},
	})

	// Index on (id, name)
	indexDef := catalog.NewIndexDef("idx_users", "main", "users", []string{"id", "name"}, false)

	// Test 1: Projecting only indexed columns - should be covering
	result := isIndexOnlyScan(indexDef, []int{0, 1}, tableDef) // id and name
	assert.True(t, result, "should be index-only when all projected columns are in index")

	// Test 2: Projecting non-indexed column - should NOT be covering
	result = isIndexOnlyScan(indexDef, []int{0, 2}, tableDef) // id and email
	assert.False(t, result, "should not be index-only when projecting non-indexed column")

	// Test 3: SELECT * (nil projections) - should NOT be covering (email not in index)
	result = isIndexOnlyScan(indexDef, nil, tableDef)
	assert.False(t, result, "should not be index-only for SELECT * when index doesn't cover all columns")
}

// TestIsIndexOnlyScan_FullTableIndex verifies behavior when index covers all columns.
func TestIsIndexOnlyScan_FullTableIndex(t *testing.T) {
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
	})

	// Index on all columns
	indexDef := catalog.NewIndexDef("idx_all", "main", "users", []string{"id", "name"}, false)

	// SELECT * should be covering since index has all columns
	result := isIndexOnlyScan(indexDef, nil, tableDef)
	assert.True(t, result, "should be index-only when index covers all table columns")
}

// TestIsIndexOnlyScan_CaseInsensitive verifies case-insensitive column matching.
func TestIsIndexOnlyScan_CaseInsensitive(t *testing.T) {
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "ID", Type: dukdb.TYPE_INTEGER},
		{Name: "Name", Type: dukdb.TYPE_VARCHAR},
	})

	// Index with different case
	indexDef := catalog.NewIndexDef("idx", "main", "users", []string{"id", "name"}, false)

	result := isIndexOnlyScan(indexDef, []int{0, 1}, tableDef)
	assert.True(t, result, "column matching should be case-insensitive")
}

// =============================================================================
// 2.6.8: PhysicalIndexScan.OutputColumns() Tests
// =============================================================================

// TestPhysicalIndexScan_OutputColumns verifies output column generation.
func TestPhysicalIndexScan_OutputColumns(t *testing.T) {
	tableDef := catalog.NewTableDef("users", []*catalog.ColumnDef{
		{Name: "id", Type: dukdb.TYPE_INTEGER},
		{Name: "name", Type: dukdb.TYPE_VARCHAR},
		{Name: "email", Type: dukdb.TYPE_VARCHAR},
	})

	indexScan := &PhysicalIndexScan{
		Schema:    "main",
		TableName: "users",
		Alias:     "u",
		TableDef:  tableDef,
	}

	// Test 1: All columns (nil projections)
	cols := indexScan.OutputColumns()
	require.Len(t, cols, 3)
	assert.Equal(t, "u", cols[0].Table)
	assert.Equal(t, "id", cols[0].Column)
	assert.Equal(t, dukdb.TYPE_INTEGER, cols[0].Type)
	assert.Equal(t, "name", cols[1].Column)
	assert.Equal(t, "email", cols[2].Column)

	// Test 2: With projections
	indexScan2 := &PhysicalIndexScan{
		Schema:      "main",
		TableName:   "users",
		Alias:       "u",
		TableDef:    tableDef,
		Projections: []int{0, 2}, // id and email only
	}

	cols2 := indexScan2.OutputColumns()
	require.Len(t, cols2, 2)
	assert.Equal(t, "id", cols2[0].Column)
	assert.Equal(t, "email", cols2[1].Column)
}

// TestPhysicalIndexScan_Children verifies Children() returns nil.
func TestPhysicalIndexScan_Children(t *testing.T) {
	indexScan := &PhysicalIndexScan{}
	assert.Nil(t, indexScan.Children())
}

// =============================================================================
// 2.6.9: createPhysicalJoinFromHint() Tests
// =============================================================================

// TestCreatePhysicalJoinFromHint_HashJoin verifies HashJoin hint handling.
func TestCreatePhysicalJoinFromHint_HashJoin(t *testing.T) {
	cat := catalog.NewCatalog()
	p := NewPlanner(cat)

	// Create mock child plans
	left := &PhysicalDummyScan{}
	right := &PhysicalDummyScan{}

	// Create a join condition
	condition := &binder.BoundBinaryExpr{
		Left:    &binder.BoundColumnRef{Table: "t1", Column: "id"},
		Right:   &binder.BoundColumnRef{Table: "t2", Column: "id"},
		Op:      5, // OpEq
		ResType: dukdb.TYPE_BOOLEAN,
	}

	// Test HashJoin with right build side
	hint := JoinHint{Method: "HashJoin", BuildSide: "right"}
	result, err := p.createPhysicalJoinFromHint(left, right, JoinTypeInner, condition, hint)
	require.NoError(t, err)

	hashJoin, ok := result.(*PhysicalHashJoin)
	require.True(t, ok)
	assert.Equal(t, left, hashJoin.Left)
	assert.Equal(t, right, hashJoin.Right)

	// Test HashJoin with left build side (sides should swap)
	hint2 := JoinHint{Method: "HashJoin", BuildSide: "left"}
	result2, err := p.createPhysicalJoinFromHint(left, right, JoinTypeInner, condition, hint2)
	require.NoError(t, err)

	hashJoin2, ok := result2.(*PhysicalHashJoin)
	require.True(t, ok)
	// When build side is "left", sides are swapped
	assert.Equal(t, right, hashJoin2.Left)
	assert.Equal(t, left, hashJoin2.Right)
}

// TestCreatePhysicalJoinFromHint_NestedLoopJoin verifies NestedLoopJoin hint handling.
func TestCreatePhysicalJoinFromHint_NestedLoopJoin(t *testing.T) {
	cat := catalog.NewCatalog()
	p := NewPlanner(cat)

	left := &PhysicalDummyScan{}
	right := &PhysicalDummyScan{}

	hint := JoinHint{Method: "NestedLoopJoin"}
	result, err := p.createPhysicalJoinFromHint(left, right, JoinTypeLeft, nil, hint)
	require.NoError(t, err)

	nlj, ok := result.(*PhysicalNestedLoopJoin)
	require.True(t, ok)
	assert.Equal(t, JoinTypeLeft, nlj.JoinType)
}

// TestCreatePhysicalJoinFromHint_UnknownMethod verifies fallback for unknown method.
func TestCreatePhysicalJoinFromHint_UnknownMethod(t *testing.T) {
	cat := catalog.NewCatalog()
	p := NewPlanner(cat)

	left := &PhysicalDummyScan{}
	right := &PhysicalDummyScan{}

	// Equi-join condition should use HashJoin as fallback
	condition := &binder.BoundBinaryExpr{
		Left:    &binder.BoundColumnRef{Table: "t1", Column: "id"},
		Right:   &binder.BoundColumnRef{Table: "t2", Column: "id"},
		Op:      5,
		ResType: dukdb.TYPE_BOOLEAN,
	}

	hint := JoinHint{Method: "UnknownMethod"}
	result, err := p.createPhysicalJoinFromHint(left, right, JoinTypeInner, condition, hint)
	require.NoError(t, err)

	// Should fall back to HashJoin for equi-join
	_, ok := result.(*PhysicalHashJoin)
	assert.True(t, ok, "unknown method with equi-join should fall back to HashJoin")

	// Non-equi-join should use NestedLoopJoin
	result2, err := p.createPhysicalJoinFromHint(left, right, JoinTypeInner, nil, hint)
	require.NoError(t, err)
	_, ok2 := result2.(*PhysicalNestedLoopJoin)
	assert.True(t, ok2, "unknown method with non-equi-join should fall back to NestedLoopJoin")
}

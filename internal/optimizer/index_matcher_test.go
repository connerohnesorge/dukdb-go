package optimizer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockIndexDef implements IndexDef for testing.
type mockIndexDef struct {
	name     string
	table    string
	columns  []string
	isUnique bool
}

func (m *mockIndexDef) GetName() string      { return m.name }
func (m *mockIndexDef) GetTable() string     { return m.table }
func (m *mockIndexDef) GetColumns() []string { return m.columns }
func (m *mockIndexDef) GetIsUnique() bool    { return m.isUnique }

// mockIndexCatalog implements CatalogProvider with index support.
type mockIndexCatalog struct {
	tables  map[string]*mockTableInfo
	indexes map[string][]IndexDef
}

func newMockIndexCatalog() *mockIndexCatalog {
	return &mockIndexCatalog{
		tables:  make(map[string]*mockTableInfo),
		indexes: make(map[string][]IndexDef),
	}
}

func (c *mockIndexCatalog) GetTableInfo(schema, table string) TableInfo {
	key := schema + "." + table
	if info, ok := c.tables[key]; ok {
		return info
	}
	return nil
}

func (c *mockIndexCatalog) GetIndexesForTableAsInterface(schema, table string) []IndexDef {
	key := schema + "." + table
	return c.indexes[key]
}

func (c *mockIndexCatalog) AddTable(schema, table string, info *mockTableInfo) {
	key := schema + "." + table
	c.tables[key] = info
}

func (c *mockIndexCatalog) AddIndex(schema, table string, idx IndexDef) {
	key := schema + "." + table
	c.indexes[key] = append(c.indexes[key], idx)
}

// mockPredicate implements PredicateExpr for testing.
type mockPredicate struct {
	predType string
}

func (m *mockPredicate) PredicateType() string { return m.predType }

// mockBinaryPredicate implements BinaryPredicateExpr for testing.
type mockBinaryPredicate struct {
	left  PredicateExpr
	right PredicateExpr
	op    BinaryOp
}

func (m *mockBinaryPredicate) PredicateType() string         { return "BinaryPredicate" }
func (m *mockBinaryPredicate) PredicateLeft() PredicateExpr  { return m.left }
func (m *mockBinaryPredicate) PredicateRight() PredicateExpr { return m.right }
func (m *mockBinaryPredicate) PredicateOperator() BinaryOp   { return m.op }

// mockColumnRefPredicate implements ColumnRefPredicateExpr for testing.
type mockColumnRefPredicate struct {
	table  string
	column string
}

func (m *mockColumnRefPredicate) PredicateType() string   { return "ColumnRef" }
func (m *mockColumnRefPredicate) PredicateTable() string  { return m.table }
func (m *mockColumnRefPredicate) PredicateColumn() string { return m.column }

// mockLiteralPredicate represents a literal value in a predicate.
type mockLiteralPredicate struct {
	value interface{}
}

func (m *mockLiteralPredicate) PredicateType() string { return "Literal" }

// mockInListPredicate implements InListPredicateExpr for testing.
type mockInListPredicate struct {
	expr   PredicateExpr
	values []PredicateExpr
	not    bool
}

func (m *mockInListPredicate) PredicateType() string            { return "InList" }
func (m *mockInListPredicate) PredicateInExpr() PredicateExpr   { return m.expr }
func (m *mockInListPredicate) PredicateValues() []PredicateExpr { return m.values }
func (m *mockInListPredicate) PredicateIsNot() bool             { return m.not }

func TestIndexMatcher_NilCatalog(t *testing.T) {
	matcher := NewIndexMatcher(nil)

	matches := matcher.FindApplicableIndexes("main", "users", nil)
	assert.Nil(t, matches)
}

func TestIndexMatcher_NoIndexes(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})

	matcher := NewIndexMatcher(catalog)

	// No indexes defined
	matches := matcher.FindApplicableIndexes("main", "users", nil)
	assert.Nil(t, matches)
}

func TestIndexMatcher_SingleColumnIndex_NoMatchingPredicate(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_email",
		table:    "users",
		columns:  []string{"email"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// Predicate on different column (name instead of email)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "name"},
			right: &mockLiteralPredicate{value: "John"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	assert.Empty(t, matches)
}

func TestIndexMatcher_SingleColumnIndex_MatchingPredicate(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_email",
		table:    "users",
		columns:  []string{"email"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// Predicate on the indexed column
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "email"},
			right: &mockLiteralPredicate{value: "test@example.com"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_users_email", match.Index.GetName())
	assert.Len(t, match.Predicates, 1)
	assert.Len(t, match.LookupKeys, 1)
	assert.Equal(t, 1, match.MatchedColumns)
	assert.True(t, match.IsFullMatch)
	assert.Equal(t, 0.01, match.Selectivity) // Unique index
}

func TestIndexMatcher_SingleColumnIndex_NonUnique(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_status",
		table:    "users",
		columns:  []string{"status"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "active"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(
		t,
		DefaultEqualitySelectivity,
		match.Selectivity,
	) // Non-unique: default selectivity
}

func TestIndexMatcher_SingleColumnIndex_ReversedOperands(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// Column on right side: 42 = id
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockLiteralPredicate{value: 42},
			right: &mockColumnRefPredicate{table: "users", column: "id"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_users_id", match.Index.GetName())
	// LookupKey should be the literal (left side)
	_, isLiteral := match.LookupKeys[0].(*mockLiteralPredicate)
	assert.True(t, isLiteral)
}

func TestIndexMatcher_SingleColumnIndex_CaseInsensitive(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_email",
		table:    "users",
		columns:  []string{"email"}, // lowercase
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// Column name with different case
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "USERS", column: "EMAIL"}, // uppercase
			right: &mockLiteralPredicate{value: "test@example.com"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)
}

func TestIndexMatcher_SingleColumnIndex_UnqualifiedColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// Column without table qualifier
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "", column: "id"}, // No table
			right: &mockLiteralPredicate{value: 42},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)
}

func TestIndexMatcher_MultipleIndexes_SelectsBest(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})

	// Add unique index (more selective)
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_email",
		table:    "users",
		columns:  []string{"email"},
		isUnique: true,
	})

	// Add non-unique index (less selective)
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_status",
		table:    "users",
		columns:  []string{"status"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Predicates on both columns
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "email"},
			right: &mockLiteralPredicate{value: "test@example.com"},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "active"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 2)

	// Should be sorted by selectivity (unique index first)
	assert.Equal(t, "idx_users_email", matches[0].Index.GetName())
	assert.Equal(t, "idx_users_status", matches[1].Index.GetName())
}

func TestIndexMatcher_CompositeIndex_FullMatch(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_customer_date",
		table:    "orders",
		columns:  []string{"customer_id", "order_date"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Predicates on both columns of the composite index
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "customer_id"},
			right: &mockLiteralPredicate{value: 123},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "order_date"},
			right: &mockLiteralPredicate{value: "2024-01-01"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, 2, match.MatchedColumns)
	assert.True(t, match.IsFullMatch)
	assert.Len(t, match.Predicates, 2)
	assert.Len(t, match.LookupKeys, 2)
	// Selectivity: 0.1 * 0.1 = 0.01
	assert.InDelta(t, 0.01, match.Selectivity, 0.001)
}

func TestIndexMatcher_CompositeIndex_PartialMatch(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_customer_date",
		table:    "orders",
		columns:  []string{"customer_id", "order_date"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Only predicate on first column
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "customer_id"},
			right: &mockLiteralPredicate{value: 123},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, 1, match.MatchedColumns)
	assert.False(t, match.IsFullMatch)
	assert.Len(t, match.Predicates, 1)
}

func TestIndexMatcher_CompositeIndex_NoFirstColumnMatch(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_customer_date",
		table:    "orders",
		columns:  []string{"customer_id", "order_date"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Only predicate on second column (can't use index)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "order_date"},
			right: &mockLiteralPredicate{value: "2024-01-01"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	assert.Empty(t, matches) // Can't use index without first column
}

func TestIndexMatcher_NonEqualityPredicate(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_age",
		table:    "users",
		columns:  []string{"age"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Non-equality predicate (greater than) - now supports range scans
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt, // Range predicate
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	// With range scan support, we now get a match
	require.Len(t, matches, 1)
	assert.Equal(t, "idx_users_age", matches[0].Index.GetName())
	assert.True(t, matches[0].IsRangeScan)
	assert.False(t, matches[0].IsFullMatch) // Range scans are not full matches
}

func TestIndexMatcher_EmptyColumns(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_empty",
		table:    "users",
		columns:  []string{}, // Empty columns (invalid index)
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "id"},
			right: &mockLiteralPredicate{value: 42},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	assert.Empty(t, matches)
}

func TestIndexMatcher_SelectivityEstimation(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	matcher := NewIndexMatcher(catalog)

	tests := []struct {
		name       string
		numPreds   int
		isUnique   bool
		wantSelect float64
	}{
		{"unique index", 1, true, 0.01},
		{"non-unique 1 pred", 1, false, 0.1},
		{"non-unique 2 preds", 2, false, 0.01},
		{"non-unique 3 preds", 3, false, 0.001},
		{"non-unique many preds clamped", 10, false, 0.001}, // Clamped to min
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preds := make([]PredicateExpr, tt.numPreds)
			for i := range preds {
				preds[i] = &mockPredicate{}
			}

			sel := matcher.estimateSelectivity(preds, tt.isUnique)
			assert.InDelta(t, tt.wantSelect, sel, 0.0001)
		})
	}
}

func TestSortMatchesBySelectivity(t *testing.T) {
	matches := []IndexMatch{
		{Index: &mockIndexDef{name: "idx3"}, Selectivity: 0.5},
		{Index: &mockIndexDef{name: "idx1"}, Selectivity: 0.1},
		{Index: &mockIndexDef{name: "idx2"}, Selectivity: 0.3},
	}

	sortMatchesBySelectivity(matches)

	assert.Equal(t, "idx1", matches[0].Index.GetName())
	assert.Equal(t, "idx2", matches[1].Index.GetName())
	assert.Equal(t, "idx3", matches[2].Index.GetName())
}

// --- IN Clause Tests ---

func TestIndexMatcher_InClause_MatchingColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause on the indexed column: id IN (1, 2, 3)
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "users", column: "id"},
			values: []PredicateExpr{
				&mockLiteralPredicate{value: 1},
				&mockLiteralPredicate{value: 2},
				&mockLiteralPredicate{value: 3},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_users_id", match.Index.GetName())
	assert.Len(t, match.Predicates, 1)
	// Should have 3 lookup keys for the 3 values in the IN list
	assert.Len(t, match.LookupKeys, 3)
	assert.Equal(t, 1, match.MatchedColumns)
	assert.True(t, match.IsFullMatch)
}

func TestIndexMatcher_InClause_MultipleLookupKeys(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "products", &mockTableInfo{})
	catalog.AddIndex("main", "products", &mockIndexDef{
		name:     "idx_products_category",
		table:    "products",
		columns:  []string{"category_id"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause with 5 values
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "products", column: "category_id"},
			values: []PredicateExpr{
				&mockLiteralPredicate{value: 10},
				&mockLiteralPredicate{value: 20},
				&mockLiteralPredicate{value: 30},
				&mockLiteralPredicate{value: 40},
				&mockLiteralPredicate{value: 50},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "products", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	// Should have 5 lookup keys
	assert.Len(t, match.LookupKeys, 5)

	// Verify each key is present
	for i, key := range match.LookupKeys {
		lit, ok := key.(*mockLiteralPredicate)
		require.True(t, ok, "LookupKey[%d] should be a literal", i)
		assert.Equal(t, (i+1)*10, lit.value)
	}
}

func TestIndexMatcher_InClause_NonIndexedColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_email",
		table:    "users",
		columns:  []string{"email"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause on a non-indexed column (status instead of email)
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "users", column: "status"},
			values: []PredicateExpr{
				&mockLiteralPredicate{value: "active"},
				&mockLiteralPredicate{value: "pending"},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	// No match since IN clause is on a different column than the index
	assert.Empty(t, matches)
}

func TestIndexMatcher_InClause_NotIn_NoMatch(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// NOT IN clause - should not match because NOT IN cannot be efficiently evaluated
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "users", column: "id"},
			values: []PredicateExpr{
				&mockLiteralPredicate{value: 1},
				&mockLiteralPredicate{value: 2},
			},
			not: true, // This is NOT IN
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	// NOT IN cannot use index efficiently
	assert.Empty(t, matches)
}

func TestIndexMatcher_InClause_EmptyList(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause with empty list
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr:   &mockColumnRefPredicate{table: "users", column: "id"},
			values: []PredicateExpr{}, // Empty list
			not:    false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	// Empty IN list should still match the index, but with 0 lookup keys
	require.Len(t, matches, 1)
	assert.Len(t, matches[0].LookupKeys, 0)
	assert.Equal(t, 0.0, matches[0].Selectivity) // No values means 0 selectivity
}

func TestIndexMatcher_InClause_SelectivityEstimation(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	matcher := NewIndexMatcher(catalog)

	tests := []struct {
		name       string
		numValues  int
		isUnique   bool
		wantSelect float64
	}{
		{"unique 1 value", 1, true, 0.01},
		{"unique 3 values", 3, true, 0.03},
		{"unique 10 values", 10, true, 0.10},
		{"non-unique 1 value", 1, false, 0.1},
		{"non-unique 3 values", 3, false, 0.3},
		{"non-unique 10 values", 10, false, 1.0}, // Clamped to 1.0
		{"non-unique 20 values", 20, false, 1.0}, // Clamped to 1.0
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := matcher.estimateInListSelectivity(tt.numValues, tt.isUnique)
			assert.InDelta(t, tt.wantSelect, sel, 0.0001)
		})
	}
}

func TestIndexMatcher_InClause_CaseInsensitive(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_status",
		table:    "users",
		columns:  []string{"status"}, // lowercase
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause with different case column name
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "USERS", column: "STATUS"}, // uppercase
			values: []PredicateExpr{
				&mockLiteralPredicate{value: "active"},
				&mockLiteralPredicate{value: "pending"},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)
	assert.Equal(t, "idx_users_status", matches[0].Index.GetName())
}

func TestIndexMatcher_InClause_UnqualifiedColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_role",
		table:    "users",
		columns:  []string{"role"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause without table qualifier
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "", column: "role"}, // No table qualifier
			values: []PredicateExpr{
				&mockLiteralPredicate{value: "admin"},
				&mockLiteralPredicate{value: "user"},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)
}

func TestIndexMatcher_InClause_PreferEqualityOverIn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// Both an equality predicate and an IN clause on the same column
	// Equality should be preferred
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "id"},
			right: &mockLiteralPredicate{value: 42},
			op:    OpEq,
		},
		&mockInListPredicate{
			expr: &mockColumnRefPredicate{table: "users", column: "id"},
			values: []PredicateExpr{
				&mockLiteralPredicate{value: 1},
				&mockLiteralPredicate{value: 2},
				&mockLiteralPredicate{value: 3},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	// Should use equality predicate, so only 1 lookup key
	assert.Len(t, match.LookupKeys, 1)
	lit, ok := match.LookupKeys[0].(*mockLiteralPredicate)
	require.True(t, ok)
	assert.Equal(t, 42, lit.value) // The value from equality, not IN list
}

func TestIndexMatcher_InClause_NonColumnExpr_NoMatch(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause where the expression is not a column reference
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr: &mockLiteralPredicate{value: 5}, // Not a column ref
			values: []PredicateExpr{
				&mockLiteralPredicate{value: 1},
				&mockLiteralPredicate{value: 2},
			},
			not: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	// No match since the IN expression is not a column reference
	assert.Empty(t, matches)
}

// --- IsCoveringIndex Tests ---

func TestIsCoveringIndex_NilIndex(t *testing.T) {
	result := IsCoveringIndex(nil, []string{"col1"})
	assert.False(t, result)
}

func TestIsCoveringIndex_EmptyRequiredColumns(t *testing.T) {
	idx := &mockIndexDef{
		name:    "idx_test",
		columns: []string{"col1", "col2"},
	}

	result := IsCoveringIndex(idx, []string{})
	assert.True(t, result)

	result = IsCoveringIndex(idx, nil)
	assert.True(t, result)
}

func TestIsCoveringIndex_SingleColumnMatch(t *testing.T) {
	idx := &mockIndexDef{
		name:    "idx_users_id",
		columns: []string{"id"},
	}

	// Single column that's in the index
	result := IsCoveringIndex(idx, []string{"id"})
	assert.True(t, result)

	// Single column not in the index
	result = IsCoveringIndex(idx, []string{"name"})
	assert.False(t, result)
}

func TestIsCoveringIndex_CompositeIndexFullCoverage(t *testing.T) {
	idx := &mockIndexDef{
		name:    "idx_orders",
		columns: []string{"customer_id", "order_date", "status"},
	}

	// All columns are in the index
	result := IsCoveringIndex(idx, []string{"customer_id", "order_date"})
	assert.True(t, result)

	// All three columns
	result = IsCoveringIndex(idx, []string{"customer_id", "order_date", "status"})
	assert.True(t, result)

	// Just one column
	result = IsCoveringIndex(idx, []string{"status"})
	assert.True(t, result)
}

func TestIsCoveringIndex_CompositeIndexPartialCoverage(t *testing.T) {
	idx := &mockIndexDef{
		name:    "idx_orders",
		columns: []string{"customer_id", "order_date"},
	}

	// One column not in index
	result := IsCoveringIndex(idx, []string{"customer_id", "total_amount"})
	assert.False(t, result)

	// Multiple columns, some not in index
	result = IsCoveringIndex(idx, []string{"customer_id", "order_date", "status"})
	assert.False(t, result)
}

func TestIsCoveringIndex_CaseInsensitive(t *testing.T) {
	idx := &mockIndexDef{
		name:    "idx_test",
		columns: []string{"CustomerID", "OrderDate"},
	}

	// Lowercase required columns
	result := IsCoveringIndex(idx, []string{"customerid", "orderdate"})
	assert.True(t, result)

	// Mixed case
	result = IsCoveringIndex(idx, []string{"CUSTOMERID", "OrderDate"})
	assert.True(t, result)
}

// --- GetRequiredColumns Tests ---

func TestGetRequiredColumns_EmptyInputs(t *testing.T) {
	result := GetRequiredColumns(nil, nil)
	assert.Empty(t, result)

	result = GetRequiredColumns([]string{}, []string{})
	assert.Empty(t, result)
}

func TestGetRequiredColumns_OnlyProjections(t *testing.T) {
	result := GetRequiredColumns([]string{"id", "name", "email"}, nil)
	assert.Len(t, result, 3)
	assert.Contains(t, result, "id")
	assert.Contains(t, result, "name")
	assert.Contains(t, result, "email")
}

func TestGetRequiredColumns_OnlyFilterColumns(t *testing.T) {
	result := GetRequiredColumns(nil, []string{"status", "created_at"})
	assert.Len(t, result, 2)
	assert.Contains(t, result, "status")
	assert.Contains(t, result, "created_at")
}

func TestGetRequiredColumns_BothProjectionsAndFilter(t *testing.T) {
	result := GetRequiredColumns(
		[]string{"id", "name"},
		[]string{"status", "created_at"},
	)
	assert.Len(t, result, 4)
	assert.Contains(t, result, "id")
	assert.Contains(t, result, "name")
	assert.Contains(t, result, "status")
	assert.Contains(t, result, "created_at")
}

func TestGetRequiredColumns_Deduplication(t *testing.T) {
	// Same column in both projections and filter
	result := GetRequiredColumns(
		[]string{"id", "name", "status"},
		[]string{"status", "created_at"},
	)
	assert.Len(t, result, 4) // "status" should only appear once
	assert.Contains(t, result, "id")
	assert.Contains(t, result, "name")
	assert.Contains(t, result, "status")
	assert.Contains(t, result, "created_at")

	// Duplicate in projections
	result = GetRequiredColumns(
		[]string{"id", "name", "id"},
		nil,
	)
	assert.Len(t, result, 2)
}

func TestGetRequiredColumns_CaseInsensitiveDeduplication(t *testing.T) {
	// Same column with different cases
	result := GetRequiredColumns(
		[]string{"ID", "name"},
		[]string{"id", "NAME"},
	)
	// Should deduplicate case-insensitively
	assert.Len(t, result, 2)
}

// --- ExtractColumnRefsFromPredicate Tests ---

func TestExtractColumnRefsFromPredicate_Nil(t *testing.T) {
	result := ExtractColumnRefsFromPredicate(nil)
	assert.Nil(t, result)
}

func TestExtractColumnRefsFromPredicate_ColumnRef(t *testing.T) {
	pred := &mockColumnRefPredicate{table: "users", column: "id"}
	result := ExtractColumnRefsFromPredicate(pred)
	assert.Len(t, result, 1)
	assert.Equal(t, "id", result[0])
}

func TestExtractColumnRefsFromPredicate_BinaryPredicate(t *testing.T) {
	// id = 42
	pred := &mockBinaryPredicate{
		left:  &mockColumnRefPredicate{table: "users", column: "id"},
		right: &mockLiteralPredicate{value: 42},
		op:    OpEq,
	}
	result := ExtractColumnRefsFromPredicate(pred)
	assert.Len(t, result, 1)
	assert.Equal(t, "id", result[0])
}

func TestExtractColumnRefsFromPredicate_BinaryPredicateBothColumns(t *testing.T) {
	// t1.id = t2.id
	pred := &mockBinaryPredicate{
		left:  &mockColumnRefPredicate{table: "t1", column: "id"},
		right: &mockColumnRefPredicate{table: "t2", column: "foreign_id"},
		op:    OpEq,
	}
	result := ExtractColumnRefsFromPredicate(pred)
	assert.Len(t, result, 2)
	assert.Contains(t, result, "id")
	assert.Contains(t, result, "foreign_id")
}

func TestExtractColumnRefsFromPredicate_InListPredicate(t *testing.T) {
	pred := &mockInListPredicate{
		expr: &mockColumnRefPredicate{table: "users", column: "status"},
		values: []PredicateExpr{
			&mockLiteralPredicate{value: "active"},
			&mockLiteralPredicate{value: "pending"},
		},
		not: false,
	}
	result := ExtractColumnRefsFromPredicate(pred)
	assert.Len(t, result, 1)
	assert.Equal(t, "status", result[0])
}

func TestExtractColumnRefsFromPredicate_LiteralOnly(t *testing.T) {
	pred := &mockLiteralPredicate{value: 42}
	result := ExtractColumnRefsFromPredicate(pred)
	assert.Empty(t, result)
}

// --- ExtractColumnsFromPredicates Tests ---

func TestExtractColumnsFromPredicates_Empty(t *testing.T) {
	result := ExtractColumnsFromPredicates(nil)
	assert.Empty(t, result)

	result = ExtractColumnsFromPredicates([]PredicateExpr{})
	assert.Empty(t, result)
}

func TestExtractColumnsFromPredicates_SinglePredicate(t *testing.T) {
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "active"},
			op:    OpEq,
		},
	}
	result := ExtractColumnsFromPredicates(predicates)
	assert.Len(t, result, 1)
	assert.Equal(t, "status", result[0])
}

func TestExtractColumnsFromPredicates_MultiplePredicates(t *testing.T) {
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "active"},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGe,
		},
	}
	result := ExtractColumnsFromPredicates(predicates)
	assert.Len(t, result, 2)
	assert.Contains(t, result, "status")
	assert.Contains(t, result, "age")
}

func TestExtractColumnsFromPredicates_Deduplication(t *testing.T) {
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "active"},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "pending"},
			op:    OpEq,
		},
	}
	result := ExtractColumnsFromPredicates(predicates)
	assert.Len(t, result, 1) // Only one "status"
	assert.Equal(t, "status", result[0])
}

func TestExtractColumnsFromPredicates_CaseInsensitiveDeduplication(t *testing.T) {
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "Status"},
			right: &mockLiteralPredicate{value: "active"},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "status"},
			right: &mockLiteralPredicate{value: "pending"},
			op:    OpEq,
		},
	}
	result := ExtractColumnsFromPredicates(predicates)
	assert.Len(t, result, 1) // Deduplicated case-insensitively
}

// --- Partial Composite Index Match Tests (Residual Filter Scenarios) ---

// TestIndexMatcher_CompositeIndex_PartialMatchWithGap tests that when a composite
// index is on (a, b, c) and the query has a = 1 AND c = 3 (missing b), only the
// first column 'a' can be used for the index lookup. The 'c = 3' predicate must
// become a residual filter applied after the index scan.
func TestIndexMatcher_CompositeIndex_PartialMatchWithGap(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Query: WHERE a = 1 AND c = 3 (skipping column b in the middle)
	// Only 'a = 1' can use the index because composite indexes require
	// contiguous prefix matches. The 'c = 3' must be a residual filter.
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "c"},
			right: &mockLiteralPredicate{value: 3},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1, "Should still find the index for partial match")

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())

	// Key assertion: Only 1 column matched because b is missing
	assert.Equal(t, 1, match.MatchedColumns, "Only 'a' should be matched (prefix)")

	// Not a full match since b and c are not matched
	assert.False(t, match.IsFullMatch, "Not a full match when middle column is missing")

	// Only the 'a = 1' predicate is used in the index lookup
	assert.Len(t, match.Predicates, 1, "Only one predicate should be matched")
	assert.Len(t, match.LookupKeys, 1, "Only one lookup key for column a")

	// Verify the matched predicate is for column 'a'
	binPred, ok := match.Predicates[0].(BinaryPredicateExpr)
	require.True(t, ok)
	colRef, ok := binPred.PredicateLeft().(ColumnRefPredicateExpr)
	require.True(t, ok)
	assert.Equal(t, "a", colRef.PredicateColumn())

	// The 'c = 3' predicate is NOT in match.Predicates - it becomes a residual filter
	// that must be applied after the index scan fetches rows.
}

// TestIndexMatcher_CompositeIndex_PartialMatchSkippingFirstColumn tests that
// if the query only has predicates on non-leading columns, the index cannot be used.
func TestIndexMatcher_CompositeIndex_PartialMatchSkippingFirstColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Query: WHERE b = 2 AND c = 3 (missing first column 'a')
	// Index cannot be used at all because first column is required
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 2},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "c"},
			right: &mockLiteralPredicate{value: 3},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	assert.Empty(t, matches, "Index should not be usable without predicate on first column")
}

// TestIndexMatcher_CompositeIndex_PartialMatchTwoOfThree tests that when the index
// is on (a, b, c) and query has a = 1 AND b = 2, but not c, the index is a partial
// match with 2 columns.
func TestIndexMatcher_CompositeIndex_PartialMatchTwoOfThree(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Query: WHERE a = 1 AND b = 2 (missing c)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 2},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, 2, match.MatchedColumns, "Two columns (a, b) should be matched")
	assert.False(t, match.IsFullMatch, "Not a full match since c is missing")
	assert.Len(t, match.Predicates, 2, "Both predicates should be matched")
	assert.Len(t, match.LookupKeys, 2, "Two lookup keys for a and b")
}

// TestIndexMatcher_CompositeIndex_PartialMatchWithExtraPredicates tests the scenario
// where some predicates match the index and others don't (residual predicates).
func TestIndexMatcher_CompositeIndex_PartialMatchWithExtraPredicates(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})

	// Composite index on (customer_id, order_date)
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_cust_date",
		table:    "orders",
		columns:  []string{"customer_id", "order_date"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Query: WHERE customer_id = 100 AND status = 'shipped'
	// Only customer_id matches the index prefix, status must be residual
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "customer_id"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "status"},
			right: &mockLiteralPredicate{value: "shipped"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, 1, match.MatchedColumns, "Only customer_id should be matched")
	assert.False(t, match.IsFullMatch)

	// Only one predicate is used by the index
	assert.Len(t, match.Predicates, 1)

	// Verify it's the customer_id predicate
	binPred := match.Predicates[0].(BinaryPredicateExpr)
	colRef := binPred.PredicateLeft().(ColumnRefPredicateExpr)
	assert.Equal(t, "customer_id", colRef.PredicateColumn())

	// The 'status = shipped' predicate must be applied as a residual filter
	// after fetching rows from the index scan
}

// TestIndexMatcher_CompositeIndex_ResidualFilterDetection demonstrates how to identify
// which predicates become residual filters after index matching.
func TestIndexMatcher_CompositeIndex_ResidualFilterDetection(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "events", &mockTableInfo{})

	// Composite index on (user_id, event_type)
	catalog.AddIndex("main", "events", &mockIndexDef{
		name:     "idx_events_user_type",
		table:    "events",
		columns:  []string{"user_id", "event_type"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Query: WHERE user_id = 5 AND timestamp > '2024-01-01' AND event_type = 'login'
	// All three predicates in the query
	allPredicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "events", column: "user_id"},
			right: &mockLiteralPredicate{value: 5},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "events", column: "timestamp"},
			right: &mockLiteralPredicate{value: "2024-01-01"},
			op:    OpGt, // Not equality - can't use index for this
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "events", column: "event_type"},
			right: &mockLiteralPredicate{value: "login"},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "events", allPredicates)
	require.Len(t, matches, 1)

	match := matches[0]

	// Full match on both user_id and event_type (both equality, contiguous)
	assert.Equal(t, 2, match.MatchedColumns)
	assert.True(t, match.IsFullMatch)
	assert.Len(t, match.Predicates, 2)

	// The 'timestamp > ...' predicate is NOT matched by the index
	// We can compute residual predicates as: allPredicates - match.Predicates
	matchedPredicateSet := make(map[PredicateExpr]bool)
	for _, p := range match.Predicates {
		matchedPredicateSet[p] = true
	}

	var residualPredicates []PredicateExpr
	for _, p := range allPredicates {
		if !matchedPredicateSet[p] {
			residualPredicates = append(residualPredicates, p)
		}
	}

	// The timestamp predicate should be in residuals
	require.Len(t, residualPredicates, 1, "One predicate should become residual")
	binPred := residualPredicates[0].(BinaryPredicateExpr)
	colRef := binPred.PredicateLeft().(ColumnRefPredicateExpr)
	assert.Equal(t, "timestamp", colRef.PredicateColumn())
}

// --- Integration Test: Covering Index Detection ---

func TestIsCoveringIndex_IntegrationWithMatcher(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_composite",
		table:    "orders",
		columns:  []string{"customer_id", "order_date", "total"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Predicate: customer_id = 123
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "customer_id"},
			right: &mockLiteralPredicate{value: 123},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	require.Len(t, matches, 1)

	// Check if index covers different column sets
	// Query: SELECT customer_id, order_date FROM orders WHERE customer_id = 123
	// Required columns: customer_id, order_date (both in index)
	requiredCols := GetRequiredColumns(
		[]string{"customer_id", "order_date"},
		ExtractColumnsFromPredicates(predicates),
	)
	assert.True(t, IsCoveringIndex(matches[0].Index, requiredCols))

	// Query: SELECT customer_id, status FROM orders WHERE customer_id = 123
	// Required columns: customer_id, status (status NOT in index)
	requiredCols = GetRequiredColumns(
		[]string{"customer_id", "status"},
		ExtractColumnsFromPredicates(predicates),
	)
	assert.False(t, IsCoveringIndex(matches[0].Index, requiredCols))
}

// --- mockBetweenPredicate for testing BETWEEN expressions ---

type mockBetweenPredicate struct {
	expr       PredicateExpr
	low        PredicateExpr
	high       PredicateExpr
	notBetween bool
}

func (m *mockBetweenPredicate) PredicateType() string               { return "Between" }
func (m *mockBetweenPredicate) PredicateBetweenExpr() PredicateExpr { return m.expr }
func (m *mockBetweenPredicate) PredicateLowBound() PredicateExpr    { return m.low }
func (m *mockBetweenPredicate) PredicateHighBound() PredicateExpr   { return m.high }
func (m *mockBetweenPredicate) PredicateIsNotBetween() bool         { return m.notBetween }

// --- findRangePredicates Tests ---

func TestFindRangePredicates_Empty(t *testing.T) {
	result := findRangePredicates(nil, "age")
	assert.Empty(t, result)

	result = findRangePredicates([]PredicateExpr{}, "age")
	assert.Empty(t, result)
}

func TestFindRangePredicates_LessThan(t *testing.T) {
	// age < 30
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 30},
			op:    OpLt,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, "age", result[0].Column)
	assert.Equal(t, "users", result[0].Table)
	assert.Equal(t, RangeOpLessThan, result[0].Op)
	assert.NotNil(t, result[0].Value)
	assert.NotNil(t, result[0].OriginalPredicate)
}

func TestFindRangePredicates_LessThanOrEqual(t *testing.T) {
	// age <= 30
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 30},
			op:    OpLe,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, RangeOpLessThanOrEqual, result[0].Op)
}

func TestFindRangePredicates_GreaterThan(t *testing.T) {
	// age > 18
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, RangeOpGreaterThan, result[0].Op)
}

func TestFindRangePredicates_GreaterThanOrEqual(t *testing.T) {
	// age >= 18
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGe,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, RangeOpGreaterThanOrEqual, result[0].Op)
}

func TestFindRangePredicates_ReversedOperands(t *testing.T) {
	// 18 < age (equivalent to age > 18)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockLiteralPredicate{value: 18},
			right: &mockColumnRefPredicate{table: "users", column: "age"},
			op:    OpLt,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	// The operator should be flipped: 18 < age means age > 18
	assert.Equal(t, RangeOpGreaterThan, result[0].Op)
	assert.Equal(t, "age", result[0].Column)
}

func TestFindRangePredicates_ReversedOperandsAllOps(t *testing.T) {
	tests := []struct {
		name       string
		op         BinaryOp
		expectedOp RangeOp
	}{
		{"literal < col -> col > literal", OpLt, RangeOpGreaterThan},
		{"literal <= col -> col >= literal", OpLe, RangeOpGreaterThanOrEqual},
		{"literal > col -> col < literal", OpGt, RangeOpLessThan},
		{"literal >= col -> col <= literal", OpGe, RangeOpLessThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicates := []PredicateExpr{
				&mockBinaryPredicate{
					left:  &mockLiteralPredicate{value: 100},
					right: &mockColumnRefPredicate{table: "t", column: "x"},
					op:    tt.op,
				},
			}

			result := findRangePredicates(predicates, "x")
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedOp, result[0].Op)
		})
	}
}

func TestFindRangePredicates_MultiplePredicates(t *testing.T) {
	// age > 18 AND age < 65 (typical range query)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 65},
			op:    OpLt,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2)

	// Check we have both a lower bound (>) and upper bound (<)
	hasLowerBound := false
	hasUpperBound := false
	for _, rp := range result {
		if rp.Op == RangeOpGreaterThan {
			hasLowerBound = true
		}
		if rp.Op == RangeOpLessThan {
			hasUpperBound = true
		}
	}
	assert.True(t, hasLowerBound, "Should have a lower bound predicate")
	assert.True(t, hasUpperBound, "Should have an upper bound predicate")
}

func TestFindRangePredicates_DifferentColumn(t *testing.T) {
	// age > 18 but searching for "salary" column
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt,
		},
	}

	result := findRangePredicates(predicates, "salary")
	assert.Empty(t, result)
}

func TestFindRangePredicates_CaseInsensitive(t *testing.T) {
	// AGE > 18 (uppercase column name)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "AGE"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, "AGE", result[0].Column) // Preserves original case
}

func TestFindRangePredicates_EqualityIgnored(t *testing.T) {
	// age = 30 (equality, not range)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 30},
			op:    OpEq,
		},
	}

	result := findRangePredicates(predicates, "age")
	assert.Empty(t, result)
}

func TestFindRangePredicates_MixedPredicates(t *testing.T) {
	// age = 30 AND score > 80 AND score < 100
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 30},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "score"},
			right: &mockLiteralPredicate{value: 80},
			op:    OpGt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "score"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpLt,
		},
	}

	// Find range predicates for "score"
	result := findRangePredicates(predicates, "score")
	require.Len(t, result, 2)

	// Find range predicates for "age" (should be empty since it's equality)
	result = findRangePredicates(predicates, "age")
	assert.Empty(t, result)
}

// --- BETWEEN Tests ---

func TestFindRangePredicates_Between(t *testing.T) {
	// age BETWEEN 18 AND 65
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2, "BETWEEN should be decomposed into 2 range predicates")

	// Should have >= lower and <= upper
	hasGe := false
	hasLe := false
	for _, rp := range result {
		if rp.Op == RangeOpGreaterThanOrEqual {
			hasGe = true
			lit, ok := rp.Value.(*mockLiteralPredicate)
			require.True(t, ok)
			assert.Equal(t, 18, lit.value)
		}
		if rp.Op == RangeOpLessThanOrEqual {
			hasLe = true
			lit, ok := rp.Value.(*mockLiteralPredicate)
			require.True(t, ok)
			assert.Equal(t, 65, lit.value)
		}
	}
	assert.True(t, hasGe, "Should have >= predicate")
	assert.True(t, hasLe, "Should have <= predicate")
}

func TestFindRangePredicates_NotBetween(t *testing.T) {
	// age NOT BETWEEN 18 AND 65 (should not be supported)
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: true,
		},
	}

	result := findRangePredicates(predicates, "age")
	assert.Empty(t, result, "NOT BETWEEN should not produce range predicates")
}

func TestFindRangePredicates_BetweenWrongColumn(t *testing.T) {
	// age BETWEEN 18 AND 65 but searching for "salary"
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "salary")
	assert.Empty(t, result)
}

func TestFindRangePredicates_BetweenCaseInsensitive(t *testing.T) {
	// AGE BETWEEN 18 AND 65 (uppercase)
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "AGE"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2)
}

func TestFindRangePredicates_BetweenNonColumnExpr(t *testing.T) {
	// BETWEEN on a non-column expression should not match
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockLiteralPredicate{value: 25}, // Not a column ref
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")
	assert.Empty(t, result)
}

func TestFindRangePredicates_MixedRangeAndBetween(t *testing.T) {
	// age > 0 AND salary BETWEEN 50000 AND 100000
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 0},
			op:    OpGt,
		},
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "salary"},
			low:        &mockLiteralPredicate{value: 50000},
			high:       &mockLiteralPredicate{value: 100000},
			notBetween: false,
		},
	}

	// Find salary range predicates
	result := findRangePredicates(predicates, "salary")
	require.Len(t, result, 2, "BETWEEN should give 2 predicates")

	// Find age range predicates
	result = findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, RangeOpGreaterThan, result[0].Op)
}

// --- RangeOp helper method tests ---

func TestRangeOp_String(t *testing.T) {
	tests := []struct {
		op       RangeOp
		expected string
	}{
		{RangeOpLessThan, "<"},
		{RangeOpLessThanOrEqual, "<="},
		{RangeOpGreaterThan, ">"},
		{RangeOpGreaterThanOrEqual, ">="},
		{RangeOp(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.String())
		})
	}
}

func TestRangeOp_IsLowerBoundOp(t *testing.T) {
	assert.True(t, RangeOpGreaterThan.IsLowerBoundOp())
	assert.True(t, RangeOpGreaterThanOrEqual.IsLowerBoundOp())
	assert.False(t, RangeOpLessThan.IsLowerBoundOp())
	assert.False(t, RangeOpLessThanOrEqual.IsLowerBoundOp())
}

func TestRangeOp_IsUpperBoundOp(t *testing.T) {
	assert.True(t, RangeOpLessThan.IsUpperBoundOp())
	assert.True(t, RangeOpLessThanOrEqual.IsUpperBoundOp())
	assert.False(t, RangeOpGreaterThan.IsUpperBoundOp())
	assert.False(t, RangeOpGreaterThanOrEqual.IsUpperBoundOp())
}

func TestRangeOp_IsInclusive(t *testing.T) {
	assert.True(t, RangeOpLessThanOrEqual.IsInclusive())
	assert.True(t, RangeOpGreaterThanOrEqual.IsInclusive())
	assert.False(t, RangeOpLessThan.IsInclusive())
	assert.False(t, RangeOpGreaterThan.IsInclusive())
}

func TestFlipRangeOp(t *testing.T) {
	tests := []struct {
		input    RangeOp
		expected RangeOp
	}{
		{RangeOpLessThan, RangeOpGreaterThan},
		{RangeOpLessThanOrEqual, RangeOpGreaterThanOrEqual},
		{RangeOpGreaterThan, RangeOpLessThan},
		{RangeOpGreaterThanOrEqual, RangeOpLessThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.input.String()+"->"+tt.expected.String(), func(t *testing.T) {
			assert.Equal(t, tt.expected, flipRangeOp(tt.input))
		})
	}
}

func TestFindRangePredicates_PreservesOriginalPredicate(t *testing.T) {
	// Verify that OriginalPredicate is set correctly
	originalPred := &mockBinaryPredicate{
		left:  &mockColumnRefPredicate{table: "users", column: "age"},
		right: &mockLiteralPredicate{value: 30},
		op:    OpGt,
	}

	predicates := []PredicateExpr{originalPred}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	assert.Equal(t, originalPred, result[0].OriginalPredicate)
}

func TestFindRangePredicates_BetweenPreservesOriginalPredicate(t *testing.T) {
	// Both range predicates from BETWEEN should reference the same original
	originalPred := &mockBetweenPredicate{
		expr:       &mockColumnRefPredicate{table: "users", column: "age"},
		low:        &mockLiteralPredicate{value: 18},
		high:       &mockLiteralPredicate{value: 65},
		notBetween: false,
	}

	predicates := []PredicateExpr{originalPred}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2)

	// Both range predicates should reference the same original BETWEEN
	assert.Equal(t, originalPred, result[0].OriginalPredicate)
	assert.Equal(t, originalPred, result[1].OriginalPredicate)
}

// --- Task 4.2: Additional BETWEEN Edge Case Tests ---

// TestFindRangePredicates_BetweenSameLowerUpper tests BETWEEN with same lower and upper bounds.
// This effectively becomes a point lookup (value >= X AND value <= X means value = X).
func TestFindRangePredicates_BetweenSameLowerUpper(t *testing.T) {
	// age BETWEEN 30 AND 30 (point lookup)
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 30},
			high:       &mockLiteralPredicate{value: 30},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2, "BETWEEN with same bounds should still produce 2 range predicates")

	// Check that we have both >= 30 and <= 30
	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op == RangeOpGreaterThanOrEqual {
			lowerBound = &result[i]
		}
		if result[i].Op == RangeOpLessThanOrEqual {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound, "Should have >= predicate")
	require.NotNil(t, upperBound, "Should have <= predicate")

	// Both bounds should have the same value
	lowerVal := lowerBound.Value.(*mockLiteralPredicate).value
	upperVal := upperBound.Value.(*mockLiteralPredicate).value
	assert.Equal(t, 30, lowerVal)
	assert.Equal(t, 30, upperVal)
}

// TestFindRangePredicates_BetweenLowerGreaterThanUpper tests BETWEEN with lower > upper.
// This is an impossible condition (no values can satisfy >= 50 AND <= 20).
// The index matcher still produces the range predicates - it's up to the executor
// or optimizer to recognize the impossible range during query execution.
func TestFindRangePredicates_BetweenLowerGreaterThanUpper(t *testing.T) {
	// age BETWEEN 50 AND 20 (impossible range: lower > upper)
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 50}, // lower bound
			high:       &mockLiteralPredicate{value: 20}, // upper bound (< lower!)
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")

	// BETWEEN still produces 2 range predicates (>= 50 AND <= 20)
	// The impossibility is detected at query execution time, not at matching time
	require.Len(t, result, 2, "BETWEEN with inverted bounds still produces predicates")

	// Verify the bounds are extracted correctly
	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op == RangeOpGreaterThanOrEqual {
			lowerBound = &result[i]
		}
		if result[i].Op == RangeOpLessThanOrEqual {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound)
	require.NotNil(t, upperBound)

	// Lower bound is 50, upper bound is 20 (inverted)
	assert.Equal(t, 50, lowerBound.Value.(*mockLiteralPredicate).value)
	assert.Equal(t, 20, upperBound.Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_BetweenWithNullBounds tests BETWEEN with NULL values.
// NULL bounds are propagated as-is - semantic handling is done during execution.
func TestFindRangePredicates_BetweenWithNullBounds(t *testing.T) {
	// age BETWEEN NULL AND 65
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: nil}, // NULL lower bound
			high:       &mockLiteralPredicate{value: 65},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2, "BETWEEN with NULL bound still produces range predicates")

	// Find the lower bound predicate
	var lowerBound *RangePredicate
	for i := range result {
		if result[i].Op == RangeOpGreaterThanOrEqual {
			lowerBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound)
	// The NULL value should be preserved
	assert.Nil(t, lowerBound.Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_BetweenInclusiveBounds verifies that BETWEEN uses inclusive bounds.
// SQL BETWEEN is defined as inclusive on both ends: col BETWEEN a AND b means col >= a AND col <= b.
func TestFindRangePredicates_BetweenInclusiveBounds(t *testing.T) {
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 2)

	// Both operators should be inclusive
	for _, rp := range result {
		assert.True(t, rp.Op.IsInclusive(), "BETWEEN bounds should use inclusive operators")
	}

	// Specifically check operators are >= and <=
	hasGe := false
	hasLe := false
	for _, rp := range result {
		if rp.Op == RangeOpGreaterThanOrEqual {
			hasGe = true
		}
		if rp.Op == RangeOpLessThanOrEqual {
			hasLe = true
		}
	}
	assert.True(t, hasGe, "Should have >= operator")
	assert.True(t, hasLe, "Should have <= operator")
}

// TestFindRangePredicates_NotBetweenReturnsEmpty confirms NOT BETWEEN produces no predicates.
// NOT BETWEEN cannot be efficiently handled with index range scans because it represents
// a disjunction: (col < low OR col > high), which would require two separate scans.
func TestFindRangePredicates_NotBetweenReturnsEmpty(t *testing.T) {
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 65},
			notBetween: true, // NOT BETWEEN
		},
	}

	result := findRangePredicates(predicates, "age")
	assert.Empty(t, result, "NOT BETWEEN should return empty slice (not optimizable)")
}

// TestFindRangePredicates_BetweenMultipleOnSameColumn tests multiple BETWEEN on same column.
// This might occur from query rewriting or nested conditions.
func TestFindRangePredicates_BetweenMultipleOnSameColumn(t *testing.T) {
	// age BETWEEN 18 AND 30 AND age BETWEEN 25 AND 40 (overlapping ranges)
	predicates := []PredicateExpr{
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 18},
			high:       &mockLiteralPredicate{value: 30},
			notBetween: false,
		},
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "users", column: "age"},
			low:        &mockLiteralPredicate{value: 25},
			high:       &mockLiteralPredicate{value: 40},
			notBetween: false,
		},
	}

	result := findRangePredicates(predicates, "age")

	// Should produce 4 range predicates (2 from each BETWEEN)
	require.Len(t, result, 4, "Two BETWEEN predicates should produce 4 range predicates")

	// Count operators
	geCount := 0
	leCount := 0
	for _, rp := range result {
		if rp.Op == RangeOpGreaterThanOrEqual {
			geCount++
		}
		if rp.Op == RangeOpLessThanOrEqual {
			leCount++
		}
	}
	assert.Equal(t, 2, geCount, "Should have 2 >= predicates")
	assert.Equal(t, 2, leCount, "Should have 2 <= predicates")
}

// =============================================================================
// Task 4.3: Comparison Operator Tests for Different Data Types
// =============================================================================
// These tests verify that <, >, <=, >= predicates are correctly identified
// and produce correct RangeOp values for various data types.

// TestFindRangePredicates_FloatDataType tests range predicates with float/double values.
func TestFindRangePredicates_FloatDataType(t *testing.T) {
	tests := []struct {
		name       string
		op         BinaryOp
		value      interface{}
		expectedOp RangeOp
	}{
		{"float less than", OpLt, 3.14159, RangeOpLessThan},
		{"float less than or equal", OpLe, 2.71828, RangeOpLessThanOrEqual},
		{"float greater than", OpGt, 0.5, RangeOpGreaterThan},
		{"float greater than or equal", OpGe, 99.99, RangeOpGreaterThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicates := []PredicateExpr{
				&mockBinaryPredicate{
					left:  &mockColumnRefPredicate{table: "metrics", column: "value"},
					right: &mockLiteralPredicate{value: tt.value},
					op:    tt.op,
				},
			}

			result := findRangePredicates(predicates, "value")
			require.Len(t, result, 1, "Should find one range predicate")
			assert.Equal(t, tt.expectedOp, result[0].Op, "Operator should match")
			assert.Equal(t, "value", result[0].Column)
		})
	}
}

// TestFindRangePredicates_StringDataType tests range predicates with string values.
// String comparisons are lexicographic.
func TestFindRangePredicates_StringDataType(t *testing.T) {
	tests := []struct {
		name       string
		op         BinaryOp
		value      string
		expectedOp RangeOp
	}{
		{"string less than", OpLt, "zebra", RangeOpLessThan},
		{"string less than or equal", OpLe, "apple", RangeOpLessThanOrEqual},
		{"string greater than", OpGt, "banana", RangeOpGreaterThan},
		{"string greater than or equal", OpGe, "cherry", RangeOpGreaterThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicates := []PredicateExpr{
				&mockBinaryPredicate{
					left:  &mockColumnRefPredicate{table: "products", column: "name"},
					right: &mockLiteralPredicate{value: tt.value},
					op:    tt.op,
				},
			}

			result := findRangePredicates(predicates, "name")
			require.Len(t, result, 1, "Should find one range predicate")
			assert.Equal(t, tt.expectedOp, result[0].Op, "Operator should match")
		})
	}
}

// TestFindRangePredicates_DateDataType tests range predicates with date values.
// Dates can be represented as strings in standard format.
func TestFindRangePredicates_DateDataType(t *testing.T) {
	tests := []struct {
		name       string
		op         BinaryOp
		value      string // Dates as strings in YYYY-MM-DD format
		expectedOp RangeOp
	}{
		{"date less than", OpLt, "2024-12-31", RangeOpLessThan},
		{"date less than or equal", OpLe, "2024-01-01", RangeOpLessThanOrEqual},
		{"date greater than", OpGt, "2020-06-15", RangeOpGreaterThan},
		{"date greater than or equal", OpGe, "2023-03-20", RangeOpGreaterThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicates := []PredicateExpr{
				&mockBinaryPredicate{
					left:  &mockColumnRefPredicate{table: "orders", column: "order_date"},
					right: &mockLiteralPredicate{value: tt.value},
					op:    tt.op,
				},
			}

			result := findRangePredicates(predicates, "order_date")
			require.Len(t, result, 1, "Should find one range predicate")
			assert.Equal(t, tt.expectedOp, result[0].Op, "Operator should match")
		})
	}
}

// TestFindRangePredicates_MixedOperators tests combined range predicates like col > 10 AND col <= 100.
// This is a common pattern for range queries.
func TestFindRangePredicates_MixedOperators(t *testing.T) {
	// col > 10 AND col <= 100
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "col"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "col"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpLe,
		},
	}

	result := findRangePredicates(predicates, "col")
	require.Len(t, result, 2, "Should find two range predicates")

	// Verify we have both a lower bound (>) and upper bound (<=)
	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op.IsLowerBoundOp() {
			lowerBound = &result[i]
		}
		if result[i].Op.IsUpperBoundOp() {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound, "Should have lower bound predicate")
	require.NotNil(t, upperBound, "Should have upper bound predicate")

	// Lower bound should be > (exclusive)
	assert.Equal(t, RangeOpGreaterThan, lowerBound.Op)
	assert.False(t, lowerBound.Op.IsInclusive(), "Lower bound should be exclusive (>)")
	assert.Equal(t, 10, lowerBound.Value.(*mockLiteralPredicate).value)

	// Upper bound should be <= (inclusive)
	assert.Equal(t, RangeOpLessThanOrEqual, upperBound.Op)
	assert.True(t, upperBound.Op.IsInclusive(), "Upper bound should be inclusive (<=)")
	assert.Equal(t, 100, upperBound.Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_MixedOperatorsExclusiveBounds tests col >= 10 AND col < 100.
func TestFindRangePredicates_MixedOperatorsExclusiveBounds(t *testing.T) {
	// col >= 10 AND col < 100
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "col"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "col"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpLt,
		},
	}

	result := findRangePredicates(predicates, "col")
	require.Len(t, result, 2, "Should find two range predicates")

	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op.IsLowerBoundOp() {
			lowerBound = &result[i]
		}
		if result[i].Op.IsUpperBoundOp() {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound)
	require.NotNil(t, upperBound)

	// Lower bound should be >= (inclusive)
	assert.Equal(t, RangeOpGreaterThanOrEqual, lowerBound.Op)
	assert.True(t, lowerBound.Op.IsInclusive())

	// Upper bound should be < (exclusive)
	assert.Equal(t, RangeOpLessThan, upperBound.Op)
	assert.False(t, upperBound.Op.IsInclusive())
}

// TestFindRangePredicates_FloatRangeQuery tests a typical float range query.
func TestFindRangePredicates_FloatRangeQuery(t *testing.T) {
	// price >= 19.99 AND price < 99.99
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "products", column: "price"},
			right: &mockLiteralPredicate{value: 19.99},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "products", column: "price"},
			right: &mockLiteralPredicate{value: 99.99},
			op:    OpLt,
		},
	}

	result := findRangePredicates(predicates, "price")
	require.Len(t, result, 2)

	// Verify values are correctly extracted
	values := make(map[float64]bool)
	for _, rp := range result {
		val := rp.Value.(*mockLiteralPredicate).value.(float64)
		values[val] = true
	}
	assert.True(t, values[19.99], "Should have lower bound value 19.99")
	assert.True(t, values[99.99], "Should have upper bound value 99.99")
}

// TestFindRangePredicates_DateRangeQuery tests a typical date range query.
func TestFindRangePredicates_DateRangeQuery(t *testing.T) {
	// order_date >= '2024-01-01' AND order_date <= '2024-12-31'
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "order_date"},
			right: &mockLiteralPredicate{value: "2024-01-01"},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "order_date"},
			right: &mockLiteralPredicate{value: "2024-12-31"},
			op:    OpLe,
		},
	}

	result := findRangePredicates(predicates, "order_date")
	require.Len(t, result, 2)

	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op.IsLowerBoundOp() {
			lowerBound = &result[i]
		}
		if result[i].Op.IsUpperBoundOp() {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound)
	require.NotNil(t, upperBound)

	// Both should be inclusive for date range
	assert.True(t, lowerBound.Op.IsInclusive())
	assert.True(t, upperBound.Op.IsInclusive())

	// Verify date values
	assert.Equal(t, "2024-01-01", lowerBound.Value.(*mockLiteralPredicate).value)
	assert.Equal(t, "2024-12-31", upperBound.Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_StringRangeQuery tests a string range query (lexicographic).
func TestFindRangePredicates_StringRangeQuery(t *testing.T) {
	// name >= 'A' AND name < 'N' (first half of alphabet)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "customers", column: "name"},
			right: &mockLiteralPredicate{value: "A"},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "customers", column: "name"},
			right: &mockLiteralPredicate{value: "N"},
			op:    OpLt,
		},
	}

	result := findRangePredicates(predicates, "name")
	require.Len(t, result, 2)

	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op.IsLowerBoundOp() {
			lowerBound = &result[i]
		}
		if result[i].Op.IsUpperBoundOp() {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound)
	require.NotNil(t, upperBound)

	assert.Equal(t, "A", lowerBound.Value.(*mockLiteralPredicate).value)
	assert.Equal(t, "N", upperBound.Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_AllFourOperatorsTogether tests all four operators on the same column.
func TestFindRangePredicates_AllFourOperatorsTogether(t *testing.T) {
	// This is a contrived example but tests that all operators work correctly
	// val > 0 AND val >= 1 AND val < 100 AND val <= 99
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "val"},
			right: &mockLiteralPredicate{value: 0},
			op:    OpGt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "val"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "val"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpLt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "val"},
			right: &mockLiteralPredicate{value: 99},
			op:    OpLe,
		},
	}

	result := findRangePredicates(predicates, "val")
	require.Len(t, result, 4, "Should find all four range predicates")

	// Count each operator
	opCounts := make(map[RangeOp]int)
	for _, rp := range result {
		opCounts[rp.Op]++
	}

	assert.Equal(t, 1, opCounts[RangeOpGreaterThan], "Should have one >")
	assert.Equal(t, 1, opCounts[RangeOpGreaterThanOrEqual], "Should have one >=")
	assert.Equal(t, 1, opCounts[RangeOpLessThan], "Should have one <")
	assert.Equal(t, 1, opCounts[RangeOpLessThanOrEqual], "Should have one <=")
}

// TestFindRangePredicates_OperatorFlipping_AllTypes tests operator flipping with different data types.
func TestFindRangePredicates_OperatorFlipping_AllTypes(t *testing.T) {
	tests := []struct {
		name       string
		literalVal interface{}
		colName    string
		op         BinaryOp
		expectedOp RangeOp
	}{
		// Integer with flipping: 10 < col -> col > 10
		{"int literal < col", 10, "int_col", OpLt, RangeOpGreaterThan},
		// Float with flipping: 3.14 >= col -> col <= 3.14
		{"float literal >= col", 3.14, "float_col", OpGe, RangeOpLessThanOrEqual},
		// String with flipping: "hello" > col -> col < "hello"
		{"string literal > col", "hello", "str_col", OpGt, RangeOpLessThan},
		// Date with flipping: "2024-01-01" <= col -> col >= "2024-01-01"
		{"date literal <= col", "2024-01-01", "date_col", OpLe, RangeOpGreaterThanOrEqual},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reversed: literal on left, column on right
			predicates := []PredicateExpr{
				&mockBinaryPredicate{
					left:  &mockLiteralPredicate{value: tt.literalVal},
					right: &mockColumnRefPredicate{column: tt.colName},
					op:    tt.op,
				},
			}

			result := findRangePredicates(predicates, tt.colName)
			require.Len(t, result, 1)
			assert.Equal(t, tt.expectedOp, result[0].Op, "Operator should be flipped correctly")
			assert.Equal(t, tt.colName, result[0].Column)
		})
	}
}

// TestFindRangePredicates_BigInt tests range predicates with int64 values.
func TestFindRangePredicates_BigInt(t *testing.T) {
	// Large values that exceed int32 range
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "big_id"},
			right: &mockLiteralPredicate{value: int64(9223372036854775800)}, // near max int64
			op:    OpLt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "big_id"},
			right: &mockLiteralPredicate{value: int64(1000000000000)}, // 1 trillion
			op:    OpGe,
		},
	}

	result := findRangePredicates(predicates, "big_id")
	require.Len(t, result, 2)

	var lowerBound, upperBound *RangePredicate
	for i := range result {
		if result[i].Op.IsLowerBoundOp() {
			lowerBound = &result[i]
		}
		if result[i].Op.IsUpperBoundOp() {
			upperBound = &result[i]
		}
	}

	require.NotNil(t, lowerBound)
	require.NotNil(t, upperBound)

	// Verify large values are preserved
	assert.Equal(t, int64(1000000000000), lowerBound.Value.(*mockLiteralPredicate).value)
	assert.Equal(t, int64(9223372036854775800), upperBound.Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_TimestampDataType tests range predicates with timestamp values.
func TestFindRangePredicates_TimestampDataType(t *testing.T) {
	// Timestamps as strings in ISO format
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "created_at"},
			right: &mockLiteralPredicate{value: "2024-01-01T00:00:00Z"},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "created_at"},
			right: &mockLiteralPredicate{value: "2024-12-31T23:59:59Z"},
			op:    OpLe,
		},
	}

	result := findRangePredicates(predicates, "created_at")
	require.Len(t, result, 2)

	// Verify timestamp values
	values := make(map[string]bool)
	for _, rp := range result {
		val := rp.Value.(*mockLiteralPredicate).value.(string)
		values[val] = true
	}
	assert.True(t, values["2024-01-01T00:00:00Z"])
	assert.True(t, values["2024-12-31T23:59:59Z"])
}

// =============================================================================
// Task 4.4: Composite Key Range Tests
// =============================================================================
// These tests verify that composite indexes with equality on prefix columns
// and range predicates on the next column are properly matched.

// TestIndexMatcher_CompositeIndex_EqualityPlusRange tests the pattern:
// WHERE a = 1 AND b BETWEEN 10 AND 20 on index (a, b, c)
func TestIndexMatcher_CompositeIndex_EqualityPlusRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b BETWEEN 10 AND 20
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "data", column: "b"},
			low:        &mockLiteralPredicate{value: 10},
			high:       &mockLiteralPredicate{value: 20},
			notBetween: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())

	// Should have matched 2 columns: 'a' with equality, 'b' with range
	assert.Equal(t, 2, match.MatchedColumns)

	// Not a full match since 'b' is range and 'c' is not matched
	assert.False(t, match.IsFullMatch)

	// Should be a range scan
	assert.True(t, match.IsRangeScan)

	// Should have range bounds
	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 1, match.RangeBounds.RangeColumnIndex) // 'b' is at index 1

	// Both bounds should be inclusive (BETWEEN)
	assert.True(t, match.RangeBounds.LowerInclusive)
	assert.True(t, match.RangeBounds.UpperInclusive)

	// Should have range predicates
	assert.Len(t, match.RangePredicates, 2) // BETWEEN generates 2 range predicates

	// Should have lookup key for 'a'
	assert.Len(t, match.LookupKeys, 1)
}

// TestIndexMatcher_CompositeIndex_FullEqualityPlusRange tests the pattern:
// WHERE a = 1 AND b = 2 AND c > 10 on index (a, b, c)
func TestIndexMatcher_CompositeIndex_FullEqualityPlusRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b = 2 AND c > 10
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 2},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "c"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())

	// Should have matched all 3 columns
	assert.Equal(t, 3, match.MatchedColumns)

	// Not a full match since 'c' is a range predicate
	assert.False(t, match.IsFullMatch)

	// Should be a range scan
	assert.True(t, match.IsRangeScan)

	// Should have range bounds
	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 2, match.RangeBounds.RangeColumnIndex) // 'c' is at index 2

	// Only lower bound (> 10)
	require.NotNil(t, match.RangeBounds.LowerBound)
	assert.Nil(t, match.RangeBounds.UpperBound)
	assert.False(t, match.RangeBounds.LowerInclusive) // > is exclusive

	// Should have lookup keys for 'a' and 'b'
	assert.Len(t, match.LookupKeys, 2)
}

// TestIndexMatcher_CompositeIndex_RangeOnFirstColumn tests the pattern:
// WHERE a > 10 on index (a, b, c) - range on first column only
func TestIndexMatcher_CompositeIndex_RangeOnFirstColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a > 10
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())

	// Should have matched 1 column
	assert.Equal(t, 1, match.MatchedColumns)

	// Not a full match
	assert.False(t, match.IsFullMatch)

	// Should be a range scan
	assert.True(t, match.IsRangeScan)

	// Should have range bounds on column index 0
	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 0, match.RangeBounds.RangeColumnIndex)

	// No lookup keys (no equality predicates)
	assert.Empty(t, match.LookupKeys)
}

// TestIndexMatcher_CompositeIndex_RangeWithBothBounds tests the pattern:
// WHERE a = 1 AND b >= 10 AND b < 20 on index (a, b)
func TestIndexMatcher_CompositeIndex_RangeWithBothBounds(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_ab",
		table:    "data",
		columns:  []string{"a", "b"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b >= 10 AND b < 20
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 20},
			op:    OpLt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_ab", match.Index.GetName())

	// Should have matched both columns
	assert.Equal(t, 2, match.MatchedColumns)

	// Not a full match since 'b' is range
	assert.False(t, match.IsFullMatch)

	// Should be a range scan
	assert.True(t, match.IsRangeScan)

	// Should have range bounds
	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 1, match.RangeBounds.RangeColumnIndex)

	// Should have both lower and upper bounds
	require.NotNil(t, match.RangeBounds.LowerBound)
	require.NotNil(t, match.RangeBounds.UpperBound)

	// Lower bound is inclusive (>=), upper is exclusive (<)
	assert.True(t, match.RangeBounds.LowerInclusive)
	assert.False(t, match.RangeBounds.UpperInclusive)

	// Should have lookup key for 'a'
	assert.Len(t, match.LookupKeys, 1)
}

// TestIndexMatcher_CompositeIndex_AllEqualityNoRange tests the pattern:
// WHERE a = 1 AND b = 2 AND c = 3 on index (a, b, c) - full equality match
func TestIndexMatcher_CompositeIndex_AllEqualityNoRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite unique index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b = 2 AND c = 3
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 2},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "c"},
			right: &mockLiteralPredicate{value: 3},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())

	// Should have matched all 3 columns
	assert.Equal(t, 3, match.MatchedColumns)

	// Should be a full match
	assert.True(t, match.IsFullMatch)

	// Should NOT be a range scan
	assert.False(t, match.IsRangeScan)

	// No range bounds
	assert.Nil(t, match.RangeBounds)

	// Should have lookup keys for all columns
	assert.Len(t, match.LookupKeys, 3)

	// Should have very low selectivity (unique full match)
	assert.Equal(t, 0.01, match.Selectivity)
}

// TestIndexMatcher_CompositeIndex_RangeOnNonContiguousColumn tests the pattern:
// WHERE a = 1 AND c > 10 on index (a, b, c) - range on non-contiguous column
// This should NOT use the range on 'c' because 'b' is skipped
func TestIndexMatcher_CompositeIndex_RangeOnNonContiguousColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND c > 10 (skipping column 'b')
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "c"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())

	// Should only match column 'a' (equality)
	// Cannot use range on 'c' because 'b' is skipped
	assert.Equal(t, 1, match.MatchedColumns)

	// Not a range scan because we only matched equality on 'a'
	assert.False(t, match.IsRangeScan)

	// The 'c > 10' predicate becomes a residual filter
	assert.Len(t, match.LookupKeys, 1)
}

// TestIndexMatcher_SingleColumnIndex_RangeScan tests range scan on single-column index.
func TestIndexMatcher_SingleColumnIndex_RangeScan(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})

	// Single-column index on age
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_age",
		table:    "users",
		columns:  []string{"age"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE age > 18 AND age <= 65
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 65},
			op:    OpLe,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_users_age", match.Index.GetName())

	// Should have matched 1 column
	assert.Equal(t, 1, match.MatchedColumns)

	// Not a full match for uniqueness purposes
	assert.False(t, match.IsFullMatch)

	// Should be a range scan
	assert.True(t, match.IsRangeScan)

	// Should have range bounds
	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 0, match.RangeBounds.RangeColumnIndex)

	// Should have both bounds
	require.NotNil(t, match.RangeBounds.LowerBound)
	require.NotNil(t, match.RangeBounds.UpperBound)

	// Lower is exclusive (>), upper is inclusive (<=)
	assert.False(t, match.RangeBounds.LowerInclusive)
	assert.True(t, match.RangeBounds.UpperInclusive)

	// No lookup keys for range scan on single column
	assert.Empty(t, match.LookupKeys)
}

// TestIndexMatcher_SingleColumnIndex_PrefersEqualityOverRange tests that
// equality predicates are preferred over range predicates on the same column.
func TestIndexMatcher_SingleColumnIndex_PrefersEqualityOverRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})

	// Single-column index on age
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_age",
		table:    "users",
		columns:  []string{"age"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE age = 30 AND age > 25 (equality takes precedence)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 30},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 25},
			op:    OpGt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)

	match := matches[0]

	// Should use equality, not range
	assert.False(t, match.IsRangeScan)
	assert.True(t, match.IsFullMatch)

	// Should have lookup key for equality
	assert.Len(t, match.LookupKeys, 1)
}

// TestIndexMatcher_CompositeIndex_RangeBetween tests BETWEEN on composite index.
func TestIndexMatcher_CompositeIndex_RangeBetween(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})

	// Composite index on (customer_id, order_date)
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_cust_date",
		table:    "orders",
		columns:  []string{"customer_id", "order_date"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE customer_id = 100 AND order_date BETWEEN '2024-01-01' AND '2024-12-31'
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "customer_id"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpEq,
		},
		&mockBetweenPredicate{
			expr:       &mockColumnRefPredicate{table: "orders", column: "order_date"},
			low:        &mockLiteralPredicate{value: "2024-01-01"},
			high:       &mockLiteralPredicate{value: "2024-12-31"},
			notBetween: false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_orders_cust_date", match.Index.GetName())

	// Should have matched both columns
	assert.Equal(t, 2, match.MatchedColumns)

	// Should be a range scan
	assert.True(t, match.IsRangeScan)

	// Should have range bounds
	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 1, match.RangeBounds.RangeColumnIndex)

	// BETWEEN is inclusive on both ends
	assert.True(t, match.RangeBounds.LowerInclusive)
	assert.True(t, match.RangeBounds.UpperInclusive)

	// Verify bounds values
	assert.Equal(t, "2024-01-01", match.RangeBounds.LowerBound.(*mockLiteralPredicate).value)
	assert.Equal(t, "2024-12-31", match.RangeBounds.UpperBound.(*mockLiteralPredicate).value)
}

// TestIndexMatcher_CompositeIndex_SelectivityWithRange tests selectivity estimation
// for composite index with both equality and range predicates.
func TestIndexMatcher_CompositeIndex_SelectivityWithRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Non-unique composite index on (a, b)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_ab",
		table:    "data",
		columns:  []string{"a", "b"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b > 10
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]

	// Selectivity should account for both equality and range predicates
	// Equality: 0.1, Range: 0.3 -> Combined: 0.1 * 0.3 = 0.03
	// With matched predicates count, it should be reasonable
	assert.True(t, match.Selectivity > 0)
	assert.True(t, match.Selectivity < 1)
}

// TestIndexMatcher_CompositeIndex_OnlyLowerBound tests range with only lower bound.
func TestIndexMatcher_CompositeIndex_OnlyLowerBound(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_ab",
		table:    "data",
		columns:  []string{"a", "b"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b >= 100
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpGe,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.True(t, match.IsRangeScan)

	require.NotNil(t, match.RangeBounds)
	require.NotNil(t, match.RangeBounds.LowerBound)
	assert.Nil(t, match.RangeBounds.UpperBound)
	assert.True(t, match.RangeBounds.LowerInclusive)
}

// TestIndexMatcher_CompositeIndex_OnlyUpperBound tests range with only upper bound.
func TestIndexMatcher_CompositeIndex_OnlyUpperBound(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_ab",
		table:    "data",
		columns:  []string{"a", "b"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE a = 1 AND b < 50
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 1},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "b"},
			right: &mockLiteralPredicate{value: 50},
			op:    OpLt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.True(t, match.IsRangeScan)

	require.NotNil(t, match.RangeBounds)
	assert.Nil(t, match.RangeBounds.LowerBound)
	require.NotNil(t, match.RangeBounds.UpperBound)
	assert.False(t, match.RangeBounds.UpperInclusive)
}

// TestCountUniqueRangeColumns tests the helper function for counting unique columns.
func TestCountUniqueRangeColumns(t *testing.T) {
	tests := []struct {
		name     string
		preds    []RangePredicate
		expected int
	}{
		{
			name:     "empty",
			preds:    nil,
			expected: 0,
		},
		{
			name: "single column single predicate",
			preds: []RangePredicate{
				{Column: "age", Op: RangeOpGreaterThan},
			},
			expected: 1,
		},
		{
			name: "single column two predicates (range)",
			preds: []RangePredicate{
				{Column: "age", Op: RangeOpGreaterThan},
				{Column: "age", Op: RangeOpLessThan},
			},
			expected: 1,
		},
		{
			name: "two columns",
			preds: []RangePredicate{
				{Column: "age", Op: RangeOpGreaterThan},
				{Column: "salary", Op: RangeOpLessThan},
			},
			expected: 2,
		},
		{
			name: "case insensitive dedup",
			preds: []RangePredicate{
				{Column: "AGE", Op: RangeOpGreaterThan},
				{Column: "age", Op: RangeOpLessThan},
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countUniqueRangeColumns(tt.preds)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestRangeScanBounds_Structure tests the RangeScanBounds structure.
func TestRangeScanBounds_Structure(t *testing.T) {
	bounds := &RangeScanBounds{
		LowerBound:       &mockLiteralPredicate{value: 10},
		UpperBound:       &mockLiteralPredicate{value: 20},
		LowerInclusive:   true,
		UpperInclusive:   false,
		RangeColumnIndex: 1,
	}

	assert.NotNil(t, bounds.LowerBound)
	assert.NotNil(t, bounds.UpperBound)
	assert.True(t, bounds.LowerInclusive)
	assert.False(t, bounds.UpperInclusive)
	assert.Equal(t, 1, bounds.RangeColumnIndex)
}

// =============================================================================
// Additional Edge Case Tests for Range Matching Coverage
// =============================================================================

// TestFlipRangeOp_UnknownCase tests the default case in flipRangeOp.
func TestFlipRangeOp_UnknownCase(t *testing.T) {
	// Create an unknown RangeOp value
	unknownOp := RangeOp(99)
	flipped := flipRangeOp(unknownOp)
	// Unknown op should be returned as-is
	assert.Equal(t, unknownOp, flipped)
}

// TestEstimateSelectivity_EmptyPredicates tests selectivity estimation with empty predicates.
func TestEstimateSelectivity_EmptyPredicates(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	// Empty predicates should return 1.0 selectivity
	sel := matcher.estimateSelectivity(nil, false)
	assert.Equal(t, 1.0, sel)

	sel = matcher.estimateSelectivity([]PredicateExpr{}, false)
	assert.Equal(t, 1.0, sel)
}

// TestEstimateSelectivity_Clamping tests that selectivity is clamped to reasonable bounds.
func TestEstimateSelectivity_Clamping(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	// Many predicates should be clamped to minimum
	manyPreds := make([]PredicateExpr, 10)
	for i := range manyPreds {
		manyPreds[i] = &mockPredicate{}
	}

	sel := matcher.estimateSelectivity(manyPreds, false)
	assert.Equal(t, 0.001, sel, "Should be clamped to minimum selectivity")
}

// TestCreateRangeBoundsFromPredicates_EmptyPredicates tests empty predicate handling.
func TestCreateRangeBoundsFromPredicates_EmptyPredicates(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	bounds := matcher.createRangeBoundsFromPredicates(nil, 0)
	assert.Nil(t, bounds)

	bounds = matcher.createRangeBoundsFromPredicates([]RangePredicate{}, 0)
	assert.Nil(t, bounds)
}

// TestCreateRangeBoundsFromPredicates_MultipleSameBounds tests handling of multiple
// predicates with the same bound type (e.g., col > 10 AND col > 20).
func TestCreateRangeBoundsFromPredicates_MultipleSameBounds(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	// Two lower bounds - should use the last one (simplification)
	rangePreds := []RangePredicate{
		{Column: "age", Op: RangeOpGreaterThan, Value: &mockLiteralPredicate{value: 10}},
		{Column: "age", Op: RangeOpGreaterThan, Value: &mockLiteralPredicate{value: 20}},
	}

	bounds := matcher.createRangeBoundsFromPredicates(rangePreds, 0)
	require.NotNil(t, bounds)
	assert.NotNil(t, bounds.LowerBound)
	assert.Nil(t, bounds.UpperBound)
	assert.Equal(t, 0, bounds.RangeColumnIndex)

	// The second predicate (>20) should be used
	lit, ok := bounds.LowerBound.(*mockLiteralPredicate)
	require.True(t, ok)
	assert.Equal(t, 20, lit.value)
}

// TestEstimateCompositeSelectivity_UniqueFalse tests selectivity estimation
// for composite index with unique=false.
func TestEstimateCompositeSelectivity_UniqueFalse(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	equalityPreds := []PredicateExpr{
		&mockPredicate{},
		&mockPredicate{},
	}

	// Not unique, not full match
	sel := matcher.estimateCompositeSelectivity(equalityPreds, nil, false)
	// 2 equality predicates: 0.1 * 0.1 = 0.01
	assert.InDelta(t, 0.01, sel, 0.001)
}

// TestEstimateCompositeSelectivity_WithRangePredicates tests selectivity with range predicates.
func TestEstimateCompositeSelectivity_WithRangePredicates(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	equalityPreds := []PredicateExpr{
		&mockPredicate{},
	}

	rangePreds := []RangePredicate{
		{Column: "age", Op: RangeOpGreaterThan},
		{Column: "age", Op: RangeOpLessThan},
	}

	// 1 equality (0.1) * 1 range column (0.3) = 0.03
	sel := matcher.estimateCompositeSelectivity(equalityPreds, rangePreds, false)
	assert.InDelta(t, 0.03, sel, 0.001)
}

// TestEstimateCompositeSelectivity_RangeOnly tests selectivity with only range predicates.
func TestEstimateCompositeSelectivity_RangeOnly(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	rangePreds := []RangePredicate{
		{Column: "age", Op: RangeOpGreaterThan},
		{Column: "age", Op: RangeOpLessThan},
	}

	// 1 range column (0.3) = 0.3
	sel := matcher.estimateCompositeSelectivity(nil, rangePreds, false)
	assert.InDelta(t, 0.3, sel, 0.001)
}

// TestEstimateCompositeSelectivity_Clamping tests selectivity clamping with many predicates.
func TestEstimateCompositeSelectivity_Clamping(t *testing.T) {
	catalog := newMockIndexCatalog()
	matcher := NewIndexMatcher(catalog)

	// Many equality predicates to push selectivity below minimum
	manyPreds := make([]PredicateExpr, 10)
	for i := range manyPreds {
		manyPreds[i] = &mockPredicate{}
	}

	sel := matcher.estimateCompositeSelectivity(manyPreds, nil, false)
	assert.Equal(t, 0.001, sel, "Should be clamped to minimum")
}

// TestMatchCompositeIndex_RangeOnlyOnFirstColumn tests range scan with no equality prefix.
func TestMatchCompositeIndex_RangeOnlyOnFirstColumn(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Only range predicate on first column: a >= 10 AND a < 50
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "a"},
			right: &mockLiteralPredicate{value: 50},
			op:    OpLt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, "idx_data_abc", match.Index.GetName())
	assert.Equal(t, 1, match.MatchedColumns)
	assert.True(t, match.IsRangeScan)
	assert.False(t, match.IsFullMatch)

	// Should have both lower and upper bounds
	require.NotNil(t, match.RangeBounds)
	assert.NotNil(t, match.RangeBounds.LowerBound)
	assert.NotNil(t, match.RangeBounds.UpperBound)
	assert.True(t, match.RangeBounds.LowerInclusive)  // >=
	assert.False(t, match.RangeBounds.UpperInclusive) // <
}

// TestMatchCompositeIndex_NoMatchNoRange tests that no match is returned
// when there are no applicable predicates.
func TestMatchCompositeIndex_NoMatchNoRange(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Composite index on (a, b, c)
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_abc",
		table:    "data",
		columns:  []string{"a", "b", "c"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Predicate on column 'd' which is not in the index
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "data", column: "d"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	assert.Empty(t, matches)
}

// TestMatchSingleColumnIndex_EmptyInList tests IN clause with empty list.
func TestMatchSingleColumnIndex_EmptyInList(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "users", &mockTableInfo{})
	catalog.AddIndex("main", "users", &mockIndexDef{
		name:     "idx_users_id",
		table:    "users",
		columns:  []string{"id"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// IN clause with empty list
	predicates := []PredicateExpr{
		&mockInListPredicate{
			expr:   &mockColumnRefPredicate{table: "users", column: "id"},
			values: []PredicateExpr{}, // Empty list
			not:    false,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	require.Len(t, matches, 1)
	assert.Len(t, matches[0].LookupKeys, 0)
}

// TestFindRangePredicates_NullValueInRange tests range predicate with NULL value.
func TestFindRangePredicates_NullValueInRange(t *testing.T) {
	// col > NULL
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: nil}, // NULL value
			op:    OpGt,
		},
	}

	result := findRangePredicates(predicates, "age")
	require.Len(t, result, 1)
	// NULL value should be preserved
	assert.Nil(t, result[0].Value.(*mockLiteralPredicate).value)
}

// TestFindRangePredicates_BoundaryValues tests range predicates with boundary integer values.
func TestFindRangePredicates_BoundaryValues(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"min int32", int32(-2147483648)},
		{"max int32", int32(2147483647)},
		{"zero", 0},
		{"negative", -100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicates := []PredicateExpr{
				&mockBinaryPredicate{
					left:  &mockColumnRefPredicate{column: "val"},
					right: &mockLiteralPredicate{value: tt.value},
					op:    OpGe,
				},
			}

			result := findRangePredicates(predicates, "val")
			require.Len(t, result, 1)
			assert.Equal(t, tt.value, result[0].Value.(*mockLiteralPredicate).value)
		})
	}
}

// TestIndexMatcher_MultipleRangePredicatesDifferentColumns tests that range predicates
// on different columns are handled correctly.
func TestIndexMatcher_MultipleRangePredicatesDifferentColumns(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "data", &mockTableInfo{})

	// Index on column 'a' only
	catalog.AddIndex("main", "data", &mockIndexDef{
		name:     "idx_data_a",
		table:    "data",
		columns:  []string{"a"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// Range predicates on both 'a' and 'b' (only 'a' has index)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "a"},
			right: &mockLiteralPredicate{value: 10},
			op:    OpGt,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "b"},
			right: &mockLiteralPredicate{value: 20},
			op:    OpLt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "data", predicates)
	require.Len(t, matches, 1)

	// Only 'a' should be matched
	match := matches[0]
	assert.Equal(t, "idx_data_a", match.Index.GetName())
	assert.True(t, match.IsRangeScan)
	require.NotNil(t, match.RangeBounds)

	// Should only have lower bound (from a > 10)
	assert.NotNil(t, match.RangeBounds.LowerBound)
	assert.Nil(t, match.RangeBounds.UpperBound)
}

// TestIndexMatcher_CompositeIndex_EqualityPlusMultipleRanges tests composite index
// with equality on first column and multiple range predicates on the second.
func TestIndexMatcher_CompositeIndex_EqualityPlusMultipleRanges(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "orders", &mockTableInfo{})

	// Composite index on (customer_id, amount)
	catalog.AddIndex("main", "orders", &mockIndexDef{
		name:     "idx_orders_customer_amount",
		table:    "orders",
		columns:  []string{"customer_id", "amount"},
		isUnique: false,
	})

	matcher := NewIndexMatcher(catalog)

	// customer_id = 100 AND amount >= 50 AND amount < 200
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "customer_id"},
			right: &mockLiteralPredicate{value: 100},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "amount"},
			right: &mockLiteralPredicate{value: 50},
			op:    OpGe,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "orders", column: "amount"},
			right: &mockLiteralPredicate{value: 200},
			op:    OpLt,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "orders", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, 2, match.MatchedColumns)
	assert.True(t, match.IsRangeScan)
	assert.False(t, match.IsFullMatch)

	require.NotNil(t, match.RangeBounds)
	assert.Equal(t, 1, match.RangeBounds.RangeColumnIndex) // 'amount' is at index 1
	assert.NotNil(t, match.RangeBounds.LowerBound)
	assert.NotNil(t, match.RangeBounds.UpperBound)
	assert.True(t, match.RangeBounds.LowerInclusive)  // >=
	assert.False(t, match.RangeBounds.UpperInclusive) // <
}

// TestRangeOp_UnknownString tests String() method for unknown RangeOp.
func TestRangeOp_UnknownString(t *testing.T) {
	unknownOp := RangeOp(255)
	assert.Equal(t, "unknown", unknownOp.String())
}

// TestRangeOp_AllMethods tests all RangeOp method combinations.
func TestRangeOp_AllMethods(t *testing.T) {
	tests := []struct {
		op           RangeOp
		str          string
		isLowerBound bool
		isUpperBound bool
		isInclusive  bool
	}{
		{RangeOpLessThan, "<", false, true, false},
		{RangeOpLessThanOrEqual, "<=", false, true, true},
		{RangeOpGreaterThan, ">", true, false, false},
		{RangeOpGreaterThanOrEqual, ">=", true, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			assert.Equal(t, tt.str, tt.op.String())
			assert.Equal(t, tt.isLowerBound, tt.op.IsLowerBoundOp())
			assert.Equal(t, tt.isUpperBound, tt.op.IsUpperBoundOp())
			assert.Equal(t, tt.isInclusive, tt.op.IsInclusive())
		})
	}
}

// TestIndexMatcher_CompositeIndex_ThreeColumnsAllEquality tests 3-column composite
// index with equality on all columns.
func TestIndexMatcher_CompositeIndex_ThreeColumnsAllEquality(t *testing.T) {
	catalog := newMockIndexCatalog()
	catalog.AddTable("main", "events", &mockTableInfo{})

	// Composite unique index on (year, month, day)
	catalog.AddIndex("main", "events", &mockIndexDef{
		name:     "idx_events_date",
		table:    "events",
		columns:  []string{"year", "month", "day"},
		isUnique: true,
	})

	matcher := NewIndexMatcher(catalog)

	// WHERE year = 2024 AND month = 6 AND day = 15
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "year"},
			right: &mockLiteralPredicate{value: 2024},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "month"},
			right: &mockLiteralPredicate{value: 6},
			op:    OpEq,
		},
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{column: "day"},
			right: &mockLiteralPredicate{value: 15},
			op:    OpEq,
		},
	}

	matches := matcher.FindApplicableIndexes("main", "events", predicates)
	require.Len(t, matches, 1)

	match := matches[0]
	assert.Equal(t, 3, match.MatchedColumns)
	assert.True(t, match.IsFullMatch)
	assert.False(t, match.IsRangeScan)
	assert.Nil(t, match.RangeBounds)
	assert.Len(t, match.LookupKeys, 3)
	assert.Equal(t, 0.01, match.Selectivity) // Unique full match
}

// TestExtractRangeFromBinaryPredicate_NonRangeOperator tests that non-range operators
// return nil.
func TestExtractRangeFromBinaryPredicate_NonRangeOperator(t *testing.T) {
	// Test with equality operator (not a range operator)
	binExpr := &mockBinaryPredicate{
		left:  &mockColumnRefPredicate{column: "age"},
		right: &mockLiteralPredicate{value: 30},
		op:    OpEq, // Equality, not range
	}

	result := extractRangeFromBinaryPredicate(binExpr, "age", binExpr)
	assert.Empty(t, result)
}

// TestExtractRangeFromBinaryPredicate_WrongColumn tests that wrong column returns nil.
func TestExtractRangeFromBinaryPredicate_WrongColumn(t *testing.T) {
	binExpr := &mockBinaryPredicate{
		left:  &mockColumnRefPredicate{column: "age"},
		right: &mockLiteralPredicate{value: 30},
		op:    OpGt,
	}

	result := extractRangeFromBinaryPredicate(binExpr, "salary", binExpr)
	assert.Empty(t, result)
}

// TestExtractRangeFromBinaryPredicate_NoColumnRef tests that predicates without
// column references return nil.
func TestExtractRangeFromBinaryPredicate_NoColumnRef(t *testing.T) {
	// Both sides are literals
	binExpr := &mockBinaryPredicate{
		left:  &mockLiteralPredicate{value: 10},
		right: &mockLiteralPredicate{value: 20},
		op:    OpGt,
	}

	result := extractRangeFromBinaryPredicate(binExpr, "age", binExpr)
	assert.Empty(t, result)
}

// TestExtractRangeFromBetweenPredicate_NonColumnExpr tests BETWEEN with non-column expression.
func TestExtractRangeFromBetweenPredicate_NonColumnExpr(t *testing.T) {
	betweenExpr := &mockBetweenPredicate{
		expr:       &mockLiteralPredicate{value: 25}, // Not a column ref
		low:        &mockLiteralPredicate{value: 18},
		high:       &mockLiteralPredicate{value: 65},
		notBetween: false,
	}

	result := extractRangeFromBetweenPredicate(betweenExpr, "age", betweenExpr)
	assert.Empty(t, result)
}

// TestIndexMatch_Structure tests the IndexMatch struct fields.
func TestIndexMatch_Structure(t *testing.T) {
	match := IndexMatch{
		Index: &mockIndexDef{name: "test_idx", columns: []string{"a", "b"}},
		Predicates: []PredicateExpr{
			&mockBinaryPredicate{op: OpEq},
		},
		LookupKeys: []PredicateExpr{
			&mockLiteralPredicate{value: 1},
		},
		MatchedColumns:  2,
		IsFullMatch:     true,
		Selectivity:     0.01,
		IsRangeScan:     false,
		RangeBounds:     nil,
		RangePredicates: nil,
	}

	assert.Equal(t, "test_idx", match.Index.GetName())
	assert.Len(t, match.Predicates, 1)
	assert.Len(t, match.LookupKeys, 1)
	assert.Equal(t, 2, match.MatchedColumns)
	assert.True(t, match.IsFullMatch)
	assert.Equal(t, 0.01, match.Selectivity)
	assert.False(t, match.IsRangeScan)
	assert.Nil(t, match.RangeBounds)
	assert.Empty(t, match.RangePredicates)
}

// TestRangePredicate_Structure tests the RangePredicate struct fields.
func TestRangePredicate_Structure(t *testing.T) {
	origPred := &mockBinaryPredicate{op: OpGt}
	rp := RangePredicate{
		Column:            "age",
		Table:             "users",
		Op:                RangeOpGreaterThan,
		Value:             &mockLiteralPredicate{value: 18},
		OriginalPredicate: origPred,
	}

	assert.Equal(t, "age", rp.Column)
	assert.Equal(t, "users", rp.Table)
	assert.Equal(t, RangeOpGreaterThan, rp.Op)
	assert.NotNil(t, rp.Value)
	assert.Equal(t, origPred, rp.OriginalPredicate)
}

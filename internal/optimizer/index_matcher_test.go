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

func (m *mockIndexDef) GetName() string       { return m.name }
func (m *mockIndexDef) GetTable() string      { return m.table }
func (m *mockIndexDef) GetColumns() []string  { return m.columns }
func (m *mockIndexDef) GetIsUnique() bool     { return m.isUnique }

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

func (m *mockBinaryPredicate) PredicateType() string       { return "BinaryPredicate" }
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

func (m *mockInListPredicate) PredicateType() string          { return "InList" }
func (m *mockInListPredicate) PredicateInExpr() PredicateExpr { return m.expr }
func (m *mockInListPredicate) PredicateValues() []PredicateExpr { return m.values }
func (m *mockInListPredicate) PredicateIsNot() bool           { return m.not }

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
	assert.Equal(t, DefaultEqualitySelectivity, match.Selectivity) // Non-unique: default selectivity
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

	// Non-equality predicate (greater than)
	predicates := []PredicateExpr{
		&mockBinaryPredicate{
			left:  &mockColumnRefPredicate{table: "users", column: "age"},
			right: &mockLiteralPredicate{value: 18},
			op:    OpGt, // Not equality
		},
	}

	matches := matcher.FindApplicableIndexes("main", "users", predicates)
	assert.Empty(t, matches) // Only equality predicates are supported for now
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

// Package optimizer provides cost-based query optimization for dukdb-go.
package optimizer

import (
	"strings"
)

// PredicateExpr represents a filter predicate expression.
// This interface allows the index matcher to work with predicates without
// directly depending on the binder package, avoiding import cycles.
type PredicateExpr interface {
	// PredicateType returns a string identifier for the predicate type.
	PredicateType() string
}

// BinaryPredicateExpr represents a binary predicate (e.g., column = value).
type BinaryPredicateExpr interface {
	PredicateExpr
	// PredicateLeft returns the left operand.
	PredicateLeft() PredicateExpr
	// PredicateRight returns the right operand.
	PredicateRight() PredicateExpr
	// PredicateOperator returns the operator (use OpEq constant for equality).
	PredicateOperator() BinaryOp
}

// ColumnRefPredicateExpr represents a column reference in a predicate.
type ColumnRefPredicateExpr interface {
	PredicateExpr
	// PredicateTable returns the table name (may be empty for unqualified columns).
	PredicateTable() string
	// PredicateColumn returns the column name.
	PredicateColumn() string
}

// InListPredicateExpr represents an IN list predicate (e.g., column IN (1, 2, 3)).
// This interface allows the index matcher to handle IN clauses as multiple equality lookups.
type InListPredicateExpr interface {
	PredicateExpr
	// PredicateExpr returns the expression being tested (typically a column reference).
	PredicateInExpr() PredicateExpr
	// PredicateValues returns the list of values in the IN clause.
	PredicateValues() []PredicateExpr
	// PredicateIsNot returns true if this is a NOT IN expression.
	PredicateIsNot() bool
}

// IndexMatch represents a usable index with its matched predicates.
// This struct captures all information needed to generate an index scan:
// - Which index to use
// - Which predicates it satisfies
// - The lookup key values
// - Estimated selectivity for cost calculation
type IndexMatch struct {
	// Index is the matched index definition (optimizer.IndexDef interface).
	Index IndexDef

	// Predicates is the list of predicates satisfied by this index.
	// These predicates can be removed from the filter after index scan.
	Predicates []PredicateExpr

	// LookupKeys contains the expressions that evaluate to the lookup key values.
	// For equality predicates like "col = 5", this would be the literal 5.
	LookupKeys []PredicateExpr

	// MatchedColumns is the number of index columns matched by predicates.
	// For composite indexes, this indicates how many prefix columns are used.
	MatchedColumns int

	// IsFullMatch is true if all index columns are matched by predicates.
	// A full match on a unique index guarantees at most one row.
	IsFullMatch bool

	// Selectivity is the estimated fraction of rows returned (0.0-1.0).
	// Used by the cost model to compare index scan vs sequential scan.
	Selectivity float64
}

// IndexMatcher finds applicable indexes for filter predicates.
// It examines predicates in a WHERE clause and determines which indexes
// can be used to satisfy them efficiently.
type IndexMatcher struct {
	catalog CatalogProvider
}

// NewIndexMatcher creates a new IndexMatcher with access to the catalog.
func NewIndexMatcher(cat CatalogProvider) *IndexMatcher {
	return &IndexMatcher{
		catalog: cat,
	}
}

// FindApplicableIndexes returns indexes usable for the given predicates.
// It checks all indexes on the specified table and returns matches
// where predicates contain equality conditions on index columns.
//
// Parameters:
//   - schema: The schema name containing the table
//   - table: The table name to find indexes for
//   - predicates: Filter predicates to match against indexes
//
// Returns a slice of IndexMatch, one for each usable index, sorted by
// estimated selectivity (best selectivity first).
func (m *IndexMatcher) FindApplicableIndexes(
	schema, table string,
	predicates []PredicateExpr,
) []IndexMatch {
	if m.catalog == nil {
		return nil
	}

	// Get all indexes for this table from the catalog
	indexes := m.catalog.GetIndexesForTableAsInterface(schema, table)
	if len(indexes) == 0 {
		return nil
	}

	var matches []IndexMatch

	for _, indexDef := range indexes {
		match := m.matchIndex(indexDef, table, predicates)
		if match != nil {
			matches = append(matches, *match)
		}
	}

	// Sort by selectivity (lower is better)
	sortMatchesBySelectivity(matches)

	return matches
}

// matchIndex checks if an index can satisfy any of the predicates.
// For single-column indexes, it looks for an equality predicate on that column.
// For composite indexes, it matches predicates on prefix columns.
func (m *IndexMatcher) matchIndex(
	indexDef IndexDef,
	tableName string,
	predicates []PredicateExpr,
) *IndexMatch {
	columns := indexDef.GetColumns()
	if len(columns) == 0 {
		return nil
	}

	// For single-column indexes, just find equality predicate on that column
	if len(columns) == 1 {
		return m.matchSingleColumnIndex(indexDef, tableName, predicates)
	}

	// For composite indexes, match prefix columns
	return m.matchCompositeIndex(indexDef, tableName, predicates)
}

// matchSingleColumnIndex matches a single-column index against predicates.
func (m *IndexMatcher) matchSingleColumnIndex(
	indexDef IndexDef,
	tableName string,
	predicates []PredicateExpr,
) *IndexMatch {
	columns := indexDef.GetColumns()
	indexCol := columns[0]

	// First, try to find an equality predicate on the index column
	pred, keyExpr := m.findEqualityPredicate(predicates, tableName, indexCol)
	if pred != nil {
		// Estimate selectivity for this equality predicate
		selectivity := m.estimateSelectivity([]PredicateExpr{pred}, indexDef.GetIsUnique())

		return &IndexMatch{
			Index:          indexDef,
			Predicates:     []PredicateExpr{pred},
			LookupKeys:     []PredicateExpr{keyExpr},
			MatchedColumns: 1,
			IsFullMatch:    true, // Single column is always full match
			Selectivity:    selectivity,
		}
	}

	// Next, try to find an IN list predicate on the index column
	inPred, keyExprs := m.findInListPredicate(predicates, tableName, indexCol)
	if inPred != nil {
		// Estimate selectivity for IN list (each value contributes some selectivity)
		// IN clause selectivity is approximately: min(num_values * equality_selectivity, 1.0)
		selectivity := m.estimateInListSelectivity(len(keyExprs), indexDef.GetIsUnique())

		return &IndexMatch{
			Index:          indexDef,
			Predicates:     []PredicateExpr{inPred},
			LookupKeys:     keyExprs,
			MatchedColumns: 1,
			IsFullMatch:    true, // Single column is always full match
			Selectivity:    selectivity,
		}
	}

	return nil
}

// matchCompositeIndex matches a composite index against predicates.
// It requires equality predicates on a contiguous prefix of index columns.
func (m *IndexMatcher) matchCompositeIndex(
	indexDef IndexDef,
	tableName string,
	predicates []PredicateExpr,
) *IndexMatch {
	columns := indexDef.GetColumns()
	matchedPreds := make([]PredicateExpr, 0, len(columns))
	lookupKeys := make([]PredicateExpr, 0, len(columns))
	matchedCols := 0

	// Match prefix columns in order
	for _, indexCol := range columns {
		pred, keyExpr := m.findEqualityPredicate(predicates, tableName, indexCol)
		if pred == nil {
			// Must match contiguous prefix
			break
		}
		matchedPreds = append(matchedPreds, pred)
		lookupKeys = append(lookupKeys, keyExpr)
		matchedCols++
	}

	if matchedCols == 0 {
		return nil
	}

	isFullMatch := matchedCols == len(columns)
	selectivity := m.estimateSelectivity(matchedPreds, indexDef.GetIsUnique() && isFullMatch)

	return &IndexMatch{
		Index:          indexDef,
		Predicates:     matchedPreds,
		LookupKeys:     lookupKeys,
		MatchedColumns: matchedCols,
		IsFullMatch:    isFullMatch,
		Selectivity:    selectivity,
	}
}

// findEqualityPredicate searches for a predicate of the form "column = value"
// where column matches the specified table and column name.
// Returns the matching predicate and the value expression, or nil if not found.
func (m *IndexMatcher) findEqualityPredicate(
	predicates []PredicateExpr,
	tableName, columnName string,
) (PredicateExpr, PredicateExpr) {
	for _, pred := range predicates {
		binExpr, ok := pred.(BinaryPredicateExpr)
		if !ok {
			continue
		}

		// Check for equality operator
		if binExpr.PredicateOperator() != OpEq {
			continue
		}

		// Check if left side is the target column
		if colRef, ok := binExpr.PredicateLeft().(ColumnRefPredicateExpr); ok {
			if m.columnMatches(colRef, tableName, columnName) {
				// Right side is the key value
				return pred, binExpr.PredicateRight()
			}
		}

		// Check if right side is the target column (commutative)
		if colRef, ok := binExpr.PredicateRight().(ColumnRefPredicateExpr); ok {
			if m.columnMatches(colRef, tableName, columnName) {
				// Left side is the key value
				return pred, binExpr.PredicateLeft()
			}
		}
	}

	return nil, nil
}

// findInListPredicate searches for a predicate of the form "column IN (value1, value2, ...)"
// where column matches the specified table and column name.
// Returns the matching predicate and the list of value expressions, or nil if not found.
// NOT IN predicates are not matched since they cannot be efficiently evaluated with index lookups.
func (m *IndexMatcher) findInListPredicate(
	predicates []PredicateExpr,
	tableName, columnName string,
) (PredicateExpr, []PredicateExpr) {
	for _, pred := range predicates {
		inExpr, ok := pred.(InListPredicateExpr)
		if !ok {
			continue
		}

		// NOT IN cannot be efficiently handled with index lookups
		if inExpr.PredicateIsNot() {
			continue
		}

		// Check if the IN expression's column matches the target column
		colRef, ok := inExpr.PredicateInExpr().(ColumnRefPredicateExpr)
		if !ok {
			continue
		}

		if m.columnMatches(colRef, tableName, columnName) {
			// Return the IN predicate and all its values as lookup keys
			return pred, inExpr.PredicateValues()
		}
	}

	return nil, nil
}

// estimateInListSelectivity estimates the selectivity of an IN list predicate.
// For unique indexes, each value in the IN list can match at most one row.
// For non-unique indexes, each value has the default equality selectivity.
func (m *IndexMatcher) estimateInListSelectivity(numValues int, isUnique bool) float64 {
	if numValues == 0 {
		return 0.0
	}

	if isUnique {
		// Unique index: each value matches at most one row
		// Selectivity is approximately numValues * (1 / total_rows)
		// Use a small constant per value
		return float64(numValues) * 0.01
	}

	// Non-unique index: each value has default selectivity
	// Selectivity is approximately min(numValues * DefaultEqualitySelectivity, 1.0)
	selectivity := float64(numValues) * DefaultEqualitySelectivity

	// Clamp to reasonable range
	if selectivity > 1.0 {
		selectivity = 1.0
	}

	return selectivity
}

// columnMatches checks if a column reference matches the expected table and column.
// Handles case-insensitive comparison and empty table names (unqualified columns).
func (m *IndexMatcher) columnMatches(
	colRef ColumnRefPredicateExpr,
	tableName, columnName string,
) bool {
	// Column name must match (case-insensitive)
	if !strings.EqualFold(colRef.PredicateColumn(), columnName) {
		return false
	}

	// If column reference has a table qualifier, it must match
	if colRef.PredicateTable() != "" {
		return strings.EqualFold(colRef.PredicateTable(), tableName)
	}

	// Unqualified column matches any table
	return true
}

// estimateSelectivity estimates the selectivity of the matched predicates.
// This is a simplified estimate that can be enhanced with statistics.
func (m *IndexMatcher) estimateSelectivity(
	predicates []PredicateExpr,
	isUnique bool,
) float64 {
	if len(predicates) == 0 {
		return 1.0
	}

	// Unique index with all columns matched: very selective
	if isUnique {
		// Unique index returns at most 1 row
		// Use a small constant as selectivity
		return 0.01
	}

	// For non-unique indexes, estimate based on number of matched columns
	// Each equality predicate typically has selectivity around 0.1
	// Multiple predicates multiply (independence assumption)
	selectivity := 1.0
	for range predicates {
		selectivity *= DefaultEqualitySelectivity
	}

	// Clamp to reasonable range
	if selectivity < 0.001 {
		selectivity = 0.001
	}
	if selectivity > 1.0 {
		selectivity = 1.0
	}

	return selectivity
}

// sortMatchesBySelectivity sorts IndexMatch slice by selectivity (ascending).
// Lower selectivity means more selective (fewer rows), which is better.
func sortMatchesBySelectivity(matches []IndexMatch) {
	// Simple insertion sort (good for small slices)
	for i := 1; i < len(matches); i++ {
		j := i
		for j > 0 && matches[j].Selectivity < matches[j-1].Selectivity {
			matches[j], matches[j-1] = matches[j-1], matches[j]
			j--
		}
	}
}

// DefaultEqualitySelectivity is the estimated selectivity for an equality predicate
// when no statistics are available. This is a conservative estimate.
const DefaultEqualitySelectivity = 0.1

// IsCoveringIndex checks if the given index covers all required columns.
// A covering index contains all columns needed by the query, allowing
// an index-only scan that avoids accessing the main table heap.
//
// Parameters:
//   - indexDef: The index definition to check
//   - requiredColumns: Column names needed by the query (projections + filter columns)
//
// Returns true if the index contains all required columns, false otherwise.
//
// Note: Current HashIndex only stores RowIDs, not column values, so true
// index-only scans are not yet fully supported. This function is used to
// detect when an index-only scan would be beneficial for future optimization.
func IsCoveringIndex(indexDef IndexDef, requiredColumns []string) bool {
	if indexDef == nil {
		return false
	}

	// Build a set of index columns for efficient lookup
	indexCols := make(map[string]bool)
	for _, col := range indexDef.GetColumns() {
		indexCols[strings.ToLower(col)] = true
	}

	// Check if all required columns are in the index
	for _, required := range requiredColumns {
		if !indexCols[strings.ToLower(required)] {
			return false
		}
	}

	return true
}

// GetRequiredColumns extracts all column names needed by a query from
// the projection list and filter predicate columns.
//
// Parameters:
//   - projections: Column names from the SELECT clause
//   - filterColumns: Column names referenced in WHERE clause predicates
//
// Returns a deduplicated list of column names needed by the query.
func GetRequiredColumns(projections []string, filterColumns []string) []string {
	// Use a map for deduplication
	seen := make(map[string]bool)
	var result []string

	// Add projection columns
	for _, col := range projections {
		lower := strings.ToLower(col)
		if !seen[lower] {
			seen[lower] = true
			result = append(result, col)
		}
	}

	// Add filter columns
	for _, col := range filterColumns {
		lower := strings.ToLower(col)
		if !seen[lower] {
			seen[lower] = true
			result = append(result, col)
		}
	}

	return result
}

// ExtractColumnRefsFromPredicate extracts column names from a predicate expression.
// It recursively traverses the predicate tree to find all column references.
//
// Parameters:
//   - predicate: The predicate expression to extract columns from
//
// Returns a list of column names referenced in the predicate.
func ExtractColumnRefsFromPredicate(predicate PredicateExpr) []string {
	if predicate == nil {
		return nil
	}

	var columns []string

	// Check if it's a column reference
	if colRef, ok := predicate.(ColumnRefPredicateExpr); ok {
		columns = append(columns, colRef.PredicateColumn())
		return columns
	}

	// Check if it's a binary predicate
	if binExpr, ok := predicate.(BinaryPredicateExpr); ok {
		columns = append(columns, ExtractColumnRefsFromPredicate(binExpr.PredicateLeft())...)
		columns = append(columns, ExtractColumnRefsFromPredicate(binExpr.PredicateRight())...)
		return columns
	}

	// Check if it's an IN list predicate
	if inExpr, ok := predicate.(InListPredicateExpr); ok {
		columns = append(columns, ExtractColumnRefsFromPredicate(inExpr.PredicateInExpr())...)
		// Values in IN list are typically literals, not column references
		// but check them just in case
		for _, val := range inExpr.PredicateValues() {
			columns = append(columns, ExtractColumnRefsFromPredicate(val)...)
		}
		return columns
	}

	return columns
}

// ExtractColumnsFromPredicates extracts all unique column names from a list of predicates.
//
// Parameters:
//   - predicates: List of predicate expressions
//
// Returns a deduplicated list of column names from all predicates.
func ExtractColumnsFromPredicates(predicates []PredicateExpr) []string {
	seen := make(map[string]bool)
	var result []string

	for _, pred := range predicates {
		cols := ExtractColumnRefsFromPredicate(pred)
		for _, col := range cols {
			lower := strings.ToLower(col)
			if !seen[lower] {
				seen[lower] = true
				result = append(result, col)
			}
		}
	}

	return result
}

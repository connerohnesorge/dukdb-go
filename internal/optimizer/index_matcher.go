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

// BetweenPredicateExpr represents a BETWEEN expression (e.g., column BETWEEN low AND high).
// This interface allows the index matcher to handle BETWEEN as two range predicates.
type BetweenPredicateExpr interface {
	PredicateExpr
	// PredicateBetweenExpr returns the expression being tested (typically a column reference).
	PredicateBetweenExpr() PredicateExpr
	// PredicateLowBound returns the lower bound expression.
	PredicateLowBound() PredicateExpr
	// PredicateHighBound returns the upper bound expression.
	PredicateHighBound() PredicateExpr
	// PredicateIsNotBetween returns true if this is a NOT BETWEEN expression.
	PredicateIsNotBetween() bool
}

// LiteralPredicateExpr represents a literal value in a predicate expression.
// This interface allows the index matcher to extract literal values for range bounds.
type LiteralPredicateExpr interface {
	PredicateExpr
	// PredicateLiteralValue returns the literal value.
	PredicateLiteralValue() any
}

// RangeOp represents a range comparison operator.
type RangeOp int

const (
	// RangeOpLessThan represents the < operator.
	RangeOpLessThan RangeOp = iota
	// RangeOpLessThanOrEqual represents the <= operator.
	RangeOpLessThanOrEqual
	// RangeOpGreaterThan represents the > operator.
	RangeOpGreaterThan
	// RangeOpGreaterThanOrEqual represents the >= operator.
	RangeOpGreaterThanOrEqual
)

// String returns a string representation of the range operator.
func (op RangeOp) String() string {
	switch op {
	case RangeOpLessThan:
		return "<"
	case RangeOpLessThanOrEqual:
		return "<="
	case RangeOpGreaterThan:
		return ">"
	case RangeOpGreaterThanOrEqual:
		return ">="
	default:
		return "unknown"
	}
}

// RangePredicate represents a range predicate extracted from a filter condition.
// It captures the column, operator, and bound value for range comparisons.
type RangePredicate struct {
	// Column is the column name involved in the range predicate.
	Column string
	// Table is the table name (may be empty for unqualified columns).
	Table string
	// Op is the range comparison operator (<, >, <=, >=).
	Op RangeOp
	// Value is the bound value expression.
	Value PredicateExpr
	// OriginalPredicate is the original predicate expression this was extracted from.
	OriginalPredicate PredicateExpr
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

	// IsRangeScan is true if this index match uses a range scan instead of point lookups.
	// Range scans are used when there's a range predicate (<, >, <=, >=, BETWEEN) on an index column.
	IsRangeScan bool

	// RangeBounds contains the range scan boundaries when IsRangeScan is true.
	// For composite indexes, the bounds encode the equality prefix + range column values.
	RangeBounds *RangeScanBounds

	// RangePredicates contains the range predicates that are satisfied by this index range scan.
	// These predicates are in addition to the equality predicates in Predicates.
	RangePredicates []RangePredicate
}

// RangeScanBounds contains the lower and upper bounds for an index range scan.
// For composite indexes with equality on prefix columns and range on the next column,
// the bounds encode the full composite key.
//
// Example for index (a, b, c) with predicate a=1 AND b=2 AND c BETWEEN 10 AND 20:
//   - LowerBound: encoded composite key (1, 2, 10)
//   - UpperBound: encoded composite key (1, 2, 20)
//   - LowerInclusive: true (BETWEEN is inclusive)
//   - UpperInclusive: true
type RangeScanBounds struct {
	// LowerBound is the lower bound value expression for the range column.
	// For composite ranges, this is the value of the range column only (not the full key).
	LowerBound PredicateExpr
	// UpperBound is the upper bound value expression for the range column.
	UpperBound PredicateExpr
	// LowerInclusive is true if the lower bound is inclusive (>=).
	LowerInclusive bool
	// UpperInclusive is true if the upper bound is inclusive (<=).
	UpperInclusive bool
	// RangeColumnIndex is the index of the range column within the composite index.
	// For index (a, b, c) with range on c, this would be 2.
	RangeColumnIndex int
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
// It checks for equality predicates first (most selective), then IN lists,
// and finally range predicates.
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

	// Finally, try to find range predicates on the index column
	// Range predicates are less selective than equality but still useful
	rangePreds := findRangePredicates(predicates, indexCol)
	if len(rangePreds) > 0 {
		bounds := m.createRangeBoundsFromPredicates(rangePreds, 0)
		if bounds != nil {
			// Collect original predicates
			matchedPreds := make([]PredicateExpr, 0, len(rangePreds))
			for _, rp := range rangePreds {
				if rp.OriginalPredicate != nil {
					// Avoid duplicates (BETWEEN generates 2 range predicates from same original)
					isDuplicate := false
					for _, mp := range matchedPreds {
						if mp == rp.OriginalPredicate {
							isDuplicate = true
							break
						}
					}
					if !isDuplicate {
						matchedPreds = append(matchedPreds, rp.OriginalPredicate)
					}
				}
			}

			// Estimate selectivity for range predicate
			selectivity := m.estimateCompositeSelectivity(nil, rangePreds, false)

			return &IndexMatch{
				Index:           indexDef,
				Predicates:      matchedPreds,
				LookupKeys:      nil, // No point lookup keys for range scan
				MatchedColumns:  1,
				IsFullMatch:     false, // Range scan is never a full match (for uniqueness purposes)
				Selectivity:     selectivity,
				IsRangeScan:     true,
				RangeBounds:     bounds,
				RangePredicates: rangePreds,
			}
		}
	}

	return nil
}

// matchCompositeIndex matches a composite index against predicates.
// It handles two patterns:
//  1. Equality predicates on a contiguous prefix of index columns (point lookup)
//  2. Equality predicates on prefix columns [0..n-1] + range predicate on column [n] (range scan)
//
// For example, with index (a, b, c):
//   - WHERE a = 1 AND b = 2 AND c = 3 -> full equality match (point lookup)
//   - WHERE a = 1 AND b = 2 -> partial equality match (prefix scan)
//   - WHERE a = 1 AND b BETWEEN 10 AND 20 -> equality on 'a', range on 'b' (range scan)
//   - WHERE a = 1 AND b = 2 AND c > 10 -> equality on 'a', 'b', range on 'c' (range scan)
func (m *IndexMatcher) matchCompositeIndex(
	indexDef IndexDef,
	tableName string,
	predicates []PredicateExpr,
) *IndexMatch {
	columns := indexDef.GetColumns()
	matchedPreds := make([]PredicateExpr, 0, len(columns))
	lookupKeys := make([]PredicateExpr, 0, len(columns))
	matchedCols := 0

	// Match prefix columns in order with equality predicates
	for _, indexCol := range columns {
		pred, keyExpr := m.findEqualityPredicate(predicates, tableName, indexCol)
		if pred == nil {
			// No more equality predicates - check for range predicate on this column
			break
		}
		matchedPreds = append(matchedPreds, pred)
		lookupKeys = append(lookupKeys, keyExpr)
		matchedCols++
	}

	// After matching equality prefix, try to find range predicates on the next column
	// This enables patterns like: a = 1 AND b BETWEEN 10 AND 20 on index (a, b, c)
	var rangeBounds *RangeScanBounds
	var rangePredicates []RangePredicate
	isRangeScan := false

	if matchedCols < len(columns) {
		// There's at least one more column we didn't match with equality
		nextColIndex := matchedCols
		nextCol := columns[nextColIndex]

		// Look for range predicates on the next column
		rangePreds := findRangePredicates(predicates, nextCol)

		if len(rangePreds) > 0 {
			// Found range predicate(s) on the next column - create range scan bounds
			bounds := m.createRangeBoundsFromPredicates(rangePreds, nextColIndex)
			if bounds != nil {
				rangeBounds = bounds
				rangePredicates = rangePreds
				isRangeScan = true

				// Add original predicates from range predicates to matched predicates
				for _, rp := range rangePreds {
					if rp.OriginalPredicate != nil {
						// Avoid duplicates if same BETWEEN generated multiple range predicates
						isDuplicate := false
						for _, mp := range matchedPreds {
							if mp == rp.OriginalPredicate {
								isDuplicate = true
								break
							}
						}
						if !isDuplicate {
							matchedPreds = append(matchedPreds, rp.OriginalPredicate)
						}
					}
				}

				// The range column counts as a matched column for selectivity purposes
				matchedCols++
			}
		}
	}

	// If no equality predicates and no range predicates, check for range on first column
	if matchedCols == 0 && len(columns) > 0 {
		firstCol := columns[0]
		rangePreds := findRangePredicates(predicates, firstCol)

		if len(rangePreds) > 0 {
			// Range predicate on first column only (no equality prefix)
			bounds := m.createRangeBoundsFromPredicates(rangePreds, 0)
			if bounds != nil {
				rangeBounds = bounds
				rangePredicates = rangePreds
				isRangeScan = true

				// Add original predicates to matched predicates
				for _, rp := range rangePreds {
					if rp.OriginalPredicate != nil {
						isDuplicate := false
						for _, mp := range matchedPreds {
							if mp == rp.OriginalPredicate {
								isDuplicate = true
								break
							}
						}
						if !isDuplicate {
							matchedPreds = append(matchedPreds, rp.OriginalPredicate)
						}
					}
				}

				matchedCols = 1
			}
		}
	}

	if matchedCols == 0 {
		return nil
	}

	// Full match means all columns are matched with equality (not range)
	// Range scan on the last column doesn't count as a full match for unique index optimization
	isFullMatch := !isRangeScan && matchedCols == len(columns)
	selectivity := m.estimateCompositeSelectivity(
		matchedPreds,
		rangePredicates,
		indexDef.GetIsUnique() && isFullMatch,
	)

	return &IndexMatch{
		Index:           indexDef,
		Predicates:      matchedPreds,
		LookupKeys:      lookupKeys,
		MatchedColumns:  matchedCols,
		IsFullMatch:     isFullMatch,
		Selectivity:     selectivity,
		IsRangeScan:     isRangeScan,
		RangeBounds:     rangeBounds,
		RangePredicates: rangePredicates,
	}
}

// createRangeBoundsFromPredicates creates RangeScanBounds from a set of range predicates.
// It consolidates lower and upper bounds from the predicates.
//
// For multiple predicates on the same column (e.g., col > 10 AND col < 20),
// it extracts the tightest bounds.
func (m *IndexMatcher) createRangeBoundsFromPredicates(
	rangePreds []RangePredicate,
	colIndex int,
) *RangeScanBounds {
	if len(rangePreds) == 0 {
		return nil
	}

	bounds := &RangeScanBounds{
		RangeColumnIndex: colIndex,
	}

	// Process each range predicate
	for _, rp := range rangePreds {
		if rp.Op.IsLowerBoundOp() {
			// This is a lower bound (>, >=)
			// If we already have a lower bound, we could compare and keep the tightest,
			// but for now we just keep the last one (simplification)
			bounds.LowerBound = rp.Value
			bounds.LowerInclusive = rp.Op.IsInclusive()
		} else if rp.Op.IsUpperBoundOp() {
			// This is an upper bound (<, <=)
			bounds.UpperBound = rp.Value
			bounds.UpperInclusive = rp.Op.IsInclusive()
		}
	}

	// At least one bound must be set for a valid range scan
	if bounds.LowerBound == nil && bounds.UpperBound == nil {
		return nil
	}

	return bounds
}

// estimateCompositeSelectivity estimates selectivity for a composite index match
// that may include both equality and range predicates.
func (m *IndexMatcher) estimateCompositeSelectivity(
	equalityPreds []PredicateExpr,
	rangePreds []RangePredicate,
	isUniqueFullMatch bool,
) float64 {
	if isUniqueFullMatch {
		// Unique index with full equality match: at most 1 row
		return 0.01
	}

	// Start with equality predicate selectivity
	selectivity := 1.0
	for range equalityPreds {
		selectivity *= DefaultEqualitySelectivity
	}

	// Apply range predicate selectivity (typically less selective than equality)
	// Use a higher selectivity factor for range predicates
	const defaultRangeSelectivity = 0.3
	if len(rangePreds) > 0 {
		// Count unique columns in range predicates
		rangeColCount := countUniqueRangeColumns(rangePreds)
		for range rangeColCount {
			selectivity *= defaultRangeSelectivity
		}
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

// countUniqueRangeColumns counts the number of unique columns in range predicates.
// This handles cases where we have both upper and lower bounds on the same column
// (e.g., col > 10 AND col < 20 or BETWEEN which generates 2 predicates).
func countUniqueRangeColumns(rangePreds []RangePredicate) int {
	columns := make(map[string]bool)
	for _, rp := range rangePreds {
		columns[strings.ToLower(rp.Column)] = true
	}
	return len(columns)
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

// findRangePredicates searches for range predicates (<, >, <=, >=, BETWEEN) on the specified column.
// It extracts all range predicates that can potentially be used for index range scans.
//
// Parameters:
//   - predicates: List of predicates to search through
//   - columnName: The column name to find range predicates for
//
// Returns a slice of RangePredicate structs representing all found range predicates.
// BETWEEN expressions are decomposed into two separate range predicates (>= lower AND <= upper).
func findRangePredicates(predicates []PredicateExpr, columnName string) []RangePredicate {
	var result []RangePredicate

	for _, pred := range predicates {
		// Check for binary predicates with range operators
		if binExpr, ok := pred.(BinaryPredicateExpr); ok {
			rangePreds := extractRangeFromBinaryPredicate(binExpr, columnName, pred)
			result = append(result, rangePreds...)
			continue
		}

		// Check for BETWEEN predicates
		if betweenExpr, ok := pred.(BetweenPredicateExpr); ok {
			rangePreds := extractRangeFromBetweenPredicate(betweenExpr, columnName, pred)
			result = append(result, rangePreds...)
			continue
		}
	}

	return result
}

// extractRangeFromBinaryPredicate extracts a range predicate from a binary expression
// if it matches the target column and has a range operator (<, >, <=, >=).
func extractRangeFromBinaryPredicate(
	binExpr BinaryPredicateExpr,
	columnName string,
	originalPred PredicateExpr,
) []RangePredicate {
	op := binExpr.PredicateOperator()

	// Map binary operators to range operators
	var rangeOp RangeOp
	var isRangeOp bool

	switch op {
	case OpLt:
		rangeOp = RangeOpLessThan
		isRangeOp = true
	case OpLe:
		rangeOp = RangeOpLessThanOrEqual
		isRangeOp = true
	case OpGt:
		rangeOp = RangeOpGreaterThan
		isRangeOp = true
	case OpGe:
		rangeOp = RangeOpGreaterThanOrEqual
		isRangeOp = true
	default:
		return nil
	}

	if !isRangeOp {
		return nil
	}

	// Check if left side is the target column
	if colRef, ok := binExpr.PredicateLeft().(ColumnRefPredicateExpr); ok {
		if strings.EqualFold(colRef.PredicateColumn(), columnName) {
			return []RangePredicate{{
				Column:            colRef.PredicateColumn(),
				Table:             colRef.PredicateTable(),
				Op:                rangeOp,
				Value:             binExpr.PredicateRight(),
				OriginalPredicate: originalPred,
			}}
		}
	}

	// Check if right side is the target column (need to flip the operator)
	if colRef, ok := binExpr.PredicateRight().(ColumnRefPredicateExpr); ok {
		if strings.EqualFold(colRef.PredicateColumn(), columnName) {
			// Flip the operator: if we have "5 < col", that's equivalent to "col > 5"
			flippedOp := flipRangeOp(rangeOp)
			return []RangePredicate{{
				Column:            colRef.PredicateColumn(),
				Table:             colRef.PredicateTable(),
				Op:                flippedOp,
				Value:             binExpr.PredicateLeft(),
				OriginalPredicate: originalPred,
			}}
		}
	}

	return nil
}

// flipRangeOp flips a range operator for when the column is on the right side.
// For example: "5 < col" becomes "col > 5".
func flipRangeOp(op RangeOp) RangeOp {
	switch op {
	case RangeOpLessThan:
		return RangeOpGreaterThan
	case RangeOpLessThanOrEqual:
		return RangeOpGreaterThanOrEqual
	case RangeOpGreaterThan:
		return RangeOpLessThan
	case RangeOpGreaterThanOrEqual:
		return RangeOpLessThanOrEqual
	default:
		return op
	}
}

// extractRangeFromBetweenPredicate extracts range predicates from a BETWEEN expression.
// BETWEEN is decomposed into two range predicates: >= lower AND <= upper.
// NOT BETWEEN is not supported for index range scans (returns empty slice).
func extractRangeFromBetweenPredicate(
	betweenExpr BetweenPredicateExpr,
	columnName string,
	originalPred PredicateExpr,
) []RangePredicate {
	// NOT BETWEEN cannot be efficiently handled with index range scans
	if betweenExpr.PredicateIsNotBetween() {
		return nil
	}

	// Check if the BETWEEN expression's column matches the target column
	colRef, ok := betweenExpr.PredicateBetweenExpr().(ColumnRefPredicateExpr)
	if !ok {
		return nil
	}

	if !strings.EqualFold(colRef.PredicateColumn(), columnName) {
		return nil
	}

	// Decompose BETWEEN into two range predicates:
	// col BETWEEN low AND high -> col >= low AND col <= high
	return []RangePredicate{
		{
			Column:            colRef.PredicateColumn(),
			Table:             colRef.PredicateTable(),
			Op:                RangeOpGreaterThanOrEqual,
			Value:             betweenExpr.PredicateLowBound(),
			OriginalPredicate: originalPred,
		},
		{
			Column:            colRef.PredicateColumn(),
			Table:             colRef.PredicateTable(),
			Op:                RangeOpLessThanOrEqual,
			Value:             betweenExpr.PredicateHighBound(),
			OriginalPredicate: originalPred,
		},
	}
}

// IsLowerBoundOp returns true if the operator represents a lower bound (>, >=).
func (op RangeOp) IsLowerBoundOp() bool {
	return op == RangeOpGreaterThan || op == RangeOpGreaterThanOrEqual
}

// IsUpperBoundOp returns true if the operator represents an upper bound (<, <=).
func (op RangeOp) IsUpperBoundOp() bool {
	return op == RangeOpLessThan || op == RangeOpLessThanOrEqual
}

// IsInclusive returns true if the operator includes the boundary value (<=, >=).
func (op RangeOp) IsInclusive() bool {
	return op == RangeOpLessThanOrEqual || op == RangeOpGreaterThanOrEqual
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

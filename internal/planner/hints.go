// Package planner provides query planning for dukdb-go.
package planner

// OptimizationHints provides hints from the cost-based optimizer
// to guide physical plan selection.
type OptimizationHints struct {
	// JoinHints contains hints for physical join implementation.
	// Key is a join identifier (e.g., "join_0", "join_1").
	JoinHints map[string]JoinHint

	// AccessHints contains hints for physical access method.
	// Key is the table name or alias.
	AccessHints map[string]AccessHint
}

// JoinHint provides hints for physical join implementation.
type JoinHint struct {
	// Method specifies the join algorithm to use.
	// Valid values: "HashJoin", "NestedLoopJoin", "SortMergeJoin"
	Method string

	// BuildSide specifies which side should be the build side for hash joins.
	// Valid values: "left", "right"
	BuildSide string
}

// AccessHint provides hints for physical access method.
type AccessHint struct {
	// Method specifies the access method to use.
	// Valid values: "SeqScan", "IndexScan", "IndexRangeScan"
	Method string

	// IndexName specifies the index to use (if Method is "IndexScan" or "IndexRangeScan").
	IndexName string

	// LookupKeys contains expressions that evaluate to the values to look up in the index.
	// For a single-column index with equality predicate (e.g., WHERE id = 5),
	// this contains the literal value 5.
	// For composite indexes, there may be multiple keys (one per matched prefix column).
	// The type is []any to allow flexibility in representation; each element
	// can be a binder.BoundExpr, a literal value, or an optimizer.PredicateExpr
	// that gets converted during plan execution.
	LookupKeys []any

	// ResidualFilter contains filter conditions that couldn't be pushed into the index lookup.
	// These must be evaluated after fetching rows from the index.
	// For example, if the index is on (a, b) but the query has WHERE a = 1 AND c > 5,
	// the "c > 5" predicate becomes a residual filter.
	// The type is any to allow flexibility; it can be a binder.BoundExpr or
	// an optimizer expression that gets converted during plan execution.
	ResidualFilter any

	// MatchedPredicates contains the predicates that are satisfied by the index.
	// These predicates can be removed from the filter after the index scan.
	// Stored as []any for type flexibility between optimizer and planner types.
	MatchedPredicates []any

	// Selectivity is the estimated fraction of rows returned by the index scan (0.0-1.0).
	// Used for cost estimation and query plan display.
	Selectivity float64

	// MatchedColumns is the number of index columns matched by predicates.
	// For composite indexes, this indicates how many prefix columns are used.
	MatchedColumns int

	// IsFullMatch is true if all index columns are matched by predicates.
	// A full match on a unique index guarantees at most one row.
	IsFullMatch bool

	// IsRangeScan is true if this is a range scan (using <, >, <=, >=, BETWEEN)
	// rather than point lookups (equality predicates).
	IsRangeScan bool

	// RangeBounds contains the lower and upper bounds for range scans.
	// Only populated when IsRangeScan is true.
	RangeBounds *RangeScanBounds
}

// RangeScanBounds contains the boundaries for an index range scan.
// It supports both single-column and composite index ranges.
type RangeScanBounds struct {
	// LowerBound is the lower bound value expression for the range.
	// May be nil for unbounded scans (e.g., col < 100 has no lower bound).
	LowerBound any

	// UpperBound is the upper bound value expression for the range.
	// May be nil for unbounded scans (e.g., col > 10 has no upper bound).
	UpperBound any

	// LowerInclusive is true if the lower bound is inclusive (>=).
	LowerInclusive bool

	// UpperInclusive is true if the upper bound is inclusive (<=).
	UpperInclusive bool

	// RangeColumnIndex is the index of the range column within the composite index.
	// For single-column indexes, this is always 0.
	// For composite indexes with equality on prefix columns, this indicates
	// which column has the range predicate.
	// Example: For index (a, b, c) with WHERE a = 1 AND b BETWEEN 10 AND 20,
	// the RangeColumnIndex would be 1 (column b).
	RangeColumnIndex int
}

// NewOptimizationHints creates a new OptimizationHints with empty maps.
func NewOptimizationHints() *OptimizationHints {
	return &OptimizationHints{
		JoinHints:   make(map[string]JoinHint),
		AccessHints: make(map[string]AccessHint),
	}
}

// GetJoinHint returns the join hint for the given join key, if any.
func (h *OptimizationHints) GetJoinHint(key string) (JoinHint, bool) {
	if h == nil || h.JoinHints == nil {
		return JoinHint{}, false
	}
	hint, ok := h.JoinHints[key]
	return hint, ok
}

// GetAccessHint returns the access hint for the given table, if any.
func (h *OptimizationHints) GetAccessHint(table string) (AccessHint, bool) {
	if h == nil || h.AccessHints == nil {
		return AccessHint{}, false
	}
	hint, ok := h.AccessHints[table]
	return hint, ok
}

// HasJoinHints returns true if any join hints are set.
func (h *OptimizationHints) HasJoinHints() bool {
	return h != nil && len(h.JoinHints) > 0
}

// HasAccessHints returns true if any access hints are set.
func (h *OptimizationHints) HasAccessHints() bool {
	return h != nil && len(h.AccessHints) > 0
}

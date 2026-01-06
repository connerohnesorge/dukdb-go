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
	// Valid values: "SeqScan", "IndexScan"
	Method string

	// IndexName specifies the index to use (if Method is "IndexScan").
	IndexName string
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

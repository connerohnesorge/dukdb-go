package metadata

var defaultOptimizers = []OptimizerMetadata{
	{
		Name:        "predicate_pushdown",
		Description: "Push filters into scans",
		Value:       "true",
	},
	{
		Name:        "join_order",
		Description: "Optimize join order",
		Value:       "true",
	},
	{
		Name:        "statistics_propagation",
		Description: "Use statistics for planning",
		Value:       "true",
	},
	{
		Name:        "expression_rewriter",
		Description: "Rewrite expressions",
		Value:       "true",
	},
}

// GetOptimizers returns optimizer metadata.
func GetOptimizers() []OptimizerMetadata {
	result := make([]OptimizerMetadata, len(defaultOptimizers))
	copy(result, defaultOptimizers)
	return result
}

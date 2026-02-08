package rules

import "github.com/dukdb/dukdb-go/internal/planner/rewrite"

// DefaultRules returns the rewrite rules enabled by config.
func DefaultRules(config rewrite.Config) []rewrite.Rule {
	rules := make([]rewrite.Rule, 0, 12)
	if config.ConstantFolding {
		rules = append(rules, ConstantFoldingRule{})
	}
	if config.ExpressionRewrites {
		rules = append(rules,
			BooleanSimplificationRule{},
			DeMorganRule{},
			ComparisonSimplificationRule{},
			NullSimplificationRule{},
			ArithmeticIdentityRule{},
			InListSimplificationRule{},
		)
	}
	// SubqueryUnnesting must run BEFORE ProjectionPushdown
	// because unnesting creates joins that may need columns
	// that would otherwise be removed by projection pushdown
	if config.SubqueryUnnesting {
		rules = append(rules, SubqueryUnnestRule{})
	}
	if config.PredicatePushdown {
		rules = append(rules, FilterPushdownRule{})
	}
	if config.ProjectionPushdown {
		rules = append(rules, ProjectionPushdownRule{})
	}
	if config.JoinReordering {
		rules = append(rules, JoinReorderingRule{})
	}
	if config.DistinctElimination {
		rules = append(rules, DistinctEliminationRule{})
	}
	return rules
}

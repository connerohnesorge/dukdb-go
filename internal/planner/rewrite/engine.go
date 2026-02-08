package rewrite

import "log"

// Engine applies rule-based rewrites with fixed-point iteration.
type Engine struct {
	config       Config
	estimator    CostEstimator
	rules        []Rule
	adapter      Adapter
	planSubquery func(any) (Plan, error)
}

// NewEngine creates a rewrite engine with the provided rules.
func NewEngine(config Config, adapter Adapter, rules []Rule) *Engine {
	return &Engine{
		config:  config,
		adapter: adapter,
		rules:   append([]Rule(nil), rules...),
	}
}

// WithEstimator sets the cost estimator.
func (e *Engine) WithEstimator(estimator CostEstimator) *Engine {
	e.estimator = estimator
	return e
}

// WithPlanSubquery sets the subquery planner hook.
func (e *Engine) WithPlanSubquery(planSubquery func(any) (Plan, error)) *Engine {
	e.planSubquery = planSubquery
	return e
}

// Apply runs the rewrite engine to a fixed point.
func (e *Engine) Apply(plan Plan) (Plan, *Stats) {
	stats := &Stats{
		Applied: make(map[string]int),
		Skipped: make(map[string]int),
	}

	if !e.config.Enabled || plan == nil {
		return plan, stats
	}

	ctx := &Context{
		Config:       e.config,
		Stats:        stats,
		Estimator:    e.estimator,
		Adapter:      e.adapter,
		PlanSubquery: e.planSubquery,
	}

	limit := e.config.IterationLimit
	if limit <= 0 {
		limit = 1
	}

	for iter := 0; iter < limit; iter++ {
		changed := false
		for _, rule := range e.rules {
			newPlan, applied := rule.Apply(plan, ctx)
			if !applied {
				continue
			}

			beforeCost := 0.0
			afterCost := 0.0
			if e.estimator != nil {
				beforeCost = e.estimator.Estimate(plan)
				afterCost = e.estimator.Estimate(newPlan)
			}

			// Skip cost threshold check for rules that are always beneficial (e.g., subquery unnesting)
			if e.config.CostThreshold > 0 && e.estimator != nil && !isExemptFromCostThreshold(rule.Name()) {
				if afterCost > beforeCost*e.config.CostThreshold {
					stats.Skipped[rule.Name()]++
					stats.Events = append(stats.Events, RuleEvent{
						Name:       rule.Name(),
						Applied:    false,
						BeforeCost: beforeCost,
						AfterCost:  afterCost,
						Reason:     "cost_threshold",
					})
					continue
				}
			}

			plan = newPlan
			stats.Applied[rule.Name()]++
			stats.Events = append(stats.Events, RuleEvent{
				Name:       rule.Name(),
				Applied:    true,
				BeforeCost: beforeCost,
				AfterCost:  afterCost,
			})
			changed = true
		}

		stats.Iterations = iter + 1
		if !changed {
			return plan, stats
		}
	}

	stats.LimitReached = true
	log.Printf("rewrite engine reached iteration limit (%d)", limit)
	return plan, stats
}

// isExemptFromCostThreshold returns true for rules that should bypass cost threshold checks.
// These rules are known to be beneficial even if the cost model suggests otherwise.
func isExemptFromCostThreshold(ruleName string) bool {
	switch ruleName {
	case "subquery_unnest":
		// Subquery unnesting is almost always beneficial as it allows
		// the optimizer to consider join orderings and access paths
		return true
	default:
		return false
	}
}

package rewrite

import "time"

// Config controls rule-based rewrites during logical planning.
type Config struct {
	Enabled             bool
	ExpressionRewrites  bool
	ConstantFolding     bool
	PredicatePushdown   bool
	ProjectionPushdown  bool
	JoinReordering      bool
	DistinctElimination bool
	SubqueryUnnesting   bool
	ViewExpansion       bool
	IterationLimit      int
	CostThreshold       float64
}

// DefaultConfig returns the default rewrite configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:             true,
		ExpressionRewrites:  true,
		ConstantFolding:     true,
		PredicatePushdown:   true,
		ProjectionPushdown:  true,
		JoinReordering:      true,
		DistinctElimination: true,
		SubqueryUnnesting:   true,
		ViewExpansion:       true,
		IterationLimit:      100,
		CostThreshold:       1.05,
	}
}

// RuleEvent captures the outcome of a rule application.
type RuleEvent struct {
	Name       string
	Applied    bool
	BeforeCost float64
	AfterCost  float64
	Reason     string
	Timestamp  time.Time
}

// Stats tracks rule application during a rewrite pass.
type Stats struct {
	Iterations   int
	Applied      map[string]int
	Skipped      map[string]int
	Events       []RuleEvent
	LimitReached bool
}

// Context carries config and services for rule application.
type Context struct {
	Config       Config
	Stats        *Stats
	Estimator    CostEstimator
	Adapter      Adapter
	PlanSubquery func(any) (Plan, error)
}

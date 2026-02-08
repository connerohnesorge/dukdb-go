package planner

import (
	"fmt"

	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite/rules"
)

// SetRewriteConfig updates the planner rewrite configuration.
func (p *Planner) SetRewriteConfig(config rewrite.Config) {
	p.rewriteConfig = config
}

func (p *Planner) applyRewrites(plan LogicalPlan) (LogicalPlan, *rewrite.Stats) {
	adapter := newPlannerAdapter()
	engine := rewrite.NewEngine(p.rewriteConfig, adapter, rules.DefaultRules(p.rewriteConfig))
	engine.WithEstimator(newPlanCostEstimator(p.catalog))
	engine.WithPlanSubquery(func(stmt any) (rewrite.Plan, error) {
		sub, ok := stmt.(*binder.BoundSelectStmt)
		if !ok {
			return nil, fmt.Errorf("invalid subquery type")
		}
		return p.planSelect(sub)
	})

	if explain, ok := plan.(*LogicalExplain); ok {
		rewritten, stats := engine.Apply(explain.Child)
		newExplain := *explain
		if rewritten != nil {
			newExplain.Child = rewritten.(LogicalPlan)
		}
		newExplain.RewriteStats = stats
		return &newExplain, stats
	}

	rewritten, stats := engine.Apply(plan)
	if rewritten == nil {
		return nil, stats
	}
	return rewritten.(LogicalPlan), stats
}

// planCostEstimator uses simple heuristics and catalog stats.
type planCostEstimator struct {
	stats *optimizer.StatisticsManager
}

func newPlanCostEstimator(cat *catalog.Catalog) *planCostEstimator {
	return &planCostEstimator{stats: optimizer.NewStatisticsManager(cat)}
}

func (e *planCostEstimator) Estimate(plan rewrite.Plan) float64 {
	if plan == nil {
		return 0
	}
	node, ok := plan.(LogicalPlan)
	if !ok {
		return 0
	}
	switch n := node.(type) {
	case *LogicalScan:
		if n.TableDef != nil {
			if stats := n.TableDef.Statistics; stats != nil {
				return float64(stats.RowCount)
			}
		}
		if e.stats != nil {
			stats := e.stats.GetTableStats(n.Schema, n.TableName)
			if stats != nil {
				return float64(stats.RowCount)
			}
		}
		return 1000
	case *LogicalFilter:
		return e.Estimate(n.Child) * 0.5
	case *LogicalProject:
		return e.Estimate(n.Child)
	case *LogicalJoin:
		return e.Estimate(n.Left) + e.Estimate(n.Right)
	case *LogicalLateralJoin:
		return e.Estimate(n.Left) + e.Estimate(n.Right)
	case *LogicalAggregate:
		return e.Estimate(n.Child) * 0.5
	case *LogicalLimit:
		return e.Estimate(n.Child)
	default:
		total := 0.0
		for _, child := range node.Children() {
			total += e.Estimate(child)
		}
		if total == 0 {
			return 1
		}
		return total
	}
}

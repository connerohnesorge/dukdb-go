// Package optimizer provides cost-based query optimization for dukdb-go.
//
// # Overview
//
// The cost-based optimizer improves query execution performance by:
//   - Estimating cardinality (output row counts) for each plan operator
//   - Calculating execution costs based on I/O and CPU requirements
//   - Optimizing join order for multi-table queries
//   - Generating hints for physical plan implementation
//
// # Architecture
//
// The optimizer consists of several integrated components:
//
//   - StatisticsManager: Provides table and column statistics for estimation
//   - CardinalityEstimator: Estimates output rows for plan operators
//   - CostModel: Calculates execution costs based on cardinality and I/O
//   - JoinOrderOptimizer: Finds optimal join order using DP or greedy algorithms
//   - PlanEnumerator: Enumerates and compares alternative plans
//   - CostBasedOptimizer: Main entry point coordinating all components
//
// # Usage
//
// Basic usage:
//
//	// Create optimizer with catalog access for statistics
//	optimizer := optimizer.NewCostBasedOptimizer(catalogProvider)
//
//	// Optimize a logical plan
//	result, err := optimizer.Optimize(logicalPlan)
//	if err != nil {
//	    return err
//	}
//
//	// Use optimization results
//	fmt.Printf("Estimated cost: %.2f\n", result.EstimatedCost.TotalCost)
//	fmt.Printf("Estimated rows: %.2f\n", result.EstimatedCost.OutputRows)
//
// # Enabling and Disabling
//
// The optimizer can be enabled or disabled at runtime:
//
//	optimizer.SetEnabled(false)  // Disable optimization
//	optimizer.SetEnabled(true)   // Enable optimization
//	enabled := optimizer.IsEnabled()  // Check status
//
// When disabled, the optimizer returns the original plan with basic cost estimates.
//
// # Cost Constant Tuning
//
// The cost model uses configurable constants that can be tuned for different
// hardware profiles. Default constants are optimized for in-memory operations:
//
//	constants := optimizer.DefaultCostConstants()
//	// Adjust for SSD storage (faster random I/O):
//	constants.RandomPageCost = 2.0  // Default: 4.0
//	// Adjust for slower CPU:
//	constants.CPUTupleCost = 0.02   // Default: 0.01
//
//	costModel := optimizer.NewCostModel(constants, estimator)
//
// Cost constants and their meanings:
//   - SeqPageCost (default 1.0): Cost of sequential page read
//   - RandomPageCost (default 4.0): Cost of random page read
//   - CPUTupleCost (default 0.01): Cost per tuple processed
//   - CPUOperatorCost (default 0.0025): Cost per operator evaluation
//   - HashBuildCost (default 0.02): Cost per tuple for hash table build
//   - HashProbeCost (default 0.01): Cost per tuple for hash table probe
//   - SortCost (default 0.05): Cost per comparison in sort
//
// # Join Order Optimization
//
// The join order optimizer uses dynamic programming (DP) for small queries
// and switches to a greedy algorithm for larger queries:
//
//	joinOpt := optimizer.GetJoinOrderOptimizer()
//
//	// Adjust DP threshold (max tables for DP algorithm)
//	joinOpt.SetDPThreshold(10)  // Default: 12
//
//	// Adjust pair enumeration limit before switching to greedy
//	joinOpt.SetPairLimit(5000)  // Default: 10000
//
// For queries with N <= DPThreshold tables and enumerated pairs <= PairLimit,
// the optimizer uses the DPccp algorithm to find the globally optimal join order.
// For larger queries, it uses a greedy algorithm that starts with the smallest
// relation and iteratively joins the lowest-cost pair.
//
// # Statistics and ANALYZE
//
// The optimizer relies on table and column statistics for accurate estimation.
// Use the ANALYZE command to collect statistics:
//
//	ANALYZE table_name;           -- Analyze specific table
//	ANALYZE;                      -- Analyze all tables
//
// Statistics include:
//   - Row count and page count for tables
//   - Distinct value count, null fraction, min/max for columns
//   - Optional histograms for skewed distributions
//
// Without statistics, the optimizer uses conservative defaults which may result
// in suboptimal plans for large or skewed datasets.
//
// # EXPLAIN with Cost Annotations
//
// Use EXPLAIN to view cost estimates for query plans:
//
//	EXPLAIN SELECT * FROM orders WHERE status = 'active';
//
// The output includes estimated costs and row counts for each operator.
//
// # Performance Considerations
//
// The optimizer is designed for minimal overhead:
//   - Simple queries (no joins): < 1ms optimization time
//   - Multi-table joins: Scales with number of tables and predicates
//   - Memory usage: Proportional to plan size and DP state
//
// For OLTP workloads with simple queries, the optimizer overhead is negligible.
// For complex OLAP queries with many joins, optimization time is offset by
// improved execution performance.
package optimizer

import (
	"fmt"
)

// Optimizer is the main interface for cost-based query optimization.
type Optimizer interface {
	// Optimize transforms a logical plan into an optimized plan with hints.
	Optimize(plan LogicalPlanNode) (*OptimizedPlan, error)
}

// OptimizedPlan wraps a logical plan with optimization annotations.
type OptimizedPlan struct {
	Plan          LogicalPlanNode       // The (possibly reordered) logical plan
	EstimatedCost PlanCost              // Total estimated cost
	JoinHints     map[string]JoinHint   // Per-join hints (build side, method)
	AccessHints   map[string]AccessHint // Per-table access hints
}

// JoinHint provides hints for physical join implementation.
type JoinHint struct {
	Method    PhysicalPlanType // HashJoin, NestedLoopJoin, SortMergeJoin
	BuildSide string           // "left" or "right"
}

// AccessHint provides hints for physical access method.
type AccessHint struct {
	Method    PhysicalPlanType // SeqScan or IndexScan
	IndexName string           // Index to use (if IndexScan)
}

// CostBasedOptimizer is the main optimizer implementation.
// It coordinates all optimization components to produce an optimized plan.
type CostBasedOptimizer struct {
	stats         *StatisticsManager
	estimator     *CardinalityEstimator
	costModel     *CostModel
	joinOptimizer *JoinOrderOptimizer
	enumerator    *PlanEnumerator
	enabled       bool
}

// NewCostBasedOptimizer creates a new CostBasedOptimizer with all components.
func NewCostBasedOptimizer(catalogProvider CatalogProvider) *CostBasedOptimizer {
	stats := NewStatisticsManager(catalogProvider)
	estimator := NewCardinalityEstimator(stats)
	costModel := NewCostModel(DefaultCostConstants(), estimator)
	joinOptimizer := NewJoinOrderOptimizer(estimator, costModel)
	enumerator := NewPlanEnumerator(estimator, costModel, stats)

	return &CostBasedOptimizer{
		stats:         stats,
		estimator:     estimator,
		costModel:     costModel,
		joinOptimizer: joinOptimizer,
		enumerator:    enumerator,
		enabled:       true,
	}
}

// Optimize transforms a logical plan into an optimized plan with hints.
// This is the main entry point for cost-based optimization.
func (o *CostBasedOptimizer) Optimize(plan LogicalPlanNode) (*OptimizedPlan, error) {
	if plan == nil {
		return nil, fmt.Errorf("cannot optimize nil plan")
	}

	if !o.enabled {
		return o.noOptimize(plan)
	}

	// Fast path for simple queries (no joins)
	if o.isSimpleQuery(plan) {
		return o.fastPathOptimize(plan)
	}

	// Full optimization for complex queries
	return o.fullOptimize(plan)
}

// SetEnabled enables or disables the optimizer.
func (o *CostBasedOptimizer) SetEnabled(enabled bool) {
	o.enabled = enabled
}

// IsEnabled returns whether the optimizer is enabled.
func (o *CostBasedOptimizer) IsEnabled() bool {
	return o.enabled
}

// GetStatisticsManager returns the statistics manager for external access.
func (o *CostBasedOptimizer) GetStatisticsManager() *StatisticsManager {
	return o.stats
}

// GetCardinalityEstimator returns the cardinality estimator for external access.
func (o *CostBasedOptimizer) GetCardinalityEstimator() *CardinalityEstimator {
	return o.estimator
}

// GetCostModel returns the cost model for external access.
func (o *CostBasedOptimizer) GetCostModel() *CostModel {
	return o.costModel
}

// GetJoinOrderOptimizer returns the join order optimizer for external access.
func (o *CostBasedOptimizer) GetJoinOrderOptimizer() *JoinOrderOptimizer {
	return o.joinOptimizer
}

// GetPlanEnumerator returns the plan enumerator for external access.
func (o *CostBasedOptimizer) GetPlanEnumerator() *PlanEnumerator {
	return o.enumerator
}

// noOptimize returns the plan unchanged with basic cost estimate.
// Used when optimization is disabled.
func (o *CostBasedOptimizer) noOptimize(plan LogicalPlanNode) (*OptimizedPlan, error) {
	// Just estimate cardinality for the plan
	cardinality := o.estimator.EstimateCardinality(plan)
	width := o.estimator.EstimateRowWidth(plan)

	return &OptimizedPlan{
		Plan: plan,
		EstimatedCost: PlanCost{
			StartupCost: 0,
			TotalCost:   cardinality, // Simple cost estimate
			OutputRows:  cardinality,
			OutputWidth: width,
		},
		JoinHints:   make(map[string]JoinHint),
		AccessHints: make(map[string]AccessHint),
	}, nil
}

// isSimpleQuery checks if the query is simple (no joins).
func (o *CostBasedOptimizer) isSimpleQuery(plan LogicalPlanNode) bool {
	return countJoins(plan) == 0
}

// fastPathOptimize performs quick optimization for single-table queries.
// No join reordering is needed, just estimate cost and generate access hints.
func (o *CostBasedOptimizer) fastPathOptimize(plan LogicalPlanNode) (*OptimizedPlan, error) {
	// Estimate cardinality and cost
	cardinality := o.estimator.EstimateCardinality(plan)
	width := o.estimator.EstimateRowWidth(plan)

	cost := PlanCost{
		StartupCost: 0,
		TotalCost:   o.estimatePlanCost(plan),
		OutputRows:  cardinality,
		OutputWidth: width,
	}

	// Generate access hints for scans
	accessHints := o.generateAccessHints(plan)

	return &OptimizedPlan{
		Plan:          plan,
		EstimatedCost: cost,
		JoinHints:     make(map[string]JoinHint),
		AccessHints:   accessHints,
	}, nil
}

// fullOptimize performs full optimization including join reordering.
func (o *CostBasedOptimizer) fullOptimize(plan LogicalPlanNode) (*OptimizedPlan, error) {
	// Extract tables and join predicates
	tables := extractTables(plan)
	predicates := extractJoinPredicatesFromPlan(plan)

	// Optimize join order if there are joins
	var joinHints map[string]JoinHint
	if len(tables) > 1 && len(predicates) > 0 {
		joinPlan, err := o.joinOptimizer.OptimizeJoinOrder(tables, predicates)
		if err != nil {
			return nil, fmt.Errorf("join order optimization failed: %w", err)
		}

		// Generate hints from join plan
		joinHints = o.generateJoinHints(joinPlan)
	} else {
		joinHints = make(map[string]JoinHint)
	}

	// Generate access hints for all scans
	accessHints := o.generateAccessHints(plan)

	// Estimate final cost
	cardinality := o.estimator.EstimateCardinality(plan)
	width := o.estimator.EstimateRowWidth(plan)

	cost := PlanCost{
		StartupCost: 0,
		TotalCost:   o.estimatePlanCost(plan),
		OutputRows:  cardinality,
		OutputWidth: width,
	}

	return &OptimizedPlan{
		Plan:          plan,
		EstimatedCost: cost,
		JoinHints:     joinHints,
		AccessHints:   accessHints,
	}, nil
}

// estimatePlanCost estimates the total cost of executing a logical plan.
func (o *CostBasedOptimizer) estimatePlanCost(plan LogicalPlanNode) float64 {
	if plan == nil {
		return 0
	}

	switch plan.PlanType() {
	case "LogicalScan":
		if scan, ok := plan.(ScanNode); ok {
			tableStats := o.stats.GetTableStats(scan.Schema(), scan.TableName())
			if tableStats != nil {
				return float64(tableStats.RowCount) * o.costModel.constants.CPUTupleCost
			}
		}
		return DefaultRowCount * o.costModel.constants.CPUTupleCost

	case "LogicalFilter":
		childCost := o.estimatePlanCost(plan.PlanChildren()[0])
		cardinality := o.estimator.EstimateCardinality(plan)
		return childCost + cardinality*o.costModel.constants.CPUOperatorCost

	case "LogicalProject":
		childCost := o.estimatePlanCost(plan.PlanChildren()[0])
		cardinality := o.estimator.EstimateCardinality(plan)
		numCols := len(plan.PlanOutputColumns())
		return childCost + cardinality*float64(numCols)*o.costModel.constants.CPUOperatorCost

	case "LogicalJoin":
		children := plan.PlanChildren()
		if len(children) < 2 {
			return DefaultRowCount * o.costModel.constants.CPUTupleCost
		}
		leftCost := o.estimatePlanCost(children[0])
		rightCost := o.estimatePlanCost(children[1])
		outputCard := o.estimator.EstimateCardinality(plan)

		// Estimate hash join cost
		leftCard := o.estimator.EstimateCardinality(children[0])
		rightCard := o.estimator.EstimateCardinality(children[1])
		buildCost := rightCard * o.costModel.constants.HashBuildCost
		probeCost := leftCard * o.costModel.constants.HashProbeCost

		return leftCost + rightCost + buildCost + probeCost + outputCard*o.costModel.constants.CPUTupleCost

	case "LogicalAggregate":
		childCost := o.estimatePlanCost(plan.PlanChildren()[0])
		inputCard := o.estimator.EstimateCardinality(plan.PlanChildren()[0])
		outputCard := o.estimator.EstimateCardinality(plan)
		hashCost := inputCard * o.costModel.constants.HashBuildCost
		return childCost + hashCost + outputCard*o.costModel.constants.CPUTupleCost

	case "LogicalSort":
		childCost := o.estimatePlanCost(plan.PlanChildren()[0])
		cardinality := o.estimator.EstimateCardinality(plan)
		if cardinality < 1 {
			cardinality = 1
		}
		logCard := log2(cardinality)
		if logCard < 1 {
			logCard = 1
		}
		sortCost := cardinality * logCard * o.costModel.constants.SortCost
		return childCost + sortCost

	case "LogicalLimit":
		// Limit can short-circuit, so we only pay for processed rows
		childCost := o.estimatePlanCost(plan.PlanChildren()[0])
		if limitNode, ok := plan.(LimitNode); ok {
			limit := limitNode.GetLimit()
			offset := limitNode.GetOffset()
			childCard := o.estimator.EstimateCardinality(plan.PlanChildren()[0])
			totalNeeded := float64(limit + offset)
			if totalNeeded > 0 && totalNeeded < childCard {
				fraction := totalNeeded / childCard
				return childCost * fraction
			}
		}
		return childCost

	case "LogicalDistinct":
		childCost := o.estimatePlanCost(plan.PlanChildren()[0])
		inputCard := o.estimator.EstimateCardinality(plan.PlanChildren()[0])
		outputCard := o.estimator.EstimateCardinality(plan)
		hashCost := inputCard * o.costModel.constants.HashBuildCost
		return childCost + hashCost + outputCard*o.costModel.constants.CPUTupleCost

	case "LogicalDummyScan":
		return o.costModel.constants.CPUTupleCost

	default:
		// For unknown plan types, sum up children costs
		children := plan.PlanChildren()
		if len(children) == 0 {
			return o.costModel.constants.CPUTupleCost
		}
		var totalCost float64
		for _, child := range children {
			totalCost += o.estimatePlanCost(child)
		}
		return totalCost
	}
}

// generateJoinHints generates join hints from an optimized join plan.
func (o *CostBasedOptimizer) generateJoinHints(joinPlan *JoinPlan) map[string]JoinHint {
	hints := make(map[string]JoinHint)

	if joinPlan == nil {
		return hints
	}

	for i, step := range joinPlan.JoinOrder {
		// Create a hint key based on join step
		key := fmt.Sprintf("join_%d", i)

		// Determine join method based on predicate
		method := PlanTypeHashJoin
		if step.Predicate == nil || !step.Predicate.IsEquality {
			method = PlanTypeNestedLoopJoin
		}

		hints[key] = JoinHint{
			Method:    method,
			BuildSide: step.BuildSide,
		}
	}

	return hints
}

// generateAccessHints generates access hints for all scans in a plan.
func (o *CostBasedOptimizer) generateAccessHints(plan LogicalPlanNode) map[string]AccessHint {
	hints := make(map[string]AccessHint)
	o.collectAccessHints(plan, hints)
	return hints
}

// collectAccessHints recursively collects access hints from a plan tree.
func (o *CostBasedOptimizer) collectAccessHints(plan LogicalPlanNode, hints map[string]AccessHint) {
	if plan == nil {
		return
	}

	if plan.PlanType() == "LogicalScan" {
		if scan, ok := plan.(ScanNode); ok {
			tableName := scan.TableName()
			if tableName == "" {
				tableName = scan.Alias()
			}

			// For now, default to sequential scan
			// In the future, we could check for available indexes and filter selectivity
			hints[tableName] = AccessHint{
				Method:    PlanTypeSeqScan,
				IndexName: "",
			}
		}
	}

	// Recurse into children
	for _, child := range plan.PlanChildren() {
		o.collectAccessHints(child, hints)
	}
}

// countJoins counts the number of join nodes in a plan tree.
func countJoins(plan LogicalPlanNode) int {
	if plan == nil {
		return 0
	}

	count := 0
	if plan.PlanType() == "LogicalJoin" || plan.PlanType() == "LogicalLateralJoin" {
		count = 1
	}

	for _, child := range plan.PlanChildren() {
		count += countJoins(child)
	}

	return count
}

// extractTables extracts table references from a plan tree.
func extractTables(plan LogicalPlanNode) []TableRef {
	var tables []TableRef
	collectTables(plan, &tables)
	return tables
}

// collectTables recursively collects table references from a plan tree.
func collectTables(plan LogicalPlanNode, tables *[]TableRef) {
	if plan == nil {
		return
	}

	if plan.PlanType() == "LogicalScan" {
		if scan, ok := plan.(ScanNode); ok {
			// Skip table functions and virtual tables
			if !scan.IsTableFunction() && !scan.IsVirtualTable() {
				ref := TableRef{
					Schema: scan.Schema(),
					Table:  scan.TableName(),
					Alias:  scan.Alias(),
				}

				// Estimate cardinality for this table
				*tables = append(*tables, ref)
			}
		}
	}

	// Recurse into children
	for _, child := range plan.PlanChildren() {
		collectTables(child, tables)
	}
}

// extractJoinPredicatesFromPlan extracts join predicates from a plan tree.
func extractJoinPredicatesFromPlan(plan LogicalPlanNode) []JoinPredicate {
	var predicates []JoinPredicate
	collectJoinPredicates(plan, &predicates)
	return predicates
}

// collectJoinPredicates recursively collects join predicates from a plan tree.
func collectJoinPredicates(plan LogicalPlanNode, predicates *[]JoinPredicate) {
	if plan == nil {
		return
	}

	if plan.PlanType() == "LogicalJoin" {
		if join, ok := plan.(JoinNode); ok {
			condition := join.JoinCondition()
			if condition != nil {
				// Extract tables from left and right children
				leftTables := getTableNames(join.LeftChild())
				rightTables := getTableNames(join.RightChild())

				// Extract predicates from condition
				preds := ExtractJoinPredicates(condition, leftTables, rightTables)
				*predicates = append(*predicates, preds...)
			}
		}
	}

	// Recurse into children
	for _, child := range plan.PlanChildren() {
		collectJoinPredicates(child, predicates)
	}
}

// getTableNames extracts table names from a plan subtree.
func getTableNames(plan LogicalPlanNode) []string {
	var names []string
	collectTableNames(plan, &names)
	return names
}

// collectTableNames recursively collects table names from a plan subtree.
func collectTableNames(plan LogicalPlanNode, names *[]string) {
	if plan == nil {
		return
	}

	if plan.PlanType() == "LogicalScan" {
		if scan, ok := plan.(ScanNode); ok {
			name := scan.Alias()
			if name == "" {
				name = scan.TableName()
			}
			*names = append(*names, name)
		}
	}

	for _, child := range plan.PlanChildren() {
		collectTableNames(child, names)
	}
}

// log2 returns the base-2 logarithm of x.
func log2(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// log2(x) = ln(x) / ln(2)
	return 1.4426950408889634 * logNatural(x)
}

// logNatural returns the natural logarithm using a simple implementation.
// This avoids importing math package just for log.
func logNatural(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Use Newton's method approximation
	// ln(x) where x = 2^n * m (1 <= m < 2)
	// ln(x) = n * ln(2) + ln(m)

	// Count powers of 2
	n := 0
	for x >= 2 {
		x /= 2
		n++
	}
	for x < 1 {
		x *= 2
		n--
	}

	// Now 1 <= x < 2
	// Use Taylor series for ln(1 + y) where y = x - 1
	y := x - 1
	result := 0.0
	power := y
	for i := 1; i <= 20; i++ {
		if i%2 == 1 {
			result += power / float64(i)
		} else {
			result -= power / float64(i)
		}
		power *= y
	}

	return result + float64(n)*0.6931471805599453 // n * ln(2)
}

// EstimatePlanCost estimates the cost of executing a logical plan.
// This is a public method that wraps the internal cost estimation.
func (o *CostBasedOptimizer) EstimatePlanCost(plan LogicalPlanNode) PlanCost {
	if plan == nil {
		return PlanCost{}
	}

	cardinality := o.estimator.EstimateCardinality(plan)
	width := o.estimator.EstimateRowWidth(plan)
	totalCost := o.estimatePlanCost(plan)

	return PlanCost{
		StartupCost: 0,
		TotalCost:   totalCost,
		OutputRows:  cardinality,
		OutputWidth: width,
	}
}

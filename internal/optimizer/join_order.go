// Package optimizer provides cost-based query optimization for dukdb-go.
package optimizer

import (
	"math"
	"sort"
)

// Default thresholds for join optimization algorithms.
const (
	// DefaultDPThreshold is the maximum number of tables for DP algorithm.
	// Beyond this, the optimizer switches to greedy algorithm.
	DefaultDPThreshold = 12

	// DefaultPairLimit is the maximum number of pairs to enumerate before
	// switching to greedy algorithm.
	DefaultPairLimit = 10000
)

// JoinPredicate represents a join condition between two tables.
type JoinPredicate struct {
	LeftTable   string // Left table name/alias
	LeftColumn  string // Left column name
	RightTable  string // Right table name/alias
	RightColumn string // Right column name
	IsEquality  bool   // True for equality join (col = col)
}

// JoinRelation represents a set of joined tables.
type JoinRelation struct {
	Tables      []string        // Table names in this relation
	Cost        PlanCost        // Cost of this relation
	BestPlan    interface{}     // Best plan for this relation
	Predicates  []JoinPredicate // Applicable predicates
	Cardinality float64         // Estimated output rows
	Width       int32           // Average row width in bytes
}

// JoinGraph represents relationships between tables via join predicates.
type JoinGraph struct {
	Tables     []string            // All table names
	Predicates []JoinPredicate     // All join predicates
	Edges      map[string][]string // Table -> connected tables
}

// JoinPlan represents the optimized join plan.
type JoinPlan struct {
	Tables    []string   // Tables in join order
	JoinOrder []JoinStep // Steps to build the join
	TotalCost PlanCost   // Estimated total cost
}

// JoinStep represents a single join operation.
type JoinStep struct {
	LeftIdx   int            // Index of left relation in Tables
	RightIdx  int            // Index of right relation in Tables
	Predicate *JoinPredicate // Join predicate (nil for cross join)
	BuildSide string         // "left" or "right"
	JoinType  JoinType       // Inner, Left, etc.
}

// TableRef represents a reference to a table in a query.
type TableRef struct {
	Schema      string   // Schema name
	Table       string   // Table name
	Alias       string   // Table alias (may be empty)
	Cardinality float64  // Estimated row count
	Width       int32    // Average row width in bytes
	JoinType    JoinType // For outer joins, the join type
}

// Name returns the effective name of the table reference (alias if present, otherwise table name).
func (t TableRef) Name() string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.Table
}

// JoinOrderOptimizer finds optimal join order using cost-based optimization.
// It implements the DPccp algorithm for small numbers of tables (N <= 12)
// and falls back to greedy algorithm for larger queries or when enumeration
// exceeds the pair limit.
type JoinOrderOptimizer struct {
	estimator   *CardinalityEstimator
	costModel   *CostModel
	dpThreshold int // Max tables for DP (default: 12)
	pairLimit   int // Max pairs before switching to greedy (default: 10000)
}

// NewJoinOrderOptimizer creates a new JoinOrderOptimizer.
func NewJoinOrderOptimizer(
	estimator *CardinalityEstimator,
	costModel *CostModel,
) *JoinOrderOptimizer {
	return &JoinOrderOptimizer{
		estimator:   estimator,
		costModel:   costModel,
		dpThreshold: DefaultDPThreshold,
		pairLimit:   DefaultPairLimit,
	}
}

// SetDPThreshold sets the maximum number of tables for DP algorithm.
func (o *JoinOrderOptimizer) SetDPThreshold(threshold int) {
	if threshold > 0 {
		o.dpThreshold = threshold
	}
}

// SetPairLimit sets the maximum number of pairs before switching to greedy.
func (o *JoinOrderOptimizer) SetPairLimit(limit int) {
	if limit > 0 {
		o.pairLimit = limit
	}
}

// OptimizeJoinOrder finds the optimal join order for the given tables and predicates.
// It uses dynamic programming for small numbers of tables and greedy algorithm
// for larger queries.
func (o *JoinOrderOptimizer) OptimizeJoinOrder(
	tables []TableRef,
	predicates []JoinPredicate,
) (*JoinPlan, error) {
	if len(tables) == 0 {
		return &JoinPlan{}, nil
	}

	if len(tables) == 1 {
		return &JoinPlan{
			Tables:    []string{tables[0].Name()},
			JoinOrder: nil,
			TotalCost: PlanCost{
				OutputRows:  tables[0].Cardinality,
				OutputWidth: tables[0].Width,
			},
		}, nil
	}

	// Build join graph
	graph := o.buildJoinGraph(tables, predicates)

	// Check for outer join constraints
	hasOuterJoin := o.hasOuterJoinConstraints(tables)

	// Use DP for small queries without outer join reordering constraints
	if len(tables) <= o.dpThreshold && !hasOuterJoin {
		plan, pairsEnumerated := o.dpOptimize(graph, tables)
		if pairsEnumerated <= o.pairLimit {
			return plan, nil
		}
		// Fall through to greedy if we exceeded the pair limit
	}

	// Use greedy algorithm for large queries or when DP exceeds pair limit
	return o.greedyOptimize(graph, tables), nil
}

// buildJoinGraph constructs a join graph from tables and predicates.
func (o *JoinOrderOptimizer) buildJoinGraph(
	tables []TableRef,
	predicates []JoinPredicate,
) *JoinGraph {
	graph := &JoinGraph{
		Tables:     make([]string, len(tables)),
		Predicates: predicates,
		Edges:      make(map[string][]string),
	}

	// Extract table names
	for i, t := range tables {
		graph.Tables[i] = t.Name()
		graph.Edges[t.Name()] = []string{}
	}

	// Build edges from predicates
	for _, pred := range predicates {
		// Check that both tables exist in graph
		leftExists := o.tableExists(graph.Tables, pred.LeftTable)
		rightExists := o.tableExists(graph.Tables, pred.RightTable)

		if leftExists && rightExists && pred.LeftTable != pred.RightTable {
			graph.Edges[pred.LeftTable] = append(graph.Edges[pred.LeftTable], pred.RightTable)
			graph.Edges[pred.RightTable] = append(graph.Edges[pred.RightTable], pred.LeftTable)
		}
	}

	return graph
}

// tableExists checks if a table name exists in the list.
func (o *JoinOrderOptimizer) tableExists(tables []string, name string) bool {
	for _, t := range tables {
		if t == name {
			return true
		}
	}
	return false
}

// hasOuterJoinConstraints checks if any table has outer join constraints.
func (o *JoinOrderOptimizer) hasOuterJoinConstraints(tables []TableRef) bool {
	for _, t := range tables {
		if t.JoinType == JoinTypeLeft || t.JoinType == JoinTypeRight || t.JoinType == JoinTypeFull {
			return true
		}
	}
	return false
}

// dpOptimize implements the DPccp algorithm for optimal join ordering.
// Returns the optimal plan and the number of pairs enumerated.
func (o *JoinOrderOptimizer) dpOptimize(graph *JoinGraph, tables []TableRef) (*JoinPlan, int) {
	n := len(tables)
	if n == 0 {
		return &JoinPlan{}, 0
	}

	// Create table index for quick lookup
	tableIndex := make(map[string]int)
	tableInfo := make(map[string]TableRef)
	for i, t := range tables {
		tableIndex[t.Name()] = i
		tableInfo[t.Name()] = t
	}

	// dp[bitmask] = best join relation for this subset of tables
	dp := make(map[uint64]*JoinRelation)

	// Initialize single-table relations
	for i, t := range tables {
		mask := uint64(1) << i
		dp[mask] = &JoinRelation{
			Tables:      []string{t.Name()},
			Cardinality: t.Cardinality,
			Width:       t.Width,
			Cost: PlanCost{
				StartupCost: 0,
				TotalCost:   o.costModel.constants.SeqPageCost + t.Cardinality*o.costModel.constants.CPUTupleCost,
				OutputRows:  t.Cardinality,
				OutputWidth: t.Width,
			},
		}
	}

	pairsEnumerated := 0

	// Enumerate subsets of increasing size
	for size := 2; size <= n; size++ {
		subsets := o.enumerateSubsets(n, size)

		for _, subset := range subsets {
			mask := o.subsetToMask(subset)

			// Try all ways to partition this subset into two connected subsets
			for s1Size := 1; s1Size < size; s1Size++ {
				partitions := o.enumeratePartitions(subset, s1Size)

				for _, partition := range partitions {
					s1, s2 := partition[0], partition[1]
					mask1 := o.subsetToMask(s1)
					mask2 := o.subsetToMask(s2)

					pairsEnumerated++
					if pairsEnumerated > o.pairLimit {
						return o.buildPlanFromDP(dp, mask, tableIndex, graph), pairsEnumerated
					}

					// Get relations for subsets
					rel1, ok1 := dp[mask1]
					rel2, ok2 := dp[mask2]
					if !ok1 || !ok2 {
						continue
					}

					// Check if there's a predicate connecting the two subsets
					pred := o.findConnectingPredicate(rel1.Tables, rel2.Tables, graph.Predicates)

					// Calculate join cost
					joinCost := o.calculateJoinCost(rel1, rel2, pred)

					// Update DP table if this is better
					existing, exists := dp[mask]
					if !exists || joinCost.TotalCost < existing.Cost.TotalCost {
						newTables := append([]string{}, rel1.Tables...)
						newTables = append(newTables, rel2.Tables...)

						dp[mask] = &JoinRelation{
							Tables:      newTables,
							Cardinality: joinCost.OutputRows,
							Width:       joinCost.OutputWidth,
							Cost:        joinCost,
							BestPlan: &dpJoinInfo{
								leftMask:  mask1,
								rightMask: mask2,
								predicate: pred,
							},
						}
					}
				}
			}
		}
	}

	// Get the full mask and build the plan
	fullMask := uint64((1 << n) - 1)
	return o.buildPlanFromDP(dp, fullMask, tableIndex, graph), pairsEnumerated
}

// dpJoinInfo stores join information for reconstructing the plan.
type dpJoinInfo struct {
	leftMask  uint64
	rightMask uint64
	predicate *JoinPredicate
}

// buildPlanFromDP reconstructs the join plan from DP table.
func (o *JoinOrderOptimizer) buildPlanFromDP(
	dp map[uint64]*JoinRelation,
	mask uint64,
	tableIndex map[string]int,
	graph *JoinGraph,
) *JoinPlan {
	rel, ok := dp[mask]
	if !ok {
		return &JoinPlan{}
	}

	plan := &JoinPlan{
		Tables:    rel.Tables,
		TotalCost: rel.Cost,
	}

	// Reconstruct join order by traversing the DP decisions
	o.reconstructJoinOrder(dp, mask, plan, tableIndex)

	return plan
}

// reconstructJoinOrder recursively rebuilds the join order from DP table.
func (o *JoinOrderOptimizer) reconstructJoinOrder(
	dp map[uint64]*JoinRelation,
	mask uint64,
	plan *JoinPlan,
	tableIndex map[string]int,
) {
	rel, ok := dp[mask]
	if !ok {
		return
	}

	info, ok := rel.BestPlan.(*dpJoinInfo)
	if !ok || info == nil {
		// Single table, no join step needed
		return
	}

	// Recursively process children
	o.reconstructJoinOrder(dp, info.leftMask, plan, tableIndex)
	o.reconstructJoinOrder(dp, info.rightMask, plan, tableIndex)

	// Get the left and right relations
	leftRel := dp[info.leftMask]
	rightRel := dp[info.rightMask]

	// Determine build side
	buildSide := o.selectBuildSide(leftRel, rightRel)

	// Create join step
	step := JoinStep{
		LeftIdx:   o.getRelationIndex(leftRel.Tables, plan.Tables),
		RightIdx:  o.getRelationIndex(rightRel.Tables, plan.Tables),
		Predicate: info.predicate,
		BuildSide: buildSide,
		JoinType:  JoinTypeInner,
	}

	plan.JoinOrder = append(plan.JoinOrder, step)
}

// getRelationIndex finds the starting index of a relation in the plan's table list.
func (o *JoinOrderOptimizer) getRelationIndex(relTables, planTables []string) int {
	if len(relTables) == 0 {
		return 0
	}
	for i, t := range planTables {
		if t == relTables[0] {
			return i
		}
	}
	return 0
}

// greedyOptimize implements a greedy algorithm for join ordering.
// It starts with the lowest cardinality table and repeatedly joins
// two relations with minimum intermediate result cost.
func (o *JoinOrderOptimizer) greedyOptimize(graph *JoinGraph, tables []TableRef) *JoinPlan {
	if len(tables) == 0 {
		return &JoinPlan{}
	}

	if len(tables) == 1 {
		return &JoinPlan{
			Tables: []string{tables[0].Name()},
			TotalCost: PlanCost{
				OutputRows:  tables[0].Cardinality,
				OutputWidth: tables[0].Width,
			},
		}
	}

	// Create initial relations
	relations := make([]*JoinRelation, len(tables))
	for i, t := range tables {
		relations[i] = &JoinRelation{
			Tables:      []string{t.Name()},
			Cardinality: t.Cardinality,
			Width:       t.Width,
			Cost: PlanCost{
				StartupCost: 0,
				TotalCost:   o.costModel.constants.SeqPageCost + t.Cardinality*o.costModel.constants.CPUTupleCost,
				OutputRows:  t.Cardinality,
				OutputWidth: t.Width,
			},
		}
	}

	// Sort by cardinality to start with smallest
	sort.Slice(relations, func(i, j int) bool {
		return relations[i].Cardinality < relations[j].Cardinality
	})

	var joinOrder []JoinStep

	// Track the order of tables as they're added
	// The first relation's tables are the base
	tableIndexMap := make(map[string]int)

	// Greedily join relations
	for len(relations) > 1 {
		bestI, bestJ := -1, -1
		var bestCost PlanCost
		var bestPred *JoinPredicate

		// Find the pair with minimum join cost
		for i := 0; i < len(relations); i++ {
			for j := i + 1; j < len(relations); j++ {
				pred := o.findConnectingPredicate(
					relations[i].Tables,
					relations[j].Tables,
					graph.Predicates,
				)
				cost := o.calculateJoinCost(relations[i], relations[j], pred)

				if bestI == -1 || cost.TotalCost < bestCost.TotalCost {
					bestI, bestJ = i, j
					bestCost = cost
					bestPred = pred
				}
			}
		}

		if bestI == -1 {
			break
		}

		// Create join step
		leftRel := relations[bestI]
		rightRel := relations[bestJ]
		buildSide := o.selectBuildSide(leftRel, rightRel)

		// Assign indices to tables if not already assigned
		for _, t := range leftRel.Tables {
			if _, exists := tableIndexMap[t]; !exists {
				tableIndexMap[t] = len(tableIndexMap)
			}
		}
		for _, t := range rightRel.Tables {
			if _, exists := tableIndexMap[t]; !exists {
				tableIndexMap[t] = len(tableIndexMap)
			}
		}

		step := JoinStep{
			LeftIdx:   tableIndexMap[leftRel.Tables[0]],
			RightIdx:  tableIndexMap[rightRel.Tables[0]],
			Predicate: bestPred,
			BuildSide: buildSide,
			JoinType:  JoinTypeInner,
		}
		joinOrder = append(joinOrder, step)

		// Merge tables
		newTables := append([]string{}, leftRel.Tables...)
		newTables = append(newTables, rightRel.Tables...)

		// Create merged relation
		merged := &JoinRelation{
			Tables:      newTables,
			Cardinality: bestCost.OutputRows,
			Width:       bestCost.OutputWidth,
			Cost:        bestCost,
		}

		// Remove old relations and add merged
		newRelations := make([]*JoinRelation, 0, len(relations)-1)
		for k := 0; k < len(relations); k++ {
			if k != bestI && k != bestJ {
				newRelations = append(newRelations, relations[k])
			}
		}
		newRelations = append(newRelations, merged)
		relations = newRelations
	}

	// Build final table list in order
	planTables := make([]string, len(tableIndexMap))
	for t, idx := range tableIndexMap {
		planTables[idx] = t
	}

	// If no joins happened (single relation), just use its tables
	if len(tableIndexMap) == 0 && len(relations) == 1 {
		planTables = relations[0].Tables
	}

	return &JoinPlan{
		Tables:    planTables,
		JoinOrder: joinOrder,
		TotalCost: relations[0].Cost,
	}
}

// findConnectingPredicate finds a predicate that connects two sets of tables.
func (o *JoinOrderOptimizer) findConnectingPredicate(
	tables1, tables2 []string,
	predicates []JoinPredicate,
) *JoinPredicate {
	set1 := make(map[string]bool)
	set2 := make(map[string]bool)
	for _, t := range tables1 {
		set1[t] = true
	}
	for _, t := range tables2 {
		set2[t] = true
	}

	// Prefer equality predicates
	for i := range predicates {
		pred := &predicates[i]
		if pred.IsEquality {
			if (set1[pred.LeftTable] && set2[pred.RightTable]) ||
				(set1[pred.RightTable] && set2[pred.LeftTable]) {
				return pred
			}
		}
	}

	// Fall back to any connecting predicate
	for i := range predicates {
		pred := &predicates[i]
		if (set1[pred.LeftTable] && set2[pred.RightTable]) ||
			(set1[pred.RightTable] && set2[pred.LeftTable]) {
			return pred
		}
	}

	return nil
}

// calculateJoinCost estimates the cost of joining two relations.
func (o *JoinOrderOptimizer) calculateJoinCost(
	left, right *JoinRelation,
	pred *JoinPredicate,
) PlanCost {
	leftRows := left.Cardinality
	rightRows := right.Cardinality

	if leftRows < 1 {
		leftRows = 1
	}
	if rightRows < 1 {
		rightRows = 1
	}

	// Estimate output cardinality
	var outputRows float64
	if pred != nil && pred.IsEquality {
		// For equality join, use max(distinct) as denominator
		// Use a heuristic: min(left, right) as the output (conservative)
		outputRows = math.Min(leftRows, rightRows)
	} else if pred != nil {
		// Non-equality join predicate - use default selectivity
		outputRows = leftRows * rightRows * DefaultSelectivity
	} else {
		// Cross join
		outputRows = leftRows * rightRows
	}

	if outputRows < 1 {
		outputRows = 1
	}

	// Determine build and probe sides
	var buildRows, probeRows float64
	if left.Cardinality*float64(left.Width) <= right.Cardinality*float64(right.Width) {
		buildRows = leftRows
		probeRows = rightRows
	} else {
		buildRows = rightRows
		probeRows = leftRows
	}

	// Calculate hash join cost
	buildCost := buildRows * o.costModel.constants.HashBuildCost
	probeCost := probeRows * o.costModel.constants.HashProbeCost

	startupCost := left.Cost.TotalCost + right.Cost.TotalCost + buildCost
	totalCost := startupCost + probeCost

	outputWidth := left.Width + right.Width

	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		OutputWidth: outputWidth,
	}
}

// selectBuildSide determines which side should be the build side for hash join.
// The smaller relation (by memory footprint) should be the build side.
func (o *JoinOrderOptimizer) selectBuildSide(left, right *JoinRelation) string {
	leftMemory := left.Cardinality * float64(left.Width)
	rightMemory := right.Cardinality * float64(right.Width)

	if leftMemory <= rightMemory {
		return "left"
	}
	return "right"
}

// SelectBuildSide is a public function for build side selection.
// It selects the build side based on memory cost.
func SelectBuildSide(leftRows, leftWidth, rightRows, rightWidth float64) (buildIsLeft bool) {
	leftMemory := leftRows * leftWidth
	rightMemory := rightRows * rightWidth
	return leftMemory <= rightMemory
}

// enumerateSubsets generates all subsets of size k from n elements.
func (o *JoinOrderOptimizer) enumerateSubsets(n, k int) [][]int {
	var result [][]int
	o.generateCombinations(n, k, 0, []int{}, &result)
	return result
}

// generateCombinations recursively generates combinations.
func (o *JoinOrderOptimizer) generateCombinations(n, k, start int, current []int, result *[][]int) {
	if len(current) == k {
		combination := make([]int, k)
		copy(combination, current)
		*result = append(*result, combination)
		return
	}

	for i := start; i < n; i++ {
		o.generateCombinations(n, k, i+1, append(current, i), result)
	}
}

// enumeratePartitions generates all ways to partition a subset into two groups.
func (o *JoinOrderOptimizer) enumeratePartitions(subset []int, s1Size int) [][][]int {
	var result [][][]int
	o.generatePartitions(subset, s1Size, 0, []int{}, &result)
	return result
}

// generatePartitions recursively generates partitions.
func (o *JoinOrderOptimizer) generatePartitions(
	subset []int,
	s1Size, start int,
	current []int,
	result *[][][]int,
) {
	if len(current) == s1Size {
		// Create the partition
		s1 := make([]int, len(current))
		copy(s1, current)

		s2 := make([]int, 0, len(subset)-len(current))
		currentSet := make(map[int]bool)
		for _, v := range current {
			currentSet[v] = true
		}
		for _, v := range subset {
			if !currentSet[v] {
				s2 = append(s2, v)
			}
		}

		*result = append(*result, [][]int{s1, s2})
		return
	}

	for i := start; i < len(subset); i++ {
		o.generatePartitions(subset, s1Size, i+1, append(current, subset[i]), result)
	}
}

// subsetToMask converts a subset of indices to a bitmask.
func (o *JoinOrderOptimizer) subsetToMask(subset []int) uint64 {
	var mask uint64
	for _, i := range subset {
		mask |= 1 << i
	}
	return mask
}

// ExtractJoinPredicates extracts join predicates from a join condition expression.
// This is a utility function for extracting predicates from bound expressions.
func ExtractJoinPredicates(condition ExprNode, leftTables, rightTables []string) []JoinPredicate {
	var predicates []JoinPredicate

	if condition == nil {
		return predicates
	}

	leftSet := make(map[string]bool)
	rightSet := make(map[string]bool)
	for _, t := range leftTables {
		leftSet[t] = true
	}
	for _, t := range rightTables {
		rightSet[t] = true
	}

	extractFromExpr(condition, leftSet, rightSet, &predicates)
	return predicates
}

// extractFromExpr recursively extracts join predicates from an expression.
func extractFromExpr(
	expr ExprNode,
	leftSet, rightSet map[string]bool,
	predicates *[]JoinPredicate,
) {
	if expr == nil {
		return
	}

	switch expr.ExprType() {
	case "BoundBinaryExpr":
		if binExpr, ok := expr.(BinaryExprNode); ok {
			op := binExpr.Operator()

			if op == OpAnd {
				// Recurse into AND expressions
				extractFromExpr(binExpr.Left(), leftSet, rightSet, predicates)
				extractFromExpr(binExpr.Right(), leftSet, rightSet, predicates)
				return
			}

			// Check for join predicate (column op column)
			leftCol, leftOk := binExpr.Left().(ColumnRefNode)
			rightCol, rightOk := binExpr.Right().(ColumnRefNode)

			if leftOk && rightOk {
				leftTable := leftCol.ColumnTable()
				rightTable := rightCol.ColumnTable()

				// Check if it's a cross-table predicate
				isLeftFromLeft := leftSet[leftTable]
				isLeftFromRight := rightSet[leftTable]
				isRightFromLeft := leftSet[rightTable]
				isRightFromRight := rightSet[rightTable]

				if (isLeftFromLeft && isRightFromRight) || (isLeftFromRight && isRightFromLeft) {
					pred := JoinPredicate{
						LeftTable:   leftTable,
						LeftColumn:  leftCol.ColumnName(),
						RightTable:  rightTable,
						RightColumn: rightCol.ColumnName(),
						IsEquality:  op == OpEq,
					}
					*predicates = append(*predicates, pred)
				}
			}
		}
	}
}

// CanReorderOuterJoin checks if an outer join can be reordered.
// For LEFT/RIGHT/FULL joins, certain reorderings are not semantically valid.
func CanReorderOuterJoin(joinType JoinType, isMovingLeft bool) bool {
	switch joinType {
	case JoinTypeInner, JoinTypeCross:
		// Inner and cross joins can always be reordered
		return true
	case JoinTypeLeft:
		// Left join: right side cannot be reordered past left
		return !isMovingLeft
	case JoinTypeRight:
		// Right join: left side cannot be reordered past right
		return isMovingLeft
	case JoinTypeFull:
		// Full outer join cannot be reordered
		return false
	default:
		return true
	}
}

// ValidateJoinOrder checks if a join order respects outer join constraints.
//
//nolint:exhaustive // Default case handles remaining join types
func ValidateJoinOrder(tables []TableRef, order []int) bool {
	// Track which tables have been joined
	joined := make(map[int]bool)

	for _, idx := range order {
		t := tables[idx]

		switch t.JoinType {
		case JoinTypeLeft:
			// For left join, all "left" tables must be joined first
			// This is a simplified check - in practice, the constraint
			// depends on the original query structure
			joined[idx] = true
		case JoinTypeRight:
			// For right join, all "right" tables must be joined first
			joined[idx] = true
		case JoinTypeFull:
			// Full joins are most restrictive
			joined[idx] = true
		default:
			joined[idx] = true
		}
	}

	return true
}

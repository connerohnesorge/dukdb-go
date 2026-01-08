// Package optimizer provides cost-based query optimization for dukdb-go.
package optimizer

// PlanEnumerator thresholds for join and access method selection.
const (
	// NestedLoopThreshold is the maximum inner cardinality for nested loop join.
	// When inner side has fewer than this many rows, NLJ may be preferred.
	NestedLoopThreshold = 100

	// IndexScanSelectivity is the threshold below which index scan is preferred.
	// When filter selectivity is less than 10%, index scan is beneficial.
	IndexScanSelectivity = 0.1

	// BTreeTraversalMultiplier is the cost multiplier for B-tree index traversal.
	BTreeTraversalMultiplier = 10
)

// PhysicalPlanType represents the type of physical plan operator.
type PhysicalPlanType string

// Physical plan types for joins and scans.
const (
	PlanTypeHashJoin       PhysicalPlanType = "HashJoin"
	PlanTypeNestedLoopJoin PhysicalPlanType = "NestedLoopJoin"
	PlanTypeSortMergeJoin  PhysicalPlanType = "SortMergeJoin"
	PlanTypeIndexScan      PhysicalPlanType = "IndexScan"
	PlanTypeIndexRangeScan PhysicalPlanType = "IndexRangeScan"
	PlanTypeSeqScan        PhysicalPlanType = "SeqScan"
)

// PlanProperties describes physical properties of a plan.
// These properties can be used for interesting order propagation.
type PlanProperties struct {
	SortedBy    []string // Columns the output is sorted by
	Partitioned bool     // Whether data is partitioned
}

// PhysicalAlternative represents one physical plan option.
// The optimizer generates multiple alternatives and selects the lowest cost.
type PhysicalAlternative struct {
	PlanType   PhysicalPlanType // Type of physical plan
	Cost       PlanCost         // Estimated cost
	Properties PlanProperties   // Physical properties
	BuildSide  string           // For joins: "left" or "right"
}

// PlanEnumerator generates physical plan alternatives for a given logical plan.
// It evaluates different implementation strategies and selects the lowest cost option.
type PlanEnumerator struct {
	estimator *CardinalityEstimator
	costModel *CostModel
	stats     *StatisticsManager
}

// NewPlanEnumerator creates a new PlanEnumerator with the given components.
func NewPlanEnumerator(
	estimator *CardinalityEstimator,
	costModel *CostModel,
	stats *StatisticsManager,
) *PlanEnumerator {
	return &PlanEnumerator{
		estimator: estimator,
		costModel: costModel,
		stats:     stats,
	}
}

// EnumerateJoinMethods generates physical join alternatives for a join operation.
// It considers hash join, nested loop join, and sort-merge join based on
// the characteristics of the inputs.
//
// Parameters:
//   - leftCard, rightCard: cardinality of left and right inputs
//   - leftWidth, rightWidth: row width in bytes for left and right inputs
//   - hasEquiJoin: true if there's an equality join condition
//   - leftSorted, rightSorted: columns the inputs are sorted by
//   - joinKeys: columns used in the join condition
//
// Returns a slice of physical alternatives sorted by cost (lowest first).
func (e *PlanEnumerator) EnumerateJoinMethods(
	leftCard, rightCard float64,
	leftWidth, rightWidth int32,
	hasEquiJoin bool,
	leftSorted, rightSorted []string,
	joinKeys []string,
) []PhysicalAlternative {
	var alternatives []PhysicalAlternative

	// Ensure minimum cardinalities
	if leftCard < 1 {
		leftCard = 1
	}
	if rightCard < 1 {
		rightCard = 1
	}

	// Create cost objects for join calculation
	leftCost := PlanCost{
		OutputRows:  leftCard,
		OutputWidth: leftWidth,
		TotalCost:   leftCard * e.costModel.constants.CPUTupleCost,
	}
	rightCost := PlanCost{
		OutputRows:  rightCard,
		OutputWidth: rightWidth,
		TotalCost:   rightCard * e.costModel.constants.CPUTupleCost,
	}

	// Estimate output cardinality
	outputRows := e.estimateJoinOutput(leftCard, rightCard, hasEquiJoin)

	// 1. Hash Join (default for equi-joins)
	if hasEquiJoin {
		hashJoinAlts := e.enumerateHashJoin(leftCost, rightCost, outputRows)
		alternatives = append(alternatives, hashJoinAlts...)
	}

	// 2. Nested Loop Join
	nljAlt := e.enumerateNestedLoopJoin(leftCost, rightCost, outputRows)
	alternatives = append(alternatives, nljAlt...)

	// 3. Sort-Merge Join (if both inputs are sorted on join keys)
	if hasEquiJoin && e.ShouldUseSortMergeJoin(leftSorted, rightSorted, joinKeys) {
		smjAlt := e.enumerateSortMergeJoin(leftCost, rightCost, outputRows, leftSorted, rightSorted)
		alternatives = append(alternatives, smjAlt...)
	}

	// Sort by total cost
	sortAlternatives(alternatives)

	return alternatives
}

// enumerateHashJoin generates hash join alternatives with different build sides.
func (e *PlanEnumerator) enumerateHashJoin(
	leftCost, rightCost PlanCost,
	outputRows float64,
) []PhysicalAlternative {
	var alternatives []PhysicalAlternative

	// Option 1: Build on right, probe on left
	buildRightParams := HashJoinParams{
		Left:       leftCost,
		Right:      rightCost,
		BuildRows:  rightCost.OutputRows,
		ProbeRows:  leftCost.OutputRows,
		OutputRows: outputRows,
	}
	buildRightCost := e.costModel.costHashJoin(buildRightParams)

	alternatives = append(alternatives, PhysicalAlternative{
		PlanType:  PlanTypeHashJoin,
		Cost:      buildRightCost,
		BuildSide: "right",
	})

	// Option 2: Build on left, probe on right
	buildLeftParams := HashJoinParams{
		Left:       rightCost, // swap for cost calculation
		Right:      leftCost,
		BuildRows:  leftCost.OutputRows,
		ProbeRows:  rightCost.OutputRows,
		OutputRows: outputRows,
	}
	buildLeftCost := e.costModel.costHashJoin(buildLeftParams)
	// Adjust output width
	buildLeftCost.OutputWidth = leftCost.OutputWidth + rightCost.OutputWidth

	alternatives = append(alternatives, PhysicalAlternative{
		PlanType:  PlanTypeHashJoin,
		Cost:      buildLeftCost,
		BuildSide: "left",
	})

	return alternatives
}

// enumerateNestedLoopJoin generates nested loop join alternatives.
func (e *PlanEnumerator) enumerateNestedLoopJoin(
	leftCost, rightCost PlanCost,
	outputRows float64,
) []PhysicalAlternative {
	var alternatives []PhysicalAlternative

	// Option 1: Left as outer
	leftOuterCost := e.costModel.costNestedLoopJoin(
		leftCost, rightCost,
		leftCost.OutputRows, outputRows,
	)

	alternatives = append(alternatives, PhysicalAlternative{
		PlanType:  PlanTypeNestedLoopJoin,
		Cost:      leftOuterCost,
		BuildSide: "right", // inner side is like build side
	})

	// Option 2: Right as outer
	rightOuterCost := e.costModel.costNestedLoopJoin(
		rightCost, leftCost,
		rightCost.OutputRows, outputRows,
	)

	alternatives = append(alternatives, PhysicalAlternative{
		PlanType:  PlanTypeNestedLoopJoin,
		Cost:      rightOuterCost,
		BuildSide: "left",
	})

	return alternatives
}

// enumerateSortMergeJoin generates sort-merge join alternatives.
func (e *PlanEnumerator) enumerateSortMergeJoin(
	leftCost, rightCost PlanCost,
	outputRows float64,
	leftSorted, _ []string,
) []PhysicalAlternative {
	var alternatives []PhysicalAlternative

	// Sort-merge join cost: merge cost is linear in both inputs
	// If already sorted, no sort cost needed
	mergeCost := (leftCost.OutputRows + rightCost.OutputRows) * e.costModel.constants.CPUTupleCost

	totalCost := leftCost.TotalCost + rightCost.TotalCost + mergeCost

	smjCost := PlanCost{
		StartupCost: leftCost.StartupCost + rightCost.StartupCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		OutputWidth: leftCost.OutputWidth + rightCost.OutputWidth,
	}

	// Determine which columns the output is sorted by
	var outputSorted []string
	if len(leftSorted) > 0 {
		outputSorted = leftSorted
	}

	alternatives = append(alternatives, PhysicalAlternative{
		PlanType: PlanTypeSortMergeJoin,
		Cost:     smjCost,
		Properties: PlanProperties{
			SortedBy: outputSorted,
		},
	})

	return alternatives
}

// estimateJoinOutput estimates the output cardinality for a join.
func (*PlanEnumerator) estimateJoinOutput(
	leftCard, rightCard float64,
	hasEquiJoin bool,
) float64 {
	if hasEquiJoin {
		// For equality join, use the smaller cardinality as a conservative estimate
		return minFloat64(leftCard, rightCard)
	}
	// For non-equality or cross join
	return leftCard * rightCard * DefaultSelectivity
}

// SelectBestJoin selects the lowest cost join method from alternatives.
// Returns nil if no alternatives are provided.
func (*PlanEnumerator) SelectBestJoin(alternatives []PhysicalAlternative) *PhysicalAlternative {
	if len(alternatives) == 0 {
		return nil
	}

	best := &alternatives[0]
	for i := 1; i < len(alternatives); i++ {
		if alternatives[i].Cost.TotalCost < best.Cost.TotalCost {
			best = &alternatives[i]
		}
	}

	return best
}

// EnumerateAccessMethods generates physical scan alternatives for a table scan.
// It considers sequential scan and index scan based on available indexes
// and filter selectivity.
//
// Parameters:
//   - tableName: name of the table being scanned
//   - schema: schema containing the table
//   - filterSelectivity: selectivity of any filter on the scan (0.0-1.0)
//   - hasIndex: true if an index exists on filtered columns
//   - indexColumns: columns covered by the index
//
// Returns a slice of physical alternatives sorted by cost (lowest first).
func (e *PlanEnumerator) EnumerateAccessMethods(
	tableName string,
	schema string,
	filterSelectivity float64,
	hasIndex bool,
	indexColumns []string,
) []PhysicalAlternative {
	var alternatives []PhysicalAlternative

	// Get table statistics
	rowCount := float64(DefaultRowCount)
	pageCount := float64(DefaultPageCount)
	rowWidth := widthDefault

	if e.stats != nil {
		tableStats := e.stats.GetTableStats(schema, tableName)
		if tableStats != nil {
			rowCount = float64(tableStats.RowCount)
			pageCount = float64(tableStats.PageCount)
			if len(tableStats.Columns) > 0 {
				// Calculate average row width
				var totalWidth int32
				for _, col := range tableStats.Columns {
					totalWidth += col.AvgWidth
				}
				rowWidth = totalWidth
			}
		}
	}

	// 1. Sequential Scan (always available)
	seqScanCost := e.costModel.costScan(rowCount, pageCount)
	seqScanCost.OutputWidth = rowWidth

	// Apply filter selectivity to output rows
	if filterSelectivity > 0 && filterSelectivity < 1 {
		seqScanCost.OutputRows = rowCount * filterSelectivity
		if seqScanCost.OutputRows < 1 {
			seqScanCost.OutputRows = 1
		}
	}

	alternatives = append(alternatives, PhysicalAlternative{
		PlanType: PlanTypeSeqScan,
		Cost:     seqScanCost,
	})

	// 2. Index Scan (if index exists and selectivity is low enough)
	if hasIndex && filterSelectivity > 0 && filterSelectivity < IndexScanSelectivity {
		indexScanCost := e.calculateIndexScanCost(rowCount, pageCount, rowWidth, filterSelectivity)
		indexScanCost.OutputWidth = rowWidth

		alternatives = append(alternatives, PhysicalAlternative{
			PlanType: PlanTypeIndexScan,
			Cost:     indexScanCost,
			Properties: PlanProperties{
				SortedBy: indexColumns,
			},
		})
	}

	// Sort by total cost
	sortAlternatives(alternatives)

	return alternatives
}

// calculateIndexScanCost calculates the cost of an index scan.
// Index scan has higher per-tuple cost but processes fewer tuples.
func (e *PlanEnumerator) calculateIndexScanCost(
	rowCount, pageCount float64,
	rowWidth int32,
	selectivity float64,
) PlanCost {
	// Rows accessed through index
	accessedRows := rowCount * selectivity
	if accessedRows < 1 {
		accessedRows = 1
	}

	// Pages accessed: random access pattern
	// Estimate pages based on selectivity
	accessedPages := pageCount * selectivity
	if accessedPages < 1 {
		accessedPages = 1
	}

	// Index lookup cost: log(rows) for B-tree traversal
	// plus random page cost for each accessed page
	indexLookupCost := e.costModel.constants.CPUOperatorCost * BTreeTraversalMultiplier
	pageCost := accessedPages * e.costModel.constants.RandomPageCost
	tupleCost := accessedRows * e.costModel.constants.CPUTupleCost

	return PlanCost{
		StartupCost: indexLookupCost,
		TotalCost:   indexLookupCost + pageCost + tupleCost,
		OutputRows:  accessedRows,
		OutputWidth: rowWidth,
	}
}

// SelectBestAccess selects the lowest cost access method from alternatives.
// Returns nil if no alternatives are provided.
func (*PlanEnumerator) SelectBestAccess(alternatives []PhysicalAlternative) *PhysicalAlternative {
	if len(alternatives) == 0 {
		return nil
	}

	best := &alternatives[0]
	for i := 1; i < len(alternatives); i++ {
		if alternatives[i].Cost.TotalCost < best.Cost.TotalCost {
			best = &alternatives[i]
		}
	}

	return best
}

// ShouldUseSortMergeJoin checks if sort-merge join is beneficial.
// Sort-merge join is beneficial when both inputs are already sorted
// on the join keys, avoiding the need for a separate sort operation.
//
// Parameters:
//   - leftSorted: columns the left input is sorted by
//   - rightSorted: columns the right input is sorted by
//   - joinKeys: columns used in the join condition
//
// Returns true if both inputs are sorted on compatible join keys.
func (*PlanEnumerator) ShouldUseSortMergeJoin(
	leftSorted, rightSorted, joinKeys []string,
) bool {
	if len(leftSorted) == 0 || len(rightSorted) == 0 || len(joinKeys) == 0 {
		return false
	}

	// Check if at least one join key is in the sorted columns
	// This is a simplified check - in practice, we'd need to match
	// specific key columns between left and right

	hasLeftKey := containsAny(leftSorted, joinKeys)
	hasRightKey := containsAny(rightSorted, joinKeys)

	return hasLeftKey && hasRightKey
}

// ShouldUseNestedLoopJoin determines if nested loop join is preferred.
// NLJ is preferred when the inner side is very small (< NestedLoopThreshold rows).
//
// Parameters:
//   - innerCard: cardinality of the inner (right) side
//   - hasEquiJoin: true if there's an equality join condition
//
// Returns true if NLJ should be considered as the primary join method.
func (*PlanEnumerator) ShouldUseNestedLoopJoin(
	innerCard float64,
	hasEquiJoin bool,
) bool {
	// NLJ is preferred when inner is very small
	if innerCard < NestedLoopThreshold {
		return true
	}

	// NLJ is required when there's no equi-join condition
	if !hasEquiJoin {
		return true
	}

	return false
}

// SelectBuildSideForHashJoin determines the optimal build side for hash join.
// The side with smaller memory footprint (rows * width) should be the build side.
//
// Parameters:
//   - leftCard, rightCard: cardinality of left and right inputs
//   - leftWidth, rightWidth: row width in bytes
//
// Returns "left" or "right" indicating which side should be the build side.
func (*PlanEnumerator) SelectBuildSideForHashJoin(
	leftCard, rightCard float64,
	leftWidth, rightWidth int32,
) string {
	leftMemory := leftCard * float64(leftWidth)
	rightMemory := rightCard * float64(rightWidth)

	if leftMemory <= rightMemory {
		return "left"
	}
	return "right"
}

// Helper functions

// sortAlternatives sorts alternatives by total cost (lowest first).
func sortAlternatives(alts []PhysicalAlternative) {
	// Simple bubble sort for small lists
	n := len(alts)
	for i := range n - 1 {
		for j := range n - i - 1 {
			if alts[j].Cost.TotalCost > alts[j+1].Cost.TotalCost {
				alts[j], alts[j+1] = alts[j+1], alts[j]
			}
		}
	}
}

// containsAny checks if any element of needles is in haystack.
func containsAny(haystack, needles []string) bool {
	haystackSet := make(map[string]bool, len(haystack))
	for _, h := range haystack {
		haystackSet[h] = true
	}

	for _, n := range needles {
		if haystackSet[n] {
			return true
		}
	}
	return false
}

// minFloat64 returns the minimum of two float64 values.
func minFloat64(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

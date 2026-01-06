// Package optimizer provides cost-based query optimization for dukdb-go.
package optimizer

import (
	"math"

	dukdb "github.com/dukdb/dukdb-go"
)

// CostConstants contains configurable cost constants for different hardware profiles.
// These values are empirically tuned for in-memory operations.
type CostConstants struct {
	SeqPageCost     float64 // Cost of sequential page read (default: 1.0)
	RandomPageCost  float64 // Cost of random page read (default: 4.0)
	CPUTupleCost    float64 // Cost per tuple processed (default: 0.01)
	CPUOperatorCost float64 // Cost per operator evaluation (default: 0.0025)
	HashBuildCost   float64 // Cost per tuple for hash table build (default: 0.02)
	HashProbeCost   float64 // Cost per tuple for hash table probe (default: 0.01)
	SortCost        float64 // Cost per comparison in sort (default: 0.05)
}

// DefaultCostConstants returns the default cost constants optimized for
// in-memory query execution. These values are based on PostgreSQL's cost
// model with adjustments for dukdb-go's architecture.
func DefaultCostConstants() CostConstants {
	return CostConstants{
		SeqPageCost:     1.0,
		RandomPageCost:  4.0,
		CPUTupleCost:    0.01,
		CPUOperatorCost: 0.0025,
		HashBuildCost:   0.02,
		HashProbeCost:   0.01,
		SortCost:        0.05,
	}
}

// PlanCost represents the estimated cost of executing a query plan.
// It separates startup cost (before first row is produced) from total cost
// (to produce all rows), enabling better pipelining analysis.
type PlanCost struct {
	StartupCost float64 // Cost before first row produced
	TotalCost   float64 // Cost to produce all rows
	OutputRows  float64 // Estimated output cardinality
	OutputWidth int32   // Average row width in bytes
}

// Add returns a new PlanCost that adds the costs of another plan.
// This is used to accumulate child costs.
func (p PlanCost) Add(other PlanCost) PlanCost {
	return PlanCost{
		StartupCost: p.StartupCost + other.StartupCost,
		TotalCost:   p.TotalCost + other.TotalCost,
		OutputRows:  p.OutputRows, // Don't add rows, keep this plan's output
		OutputWidth: p.OutputWidth,
	}
}

// Less returns true if this cost is less than another.
// Comparison is based on TotalCost.
func (p PlanCost) Less(other PlanCost) bool {
	return p.TotalCost < other.TotalCost
}

// PhysicalPlanNode is an interface for physical plan nodes used in cost estimation.
// This interface allows the CostModel to work with plan nodes without directly
// importing the planner package, avoiding import cycles.
type PhysicalPlanNode interface {
	// PhysicalPlanType returns a string identifier for the plan node type.
	PhysicalPlanType() string
	// PhysicalChildren returns the child plan nodes.
	PhysicalChildren() []PhysicalPlanNode
	// PhysicalOutputColumns returns the output column bindings.
	PhysicalOutputColumns() []PhysicalOutputColumn
}

// PhysicalOutputColumn represents a column in the physical plan output.
type PhysicalOutputColumn struct {
	Table     string
	Column    string
	Type      dukdb.Type
	TableIdx  int
	ColumnIdx int
}

// PhysicalScanNode represents a table scan operation.
type PhysicalScanNode interface {
	PhysicalPlanNode
	ScanSchema() string
	ScanTableName() string
	ScanAlias() string
	ScanRowCount() float64  // Estimated row count from statistics
	ScanPageCount() float64 // Estimated page count from statistics
}

// PhysicalFilterNode represents a filter operation.
type PhysicalFilterNode interface {
	PhysicalPlanNode
	FilterChild() PhysicalPlanNode
	FilterSelectivity() float64 // Estimated selectivity of the filter condition
}

// PhysicalProjectNode represents a projection operation.
type PhysicalProjectNode interface {
	PhysicalPlanNode
	ProjectChild() PhysicalPlanNode
	ProjectExpressionCount() int // Number of expressions being projected
}

// PhysicalHashJoinNode represents a hash join operation.
type PhysicalHashJoinNode interface {
	PhysicalPlanNode
	HashJoinLeft() PhysicalPlanNode
	HashJoinRight() PhysicalPlanNode // Build side
	HashJoinBuildRows() float64      // Estimated rows on build side
	HashJoinProbeRows() float64      // Estimated rows on probe side
	HashJoinOutputRows() float64     // Estimated output rows
}

// PhysicalNestedLoopJoinNode represents a nested loop join operation.
type PhysicalNestedLoopJoinNode interface {
	PhysicalPlanNode
	NLJOuter() PhysicalPlanNode
	NLJInner() PhysicalPlanNode
	NLJOuterRows() float64
	NLJOutputRows() float64
}

// PhysicalSortNode represents a sort operation.
type PhysicalSortNode interface {
	PhysicalPlanNode
	SortChild() PhysicalPlanNode
	SortRows() float64 // Estimated rows to sort
}

// PhysicalHashAggregateNode represents a hash aggregate operation.
type PhysicalHashAggregateNode interface {
	PhysicalPlanNode
	AggChild() PhysicalPlanNode
	AggInputRows() float64 // Estimated input rows
	AggGroups() float64    // Estimated number of groups
}

// PhysicalLimitNode represents a limit operation.
type PhysicalLimitNode interface {
	PhysicalPlanNode
	LimitChild() PhysicalPlanNode
	LimitValue() int64
	OffsetValue() int64
}

// CostModel estimates execution cost for physical plans.
// It uses configurable cost constants and cardinality estimates to
// compute realistic costs for plan comparison.
type CostModel struct {
	constants CostConstants
	estimator *CardinalityEstimator
}

// NewCostModel creates a new CostModel with the given constants and estimator.
func NewCostModel(constants CostConstants, estimator *CardinalityEstimator) *CostModel {
	return &CostModel{
		constants: constants,
		estimator: estimator,
	}
}

// EstimateCost calculates the estimated cost for a physical plan node.
// This is the main entry point for cost estimation. It recursively
// calculates costs for child nodes and combines them with the current
// node's cost.
//
//nolint:gocyclo // Switch on plan types requires multiple cases
func (m *CostModel) EstimateCost(plan PhysicalPlanNode) PlanCost {
	if plan == nil {
		return PlanCost{}
	}

	switch plan.PhysicalPlanType() {
	case "PhysicalScan":
		return m.estimateScanCost(plan)
	case "PhysicalFilter":
		return m.estimateFilterCost(plan)
	case "PhysicalProject":
		return m.estimateProjectCost(plan)
	case "PhysicalHashJoin":
		return m.estimateHashJoinCost(plan)
	case "PhysicalNestedLoopJoin":
		return m.estimateNLJCost(plan)
	case "PhysicalSort":
		return m.estimateSortCost(plan)
	case "PhysicalHashAggregate":
		return m.estimateAggregateCost(plan)
	case "PhysicalLimit":
		return m.estimateLimitCost(plan)
	case "PhysicalDistinct":
		return m.costDistinctFromChildren(plan)
	case "PhysicalWindow":
		return m.costWindowFromChildren(plan)
	case "PhysicalDummyScan":
		return m.dummyScanCost()
	default:
		return m.costFromChildren(plan)
	}
}

// estimateScanCost handles PhysicalScan cost estimation.
func (m *CostModel) estimateScanCost(plan PhysicalPlanNode) PlanCost {
	if scan, ok := plan.(PhysicalScanNode); ok {
		return m.costScan(scan.ScanRowCount(), scan.ScanPageCount())
	}

	return m.costScan(DefaultRowCount, DefaultPageCount)
}

// estimateFilterCost handles PhysicalFilter cost estimation.
func (m *CostModel) estimateFilterCost(plan PhysicalPlanNode) PlanCost {
	if filter, ok := plan.(PhysicalFilterNode); ok {
		childCost := m.EstimateCost(filter.FilterChild())

		return m.costFilter(childCost, childCost.OutputRows)
	}

	return m.costFilterFromChildren(plan)
}

// estimateProjectCost handles PhysicalProject cost estimation.
func (m *CostModel) estimateProjectCost(plan PhysicalPlanNode) PlanCost {
	if project, ok := plan.(PhysicalProjectNode); ok {
		childCost := m.EstimateCost(project.ProjectChild())

		return m.costProject(childCost, childCost.OutputRows, project.ProjectExpressionCount())
	}

	return m.costProjectFromChildren(plan)
}

// estimateHashJoinCost handles PhysicalHashJoin cost estimation.
func (m *CostModel) estimateHashJoinCost(plan PhysicalPlanNode) PlanCost {
	if join, ok := plan.(PhysicalHashJoinNode); ok {
		leftCost := m.EstimateCost(join.HashJoinLeft())
		rightCost := m.EstimateCost(join.HashJoinRight())
		params := HashJoinParams{
			Left:       leftCost,
			Right:      rightCost,
			BuildRows:  join.HashJoinBuildRows(),
			ProbeRows:  join.HashJoinProbeRows(),
			OutputRows: join.HashJoinOutputRows(),
		}

		return m.costHashJoin(params)
	}

	return m.costHashJoinFromChildren(plan)
}

// estimateNLJCost handles PhysicalNestedLoopJoin cost estimation.
func (m *CostModel) estimateNLJCost(plan PhysicalPlanNode) PlanCost {
	if nlj, ok := plan.(PhysicalNestedLoopJoinNode); ok {
		outerCost := m.EstimateCost(nlj.NLJOuter())
		innerCost := m.EstimateCost(nlj.NLJInner())

		return m.costNestedLoopJoin(outerCost, innerCost, nlj.NLJOuterRows(), nlj.NLJOutputRows())
	}

	return m.costNLJFromChildren(plan)
}

// estimateSortCost handles PhysicalSort cost estimation.
func (m *CostModel) estimateSortCost(plan PhysicalPlanNode) PlanCost {
	if sort, ok := plan.(PhysicalSortNode); ok {
		childCost := m.EstimateCost(sort.SortChild())

		return m.costSort(childCost, sort.SortRows())
	}

	return m.costSortFromChildren(plan)
}

// estimateAggregateCost handles PhysicalHashAggregate cost estimation.
func (m *CostModel) estimateAggregateCost(plan PhysicalPlanNode) PlanCost {
	if agg, ok := plan.(PhysicalHashAggregateNode); ok {
		childCost := m.EstimateCost(agg.AggChild())

		return m.costHashAggregate(childCost, agg.AggInputRows(), agg.AggGroups())
	}

	return m.costAggFromChildren(plan)
}

// estimateLimitCost handles PhysicalLimit cost estimation.
func (m *CostModel) estimateLimitCost(plan PhysicalPlanNode) PlanCost {
	if limit, ok := plan.(PhysicalLimitNode); ok {
		childCost := m.EstimateCost(limit.LimitChild())

		return m.costLimit(childCost, limit.LimitValue(), limit.OffsetValue())
	}

	return m.costLimitFromChildren(plan)
}

// dummyScanCost returns the cost for a dummy scan node.
func (m *CostModel) dummyScanCost() PlanCost {
	return PlanCost{
		StartupCost: 0,
		TotalCost:   m.constants.CPUTupleCost,
		OutputRows:  1,
		OutputWidth: widthDefault,
	}
}

// costScan calculates the cost of a sequential table scan.
// Formula: startup_cost = 0, total_cost = (pages * SeqPageCost) + (rows * CPUTupleCost)
func (m *CostModel) costScan(rows, pages float64) PlanCost {
	if rows < 1 {
		rows = 1
	}
	if pages < 1 {
		pages = 1
	}

	totalCost := (pages * m.constants.SeqPageCost) + (rows * m.constants.CPUTupleCost)

	return PlanCost{
		StartupCost: 0,
		TotalCost:   totalCost,
		OutputRows:  rows,
		OutputWidth: widthDefault,
	}
}

// costFilter calculates the cost of a filter operation.
// Formula: startup_cost = child_startup, total_cost = child_total + (input_rows * CPUOperatorCost)
func (m *CostModel) costFilter(child PlanCost, inputRows float64) PlanCost {
	if inputRows < 1 {
		inputRows = 1
	}

	// Filter doesn't change the number of pages to read, just evaluates condition
	filterCost := inputRows * m.constants.CPUOperatorCost

	// Output rows are reduced by selectivity (use default if not known)
	outputRows := inputRows * DefaultSelectivity
	if outputRows < 1 {
		outputRows = 1
	}

	return PlanCost{
		StartupCost: child.StartupCost,
		TotalCost:   child.TotalCost + filterCost,
		OutputRows:  outputRows,
		OutputWidth: child.OutputWidth,
	}
}

// costFilterWithSelectivity calculates the cost of a filter operation with known selectivity.
func (m *CostModel) costFilterWithSelectivity(child PlanCost, inputRows, selectivity float64) PlanCost {
	if inputRows < 1 {
		inputRows = 1
	}

	filterCost := inputRows * m.constants.CPUOperatorCost

	outputRows := inputRows * selectivity
	if outputRows < 1 {
		outputRows = 1
	}

	return PlanCost{
		StartupCost: child.StartupCost,
		TotalCost:   child.TotalCost + filterCost,
		OutputRows:  outputRows,
		OutputWidth: child.OutputWidth,
	}
}

// costProject calculates the cost of a projection operation.
// Formula: startup_cost = child_startup, total_cost = child_total + (input_rows * num_exprs * CPUOperatorCost)
func (m *CostModel) costProject(child PlanCost, inputRows float64, numExprs int) PlanCost {
	if inputRows < 1 {
		inputRows = 1
	}
	if numExprs < 1 {
		numExprs = 1
	}

	// Each expression evaluation costs CPUOperatorCost
	projectCost := inputRows * float64(numExprs) * m.constants.CPUOperatorCost

	return PlanCost{
		StartupCost: child.StartupCost,
		TotalCost:   child.TotalCost + projectCost,
		OutputRows:  inputRows, // Projection doesn't change row count
		OutputWidth: child.OutputWidth,
	}
}

// HashJoinParams contains parameters for hash join cost calculation.
type HashJoinParams struct {
	Left       PlanCost // Cost of probe side
	Right      PlanCost // Cost of build side
	BuildRows  float64  // Estimated rows on build side
	ProbeRows  float64  // Estimated rows on probe side
	OutputRows float64  // Estimated output rows
}

// costHashJoin calculates the cost of a hash join operation.
// Formula:
//
//	startup_cost = build_rows * HashBuildCost + child_costs_startup
//	total_cost = startup_cost + (probe_rows * HashProbeCost) + child_costs_total
func (m *CostModel) costHashJoin(params HashJoinParams) PlanCost {
	buildRows := params.BuildRows
	probeRows := params.ProbeRows
	outputRows := params.OutputRows

	if buildRows < 1 {
		buildRows = 1
	}
	if probeRows < 1 {
		probeRows = 1
	}
	if outputRows < 1 {
		outputRows = 1
	}

	// Build phase: construct hash table from build side (right)
	buildCost := buildRows * m.constants.HashBuildCost

	// Startup cost includes building the hash table plus getting children ready
	startupCost := params.Left.StartupCost + params.Right.TotalCost + buildCost

	// Probe phase: probe hash table for each row on probe side (left)
	probeCost := probeRows * m.constants.HashProbeCost

	// Total cost is startup plus probing plus output processing
	totalCost := startupCost + probeCost + params.Left.TotalCost - params.Left.StartupCost

	// Output width is sum of both sides
	outputWidth := params.Left.OutputWidth + params.Right.OutputWidth

	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		OutputWidth: outputWidth,
	}
}

// costNestedLoopJoin calculates the cost of a nested loop join.
// Formula:
//
//	startup_cost = outer_startup
//	total_cost = outer_cost + (outer_rows * inner_cost)
func costNestedLoopJoin(outer, inner PlanCost, outerRows, outputRows float64) PlanCost {
	if outerRows < 1 {
		outerRows = 1
	}
	if outputRows < 1 {
		outputRows = 1
	}

	// For each outer row, we scan the entire inner relation
	// Inner cost is paid once per outer row
	totalCost := outer.TotalCost + (outerRows * inner.TotalCost)

	outputWidth := outer.OutputWidth + inner.OutputWidth

	return PlanCost{
		StartupCost: outer.StartupCost,
		TotalCost:   totalCost,
		OutputRows:  outputRows,
		OutputWidth: outputWidth,
	}
}

// costNestedLoopJoin is a method wrapper for the package-level function.
func (m *CostModel) costNestedLoopJoin(outer, inner PlanCost, outerRows, outputRows float64) PlanCost {
	return costNestedLoopJoin(outer, inner, outerRows, outputRows)
}

// costSort calculates the cost of a sort operation.
// Formula:
//
//	startup_cost = child_total + (rows * log2(rows) * SortCost)
//	total_cost = startup_cost + (rows * CPUTupleCost)
func (m *CostModel) costSort(child PlanCost, rows float64) PlanCost {
	if rows < 1 {
		rows = 1
	}

	// Sort requires all input before producing output
	// Sort cost is O(n log n) comparisons
	logRows := math.Log2(rows)
	if logRows < 1 {
		logRows = 1
	}
	sortCost := rows * logRows * m.constants.SortCost

	startupCost := child.TotalCost + sortCost

	// After sorting, output cost is linear scan
	outputCost := rows * m.constants.CPUTupleCost

	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   startupCost + outputCost,
		OutputRows:  rows,
		OutputWidth: child.OutputWidth,
	}
}

// costHashAggregate calculates the cost of a hash aggregate operation.
// Formula:
//
//	startup_cost = child_total + (rows * HashBuildCost)
//	total_cost = startup_cost + (groups * CPUTupleCost)
func (m *CostModel) costHashAggregate(child PlanCost, inputRows, groups float64) PlanCost {
	if inputRows < 1 {
		inputRows = 1
	}
	if groups < 1 {
		groups = 1
	}

	// Build hash table for grouping
	hashCost := inputRows * m.constants.HashBuildCost

	startupCost := child.TotalCost + hashCost

	// Output cost is linear in number of groups
	outputCost := groups * m.constants.CPUTupleCost

	return PlanCost{
		StartupCost: startupCost,
		TotalCost:   startupCost + outputCost,
		OutputRows:  groups,
		OutputWidth: child.OutputWidth, // Simplified: could compute actual aggregate width
	}
}

// costLimit calculates the cost of a limit operation.
// Limit can short-circuit execution, so we only pay for limit+offset rows.
func costLimit(child PlanCost, limit, offset int64) PlanCost {
	totalToProcess := float64(limit)
	if offset > 0 {
		totalToProcess += float64(offset)
	}

	if totalToProcess <= 0 || totalToProcess >= child.OutputRows {
		// No limit or limit exceeds available rows
		return child
	}

	// Fraction of child we need to process
	fraction := totalToProcess / child.OutputRows
	if fraction > 1 {
		fraction = 1
	}

	// Startup cost is proportionally less if we can stop early
	// But we still need child's startup cost
	runCost := (child.TotalCost - child.StartupCost) * fraction

	outputRows := float64(limit)
	if outputRows > child.OutputRows {
		outputRows = child.OutputRows
	}
	if offset > 0 && float64(offset) < child.OutputRows {
		remaining := child.OutputRows - float64(offset)
		if outputRows > remaining {
			outputRows = remaining
		}
	}

	return PlanCost{
		StartupCost: child.StartupCost,
		TotalCost:   child.StartupCost + runCost,
		OutputRows:  outputRows,
		OutputWidth: child.OutputWidth,
	}
}

// costLimit is a method wrapper for the package-level function.
func (m *CostModel) costLimit(child PlanCost, limit, offset int64) PlanCost {
	return costLimit(child, limit, offset)
}

// Fallback cost estimation methods when specific interfaces are not implemented

func (m *CostModel) costFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{
			StartupCost: 0,
			TotalCost:   m.constants.CPUTupleCost,
			OutputRows:  1,
			OutputWidth: widthDefault,
		}
	}

	// Sum up child costs
	var total PlanCost
	for _, child := range children {
		childCost := m.EstimateCost(child)
		total = total.Add(childCost)
		if total.OutputRows < childCost.OutputRows {
			total.OutputRows = childCost.OutputRows
		}
		if total.OutputWidth < childCost.OutputWidth {
			total.OutputWidth = childCost.OutputWidth
		}
	}

	return total
}

func (m *CostModel) costFilterFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])

	return m.costFilter(childCost, childCost.OutputRows)
}

func (m *CostModel) costProjectFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])
	cols := plan.PhysicalOutputColumns()

	return m.costProject(childCost, childCost.OutputRows, len(cols))
}

func (m *CostModel) costHashJoinFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) < 2 {
		return PlanCost{}
	}
	leftCost := m.EstimateCost(children[0])
	rightCost := m.EstimateCost(children[1])

	// Default: right side is build, left side is probe
	buildRows := rightCost.OutputRows
	probeRows := leftCost.OutputRows
	// Default join cardinality
	outputRows := probeRows * DefaultSelectivity

	params := HashJoinParams{
		Left:       leftCost,
		Right:      rightCost,
		BuildRows:  buildRows,
		ProbeRows:  probeRows,
		OutputRows: outputRows,
	}

	return m.costHashJoin(params)
}

func (m *CostModel) costNLJFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) < 2 {
		return PlanCost{}
	}
	outerCost := m.EstimateCost(children[0])
	innerCost := m.EstimateCost(children[1])

	outputRows := outerCost.OutputRows * innerCost.OutputRows * DefaultSelectivity

	return m.costNestedLoopJoin(outerCost, innerCost, outerCost.OutputRows, outputRows)
}

func (m *CostModel) costSortFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])

	return m.costSort(childCost, childCost.OutputRows)
}

func (m *CostModel) costAggFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])

	// Default: assume sqrt(rows) groups
	groups := math.Sqrt(childCost.OutputRows)
	if groups < 1 {
		groups = 1
	}

	return m.costHashAggregate(childCost, childCost.OutputRows, groups)
}

func (m *CostModel) costLimitFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])

	// Default limit: assume 100 rows
	return m.costLimit(childCost, 100, 0)
}

func (m *CostModel) costDistinctFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])

	// DISTINCT is similar to hash aggregate on all columns
	// Estimate distinct rows as sqrt of input
	groups := math.Sqrt(childCost.OutputRows)
	if groups < 1 {
		groups = 1
	}

	return m.costHashAggregate(childCost, childCost.OutputRows, groups)
}

func (m *CostModel) costWindowFromChildren(plan PhysicalPlanNode) PlanCost {
	children := plan.PhysicalChildren()
	if len(children) == 0 {
		return PlanCost{}
	}
	childCost := m.EstimateCost(children[0])

	// Window functions typically require sorting by partition/order keys
	// and maintaining running state
	sortCost := m.costSort(childCost, childCost.OutputRows)

	// Additional cost for window function evaluation
	windowCost := childCost.OutputRows * m.constants.CPUOperatorCost

	return PlanCost{
		StartupCost: sortCost.StartupCost,
		TotalCost:   sortCost.TotalCost + windowCost,
		OutputRows:  childCost.OutputRows,                  // Window doesn't change row count
		OutputWidth: childCost.OutputWidth + widthInt64, // Add column for window result
	}
}

// GetConstants returns the cost constants used by this model.
func (m *CostModel) GetConstants() CostConstants {
	return m.constants
}

// SetConstants updates the cost constants used by this model.
func (m *CostModel) SetConstants(constants CostConstants) {
	m.constants = constants
}

// ComparePlans compares two plan costs and returns the cheaper one.
// Returns -1 if plan1 is cheaper, 1 if plan2 is cheaper, 0 if equal.
func ComparePlans(plan1, plan2 PlanCost) int {
	if plan1.TotalCost < plan2.TotalCost {
		return -1
	}
	if plan1.TotalCost > plan2.TotalCost {
		return 1
	}

	return 0
}

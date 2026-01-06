// Package optimizer provides cost-based query optimization for dukdb-go.
package optimizer

import (
	"math"

	dukdb "github.com/dukdb/dukdb-go"
)

// BinaryOp represents a binary operator used in expressions.
// These values must match parser.BinaryOp.
type BinaryOp int

const (
	// Arithmetic operators
	OpAdd BinaryOp = iota
	OpSub
	OpMul
	OpDiv
	OpMod

	// Comparison operators
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe

	// Logical operators
	OpAnd
	OpOr

	// String operators
	OpLike
	OpILike
	OpNotLike
	OpNotILike

	// Other operators
	OpIn
	OpNotIn
	OpIs
	OpIsNot
	OpConcat

	// JSON operators
	OpJSONExtract
	OpJSONText
)

// UnaryOp represents a unary operator.
// These values must match parser.UnaryOp.
type UnaryOp int

const (
	OpNot UnaryOp = iota
	OpNeg
	OpPos
)

// JoinType represents the type of join.
// These values must match planner.JoinType.
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeCross
)

// LogicalPlanNode represents a node in the logical query plan.
// This interface allows the CardinalityEstimator to work with plan nodes
// without directly importing the planner package.
type LogicalPlanNode interface {
	// PlanType returns a string identifier for the plan node type.
	PlanType() string
	// Children returns the child plan nodes.
	PlanChildren() []LogicalPlanNode
	// OutputColumns returns the output column bindings.
	PlanOutputColumns() []OutputColumn
}

// OutputColumn represents a column in the plan output.
type OutputColumn struct {
	Table     string
	Column    string
	Type      dukdb.Type
	TableIdx  int
	ColumnIdx int
}

// ScanNode represents a table scan operation.
type ScanNode interface {
	LogicalPlanNode
	Schema() string
	TableName() string
	Alias() string
	IsTableFunction() bool
	IsVirtualTable() bool
}

// FilterNode represents a filter operation.
type FilterNode interface {
	LogicalPlanNode
	FilterChild() LogicalPlanNode
	FilterCondition() ExprNode
}

// JoinNode represents a join operation.
type JoinNode interface {
	LogicalPlanNode
	LeftChild() LogicalPlanNode
	RightChild() LogicalPlanNode
	GetJoinType() JoinType
	JoinCondition() ExprNode
}

// AggregateNode represents an aggregate operation.
type AggregateNode interface {
	LogicalPlanNode
	AggChild() LogicalPlanNode
	GroupByExprs() []ExprNode
}

// LimitNode represents a limit operation.
type LimitNode interface {
	LogicalPlanNode
	LimitChild() LogicalPlanNode
	GetLimit() int64
	GetOffset() int64
}

// DistinctOnNode represents a DISTINCT ON operation.
type DistinctOnNode interface {
	LogicalPlanNode
	DistinctOnChild() LogicalPlanNode
	DistinctOnExprs() []ExprNode
}

// SampleNode represents a SAMPLE operation.
type SampleNode interface {
	LogicalPlanNode
	SampleChild() LogicalPlanNode
	SampleValue() float64
	IsPercentage() bool
}

// PivotNode represents a PIVOT operation.
type PivotNode interface {
	LogicalPlanNode
	PivotSource() LogicalPlanNode
	PivotGroupBy() []ExprNode
}

// UnpivotNode represents an UNPIVOT operation.
type UnpivotNode interface {
	LogicalPlanNode
	UnpivotSource() LogicalPlanNode
	UnpivotColumnCount() int
}

// CTEScanNode represents a CTE scan operation.
type CTEScanNode interface {
	LogicalPlanNode
	CTEPlan() LogicalPlanNode
}

// RecursiveCTENode represents a recursive CTE operation.
type RecursiveCTENode interface {
	LogicalPlanNode
	BasePlan() LogicalPlanNode
}

// ExprNode represents an expression in the plan.
type ExprNode interface {
	// ExprType returns a string identifier for the expression type.
	ExprType() string
	// ResultType returns the result type of the expression.
	ExprResultType() dukdb.Type
}

// BinaryExprNode represents a binary expression.
type BinaryExprNode interface {
	ExprNode
	Left() ExprNode
	Right() ExprNode
	Operator() BinaryOp
}

// UnaryExprNode represents a unary expression.
type UnaryExprNode interface {
	ExprNode
	Operand() ExprNode
	UnaryOperator() UnaryOp
}

// ColumnRefNode represents a column reference.
type ColumnRefNode interface {
	ExprNode
	ColumnTable() string
	ColumnName() string
}

// LiteralNode represents a literal value.
type LiteralNode interface {
	ExprNode
	LiteralValue() any
}

// InListNode represents an IN list expression.
type InListNode interface {
	ExprNode
	InExpr() ExprNode
	InValues() []ExprNode
	IsNot() bool
}

// BetweenNode represents a BETWEEN expression.
type BetweenNode interface {
	ExprNode
	BetweenExpr() ExprNode
	LowBound() ExprNode
	HighBound() ExprNode
	IsNotBetween() bool
}

// CardinalityEstimator estimates output cardinality for plan nodes.
// It uses table and column statistics to provide accurate estimates
// for query optimization.
type CardinalityEstimator struct {
	stats *StatisticsManager
}

// NewCardinalityEstimator creates a new CardinalityEstimator.
func NewCardinalityEstimator(stats *StatisticsManager) *CardinalityEstimator {
	return &CardinalityEstimator{stats: stats}
}

// EstimateCardinality returns estimated rows for a logical plan node.
// This is the main entry point for cardinality estimation.
func (e *CardinalityEstimator) EstimateCardinality(plan LogicalPlanNode) float64 {
	if plan == nil {
		return DefaultRowCount
	}

	// Handle specific node types
	switch plan.PlanType() {
	case "LogicalScan":
		if scan, ok := plan.(ScanNode); ok {
			return e.estimateScan(scan)
		}
	case "LogicalFilter":
		if filter, ok := plan.(FilterNode); ok {
			return e.estimateFilter(filter)
		}
	case "LogicalJoin":
		if join, ok := plan.(JoinNode); ok {
			return e.estimateJoin(join)
		}
	case "LogicalLateralJoin":
		if join, ok := plan.(JoinNode); ok {
			return e.estimateLateralJoin(join)
		}
	case "LogicalAggregate":
		if agg, ok := plan.(AggregateNode); ok {
			return e.estimateAggregate(agg)
		}
	case "LogicalProject", "LogicalSort", "LogicalWindow":
		// These don't change cardinality
		children := plan.PlanChildren()
		if len(children) > 0 {
			return e.EstimateCardinality(children[0])
		}
	case "LogicalLimit":
		if limit, ok := plan.(LimitNode); ok {
			return e.estimateLimit(limit)
		}
	case "LogicalDistinct":
		children := plan.PlanChildren()
		if len(children) > 0 {
			return e.estimateDistinct(plan, children[0])
		}
	case "LogicalDistinctOn":
		if distinctOn, ok := plan.(DistinctOnNode); ok {
			return e.estimateDistinctOn(distinctOn)
		}
	case "LogicalDummyScan":
		return 1 // Dummy scan produces exactly one row
	case "LogicalCTEScan":
		if cte, ok := plan.(CTEScanNode); ok {
			return e.estimateCTEScan(cte)
		}
	case "LogicalRecursiveCTE":
		if cte, ok := plan.(RecursiveCTENode); ok {
			return e.estimateRecursiveCTE(cte)
		}
	case "LogicalPivot":
		if pivot, ok := plan.(PivotNode); ok {
			return e.estimatePivot(pivot)
		}
	case "LogicalUnpivot":
		if unpivot, ok := plan.(UnpivotNode); ok {
			return e.estimateUnpivot(unpivot)
		}
	case "LogicalSample":
		if sample, ok := plan.(SampleNode); ok {
			return e.estimateSample(sample)
		}
	}

	// Default to child cardinality or DefaultRowCount
	children := plan.PlanChildren()
	if len(children) > 0 {
		return e.EstimateCardinality(children[0])
	}
	return DefaultRowCount
}

// estimateScan returns the row count from table statistics.
func (e *CardinalityEstimator) estimateScan(scan ScanNode) float64 {
	// For table functions or virtual tables, use default cardinality
	if scan.IsTableFunction() || scan.IsVirtualTable() {
		return DefaultRowCount
	}

	// Get statistics for the table
	if e.stats == nil {
		return DefaultRowCount
	}

	tableStats := e.stats.GetTableStats(scan.Schema(), scan.TableName())
	if tableStats == nil {
		return DefaultRowCount
	}

	return float64(tableStats.RowCount)
}

// estimateFilter estimates cardinality after applying a filter condition.
// Result = child cardinality * selectivity of the condition.
func (e *CardinalityEstimator) estimateFilter(filter FilterNode) float64 {
	childCardinality := e.EstimateCardinality(filter.FilterChild())
	condition := filter.FilterCondition()
	if condition == nil {
		return childCardinality
	}

	// Extract table name from child for column statistics lookup
	tableName := e.extractTableName(filter.FilterChild())
	schemaName := e.extractSchemaName(filter.FilterChild())

	selectivity := e.estimateSelectivity(condition, schemaName, tableName)
	result := childCardinality * selectivity

	// Ensure at least 1 row
	if result < 1.0 {
		result = 1.0
	}
	return result
}

// estimateSelectivity estimates the selectivity of a filter expression.
// Selectivity is a value between 0 and 1 representing the fraction of rows
// that pass the filter.
func (e *CardinalityEstimator) estimateSelectivity(
	expr ExprNode,
	schema, table string,
) float64 {
	if expr == nil {
		return 1.0
	}

	switch expr.ExprType() {
	case "BoundBinaryExpr":
		if binExpr, ok := expr.(BinaryExprNode); ok {
			return e.estimateBinarySelectivity(binExpr, schema, table)
		}
	case "BoundUnaryExpr":
		if unaryExpr, ok := expr.(UnaryExprNode); ok {
			return e.estimateUnarySelectivity(unaryExpr, schema, table)
		}
	case "BoundBetweenExpr":
		if betweenExpr, ok := expr.(BetweenNode); ok {
			return e.estimateBetweenSelectivity(betweenExpr, schema, table)
		}
	case "BoundInListExpr":
		if inListExpr, ok := expr.(InListNode); ok {
			return e.estimateInListSelectivity(inListExpr, schema, table)
		}
	case "BoundLiteral":
		if lit, ok := expr.(LiteralNode); ok {
			if val, isBool := lit.LiteralValue().(bool); isBool {
				if val {
					return 1.0
				}
				return 0.0
			}
		}
		return DefaultSelectivity
	}

	return DefaultSelectivity
}

// estimateBinarySelectivity estimates selectivity for binary expressions.
//
//nolint:gocyclo // Complex switch is necessary for operator handling
func (e *CardinalityEstimator) estimateBinarySelectivity(
	expr BinaryExprNode,
	schema, table string,
) float64 {
	op := expr.Operator()

	switch op {
	case OpAnd:
		// AND: multiply selectivities (independence assumption)
		leftSel := e.estimateSelectivity(expr.Left(), schema, table)
		rightSel := e.estimateSelectivity(expr.Right(), schema, table)
		return leftSel * rightSel

	case OpOr:
		// OR: handle conservatively using max(s1, s2)
		leftSel := e.estimateSelectivity(expr.Left(), schema, table)
		rightSel := e.estimateSelectivity(expr.Right(), schema, table)
		return math.Max(leftSel, rightSel)

	case OpEq:
		// Equality: selectivity = 1 / distinct_count
		return e.estimateEqualitySelectivity(expr, schema, table)

	case OpNe:
		// Not equal: 1 - equality selectivity
		eqSel := e.estimateEqualitySelectivity(expr, schema, table)
		return 1.0 - eqSel

	case OpLt, OpLe, OpGt, OpGe:
		// Range predicates: use histogram if available, else default 0.2
		return e.estimateRangeSelectivity(expr, schema, table)

	case OpLike, OpILike:
		// LIKE predicates: use default selectivity 0.2
		return DefaultSelectivity

	case OpNotLike, OpNotILike:
		// NOT LIKE: 1 - LIKE selectivity
		return 1.0 - DefaultSelectivity

	case OpIs:
		// IS NULL or IS TRUE/FALSE
		return e.estimateIsSelectivity(expr, schema, table)

	case OpIsNot:
		// IS NOT NULL: 1 - null_fraction
		return 1.0 - e.estimateIsSelectivity(expr, schema, table)

	case OpIn:
		// IN: number of values / distinct count
		return e.estimateInSelectivity(expr, schema, table)

	case OpNotIn:
		// NOT IN: 1 - IN selectivity
		return 1.0 - e.estimateInSelectivity(expr, schema, table)

	default:
		return DefaultSelectivity
	}
}

// estimateUnarySelectivity estimates selectivity for unary expressions.
func (e *CardinalityEstimator) estimateUnarySelectivity(
	expr UnaryExprNode,
	schema, table string,
) float64 {
	switch expr.UnaryOperator() {
	case OpNot:
		// NOT: 1 - selectivity of the inner expression
		innerSel := e.estimateSelectivity(expr.Operand(), schema, table)
		return 1.0 - innerSel
	case OpNeg, OpPos:
		// Negation/positive don't affect selectivity
		return 1.0
	default:
		return DefaultSelectivity
	}
}

// estimateEqualitySelectivity estimates selectivity for equality predicates.
// selectivity = 1 / distinct_count
func (e *CardinalityEstimator) estimateEqualitySelectivity(
	expr BinaryExprNode,
	schema, table string,
) float64 {
	// Try to get column statistics
	colStats := e.getColumnStatsFromExpr(expr.Left(), schema, table)
	if colStats == nil {
		colStats = e.getColumnStatsFromExpr(expr.Right(), schema, table)
	}

	if colStats != nil && colStats.DistinctCount > 0 {
		return 1.0 / float64(colStats.DistinctCount)
	}

	// Fallback: use default distinct count
	return 1.0 / float64(DefaultDistinctCount)
}

// estimateRangeSelectivity estimates selectivity for range predicates.
// Uses histogram if available, else default 0.2.
func (e *CardinalityEstimator) estimateRangeSelectivity(
	expr BinaryExprNode,
	schema, table string,
) float64 {
	// Get column statistics
	colStats := e.getColumnStatsFromExpr(expr.Left(), schema, table)
	if colStats == nil {
		colStats = e.getColumnStatsFromExpr(expr.Right(), schema, table)
	}

	// Get the literal value from the expression
	var value any
	var colType dukdb.Type
	if lit, ok := expr.Right().(LiteralNode); ok {
		value = lit.LiteralValue()
		colType = lit.ExprResultType()
	} else if lit, ok := expr.Left().(LiteralNode); ok {
		value = lit.LiteralValue()
		colType = lit.ExprResultType()
	}

	// If we have histogram and value, use histogram-based estimation
	if colStats != nil && colStats.Histogram != nil && value != nil {
		return e.estimateRangeFromHistogram(colStats.Histogram, value, colType, expr.Operator())
	}

	// If we have min/max and value, use linear interpolation
	if colStats != nil && colStats.MinValue != nil && colStats.MaxValue != nil && value != nil {
		return e.estimateRangeFromMinMax(
			colStats.MinValue,
			colStats.MaxValue,
			value,
			colStats.ColumnType,
			expr.Operator(),
		)
	}

	// Default selectivity for range predicates
	return DefaultSelectivity
}

// estimateRangeFromHistogram estimates range selectivity using histogram.
func (e *CardinalityEstimator) estimateRangeFromHistogram(
	hist *Histogram,
	value any,
	colType dukdb.Type,
	op BinaryOp,
) float64 {
	if hist == nil || len(hist.Buckets) == 0 {
		return DefaultSelectivity
	}

	// Find the bucket containing the value using linear interpolation
	selectivity := 0.0

	for i, bucket := range hist.Buckets {
		compareToLower := compareValues(value, bucket.LowerBound, colType)
		compareToUpper := compareValues(value, bucket.UpperBound, colType)

		switch op {
		case OpLt:
			if compareToLower < 0 {
				break
			} else if compareToLower >= 0 && compareToUpper < 0 {
				fraction := e.interpolateBucketFraction(bucket, value, colType)
				selectivity += bucket.Frequency * fraction
				break
			} else {
				selectivity += bucket.Frequency
			}

		case OpLe:
			if compareToLower < 0 {
				break
			} else if compareToLower >= 0 && compareToUpper <= 0 {
				fraction := e.interpolateBucketFraction(bucket, value, colType)
				selectivity += bucket.Frequency * fraction
				break
			} else {
				selectivity += bucket.Frequency
			}

		case OpGt:
			if compareToUpper > 0 {
				continue
			} else if compareToLower >= 0 && compareToUpper <= 0 {
				fraction := 1.0 - e.interpolateBucketFraction(bucket, value, colType)
				selectivity += bucket.Frequency * fraction
			} else if compareToLower < 0 {
				for j := i; j < len(hist.Buckets); j++ {
					selectivity += hist.Buckets[j].Frequency
				}
				break
			}

		case OpGe:
			if compareToUpper > 0 {
				continue
			} else if compareToLower >= 0 && compareToUpper <= 0 {
				fraction := 1.0 - e.interpolateBucketFraction(bucket, value, colType)
				selectivity += bucket.Frequency * fraction
			} else if compareToLower <= 0 {
				for j := i; j < len(hist.Buckets); j++ {
					selectivity += hist.Buckets[j].Frequency
				}
				break
			}
		}
	}

	// Clamp selectivity to valid range
	if selectivity < 0 {
		selectivity = 0
	}
	if selectivity > 1 {
		selectivity = 1
	}

	return selectivity
}

// interpolateBucketFraction estimates what fraction of a bucket is before a value.
func (e *CardinalityEstimator) interpolateBucketFraction(
	bucket Bucket,
	value any,
	colType dukdb.Type,
) float64 {
	lower := toNumeric(bucket.LowerBound)
	upper := toNumeric(bucket.UpperBound)
	val := toNumeric(value)

	if upper == lower {
		return 0.5 // Avoid division by zero
	}

	fraction := (val - lower) / (upper - lower)
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}

	return fraction
}

// toNumeric converts a value to float64 for interpolation.
func toNumeric(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	case string:
		if len(val) > 0 {
			return float64(val[0])
		}
		return 0
	default:
		return 0
	}
}

// estimateRangeFromMinMax estimates range selectivity using min/max values.
func (e *CardinalityEstimator) estimateRangeFromMinMax(
	minVal, maxVal, value any,
	colType dukdb.Type,
	op BinaryOp,
) float64 {
	min := toNumeric(minVal)
	max := toNumeric(maxVal)
	val := toNumeric(value)

	if max == min {
		return DefaultSelectivity
	}

	position := (val - min) / (max - min)
	if position < 0 {
		position = 0
	}
	if position > 1 {
		position = 1
	}

	switch op {
	case OpLt, OpLe:
		return position
	case OpGt, OpGe:
		return 1.0 - position
	default:
		return DefaultSelectivity
	}
}

// estimateIsSelectivity estimates selectivity for IS NULL/IS NOT NULL.
func (e *CardinalityEstimator) estimateIsSelectivity(
	expr BinaryExprNode,
	schema, table string,
) float64 {
	// Check if this is IS NULL
	if lit, ok := expr.Right().(LiteralNode); ok && lit.LiteralValue() == nil {
		colStats := e.getColumnStatsFromExpr(expr.Left(), schema, table)
		if colStats != nil {
			return colStats.NullFraction
		}
		return 0.01
	}

	return DefaultSelectivity
}

// estimateInSelectivity estimates selectivity for IN expressions.
func (e *CardinalityEstimator) estimateInSelectivity(
	expr BinaryExprNode,
	schema, table string,
) float64 {
	colStats := e.getColumnStatsFromExpr(expr.Left(), schema, table)
	if colStats == nil {
		return DefaultSelectivity
	}
	return DefaultSelectivity
}

// estimateInListSelectivity estimates selectivity for IN list expressions.
func (e *CardinalityEstimator) estimateInListSelectivity(
	expr InListNode,
	schema, table string,
) float64 {
	colStats := e.getColumnStatsFromExpr(expr.InExpr(), schema, table)

	listSize := float64(len(expr.InValues()))
	if listSize == 0 {
		return 0.0
	}

	var distinctCount float64 = DefaultDistinctCount
	if colStats != nil && colStats.DistinctCount > 0 {
		distinctCount = float64(colStats.DistinctCount)
	}

	selectivity := listSize / distinctCount
	if selectivity > 1.0 {
		selectivity = 1.0
	}

	if expr.IsNot() {
		return 1.0 - selectivity
	}
	return selectivity
}

// estimateBetweenSelectivity estimates selectivity for BETWEEN expressions.
func (e *CardinalityEstimator) estimateBetweenSelectivity(
	expr BetweenNode,
	schema, table string,
) float64 {
	colStats := e.getColumnStatsFromExpr(expr.BetweenExpr(), schema, table)
	if colStats == nil {
		selectivity := DefaultSelectivity * DefaultSelectivity
		if expr.IsNotBetween() {
			return 1.0 - selectivity
		}
		return selectivity
	}

	var low, high any
	if lit, ok := expr.LowBound().(LiteralNode); ok {
		low = lit.LiteralValue()
	}
	if lit, ok := expr.HighBound().(LiteralNode); ok {
		high = lit.LiteralValue()
	}

	if colStats.Histogram != nil && low != nil && high != nil {
		lowSel := e.estimateRangeFromHistogram(colStats.Histogram, low, colStats.ColumnType, OpGe)
		highSel := e.estimateRangeFromHistogram(colStats.Histogram, high, colStats.ColumnType, OpLe)
		selectivity := math.Min(lowSel, highSel)
		if expr.IsNotBetween() {
			return 1.0 - selectivity
		}
		return selectivity
	}

	if colStats.MinValue != nil && colStats.MaxValue != nil && low != nil && high != nil {
		min := toNumeric(colStats.MinValue)
		max := toNumeric(colStats.MaxValue)
		lowVal := toNumeric(low)
		highVal := toNumeric(high)

		if max > min {
			selectivity := (highVal - lowVal) / (max - min)
			if selectivity < 0 {
				selectivity = 0
			}
			if selectivity > 1 {
				selectivity = 1
			}
			if expr.IsNotBetween() {
				return 1.0 - selectivity
			}
			return selectivity
		}
	}

	selectivity := DefaultSelectivity
	if expr.IsNotBetween() {
		return 1.0 - selectivity
	}
	return selectivity
}

// getColumnStatsFromExpr extracts column statistics from an expression.
// The schema and table parameters are the actual table identifiers for stats lookup.
// Column references may use table aliases, but we use the passed-in actual table name.
func (e *CardinalityEstimator) getColumnStatsFromExpr(
	expr ExprNode,
	schema, table string,
) *ColumnStatistics {
	if e.stats == nil {
		return nil
	}

	colRef, ok := expr.(ColumnRefNode)
	if !ok {
		return nil
	}

	// Use the passed-in table name, which should be the actual table name (not alias)
	// Column references often have aliases (like "o" for "orders"), but stats are
	// stored by actual table name
	return e.stats.GetColumnStats(schema, table, colRef.ColumnName())
}

// extractTableName extracts the table name from a logical plan node.
// Returns the actual table name (not alias) for statistics lookup.
func (e *CardinalityEstimator) extractTableName(plan LogicalPlanNode) string {
	if scan, ok := plan.(ScanNode); ok {
		return scan.TableName()
	}

	// Try children
	children := plan.PlanChildren()
	if len(children) > 0 {
		return e.extractTableName(children[0])
	}
	return ""
}

// extractTableAlias extracts the table alias from a logical plan node.
// Returns the alias if present, otherwise the table name.
func (e *CardinalityEstimator) extractTableAlias(plan LogicalPlanNode) string {
	if scan, ok := plan.(ScanNode); ok {
		if scan.Alias() != "" {
			return scan.Alias()
		}
		return scan.TableName()
	}

	// Try children
	children := plan.PlanChildren()
	if len(children) > 0 {
		return e.extractTableAlias(children[0])
	}
	return ""
}

// extractSchemaName extracts the schema name from a logical plan node.
func (e *CardinalityEstimator) extractSchemaName(plan LogicalPlanNode) string {
	if scan, ok := plan.(ScanNode); ok {
		return scan.Schema()
	}

	children := plan.PlanChildren()
	if len(children) > 0 {
		return e.extractSchemaName(children[0])
	}
	return ""
}

// estimateJoin estimates cardinality for join operations.
func (e *CardinalityEstimator) estimateJoin(join JoinNode) float64 {
	leftRows := e.EstimateCardinality(join.LeftChild())
	rightRows := e.EstimateCardinality(join.RightChild())

	switch join.GetJoinType() {
	case JoinTypeCross:
		return leftRows * rightRows

	case JoinTypeInner:
		if join.JoinCondition() == nil {
			return leftRows * rightRows
		}
		return e.estimateJoinWithCondition(join, leftRows, rightRows)

	case JoinTypeLeft:
		if join.JoinCondition() == nil {
			return leftRows * rightRows
		}
		estimated := e.estimateJoinWithCondition(join, leftRows, rightRows)
		return math.Max(estimated, leftRows)

	case JoinTypeRight:
		if join.JoinCondition() == nil {
			return leftRows * rightRows
		}
		estimated := e.estimateJoinWithCondition(join, leftRows, rightRows)
		return math.Max(estimated, rightRows)

	case JoinTypeFull:
		if join.JoinCondition() == nil {
			return leftRows * rightRows
		}
		estimated := e.estimateJoinWithCondition(join, leftRows, rightRows)
		return math.Max(estimated, math.Max(leftRows, rightRows))

	default:
		return leftRows * rightRows * DefaultSelectivity
	}
}

// estimateJoinWithCondition estimates join cardinality with a join condition.
func (e *CardinalityEstimator) estimateJoinWithCondition(
	join JoinNode,
	leftRows, rightRows float64,
) float64 {
	leftDistinct, rightDistinct := e.extractJoinKeyDistinct(join)

	if leftDistinct > 0 && rightDistinct > 0 {
		maxDistinct := math.Max(float64(leftDistinct), float64(rightDistinct))
		return (leftRows * rightRows) / maxDistinct
	}

	return leftRows * rightRows * DefaultSelectivity
}

// extractJoinKeyDistinct extracts distinct counts for join key columns.
func (e *CardinalityEstimator) extractJoinKeyDistinct(
	join JoinNode,
) (leftDistinct, rightDistinct int64) {
	condition := join.JoinCondition()
	if condition == nil {
		return 0, 0
	}

	binExpr, ok := condition.(BinaryExprNode)
	if !ok || binExpr.Operator() != OpEq {
		return 0, 0
	}

	if leftColRef, ok := binExpr.Left().(ColumnRefNode); ok {
		leftSchema := e.extractSchemaName(join.LeftChild())
		leftTable := e.extractTableName(join.LeftChild())
		if stats := e.stats.GetColumnStats(leftSchema, leftTable, leftColRef.ColumnName()); stats != nil {
			leftDistinct = stats.DistinctCount
		}
	}

	if rightColRef, ok := binExpr.Right().(ColumnRefNode); ok {
		rightSchema := e.extractSchemaName(join.RightChild())
		rightTable := e.extractTableName(join.RightChild())
		if stats := e.stats.GetColumnStats(rightSchema, rightTable, rightColRef.ColumnName()); stats != nil {
			rightDistinct = stats.DistinctCount
		}
	}

	return leftDistinct, rightDistinct
}

// estimateLateralJoin estimates cardinality for lateral joins.
func (e *CardinalityEstimator) estimateLateralJoin(join JoinNode) float64 {
	leftRows := e.EstimateCardinality(join.LeftChild())
	rightRows := e.EstimateCardinality(join.RightChild())
	return leftRows * rightRows
}

// estimateAggregate estimates cardinality for aggregate operations.
func (e *CardinalityEstimator) estimateAggregate(agg AggregateNode) float64 {
	childRows := e.EstimateCardinality(agg.AggChild())

	groupByExprs := agg.GroupByExprs()
	if len(groupByExprs) == 0 {
		return 1
	}

	// Get actual table info from child for stats lookup
	schema := e.extractSchemaName(agg.AggChild())
	table := e.extractTableName(agg.AggChild())

	distinctGroups := 1.0
	for _, groupExpr := range groupByExprs {
		colRef, ok := groupExpr.(ColumnRefNode)
		if !ok {
			distinctGroups *= math.Sqrt(childRows)
			continue
		}

		// Use the actual table name from the child, not the alias in the column reference
		colStats := e.stats.GetColumnStats(schema, table, colRef.ColumnName())
		if colStats != nil && colStats.DistinctCount > 0 {
			distinctGroups *= float64(colStats.DistinctCount)
		} else {
			distinctGroups *= math.Sqrt(childRows)
		}
	}

	if distinctGroups > childRows {
		distinctGroups = childRows
	}
	if distinctGroups < 1 {
		distinctGroups = 1
	}

	return distinctGroups
}

// estimateLimit estimates cardinality for limit operations.
func (e *CardinalityEstimator) estimateLimit(limit LimitNode) float64 {
	childRows := e.EstimateCardinality(limit.LimitChild())

	limitVal := limit.GetLimit()
	if limitVal >= 0 && float64(limitVal) < childRows {
		childRows = float64(limitVal)
	}

	offsetVal := limit.GetOffset()
	if offsetVal >= 0 {
		childRows -= float64(offsetVal)
		if childRows < 0 {
			childRows = 0
		}
	}

	return childRows
}

// estimateDistinct estimates cardinality for distinct operations.
func (e *CardinalityEstimator) estimateDistinct(plan, child LogicalPlanNode) float64 {
	childRows := e.EstimateCardinality(child)

	outputCols := child.PlanOutputColumns()
	if len(outputCols) == 0 {
		return childRows * DefaultSelectivity
	}

	distinctProduct := 1.0
	for _, col := range outputCols {
		schema := ""
		table := col.Table
		if scan, ok := child.(ScanNode); ok {
			schema = scan.Schema()
			if table == "" {
				table = scan.TableName()
			}
		}

		colStats := e.stats.GetColumnStats(schema, table, col.Column)
		if colStats != nil && colStats.DistinctCount > 0 {
			distinctProduct *= float64(colStats.DistinctCount)
		} else {
			distinctProduct *= math.Sqrt(childRows)
		}
	}

	result := math.Min(childRows, distinctProduct)
	if result < 1 {
		result = 1
	}
	return result
}

// estimateDistinctOn estimates cardinality for DISTINCT ON operations.
func (e *CardinalityEstimator) estimateDistinctOn(distinctOn DistinctOnNode) float64 {
	childRows := e.EstimateCardinality(distinctOn.DistinctOnChild())

	exprs := distinctOn.DistinctOnExprs()
	if len(exprs) == 0 {
		return childRows
	}

	distinctProduct := 1.0
	for _, expr := range exprs {
		colRef, ok := expr.(ColumnRefNode)
		if !ok {
			distinctProduct *= math.Sqrt(childRows)
			continue
		}

		schema := e.extractSchemaName(distinctOn.DistinctOnChild())
		table := colRef.ColumnTable()
		if table == "" {
			table = e.extractTableName(distinctOn.DistinctOnChild())
		}

		colStats := e.stats.GetColumnStats(schema, table, colRef.ColumnName())
		if colStats != nil && colStats.DistinctCount > 0 {
			distinctProduct *= float64(colStats.DistinctCount)
		} else {
			distinctProduct *= math.Sqrt(childRows)
		}
	}

	result := math.Min(childRows, distinctProduct)
	if result < 1 {
		result = 1
	}
	return result
}

// estimateCTEScan estimates cardinality for CTE scans.
func (e *CardinalityEstimator) estimateCTEScan(cte CTEScanNode) float64 {
	ctePlan := cte.CTEPlan()
	if ctePlan != nil {
		return e.EstimateCardinality(ctePlan)
	}
	return DefaultRowCount
}

// estimateRecursiveCTE estimates cardinality for recursive CTEs.
func (e *CardinalityEstimator) estimateRecursiveCTE(cte RecursiveCTENode) float64 {
	baseRows := e.EstimateCardinality(cte.BasePlan())
	const avgIterations = 10.0
	return baseRows * avgIterations
}

// estimatePivot estimates cardinality for PIVOT operations.
func (e *CardinalityEstimator) estimatePivot(pivot PivotNode) float64 {
	childRows := e.EstimateCardinality(pivot.PivotSource())

	groupByExprs := pivot.PivotGroupBy()
	if len(groupByExprs) == 0 {
		return 1
	}

	distinctGroups := 1.0
	for _, groupExpr := range groupByExprs {
		colRef, ok := groupExpr.(ColumnRefNode)
		if !ok {
			distinctGroups *= math.Sqrt(childRows)
			continue
		}

		schema := e.extractSchemaName(pivot.PivotSource())
		table := colRef.ColumnTable()
		if table == "" {
			table = e.extractTableName(pivot.PivotSource())
		}

		colStats := e.stats.GetColumnStats(schema, table, colRef.ColumnName())
		if colStats != nil && colStats.DistinctCount > 0 {
			distinctGroups *= float64(colStats.DistinctCount)
		} else {
			distinctGroups *= math.Sqrt(childRows)
		}
	}

	if distinctGroups > childRows {
		distinctGroups = childRows
	}
	if distinctGroups < 1 {
		distinctGroups = 1
	}

	return distinctGroups
}

// estimateUnpivot estimates cardinality for UNPIVOT operations.
func (e *CardinalityEstimator) estimateUnpivot(unpivot UnpivotNode) float64 {
	childRows := e.EstimateCardinality(unpivot.UnpivotSource())
	numCols := float64(unpivot.UnpivotColumnCount())
	if numCols < 1 {
		numCols = 1
	}
	return childRows * numCols
}

// estimateSample estimates cardinality for SAMPLE operations.
func (e *CardinalityEstimator) estimateSample(sample SampleNode) float64 {
	childRows := e.EstimateCardinality(sample.SampleChild())

	if sample.IsPercentage() {
		return childRows * sample.SampleValue() / 100.0
	}

	if sample.SampleValue() < childRows {
		return sample.SampleValue()
	}
	return childRows
}

// EstimateRowWidth calculates average row width in bytes for a plan node.
func (e *CardinalityEstimator) EstimateRowWidth(plan LogicalPlanNode) int32 {
	cols := plan.PlanOutputColumns()
	if len(cols) == 0 {
		return int32(widthDefault)
	}

	var totalWidth int32
	for _, col := range cols {
		totalWidth += EstimateTypeWidth(col.Type)
	}

	return totalWidth
}

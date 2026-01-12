// Package planner provides query planning for the native Go DuckDB implementation.
// This file implements Iceberg-specific planning logic for partition pruning and column projection.
package planner

import (
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// IcebergPlanner provides Iceberg-specific query planning functionality.
// It handles extraction of partition filters from predicates and column projection optimization.
type IcebergPlanner struct{}

// NewIcebergPlanner creates a new IcebergPlanner.
func NewIcebergPlanner() *IcebergPlanner {
	return &IcebergPlanner{}
}

// ExtractPartitionFilters analyzes a filter expression and extracts predicates
// that can be pushed down to Iceberg partition pruning.
// Returns the extracted partition filters and any residual filter that cannot be pushed down.
//
// Supported predicates for partition pruning:
//   - Equality: col = value
//   - Inequality: col != value, col <> value
//   - Range: col < value, col <= value, col > value, col >= value
//   - IN list: col IN (v1, v2, v3)
//   - IS NULL, IS NOT NULL
//
// Supported partition transforms:
//   - identity: Direct equality on partition column
//   - year: Year extraction from date/timestamp
//   - month: Month extraction from date/timestamp
//   - day: Day extraction from date/timestamp
//   - hour: Hour extraction from timestamp
//   - bucket: Hash bucket (requires knowing bucket count)
//   - truncate: Truncation (requires knowing width)
func (p *IcebergPlanner) ExtractPartitionFilters(
	filter binder.BoundExpr,
) ([]PartitionFilter, binder.BoundExpr) {
	if filter == nil {
		return nil, nil
	}

	var partitionFilters []PartitionFilter
	var residualPredicates []binder.BoundExpr

	// Extract conjuncts (AND-ed predicates)
	conjuncts := p.extractConjuncts(filter)

	for _, conjunct := range conjuncts {
		if pf, ok := p.tryExtractPartitionFilter(conjunct); ok {
			partitionFilters = append(partitionFilters, pf)
		} else {
			// Cannot push down - add to residual filter
			residualPredicates = append(residualPredicates, conjunct)
		}
	}

	// Reconstruct residual filter from remaining predicates
	residualFilter := p.combineConjuncts(residualPredicates)

	return partitionFilters, residualFilter
}

// extractConjuncts extracts all conjuncts from an AND expression.
// For example, (a = 1 AND b = 2 AND c = 3) returns [a = 1, b = 2, c = 3].
func (p *IcebergPlanner) extractConjuncts(expr binder.BoundExpr) []binder.BoundExpr {
	if expr == nil {
		return nil
	}

	// Check if this is an AND expression
	if binExpr, ok := expr.(*binder.BoundBinaryExpr); ok {
		if binExpr.Op == parser.OpAnd {
			// Recursively extract from both sides
			left := p.extractConjuncts(binExpr.Left)
			right := p.extractConjuncts(binExpr.Right)
			return append(left, right...)
		}
	}

	// Not an AND - this is a single conjunct
	return []binder.BoundExpr{expr}
}

// combineConjuncts combines a list of predicates with AND.
func (p *IcebergPlanner) combineConjuncts(predicates []binder.BoundExpr) binder.BoundExpr {
	if len(predicates) == 0 {
		return nil
	}

	if len(predicates) == 1 {
		return predicates[0]
	}

	// Combine with AND
	result := predicates[0]
	for i := 1; i < len(predicates); i++ {
		result = &binder.BoundBinaryExpr{
			Left:  result,
			Op:    parser.OpAnd,
			Right: predicates[i],
		}
	}

	return result
}

// tryExtractPartitionFilter attempts to convert a predicate to a partition filter.
// Returns the filter and true if successful, or nil and false if the predicate
// cannot be pushed down to partition pruning.
func (p *IcebergPlanner) tryExtractPartitionFilter(expr binder.BoundExpr) (PartitionFilter, bool) {
	switch e := expr.(type) {
	case *binder.BoundBinaryExpr:
		return p.tryExtractBinaryPartitionFilter(e)
	case *binder.BoundInListExpr:
		return p.tryExtractInListPartitionFilter(e)
	case *binder.BoundUnaryExpr:
		return p.tryExtractUnaryPartitionFilter(e)
	default:
		return PartitionFilter{}, false
	}
}

// tryExtractBinaryPartitionFilter attempts to extract a partition filter from a binary expression.
func (p *IcebergPlanner) tryExtractBinaryPartitionFilter(expr *binder.BoundBinaryExpr) (PartitionFilter, bool) {
	// Check if this is a comparison operator
	op := p.binaryOpToFilterOp(expr.Op)
	if op == "" {
		return PartitionFilter{}, false
	}

	// Try column = literal or literal = column
	colRef, literal, ok := p.extractColumnAndLiteral(expr.Left, expr.Right)
	if !ok {
		// Try reversed
		colRef, literal, ok = p.extractColumnAndLiteral(expr.Right, expr.Left)
		if !ok {
			return PartitionFilter{}, false
		}
		// Reverse the operator for reversed operands
		op = p.reverseCompareOp(op)
	}

	// Check for partition transform in column reference
	transform, transformArg := p.detectPartitionTransform(colRef)

	return PartitionFilter{
		FieldName:    colRef.Column,
		Operator:     op,
		Value:        literal.Value,
		Transform:    transform,
		TransformArg: transformArg,
	}, true
}

// tryExtractInListPartitionFilter attempts to extract a partition filter from an IN list expression.
func (p *IcebergPlanner) tryExtractInListPartitionFilter(expr *binder.BoundInListExpr) (PartitionFilter, bool) {
	// Check if the expression is a column reference
	colRef, ok := expr.Expr.(*binder.BoundColumnRef)
	if !ok {
		return PartitionFilter{}, false
	}

	// Extract all literal values
	values := make([]any, 0, len(expr.Values))
	for _, v := range expr.Values {
		lit, ok := v.(*binder.BoundLiteral)
		if !ok {
			return PartitionFilter{}, false
		}
		values = append(values, lit.Value)
	}

	op := "IN"
	if expr.Not {
		op = "NOT IN"
	}

	return PartitionFilter{
		FieldName: colRef.Column,
		Operator:  op,
		Value:     values,
		Transform: "identity",
	}, true
}

// tryExtractUnaryPartitionFilter attempts to extract a partition filter from a unary expression.
func (p *IcebergPlanner) tryExtractUnaryPartitionFilter(expr *binder.BoundUnaryExpr) (PartitionFilter, bool) {
	// Check for IS NULL or IS NOT NULL on a column
	if expr.Op == parser.OpIsNull || expr.Op == parser.OpIsNotNull {
		colRef, ok := expr.Expr.(*binder.BoundColumnRef)
		if !ok {
			return PartitionFilter{}, false
		}

		op := "IS NULL"
		if expr.Op == parser.OpIsNotNull {
			op = "IS NOT NULL"
		}

		return PartitionFilter{
			FieldName: colRef.Column,
			Operator:  op,
			Value:     nil,
			Transform: "identity",
		}, true
	}

	return PartitionFilter{}, false
}

// extractColumnAndLiteral extracts a column reference and literal from two expressions.
func (p *IcebergPlanner) extractColumnAndLiteral(
	left, right binder.BoundExpr,
) (*binder.BoundColumnRef, *binder.BoundLiteral, bool) {
	colRef, colOk := left.(*binder.BoundColumnRef)
	literal, litOk := right.(*binder.BoundLiteral)

	if colOk && litOk {
		return colRef, literal, true
	}

	return nil, nil, false
}

// binaryOpToFilterOp converts a parser binary operator to a filter operator string.
func (p *IcebergPlanner) binaryOpToFilterOp(op parser.BinaryOp) string {
	switch op {
	case parser.OpEq:
		return "="
	case parser.OpNe:
		return "!="
	case parser.OpLt:
		return "<"
	case parser.OpLe:
		return "<="
	case parser.OpGt:
		return ">"
	case parser.OpGe:
		return ">="
	default:
		return ""
	}
}

// reverseCompareOp reverses a comparison operator.
// For example, "<" becomes ">", "<=" becomes ">=".
func (p *IcebergPlanner) reverseCompareOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return op // = and != are symmetric
	}
}

// detectPartitionTransform detects partition transforms from function calls.
// For example, YEAR(col), MONTH(col), etc.
// Returns the transform name and optional transform argument.
func (p *IcebergPlanner) detectPartitionTransform(colRef *binder.BoundColumnRef) (string, int) {
	// For now, assume identity transform for direct column references.
	// TODO: Handle function-wrapped columns like YEAR(col), MONTH(col), etc.
	return "identity", 0
}

// ExtractColumnProjection extracts the column names referenced in a SELECT clause.
// This is used to push down column projection to the Iceberg reader.
func (p *IcebergPlanner) ExtractColumnProjection(
	columns []*binder.BoundSelectColumn,
) []string {
	// Use a map to deduplicate columns
	columnSet := make(map[string]struct{})

	for _, col := range columns {
		p.collectColumnReferences(col.Expr, columnSet)
	}

	// Convert map to slice
	result := make([]string, 0, len(columnSet))
	for colName := range columnSet {
		result = append(result, colName)
	}

	return result
}

// collectColumnReferences recursively collects column references from an expression.
func (p *IcebergPlanner) collectColumnReferences(expr binder.BoundExpr, columns map[string]struct{}) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *binder.BoundColumnRef:
		columns[e.Column] = struct{}{}

	case *binder.BoundBinaryExpr:
		p.collectColumnReferences(e.Left, columns)
		p.collectColumnReferences(e.Right, columns)

	case *binder.BoundUnaryExpr:
		p.collectColumnReferences(e.Expr, columns)

	case *binder.BoundFunctionCall:
		for _, arg := range e.Args {
			p.collectColumnReferences(arg, columns)
		}

	case *binder.BoundCastExpr:
		p.collectColumnReferences(e.Expr, columns)

	case *binder.BoundCaseExpr:
		if e.Operand != nil {
			p.collectColumnReferences(e.Operand, columns)
		}
		for _, when := range e.Whens {
			p.collectColumnReferences(when.Condition, columns)
			p.collectColumnReferences(when.Result, columns)
		}
		if e.Else != nil {
			p.collectColumnReferences(e.Else, columns)
		}

	case *binder.BoundBetweenExpr:
		p.collectColumnReferences(e.Expr, columns)
		p.collectColumnReferences(e.Low, columns)
		p.collectColumnReferences(e.High, columns)

	case *binder.BoundInListExpr:
		p.collectColumnReferences(e.Expr, columns)
		for _, v := range e.Values {
			p.collectColumnReferences(v, columns)
		}
	}
}

// ExtractFilterColumnReferences extracts column references from a filter expression.
// This is used to determine which columns are needed for filter evaluation.
func (p *IcebergPlanner) ExtractFilterColumnReferences(filter binder.BoundExpr) []string {
	columns := make(map[string]struct{})
	p.collectColumnReferences(filter, columns)

	result := make([]string, 0, len(columns))
	for colName := range columns {
		result = append(result, colName)
	}

	return result
}

// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

// ---------- GROUPING SETS/ROLLUP/CUBE Expression Types ----------

// GroupingSetType represents the type of grouping set operation.
type GroupingSetType int

const (
	// GroupingSetSimple represents explicit GROUPING SETS ((...), (...), ...).
	GroupingSetSimple GroupingSetType = iota
	// GroupingSetRollup represents ROLLUP(...) which generates hierarchical subtotals.
	GroupingSetRollup
	// GroupingSetCube represents CUBE(...) which generates all possible subtotals.
	GroupingSetCube
)

// GroupingSetExpr represents a GROUPING SETS, ROLLUP, or CUBE expression
// in the GROUP BY clause. These allow computing multiple levels of aggregation
// in a single query pass.
//
// Example GROUPING SETS:
//
//	SELECT region, product, SUM(sales)
//	FROM sales_data
//	GROUP BY GROUPING SETS ((region, product), (region), ())
//
// Example ROLLUP:
//
//	SELECT year, quarter, month, SUM(sales)
//	FROM sales_data
//	GROUP BY ROLLUP (year, quarter, month)
//	-- Generates: (year, quarter, month), (year, quarter), (year), ()
//
// Example CUBE:
//
//	SELECT region, product, SUM(sales)
//	FROM sales_data
//	GROUP BY CUBE (region, product)
//	-- Generates: (region, product), (region), (product), ()
type GroupingSetExpr struct {
	// Type indicates whether this is a simple GROUPING SETS, ROLLUP, or CUBE.
	Type GroupingSetType
	// Exprs contains the grouping sets. Each inner slice represents one grouping set.
	// For GROUPING SETS, this contains the explicit sets provided.
	// For ROLLUP and CUBE, this is expanded from the original expression list.
	Exprs [][]Expr
}

// exprNode implements the Expr interface for GroupingSetExpr.
func (*GroupingSetExpr) exprNode() {}

// RollupExpr is a convenience wrapper type for ROLLUP expressions.
// ROLLUP creates a hierarchical set of groupings from left to right.
//
// Example:
//
//	ROLLUP (a, b, c)
//	-- Expands to: (a, b, c), (a, b), (a), ()
//
// This provides subtotals for each level of the hierarchy plus a grand total.
type RollupExpr struct {
	// Exprs contains the expressions to roll up.
	// They are processed left-to-right to create hierarchical subtotals.
	Exprs []Expr
}

// exprNode implements the Expr interface for RollupExpr.
func (*RollupExpr) exprNode() {}

// CubeExpr is a convenience wrapper type for CUBE expressions.
// CUBE creates all possible combinations of the given expressions.
//
// Example:
//
//	CUBE (a, b)
//	-- Expands to: (a, b), (a), (b), ()
//
// For n expressions, CUBE generates 2^n grouping sets.
type CubeExpr struct {
	// Exprs contains the expressions to cube.
	// All possible combinations of these expressions form the grouping sets.
	Exprs []Expr
}

// exprNode implements the Expr interface for CubeExpr.
func (*CubeExpr) exprNode() {}

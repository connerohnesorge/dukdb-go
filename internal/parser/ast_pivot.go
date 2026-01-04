// Package parser provides SQL parsing for the native Go DuckDB implementation.
package parser

import (
	dukdb "github.com/dukdb/dukdb-go"
)

// ---------- PIVOT/UNPIVOT Statement Types ----------

// PivotStmt represents a PIVOT expression that transforms rows into columns.
// PIVOT rotates a table-valued expression by turning the unique values from
// one column in the expression into multiple columns in the output.
//
// Example:
//
//	FROM sales
//	PIVOT (
//	    SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
//	) AS pivoted_sales
type PivotStmt struct {
	// Source is the table reference being pivoted (table, subquery, or table function).
	Source TableRef
	// ForColumn is the column whose unique values become column names.
	// In "FOR quarter IN ('Q1', 'Q2', ...)", this is "quarter".
	ForColumn string
	// PivotOn contains the expressions whose unique values become column names.
	// In "FOR quarter IN ('Q1', 'Q2', ...)", these are the IN values.
	PivotOn []Expr
	// Using contains the aggregate functions to apply to the pivoted data.
	// In "SUM(amount)", this represents the aggregation specification.
	Using []PivotAggregate
	// GroupBy contains expressions to group by (implicit or explicit).
	// Columns not in PIVOT ON or aggregated become GROUP BY columns.
	GroupBy []Expr
	// Alias is the optional alias for the pivoted result set.
	Alias string
	// ColumnPrefix is an optional prefix for generated column names.
	ColumnPrefix string
}

func (*PivotStmt) stmtNode() {}

// Type returns the statement type for PivotStmt.
// Returns STATEMENT_TYPE_PIVOT as this is a PIVOT operation.
func (*PivotStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_PIVOT }

// Accept implements the Visitor pattern for PivotStmt.
func (s *PivotStmt) Accept(v Visitor) {
	v.VisitPivotStmt(s)
}

// PivotAggregate represents an aggregate function in a PIVOT clause.
// It specifies the function to apply and an optional alias for the result column.
//
// Example:
//
//	SUM(amount) AS total
type PivotAggregate struct {
	// Function is the aggregate function name (e.g., "SUM", "COUNT", "AVG", "MAX", "MIN").
	Function string
	// Expr is the expression to aggregate.
	Expr Expr
	// Alias is the optional alias for the aggregated column.
	// If empty, a default name is generated from the function and expression.
	Alias string
}

// UnpivotStmt represents an UNPIVOT expression that transforms columns into rows.
// UNPIVOT rotates columns in a table-valued expression into row values.
// This is the inverse operation of PIVOT.
//
// Example:
//
//	FROM quarterly_sales
//	UNPIVOT (
//	    amount FOR quarter IN (Q1, Q2, Q3, Q4)
//	) AS unpivoted_sales
type UnpivotStmt struct {
	// Source is the table reference being unpivoted (table, subquery, or table function).
	Source TableRef
	// Into is the column name for the unpivoted values.
	// In "amount FOR quarter IN ...", this is "amount".
	Into string
	// For is the column name that will contain the original column names.
	// In "amount FOR quarter IN ...", this is "quarter".
	For string
	// Using contains the column names to unpivot.
	// In "... IN (Q1, Q2, Q3, Q4)", these are ["Q1", "Q2", "Q3", "Q4"].
	Using []string
	// Alias is the optional alias for the unpivoted result set.
	Alias string
}

func (*UnpivotStmt) stmtNode() {}

// Type returns the statement type for UnpivotStmt.
// Returns STATEMENT_TYPE_UNPIVOT as this is an UNPIVOT operation.
func (*UnpivotStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_UNPIVOT }

// Accept implements the Visitor pattern for UnpivotStmt.
func (s *UnpivotStmt) Accept(v Visitor) {
	v.VisitUnpivotStmt(s)
}

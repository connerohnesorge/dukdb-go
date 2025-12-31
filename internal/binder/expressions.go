package binder

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// ---------- Bound Expression Types ----------

// BoundColumnRef represents a bound column reference.
type BoundColumnRef struct {
	Table     string
	Column    string
	ColumnIdx int
	ColType   dukdb.Type
}

func (*BoundColumnRef) boundExprNode() {}

func (c *BoundColumnRef) ResultType() dukdb.Type { return c.ColType }

// BoundLiteral represents a bound literal value.
type BoundLiteral struct {
	Value   any
	ValType dukdb.Type
}

func (*BoundLiteral) boundExprNode() {}

func (l *BoundLiteral) ResultType() dukdb.Type { return l.ValType }

// BoundParameter represents a bound parameter placeholder.
type BoundParameter struct {
	Position  int
	ParamType dukdb.Type
}

func (*BoundParameter) boundExprNode() {}

func (p *BoundParameter) ResultType() dukdb.Type { return p.ParamType }

// BoundBinaryExpr represents a bound binary expression.
type BoundBinaryExpr struct {
	Left    BoundExpr
	Op      parser.BinaryOp
	Right   BoundExpr
	ResType dukdb.Type
}

func (*BoundBinaryExpr) boundExprNode() {}

func (e *BoundBinaryExpr) ResultType() dukdb.Type { return e.ResType }

// BoundUnaryExpr represents a bound unary expression.
type BoundUnaryExpr struct {
	Op      parser.UnaryOp
	Expr    BoundExpr
	ResType dukdb.Type
}

func (*BoundUnaryExpr) boundExprNode() {}

func (e *BoundUnaryExpr) ResultType() dukdb.Type { return e.ResType }

// BoundFunctionCall represents a bound function call.
type BoundFunctionCall struct {
	Name     string
	Args     []BoundExpr
	Distinct bool
	Star     bool
	ResType  dukdb.Type
}

func (*BoundFunctionCall) boundExprNode() {}

func (f *BoundFunctionCall) ResultType() dukdb.Type { return f.ResType }

// BoundScalarUDF represents a bound scalar user-defined function call.
type BoundScalarUDF struct {
	Name    string
	Args    []BoundExpr
	ResType dukdb.Type
	// UDFInfo contains the registered UDF metadata for execution.
	// This is an opaque pointer to dukdb.registeredScalarFunc.
	UDFInfo any
	// ArgInfo contains metadata about each argument for constant folding.
	ArgInfo []dukdb.ScalarUDFArg
	// BindCtx contains the context returned from ScalarBinder callback.
	BindCtx any
}

func (*BoundScalarUDF) boundExprNode() {}

func (f *BoundScalarUDF) ResultType() dukdb.Type { return f.ResType }

// BoundCastExpr represents a bound CAST expression.
type BoundCastExpr struct {
	Expr       BoundExpr
	TargetType dukdb.Type
}

func (*BoundCastExpr) boundExprNode() {}

func (c *BoundCastExpr) ResultType() dukdb.Type { return c.TargetType }

// BoundCaseExpr represents a bound CASE expression.
type BoundCaseExpr struct {
	Operand BoundExpr
	Whens   []*BoundWhenClause
	Else    BoundExpr
	ResType dukdb.Type
}

func (*BoundCaseExpr) boundExprNode() {}

func (c *BoundCaseExpr) ResultType() dukdb.Type { return c.ResType }

// BoundWhenClause represents a bound WHEN clause.
type BoundWhenClause struct {
	Condition BoundExpr
	Result    BoundExpr
}

// BoundBetweenExpr represents a bound BETWEEN expression.
type BoundBetweenExpr struct {
	Expr BoundExpr
	Low  BoundExpr
	High BoundExpr
	Not  bool
}

func (*BoundBetweenExpr) boundExprNode() {}

func (*BoundBetweenExpr) ResultType() dukdb.Type { return dukdb.TYPE_BOOLEAN }

// BoundInListExpr represents a bound IN expression with a list.
type BoundInListExpr struct {
	Expr   BoundExpr
	Values []BoundExpr
	Not    bool
}

func (*BoundInListExpr) boundExprNode() {}

func (*BoundInListExpr) ResultType() dukdb.Type { return dukdb.TYPE_BOOLEAN }

// BoundInSubqueryExpr represents a bound IN expression with a subquery.
type BoundInSubqueryExpr struct {
	Expr     BoundExpr
	Subquery *BoundSelectStmt
	Not      bool
}

func (*BoundInSubqueryExpr) boundExprNode() {}

func (*BoundInSubqueryExpr) ResultType() dukdb.Type { return dukdb.TYPE_BOOLEAN }

// BoundExistsExpr represents a bound EXISTS expression.
type BoundExistsExpr struct {
	Subquery *BoundSelectStmt
	Not      bool
}

func (*BoundExistsExpr) boundExprNode() {}

func (*BoundExistsExpr) ResultType() dukdb.Type { return dukdb.TYPE_BOOLEAN }

// BoundStarExpr represents a bound * expression.
type BoundStarExpr struct {
	Table   string
	Columns []*BoundColumn
}

func (*BoundStarExpr) boundExprNode() {}

func (*BoundStarExpr) ResultType() dukdb.Type { return dukdb.TYPE_ANY }

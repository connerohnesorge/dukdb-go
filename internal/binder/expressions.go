package binder

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
)

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
	OrderBy  []BoundOrderByExpr // ORDER BY within aggregate functions
	ResType  dukdb.Type
}

// BoundOrderByExpr represents a bound ORDER BY expression within an aggregate function.
type BoundOrderByExpr struct {
	Expr BoundExpr
	Desc bool
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

// BoundExtractExpr represents a bound EXTRACT(part FROM source) expression.
// This extracts a date/time field from a temporal value.
// Returns DOUBLE per SQL standard (same as DATE_PART).
type BoundExtractExpr struct {
	Part   string    // The part to extract (YEAR, MONTH, DAY, etc.)
	Source BoundExpr // The source expression
}

func (*BoundExtractExpr) boundExprNode() {}

func (*BoundExtractExpr) ResultType() dukdb.Type { return dukdb.TYPE_DOUBLE }

// BoundIntervalLiteral represents a bound INTERVAL literal expression.
// Contains the parsed interval components: months, days, and microseconds.
type BoundIntervalLiteral struct {
	Months int32 // Number of months (includes years * 12)
	Days   int32 // Number of days
	Micros int64 // Number of microseconds (sub-day time)
}

func (*BoundIntervalLiteral) boundExprNode() {}

func (*BoundIntervalLiteral) ResultType() dukdb.Type { return dukdb.TYPE_INTERVAL }

// ---------- Window Function Types ----------

// WindowFunctionType categorizes window functions by their behavior.
type WindowFunctionType int

const (
	// WindowFunctionRanking - ROW_NUMBER, RANK, DENSE_RANK, NTILE
	WindowFunctionRanking WindowFunctionType = iota
	// WindowFunctionValue - LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
	WindowFunctionValue
	// WindowFunctionDistribution - PERCENT_RANK, CUME_DIST
	WindowFunctionDistribution
	// WindowFunctionAggregate - COUNT, SUM, AVG, MIN, MAX with OVER
	WindowFunctionAggregate
)

// BoundWindowOrder represents bound ORDER BY with null ordering.
type BoundWindowOrder struct {
	Expr       BoundExpr
	Desc       bool
	NullsFirst bool
}

// BoundWindowExpr represents a bound window expression.
type BoundWindowExpr struct {
	FunctionName string             // e.g., "row_number", "sum"
	FunctionType WindowFunctionType // Ranking, Value, Distribution, Aggregate
	Args         []BoundExpr        // Bound function arguments
	PartitionBy  []BoundExpr        // Bound partition expressions
	OrderBy      []BoundWindowOrder // Bound order expressions with NULLS FIRST/LAST
	Frame        *parser.WindowFrame // Resolved frame (with defaults applied)
	ResType      dukdb.Type          // Result type of the window function
	IgnoreNulls  bool               // IGNORE NULLS modifier
	Filter       BoundExpr          // Bound FILTER expression (or nil)
	Distinct     bool               // DISTINCT modifier for aggregates
	ResultIndex  int                // Column index in output
	Alias        string             // Column alias from SELECT (e.g., "rn" in "ROW_NUMBER() OVER (...) as rn")
}

func (*BoundWindowExpr) boundExprNode() {}

// ResultType returns the result type of the window expression.
func (w *BoundWindowExpr) ResultType() dukdb.Type { return w.ResType }

// BoundSequenceCall represents a bound sequence function call (NEXTVAL or CURRVAL).
type BoundSequenceCall struct {
	FunctionName string // "NEXTVAL" or "CURRVAL"
	SchemaName   string // Schema containing the sequence
	SequenceName string // Name of the sequence
}

func (*BoundSequenceCall) boundExprNode() {}

func (*BoundSequenceCall) ResultType() dukdb.Type { return dukdb.TYPE_BIGINT }

// ---------- Grouping Set Types ----------

// BoundGroupingSetType represents the type of grouping set operation.
type BoundGroupingSetType int

const (
	// BoundGroupingSetSimple represents explicit GROUPING SETS ((...), (...), ...).
	BoundGroupingSetSimple BoundGroupingSetType = iota
	// BoundGroupingSetRollup represents ROLLUP(...) which has been expanded.
	BoundGroupingSetRollup
	// BoundGroupingSetCube represents CUBE(...) which has been expanded.
	BoundGroupingSetCube
)

// BoundGroupingSetExpr represents a bound GROUPING SETS, ROLLUP, or CUBE expression.
// After binding, ROLLUP and CUBE are expanded into their full grouping sets.
//
// ROLLUP(a, b, c) expands to: (a, b, c), (a, b), (a), ()
// CUBE(a, b) expands to: (a, b), (a), (b), ()
type BoundGroupingSetExpr struct {
	// Type indicates whether this was originally GROUPING SETS, ROLLUP, or CUBE.
	Type BoundGroupingSetType
	// Sets contains the expanded grouping sets. Each inner slice is a set of
	// bound expressions representing one grouping level.
	Sets [][]BoundExpr
}

func (*BoundGroupingSetExpr) boundExprNode() {}

// ResultType returns TYPE_ANY since grouping sets are not evaluated as expressions
// but rather control aggregation behavior.
func (*BoundGroupingSetExpr) ResultType() dukdb.Type { return dukdb.TYPE_ANY }

// BoundGroupingCall represents a bound GROUPING(col1, col2, ...) function call.
// GROUPING() returns a bitmask indicating which of its arguments are aggregated
// (null) in the current grouping set.
//
// For each argument:
//   - If the column is aggregated (null in this grouping set), the bit is 1
//   - If the column is grouped (not null), the bit is 0
//
// Bits are ordered with the first argument in the most significant position.
//
// Example: GROUPING(a, b) with grouping set (a) returns 1 (binary: 01)
// because 'a' is grouped (0) and 'b' is aggregated (1).
type BoundGroupingCall struct {
	// Args contains the column references to check.
	Args []*BoundColumnRef
}

func (*BoundGroupingCall) boundExprNode() {}

// ResultType returns BIGINT as the bitmask result.
func (*BoundGroupingCall) ResultType() dukdb.Type { return dukdb.TYPE_BIGINT }

// Package binder provides name and type resolution for parsed SQL statements.
package binder

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// BoundStatement represents a statement that has been bound to the catalog.
type BoundStatement interface {
	boundStmtNode()
	Type() dukdb.StmtType
}

// BoundExpr represents an expression that has been bound to the catalog.
type BoundExpr interface {
	boundExprNode()
	ResultType() dukdb.Type
}

// ScalarUDFResolver is the interface for resolving scalar UDFs.
// This is used to decouple the binder from the dukdb package.
type ScalarUDFResolver interface {
	// LookupScalarUDF looks up a scalar UDF by name and argument types.
	// Returns the UDF info (opaque), result type, and whether it was found.
	LookupScalarUDF(
		name string,
		argTypes []dukdb.Type,
	) (udfInfo any, resultType dukdb.Type, found bool)
	// BindScalarUDF calls the ScalarBinder callback if present.
	// Returns the bind context to be used during execution.
	// For volatile functions, this returns nil to prevent caching.
	BindScalarUDF(
		udfInfo any,
		args []dukdb.ScalarUDFArg,
	) (bindCtx any, err error)
	// IsVolatile returns true if the UDF is marked as volatile.
	// Volatile functions produce different results each invocation and cannot be cached.
	IsVolatile(udfInfo any) bool
}

// Binder resolves names and checks types in parsed statements.
type Binder struct {
	catalog     *catalog.Catalog
	scope       *BindScope
	udfResolver ScalarUDFResolver
}

// BindScope represents the current binding scope with available tables and columns.
type BindScope struct {
	parent     *BindScope
	tables     map[string]*BoundTableRef
	aliases    map[string]string // alias -> table name
	paramCount int
	params     map[int]dukdb.Type // position -> inferred type
}

// BoundTableRef represents a bound table reference.
type BoundTableRef struct {
	Schema       string
	TableName    string
	Alias        string
	TableDef     *catalog.TableDef
	VirtualTable *catalog.VirtualTableDef // Set for virtual tables
	Columns      []*BoundColumn
}

// BoundColumn represents a bound column reference.
type BoundColumn struct {
	Table      string // Table alias or name
	Column     string
	ColumnIdx  int
	Type       dukdb.Type
	SourceType string // "table", "subquery", "function"
}

// NewBinder creates a new Binder.
func NewBinder(cat *catalog.Catalog) *Binder {
	return &Binder{
		catalog: cat,
		scope:   newBindScope(nil),
	}
}

// WithUDFResolver sets the scalar UDF resolver and returns the binder.
func (b *Binder) WithUDFResolver(
	resolver ScalarUDFResolver,
) *Binder {
	b.udfResolver = resolver

	return b
}

func newBindScope(parent *BindScope) *BindScope {
	return &BindScope{
		parent:  parent,
		tables:  make(map[string]*BoundTableRef),
		aliases: make(map[string]string),
		params:  make(map[int]dukdb.Type),
	}
}

// Bind binds a parsed statement to the catalog.
func (b *Binder) Bind(
	stmt parser.Statement,
) (BoundStatement, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return b.bindSelect(s)
	case *parser.InsertStmt:
		return b.bindInsert(s)
	case *parser.UpdateStmt:
		return b.bindUpdate(s)
	case *parser.DeleteStmt:
		return b.bindDelete(s)
	case *parser.CreateTableStmt:
		return b.bindCreateTable(s)
	case *parser.DropTableStmt:
		return b.bindDropTable(s)
	case *parser.BeginStmt:
		return &BoundBeginStmt{}, nil
	case *parser.CommitStmt:
		return &BoundCommitStmt{}, nil
	case *parser.RollbackStmt:
		return &BoundRollbackStmt{}, nil
	default:
		return nil, b.errorf("unsupported statement type: %T", stmt)
	}
}

func (*Binder) errorf(
	format string,
	args ...any,
) error {
	return &dukdb.Error{
		Type: dukdb.ErrorTypeBinder,
		Msg: fmt.Sprintf(
			"Binder Error: "+format,
			args...),
	}
}

// ---------- Bound Statement Types ----------

// BoundSelectStmt represents a bound SELECT statement.
type BoundSelectStmt struct {
	Distinct bool
	Columns  []*BoundSelectColumn
	From     []*BoundTableRef
	Joins    []*BoundJoin
	Where    BoundExpr
	GroupBy  []BoundExpr
	Having   BoundExpr
	OrderBy  []*BoundOrderBy
	Limit    BoundExpr
	Offset   BoundExpr
}

func (*BoundSelectStmt) boundStmtNode() {}

func (*BoundSelectStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_SELECT }

func (*BoundSelectStmt) boundExprNode() {}

func (*BoundSelectStmt) ResultType() dukdb.Type { return dukdb.TYPE_ANY }

// BoundSelectColumn represents a bound column in SELECT.
type BoundSelectColumn struct {
	Expr  BoundExpr
	Alias string
	Star  bool
}

// BoundJoin represents a bound JOIN.
type BoundJoin struct {
	Type      parser.JoinType
	Table     *BoundTableRef
	Condition BoundExpr
}

// BoundOrderBy represents a bound ORDER BY expression.
type BoundOrderBy struct {
	Expr BoundExpr
	Desc bool
}

// BoundInsertStmt represents a bound INSERT statement.
type BoundInsertStmt struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Columns  []int // Column indices
	Values   [][]BoundExpr
	Select   *BoundSelectStmt
}

func (*BoundInsertStmt) boundStmtNode() {}

func (*BoundInsertStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_INSERT }

// BoundUpdateStmt represents a bound UPDATE statement.
type BoundUpdateStmt struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Set      []*BoundSetClause
	Where    BoundExpr
}

func (*BoundUpdateStmt) boundStmtNode() {}

func (*BoundUpdateStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_UPDATE }

// BoundSetClause represents a bound SET clause.
type BoundSetClause struct {
	ColumnIdx int
	Value     BoundExpr
}

// BoundDeleteStmt represents a bound DELETE statement.
type BoundDeleteStmt struct {
	Schema   string
	Table    string
	TableDef *catalog.TableDef
	Where    BoundExpr
}

func (*BoundDeleteStmt) boundStmtNode() {}

func (*BoundDeleteStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DELETE }

// BoundCreateTableStmt represents a bound CREATE TABLE statement.
type BoundCreateTableStmt struct {
	Schema      string
	Table       string
	IfNotExists bool
	Columns     []*catalog.ColumnDef
	PrimaryKey  []string
}

func (*BoundCreateTableStmt) boundStmtNode() {}

func (*BoundCreateTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_CREATE }

// BoundDropTableStmt represents a bound DROP TABLE statement.
type BoundDropTableStmt struct {
	Schema   string
	Table    string
	IfExists bool
}

func (*BoundDropTableStmt) boundStmtNode() {}

func (*BoundDropTableStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_DROP }

// BoundBeginStmt represents a bound BEGIN statement.
type BoundBeginStmt struct{}

func (*BoundBeginStmt) boundStmtNode() {}

func (*BoundBeginStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// BoundCommitStmt represents a bound COMMIT statement.
type BoundCommitStmt struct{}

func (*BoundCommitStmt) boundStmtNode() {}

func (*BoundCommitStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

// BoundRollbackStmt represents a bound ROLLBACK statement.
type BoundRollbackStmt struct{}

func (*BoundRollbackStmt) boundStmtNode() {}

func (*BoundRollbackStmt) Type() dukdb.StmtType { return dukdb.STATEMENT_TYPE_TRANSACTION }

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

func (b *Binder) bindSelect(
	s *parser.SelectStmt,
) (*BoundSelectStmt, error) {
	bound := &BoundSelectStmt{
		Distinct: s.Distinct,
	}

	// Push new scope
	oldScope := b.scope
	b.scope = newBindScope(oldScope)
	defer func() {
		// Merge parameters from inner scope to outer scope before restoring
		for pos, typ := range b.scope.params {
			oldScope.params[pos] = typ
		}
		// Also update paramCount
		oldScope.paramCount = b.scope.paramCount
		b.scope = oldScope
	}()

	// Bind FROM clause first to establish table bindings
	if s.From != nil {
		for _, table := range s.From.Tables {
			ref, err := b.bindTableRef(table)
			if err != nil {
				return nil, err
			}
			bound.From = append(bound.From, ref)
		}

		for _, join := range s.From.Joins {
			j, err := b.bindJoin(join)
			if err != nil {
				return nil, err
			}
			bound.Joins = append(bound.Joins, j)
		}
	}

	// Bind columns
	for _, col := range s.Columns {
		if col.Star {
			// Expand star to all columns
			if starExpr, ok := col.Expr.(*parser.StarExpr); ok {
				boundStar, err := b.bindStarExpr(
					starExpr,
				)
				if err != nil {
					return nil, err
				}
				for _, c := range boundStar.Columns {
					bound.Columns = append(
						bound.Columns,
						&BoundSelectColumn{
							Expr: &BoundColumnRef{
								Table:     c.Table,
								Column:    c.Column,
								ColumnIdx: c.ColumnIdx,
								ColType:   c.Type,
							},
							Alias: c.Column,
						},
					)
				}
			}
		} else {
			expr, err := b.bindExpr(col.Expr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			alias := col.Alias
			if alias == "" {
				// If no explicit alias and the expression is a column reference,
				// use the column name as the alias
				if colRef, ok := expr.(*BoundColumnRef); ok {
					alias = colRef.Column
				}
			}
			bound.Columns = append(bound.Columns, &BoundSelectColumn{
				Expr:  expr,
				Alias: alias,
			})
		}
	}

	// Bind WHERE
	if s.Where != nil {
		where, err := b.bindExpr(
			s.Where,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
		bound.Where = where
	}

	// Bind GROUP BY
	for _, g := range s.GroupBy {
		expr, err := b.bindExpr(g, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		bound.GroupBy = append(
			bound.GroupBy,
			expr,
		)
	}

	// Bind HAVING
	if s.Having != nil {
		having, err := b.bindExpr(
			s.Having,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
		bound.Having = having
	}

	// Bind ORDER BY
	for _, o := range s.OrderBy {
		expr, err := b.bindExpr(
			o.Expr,
			dukdb.TYPE_ANY,
		)
		if err != nil {
			return nil, err
		}
		bound.OrderBy = append(
			bound.OrderBy,
			&BoundOrderBy{
				Expr: expr,
				Desc: o.Desc,
			},
		)
	}

	// Bind LIMIT
	if s.Limit != nil {
		limit, err := b.bindExpr(
			s.Limit,
			dukdb.TYPE_BIGINT,
		)
		if err != nil {
			return nil, err
		}
		bound.Limit = limit
	}

	// Bind OFFSET
	if s.Offset != nil {
		offset, err := b.bindExpr(
			s.Offset,
			dukdb.TYPE_BIGINT,
		)
		if err != nil {
			return nil, err
		}
		bound.Offset = offset
	}

	return bound, nil
}

func (b *Binder) bindTableRef(
	ref parser.TableRef,
) (*BoundTableRef, error) {
	if ref.Subquery != nil {
		// Bind subquery
		subquery, err := b.bindSelect(
			ref.Subquery,
		)
		if err != nil {
			return nil, err
		}

		alias := ref.Alias
		if alias == "" {
			alias = "subquery"
		}

		boundRef := &BoundTableRef{
			Alias: alias,
		}

		// Create columns from subquery
		for i, col := range subquery.Columns {
			colName := col.Alias
			if colName == "" {
				colName = fmt.Sprintf("col%d", i)
			}
			boundRef.Columns = append(
				boundRef.Columns,
				&BoundColumn{
					Table:      alias,
					Column:     colName,
					ColumnIdx:  i,
					Type:       col.Expr.ResultType(),
					SourceType: "subquery",
				},
			)
		}

		b.scope.tables[alias] = boundRef
		b.scope.aliases[alias] = alias

		return boundRef, nil
	}

	// Table reference
	schema := ref.Schema
	if schema == "" {
		schema = "main"
	}

	alias := ref.Alias
	if alias == "" {
		alias = ref.TableName
	}

	// First check for virtual tables (they take precedence in the main schema)
	if schema == "main" {
		if vtDef, ok := b.catalog.GetVirtualTableDef(ref.TableName); ok {
			boundRef := &BoundTableRef{
				Schema:       schema,
				TableName:    ref.TableName,
				Alias:        alias,
				VirtualTable: vtDef,
				TableDef:     vtDef.ToTableDef(),
			}

			// Create bound columns from virtual table
			for i, col := range vtDef.Columns() {
				boundRef.Columns = append(
					boundRef.Columns,
					&BoundColumn{
						Table:      alias,
						Column:     col.Name,
						ColumnIdx:  i,
						Type:       col.Type,
						SourceType: "virtual",
					},
				)
			}

			b.scope.tables[alias] = boundRef
			b.scope.aliases[alias] = ref.TableName

			return boundRef, nil
		}
	}

	// Fall back to regular table lookup
	tableDef, ok := b.catalog.GetTableInSchema(
		schema,
		ref.TableName,
	)
	if !ok {
		return nil, b.errorf(
			"table not found: %s",
			ref.TableName,
		)
	}

	boundRef := &BoundTableRef{
		Schema:    schema,
		TableName: ref.TableName,
		Alias:     alias,
		TableDef:  tableDef,
	}

	// Create bound columns
	for i, col := range tableDef.Columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = ref.TableName

	return boundRef, nil
}

func (b *Binder) bindJoin(
	join parser.JoinClause,
) (*BoundJoin, error) {
	table, err := b.bindTableRef(join.Table)
	if err != nil {
		return nil, err
	}

	var cond BoundExpr
	if join.Condition != nil {
		cond, err = b.bindExpr(
			join.Condition,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
	}

	return &BoundJoin{
		Type:      join.Type,
		Table:     table,
		Condition: cond,
	}, nil
}

func (b *Binder) bindExpr(
	expr parser.Expr,
	expectedType dukdb.Type,
) (BoundExpr, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *parser.ColumnRef:
		return b.bindColumnRef(e)
	case *parser.Literal:
		return &BoundLiteral{Value: e.Value, ValType: e.Type}, nil
	case *parser.Parameter:
		b.scope.paramCount++
		pos := e.Position
		if pos == 0 {
			pos = b.scope.paramCount
		}
		// Use expected type if available, otherwise TYPE_ANY
		inferredType := expectedType
		if inferredType == dukdb.TYPE_INVALID || inferredType == 0 {
			inferredType = dukdb.TYPE_ANY
		}
		// Store in scope for later retrieval
		b.scope.params[pos] = inferredType

		return &BoundParameter{Position: pos, ParamType: inferredType}, nil
	case *parser.BinaryExpr:
		return b.bindBinaryExpr(e)
	case *parser.UnaryExpr:
		return b.bindUnaryExpr(e, expectedType)
	case *parser.FunctionCall:
		return b.bindFunctionCall(e)
	case *parser.CastExpr:
		// For CAST, the inner expression type is not constrained by outer context
		inner, err := b.bindExpr(e.Expr, e.TargetType)
		if err != nil {
			return nil, err
		}

		return &BoundCastExpr{Expr: inner, TargetType: e.TargetType}, nil
	case *parser.CaseExpr:
		return b.bindCaseExpr(e, expectedType)
	case *parser.BetweenExpr:
		return b.bindBetweenExpr(e)
	case *parser.InListExpr:
		return b.bindInListExpr(e)
	case *parser.InSubqueryExpr:
		return b.bindInSubqueryExpr(e)
	case *parser.ExistsExpr:
		return b.bindExistsExpr(e)
	case *parser.StarExpr:
		return b.bindStarExpr(e)
	case *parser.SelectStmt:
		return b.bindSelect(e)
	default:
		return nil, b.errorf("unsupported expression type: %T", expr)
	}
}

func (b *Binder) bindColumnRef(
	ref *parser.ColumnRef,
) (*BoundColumnRef, error) {
	if ref.Table != "" {
		// Qualified column reference
		tableRef, ok := b.scope.tables[ref.Table]
		if !ok {
			return nil, b.errorf(
				"table not found: %s",
				ref.Table,
			)
		}

		for _, col := range tableRef.Columns {
			if strings.EqualFold(
				col.Column,
				ref.Column,
			) {
				return &BoundColumnRef{
					Table:     ref.Table,
					Column:    col.Column,
					ColumnIdx: col.ColumnIdx,
					ColType:   col.Type,
				}, nil
			}
		}

		return nil, b.errorf(
			"column not found: %s.%s",
			ref.Table,
			ref.Column,
		)
	}

	// Unqualified column reference - search all tables
	var found *BoundColumn
	var foundTable string
	for tableName, tableRef := range b.scope.tables {
		for _, col := range tableRef.Columns {
			if strings.EqualFold(
				col.Column,
				ref.Column,
			) {
				if found != nil {
					return nil, b.errorf(
						"ambiguous column reference: %s",
						ref.Column,
					)
				}
				found = col
				foundTable = tableName
			}
		}
	}

	if found == nil {
		return nil, b.errorf(
			"column not found: %s",
			ref.Column,
		)
	}

	return &BoundColumnRef{
		Table:     foundTable,
		Column:    found.Column,
		ColumnIdx: found.ColumnIdx,
		ColType:   found.Type,
	}, nil
}

func (b *Binder) bindBinaryExpr(
	e *parser.BinaryExpr,
) (*BoundBinaryExpr, error) {
	// Bind left first without expectation
	left, err := b.bindExpr(
		e.Left,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	// For comparison and LIKE operators, use left's type as expected for right
	var rightExpected dukdb.Type
	switch e.Op {
	case parser.OpEq,
		parser.OpNe,
		parser.OpLt,
		parser.OpLe,
		parser.OpGt,
		parser.OpGe:
		rightExpected = left.ResultType()
	case parser.OpLike,
		parser.OpILike,
		parser.OpNotLike,
		parser.OpNotILike:
		rightExpected = dukdb.TYPE_VARCHAR
	case parser.OpAdd,
		parser.OpSub,
		parser.OpMul,
		parser.OpDiv,
		parser.OpMod:
		rightExpected = dukdb.TYPE_DOUBLE // arithmetic context
	default:
		rightExpected = dukdb.TYPE_ANY
	}

	right, err := b.bindExpr(
		e.Right,
		rightExpected,
	)
	if err != nil {
		return nil, err
	}

	// If left was parameter with TYPE_ANY and right has concrete type, update left
	if leftParam, ok := left.(*BoundParameter); ok {
		if leftParam.ParamType == dukdb.TYPE_ANY &&
			right.ResultType() != dukdb.TYPE_ANY {
			leftParam.ParamType = right.ResultType()
			b.scope.params[leftParam.Position] = right.ResultType()
		}
	}

	// Determine result type
	var resType dukdb.Type
	switch e.Op {
	case parser.OpEq,
		parser.OpNe,
		parser.OpLt,
		parser.OpLe,
		parser.OpGt,
		parser.OpGe,
		parser.OpAnd,
		parser.OpOr,
		parser.OpLike,
		parser.OpILike,
		parser.OpNotLike,
		parser.OpNotILike,
		parser.OpIn,
		parser.OpNotIn,
		parser.OpIs,
		parser.OpIsNot:
		resType = dukdb.TYPE_BOOLEAN
	case parser.OpConcat:
		resType = dukdb.TYPE_VARCHAR
	default:
		// For arithmetic, use the more precise type
		resType = promoteType(
			left.ResultType(),
			right.ResultType(),
		)
	}

	return &BoundBinaryExpr{
		Left:    left,
		Op:      e.Op,
		Right:   right,
		ResType: resType,
	}, nil
}

func (b *Binder) bindUnaryExpr(
	e *parser.UnaryExpr,
	expectedType dukdb.Type,
) (*BoundUnaryExpr, error) {
	inner, err := b.bindExpr(e.Expr, expectedType)
	if err != nil {
		return nil, err
	}

	var resType dukdb.Type
	switch e.Op {
	case parser.OpNot,
		parser.OpIsNull,
		parser.OpIsNotNull:
		resType = dukdb.TYPE_BOOLEAN
	default:
		resType = inner.ResultType()
	}

	return &BoundUnaryExpr{
		Op:      e.Op,
		Expr:    inner,
		ResType: resType,
	}, nil
}

func (b *Binder) bindFunctionCall(
	f *parser.FunctionCall,
) (BoundExpr, error) {
	// Get expected argument types from function signature if available
	argTypes := getFunctionArgTypes(
		f.Name,
		len(f.Args),
	)

	var args []BoundExpr
	for i, arg := range f.Args {
		expectedType := dukdb.TYPE_ANY
		if i < len(argTypes) {
			expectedType = argTypes[i]
		}
		bound, err := b.bindExpr(
			arg,
			expectedType,
		)
		if err != nil {
			return nil, err
		}
		args = append(args, bound)
	}

	// Check for scalar UDF first
	if b.udfResolver != nil {
		argTypes := make([]dukdb.Type, len(args))
		for i, arg := range args {
			argTypes[i] = arg.ResultType()
		}

		if udfInfo, resType, found := b.udfResolver.LookupScalarUDF(f.Name, argTypes); found {
			// Build argument info for constant folding.
			// For volatile functions, skip foldability detection to prevent caching.
			argInfo := make(
				[]dukdb.ScalarUDFArg,
				len(args),
			)
			isVolatile := b.udfResolver.IsVolatile(
				udfInfo,
			)
			if !isVolatile {
				for i, arg := range args {
					if lit, ok := arg.(*BoundLiteral); ok {
						argInfo[i] = dukdb.ScalarUDFArg{
							Foldable: true,
							Value:    lit.Value,
						}
					}
				}
			}

			// Call ScalarBinder callback if present (skipped for volatile functions)
			bindCtx, err := b.udfResolver.BindScalarUDF(
				udfInfo,
				argInfo,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"scalar UDF '%s' bind error: %w",
					f.Name,
					err,
				)
			}

			return &BoundScalarUDF{
				Name:    f.Name,
				Args:    args,
				ResType: resType,
				UDFInfo: udfInfo,
				ArgInfo: argInfo,
				BindCtx: bindCtx,
			}, nil
		}
	}

	// Fall back to built-in function
	resType := inferFunctionResultType(
		f.Name,
		args,
	)

	return &BoundFunctionCall{
		Name:     f.Name,
		Args:     args,
		Distinct: f.Distinct,
		Star:     f.Star,
		ResType:  resType,
	}, nil
}

func (b *Binder) bindCaseExpr(
	e *parser.CaseExpr,
	expectedType dukdb.Type,
) (*BoundCaseExpr, error) {
	bound := &BoundCaseExpr{}

	if e.Operand != nil {
		operand, err := b.bindExpr(
			e.Operand,
			dukdb.TYPE_ANY,
		)
		if err != nil {
			return nil, err
		}
		bound.Operand = operand
	}

	for _, w := range e.Whens {
		cond, err := b.bindExpr(
			w.Condition,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
		result, err := b.bindExpr(
			w.Result,
			expectedType,
		)
		if err != nil {
			return nil, err
		}
		bound.Whens = append(
			bound.Whens,
			&BoundWhenClause{
				Condition: cond,
				Result:    result,
			},
		)
	}

	if e.Else != nil {
		elseExpr, err := b.bindExpr(
			e.Else,
			expectedType,
		)
		if err != nil {
			return nil, err
		}
		bound.Else = elseExpr
	}

	// Determine result type from THEN/ELSE expressions
	if len(bound.Whens) > 0 {
		bound.ResType = bound.Whens[0].Result.ResultType()
	} else if bound.Else != nil {
		bound.ResType = bound.Else.ResultType()
	} else {
		bound.ResType = dukdb.TYPE_SQLNULL
	}

	return bound, nil
}

func (b *Binder) bindBetweenExpr(
	e *parser.BetweenExpr,
) (*BoundBetweenExpr, error) {
	// Bind expr first to get its type
	expr, err := b.bindExpr(
		e.Expr,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	// Use expr's type as expected type for low and high bounds
	exprType := expr.ResultType()

	low, err := b.bindExpr(e.Low, exprType)
	if err != nil {
		return nil, err
	}

	high, err := b.bindExpr(e.High, exprType)
	if err != nil {
		return nil, err
	}

	return &BoundBetweenExpr{
		Expr: expr,
		Low:  low,
		High: high,
		Not:  e.Not,
	}, nil
}

func (b *Binder) bindInListExpr(
	e *parser.InListExpr,
) (*BoundInListExpr, error) {
	// Bind expr first to get its type
	expr, err := b.bindExpr(
		e.Expr,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	// Use expr's type as expected type for IN list values
	exprType := expr.ResultType()

	var values []BoundExpr
	for _, v := range e.Values {
		bound, err := b.bindExpr(v, exprType)
		if err != nil {
			return nil, err
		}
		values = append(values, bound)
	}

	return &BoundInListExpr{
		Expr:   expr,
		Values: values,
		Not:    e.Not,
	}, nil
}

func (b *Binder) bindInSubqueryExpr(
	e *parser.InSubqueryExpr,
) (*BoundInSubqueryExpr, error) {
	expr, err := b.bindExpr(
		e.Expr,
		dukdb.TYPE_ANY,
	)
	if err != nil {
		return nil, err
	}

	subquery, err := b.bindSelect(e.Subquery)
	if err != nil {
		return nil, err
	}

	return &BoundInSubqueryExpr{
		Expr:     expr,
		Subquery: subquery,
		Not:      e.Not,
	}, nil
}

func (b *Binder) bindExistsExpr(
	e *parser.ExistsExpr,
) (*BoundExistsExpr, error) {
	subquery, err := b.bindSelect(e.Subquery)
	if err != nil {
		return nil, err
	}

	return &BoundExistsExpr{
		Subquery: subquery,
		Not:      e.Not,
	}, nil
}

func (b *Binder) bindStarExpr(
	e *parser.StarExpr,
) (*BoundStarExpr, error) {
	bound := &BoundStarExpr{Table: e.Table}

	if e.Table != "" {
		// Specific table's columns
		tableRef, ok := b.scope.tables[e.Table]
		if !ok {
			return nil, b.errorf(
				"table not found: %s",
				e.Table,
			)
		}
		bound.Columns = tableRef.Columns
	} else {
		// All tables' columns
		for _, tableRef := range b.scope.tables {
			bound.Columns = append(bound.Columns, tableRef.Columns...)
		}
	}

	return bound, nil
}

func (b *Binder) bindInsert(
	s *parser.InsertStmt,
) (*BoundInsertStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	tableDef, ok := b.catalog.GetTableInSchema(
		schema,
		s.Table,
	)
	if !ok {
		return nil, b.errorf(
			"table not found: %s",
			s.Table,
		)
	}

	bound := &BoundInsertStmt{
		Schema:   schema,
		Table:    s.Table,
		TableDef: tableDef,
	}

	// Resolve column indices
	if len(s.Columns) == 0 {
		// Insert into all columns
		for i := range tableDef.Columns {
			bound.Columns = append(
				bound.Columns,
				i,
			)
		}
	} else {
		for _, colName := range s.Columns {
			idx, ok := tableDef.GetColumnIndex(colName)
			if !ok {
				return nil, b.errorf("column not found: %s", colName)
			}
			bound.Columns = append(bound.Columns, idx)
		}
	}

	// Bind values with column types for parameter inference
	for _, row := range s.Values {
		// Validate number of values matches number of columns
		if len(row) != len(bound.Columns) {
			return nil, b.errorf(
				"column count mismatch: expected %d values, got %d",
				len(bound.Columns),
				len(row),
			)
		}

		var boundRow []BoundExpr
		for j, val := range row {
			// Get column type for this position
			colIdx := bound.Columns[j]
			colType := tableDef.Columns[colIdx].Type

			expr, err := b.bindExpr(val, colType)
			if err != nil {
				return nil, err
			}
			boundRow = append(boundRow, expr)
		}
		bound.Values = append(
			bound.Values,
			boundRow,
		)
	}

	// Bind SELECT
	if s.Select != nil {
		sel, err := b.bindSelect(s.Select)
		if err != nil {
			return nil, err
		}
		bound.Select = sel
	}

	return bound, nil
}

func (b *Binder) bindUpdate(
	s *parser.UpdateStmt,
) (*BoundUpdateStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	tableDef, ok := b.catalog.GetTableInSchema(
		schema,
		s.Table,
	)
	if !ok {
		return nil, b.errorf(
			"table not found: %s",
			s.Table,
		)
	}

	// Add table to scope for WHERE clause binding
	alias := s.Table
	b.scope.tables[alias] = &BoundTableRef{
		Schema:    schema,
		TableName: s.Table,
		Alias:     alias,
		TableDef:  tableDef,
	}
	for i, col := range tableDef.Columns {
		b.scope.tables[alias].Columns = append(
			b.scope.tables[alias].Columns,
			&BoundColumn{
				Table:     alias,
				Column:    col.Name,
				ColumnIdx: i,
				Type:      col.Type,
			},
		)
	}

	bound := &BoundUpdateStmt{
		Schema:   schema,
		Table:    s.Table,
		TableDef: tableDef,
	}

	// Bind SET clauses with column types for parameter inference
	for _, set := range s.Set {
		idx, ok := tableDef.GetColumnIndex(
			set.Column,
		)
		if !ok {
			return nil, b.errorf(
				"column not found: %s",
				set.Column,
			)
		}

		// Get column type for parameter inference
		colType := tableDef.Columns[idx].Type

		val, err := b.bindExpr(set.Value, colType)
		if err != nil {
			return nil, err
		}
		bound.Set = append(
			bound.Set,
			&BoundSetClause{
				ColumnIdx: idx,
				Value:     val,
			},
		)
	}

	// Bind WHERE
	if s.Where != nil {
		where, err := b.bindExpr(
			s.Where,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
		bound.Where = where
	}

	return bound, nil
}

func (b *Binder) bindDelete(
	s *parser.DeleteStmt,
) (*BoundDeleteStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	tableDef, ok := b.catalog.GetTableInSchema(
		schema,
		s.Table,
	)
	if !ok {
		return nil, b.errorf(
			"table not found: %s",
			s.Table,
		)
	}

	// Add table to scope for WHERE clause binding
	alias := s.Table
	b.scope.tables[alias] = &BoundTableRef{
		Schema:    schema,
		TableName: s.Table,
		Alias:     alias,
		TableDef:  tableDef,
	}
	for i, col := range tableDef.Columns {
		b.scope.tables[alias].Columns = append(
			b.scope.tables[alias].Columns,
			&BoundColumn{
				Table:     alias,
				Column:    col.Name,
				ColumnIdx: i,
				Type:      col.Type,
			},
		)
	}

	bound := &BoundDeleteStmt{
		Schema:   schema,
		Table:    s.Table,
		TableDef: tableDef,
	}

	// Bind WHERE
	if s.Where != nil {
		where, err := b.bindExpr(
			s.Where,
			dukdb.TYPE_BOOLEAN,
		)
		if err != nil {
			return nil, err
		}
		bound.Where = where
	}

	return bound, nil
}

func (b *Binder) bindCreateTable(
	s *parser.CreateTableStmt,
) (*BoundCreateTableStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	bound := &BoundCreateTableStmt{
		Schema:      schema,
		Table:       s.Table,
		IfNotExists: s.IfNotExists,
		PrimaryKey:  s.PrimaryKey,
	}

	// Convert column definitions
	for _, col := range s.Columns {
		colDef := catalog.NewColumnDef(
			col.Name,
			col.DataType,
		)
		colDef.Nullable = !col.NotNull
		if col.Default != nil {
			// For now, just mark that there's a default
			colDef.HasDefault = true
		}
		bound.Columns = append(
			bound.Columns,
			colDef,
		)

		if col.PrimaryKey {
			bound.PrimaryKey = append(
				bound.PrimaryKey,
				col.Name,
			)
		}
	}

	return bound, nil
}

func (b *Binder) bindDropTable(
	s *parser.DropTableStmt,
) (*BoundDropTableStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	return &BoundDropTableStmt{
		Schema:   schema,
		Table:    s.Table,
		IfExists: s.IfExists,
	}, nil
}

// Helper functions

func promoteType(t1, t2 dukdb.Type) dukdb.Type {
	if t1 == t2 {
		return t1
	}

	// NULL promotion
	if t1 == dukdb.TYPE_SQLNULL {
		return t2
	}
	if t2 == dukdb.TYPE_SQLNULL {
		return t1
	}

	// Integer to float promotion
	if isIntegerType(t1) && isFloatType(t2) {
		return t2
	}
	if isFloatType(t1) && isIntegerType(t2) {
		return t1
	}

	// Wider integer types take precedence
	if isIntegerType(t1) && isIntegerType(t2) {
		if typeSize(t1) > typeSize(t2) {
			return t1
		}

		return t2
	}

	// Wider float types take precedence
	if isFloatType(t1) && isFloatType(t2) {
		if t1 == dukdb.TYPE_DOUBLE {
			return t1
		}

		return t2
	}

	// Default to the first type
	return t1
}

func isIntegerType(t dukdb.Type) bool {
	switch t {
	case dukdb.TYPE_TINYINT,
		dukdb.TYPE_SMALLINT,
		dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT,
		dukdb.TYPE_UTINYINT,
		dukdb.TYPE_USMALLINT,
		dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT,
		dukdb.TYPE_HUGEINT,
		dukdb.TYPE_UHUGEINT:
		return true
	}

	return false
}

func isFloatType(t dukdb.Type) bool {
	return t == dukdb.TYPE_FLOAT ||
		t == dukdb.TYPE_DOUBLE
}

func typeSize(t dukdb.Type) int {
	switch t {
	case dukdb.TYPE_TINYINT, dukdb.TYPE_UTINYINT:
		return 1
	case dukdb.TYPE_SMALLINT,
		dukdb.TYPE_USMALLINT:
		return 2
	case dukdb.TYPE_INTEGER, dukdb.TYPE_UINTEGER:
		return 4
	case dukdb.TYPE_BIGINT, dukdb.TYPE_UBIGINT:
		return 8
	case dukdb.TYPE_HUGEINT, dukdb.TYPE_UHUGEINT:
		return 16
	default:
		return 0
	}
}

func inferFunctionResultType(
	name string,
	args []BoundExpr,
) dukdb.Type {
	name = strings.ToUpper(name)
	switch name {
	case "COUNT":
		return dukdb.TYPE_BIGINT
	case "SUM":
		if len(args) > 0 {
			switch args[0].ResultType() {
			case dukdb.TYPE_TINYINT,
				dukdb.TYPE_SMALLINT,
				dukdb.TYPE_INTEGER,
				dukdb.TYPE_BIGINT:
				return dukdb.TYPE_BIGINT
			case dukdb.TYPE_FLOAT,
				dukdb.TYPE_DOUBLE:
				return dukdb.TYPE_DOUBLE
			case dukdb.TYPE_DECIMAL:
				return dukdb.TYPE_DECIMAL
			}
		}

		return dukdb.TYPE_BIGINT
	case "AVG":
		return dukdb.TYPE_DOUBLE
	case "MIN", "MAX":
		if len(args) > 0 {
			return args[0].ResultType()
		}

		return dukdb.TYPE_ANY
	case "COALESCE":
		if len(args) > 0 {
			return args[0].ResultType()
		}

		return dukdb.TYPE_ANY
	case "ABS":
		if len(args) > 0 {
			return args[0].ResultType()
		}

		return dukdb.TYPE_DOUBLE
	case "UPPER",
		"LOWER",
		"TRIM",
		"LTRIM",
		"RTRIM",
		"SUBSTR",
		"SUBSTRING",
		"CONCAT",
		"REPLACE":
		return dukdb.TYPE_VARCHAR
	case "LENGTH",
		"CHAR_LENGTH",
		"CHARACTER_LENGTH":
		return dukdb.TYPE_INTEGER
	case "NOW", "CURRENT_TIMESTAMP":
		return dukdb.TYPE_TIMESTAMP
	case "CURRENT_DATE":
		return dukdb.TYPE_DATE
	case "CURRENT_TIME":
		return dukdb.TYPE_TIME
	default:
		return dukdb.TYPE_ANY
	}
}

// getFunctionArgTypes returns expected argument types for known functions.
// This is used for parameter type inference in function calls.
func getFunctionArgTypes(
	name string,
	argCount int,
) []dukdb.Type {
	name = strings.ToUpper(name)
	switch name {
	case "ABS", "SUM", "AVG", "MIN", "MAX":
		// Numeric functions - first arg should be numeric
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_DOUBLE}
		}
	case "UPPER",
		"LOWER",
		"TRIM",
		"LTRIM",
		"RTRIM",
		"LENGTH",
		"CHAR_LENGTH",
		"CHARACTER_LENGTH":
		// String functions
		if argCount >= 1 {
			return []dukdb.Type{
				dukdb.TYPE_VARCHAR,
			}
		}
	case "SUBSTR", "SUBSTRING":
		// SUBSTR(string, start, [length])
		types := make([]dukdb.Type, argCount)
		if argCount >= 1 {
			types[0] = dukdb.TYPE_VARCHAR
		}
		if argCount >= 2 {
			types[1] = dukdb.TYPE_INTEGER
		}
		if argCount >= 3 {
			types[2] = dukdb.TYPE_INTEGER
		}

		return types
	case "REPLACE":
		// REPLACE(string, from, to)
		if argCount >= 3 {
			return []dukdb.Type{
				dukdb.TYPE_VARCHAR,
				dukdb.TYPE_VARCHAR,
				dukdb.TYPE_VARCHAR,
			}
		}
	case "CONCAT":
		// CONCAT accepts varargs of VARCHAR
		types := make([]dukdb.Type, argCount)
		for i := range types {
			types[i] = dukdb.TYPE_VARCHAR
		}

		return types
	case "COALESCE":
		// COALESCE accepts homogeneous types - first arg determines type
		// Return empty to let the first arg drive inference
		return nil
	case "COUNT":
		// COUNT can take any type
		if argCount >= 1 {
			return []dukdb.Type{dukdb.TYPE_ANY}
		}
	}

	return nil
}

// GetParamTypes returns the inferred parameter types after binding.
func (b *Binder) GetParamTypes() map[int]dukdb.Type {
	if b.scope == nil {
		return nil
	}

	return b.scope.params
}

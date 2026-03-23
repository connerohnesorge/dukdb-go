package binder

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
	internaltypes "github.com/dukdb/dukdb-go/internal/types"
)

// bindCreateView binds a CREATE VIEW statement.
func (b *Binder) bindCreateView(
	s *parser.CreateViewStmt,
) (*BoundCreateViewStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Check if view already exists
	if _, ok := b.catalog.GetViewInSchema(schema, s.View); ok {
		if s.IfNotExists {
			// IF NOT EXISTS - silently succeed
			return &BoundCreateViewStmt{
				Schema:      schema,
				View:        s.View,
				IfNotExists: true,
			}, nil
		}
		return nil, b.errorf("view already exists: %s.%s", schema, s.View)
	}

	// Bind the SELECT query
	boundQuery, err := b.bindSelect(s.Query)
	if err != nil {
		return nil, b.errorf("failed to bind view query: %v", err)
	}

	// Serialize the query for storage (we'll use a simple serialization)
	// In a real implementation, we'd want to serialize the AST properly
	queryText := serializeSelectStmt(s.Query)

	return &BoundCreateViewStmt{
		Schema:      schema,
		View:        s.View,
		IfNotExists: s.IfNotExists,
		Query:       boundQuery,
		QueryText:   queryText,
	}, nil
}

// bindDropView binds a DROP VIEW statement.
func (b *Binder) bindDropView(
	s *parser.DropViewStmt,
) (*BoundDropViewStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Check if view exists
	if _, ok := b.catalog.GetViewInSchema(schema, s.View); !ok {
		if s.IfExists {
			// IF EXISTS - silently succeed
			return &BoundDropViewStmt{
				Schema:   schema,
				View:     s.View,
				IfExists: true,
			}, nil
		}
		return nil, b.errorf("view not found: %s.%s", schema, s.View)
	}

	return &BoundDropViewStmt{
		Schema:   schema,
		View:     s.View,
		IfExists: s.IfExists,
	}, nil
}

// bindCreateIndex binds a CREATE INDEX statement.
func (b *Binder) bindCreateIndex(
	s *parser.CreateIndexStmt,
) (*BoundCreateIndexStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Check if index already exists
	if _, ok := b.catalog.GetIndexInSchema(schema, s.Index); ok {
		if s.IfNotExists {
			// IF NOT EXISTS - silently succeed
			return &BoundCreateIndexStmt{
				Schema:      schema,
				Table:       s.Table,
				Index:       s.Index,
				IfNotExists: true,
			}, nil
		}
		return nil, b.errorf("index already exists: %s.%s", schema, s.Index)
	}

	// Validate that table exists
	tableDef, ok := b.catalog.GetTableInSchema(schema, s.Table)
	if !ok {
		return nil, b.errorf("table not found: %s.%s", schema, s.Table)
	}

	// Validate that all columns exist in the table
	for _, colName := range s.Columns {
		if _, ok := tableDef.GetColumnIndex(colName); !ok {
			return nil, b.errorf("column not found in table %s: %s", s.Table, colName)
		}
	}

	return &BoundCreateIndexStmt{
		Schema:      schema,
		Table:       s.Table,
		Index:       s.Index,
		IfNotExists: s.IfNotExists,
		Columns:     s.Columns,
		IsUnique:    s.IsUnique,
		TableDef:    tableDef,
	}, nil
}

// bindDropIndex binds a DROP INDEX statement.
func (b *Binder) bindDropIndex(
	s *parser.DropIndexStmt,
) (*BoundDropIndexStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Check if index exists
	if _, ok := b.catalog.GetIndexInSchema(schema, s.Index); !ok {
		if s.IfExists {
			// IF EXISTS - silently succeed
			return &BoundDropIndexStmt{
				Schema:   schema,
				Index:    s.Index,
				IfExists: true,
			}, nil
		}
		return nil, b.errorf("index not found: %s.%s", schema, s.Index)
	}

	return &BoundDropIndexStmt{
		Schema:   schema,
		Index:    s.Index,
		IfExists: s.IfExists,
	}, nil
}

// bindCreateSequence binds a CREATE SEQUENCE statement.
func (b *Binder) bindCreateSequence(
	s *parser.CreateSequenceStmt,
) (*BoundCreateSequenceStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Check if sequence already exists
	if _, ok := b.catalog.GetSequenceInSchema(schema, s.Sequence); ok {
		if s.IfNotExists {
			// IF NOT EXISTS - silently succeed
			return &BoundCreateSequenceStmt{
				Schema:      schema,
				Sequence:    s.Sequence,
				IfNotExists: true,
			}, nil
		}
		return nil, b.errorf("sequence already exists: %s.%s", schema, s.Sequence)
	}

	// Validate sequence parameters
	if s.IncrementBy == 0 {
		return nil, b.errorf("sequence increment cannot be zero")
	}

	// Validate min/max values
	if s.MinValue != nil && s.MaxValue != nil {
		if *s.MinValue >= *s.MaxValue {
			return nil, b.errorf("sequence minimum value must be less than maximum value")
		}
	}

	// Validate start value
	if s.MinValue != nil && s.StartWith < *s.MinValue {
		return nil, b.errorf("sequence start value cannot be less than minimum value")
	}
	if s.MaxValue != nil && s.StartWith > *s.MaxValue {
		return nil, b.errorf("sequence start value cannot be greater than maximum value")
	}

	return &BoundCreateSequenceStmt{
		Schema:      schema,
		Sequence:    s.Sequence,
		IfNotExists: s.IfNotExists,
		StartWith:   s.StartWith,
		IncrementBy: s.IncrementBy,
		MinValue:    s.MinValue,
		MaxValue:    s.MaxValue,
		IsCycle:     s.IsCycle,
	}, nil
}

// bindDropSequence binds a DROP SEQUENCE statement.
func (b *Binder) bindDropSequence(
	s *parser.DropSequenceStmt,
) (*BoundDropSequenceStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Check if sequence exists
	if _, ok := b.catalog.GetSequenceInSchema(schema, s.Sequence); !ok {
		if s.IfExists {
			// IF EXISTS - silently succeed
			return &BoundDropSequenceStmt{
				Schema:   schema,
				Sequence: s.Sequence,
				IfExists: true,
			}, nil
		}
		return nil, b.errorf("sequence not found: %s.%s", schema, s.Sequence)
	}

	return &BoundDropSequenceStmt{
		Schema:   schema,
		Sequence: s.Sequence,
		IfExists: s.IfExists,
	}, nil
}

// bindCreateSchema binds a CREATE SCHEMA statement.
func (b *Binder) bindCreateSchema(
	s *parser.CreateSchemaStmt,
) (*BoundCreateSchemaStmt, error) {
	// Check if schema already exists
	if _, ok := b.catalog.GetSchema(s.Schema); ok {
		if s.IfNotExists {
			// IF NOT EXISTS - silently succeed
			return &BoundCreateSchemaStmt{
				Schema:      s.Schema,
				IfNotExists: true,
			}, nil
		}
		return nil, b.errorf("schema already exists: %s", s.Schema)
	}

	return &BoundCreateSchemaStmt{
		Schema:      s.Schema,
		IfNotExists: s.IfNotExists,
	}, nil
}

// bindDropSchema binds a DROP SCHEMA statement.
func (b *Binder) bindDropSchema(
	s *parser.DropSchemaStmt,
) (*BoundDropSchemaStmt, error) {
	// Cannot drop main schema
	if s.Schema == "main" {
		return nil, b.errorf("cannot drop main schema")
	}

	// Check if schema exists
	schema, ok := b.catalog.GetSchema(s.Schema)
	if !ok {
		if s.IfExists {
			// IF EXISTS - silently succeed
			return &BoundDropSchemaStmt{
				Schema:   s.Schema,
				IfExists: true,
			}, nil
		}
		return nil, b.errorf("schema not found: %s", s.Schema)
	}

	// If not CASCADE, check if schema has objects
	if !s.Cascade {
		tables := schema.ListTables()
		views := schema.ListViews()
		indexes := schema.ListIndexes()
		sequences := schema.ListSequences()

		objectCount := len(tables) + len(views) + len(indexes) + len(sequences)
		if objectCount > 0 {
			return nil, b.errorf(
				"cannot drop schema %s because it contains objects (use CASCADE to drop all objects)",
				s.Schema,
			)
		}
	}

	return &BoundDropSchemaStmt{
		Schema:   s.Schema,
		IfExists: s.IfExists,
		Cascade:  s.Cascade,
	}, nil
}

// bindAlterTable binds an ALTER TABLE statement.
func (b *Binder) bindAlterTable(
	s *parser.AlterTableStmt,
) (*BoundAlterTableStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Validate that table exists (unless IF EXISTS is specified)
	tableDef, ok := b.catalog.GetTableInSchema(schema, s.Table)
	if !ok {
		if s.IfExists {
			// IF EXISTS - silently succeed
			return &BoundAlterTableStmt{
				Schema:    schema,
				Table:     s.Table,
				Operation: s.Operation,
				IfExists:  true,
			}, nil
		}
		return nil, b.errorf("table not found: %s.%s", schema, s.Table)
	}

	bound := &BoundAlterTableStmt{
		Schema:    schema,
		Table:     s.Table,
		TableDef:  tableDef,
		Operation: s.Operation,
		IfExists:  s.IfExists,
	}

	// Validate operation-specific requirements
	switch s.Operation {
	case parser.AlterTableRenameTo:
		if s.NewTableName == "" {
			return nil, b.errorf("new table name is required for RENAME TO")
		}
		// Check if new name already exists
		if _, ok := b.catalog.GetTableInSchema(schema, s.NewTableName); ok {
			return nil, b.errorf("table already exists: %s.%s", schema, s.NewTableName)
		}
		bound.NewTableName = s.NewTableName

	case parser.AlterTableRenameColumn:
		if s.OldColumn == "" || s.NewColumn == "" {
			return nil, b.errorf("both old and new column names are required for RENAME COLUMN")
		}
		// Check if old column exists
		if _, ok := tableDef.GetColumnIndex(s.OldColumn); !ok {
			return nil, b.errorf("column not found: %s", s.OldColumn)
		}
		// Check if new column name already exists
		if _, ok := tableDef.GetColumnIndex(s.NewColumn); ok {
			return nil, b.errorf("column already exists: %s", s.NewColumn)
		}
		bound.OldColumn = s.OldColumn
		bound.NewColumn = s.NewColumn

	case parser.AlterTableDropColumn:
		if s.DropColumn == "" {
			return nil, b.errorf("column name is required for DROP COLUMN")
		}
		// Check if column exists
		if _, ok := tableDef.GetColumnIndex(s.DropColumn); !ok {
			return nil, b.errorf("column not found: %s", s.DropColumn)
		}
		// Cannot drop the last column
		if len(tableDef.Columns) == 1 {
			return nil, b.errorf("cannot drop the last column of a table")
		}
		bound.DropColumn = s.DropColumn

	case parser.AlterTableAddColumn:
		if s.AddColumn == nil {
			return nil, b.errorf("column definition is required for ADD COLUMN")
		}
		// Check if column already exists
		if _, ok := tableDef.GetColumnIndex(s.AddColumn.Name); ok {
			return nil, b.errorf("column already exists: %s", s.AddColumn.Name)
		}
		// Convert parser column def to catalog column def
		colDef := catalog.NewColumnDef(s.AddColumn.Name, s.AddColumn.DataType)
		if s.AddColumn.TypeInfo != nil {
			colDef.TypeInfo = s.AddColumn.TypeInfo
		}
		colDef.Nullable = !s.AddColumn.NotNull
		if s.AddColumn.Default != nil {
			colDef.HasDefault = true
			if lit, ok := s.AddColumn.Default.(*parser.Literal); ok {
				colDef.DefaultValue = lit.Value
			}
		}
		bound.AddColumn = colDef

	case parser.AlterTableAlterColumnType:
		if s.AlterColumn == "" {
			return nil, b.errorf("column name is required for ALTER COLUMN TYPE")
		}
		if _, ok := tableDef.GetColumnIndex(s.AlterColumn); !ok {
			return nil, b.errorf("column not found: %s", s.AlterColumn)
		}
		// Resolve type from raw string
		info, err := internaltypes.ParseTypeExpression(s.NewTypeRaw)
		if err != nil {
			return nil, b.errorf("invalid type %q: %v", s.NewTypeRaw, err)
		}
		bound.AlterColumn = s.AlterColumn
		bound.NewColumnType = info.InternalType()

	case parser.AlterTableAddConstraint:
		if s.Constraint == nil {
			return nil, b.errorf("constraint definition is required for ADD CONSTRAINT")
		}
		bound.Constraint = s.Constraint

	case parser.AlterTableDropConstraint:
		if s.ConstraintName == "" {
			return nil, b.errorf("constraint name is required for DROP CONSTRAINT")
		}
		bound.ConstraintName = s.ConstraintName

	case parser.AlterTableSetColumnDefault:
		if s.AlterColumn == "" {
			return nil, b.errorf("column name is required for ALTER COLUMN SET DEFAULT")
		}
		if _, ok := tableDef.GetColumnIndex(s.AlterColumn); !ok {
			return nil, b.errorf("column not found: %s", s.AlterColumn)
		}
		boundExpr, err := b.bindExpr(s.DefaultExpr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		bound.AlterColumn = s.AlterColumn
		bound.DefaultExpr = boundExpr

	case parser.AlterTableDropColumnDefault:
		if s.AlterColumn == "" {
			return nil, b.errorf("column name is required for ALTER COLUMN DROP DEFAULT")
		}
		if _, ok := tableDef.GetColumnIndex(s.AlterColumn); !ok {
			return nil, b.errorf("column not found: %s", s.AlterColumn)
		}
		bound.AlterColumn = s.AlterColumn

	case parser.AlterTableSetColumnNotNull:
		if s.AlterColumn == "" {
			return nil, b.errorf("column name is required for ALTER COLUMN SET NOT NULL")
		}
		if _, ok := tableDef.GetColumnIndex(s.AlterColumn); !ok {
			return nil, b.errorf("column not found: %s", s.AlterColumn)
		}
		bound.AlterColumn = s.AlterColumn

	case parser.AlterTableDropColumnNotNull:
		if s.AlterColumn == "" {
			return nil, b.errorf("column name is required for ALTER COLUMN DROP NOT NULL")
		}
		if _, ok := tableDef.GetColumnIndex(s.AlterColumn); !ok {
			return nil, b.errorf("column not found: %s", s.AlterColumn)
		}
		bound.AlterColumn = s.AlterColumn

	case parser.AlterTableSetOption:
		// Not yet implemented
		return nil, b.errorf("ALTER TABLE SET OPTION is not yet implemented")

	default:
		return nil, b.errorf("unsupported ALTER TABLE operation: %v", s.Operation)
	}

	return bound, nil
}

// serializeSelectStmt serializes a SELECT statement to a string.
// This reconstructs valid SQL from the AST for view storage.
func serializeSelectStmt(stmt *parser.SelectStmt) string {
	var sql string

	// Handle CTEs (WITH clause)
	if len(stmt.CTEs) > 0 {
		sql += "WITH "
		for i, cte := range stmt.CTEs {
			if i > 0 {
				sql += ", "
			}
			sql += cte.Name
			if len(cte.Columns) > 0 {
				sql += " ("
				for j, col := range cte.Columns {
					if j > 0 {
						sql += ", "
					}
					sql += col
				}
				sql += ")"
			}
			sql += " AS (" + serializeSelectStmt(cte.Query) + ")"
		}
		sql += " "
	}

	// SELECT clause
	sql += "SELECT "
	if stmt.Distinct {
		sql += "DISTINCT "
	}

	// Columns
	for i, col := range stmt.Columns {
		if i > 0 {
			sql += ", "
		}
		if col.Star {
			sql += "*"
		} else {
			sql += serializeExpr(col.Expr)
			if col.Alias != "" {
				sql += " AS " + col.Alias
			}
		}
	}

	// FROM clause
	if stmt.From != nil {
		sql += " FROM "
		for i, table := range stmt.From.Tables {
			if i > 0 {
				sql += ", "
			}
			sql += serializeTableRef(&table)
		}

		// JOINs
		for _, join := range stmt.From.Joins {
			switch join.Type {
			case parser.JoinTypeInner:
				sql += " JOIN "
			case parser.JoinTypeLeft:
				sql += " LEFT JOIN "
			case parser.JoinTypeRight:
				sql += " RIGHT JOIN "
			case parser.JoinTypeFull:
				sql += " FULL JOIN "
			case parser.JoinTypeCross:
				sql += " CROSS JOIN "
			case parser.JoinTypePositional:
				sql += " POSITIONAL JOIN "
			case parser.JoinTypeAsOf:
				sql += " ASOF JOIN "
			case parser.JoinTypeAsOfLeft:
				sql += " ASOF LEFT JOIN "
			}
			sql += serializeTableRef(&join.Table)
			if join.Condition != nil {
				sql += " ON " + serializeExpr(join.Condition)
			}
		}
	}

	// WHERE clause
	if stmt.Where != nil {
		sql += " WHERE " + serializeExpr(stmt.Where)
	}

	// GROUP BY clause
	if len(stmt.GroupBy) > 0 {
		sql += " GROUP BY "
		for i, expr := range stmt.GroupBy {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(expr)
		}
	}

	// HAVING clause
	if stmt.Having != nil {
		sql += " HAVING " + serializeExpr(stmt.Having)
	}

	// ORDER BY clause
	if len(stmt.OrderBy) > 0 {
		sql += " ORDER BY "
		for i, orderBy := range stmt.OrderBy {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(orderBy.Expr)
			if orderBy.Desc {
				sql += " DESC"
			}
		}
	}

	// LIMIT clause
	if stmt.Limit != nil {
		sql += " LIMIT " + serializeExpr(stmt.Limit)
	}

	// OFFSET clause
	if stmt.Offset != nil {
		sql += " OFFSET " + serializeExpr(stmt.Offset)
	}

	// Set operations (UNION, INTERSECT, EXCEPT)
	if stmt.SetOp != parser.SetOpNone && stmt.Right != nil {
		switch stmt.SetOp {
		case parser.SetOpUnion:
			sql += " UNION "
		case parser.SetOpUnionAll:
			sql += " UNION ALL "
		case parser.SetOpIntersect:
			sql += " INTERSECT "
		case parser.SetOpIntersectAll:
			sql += " INTERSECT ALL "
		case parser.SetOpExcept:
			sql += " EXCEPT "
		case parser.SetOpExceptAll:
			sql += " EXCEPT ALL "
		}
		sql += serializeSelectStmt(stmt.Right)
	}

	return sql
}

// serializeTableRef serializes a table reference to SQL.
func serializeTableRef(table *parser.TableRef) string {
	var sql string

	if table.Subquery != nil {
		sql += "(" + serializeSelectStmt(table.Subquery) + ")"
	} else if table.TableFunction != nil {
		sql += table.TableFunction.Name + "("
		// Positional args
		for i, arg := range table.TableFunction.Args {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(arg)
		}
		// Named args
		if len(table.TableFunction.NamedArgs) > 0 {
			if len(table.TableFunction.Args) > 0 {
				sql += ", "
			}
			i := 0
			for key, val := range table.TableFunction.NamedArgs {
				if i > 0 {
					sql += ", "
				}
				sql += key + " = " + serializeExpr(val)
				i++
			}
		}
		sql += ")"
	} else {
		if table.Schema != "" && table.Schema != "main" {
			sql += table.Schema + "."
		}
		sql += table.TableName
	}

	if table.Alias != "" {
		sql += " AS " + table.Alias
	}

	return sql
}

// serializeExpr serializes an expression to SQL.
func serializeExpr(expr parser.Expr) string {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Column
		}
		return e.Column

	case *parser.Literal:
		return serializeLiteral(e)

	case *parser.Parameter:
		if e.Position > 0 {
			return fmt.Sprintf("$%d", e.Position)
		}
		return "?"

	case *parser.BinaryExpr:
		return fmt.Sprintf("(%s %s %s)",
			serializeExpr(e.Left),
			serializeBinaryOp(e.Op),
			serializeExpr(e.Right))

	case *parser.UnaryExpr:
		return serializeUnaryExpr(e)

	case *parser.FunctionCall:
		return serializeFunctionCall(e)

	case *parser.CastExpr:
		keyword := "CAST"
		if e.TryCast {
			keyword = "TRY_CAST"
		}
		return fmt.Sprintf("%s(%s AS %s)",
			keyword,
			serializeExpr(e.Expr),
			e.TargetType.String())

	case *parser.CaseExpr:
		return serializeCaseExpr(e)

	case *parser.BetweenExpr:
		if e.Not {
			return fmt.Sprintf("%s NOT BETWEEN %s AND %s",
				serializeExpr(e.Expr),
				serializeExpr(e.Low),
				serializeExpr(e.High))
		}
		return fmt.Sprintf("%s BETWEEN %s AND %s",
			serializeExpr(e.Expr),
			serializeExpr(e.Low),
			serializeExpr(e.High))

	case *parser.InListExpr:
		sql := serializeExpr(e.Expr)
		if e.Not {
			sql += " NOT IN ("
		} else {
			sql += " IN ("
		}
		for i, val := range e.Values {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(val)
		}
		sql += ")"
		return sql

	case *parser.InSubqueryExpr:
		sql := serializeExpr(e.Expr)
		if e.Not {
			sql += " NOT IN ("
		} else {
			sql += " IN ("
		}
		sql += serializeSelectStmt(e.Subquery) + ")"
		return sql

	case *parser.ExistsExpr:
		if e.Not {
			return "NOT EXISTS (" + serializeSelectStmt(e.Subquery) + ")"
		}
		return "EXISTS (" + serializeSelectStmt(e.Subquery) + ")"

	case *parser.StarExpr:
		if e.Table != "" {
			return e.Table + ".*"
		}
		return "*"

	case *parser.SelectStmt:
		return "(" + serializeSelectStmt(e) + ")"

	case *parser.ExtractExpr:
		return fmt.Sprintf("EXTRACT(%s FROM %s)", e.Part, serializeExpr(e.Source))

	case *parser.IntervalLiteral:
		return serializeIntervalLiteral(e)

	case *parser.WindowExpr:
		return serializeWindowExpr(e)

	default:
		return "UNKNOWN_EXPR"
	}
}

// serializeLiteral serializes a literal value to SQL.
func serializeLiteral(lit *parser.Literal) string {
	if lit.Value == nil {
		return "NULL"
	}

	switch lit.Type {
	case dukdb.TYPE_BOOLEAN:
		if lit.Value.(bool) {
			return "TRUE"
		}
		return "FALSE"
	case dukdb.TYPE_VARCHAR:
		// Escape single quotes
		s := lit.Value.(string)
		s = fmt.Sprintf("'%s'", s)
		return s
	case dukdb.TYPE_INVALID, dukdb.TYPE_TINYINT, dukdb.TYPE_SMALLINT, dukdb.TYPE_INTEGER,
		dukdb.TYPE_BIGINT, dukdb.TYPE_UTINYINT, dukdb.TYPE_USMALLINT, dukdb.TYPE_UINTEGER,
		dukdb.TYPE_UBIGINT, dukdb.TYPE_FLOAT, dukdb.TYPE_DOUBLE, dukdb.TYPE_TIMESTAMP,
		dukdb.TYPE_DATE, dukdb.TYPE_TIME, dukdb.TYPE_INTERVAL, dukdb.TYPE_HUGEINT,
		dukdb.TYPE_UHUGEINT, dukdb.TYPE_BLOB, dukdb.TYPE_DECIMAL, dukdb.TYPE_TIMESTAMP_S,
		dukdb.TYPE_TIMESTAMP_MS, dukdb.TYPE_TIMESTAMP_NS, dukdb.TYPE_ENUM, dukdb.TYPE_LIST,
		dukdb.TYPE_STRUCT, dukdb.TYPE_MAP, dukdb.TYPE_ARRAY, dukdb.TYPE_UUID, dukdb.TYPE_UNION,
		dukdb.TYPE_BIT, dukdb.TYPE_TIME_TZ, dukdb.TYPE_TIMESTAMP_TZ, dukdb.TYPE_ANY,
		dukdb.TYPE_BIGNUM, dukdb.TYPE_SQLNULL, dukdb.TYPE_JSON, dukdb.TYPE_GEOMETRY,
		dukdb.TYPE_LAMBDA, dukdb.TYPE_VARIANT:
		return fmt.Sprintf("%v", lit.Value)
	}
	return fmt.Sprintf("%v", lit.Value)
}

// serializeBinaryOp serializes a binary operator.
func serializeBinaryOp(op parser.BinaryOp) string {
	switch op {
	case parser.OpAdd:
		return "+"
	case parser.OpSub:
		return "-"
	case parser.OpMul:
		return "*"
	case parser.OpDiv:
		return "/"
	case parser.OpMod:
		return "%"
	case parser.OpEq:
		return "="
	case parser.OpNe:
		return "<>"
	case parser.OpLt:
		return "<"
	case parser.OpLe:
		return "<="
	case parser.OpGt:
		return ">"
	case parser.OpGe:
		return ">="
	case parser.OpAnd:
		return "AND"
	case parser.OpOr:
		return "OR"
	case parser.OpLike:
		return "LIKE"
	case parser.OpILike:
		return "ILIKE"
	case parser.OpNotLike:
		return "NOT LIKE"
	case parser.OpNotILike:
		return "NOT ILIKE"
	case parser.OpSimilarTo:
		return "SIMILAR TO"
	case parser.OpNotSimilarTo:
		return "NOT SIMILAR TO"
	case parser.OpConcat:
		return "||"
	default:
		return "UNKNOWN_OP"
	}
}

// serializeUnaryExpr serializes a unary expression.
func serializeUnaryExpr(e *parser.UnaryExpr) string {
	switch e.Op {
	case parser.OpNot:
		return "NOT " + serializeExpr(e.Expr)
	case parser.OpNeg:
		return "-" + serializeExpr(e.Expr)
	case parser.OpPos:
		return "+" + serializeExpr(e.Expr)
	case parser.OpIsNull:
		return serializeExpr(e.Expr) + " IS NULL"
	case parser.OpIsNotNull:
		return serializeExpr(e.Expr) + " IS NOT NULL"
	default:
		return "UNKNOWN_UNARY"
	}
}

// serializeFunctionCall serializes a function call.
func serializeFunctionCall(fn *parser.FunctionCall) string {
	sql := fn.Name + "("
	if fn.Star {
		sql += "*"
	} else {
		if fn.Distinct {
			sql += "DISTINCT "
		}
		for i, arg := range fn.Args {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(arg)
		}
	}
	sql += ")"
	return sql
}

// serializeCaseExpr serializes a CASE expression.
func serializeCaseExpr(e *parser.CaseExpr) string {
	sql := "CASE"
	if e.Operand != nil {
		sql += " " + serializeExpr(e.Operand)
	}
	for _, when := range e.Whens {
		sql += " WHEN " + serializeExpr(when.Condition)
		sql += " THEN " + serializeExpr(when.Result)
	}
	if e.Else != nil {
		sql += " ELSE " + serializeExpr(e.Else)
	}
	sql += " END"
	return sql
}

// serializeIntervalLiteral serializes an interval literal.
func serializeIntervalLiteral(e *parser.IntervalLiteral) string {
	// Simple serialization - just reconstruct as microseconds for now
	// A more complete implementation would analyze the components
	totalMicros := e.Micros + int64(e.Days)*24*3600*1000000 + int64(e.Months)*30*24*3600*1000000
	return fmt.Sprintf("INTERVAL '%d' MICROSECOND", totalMicros)
}

// serializeWindowExpr serializes a window expression.
func serializeWindowExpr(e *parser.WindowExpr) string {
	sql := serializeFunctionCall(e.Function)

	// IGNORE/RESPECT NULLS
	if e.IgnoreNulls {
		sql += " IGNORE NULLS"
	}

	// FILTER clause
	if e.Filter != nil {
		sql += " FILTER (WHERE " + serializeExpr(e.Filter) + ")"
	}

	sql += " OVER ("

	// PARTITION BY
	if len(e.PartitionBy) > 0 {
		sql += "PARTITION BY "
		for i, expr := range e.PartitionBy {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(expr)
		}
	}

	// ORDER BY
	if len(e.OrderBy) > 0 {
		if len(e.PartitionBy) > 0 {
			sql += " "
		}
		sql += "ORDER BY "
		for i, orderBy := range e.OrderBy {
			if i > 0 {
				sql += ", "
			}
			sql += serializeExpr(orderBy.Expr)
			if orderBy.Desc {
				sql += " DESC"
			}
			if orderBy.NullsFirst {
				sql += " NULLS FIRST"
			}
		}
	}

	// Frame clause (if present)
	if e.Frame != nil {
		// Add frame serialization here if needed
	}

	sql += ")"
	return sql
}

// ---------- Secret DDL Binder Methods ----------

// bindCreateSecret binds a CREATE SECRET statement.
func (b *Binder) bindCreateSecret(
	s *parser.CreateSecretStmt,
) (*BoundCreateSecretStmt, error) {
	// Validate secret name
	if s.Name == "" {
		return nil, b.errorf("secret name is required")
	}

	// Validate secret type
	if s.SecretType == "" {
		return nil, b.errorf("TYPE is required for CREATE SECRET")
	}

	// Create bound statement
	return &BoundCreateSecretStmt{
		Name:        s.Name,
		IfNotExists: s.IfNotExists,
		OrReplace:   s.OrReplace,
		Persistent:  s.Persistent,
		SecretType:  s.SecretType,
		Provider:    s.Provider,
		Scope:       s.Scope,
		Options:     s.Options,
	}, nil
}

// bindDropSecret binds a DROP SECRET statement.
func (b *Binder) bindDropSecret(
	s *parser.DropSecretStmt,
) (*BoundDropSecretStmt, error) {
	// Validate secret name
	if s.Name == "" {
		return nil, b.errorf("secret name is required")
	}

	return &BoundDropSecretStmt{
		Name:     s.Name,
		IfExists: s.IfExists,
	}, nil
}

// bindAlterSecret binds an ALTER SECRET statement.
func (b *Binder) bindAlterSecret(
	s *parser.AlterSecretStmt,
) (*BoundAlterSecretStmt, error) {
	// Validate secret name
	if s.Name == "" {
		return nil, b.errorf("secret name is required")
	}

	// Validate that at least one option is provided
	if len(s.Options) == 0 {
		return nil, b.errorf("at least one option must be specified for ALTER SECRET")
	}

	return &BoundAlterSecretStmt{
		Name:    s.Name,
		Options: s.Options,
	}, nil
}

// bindComment binds a COMMENT ON statement, validating the target object exists.
func (b *Binder) bindComment(s *parser.CommentStmt) (*BoundCommentStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Validate object exists
	switch s.ObjectType {
	case "TABLE":
		if _, ok := b.catalog.GetTableInSchema(schema, s.ObjectName); !ok {
			return nil, b.errorf("table not found: %s", s.ObjectName)
		}
	case "COLUMN":
		tableDef, ok := b.catalog.GetTableInSchema(schema, s.ObjectName)
		if !ok {
			return nil, b.errorf("table not found: %s", s.ObjectName)
		}
		if _, ok := tableDef.GetColumnIndex(s.ColumnName); !ok {
			return nil, b.errorf("column %q not found in table %q", s.ColumnName, s.ObjectName)
		}
	case "VIEW":
		if _, ok := b.catalog.GetViewInSchema(schema, s.ObjectName); !ok {
			return nil, b.errorf("view not found: %s", s.ObjectName)
		}
	case "INDEX":
		if _, ok := b.catalog.GetIndexInSchema(schema, s.ObjectName); !ok {
			return nil, b.errorf("index not found: %s", s.ObjectName)
		}
	case "SCHEMA":
		if _, ok := b.catalog.GetSchema(s.ObjectName); !ok {
			return nil, b.errorf("schema not found: %s", s.ObjectName)
		}
	}

	return &BoundCommentStmt{
		ObjectType: s.ObjectType,
		Schema:     schema,
		ObjectName: s.ObjectName,
		ColumnName: s.ColumnName,
		Comment:    s.Comment,
	}, nil
}

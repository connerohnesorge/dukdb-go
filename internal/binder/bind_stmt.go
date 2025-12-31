package binder

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

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

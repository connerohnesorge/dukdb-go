package binder

import (
	"fmt"
	"slices"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// bindPivot binds a PIVOT statement.
// PIVOT transforms rows into columns using aggregation and conditional logic.
func (b *Binder) bindPivot(s *parser.PivotStmt) (*BoundPivotStmt, error) {
	// Push new scope for binding
	oldScope := b.scope
	b.scope = newBindScope(oldScope)
	defer func() { b.scope = oldScope }()

	// Bind the source table reference
	sourceRef, err := b.bindTableRef(s.Source)
	if err != nil {
		return nil, err
	}

	// Bind the FOR column (the column whose values become pivot column names)
	var forColumn *BoundColumnRef
	if s.ForColumn != "" {
		// Create a column reference and bind it
		colRef := &parser.ColumnRef{Column: s.ForColumn}
		boundExpr, err := b.bindExpr(colRef, dukdb.TYPE_ANY)
		if err != nil {
			return nil, b.errorf("PIVOT FOR column not found: %s", s.ForColumn)
		}
		var ok bool
		forColumn, ok = boundExpr.(*BoundColumnRef)
		if !ok {
			return nil, b.errorf("PIVOT FOR clause must reference a column")
		}
	}

	// Validate that we have IN values
	if len(s.PivotOn) == 0 {
		return nil, b.errorf("PIVOT requires at least one IN value")
	}

	// Bind the IN values (these become column names)
	var inValues []any
	for _, expr := range s.PivotOn {
		val, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		// Extract the literal value
		if lit, ok := val.(*BoundLiteral); ok {
			inValues = append(inValues, lit.Value)
		} else {
			return nil, b.errorf("PIVOT IN values must be literals")
		}
	}

	// Bind the aggregates
	var boundAggregates []*BoundPivotAggregate
	for _, agg := range s.Using {
		boundExpr, err := b.bindExpr(agg.Expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		boundAggregates = append(boundAggregates, &BoundPivotAggregate{
			Function: agg.Function,
			Expr:     boundExpr,
			Alias:    agg.Alias,
		})
	}

	// Bind the GROUP BY expressions
	var groupBy []BoundExpr
	for _, expr := range s.GroupBy {
		boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		groupBy = append(groupBy, boundExpr)
	}

	return &BoundPivotStmt{
		Source:     sourceRef,
		ForColumn:  forColumn,
		InValues:   inValues,
		Aggregates: boundAggregates,
		GroupBy:    groupBy,
		Alias:      s.Alias,
	}, nil
}

// bindUnpivot binds an UNPIVOT statement.
// UNPIVOT transforms columns into rows (the inverse of PIVOT).
func (b *Binder) bindUnpivot(s *parser.UnpivotStmt) (*BoundUnpivotStmt, error) {
	// Push new scope for binding
	oldScope := b.scope
	b.scope = newBindScope(oldScope)
	defer func() { b.scope = oldScope }()

	// Bind the source table reference
	sourceRef, err := b.bindTableRef(s.Source)
	if err != nil {
		return nil, err
	}

	// Validate that the columns to unpivot exist in the source
	for _, colName := range s.Using {
		found := false
		for _, col := range sourceRef.Columns {
			if col.Column == colName {
				found = true
				break
			}
		}
		if !found {
			return nil, b.errorf("column %s not found in UNPIVOT source", colName)
		}
	}

	return &BoundUnpivotStmt{
		Source:         sourceRef,
		ValueColumn:    s.Into,
		NameColumn:     s.For,
		UnpivotColumns: s.Using,
		Alias:          s.Alias,
	}, nil
}

// bindPivotTableRef binds a PIVOT table reference (PIVOT used in FROM clause).
// This creates a BoundTableRef with PivotStmt information that the planner
// will convert to a LogicalPivot operation.
func (b *Binder) bindPivotTableRef(ref parser.TableRef) (*BoundTableRef, error) {
	pivotStmt := ref.PivotRef

	// First bind the source table
	sourceRef, err := b.bindTableRef(pivotStmt.Source)
	if err != nil {
		return nil, err
	}

	// Remove the source table from scope after binding (PIVOT becomes the new table)
	sourceAlias := sourceRef.Alias
	if sourceAlias == "" {
		sourceAlias = sourceRef.TableName
	}

	// Bind the FOR column
	var forColumn *BoundColumnRef
	if pivotStmt.ForColumn != "" {
		colRef := &parser.ColumnRef{Column: pivotStmt.ForColumn}
		boundExpr, err := b.bindExpr(colRef, dukdb.TYPE_ANY)
		if err != nil {
			return nil, b.errorf("PIVOT FOR column not found: %s", pivotStmt.ForColumn)
		}
		var ok bool
		forColumn, ok = boundExpr.(*BoundColumnRef)
		if !ok {
			return nil, b.errorf("PIVOT FOR clause must reference a column")
		}
	}

	// Bind the IN values
	var inValues []any
	for _, expr := range pivotStmt.PivotOn {
		val, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		if lit, ok := val.(*BoundLiteral); ok {
			inValues = append(inValues, lit.Value)
		} else {
			return nil, b.errorf("PIVOT IN values must be literals")
		}
	}

	// Bind the aggregates
	var boundAggregates []*BoundPivotAggregate
	for _, agg := range pivotStmt.Using {
		boundExpr, err := b.bindExpr(agg.Expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		boundAggregates = append(boundAggregates, &BoundPivotAggregate{
			Function: agg.Function,
			Expr:     boundExpr,
			Alias:    agg.Alias,
		})
	}

	// Bind the GROUP BY expressions (or infer them from source columns)
	var groupBy []BoundExpr
	if len(pivotStmt.GroupBy) > 0 {
		for _, expr := range pivotStmt.GroupBy {
			boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			groupBy = append(groupBy, boundExpr)
		}
	} else {
		// Infer GROUP BY from source columns that are not:
		// - The FOR column
		// - The aggregated column
		for _, col := range sourceRef.Columns {
			// Skip the FOR column
			if forColumn != nil && col.Column == forColumn.Column {
				continue
			}
			// Skip columns used in aggregates
			isAggCol := false
			for _, agg := range boundAggregates {
				if colRef, ok := agg.Expr.(*BoundColumnRef); ok {
					if col.Column == colRef.Column {
						isAggCol = true
						break
					}
				}
			}
			if !isAggCol {
				groupBy = append(groupBy, &BoundColumnRef{
					Table:     col.Table,
					Column:    col.Column,
					ColumnIdx: col.ColumnIdx,
					ColType:   col.Type,
				})
			}
		}
	}

	// Create the bound PIVOT statement
	boundPivot := &BoundPivotStmt{
		Source:     sourceRef,
		ForColumn:  forColumn,
		InValues:   inValues,
		Aggregates: boundAggregates,
		GroupBy:    groupBy,
		Alias:      pivotStmt.Alias,
	}

	// Determine the alias for the pivot result
	alias := pivotStmt.Alias
	if alias == "" {
		alias = "pivot"
	}
	if ref.Alias != "" {
		alias = ref.Alias
	}

	// Build output columns: GROUP BY columns + pivot value columns
	var columns []*BoundColumn
	colIdx := 0

	// Add GROUP BY columns
	for _, expr := range groupBy {
		if colRef, ok := expr.(*BoundColumnRef); ok {
			columns = append(columns, &BoundColumn{
				Table:      alias,
				Column:     colRef.Column,
				ColumnIdx:  colIdx,
				Type:       colRef.ColType,
				SourceType: "pivot",
			})
			colIdx++
		}
	}

	// Add pivot value columns (one per IN value per aggregate)
	for _, agg := range boundAggregates {
		for _, val := range inValues {
			colName := formatPivotColumnName(agg.Alias, val)
			columns = append(columns, &BoundColumn{
				Table:      alias,
				Column:     colName,
				ColumnIdx:  colIdx,
				Type:       agg.Expr.ResultType(),
				SourceType: "pivot",
			})
			colIdx++
		}
	}

	boundRef := &BoundTableRef{
		TableName: "pivot",
		Alias:     alias,
		Columns:   columns,
		PivotStmt: boundPivot,
	}

	// Remove the source table from scope (PIVOT replaces it)
	delete(b.scope.tables, sourceAlias)
	delete(b.scope.aliases, sourceAlias)

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = alias

	return boundRef, nil
}

// bindUnpivotTableRef binds an UNPIVOT table reference (UNPIVOT used in FROM clause).
func (b *Binder) bindUnpivotTableRef(ref parser.TableRef) (*BoundTableRef, error) {
	unpivotStmt := ref.UnpivotRef

	// First bind the source table
	sourceRef, err := b.bindTableRef(unpivotStmt.Source)
	if err != nil {
		return nil, err
	}

	// Capture source alias before we remove it from scope
	sourceAlias := sourceRef.Alias
	if sourceAlias == "" {
		sourceAlias = sourceRef.TableName
	}

	// Validate that the columns to unpivot exist in the source
	for _, colName := range unpivotStmt.Using {
		found := false
		for _, col := range sourceRef.Columns {
			if col.Column == colName {
				found = true
				break
			}
		}
		if !found {
			return nil, b.errorf("column %s not found in UNPIVOT source", colName)
		}
	}

	// Create the bound UNPIVOT statement
	boundUnpivot := &BoundUnpivotStmt{
		Source:         sourceRef,
		ValueColumn:    unpivotStmt.Into,
		NameColumn:     unpivotStmt.For,
		UnpivotColumns: unpivotStmt.Using,
		Alias:          unpivotStmt.Alias,
	}

	// Determine the alias
	alias := unpivotStmt.Alias
	if alias == "" {
		alias = "unpivot"
	}
	if ref.Alias != "" {
		alias = ref.Alias
	}

	// Build output columns: non-unpivoted columns + name column + value column
	var columns []*BoundColumn
	colIdx := 0

	// Add non-unpivoted columns
	for _, col := range sourceRef.Columns {
		isUnpivot := false
		for _, unpivotCol := range unpivotStmt.Using {
			if col.Column == unpivotCol {
				isUnpivot = true
				break
			}
		}
		if !isUnpivot {
			columns = append(columns, &BoundColumn{
				Table:      alias,
				Column:     col.Column,
				ColumnIdx:  colIdx,
				Type:       col.Type,
				SourceType: "unpivot",
			})
			colIdx++
		}
	}

	// Add name column (contains original column names)
	columns = append(columns, &BoundColumn{
		Table:      alias,
		Column:     unpivotStmt.For,
		ColumnIdx:  colIdx,
		Type:       dukdb.TYPE_VARCHAR,
		SourceType: "unpivot",
	})
	colIdx++

	// Add value column (contains unpivoted values)
	// Determine the type from the first unpivot column
	var valueType = dukdb.TYPE_ANY
	for _, col := range sourceRef.Columns {
		if slices.Contains(unpivotStmt.Using, col.Column) {
			valueType = col.Type
		}
		if valueType != dukdb.TYPE_ANY {
			break
		}
	}
	columns = append(columns, &BoundColumn{
		Table:      alias,
		Column:     unpivotStmt.Into,
		ColumnIdx:  colIdx,
		Type:       valueType,
		SourceType: "unpivot",
	})

	boundRef := &BoundTableRef{
		TableName:   "unpivot",
		Alias:       alias,
		Columns:     columns,
		UnpivotStmt: boundUnpivot,
	}

	// Remove the source table from scope (UNPIVOT replaces it)
	delete(b.scope.tables, sourceAlias)
	delete(b.scope.aliases, sourceAlias)

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = alias

	return boundRef, nil
}

// formatPivotColumnName creates a column name for a pivot result column.
func formatPivotColumnName(alias string, pivotValue any) string {
	var valStr string
	switch v := pivotValue.(type) {
	case string:
		valStr = v
	case int64:
		valStr = fmt.Sprintf("%d", v)
	case float64:
		valStr = fmt.Sprintf("%v", v)
	case bool:
		if v {
			valStr = "true"
		} else {
			valStr = "false"
		}
	default:
		valStr = fmt.Sprintf("%v", v)
	}
	if alias != "" {
		return valStr + "_" + alias
	}
	return valStr
}

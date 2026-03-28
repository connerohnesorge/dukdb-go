package binder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	fileio "github.com/dukdb/dukdb-go/internal/io"
	"github.com/dukdb/dukdb-go/internal/io/arrow"
	"github.com/dukdb/dukdb-go/internal/io/csv"
	"github.com/dukdb/dukdb-go/internal/io/filesystem"
	"github.com/dukdb/dukdb-go/internal/io/iceberg"
	"github.com/dukdb/dukdb-go/internal/io/json"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
	"github.com/dukdb/dukdb-go/internal/io/xlsx"
	"github.com/dukdb/dukdb-go/internal/metadata"
	"github.com/dukdb/dukdb-go/internal/parser"
)

func (b *Binder) bindSelect(
	s *parser.SelectStmt,
) (*BoundSelectStmt, error) {
	bound := &BoundSelectStmt{
		Distinct:        s.Distinct,
		RecursionOption: s.Options,
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

	// Bind CTEs (WITH clause) first, before any other references
	if len(s.CTEs) > 0 {
		boundCTEs, err := b.bindCTEs(s.CTEs)
		if err != nil {
			return nil, err
		}
		bound.CTEs = boundCTEs
	}

	// Collect named window definitions
	if len(s.Windows) > 0 {
		b.windowDefs = make(map[string]*parser.WindowDef, len(s.Windows))
		for i := range s.Windows {
			name := strings.ToUpper(s.Windows[i].Name)
			if _, exists := b.windowDefs[name]; exists {
				return nil, b.errorf("duplicate window name: %s", s.Windows[i].Name)
			}
			b.windowDefs[name] = &s.Windows[i]
		}
		// Resolve transitive references and detect cycles
		for name := range b.windowDefs {
			if _, err := b.resolveWindowDef(name, nil); err != nil {
				return nil, err
			}
		}
	}

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

	// Bind DISTINCT ON expressions
	for _, expr := range s.DistinctOn {
		boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		bound.DistinctOn = append(bound.DistinctOn, boundExpr)
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
					// Check if this column has a replacement
					if boundStar.Replacements != nil {
						if replExpr, ok := boundStar.Replacements[strings.ToUpper(c.Column)]; ok {
							bound.Columns = append(bound.Columns, &BoundSelectColumn{
								Expr:  replExpr,
								Alias: c.Column,
							})
							continue
						}
					}
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
			// Handle COLUMNS() which returns BoundStarExpr
			if starResult, ok := expr.(*BoundStarExpr); ok {
				for _, c := range starResult.Columns {
					bound.Columns = append(bound.Columns, &BoundSelectColumn{
						Expr: &BoundColumnRef{
							Table:     c.Table,
							Column:    c.Column,
							ColumnIdx: c.ColumnIdx,
							ColType:   c.Type,
						},
						Alias: c.Column,
					})
				}
				continue
			}
			alias := col.Alias
			if alias == "" {
				// If no explicit alias, derive alias from expression type
				switch e := expr.(type) {
				case *BoundColumnRef:
					// Use the column name as the alias
					alias = e.Column
				case *BoundSequenceCall:
					// Use the function name (lowercase) as the alias
					alias = strings.ToLower(e.FunctionName)
				case *BoundFunctionCall:
					// Use the function name (lowercase) as the alias
					alias = strings.ToLower(e.Name)
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

	// Expand GROUP BY ALL
	if s.GroupByAll {
		for _, col := range s.Columns {
			if col.Star {
				continue // Skip * — it expands to all columns later
			}
			expr := col.Expr
			if expr == nil {
				continue
			}
			if !containsAggregate(expr) {
				s.GroupBy = append(s.GroupBy, expr)
			}
		}
	}

	// Bind GROUP BY - SELECT aliases are visible in GROUP BY (DuckDB/PostgreSQL semantics)
	for _, g := range s.GroupBy {
		expr, err := b.bindExprWithSelectAliases(g, dukdb.TYPE_ANY, bound.Columns)
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

	// Bind QUALIFY clause (filter after window functions)
	// QUALIFY can reference SELECT column aliases (e.g., "QUALIFY rn <= 2" where rn is a window function alias).
	// FROM-clause columns take precedence over SELECT aliases to avoid ambiguity.
	if s.Qualify != nil {
		qualify, err := b.bindQualifyWithSelectAliases(s.Qualify, bound.Columns)
		if err != nil {
			return nil, err
		}
		bound.Qualify = qualify
	}

	// Bind ORDER BY - SELECT aliases are visible in ORDER BY (DuckDB/PostgreSQL semantics)
	for _, o := range s.OrderBy {
		expr, err := b.bindExprWithSelectAliases(o.Expr, dukdb.TYPE_ANY, bound.Columns)
		if err != nil {
			return nil, err
		}
		bound.OrderBy = append(
			bound.OrderBy,
			&BoundOrderBy{
				Expr:       expr,
				Desc:       o.Desc,
				NullsFirst: o.NullsFirst,
				Collation:  o.Collation,
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

	// Bind WITH TIES
	if s.FetchWithTies {
		if len(s.OrderBy) == 0 {
			return nil, fmt.Errorf("WITH TIES requires ORDER BY")
		}
		bound.WithTies = true
	}

	// Bind SAMPLE clause
	if s.Sample != nil {
		bound.Sample = &BoundSampleOptions{
			Method:     s.Sample.Method,
			Percentage: s.Sample.Percentage,
			Rows:       s.Sample.Rows,
			Seed:       s.Sample.Seed,
		}
	}

	// Bind set operations (UNION, INTERSECT, EXCEPT)
	if s.SetOp != parser.SetOpNone && s.Right != nil {
		bound.SetOp = s.SetOp
		right, err := b.bindSelect(s.Right)
		if err != nil {
			return nil, err
		}
		bound.Right = right

		// For BY NAME variants, skip column count validation
		// Column matching happens at execution time
		if s.SetOp != parser.SetOpUnionByName && s.SetOp != parser.SetOpUnionAllByName {
			// Validate that both sides have the same number of columns
			if len(bound.Columns) != len(right.Columns) {
				return nil, b.errorf(
					"each %s query must have the same number of columns",
					setOpName(s.SetOp),
				)
			}
		}
	}

	return bound, nil
}

// setOpName returns a human-readable name for a set operation type.
func setOpName(op parser.SetOpType) string {
	switch op {
	case parser.SetOpUnion:
		return "UNION"
	case parser.SetOpUnionAll:
		return "UNION ALL"
	case parser.SetOpIntersect:
		return "INTERSECT"
	case parser.SetOpIntersectAll:
		return "INTERSECT ALL"
	case parser.SetOpExcept:
		return "EXCEPT"
	case parser.SetOpExceptAll:
		return "EXCEPT ALL"
	case parser.SetOpUnionByName:
		return "UNION BY NAME"
	case parser.SetOpUnionAllByName:
		return "UNION ALL BY NAME"
	default:
		return "set operation"
	}
}

// detectTableFunction maps a file path to the appropriate table function name
// based on file extension. This is used by replacement scans to determine which
// reader function to invoke when a string literal appears in FROM position.
func detectTableFunction(path string) (string, error) {
	format := fileio.DetectFormatFromPath(path)
	switch format {
	case fileio.FormatCSV:
		return "read_csv_auto", nil
	case fileio.FormatParquet:
		return "read_parquet", nil
	case fileio.FormatJSON:
		return "read_json_auto", nil
	case fileio.FormatNDJSON:
		return "read_ndjson", nil
	case fileio.FormatXLSX:
		return "read_xlsx", nil
	default:
		// Check for Arrow IPC files
		if isArrowPath(path) {
			return "read_arrow_auto", nil
		}
		return "", fmt.Errorf(
			"unrecognized file format for '%s': cannot determine table function from extension",
			path,
		)
	}
}

// isArrowPath checks if a file path has an Arrow IPC file extension.
func isArrowPath(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".arrow" || ext == ".ipc" || ext == ".feather"
}

func (b *Binder) bindSummarize(s *parser.SummarizeStmt) (BoundStatement, error) {
	if s.Query != nil {
		// SUMMARIZE SELECT ...
		boundQuery, err := b.Bind(s.Query)
		if err != nil {
			return nil, err
		}
		selectStmt, ok := boundQuery.(*BoundSelectStmt)
		if !ok {
			return nil, b.errorf("SUMMARIZE query must be a SELECT statement")
		}
		return &BoundSummarizeStmt{
			Query: selectStmt,
		}, nil
	}

	// SUMMARIZE table_name
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}
	tableDef, ok := b.catalog.GetTableInSchema(schema, s.TableName)
	if !ok || tableDef == nil {
		return nil, b.errorf("Table with name %s does not exist!", s.TableName)
	}
	return &BoundSummarizeStmt{
		Schema:    schema,
		TableName: s.TableName,
		TableDef:  tableDef,
	}, nil
}

func (b *Binder) bindTableRef(
	ref parser.TableRef,
) (*BoundTableRef, error) {
	// Rewrite replacement scans (string literals in FROM position) to table function calls.
	// e.g., SELECT * FROM 'data.csv' -> SELECT * FROM read_csv_auto('data.csv')
	if ref.ReplacementScan != nil {
		funcName, err := detectTableFunction(ref.ReplacementScan.Path)
		if err != nil {
			return nil, err
		}
		// Create a synthetic TableFunctionRef so the existing table function
		// binding logic handles the rest.
		ref.TableFunction = &parser.TableFunctionRef{
			Name: funcName,
			Args: []parser.Expr{&parser.Literal{
				Value: ref.ReplacementScan.Path,
				Type:  dukdb.TYPE_VARCHAR,
			}},
		}
		ref.TableName = funcName
		// Fall through to existing table function binding below.
	}

	// Check for PIVOT table reference
	if ref.PivotRef != nil {
		return b.bindPivotTableRef(ref)
	}

	// Check for UNPIVOT table reference
	if ref.UnpivotRef != nil {
		return b.bindUnpivotTableRef(ref)
	}

	// Check for VALUES table reference
	if ref.ValuesRef != nil {
		return b.bindValuesRef(ref)
	}

	if ref.Subquery != nil {
		var subquery *BoundSelectStmt
		var err error

		if ref.Lateral {
			// LATERAL subquery: bind with access to outer scope tables
			subquery, err = b.bindLateralSubquery(ref.Subquery)
		} else {
			// Regular subquery: bind in isolated scope
			subquery, err = b.bindSelect(ref.Subquery)
		}
		if err != nil {
			return nil, err
		}

		alias := ref.Alias
		if alias == "" {
			alias = "subquery"
		}

		boundRef := &BoundTableRef{
			Alias:    alias,
			Lateral:  ref.Lateral, // Pass through LATERAL flag
			Subquery: subquery,    // Store bound subquery for planner
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

	// Check for table function
	if ref.TableFunction != nil {
		return b.bindTableFunction(ref)
	}

	// Table reference
	schema := ref.Schema
	if schema == "" {
		schema = "main"
	}

	// Intercept information_schema references
	if strings.EqualFold(schema, "information_schema") {
		return b.bindInformationSchemaTable(ref)
	}

	// Intercept pg_catalog references
	if strings.EqualFold(schema, "pg_catalog") {
		return b.bindPgCatalogTable(ref)
	}

	alias := ref.Alias
	if alias == "" {
		alias = ref.TableName
	}

	// Check for CTE reference first (CTEs take precedence over tables/views)
	// Only check if no schema is specified (CTEs don't have schemas)
	if ref.Schema == "" {
		if cteBinding := b.scope.getCTE(ref.TableName); cteBinding != nil {
			return b.bindCTERef(cteBinding, alias)
		}
	}

	// First check for virtual tables (they take precedence over regular tables)
	virtualTableName := ref.TableName
	if schema != "main" {
		virtualTableName = schema + "." + ref.TableName
	}
	if vtDef, ok := b.catalog.GetVirtualTableDef(virtualTableName); ok {
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

	// Check for views (takes precedence over regular tables)
	if viewDef, ok := b.catalog.GetViewInSchema(schema, ref.TableName); ok {
		return b.bindViewRef(viewDef, alias)
	}

	// Fall back to regular table lookup (also checks attached database catalogs)
	tableDef, ok := b.getTableInSchema(schema, ref.TableName)
	if !ok && schema != "temp" {
		// Check the "temp" schema as a fallback for temporary tables
		tableDef, ok = b.getTableInSchema("temp", ref.TableName)
		if ok {
			schema = "temp"
		}
	}

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

// bindValuesRef binds a VALUES clause table reference.
// It evaluates each row's expressions and generates column names.
func (b *Binder) bindValuesRef(ref parser.TableRef) (*BoundTableRef, error) {
	vc := ref.ValuesRef
	alias := ref.Alias
	if alias == "" {
		alias = "valueslist"
	}

	if len(vc.Rows) == 0 {
		return nil, b.errorf("VALUES clause must have at least one row")
	}

	numCols := len(vc.Rows[0])

	// Bind all expressions in all rows
	var boundRows [][]BoundExpr
	for _, row := range vc.Rows {
		var boundRow []BoundExpr
		for _, expr := range row {
			bound, err := b.bindExpr(expr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			boundRow = append(boundRow, bound)
		}
		boundRows = append(boundRows, boundRow)
	}

	// Determine column types from the first non-NULL value in each column
	colTypes := make([]dukdb.Type, numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		colTypes[colIdx] = dukdb.TYPE_ANY
		for _, row := range boundRows {
			rt := row[colIdx].ResultType()
			if rt != dukdb.TYPE_ANY && rt != dukdb.TYPE_INVALID {
				colTypes[colIdx] = rt
				break
			}
		}
		// Default to VARCHAR if all NULLs
		if colTypes[colIdx] == dukdb.TYPE_ANY {
			colTypes[colIdx] = dukdb.TYPE_VARCHAR
		}
	}

	// Generate column names
	colNames := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		if i < len(vc.ColumnAliases) {
			colNames[i] = vc.ColumnAliases[i]
		} else {
			colNames[i] = fmt.Sprintf("column%d", i+1)
		}
	}

	boundRef := &BoundTableRef{
		Alias:      alias,
		ValuesRows: boundRows,
	}

	// Create columns
	for i := 0; i < numCols; i++ {
		boundRef.Columns = append(boundRef.Columns, &BoundColumn{
			Table:      alias,
			Column:     colNames[i],
			ColumnIdx:  i,
			Type:       colTypes[i],
			SourceType: "values",
		})
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = alias

	return boundRef, nil
}

// bindTableFunction binds a table function call (e.g., read_csv, read_json).
func (b *Binder) bindTableFunction(ref parser.TableRef) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	alias := ref.Alias
	if alias == "" {
		alias = tableFunc.Name
	}

	// Check for secret system functions first
	funcNameLower := strings.ToLower(tableFunc.Name)
	if funcNameLower == "which_secret" || funcNameLower == "duckdb_secrets" {
		return b.bindSecretSystemFunction(ref)
	}
	if isSystemTableFunction(funcNameLower) {
		return b.bindSystemTableFunction(ref)
	}

	// Check for UNNEST table function
	if funcNameLower == "unnest" {
		return b.bindUnnestTableFunction(ref)
	}

	// Check for generate_series/range table functions
	if funcNameLower == "generate_series" || funcNameLower == "range" {
		return b.bindGenerateSeriesTableFunction(ref, funcNameLower)
	}

	// Check for FTS search table function
	if funcNameLower == "fts_search" || funcNameLower == "match_bm25" {
		return b.bindFTSSearchFunction(ref, funcNameLower)
	}

	// Check for json_each table function
	if funcNameLower == "json_each" {
		return b.bindJSONEachFunction(ref)
	}

	// Check for table macro expansion
	if b.catalog != nil {
		if macro, ok := b.catalog.GetMacro("", funcNameLower); ok && macro.Type == catalog.MacroTypeTable {
			return b.expandTableMacro(ref, macro)
		}
	}

	// Extract the file path(s) from the first positional argument
	if len(tableFunc.Args) == 0 {
		return nil, b.errorf("table function %s requires a file path argument", tableFunc.Name)
	}

	pathExpr := tableFunc.Args[0]
	var path string
	var paths []string

	switch pe := pathExpr.(type) {
	case *parser.Literal:
		// Single path: read_csv('file.csv')
		if pe.Type != dukdb.TYPE_VARCHAR {
			return nil, b.errorf("table function %s requires a string path as first argument", tableFunc.Name)
		}
		pathStr, ok := pe.Value.(string)
		if !ok {
			return nil, b.errorf("table function %s requires a string path as first argument", tableFunc.Name)
		}
		path = pathStr

	case *parser.ArrayExpr:
		// Array of paths: read_csv(['file1.csv', 'file2.csv'])
		paths = make([]string, 0, len(pe.Elements))
		for i, elem := range pe.Elements {
			lit, ok := elem.(*parser.Literal)
			if !ok || lit.Type != dukdb.TYPE_VARCHAR {
				return nil, b.errorf("table function %s array element %d must be a string path", tableFunc.Name, i)
			}
			pathStr, ok := lit.Value.(string)
			if !ok {
				return nil, b.errorf("table function %s array element %d must be a string path", tableFunc.Name, i)
			}
			paths = append(paths, pathStr)
		}
		// Use first path for schema inference
		if len(paths) > 0 {
			path = paths[0]
		}

	default:
		return nil, b.errorf("table function %s requires a string path or array of paths as first argument", tableFunc.Name)
	}

	// Build options map from named arguments
	options := make(map[string]any)
	for name, expr := range tableFunc.NamedArgs {
		lit, ok := expr.(*parser.Literal)
		if !ok {
			return nil, b.errorf("table function option %s must be a literal value", name)
		}
		options[name] = lit.Value
	}

	// Determine the schema at bind time by reading the file
	columns, err := b.inferTableFunctionSchema(tableFunc.Name, path, options)
	if err != nil {
		return nil, err
	}

	// Create bound table function reference
	boundFunc := &BoundTableFunctionRef{
		Name:    tableFunc.Name,
		Path:    path,
		Paths:   paths,
		Options: options,
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     tableFunc.Name,
		Alias:         alias,
		TableFunction: boundFunc,
		Lateral:       ref.Lateral, // Pass through LATERAL flag for table functions
	}

	// Create bound columns for the table function
	for i, col := range columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table_function",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = tableFunc.Name

	return boundRef, nil
}

// bindSecretSystemFunction binds secret system functions (which_secret, duckdb_secrets).
func (b *Binder) bindSecretSystemFunction(ref parser.TableRef) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	funcNameLower := strings.ToLower(tableFunc.Name)

	alias := ref.Alias
	if alias == "" {
		alias = tableFunc.Name
	}

	// Build options map from positional and named arguments
	options := make(map[string]any)

	// Add positional arguments as arg0, arg1, etc.
	for i, argExpr := range tableFunc.Args {
		lit, ok := argExpr.(*parser.Literal)
		if !ok {
			return nil, b.errorf(
				"table function %s argument %d must be a literal value",
				tableFunc.Name,
				i,
			)
		}
		options[fmt.Sprintf("arg%d", i)] = lit.Value
	}

	// Add named arguments
	for name, expr := range tableFunc.NamedArgs {
		lit, ok := expr.(*parser.Literal)
		if !ok {
			return nil, b.errorf("table function option %s must be a literal value", name)
		}
		options[name] = lit.Value
	}

	// Determine columns based on function
	var columns []*catalog.ColumnDef
	switch funcNameLower {
	case "which_secret":
		// Validate arguments: which_secret(path, type)
		if len(tableFunc.Args) < 2 {
			return nil, b.errorf("which_secret requires 2 arguments: path and type")
		}
		columns = []*catalog.ColumnDef{
			catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("persistent", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("storage", dukdb.TYPE_VARCHAR),
		}
	case "duckdb_secrets":
		// duckdb_secrets() takes no required arguments
		columns = []*catalog.ColumnDef{
			catalog.NewColumnDef("name", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("type", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("provider", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("persistent", dukdb.TYPE_BOOLEAN),
			catalog.NewColumnDef("storage", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("scope", dukdb.TYPE_VARCHAR),
			catalog.NewColumnDef("secret_string", dukdb.TYPE_VARCHAR),
		}
	default:
		return nil, b.errorf("unknown secret system function: %s", tableFunc.Name)
	}

	// Create bound table function reference
	boundFunc := &BoundTableFunctionRef{
		Name:    tableFunc.Name,
		Path:    "", // No path for system functions
		Options: options,
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     tableFunc.Name,
		Alias:         alias,
		TableFunction: boundFunc,
		Lateral:       ref.Lateral,
	}

	// Create bound columns for the table function
	for i, col := range columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table_function",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = tableFunc.Name

	return boundRef, nil
}

func isSystemTableFunction(name string) bool {
	switch name {
	case "duckdb_settings",
		"duckdb_tables",
		"duckdb_columns",
		"duckdb_views",
		"duckdb_functions",
		"duckdb_constraints",
		"duckdb_indexes",
		"duckdb_databases",
		"duckdb_sequences",
		"duckdb_dependencies",
		"duckdb_optimizers",
		"duckdb_keywords",
		"duckdb_extensions",
		"duckdb_memory_usage",
		"duckdb_temp_directory",
		"duckdb_schemas",
		"duckdb_types":
		return true
	default:
		return false
	}
}

func (b *Binder) bindSystemTableFunction(ref parser.TableRef) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	funcNameLower := strings.ToLower(tableFunc.Name)

	alias := ref.Alias
	if alias == "" {
		alias = tableFunc.Name
	}

	if len(tableFunc.Args) > 0 || len(tableFunc.NamedArgs) > 0 {
		return nil, b.errorf("table function %s does not accept arguments", tableFunc.Name)
	}

	var columns []*catalog.ColumnDef
	switch funcNameLower {
	case "duckdb_settings":
		columns = metadata.DuckDBSettingsColumns()
	case "duckdb_tables":
		columns = metadata.DuckDBTablesColumns()
	case "duckdb_columns":
		columns = metadata.DuckDBColumnsColumns()
	case "duckdb_views":
		columns = metadata.DuckDBViewsColumns()
	case "duckdb_functions":
		columns = metadata.DuckDBFunctionsColumns()
	case "duckdb_constraints":
		columns = metadata.DuckDBConstraintsColumns()
	case "duckdb_indexes":
		columns = metadata.DuckDBIndexesColumns()
	case "duckdb_databases":
		columns = metadata.DuckDBDatabasesColumns()
	case "duckdb_sequences":
		columns = metadata.DuckDBSequencesColumns()
	case "duckdb_dependencies":
		columns = metadata.DuckDBDependenciesColumns()
	case "duckdb_optimizers":
		columns = metadata.DuckDBOptimizersColumns()
	case "duckdb_keywords":
		columns = metadata.DuckDBKeywordsColumns()
	case "duckdb_extensions":
		columns = metadata.DuckDBExtensionsColumns()
	case "duckdb_memory_usage":
		columns = metadata.DuckDBMemoryUsageColumns()
	case "duckdb_temp_directory":
		columns = metadata.DuckDBTempDirectoryColumns()
	case "duckdb_schemas":
		columns = metadata.DuckDBSchemasColumns()
	case "duckdb_types":
		columns = metadata.DuckDBTypesColumns()
	default:
		return nil, b.errorf("unknown system table function: %s", tableFunc.Name)
	}

	boundFunc := &BoundTableFunctionRef{
		Name:    tableFunc.Name,
		Path:    "",
		Options: map[string]any{},
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     tableFunc.Name,
		Alias:         alias,
		TableFunction: boundFunc,
		Lateral:       ref.Lateral,
	}

	for i, col := range columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table_function",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = tableFunc.Name

	return boundRef, nil
}

// bindUnnestTableFunction binds an UNNEST table function call.
// UNNEST expands arrays/lists into rows, producing one row per element.
//
// Syntax examples:
//   - SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(x)
//   - SELECT * FROM UNNEST([1, 2, 3]) AS t(x)
//   - SELECT t.id, u.val FROM test_table t, UNNEST(t.arr_col) AS u(val)
func (b *Binder) bindUnnestTableFunction(ref parser.TableRef) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	alias := ref.Alias
	if alias == "" {
		alias = "unnest"
	}

	// UNNEST requires exactly one argument - the array expression
	if len(tableFunc.Args) == 0 {
		return nil, b.errorf("UNNEST requires an array argument")
	}
	if len(tableFunc.Args) > 1 {
		return nil, b.errorf("UNNEST accepts only one array argument")
	}

	// Bind the array expression - expect a LIST type
	arrayExpr, err := b.bindExpr(tableFunc.Args[0], dukdb.TYPE_LIST)
	if err != nil {
		return nil, b.errorf("UNNEST: failed to bind array expression: %v", err)
	}

	// Determine the element type from the array expression
	var elemType dukdb.Type
	switch arrayExpr.ResultType() {
	case dukdb.TYPE_LIST:
		// For LIST types, we need to get the child type
		// For now, default to VARCHAR if we can't determine it
		elemType = dukdb.TYPE_VARCHAR
		// Try to get the actual element type from BoundArrayExpr
		if boundArray, ok := arrayExpr.(*BoundArrayExpr); ok &&
			boundArray.ElemType != dukdb.TYPE_INVALID {
			elemType = boundArray.ElemType
		}
	case dukdb.TYPE_ARRAY:
		// Fixed-size array type
		elemType = dukdb.TYPE_VARCHAR
	default:
		// For other types (e.g., column references), default to VARCHAR
		// The actual type will be determined at execution time
		elemType = dukdb.TYPE_VARCHAR
	}

	// Determine output column name - defaults to "unnest"
	// Note: Column alias syntax like AS t(x) is not fully supported yet
	outputColName := "unnest"

	// Build options map with the bound array expression
	options := make(map[string]any)
	options["array_expr"] = arrayExpr
	options["output_column"] = outputColName

	// Create column definition for the output
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef(outputColName, elemType),
	}

	// Create bound table function reference
	boundFunc := &BoundTableFunctionRef{
		Name:    "unnest",
		Path:    "", // No path for UNNEST
		Options: options,
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     "unnest",
		Alias:         alias,
		TableFunction: boundFunc,
		Lateral:       ref.Lateral,
	}

	// Create bound columns for the table function
	for i, col := range columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table_function",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = "unnest"

	return boundRef, nil
}

// bindJSONEachFunction binds a json_each table function call.
// json_each(json_string) expands a JSON object or array into rows with key and value columns.
func (b *Binder) bindJSONEachFunction(ref parser.TableRef) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	alias := ref.Alias
	if alias == "" {
		alias = "json_each"
	}

	if len(tableFunc.Args) == 0 {
		return nil, b.errorf("json_each requires 1 argument")
	}

	// Bind the JSON argument
	boundArg, err := b.bindExpr(tableFunc.Args[0], dukdb.TYPE_VARCHAR)
	if err != nil {
		return nil, b.errorf("json_each: failed to bind argument: %v", err)
	}

	// Build options map with the bound expression
	options := make(map[string]any)
	options["json_expr"] = boundArg

	// Create column definitions for the output: key and value, both VARCHAR
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("key", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("value", dukdb.TYPE_VARCHAR),
	}

	// Create bound table function reference
	boundFunc := &BoundTableFunctionRef{
		Name:    "json_each",
		Path:    "",
		Options: options,
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     "json_each",
		Alias:         alias,
		TableFunction: boundFunc,
	}

	// Create bound columns for the table function
	for i, col := range columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table_function",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = "json_each"

	return boundRef, nil
}

// bindGenerateSeriesTableFunction binds a generate_series or range table function call.
func (b *Binder) bindGenerateSeriesTableFunction(ref parser.TableRef, funcName string) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	alias := ref.Alias
	if alias == "" {
		alias = funcName
	}

	// Validate argument count: 2 or 3
	if len(tableFunc.Args) < 2 || len(tableFunc.Args) > 3 {
		return nil, b.errorf("%s requires 2 or 3 arguments, got %d", funcName, len(tableFunc.Args))
	}

	// Bind start and stop expressions
	startExpr, err := b.bindExpr(tableFunc.Args[0], dukdb.TYPE_INVALID)
	if err != nil {
		return nil, b.errorf("%s: failed to bind start expression: %v", funcName, err)
	}
	stopExpr, err := b.bindExpr(tableFunc.Args[1], dukdb.TYPE_INVALID)
	if err != nil {
		return nil, b.errorf("%s: failed to bind stop expression: %v", funcName, err)
	}

	// Determine output type from start/stop types
	startType := startExpr.ResultType()
	stopType := stopExpr.ResultType()
	var outputType dukdb.Type

	switch {
	case isIntegerType(startType) && isIntegerType(stopType):
		if startType == dukdb.TYPE_BIGINT || stopType == dukdb.TYPE_BIGINT {
			outputType = dukdb.TYPE_BIGINT
		} else {
			outputType = dukdb.TYPE_INTEGER
		}
	case startType == dukdb.TYPE_DATE && stopType == dukdb.TYPE_DATE:
		outputType = dukdb.TYPE_DATE
	case startType == dukdb.TYPE_TIMESTAMP && stopType == dukdb.TYPE_TIMESTAMP:
		outputType = dukdb.TYPE_TIMESTAMP
	default:
		outputType = dukdb.TYPE_BIGINT
	}

	// Build options map with bound expressions
	options := make(map[string]any)
	options["start"] = startExpr
	options["stop"] = stopExpr

	// Bind optional step expression
	if len(tableFunc.Args) == 3 {
		stepExpr, err := b.bindExpr(tableFunc.Args[2], dukdb.TYPE_INVALID)
		if err != nil {
			return nil, b.errorf("%s: failed to bind step expression: %v", funcName, err)
		}
		options["step"] = stepExpr
	}

	// Determine output column name: use first column alias if provided, else funcName
	colName := funcName
	if len(tableFunc.ColumnAliases) > 0 {
		colName = tableFunc.ColumnAliases[0]
	}

	// Create column definition for the output
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef(colName, outputType),
	}

	// Create bound table function reference
	boundFunc := &BoundTableFunctionRef{
		Name:    funcName,
		Path:    "",
		Options: options,
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     funcName,
		Alias:         alias,
		TableFunction: boundFunc,
		Lateral:       ref.Lateral,
	}

	// Create bound columns for the table function
	for i, col := range columns {
		boundRef.Columns = append(
			boundRef.Columns,
			&BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table_function",
			},
		)
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = funcName

	return boundRef, nil
}

// inferTableFunctionSchema infers the schema for a table function by reading the file.
func (b *Binder) inferTableFunctionSchema(
	funcName string,
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	switch strings.ToLower(funcName) {
	case "read_csv":
		return b.inferCSVSchema(path, options)
	case "read_csv_auto":
		// read_csv_auto uses auto-detection for everything
		// Pass options for metadata columns (filename, file_row_number, etc.)
		return b.inferCSVSchema(path, options)
	case "read_json":
		return b.inferJSONSchema(path, options)
	case "read_json_auto":
		// read_json_auto uses auto-detection for format and schema
		// Pass options for metadata columns (filename, file_row_number, etc.)
		return b.inferJSONSchema(path, options)
	case "read_ndjson":
		// read_ndjson is an alias for read_json with NDJSON format
		if options == nil {
			options = make(map[string]any)
		}
		options["format"] = "newline_delimited"
		return b.inferJSONSchema(path, options)
	case "read_parquet":
		return b.inferParquetSchema(path, options)
	case "read_arrow":
		return b.inferArrowSchema(path, options)
	case "read_arrow_auto":
		// read_arrow_auto uses auto-detection for file vs stream format
		return b.inferArrowSchemaAuto(path, options)
	case "read_xlsx":
		return b.inferXLSXSchema(path, options)
	case "read_xlsx_auto":
		// read_xlsx_auto uses auto-detection for header and types
		return b.inferXLSXSchema(path, options)
	case "iceberg_scan":
		return b.inferIcebergScanSchema(path, options)
	case "iceberg_metadata":
		return b.inferIcebergMetadataSchema()
	case "iceberg_snapshots":
		return b.inferIcebergSnapshotsSchema()
	case "duckdb_iceberg_tables":
		return b.inferIcebergTablesSchema()
	case "duckdb_settings":
		return metadata.DuckDBSettingsColumns(), nil
	case "duckdb_tables":
		return metadata.DuckDBTablesColumns(), nil
	case "duckdb_columns":
		return metadata.DuckDBColumnsColumns(), nil
	case "duckdb_views":
		return metadata.DuckDBViewsColumns(), nil
	case "duckdb_functions":
		return metadata.DuckDBFunctionsColumns(), nil
	case "duckdb_constraints":
		return metadata.DuckDBConstraintsColumns(), nil
	case "duckdb_indexes":
		return metadata.DuckDBIndexesColumns(), nil
	case "duckdb_databases":
		return metadata.DuckDBDatabasesColumns(), nil
	case "duckdb_sequences":
		return metadata.DuckDBSequencesColumns(), nil
	case "duckdb_dependencies":
		return metadata.DuckDBDependenciesColumns(), nil
	case "duckdb_optimizers":
		return metadata.DuckDBOptimizersColumns(), nil
	case "duckdb_keywords":
		return metadata.DuckDBKeywordsColumns(), nil
	case "duckdb_extensions":
		return metadata.DuckDBExtensionsColumns(), nil
	case "duckdb_memory_usage":
		return metadata.DuckDBMemoryUsageColumns(), nil
	case "duckdb_temp_directory":
		return metadata.DuckDBTempDirectoryColumns(), nil
	default:
		// For unknown table functions, return no columns
		// The executor will handle the error
		return nil, nil
	}
}

// inferCSVSchema reads a CSV file and infers its schema.
// It supports glob patterns - if a glob is detected, it expands the pattern
// and uses the first matching file for schema inference.
func (b *Binder) inferCSVSchema(path string, options map[string]any) ([]*catalog.ColumnDef, error) {
	// Check for glob patterns and resolve to actual file path
	resolvedPath, err := b.resolveCSVPath(path, options)
	if err != nil {
		return nil, err
	}

	// If resolved path is empty, it means allow_empty was set and no files matched
	if resolvedPath == "" {
		return []*catalog.ColumnDef{}, nil
	}

	// Open the file
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, b.errorf("failed to open CSV file: %v", err)
	}
	defer func() { _ = file.Close() }()

	// Build reader options from options map
	opts := csv.DefaultReaderOptions()
	for name, val := range options {
		switch strings.ToLower(name) {
		case "delimiter", "delim", "sep":
			if s, ok := val.(string); ok && s != "" {
				opts.Delimiter = rune(s[0])
			}
		case "quote":
			if s, ok := val.(string); ok && s != "" {
				opts.Quote = rune(s[0])
			}
		case "header":
			if boolVal, ok := val.(bool); ok {
				opts.Header = boolVal
			}
		case "nullstr", "null":
			if s, ok := val.(string); ok {
				opts.NullStr = s
			}
		case "skip":
			switch v := val.(type) {
			case int64:
				opts.Skip = int(v)
			case int:
				opts.Skip = v
			}
		case "ignore_errors":
			if boolVal, ok := val.(bool); ok {
				opts.IgnoreErrors = boolVal
			}
		}
	}

	// Create the CSV reader
	reader, err := csv.NewReader(file, opts)
	if err != nil {
		return nil, b.errorf("failed to create CSV reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columnNames, err := reader.Schema()
	if err != nil {
		return nil, b.errorf("failed to get CSV schema: %v", err)
	}

	// Get the column types
	columnTypes, err := reader.Types()
	if err != nil {
		return nil, b.errorf("failed to get CSV types: %v", err)
	}

	// Check for metadata columns
	hasFilename := false
	hasFileRowNumber := false
	hasFileIndex := false
	hasHivePartitioning := false
	var hivePartitionCols []string

	for name, val := range options {
		switch strings.ToLower(name) {
		case "filename":
			if boolVal, ok := val.(bool); ok {
				hasFilename = boolVal
			}
		case "file_row_number":
			if boolVal, ok := val.(bool); ok {
				hasFileRowNumber = boolVal
			}
		case "file_index":
			if boolVal, ok := val.(bool); ok {
				hasFileIndex = boolVal
			}
		case "hive_partitioning":
			switch v := val.(type) {
			case bool:
				hasHivePartitioning = v
			case string:
				hasHivePartitioning = strings.ToLower(v) == "auto" || v == "true" || v == "1"
			}
		}
	}

	// If hive partitioning is enabled, extract partition column names from the path
	if hasHivePartitioning {
		hivePartitionCols = extractHivePartitionColumns(path)
	}

	// Create column definitions
	totalCols := len(columnNames)
	if hasFilename {
		totalCols++
	}
	if hasFileRowNumber {
		totalCols++
	}
	if hasFileIndex {
		totalCols++
	}
	totalCols += len(hivePartitionCols)

	columns := make([]*catalog.ColumnDef, 0, totalCols)

	// Add data columns
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns = append(columns, catalog.NewColumnDef(colName, colType))
	}

	// Add metadata columns
	if hasFilename {
		columns = append(columns, catalog.NewColumnDef("filename", dukdb.TYPE_VARCHAR))
	}
	if hasFileRowNumber {
		columns = append(columns, catalog.NewColumnDef("file_row_number", dukdb.TYPE_BIGINT))
	}
	if hasFileIndex {
		columns = append(columns, catalog.NewColumnDef("file_index", dukdb.TYPE_INTEGER))
	}

	// Add hive partition columns
	for _, colName := range hivePartitionCols {
		columns = append(columns, catalog.NewColumnDef(colName, dukdb.TYPE_VARCHAR))
	}

	return columns, nil
}

// resolveCSVPath resolves a path or glob pattern to a single file path for schema inference.
func (b *Binder) resolveCSVPath(path string, options map[string]any) (string, error) {
	// Check if it's a glob pattern
	if !isGlobPattern(path) {
		// Not a glob - return as is
		return path, nil
	}

	// Check file_glob_behavior option
	allowEmpty := false
	if options != nil {
		if behavior, ok := options["file_glob_behavior"]; ok {
			if s, ok := behavior.(string); ok && strings.ToUpper(s) == "ALLOW_EMPTY" {
				allowEmpty = true
			}
		}
	}

	// Expand the glob pattern
	fs := filesystem.NewLocalFileSystem("")
	paths, err := fs.Glob(path)
	if err != nil {
		return "", b.errorf("failed to expand glob pattern: %v", err)
	}

	// Sort paths alphabetically for consistent behavior
	sort.Strings(paths)

	if len(paths) == 0 {
		if allowEmpty {
			return "", nil
		}
		return "", b.errorf("no files match pattern: %s", path)
	}

	// Return the first matching file for schema inference
	return paths[0], nil
}

// isGlobPattern checks if a path contains glob pattern characters.
func isGlobPattern(path string) bool {
	return strings.ContainsAny(path, "*?[")
}

// extractHivePartitionColumns extracts partition column names from a path.
// Example: /data/year=2024/month=01/file.csv returns ["month", "year"] (sorted)
func extractHivePartitionColumns(path string) []string {
	var cols []string
	seen := make(map[string]bool)

	// Split path into components
	parts := strings.Split(filepath.ToSlash(path), "/")

	// Look for key=value patterns
	hivePattern := regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)=`)
	for _, part := range parts {
		if matches := hivePattern.FindStringSubmatch(part); matches != nil {
			key := matches[1]
			if !seen[key] {
				seen[key] = true
				cols = append(cols, key)
			}
		}
	}

	// Sort for consistent ordering
	sort.Strings(cols)

	return cols
}

// inferJSONSchema reads a JSON file and infers its schema.
// It supports glob patterns - if a glob is detected, it expands the pattern
// and uses the first matching file for schema inference.
func (b *Binder) inferJSONSchema(
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	// Check for glob patterns and resolve to actual file path
	resolvedPath, err := b.resolveJSONPath(path, options)
	if err != nil {
		return nil, err
	}

	// If resolved path is empty, it means allow_empty was set and no files matched
	if resolvedPath == "" {
		return []*catalog.ColumnDef{}, nil
	}

	// Open the file
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, b.errorf("failed to open JSON file: %v", err)
	}
	defer func() { _ = file.Close() }()

	// Build reader options from options map
	opts := json.DefaultReaderOptions()
	for name, val := range options {
		switch strings.ToLower(name) {
		case "format":
			if s, ok := val.(string); ok {
				switch strings.ToLower(s) {
				case "array":
					opts.Format = json.FormatArray
				case "newline_delimited", "ndjson":
					opts.Format = json.FormatNDJSON
				case "auto":
					opts.Format = json.FormatAuto
				}
			}
		case "maximum_depth", "max_depth":
			switch v := val.(type) {
			case int64:
				opts.MaxDepth = int(v)
			case int:
				opts.MaxDepth = v
			}
		case "sample_size":
			switch v := val.(type) {
			case int64:
				opts.SampleSize = int(v)
			case int:
				opts.SampleSize = v
			}
		case "ignore_errors":
			if boolVal, ok := val.(bool); ok {
				opts.IgnoreErrors = boolVal
			}
		}
	}

	// Create the JSON reader
	reader, err := json.NewReader(file, opts)
	if err != nil {
		return nil, b.errorf("failed to create JSON reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columnNames, err := reader.Schema()
	if err != nil {
		return nil, b.errorf("failed to get JSON schema: %v", err)
	}

	// Get the column types
	columnTypes, err := reader.Types()
	if err != nil {
		return nil, b.errorf("failed to get JSON types: %v", err)
	}

	// Check for metadata columns
	hasFilename := false
	hasFileRowNumber := false
	hasFileIndex := false
	hasHivePartitioning := false
	var hivePartitionCols []string

	for name, val := range options {
		switch strings.ToLower(name) {
		case "filename":
			if boolVal, ok := val.(bool); ok {
				hasFilename = boolVal
			}
		case "file_row_number":
			if boolVal, ok := val.(bool); ok {
				hasFileRowNumber = boolVal
			}
		case "file_index":
			if boolVal, ok := val.(bool); ok {
				hasFileIndex = boolVal
			}
		case "hive_partitioning":
			switch v := val.(type) {
			case bool:
				hasHivePartitioning = v
			case string:
				hasHivePartitioning = strings.ToLower(v) == "auto" || v == "true" || v == "1"
			}
		}
	}

	// If hive partitioning is enabled, extract partition column names from the path
	if hasHivePartitioning {
		hivePartitionCols = extractHivePartitionColumns(path)
	}

	// Create column definitions
	totalCols := len(columnNames)
	if hasFilename {
		totalCols++
	}
	if hasFileRowNumber {
		totalCols++
	}
	if hasFileIndex {
		totalCols++
	}
	totalCols += len(hivePartitionCols)

	columns := make([]*catalog.ColumnDef, 0, totalCols)

	// Add data columns
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns = append(columns, catalog.NewColumnDef(colName, colType))
	}

	// Add metadata columns
	if hasFilename {
		columns = append(columns, catalog.NewColumnDef("filename", dukdb.TYPE_VARCHAR))
	}
	if hasFileRowNumber {
		columns = append(columns, catalog.NewColumnDef("file_row_number", dukdb.TYPE_BIGINT))
	}
	if hasFileIndex {
		columns = append(columns, catalog.NewColumnDef("file_index", dukdb.TYPE_INTEGER))
	}

	// Add hive partition columns
	for _, colName := range hivePartitionCols {
		columns = append(columns, catalog.NewColumnDef(colName, dukdb.TYPE_VARCHAR))
	}

	return columns, nil
}

// resolveJSONPath resolves a path or glob pattern to a single file path for schema inference.
func (b *Binder) resolveJSONPath(path string, options map[string]any) (string, error) {
	// Check if it's a glob pattern
	if !isGlobPattern(path) {
		// Not a glob - return as is
		return path, nil
	}

	// Check file_glob_behavior option
	allowEmpty := false
	if options != nil {
		if behavior, ok := options["file_glob_behavior"]; ok {
			if s, ok := behavior.(string); ok && strings.ToUpper(s) == "ALLOW_EMPTY" {
				allowEmpty = true
			}
		}
	}

	// Expand the glob pattern
	fs := filesystem.NewLocalFileSystem("")
	paths, err := fs.Glob(path)
	if err != nil {
		return "", b.errorf("failed to expand glob pattern: %v", err)
	}

	// Sort paths alphabetically for consistent behavior
	sort.Strings(paths)

	if len(paths) == 0 {
		if allowEmpty {
			return "", nil
		}
		return "", b.errorf("no files match pattern: %s", path)
	}

	// Return the first matching file for schema inference
	return paths[0], nil
}

// inferParquetSchema reads a Parquet file and infers its schema.
func (b *Binder) inferParquetSchema(
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	// Check for glob patterns and resolve to actual file path
	resolvedPath, err := b.resolveParquetPath(path, options)
	if err != nil {
		return nil, err
	}

	// If resolved path is empty, it means allow_empty was set and no files matched
	if resolvedPath == "" {
		return []*catalog.ColumnDef{}, nil
	}

	// Build reader options from options map
	opts := parquet.DefaultReaderOptions()

	// Apply column projection if specified
	for name, val := range options {
		switch strings.ToLower(name) {
		case "columns":
			switch v := val.(type) {
			case []string:
				opts.Columns = v
			case []any:
				cols := make([]string, 0, len(v))
				for _, c := range v {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				opts.Columns = cols
			}
		}
	}

	// Create the Parquet reader from path
	reader, err := parquet.NewReaderFromPath(resolvedPath, opts)
	if err != nil {
		return nil, b.errorf("failed to create Parquet reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columnNames, err := reader.Schema()
	if err != nil {
		return nil, b.errorf("failed to get Parquet schema: %v", err)
	}

	// Get the column types
	columnTypes, err := reader.Types()
	if err != nil {
		return nil, b.errorf("failed to get Parquet types: %v", err)
	}

	// Check for metadata columns
	hasFilename := false
	hasFileRowNumber := false
	hasFileIndex := false
	hasHivePartitioning := false
	var hivePartitionCols []string

	for name, val := range options {
		switch strings.ToLower(name) {
		case "filename":
			if boolVal, ok := val.(bool); ok {
				hasFilename = boolVal
			}
		case "file_row_number":
			if boolVal, ok := val.(bool); ok {
				hasFileRowNumber = boolVal
			}
		case "file_index":
			if boolVal, ok := val.(bool); ok {
				hasFileIndex = boolVal
			}
		case "hive_partitioning":
			switch v := val.(type) {
			case bool:
				hasHivePartitioning = v
			case string:
				hasHivePartitioning = strings.ToLower(v) == "auto" || v == "true" || v == "1"
			}
		}
	}

	// If hive partitioning is enabled, extract partition column names from the resolved path
	if hasHivePartitioning {
		hivePartitionCols = extractHivePartitionColumns(resolvedPath)
	}

	// Create column definitions
	totalCols := len(columnNames)
	if hasFilename {
		totalCols++
	}
	if hasFileRowNumber {
		totalCols++
	}
	if hasFileIndex {
		totalCols++
	}
	totalCols += len(hivePartitionCols)

	columns := make([]*catalog.ColumnDef, 0, totalCols)

	// Add data columns
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns = append(columns, catalog.NewColumnDef(colName, colType))
	}

	// Add metadata columns
	if hasFilename {
		columns = append(columns, catalog.NewColumnDef("filename", dukdb.TYPE_VARCHAR))
	}
	if hasFileRowNumber {
		columns = append(columns, catalog.NewColumnDef("file_row_number", dukdb.TYPE_BIGINT))
	}
	if hasFileIndex {
		columns = append(columns, catalog.NewColumnDef("file_index", dukdb.TYPE_INTEGER))
	}

	// Add hive partition columns
	for _, colName := range hivePartitionCols {
		columns = append(columns, catalog.NewColumnDef(colName, dukdb.TYPE_VARCHAR))
	}

	return columns, nil
}

// resolveParquetPath resolves a path or glob pattern to a single file path for schema inference.
func (b *Binder) resolveParquetPath(path string, options map[string]any) (string, error) {
	// Check if it's a glob pattern
	if !isGlobPattern(path) {
		// Not a glob - return as is
		return path, nil
	}

	// Check file_glob_behavior option
	allowEmpty := false
	if options != nil {
		if behavior, ok := options["file_glob_behavior"]; ok {
			if s, ok := behavior.(string); ok && strings.ToUpper(s) == "ALLOW_EMPTY" {
				allowEmpty = true
			}
		}
	}

	// Expand the glob pattern
	fs := filesystem.NewLocalFileSystem("")
	paths, err := fs.Glob(path)
	if err != nil {
		return "", b.errorf("failed to expand glob pattern: %v", err)
	}

	// Sort paths alphabetically for consistent behavior
	sort.Strings(paths)

	if len(paths) == 0 {
		if allowEmpty {
			return "", nil
		}
		return "", b.errorf("no files match pattern: %s", path)
	}

	// Return the first matching file for schema inference
	return paths[0], nil
}

// inferXLSXSchema reads an XLSX file and infers its schema.
// It supports glob patterns - if a glob is detected, it expands the pattern
// and uses the first matching file for schema inference.
func (b *Binder) inferXLSXSchema(
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	// Check for glob patterns and resolve to actual file path
	resolvedPath, err := b.resolveXLSXPath(path, options)
	if err != nil {
		return nil, err
	}

	// If resolved path is empty, it means allow_empty was set and no files matched
	if resolvedPath == "" {
		return []*catalog.ColumnDef{}, nil
	}

	// Build reader options from options map
	opts := xlsx.DefaultReaderOptions()
	for name, val := range options {
		switch strings.ToLower(name) {
		case "sheet":
			if s, ok := val.(string); ok {
				opts.Sheet = s
			}
		case "sheet_index":
			switch v := val.(type) {
			case int64:
				opts.SheetIndex = int(v)
			case int:
				opts.SheetIndex = v
			}
		case "range":
			if s, ok := val.(string); ok {
				opts.Range = s
			}
		case "header":
			if boolVal, ok := val.(bool); ok {
				opts.Header = boolVal
			}
		case "skip":
			switch v := val.(type) {
			case int64:
				opts.Skip = int(v)
			case int:
				opts.Skip = v
			}
		case "empty_as_null":
			if boolVal, ok := val.(bool); ok {
				opts.EmptyAsNull = boolVal
			}
		case "infer_types":
			if boolVal, ok := val.(bool); ok {
				opts.InferTypes = boolVal
			}
		}
	}

	// Open the file
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, b.errorf("failed to open XLSX file: %v", err)
	}
	defer func() { _ = file.Close() }()

	// Create the XLSX reader
	reader, err := xlsx.NewReader(file, opts)
	if err != nil {
		return nil, b.errorf("failed to create XLSX reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columnNames, err := reader.Schema()
	if err != nil {
		return nil, b.errorf("failed to get XLSX schema: %v", err)
	}

	// Get the column types
	columnTypes, err := reader.Types()
	if err != nil {
		return nil, b.errorf("failed to get XLSX types: %v", err)
	}

	// Check for metadata columns
	hasFilename := false
	hasFileRowNumber := false
	hasFileIndex := false

	for name, val := range options {
		switch strings.ToLower(name) {
		case "filename":
			if boolVal, ok := val.(bool); ok {
				hasFilename = boolVal
			}
		case "file_row_number":
			if boolVal, ok := val.(bool); ok {
				hasFileRowNumber = boolVal
			}
		case "file_index":
			if boolVal, ok := val.(bool); ok {
				hasFileIndex = boolVal
			}
		}
	}

	// Create column definitions
	totalCols := len(columnNames)
	if hasFilename {
		totalCols++
	}
	if hasFileRowNumber {
		totalCols++
	}
	if hasFileIndex {
		totalCols++
	}

	columns := make([]*catalog.ColumnDef, 0, totalCols)

	// Add data columns
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns = append(columns, catalog.NewColumnDef(colName, colType))
	}

	// Add metadata columns
	if hasFilename {
		columns = append(columns, catalog.NewColumnDef("filename", dukdb.TYPE_VARCHAR))
	}
	if hasFileRowNumber {
		columns = append(columns, catalog.NewColumnDef("file_row_number", dukdb.TYPE_BIGINT))
	}
	if hasFileIndex {
		columns = append(columns, catalog.NewColumnDef("file_index", dukdb.TYPE_INTEGER))
	}

	return columns, nil
}

// resolveXLSXPath resolves a path or glob pattern to a single file path for schema inference.
func (b *Binder) resolveXLSXPath(path string, options map[string]any) (string, error) {
	// Check if it's a glob pattern
	if !isGlobPattern(path) {
		// Not a glob - return as is
		return path, nil
	}

	// Check file_glob_behavior option
	allowEmpty := false
	if options != nil {
		if behavior, ok := options["file_glob_behavior"]; ok {
			if s, ok := behavior.(string); ok && strings.ToUpper(s) == "ALLOW_EMPTY" {
				allowEmpty = true
			}
		}
	}

	// Expand the glob pattern
	fs := filesystem.NewLocalFileSystem("")
	paths, err := fs.Glob(path)
	if err != nil {
		return "", b.errorf("failed to expand glob pattern: %v", err)
	}

	// Sort paths alphabetically for consistent behavior
	sort.Strings(paths)

	if len(paths) == 0 {
		if allowEmpty {
			return "", nil
		}
		return "", b.errorf("no files match pattern: %s", path)
	}

	// Return the first matching file for schema inference
	return paths[0], nil
}

// inferArrowSchema reads an Arrow IPC file and infers its schema.
// This function defaults to the file format (random access).
// It supports glob patterns - if a glob is detected, it expands the pattern
// and uses the first matching file for schema inference.
func (b *Binder) inferArrowSchema(
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	// Check for glob patterns and resolve to actual file path
	resolvedPath, err := b.resolveArrowPath(path, options)
	if err != nil {
		return nil, err
	}

	// If resolved path is empty, it means allow_empty was set and no files matched
	if resolvedPath == "" {
		return []*catalog.ColumnDef{}, nil
	}

	// Build reader options from options map
	opts := arrow.DefaultReaderOptions()

	// Apply column projection if specified
	for name, val := range options {
		switch strings.ToLower(name) {
		case "columns":
			switch v := val.(type) {
			case []string:
				opts.Columns = v
			case []any:
				cols := make([]string, 0, len(v))
				for _, c := range v {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				opts.Columns = cols
			}
		}
	}

	// Create the Arrow reader from path (file format)
	reader, err := arrow.NewReaderFromPath(resolvedPath, opts)
	if err != nil {
		return nil, b.errorf("failed to create Arrow reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columnNames, err := reader.Schema()
	if err != nil {
		return nil, b.errorf("failed to get Arrow schema: %v", err)
	}

	// Get the column types
	columnTypes, err := reader.Types()
	if err != nil {
		return nil, b.errorf("failed to get Arrow types: %v", err)
	}

	// Check for metadata columns
	hasFilename := false
	hasFileRowNumber := false
	hasFileIndex := false

	for name, val := range options {
		switch strings.ToLower(name) {
		case "filename":
			if boolVal, ok := val.(bool); ok {
				hasFilename = boolVal
			}
		case "file_row_number":
			if boolVal, ok := val.(bool); ok {
				hasFileRowNumber = boolVal
			}
		case "file_index":
			if boolVal, ok := val.(bool); ok {
				hasFileIndex = boolVal
			}
		}
	}

	// Create column definitions
	totalCols := len(columnNames)
	if hasFilename {
		totalCols++
	}
	if hasFileRowNumber {
		totalCols++
	}
	if hasFileIndex {
		totalCols++
	}

	columns := make([]*catalog.ColumnDef, 0, totalCols)

	// Add data columns
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns = append(columns, catalog.NewColumnDef(colName, colType))
	}

	// Add metadata columns
	if hasFilename {
		columns = append(columns, catalog.NewColumnDef("filename", dukdb.TYPE_VARCHAR))
	}
	if hasFileRowNumber {
		columns = append(columns, catalog.NewColumnDef("file_row_number", dukdb.TYPE_BIGINT))
	}
	if hasFileIndex {
		columns = append(columns, catalog.NewColumnDef("file_index", dukdb.TYPE_INTEGER))
	}

	return columns, nil
}

// resolveArrowPath resolves a path or glob pattern to a single file path for schema inference.
func (b *Binder) resolveArrowPath(path string, options map[string]any) (string, error) {
	// Check if it's a glob pattern
	if !isGlobPattern(path) {
		// Not a glob - return as is
		return path, nil
	}

	// Check file_glob_behavior option
	allowEmpty := false
	if options != nil {
		if behavior, ok := options["file_glob_behavior"]; ok {
			if s, ok := behavior.(string); ok && strings.ToUpper(s) == "ALLOW_EMPTY" {
				allowEmpty = true
			}
		}
	}

	// Expand the glob pattern
	fs := filesystem.NewLocalFileSystem("")
	paths, err := fs.Glob(path)
	if err != nil {
		return "", b.errorf("failed to expand glob pattern: %v", err)
	}

	// Sort paths alphabetically for consistent behavior
	sort.Strings(paths)

	if len(paths) == 0 {
		if allowEmpty {
			return "", nil
		}
		return "", b.errorf("no files match pattern: %s", path)
	}

	// Return the first matching file for schema inference
	return paths[0], nil
}

// inferArrowSchemaAuto reads an Arrow IPC file with auto-detection for format.
// It detects whether the file is in file format or stream format based on magic bytes.
// It supports glob patterns - if a glob is detected, it expands the pattern
// and uses the first matching file for schema inference.
func (b *Binder) inferArrowSchemaAuto(
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	// Check for glob patterns and resolve to actual file path
	resolvedPath, err := b.resolveArrowPath(path, options)
	if err != nil {
		return nil, err
	}

	// If resolved path is empty, it means allow_empty was set and no files matched
	if resolvedPath == "" {
		return []*catalog.ColumnDef{}, nil
	}

	// Build reader options from options map
	opts := arrow.DefaultReaderOptions()

	// Apply column projection if specified
	for name, val := range options {
		switch strings.ToLower(name) {
		case "columns":
			switch v := val.(type) {
			case []string:
				opts.Columns = v
			case []any:
				cols := make([]string, 0, len(v))
				for _, c := range v {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				opts.Columns = cols
			}
		}
	}

	// Detect format from file extension first
	format := arrow.DetectFormatFromPath(resolvedPath)

	var columnNames []string
	var columnTypes []dukdb.Type

	if format == arrow.FormatStream {
		// Use stream reader for stream format
		reader, err := arrow.NewStreamReaderFromPath(resolvedPath, opts)
		if err != nil {
			return nil, b.errorf("failed to create Arrow stream reader: %v", err)
		}
		defer func() { _ = reader.Close() }()

		columnNames, err = reader.Schema()
		if err != nil {
			return nil, b.errorf("failed to get Arrow schema: %v", err)
		}

		columnTypes, err = reader.Types()
		if err != nil {
			return nil, b.errorf("failed to get Arrow types: %v", err)
		}
	} else {
		// Default to file reader (handles .arrow, .feather, .ipc, and unknown extensions)
		reader, err := arrow.NewReaderFromPath(resolvedPath, opts)
		if err != nil {
			// If file format fails, try stream format as fallback
			streamReader, streamErr := arrow.NewStreamReaderFromPath(resolvedPath, opts)
			if streamErr != nil {
				return nil, b.errorf("failed to create Arrow reader: %v", err)
			}
			defer func() { _ = streamReader.Close() }()

			columnNames, err = streamReader.Schema()
			if err != nil {
				return nil, b.errorf("failed to get Arrow schema: %v", err)
			}

			columnTypes, err = streamReader.Types()
			if err != nil {
				return nil, b.errorf("failed to get Arrow types: %v", err)
			}
		} else {
			defer func() { _ = reader.Close() }()

			columnNames, err = reader.Schema()
			if err != nil {
				return nil, b.errorf("failed to get Arrow schema: %v", err)
			}

			columnTypes, err = reader.Types()
			if err != nil {
				return nil, b.errorf("failed to get Arrow types: %v", err)
			}
		}
	}

	// Check for metadata columns
	hasFilename := false
	hasFileRowNumber := false
	hasFileIndex := false

	for name, val := range options {
		switch strings.ToLower(name) {
		case "filename":
			if boolVal, ok := val.(bool); ok {
				hasFilename = boolVal
			}
		case "file_row_number":
			if boolVal, ok := val.(bool); ok {
				hasFileRowNumber = boolVal
			}
		case "file_index":
			if boolVal, ok := val.(bool); ok {
				hasFileIndex = boolVal
			}
		}
	}

	// Create column definitions
	totalCols := len(columnNames)
	if hasFilename {
		totalCols++
	}
	if hasFileRowNumber {
		totalCols++
	}
	if hasFileIndex {
		totalCols++
	}

	columns := make([]*catalog.ColumnDef, 0, totalCols)

	// Add data columns
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns = append(columns, catalog.NewColumnDef(colName, colType))
	}

	// Add metadata columns
	if hasFilename {
		columns = append(columns, catalog.NewColumnDef("filename", dukdb.TYPE_VARCHAR))
	}
	if hasFileRowNumber {
		columns = append(columns, catalog.NewColumnDef("file_row_number", dukdb.TYPE_BIGINT))
	}
	if hasFileIndex {
		columns = append(columns, catalog.NewColumnDef("file_index", dukdb.TYPE_INTEGER))
	}

	return columns, nil
}

// inferIcebergScanSchema reads an Iceberg table and infers its schema.
func (b *Binder) inferIcebergScanSchema(
	path string,
	options map[string]any,
) ([]*catalog.ColumnDef, error) {
	// Build reader options from options map
	opts := iceberg.DefaultReaderOptions()

	// Apply options from the query
	for name, val := range options {
		switch strings.ToLower(name) {
		case "columns":
			switch v := val.(type) {
			case []string:
				opts.SelectedColumns = v
			case []any:
				cols := make([]string, 0, len(v))
				for _, c := range v {
					if s, ok := c.(string); ok {
						cols = append(cols, s)
					}
				}
				opts.SelectedColumns = cols
			}
		case "snapshot_id":
			switch v := val.(type) {
			case int64:
				opts.SnapshotID = &v
			case int:
				snapshotID := int64(v)
				opts.SnapshotID = &snapshotID
			}
		case "timestamp", "as_of_timestamp":
			switch v := val.(type) {
			case int64:
				opts.Timestamp = &v
			case int:
				ts := int64(v)
				opts.Timestamp = &ts
			}
		}
	}

	// Create the Iceberg reader
	reader, err := iceberg.NewReader(context.Background(), path, opts)
	if err != nil {
		return nil, b.errorf("failed to create Iceberg reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	// Get the schema (column names)
	columnNames, err := reader.Schema()
	if err != nil {
		return nil, b.errorf("failed to get Iceberg schema: %v", err)
	}

	// Get the column types
	columnTypes, err := reader.Types()
	if err != nil {
		return nil, b.errorf("failed to get Iceberg types: %v", err)
	}

	// Create column definitions
	columns := make([]*catalog.ColumnDef, len(columnNames))
	for i, colName := range columnNames {
		var colType dukdb.Type
		if i < len(columnTypes) {
			colType = columnTypes[i]
		} else {
			colType = dukdb.TYPE_VARCHAR
		}
		columns[i] = catalog.NewColumnDef(colName, colType)
	}

	return columns, nil
}

// inferIcebergMetadataSchema returns the schema for the iceberg_metadata table function.
// This function returns metadata about data files in the Iceberg table.
func (b *Binder) inferIcebergMetadataSchema() ([]*catalog.ColumnDef, error) {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("file_path", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("file_format", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("record_count", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("file_size_in_bytes", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("partition_data", dukdb.TYPE_VARCHAR), // JSON string
		catalog.NewColumnDef("value_counts", dukdb.TYPE_VARCHAR),   // JSON string
		catalog.NewColumnDef("null_value_counts", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("lower_bounds", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("upper_bounds", dukdb.TYPE_VARCHAR),
	}, nil
}

// inferIcebergSnapshotsSchema returns the schema for the iceberg_snapshots table function.
// This function returns information about snapshots in the Iceberg table.
func (b *Binder) inferIcebergSnapshotsSchema() ([]*catalog.ColumnDef, error) {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("snapshot_id", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("parent_snapshot_id", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("timestamp_ms", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("timestamp", dukdb.TYPE_TIMESTAMP),
		catalog.NewColumnDef("manifest_list", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("operation", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("summary", dukdb.TYPE_VARCHAR), // JSON string
		catalog.NewColumnDef("added_data_files", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("deleted_data_files", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("added_records", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("deleted_records", dukdb.TYPE_BIGINT),
	}, nil
}

// inferIcebergTablesSchema returns the schema for the duckdb_iceberg_tables table function.
// This function discovers and returns information about Iceberg tables in a directory.
func (b *Binder) inferIcebergTablesSchema() ([]*catalog.ColumnDef, error) {
	return []*catalog.ColumnDef{
		catalog.NewColumnDef("table_name", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("table_location", dukdb.TYPE_VARCHAR),
		catalog.NewColumnDef("current_snapshot_id", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("last_updated_ms", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("format_version", dukdb.TYPE_INTEGER),
		catalog.NewColumnDef("partition_columns", dukdb.TYPE_VARCHAR),
	}, nil
}

func (b *Binder) bindJoin(
	join parser.JoinClause,
) (*BoundJoin, error) {
	// For NATURAL joins, collect columns from left-side tables BEFORE binding right side
	var leftColumnNames []string
	if join.Type == parser.JoinTypeNatural || join.Type == parser.JoinTypeNaturalLeft ||
		join.Type == parser.JoinTypeNaturalRight || join.Type == parser.JoinTypeNaturalFull {
		for _, tableRef := range b.scope.tables {
			for _, col := range tableRef.Columns {
				leftColumnNames = append(leftColumnNames, col.Column)
			}
		}
	}

	table, err := b.bindTableRef(join.Table)
	if err != nil {
		return nil, err
	}

	boundJoin := &BoundJoin{
		Type:  join.Type,
		Table: table,
	}

	switch join.Type {
	case parser.JoinTypeNatural, parser.JoinTypeNaturalLeft,
		parser.JoinTypeNaturalRight, parser.JoinTypeNaturalFull:
		// Find common columns between left tables and the right (joined) table
		rightColNames := make(map[string]bool)
		for _, col := range table.Columns {
			rightColNames[col.Column] = true
		}
		var commonCols []string
		seen := make(map[string]bool)
		for _, name := range leftColumnNames {
			if rightColNames[name] && !seen[name] {
				commonCols = append(commonCols, name)
				seen[name] = true
			}
		}
		if len(commonCols) == 0 {
			return nil, b.errorf("NATURAL JOIN has no common columns")
		}

		// Build equi-join condition from common columns
		var cond BoundExpr
		for _, colName := range commonCols {
			// Find the left column ref
			var leftTable string
			for _, tableRef := range b.scope.tables {
				if tableRef == table {
					continue // skip the right table
				}
				for _, col := range tableRef.Columns {
					if col.Column == colName {
						leftTable = col.Table
						break
					}
				}
				if leftTable != "" {
					break
				}
			}
			rightTable := table.Alias
			if rightTable == "" {
				rightTable = table.TableName
			}

			eq := &BoundBinaryExpr{
				Left: &BoundColumnRef{
					Table:   leftTable,
					Column:  colName,
					ColType: dukdb.TYPE_ANY,
				},
				Op: parser.OpEq,
				Right: &BoundColumnRef{
					Table:   rightTable,
					Column:  colName,
					ColType: dukdb.TYPE_ANY,
				},
				ResType: dukdb.TYPE_BOOLEAN,
			}
			if cond == nil {
				cond = eq
			} else {
				cond = &BoundBinaryExpr{
					Left:    cond,
					Op:      parser.OpAnd,
					Right:   eq,
					ResType: dukdb.TYPE_BOOLEAN,
				}
			}
		}
		boundJoin.Condition = cond

		// Convert NATURAL join type to base join type
		switch join.Type {
		case parser.JoinTypeNatural:
			boundJoin.Type = parser.JoinTypeInner
		case parser.JoinTypeNaturalLeft:
			boundJoin.Type = parser.JoinTypeLeft
		case parser.JoinTypeNaturalRight:
			boundJoin.Type = parser.JoinTypeRight
		case parser.JoinTypeNaturalFull:
			boundJoin.Type = parser.JoinTypeFull
		}

	case parser.JoinTypePositional:
		// No condition for positional join - executor handles row matching by position
		boundJoin.Type = parser.JoinTypePositional

	case parser.JoinTypeAsOf, parser.JoinTypeAsOfLeft:
		// ASOF join has an ON condition that includes equality + inequality
		if join.Condition != nil {
			cond, err := b.bindExpr(
				join.Condition,
				dukdb.TYPE_BOOLEAN,
			)
			if err != nil {
				return nil, err
			}
			boundJoin.Condition = cond
		}

	default:
		// Standard joins: bind ON condition or USING clause
		if join.Condition != nil {
			cond, err := b.bindExpr(
				join.Condition,
				dukdb.TYPE_BOOLEAN,
			)
			if err != nil {
				return nil, err
			}
			boundJoin.Condition = cond
		} else if len(join.Using) > 0 {
			// Build equi-join from USING columns
			boundJoin.Using = join.Using
			var cond BoundExpr
			for _, colName := range join.Using {
				// Find the left table containing this column
				var leftTable string
				for _, tableRef := range b.scope.tables {
					if tableRef == table {
						continue
					}
					for _, col := range tableRef.Columns {
						if col.Column == colName {
							leftTable = col.Table
							break
						}
					}
					if leftTable != "" {
						break
					}
				}
				if leftTable == "" {
					return nil, b.errorf("column %q not found in left side of USING", colName)
				}
				rightTable := table.Alias
				if rightTable == "" {
					rightTable = table.TableName
				}

				eq := &BoundBinaryExpr{
					Left: &BoundColumnRef{
						Table:   leftTable,
						Column:  colName,
						ColType: dukdb.TYPE_ANY,
					},
					Op: parser.OpEq,
					Right: &BoundColumnRef{
						Table:   rightTable,
						Column:  colName,
						ColType: dukdb.TYPE_ANY,
					},
					ResType: dukdb.TYPE_BOOLEAN,
				}
				if cond == nil {
					cond = eq
				} else {
					cond = &BoundBinaryExpr{
						Left:    cond,
						Op:      parser.OpAnd,
						Right:   eq,
						ResType: dukdb.TYPE_BOOLEAN,
					}
				}
			}
			boundJoin.Condition = cond
		}
	}

	return boundJoin, nil
}

func (b *Binder) bindInsert(
	s *parser.InsertStmt,
) (*BoundInsertStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	tableDef, ok := b.getTableInSchema(schema, s.Table)
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
		// Insert into all columns — but exclude generated columns
		for i, col := range tableDef.Columns {
			if col.IsGenerated {
				continue
			}
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
			// Reject explicit insert into generated column
			if tableDef.Columns[idx].IsGenerated {
				return nil, b.errorf(
					"cannot insert a non-DEFAULT value into column %q: column is a generated column",
					colName,
				)
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

	// Bind ON CONFLICT clause if present
	if s.OnConflict != nil {
		oc := s.OnConflict
		boundOC := &BoundOnConflictClause{
			Action: oc.Action,
		}

		// Resolve conflict columns (or infer from PK)
		if len(oc.ConflictColumns) > 0 {
			for _, colName := range oc.ConflictColumns {
				idx, ok := tableDef.GetColumnIndex(colName)
				if !ok {
					return nil, b.errorf("column %q not found in table %q", colName, s.Table)
				}
				boundOC.ConflictColumnIndices = append(boundOC.ConflictColumnIndices, idx)
			}
		} else if tableDef.HasPrimaryKey() {
			boundOC.ConflictColumnIndices = tableDef.PrimaryKey
		} else {
			return nil, b.errorf("ON CONFLICT requires a conflict target or table with primary key")
		}

		// Bind SET clauses for DO UPDATE
		if oc.Action == parser.OnConflictDoUpdate {
			// If UpdateSet is empty, this came from INSERT OR REPLACE.
			// Auto-generate SET col = EXCLUDED.col for all non-PK columns.
			if len(oc.UpdateSet) == 0 {
				for i, col := range tableDef.Columns {
					// Skip primary key columns — they are the conflict target
					isPK := false
					for _, pkIdx := range boundOC.ConflictColumnIndices {
						if pkIdx == i {
							isPK = true
							break
						}
					}
					if isPK {
						continue
					}
					boundOC.UpdateSet = append(boundOC.UpdateSet, &BoundSetClause{
						ColumnIdx: i,
						Value: &BoundExcludedColumnRef{
							ColumnIndex: i,
							ColumnName:  col.Name,
							DataType:    col.Type,
						},
					})
				}
			} else {
				for _, sc := range oc.UpdateSet {
					colIdx, ok := tableDef.GetColumnIndex(sc.Column)
					if !ok {
						return nil, b.errorf("column %q not found in SET clause", sc.Column)
					}
					// Bind the value expression, resolving EXCLUDED.col references
					boundVal, err := b.bindExprWithExcluded(sc.Value, tableDef)
					if err != nil {
						return nil, err
					}
					boundOC.UpdateSet = append(boundOC.UpdateSet, &BoundSetClause{
						ColumnIdx: colIdx,
						Value:     boundVal,
					})
				}
				if oc.UpdateWhere != nil {
					boundWhere, err := b.bindExprWithExcluded(oc.UpdateWhere, tableDef)
					if err != nil {
						return nil, err
					}
					boundOC.UpdateWhere = boundWhere
				}
			}
		}

		bound.OnConflict = boundOC
	}

	// Bind RETURNING clause if present
	if len(s.Returning) > 0 {
		// Add table to scope for RETURNING clause binding
		alias := s.Table
		tableRef := &BoundTableRef{
			Schema:    schema,
			TableName: s.Table,
			Alias:     alias,
			TableDef:  tableDef,
		}
		for i, col := range tableDef.Columns {
			tableRef.Columns = append(tableRef.Columns, &BoundColumn{
				Table:      alias,
				Column:     col.Name,
				ColumnIdx:  i,
				Type:       col.Type,
				SourceType: "table",
			})
		}
		b.scope.tables[alias] = tableRef
		b.scope.aliases[alias] = s.Table

		returning, err := b.bindReturningClause(s.Returning, tableRef)
		if err != nil {
			return nil, err
		}
		bound.Returning = returning
	}

	return bound, nil
}

// bindExprWithExcluded binds an expression that may contain EXCLUDED.column references.
// EXCLUDED is a pseudo-table that refers to the row values being inserted.
func (b *Binder) bindExprWithExcluded(expr parser.Expr, tableDef *catalog.TableDef) (BoundExpr, error) {
	// Check if it's a column reference to EXCLUDED or the target table
	if colRef, ok := expr.(*parser.ColumnRef); ok {
		if strings.EqualFold(colRef.Table, "EXCLUDED") {
			idx, ok := tableDef.GetColumnIndex(colRef.Column)
			if !ok {
				return nil, b.errorf("column %q not found in EXCLUDED", colRef.Column)
			}
			return &BoundExcludedColumnRef{
				ColumnIndex: idx,
				ColumnName:  colRef.Column,
				DataType:    tableDef.Columns[idx].Type,
			}, nil
		}
		// Bare column reference or table-qualified reference to the target table:
		// resolve against the target table definition so existing row values are accessible.
		if colRef.Table == "" || strings.EqualFold(colRef.Table, tableDef.Name) {
			idx, ok := tableDef.GetColumnIndex(colRef.Column)
			if ok {
				return &BoundColumnRef{
					Table:     tableDef.Name,
					Column:    colRef.Column,
					ColumnIdx: idx,
					ColType:   tableDef.Columns[idx].Type,
				}, nil
			}
		}
	}
	// For binary expressions, recursively check both sides for EXCLUDED references
	if binExpr, ok := expr.(*parser.BinaryExpr); ok {
		left, err := b.bindExprWithExcluded(binExpr.Left, tableDef)
		if err != nil {
			return nil, err
		}
		right, err := b.bindExprWithExcluded(binExpr.Right, tableDef)
		if err != nil {
			return nil, err
		}
		return &BoundBinaryExpr{Left: left, Op: binExpr.Op, Right: right}, nil
	}
	// For function calls, check arguments for EXCLUDED references
	if fnCall, ok := expr.(*parser.FunctionCall); ok {
		var boundArgs []BoundExpr
		for _, arg := range fnCall.Args {
			boundArg, err := b.bindExprWithExcluded(arg, tableDef)
			if err != nil {
				return nil, err
			}
			boundArgs = append(boundArgs, boundArg)
		}
		return &BoundFunctionCall{
			Name:     fnCall.Name,
			Args:     boundArgs,
			Distinct: fnCall.Distinct,
			Star:     fnCall.Star,
		}, nil
	}
	// Fall back to regular binding for non-EXCLUDED expressions
	return b.bindExpr(expr, dukdb.TYPE_INVALID)
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

	// Bind FROM clause tables into scope (for UPDATE...FROM syntax)
	if s.From != nil {
		for _, table := range s.From.Tables {
			ref, err := b.bindTableRef(table)
			if err != nil {
				return nil, err
			}
			bound.From = append(bound.From, ref)
		}
		for _, join := range s.From.Joins {
			joinRef, err := b.bindTableRef(join.Table)
			if err != nil {
				return nil, err
			}
			bound.From = append(bound.From, joinRef)
		}
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

		// Reject direct SET on generated columns
		if tableDef.Columns[idx].IsGenerated {
			return nil, b.errorf(
				"column %q is a generated column",
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

	// Bind RETURNING clause if present
	if len(s.Returning) > 0 {
		returning, err := b.bindReturningClause(s.Returning, b.scope.tables[alias])
		if err != nil {
			return nil, err
		}
		bound.Returning = returning
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

	// Bind USING tables
	if len(s.Using) > 0 {
		for _, usingRef := range s.Using {
			usingSchema := usingRef.Schema
			if usingSchema == "" {
				usingSchema = "main"
			}
			usingTableDef, ok := b.catalog.GetTableInSchema(usingSchema, usingRef.TableName)
			if !ok {
				return nil, b.errorf("table not found: %s", usingRef.TableName)
			}

			usingAlias := usingRef.TableName
			if usingRef.Alias != "" {
				usingAlias = usingRef.Alias
			}

			boundRef := &BoundTableRef{
				Schema:    usingSchema,
				TableName: usingRef.TableName,
				Alias:     usingAlias,
				TableDef:  usingTableDef,
			}

			for i, col := range usingTableDef.Columns {
				boundRef.Columns = append(boundRef.Columns, &BoundColumn{
					Table:     usingAlias,
					Column:    col.Name,
					ColumnIdx: i,
					Type:      col.Type,
				})
			}

			b.scope.tables[usingAlias] = boundRef
			bound.Using = append(bound.Using, boundRef)
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

	// Bind RETURNING clause if present
	if len(s.Returning) > 0 {
		returning, err := b.bindReturningClause(s.Returning, b.scope.tables[alias])
		if err != nil {
			return nil, err
		}
		bound.Returning = returning
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
		OrReplace:   s.OrReplace,
		Temporary:   s.Temporary,
		PrimaryKey:  s.PrimaryKey,
	}

	// Convert column definitions
	for _, col := range s.Columns {
		colDef := catalog.NewColumnDef(
			col.Name,
			col.DataType,
		)
		if col.TypeInfo != nil {
			colDef.TypeInfo = col.TypeInfo
		}
		colDef.Nullable = !col.NotNull
		if col.Default != nil {
			colDef.HasDefault = true
			// Extract literal value from default expression if possible
			if lit, ok := col.Default.(*parser.Literal); ok {
				colDef.DefaultValue = lit.Value
			} else {
				// Non-literal default (e.g., NEXTVAL('seq')): store as expression text
				// to be evaluated at row-insert time.
				colDef.DefaultExprText = serializeExpr(col.Default)
			}
		}
		if col.IsGenerated {
			colDef.IsGenerated = true
			colDef.GeneratedExpr = serializeExpr(col.GeneratedExpr)
			colDef.GeneratedStored = (col.GeneratedKind == parser.GeneratedKindStored)
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

	// Convert table constraints to catalog constraint definitions
	for _, tc := range s.Constraints {
		switch tc.Type {
		case "UNIQUE":
			// Validate that all columns exist
			for _, colName := range tc.Columns {
				found := false
				for _, col := range s.Columns {
					if strings.EqualFold(col.Name, colName) {
						found = true
						break
					}
				}
				if !found {
					return nil, b.errorf(
						"column %q referenced in UNIQUE constraint does not exist",
						colName,
					)
				}
			}
			bound.Constraints = append(bound.Constraints, &catalog.UniqueConstraintDef{
				Name:    tc.Name,
				Columns: tc.Columns,
			})
		case "CHECK":
			exprStr := ""
			if tc.Expression != nil {
				exprStr = serializeExpr(tc.Expression)
			}
			bound.Constraints = append(bound.Constraints, &catalog.CheckConstraintDef{
				Name:       tc.Name,
				Expression: exprStr,
			})
		case "FOREIGN_KEY":
			// Convert parser FK action to catalog FK action
			var onDelete, onUpdate catalog.ForeignKeyAction
			switch tc.OnDelete {
			case parser.FKActionRestrict:
				onDelete = catalog.FKActionRestrict
			case parser.FKActionNoAction:
				onDelete = catalog.FKActionNoAction
			default:
				onDelete = catalog.FKActionNoAction
			}
			switch tc.OnUpdate {
			case parser.FKActionRestrict:
				onUpdate = catalog.FKActionRestrict
			case parser.FKActionNoAction:
				onUpdate = catalog.FKActionNoAction
			default:
				onUpdate = catalog.FKActionNoAction
			}
			bound.Constraints = append(bound.Constraints, &catalog.ForeignKeyConstraintDef{
				Name:       tc.Name,
				Columns:    tc.Columns,
				RefTable:   tc.RefTable,
				RefColumns: tc.RefColumns,
				OnDelete:   onDelete,
				OnUpdate:   onUpdate,
			})
		}
	}

	// Handle CREATE TABLE ... AS SELECT
	if s.AsSelect != nil {
		boundSelect, err := b.bindSelect(s.AsSelect)
		if err != nil {
			return nil, err
		}

		bound.AsSelect = boundSelect

		// If no columns were explicitly specified, derive them from the SELECT
		if len(bound.Columns) == 0 {
			for _, col := range boundSelect.Columns {
				colName := col.Alias
				if colName == "" {
					colName = fmt.Sprintf("%v", col.Expr)
				}

				colDef := catalog.NewColumnDef(colName, col.Expr.ResultType())
				colDef.Nullable = true
				bound.Columns = append(bound.Columns, colDef)
			}
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

func (b *Binder) bindTruncate(
	s *parser.TruncateStmt,
) (*BoundTruncateStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Verify the table exists
	_, exists := b.catalog.GetTableInSchema(schema, s.Table)
	if !exists {
		return nil, fmt.Errorf("table %q not found", s.Table)
	}

	return &BoundTruncateStmt{
		Schema: schema,
		Table:  s.Table,
	}, nil
}

func (b *Binder) bindCopy(
	s *parser.CopyStmt,
) (*BoundCopyStmt, error) {
	bound := &BoundCopyStmt{
		FilePath: s.FilePath,
		IsFrom:   s.IsFrom,
		Options:  s.Options,
	}

	// Handle COPY (SELECT...) TO syntax
	if s.Query != nil {
		// Bind the SELECT query
		query, err := b.bindSelect(s.Query)
		if err != nil {
			return nil, err
		}
		bound.Query = query
		return bound, nil
	}

	// Handle COPY table FROM/TO syntax
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}
	bound.Schema = schema
	bound.Table = s.TableName

	// For COPY FROM, we need the table to exist
	// For COPY TO, we need the table to exist
	tableDef, ok := b.catalog.GetTableInSchema(schema, s.TableName)
	if !ok {
		return nil, b.errorf("table not found: %s", s.TableName)
	}
	bound.TableDef = tableDef

	// Resolve column indices if specified
	if len(s.Columns) > 0 {
		for _, colName := range s.Columns {
			idx, ok := tableDef.GetColumnIndex(colName)
			if !ok {
				return nil, b.errorf("column not found: %s", colName)
			}
			bound.Columns = append(bound.Columns, idx)
		}
	}

	// Validate options
	if err := b.validateCopyOptions(bound); err != nil {
		return nil, err
	}

	return bound, nil
}

// bindViewRef binds a view reference by expanding its definition.
func (b *Binder) bindViewRef(viewDef *catalog.ViewDef, alias string) (*BoundTableRef, error) {
	// Parse the view's SELECT statement
	viewStmt, err := parser.Parse(viewDef.Query)
	if err != nil {
		return nil, b.errorf("failed to parse view definition: %v", err)
	}

	// Cast to SelectStmt (views are always SELECT)
	selectStmt, ok := viewStmt.(*parser.SelectStmt)
	if !ok {
		return nil, b.errorf("invalid view definition: expected SELECT statement")
	}

	// Bind the view's SELECT statement (recursive - handles nested views)
	boundSelect, err := b.bindSelect(selectStmt)
	if err != nil {
		return nil, b.errorf("failed to bind view query: %v", err)
	}

	// Create bound table reference for the view
	boundRef := &BoundTableRef{
		Schema:    viewDef.Schema,
		TableName: viewDef.Name,
		Alias:     alias,
		ViewDef:   viewDef,
		ViewQuery: boundSelect,
	}

	// Create columns from the expanded query
	for i, col := range boundSelect.Columns {
		colName := col.Alias
		if colName == "" {
			colName = fmt.Sprintf("col%d", i)
		}
		boundRef.Columns = append(boundRef.Columns, &BoundColumn{
			Table:      alias,
			Column:     colName,
			ColumnIdx:  i,
			Type:       col.Expr.ResultType(),
			SourceType: "view",
		})
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = viewDef.Name

	return boundRef, nil
}

// bindMerge binds a MERGE INTO statement.
func (b *Binder) bindMerge(s *parser.MergeStmt) (*BoundMergeStmt, error) {
	schema := s.Schema
	if schema == "" {
		schema = "main"
	}

	// Look up the target table
	targetTableDef, ok := b.catalog.GetTableInSchema(schema, s.Into.TableName)
	if !ok {
		return nil, b.errorf("table not found: %s", s.Into.TableName)
	}

	targetAlias := s.Into.Alias
	if targetAlias == "" {
		targetAlias = s.Into.TableName
	}

	// Push new scope for binding
	oldScope := b.scope
	b.scope = newBindScope(oldScope)
	defer func() { b.scope = oldScope }()

	// Add target table to scope
	targetRef := &BoundTableRef{
		Schema:    schema,
		TableName: s.Into.TableName,
		Alias:     targetAlias,
		TableDef:  targetTableDef,
	}
	for i, col := range targetTableDef.Columns {
		targetRef.Columns = append(targetRef.Columns, &BoundColumn{
			Table:      targetAlias,
			Column:     col.Name,
			ColumnIdx:  i,
			Type:       col.Type,
			SourceType: "table",
		})
	}
	b.scope.tables[targetAlias] = targetRef
	b.scope.aliases[targetAlias] = s.Into.TableName

	// Bind the source table reference
	sourceRef, err := b.bindTableRef(s.Using)
	if err != nil {
		return nil, err
	}

	// Bind the ON condition
	onCondition, err := b.bindExpr(s.On, dukdb.TYPE_BOOLEAN)
	if err != nil {
		return nil, err
	}

	bound := &BoundMergeStmt{
		Schema:         schema,
		TargetTable:    s.Into.TableName,
		TargetTableDef: targetTableDef,
		TargetAlias:    targetAlias,
		SourceRef:      sourceRef,
		OnCondition:    onCondition,
	}

	// Bind WHEN MATCHED actions
	for _, action := range s.WhenMatched {
		boundAction, err := b.bindMergeAction(action, targetTableDef, true)
		if err != nil {
			return nil, err
		}
		bound.WhenMatched = append(bound.WhenMatched, boundAction)
	}

	// Bind WHEN NOT MATCHED actions
	for _, action := range s.WhenNotMatched {
		boundAction, err := b.bindMergeAction(action, targetTableDef, false)
		if err != nil {
			return nil, err
		}
		bound.WhenNotMatched = append(bound.WhenNotMatched, boundAction)
	}

	// Bind WHEN NOT MATCHED BY SOURCE actions
	for _, action := range s.WhenNotMatchedBySource {
		boundAction, err := b.bindMergeAction(action, targetTableDef, true)
		if err != nil {
			return nil, err
		}
		bound.WhenNotMatchedBySource = append(bound.WhenNotMatchedBySource, boundAction)
	}

	// Bind RETURNING clause if present
	if len(s.Returning) > 0 {
		returning, err := b.bindReturningClause(s.Returning, targetRef)
		if err != nil {
			return nil, err
		}
		bound.Returning = returning
	}

	return bound, nil
}

// bindMergeAction binds a single MERGE action.
func (b *Binder) bindMergeAction(
	action parser.MergeAction,
	targetTableDef *catalog.TableDef,
	isMatched bool,
) (*BoundMergeAction, error) {
	bound := &BoundMergeAction{}

	// Map action type
	switch action.Type {
	case parser.MergeActionUpdate:
		bound.Type = BoundMergeActionUpdate
	case parser.MergeActionDelete:
		bound.Type = BoundMergeActionDelete
	case parser.MergeActionInsert:
		bound.Type = BoundMergeActionInsert
	case parser.MergeActionDoNothing:
		bound.Type = BoundMergeActionDoNothing
	}

	// Bind optional condition
	if action.Cond != nil {
		cond, err := b.bindExpr(action.Cond, dukdb.TYPE_BOOLEAN)
		if err != nil {
			return nil, err
		}
		bound.Cond = cond
	}

	// Bind UPDATE SET clauses
	if action.Type == parser.MergeActionUpdate {
		for _, set := range action.Update {
			idx, ok := targetTableDef.GetColumnIndex(set.Column)
			if !ok {
				return nil, b.errorf("column not found: %s", set.Column)
			}
			colType := targetTableDef.Columns[idx].Type
			val, err := b.bindExpr(set.Value, colType)
			if err != nil {
				return nil, err
			}
			bound.Update = append(bound.Update, &BoundSetClause{
				ColumnIdx: idx,
				Value:     val,
			})
		}
	}

	// Bind INSERT clauses
	if action.Type == parser.MergeActionInsert {
		for i, set := range action.Insert {
			var idx int
			var colType dukdb.Type
			if set.Column != "" {
				var ok bool
				idx, ok = targetTableDef.GetColumnIndex(set.Column)
				if !ok {
					return nil, b.errorf("column not found: %s", set.Column)
				}
				colType = targetTableDef.Columns[idx].Type
			} else {
				if i >= len(targetTableDef.Columns) {
					return nil, b.errorf("INSERT VALUES has more values than target table columns")
				}
				idx = i
				colType = targetTableDef.Columns[i].Type
			}
			val, err := b.bindExpr(set.Value, colType)
			if err != nil {
				return nil, err
			}
			bound.InsertColumns = append(bound.InsertColumns, idx)
			bound.InsertValues = append(bound.InsertValues, val)
		}
	}

	return bound, nil
}

// bindReturningClause binds the RETURNING clause for DML statements.
func (b *Binder) bindReturningClause(
	returning []parser.SelectColumn,
	tableRef *BoundTableRef,
) ([]*BoundSelectColumn, error) {
	var result []*BoundSelectColumn

	for _, col := range returning {
		if col.Star {
			// RETURNING * - expand to all columns
			if starExpr, ok := col.Expr.(*parser.StarExpr); ok {
				boundStar, err := b.bindStarExpr(starExpr)
				if err != nil {
					return nil, err
				}
				for _, c := range boundStar.Columns {
					// Check if this column has a replacement
					if boundStar.Replacements != nil {
						if replExpr, ok := boundStar.Replacements[strings.ToUpper(c.Column)]; ok {
							result = append(result, &BoundSelectColumn{
								Expr:  replExpr,
								Alias: c.Column,
							})
							continue
						}
					}
					result = append(result, &BoundSelectColumn{
						Expr: &BoundColumnRef{
							Table:     c.Table,
							Column:    c.Column,
							ColumnIdx: c.ColumnIdx,
							ColType:   c.Type,
						},
						Alias: c.Column,
					})
				}
			}
		} else {
			expr, err := b.bindExpr(col.Expr, dukdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			// Handle COLUMNS() which returns BoundStarExpr
			if starResult, ok := expr.(*BoundStarExpr); ok {
				for _, c := range starResult.Columns {
					result = append(result, &BoundSelectColumn{
						Expr: &BoundColumnRef{
							Table:     c.Table,
							Column:    c.Column,
							ColumnIdx: c.ColumnIdx,
							ColType:   c.Type,
						},
						Alias: c.Column,
					})
				}
				continue
			}
			alias := col.Alias
			if alias == "" {
				// Derive alias from expression
				if colRef, ok := expr.(*BoundColumnRef); ok {
					alias = colRef.Column
				}
			}
			result = append(result, &BoundSelectColumn{
				Expr:  expr,
				Alias: alias,
			})
		}
	}

	return result, nil
}

// validateCopyOptions validates COPY statement options.
func (b *Binder) validateCopyOptions(stmt *BoundCopyStmt) error {
	// Validate FORMAT option
	if format, ok := stmt.Options["FORMAT"]; ok {
		formatStr, isStr := format.(string)
		if !isStr {
			return b.errorf("FORMAT option must be a string")
		}
		switch strings.ToUpper(formatStr) {
		case "CSV", "PARQUET", "JSON", "NDJSON", "ARROW", "ARROW_STREAM", "ARROWS":
			// Valid formats
		default:
			return b.errorf(
				"unsupported FORMAT: %s (supported: CSV, PARQUET, JSON, NDJSON, ARROW, ARROW_STREAM)",
				formatStr,
			)
		}
	}

	// Validate CODEC option for Parquet/Arrow
	if codec, ok := stmt.Options["CODEC"]; ok {
		codecStr, isStr := codec.(string)
		if !isStr {
			return b.errorf("CODEC option must be a string")
		}
		switch strings.ToUpper(codecStr) {
		case "UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4", "LZ4_RAW", "BROTLI", "NONE":
			// Valid codecs
		default:
			return b.errorf(
				"unsupported CODEC: %s (supported: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW, BROTLI, NONE)",
				codecStr,
			)
		}
	}

	// Validate COMPRESSION option (for CSV, JSON, Arrow)
	if compression, ok := stmt.Options["COMPRESSION"]; ok {
		compStr, isStr := compression.(string)
		if !isStr {
			return b.errorf("COMPRESSION option must be a string")
		}
		switch strings.ToUpper(compStr) {
		case "NONE", "GZIP", "ZSTD", "SNAPPY", "LZ4":
			// Valid compressions
		default:
			return b.errorf(
				"unsupported COMPRESSION: %s (supported: NONE, GZIP, ZSTD, SNAPPY, LZ4)",
				compStr,
			)
		}
	}

	return nil
}

// bindCTEs binds the Common Table Expressions (WITH clause).
// For recursive CTEs, this creates a placeholder binding before binding the CTE query
// to allow self-reference.
func (b *Binder) bindCTEs(ctes []parser.CTE) ([]*BoundCTE, error) {
	var boundCTEs []*BoundCTE

	for _, cte := range ctes {
		boundCTE, err := b.bindCTE(cte)
		if err != nil {
			return nil, err
		}
		boundCTEs = append(boundCTEs, boundCTE)
	}

	return boundCTEs, nil
}

// bindCTE binds a single Common Table Expression.
func (b *Binder) bindCTE(cte parser.CTE) (*BoundCTE, error) {
	if cte.Recursive {
		return b.bindRecursiveCTE(cte)
	}
	return b.bindNonRecursiveCTE(cte)
}

// bindNonRecursiveCTE binds a non-recursive CTE.
func (b *Binder) bindNonRecursiveCTE(cte parser.CTE) (*BoundCTE, error) {
	if len(cte.UsingKey) > 0 {
		return nil, b.errorf(
			"CTE %s cannot use USING KEY without RECURSIVE",
			cte.Name,
		)
	}

	// Bind the CTE query
	boundQuery, err := b.bindSelect(cte.Query)
	if err != nil {
		return nil, err
	}

	// Determine column names and types
	var names []string
	var types []dukdb.Type

	// If column aliases are provided, use them
	if len(cte.Columns) > 0 {
		if len(cte.Columns) != len(boundQuery.Columns) {
			return nil, b.errorf("CTE %s has %d columns but %d column aliases specified",
				cte.Name, len(boundQuery.Columns), len(cte.Columns))
		}
		names = cte.Columns
	} else {
		// Derive names from the query
		for _, col := range boundQuery.Columns {
			colName := col.Alias
			if colName == "" {
				colName = fmt.Sprintf("col%d", len(names))
			}
			names = append(names, colName)
		}
	}

	// Get types from the query
	for _, col := range boundQuery.Columns {
		types = append(types, col.Expr.ResultType())
	}

	// Create bound columns for the CTE
	var columns []*BoundColumn
	for i, name := range names {
		columns = append(columns, &BoundColumn{
			Table:      cte.Name,
			Column:     name,
			ColumnIdx:  i,
			Type:       types[i],
			SourceType: "cte",
		})
	}

	// Create and register the CTE binding
	cteBinding := &CTEBinding{
		Name:            cte.Name,
		Columns:         columns,
		Types:           types,
		Names:           names,
		IsSelfReference: false,
		Query:           boundQuery,
		UsingKey:        nil,
		SetOp:           parser.SetOpNone,
		MaxRecursion:    -1,
	}
	b.scope.addCTE(cteBinding)

	return &BoundCTE{
		Name:         cte.Name,
		Columns:      cte.Columns,
		Query:        boundQuery,
		Recursive:    false,
		UsingKey:     nil,
		SetOp:        parser.SetOpNone,
		MaxRecursion: -1,
		ResultTypes:  types,
		ResultNames:  names,
	}, nil
}

// bindRecursiveCTE binds a recursive CTE (WITH RECURSIVE).
// Recursive CTEs require special handling:
// 1. Create a placeholder binding for the CTE name to allow self-reference
// 2. The CTE query must have a UNION ALL structure with base case and recursive case
// 3. Bind the query with the placeholder, then update with final types
func (b *Binder) bindRecursiveCTE(cte parser.CTE) (*BoundCTE, error) {
	// Validate that the recursive CTE has the expected set operation
	if len(cte.UsingKey) > 0 {
		if cte.Query.SetOp != parser.SetOpUnion {
			return nil, b.errorf(
				"recursive CTE %s using USING KEY must use UNION (not UNION ALL)",
				cte.Name,
			)
		}
	} else if cte.Query.SetOp != parser.SetOpUnionAll {
		return nil, b.errorf(
			"recursive CTE %s must use UNION ALL between base case and recursive case",
			cte.Name,
		)
	}

	if cte.Query.Right == nil {
		return nil, b.errorf(
			"recursive CTE %s must have a UNION ALL with base and recursive parts",
			cte.Name,
		)
	}

	// First, bind the base case (left side of UNION ALL) without the CTE binding
	// The base case cannot reference the CTE itself
	baseCaseQuery, err := b.bindSelectWithoutSetOp(cte.Query)
	if err != nil {
		return nil, b.errorf("failed to bind base case of recursive CTE %s: %v", cte.Name, err)
	}

	// Determine column names from base case
	var names []string
	var types []dukdb.Type

	if len(cte.Columns) > 0 {
		if len(cte.Columns) != len(baseCaseQuery.Columns) {
			return nil, b.errorf("CTE %s has %d columns but %d column aliases specified",
				cte.Name, len(baseCaseQuery.Columns), len(cte.Columns))
		}
		names = cte.Columns
	} else {
		for _, col := range baseCaseQuery.Columns {
			colName := col.Alias
			if colName == "" {
				colName = fmt.Sprintf("col%d", len(names))
			}
			names = append(names, colName)
		}
	}

	for _, col := range baseCaseQuery.Columns {
		types = append(types, col.Expr.ResultType())
	}

	if len(cte.UsingKey) > 0 {
		nameSet := make(map[string]struct{}, len(names))
		for _, name := range names {
			nameSet[name] = struct{}{}
		}
		for _, key := range cte.UsingKey {
			if _, ok := nameSet[key]; !ok {
				return nil, b.errorf(
					"recursive CTE %s USING KEY column %s not found in CTE output",
					cte.Name,
					key,
				)
			}
		}
	}

	// Create bound columns for the CTE placeholder
	var columns []*BoundColumn
	for i, name := range names {
		columns = append(columns, &BoundColumn{
			Table:      cte.Name,
			Column:     name,
			ColumnIdx:  i,
			Type:       types[i],
			SourceType: "cte",
		})
	}

	// Create a placeholder CTE binding for self-reference in the recursive part
	placeholderBinding := &CTEBinding{
		Name:            cte.Name,
		Columns:         columns,
		Types:           types,
		Names:           names,
		IsSelfReference: true, // Mark as self-reference placeholder
		Query:           nil,  // Query will be set after full binding
		UsingKey:        cte.UsingKey,
		SetOp:           cte.Query.SetOp,
		MaxRecursion:    -1,
	}
	if cte.Query.Options != nil {
		placeholderBinding.MaxRecursion = cte.Query.Options.MaxRecursion
	}
	b.scope.addCTE(placeholderBinding)

	// Now bind the recursive case (right side of UNION ALL) with the CTE binding available
	recursiveCaseQuery, err := b.bindSelect(cte.Query.Right)
	if err != nil {
		return nil, b.errorf("failed to bind recursive case of CTE %s: %v", cte.Name, err)
	}

	// Validate that recursive case has same number of columns
	if len(recursiveCaseQuery.Columns) != len(baseCaseQuery.Columns) {
		return nil, b.errorf(
			"recursive CTE %s: base case has %d columns but recursive case has %d columns",
			cte.Name,
			len(baseCaseQuery.Columns),
			len(recursiveCaseQuery.Columns),
		)
	}

	// Update the placeholder binding with both the base and recursive queries
	placeholderBinding.IsSelfReference = false
	placeholderBinding.Query = baseCaseQuery
	placeholderBinding.RecursiveQuery = recursiveCaseQuery
	placeholderBinding.Recursive = true

	return &BoundCTE{
		Name:           cte.Name,
		Columns:        cte.Columns,
		Query:          baseCaseQuery,
		RecursiveQuery: recursiveCaseQuery,
		Recursive:      true,
		UsingKey:       cte.UsingKey,
		SetOp:          cte.Query.SetOp,
		MaxRecursion:   placeholderBinding.MaxRecursion,
		ResultTypes:    types,
		ResultNames:    names,
	}, nil
}

// bindSelectWithoutSetOp binds only the base SELECT part without processing set operations.
// This is used to bind the left side of a UNION ALL in recursive CTEs.
func (b *Binder) bindSelectWithoutSetOp(s *parser.SelectStmt) (*BoundSelectStmt, error) {
	// Create a copy of the select without the set operation to bind just the base case
	baseSelect := &parser.SelectStmt{
		Distinct:   s.Distinct,
		DistinctOn: s.DistinctOn,
		Columns:    s.Columns,
		From:       s.From,
		Where:      s.Where,
		GroupBy:    s.GroupBy,
		GroupByAll: s.GroupByAll,
		Having:     s.Having,
		Qualify:    s.Qualify,
		Windows:    s.Windows,
		OrderBy:    nil, // ORDER BY is applied to the final result, not base case
		Limit:      nil, // Same for LIMIT
		Offset:     nil, // Same for OFFSET
		Sample:     s.Sample,
		SetOp:      parser.SetOpNone, // No set operation for base case
		Right:      nil,
	}

	return b.bindSelect(baseSelect)
}

// bindCTERef binds a reference to a CTE.
func (b *Binder) bindCTERef(cteBinding *CTEBinding, alias string) (*BoundTableRef, error) {
	// Create a copy of the columns with the new alias
	var columns []*BoundColumn
	for i, col := range cteBinding.Columns {
		columns = append(columns, &BoundColumn{
			Table:      alias,
			Column:     col.Column,
			ColumnIdx:  i,
			Type:       col.Type,
			SourceType: "cte",
		})
	}

	// Capture IsSelfReference at bind time - this is true when we're binding
	// the recursive part of a recursive CTE and referencing the CTE itself
	isSelfRef := cteBinding.IsSelfReference

	boundRef := &BoundTableRef{
		TableName:    cteBinding.Name,
		Alias:        alias,
		CTERef:       cteBinding,
		Columns:      columns,
		IsCTESelfRef: isSelfRef,
	}

	b.scope.tables[alias] = boundRef
	b.scope.aliases[alias] = cteBinding.Name

	return boundRef, nil
}

// bindLateralSubquery binds a LATERAL subquery with access to outer scope tables.
// LATERAL allows the subquery to reference columns from tables that appear earlier
// in the FROM clause. This is achieved by creating a new scope that inherits the
// current scope's table bindings, allowing column references to resolve against
// both the subquery's own tables and the outer scope's preceding tables.
func (b *Binder) bindLateralSubquery(
	s *parser.SelectStmt,
) (*BoundSelectStmt, error) {
	bound := &BoundSelectStmt{
		Distinct: s.Distinct,
	}

	// Create a lateral scope that inherits outer tables.
	// Unlike regular subquery binding, LATERAL subqueries can see tables
	// from the enclosing FROM clause that were bound before this subquery.
	oldScope := b.scope
	lateralScope := newLateralScope(oldScope)
	b.scope = lateralScope
	defer func() {
		// Merge parameters from lateral scope to outer scope before restoring
		for pos, typ := range b.scope.params {
			oldScope.params[pos] = typ
		}
		// Also update paramCount
		oldScope.paramCount = b.scope.paramCount
		b.scope = oldScope
	}()

	// Bind CTEs (WITH clause) first, before any other references
	if len(s.CTEs) > 0 {
		boundCTEs, err := b.bindCTEs(s.CTEs)
		if err != nil {
			return nil, err
		}
		bound.CTEs = boundCTEs
	}

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

	// Bind DISTINCT ON expressions
	for _, expr := range s.DistinctOn {
		boundExpr, err := b.bindExpr(expr, dukdb.TYPE_ANY)
		if err != nil {
			return nil, err
		}
		bound.DistinctOn = append(bound.DistinctOn, boundExpr)
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
					// Check if this column has a replacement
					if boundStar.Replacements != nil {
						if replExpr, ok := boundStar.Replacements[strings.ToUpper(c.Column)]; ok {
							bound.Columns = append(bound.Columns, &BoundSelectColumn{
								Expr:  replExpr,
								Alias: c.Column,
							})
							continue
						}
					}
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
			// Handle COLUMNS() which returns BoundStarExpr
			if starResult, ok := expr.(*BoundStarExpr); ok {
				for _, c := range starResult.Columns {
					bound.Columns = append(bound.Columns, &BoundSelectColumn{
						Expr: &BoundColumnRef{
							Table:     c.Table,
							Column:    c.Column,
							ColumnIdx: c.ColumnIdx,
							ColType:   c.Type,
						},
						Alias: c.Column,
					})
				}
				continue
			}
			alias := col.Alias
			if alias == "" {
				// If no explicit alias, derive alias from expression type
				switch e := expr.(type) {
				case *BoundColumnRef:
					// Use the column name as the alias
					alias = e.Column
				case *BoundSequenceCall:
					// Use the function name (lowercase) as the alias
					alias = strings.ToLower(e.FunctionName)
				case *BoundFunctionCall:
					// Use the function name (lowercase) as the alias
					alias = strings.ToLower(e.Name)
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

	// Expand GROUP BY ALL
	if s.GroupByAll {
		for _, col := range s.Columns {
			if col.Star {
				continue // Skip * — it expands to all columns later
			}
			expr := col.Expr
			if expr == nil {
				continue
			}
			if !containsAggregate(expr) {
				s.GroupBy = append(s.GroupBy, expr)
			}
		}
	}

	// Bind GROUP BY - SELECT aliases are visible in GROUP BY (DuckDB/PostgreSQL semantics)
	for _, g := range s.GroupBy {
		expr, err := b.bindExprWithSelectAliases(g, dukdb.TYPE_ANY, bound.Columns)
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

	// Bind QUALIFY clause (filter after window functions)
	// QUALIFY can reference SELECT column aliases (e.g., "QUALIFY rn <= 2" where rn is a window function alias).
	// FROM-clause columns take precedence over SELECT aliases to avoid ambiguity.
	if s.Qualify != nil {
		qualify, err := b.bindQualifyWithSelectAliases(s.Qualify, bound.Columns)
		if err != nil {
			return nil, err
		}
		bound.Qualify = qualify
	}

	// Bind ORDER BY - SELECT aliases are visible in ORDER BY (DuckDB/PostgreSQL semantics)
	for _, o := range s.OrderBy {
		expr, err := b.bindExprWithSelectAliases(o.Expr, dukdb.TYPE_ANY, bound.Columns)
		if err != nil {
			return nil, err
		}
		bound.OrderBy = append(
			bound.OrderBy,
			&BoundOrderBy{
				Expr:       expr,
				Desc:       o.Desc,
				NullsFirst: o.NullsFirst,
				Collation:  o.Collation,
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

	// Bind WITH TIES
	if s.FetchWithTies {
		if len(s.OrderBy) == 0 {
			return nil, fmt.Errorf("WITH TIES requires ORDER BY")
		}
		bound.WithTies = true
	}

	// Bind SAMPLE clause
	if s.Sample != nil {
		bound.Sample = &BoundSampleOptions{
			Method:     s.Sample.Method,
			Percentage: s.Sample.Percentage,
			Rows:       s.Sample.Rows,
			Seed:       s.Sample.Seed,
		}
	}

	return bound, nil
}

// bindQualifyWithSelectAliases binds a QUALIFY expression with access to SELECT column aliases.
// QUALIFY can reference SELECT column aliases (e.g., "QUALIFY rn <= 2" where rn is a window function alias).
// This function temporarily adds SELECT column aliases to the scope before binding QUALIFY.
// Only aliases that do NOT shadow an existing FROM-clause column are added to avoid ambiguity
// (FROM columns take precedence, matching DuckDB/PostgreSQL semantics for QUALIFY).
func (b *Binder) bindQualifyWithSelectAliases(
	qualify parser.Expr,
	boundColumns []*BoundSelectColumn,
) (BoundExpr, error) {
	// Build the set of column names that already exist in the FROM scope so we can
	// skip aliases that would create an ambiguity.
	fromCols := make(map[string]bool)
	for tableName, tableRef := range b.scope.tables {
		if tableName == "__select_aliases__" {
			continue
		}
		for _, col := range tableRef.Columns {
			fromCols[strings.ToLower(col.Column)] = true
		}
	}

	// Add SELECT column aliases to scope temporarily, but only those that are not
	// already present as FROM-clause columns.  This allows "QUALIFY rn = 1" to work
	// (rn is a pure SELECT alias) while letting "QUALIFY dept = 'eng'" resolve to
	// the base-table column without triggering the ambiguity check.
	selectAliasRef := &BoundTableRef{
		Alias:   "__select_aliases__",
		Columns: make([]*BoundColumn, 0, len(boundColumns)),
	}
	for i, col := range boundColumns {
		if col.Alias != "" && !fromCols[strings.ToLower(col.Alias)] {
			selectAliasRef.Columns = append(selectAliasRef.Columns, &BoundColumn{
				Table:      "__select_aliases__",
				Column:     col.Alias,
				ColumnIdx:  i,
				Type:       col.Expr.ResultType(),
				SourceType: "select_alias",
			})
		}
	}
	b.scope.tables["__select_aliases__"] = selectAliasRef

	boundQualify, err := b.bindExpr(
		qualify,
		dukdb.TYPE_BOOLEAN,
	)

	// Remove the temporary table
	delete(b.scope.tables, "__select_aliases__")

	return boundQualify, err
}

// bindExprWithSelectAliases binds an expression with access to SELECT column aliases.
// This is used for ORDER BY and GROUP BY which can reference SELECT list aliases
// in DuckDB/PostgreSQL semantics. The resolution precedence is:
//  1. FROM clause columns (tried first via normal bindExpr)
//  2. SELECT aliases (tried as fallback when normal binding fails with "column not found")
func (b *Binder) bindExprWithSelectAliases(
	expr parser.Expr,
	expectedType dukdb.Type,
	boundColumns []*BoundSelectColumn,
) (BoundExpr, error) {
	// First try to bind normally (FROM clause columns take precedence)
	bound, err := b.bindExpr(expr, expectedType)
	if err == nil {
		return bound, nil
	}

	// Only fall through to alias resolution for "not found" errors.
	// Preserve type errors, ambiguity errors, etc.
	if !strings.Contains(strings.ToLower(err.Error()), "not found") {
		return nil, err
	}

	// If there was an error, check if it was a "column not found" error on a bare column ref.
	// In that case, try to resolve it as a SELECT alias.
	colRef, isColRef := expr.(*parser.ColumnRef)
	if !isColRef || colRef.Table != "" {
		// Not a bare column reference, so we can't try SELECT alias resolution
		return nil, err
	}

	// Build alias map from SELECT list
	for i, col := range boundColumns {
		if col.Alias != "" && strings.EqualFold(col.Alias, colRef.Column) {
			// Found a matching SELECT alias - resolve to output column reference
			return &BoundColumnRef{
				Table:     "__select_output__",
				Column:    col.Alias,
				ColumnIdx: i,
				ColType:   col.Expr.ResultType(),
			}, nil
		}
	}

	// No alias match found, return the original error
	return nil, err
}

// resolveWindowDef resolves a named window definition, handling transitive references.
// visited tracks the chain for cycle detection.
func (b *Binder) resolveWindowDef(name string, visited []string) (*parser.WindowDef, error) {
	def, ok := b.windowDefs[name]
	if !ok {
		return nil, b.errorf("undefined window: %s", name)
	}

	if def.RefName == "" {
		return def, nil
	}

	// Cycle detection
	refUpper := strings.ToUpper(def.RefName)
	for _, v := range visited {
		if v == refUpper {
			return nil, b.errorf("circular window reference: %s", def.RefName)
		}
	}

	visited = append(visited, name)
	base, err := b.resolveWindowDef(refUpper, visited)
	if err != nil {
		return nil, err
	}

	// Merge: base provides defaults, current overrides are NOT allowed for existing clauses
	if len(def.PartitionBy) > 0 && len(base.PartitionBy) > 0 {
		return nil, b.errorf("cannot override PARTITION BY of window %s", def.RefName)
	}
	if len(def.OrderBy) > 0 && len(base.OrderBy) > 0 {
		return nil, b.errorf("cannot override ORDER BY of window %s", def.RefName)
	}
	if def.Frame != nil && base.Frame != nil {
		return nil, b.errorf("cannot override frame of window %s", def.RefName)
	}

	// Merge base into def
	if len(def.PartitionBy) == 0 {
		def.PartitionBy = base.PartitionBy
	}
	if len(def.OrderBy) == 0 {
		def.OrderBy = base.OrderBy
	}
	if def.Frame == nil {
		def.Frame = base.Frame
	}

	// Clear RefName since we've resolved it
	def.RefName = ""

	return def, nil
}

// aggregateFunctions is the set of known aggregate function names (upper-cased).
var aggregateFunctions = map[string]struct{}{
	"COUNT": {}, "SUM": {}, "AVG": {}, "MIN": {}, "MAX": {},
	"MEDIAN": {}, "MODE": {}, "QUANTILE": {},
	"PERCENTILE_CONT": {}, "PERCENTILE_DISC": {},
	"ENTROPY": {}, "SKEWNESS": {}, "KURTOSIS": {},
	"STDDEV": {}, "STDDEV_SAMP": {}, "STDDEV_POP": {},
	"VARIANCE": {}, "VAR_SAMP": {}, "VAR_POP": {},
	"COVAR_SAMP": {}, "COVAR_POP": {}, "CORR": {},
	"REGR_SLOPE": {}, "REGR_INTERCEPT": {}, "REGR_COUNT": {},
	"REGR_R2": {}, "REGR_AVGX": {}, "REGR_AVGY": {},
	"REGR_SXX": {}, "REGR_SYY": {}, "REGR_SXY": {},
	"STRING_AGG": {}, "LISTAGG": {}, "GROUP_CONCAT": {},
	"ARRAY_AGG": {}, "LIST": {}, "FIRST": {}, "LAST": {},
	"ANY_VALUE": {}, "COUNT_IF": {}, "SUM_IF": {},
	"AVG_IF": {}, "MIN_IF": {}, "MAX_IF": {},
	"APPROX_COUNT_DISTINCT": {}, "APPROX_QUANTILE": {},
	"RESERVOIR_QUANTILE": {}, "HISTOGRAM": {},
	"ARG_MIN": {}, "ARGMIN": {}, "MIN_BY": {},
	"ARG_MAX": {}, "ARGMAX": {}, "MAX_BY": {},
	"PRODUCT": {}, "BITSTRING_AGG": {},
	"BOOL_AND": {}, "BOOL_OR": {}, "EVERY": {},
	"BIT_AND": {}, "BIT_OR": {}, "BIT_XOR": {},
	"FAVG": {}, "FSUM": {},
	"ARBITRARY": {}, "MEAN": {},
	"GEOMETRIC_MEAN": {}, "GEOMEAN": {}, "WEIGHTED_AVG": {},
}

// containsAggregate checks whether a parser expression contains an aggregate
// function call. Used by GROUP BY ALL expansion to distinguish grouping columns
// from aggregate expressions.
func containsAggregate(expr parser.Expr) bool {
	switch e := expr.(type) {
	case *parser.FunctionCall:
		if e.Star {
			// COUNT(*) is always an aggregate
			return true
		}
		_, ok := aggregateFunctions[strings.ToUpper(e.Name)]
		if ok {
			return true
		}
		// Check arguments recursively (e.g. SUM(a+b) nested inside another func)
		for _, arg := range e.Args {
			if containsAggregate(arg) {
				return true
			}
		}
		return false
	case *parser.BinaryExpr:
		return containsAggregate(e.Left) || containsAggregate(e.Right)
	case *parser.UnaryExpr:
		return containsAggregate(e.Expr)
	case *parser.CastExpr:
		return containsAggregate(e.Expr)
	default:
		return false
	}
}

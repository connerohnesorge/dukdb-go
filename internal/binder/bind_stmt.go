package binder

import (
	"fmt"
	"os"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/io/csv"
	"github.com/dukdb/dukdb-go/internal/io/json"
	"github.com/dukdb/dukdb-go/internal/io/parquet"
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

	// Check for table function
	if ref.TableFunction != nil {
		return b.bindTableFunction(ref)
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

// bindTableFunction binds a table function call (e.g., read_csv, read_json).
func (b *Binder) bindTableFunction(ref parser.TableRef) (*BoundTableRef, error) {
	tableFunc := ref.TableFunction
	alias := ref.Alias
	if alias == "" {
		alias = tableFunc.Name
	}

	// Extract the file path from the first positional argument
	if len(tableFunc.Args) == 0 {
		return nil, b.errorf("table function %s requires a file path argument", tableFunc.Name)
	}

	pathExpr := tableFunc.Args[0]
	pathLit, ok := pathExpr.(*parser.Literal)
	if !ok || pathLit.Type != dukdb.TYPE_VARCHAR {
		return nil, b.errorf("table function %s requires a string path as first argument", tableFunc.Name)
	}
	path, ok := pathLit.Value.(string)
	if !ok {
		return nil, b.errorf("table function %s requires a string path as first argument", tableFunc.Name)
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
		Options: options,
		Columns: columns,
	}

	boundRef := &BoundTableRef{
		TableName:     tableFunc.Name,
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
	b.scope.aliases[alias] = tableFunc.Name

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
		// No explicit options are passed - let the CSV reader auto-detect
		return b.inferCSVSchema(path, nil)
	case "read_json":
		return b.inferJSONSchema(path, options)
	case "read_json_auto":
		// read_json_auto uses auto-detection for format and schema
		return b.inferJSONSchema(path, nil)
	case "read_ndjson":
		// read_ndjson is an alias for read_json with NDJSON format
		if options == nil {
			options = make(map[string]any)
		}
		options["format"] = "newline_delimited"
		return b.inferJSONSchema(path, options)
	case "read_parquet":
		return b.inferParquetSchema(path, options)
	default:
		// For unknown table functions, return no columns
		// The executor will handle the error
		return nil, nil
	}
}

// inferCSVSchema reads a CSV file and infers its schema.
func (b *Binder) inferCSVSchema(path string, options map[string]any) ([]*catalog.ColumnDef, error) {
	// Open the file
	file, err := os.Open(path)
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

// inferJSONSchema reads a JSON file and infers its schema.
func (b *Binder) inferJSONSchema(path string, options map[string]any) ([]*catalog.ColumnDef, error) {
	// Open the file
	file, err := os.Open(path)
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

// inferParquetSchema reads a Parquet file and infers its schema.
func (b *Binder) inferParquetSchema(path string, options map[string]any) ([]*catalog.ColumnDef, error) {
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
	reader, err := parquet.NewReaderFromPath(path, opts)
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

// validateCopyOptions validates COPY statement options.
func (b *Binder) validateCopyOptions(stmt *BoundCopyStmt) error {
	// Validate FORMAT option
	if format, ok := stmt.Options["FORMAT"]; ok {
		formatStr, isStr := format.(string)
		if !isStr {
			return b.errorf("FORMAT option must be a string")
		}
		switch strings.ToUpper(formatStr) {
		case "CSV", "PARQUET", "JSON", "NDJSON":
			// Valid formats
		default:
			return b.errorf("unsupported FORMAT: %s (supported: CSV, PARQUET, JSON, NDJSON)", formatStr)
		}
	}

	// Validate CODEC option for Parquet
	if codec, ok := stmt.Options["CODEC"]; ok {
		codecStr, isStr := codec.(string)
		if !isStr {
			return b.errorf("CODEC option must be a string")
		}
		switch strings.ToUpper(codecStr) {
		case "UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD", "LZ4", "LZ4_RAW", "BROTLI":
			// Valid codecs
		default:
			return b.errorf("unsupported CODEC: %s (supported: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW, BROTLI)", codecStr)
		}
	}

	// Validate COMPRESSION option
	if compression, ok := stmt.Options["COMPRESSION"]; ok {
		compStr, isStr := compression.(string)
		if !isStr {
			return b.errorf("COMPRESSION option must be a string")
		}
		switch strings.ToUpper(compStr) {
		case "NONE", "GZIP", "ZSTD", "SNAPPY":
			// Valid compressions
		default:
			return b.errorf("unsupported COMPRESSION: %s (supported: NONE, GZIP, ZSTD, SNAPPY)", compStr)
		}
	}

	return nil
}

package binder

import (
	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/parser"
)

// bindFTSSearchFunction binds a full-text search table function call.
// Syntax: fts_search('table_name', 'search_query')
//      or match_bm25('table_name', 'search_query')
// Returns columns: rowid (BIGINT), score (DOUBLE)
func (b *Binder) bindFTSSearchFunction(ref parser.TableRef, funcName string) (*BoundTableRef, error) { //nolint:gocritic // hugeParam: consistent with other bind functions
	tableFunc := ref.TableFunction
	alias := ref.Alias
	if alias == "" {
		alias = funcName
	}

	if len(tableFunc.Args) < 2 {
		return nil, b.errorf("%s requires two arguments: table name and search query", funcName)
	}

	// Extract table name from first argument
	tableNameLit, ok := tableFunc.Args[0].(*parser.Literal)
	if !ok || tableNameLit.Type != dukdb.TYPE_VARCHAR {
		return nil, b.errorf("%s first argument must be a string (table name)", funcName)
	}
	tableName, ok := tableNameLit.Value.(string)
	if !ok {
		return nil, b.errorf("%s first argument must be a string (table name)", funcName)
	}

	// Extract search query from second argument
	queryLit, ok := tableFunc.Args[1].(*parser.Literal)
	if !ok || queryLit.Type != dukdb.TYPE_VARCHAR {
		return nil, b.errorf("%s second argument must be a string (search query)", funcName)
	}
	searchQuery, ok := queryLit.Value.(string)
	if !ok {
		return nil, b.errorf("%s second argument must be a string (search query)", funcName)
	}

	// Define output columns
	columns := []*catalog.ColumnDef{
		catalog.NewColumnDef("rowid", dukdb.TYPE_BIGINT),
		catalog.NewColumnDef("score", dukdb.TYPE_DOUBLE),
	}

	// Build options map
	options := map[string]any{
		"table_name": tableName,
		"query":      searchQuery,
	}

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

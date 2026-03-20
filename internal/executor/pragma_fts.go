package executor

import (
	"fmt"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// pragmaCreateFTSIndex handles PRAGMA create_fts_index('table', 'column').
// It creates a full-text search index on the specified column of the table,
// scanning all existing rows to populate the index.
func (e *Executor) pragmaCreateFTSIndex(
	_ *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if e.ftsRegistry == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "full-text search is not available",
		}
	}

	if len(plan.Args) < 2 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "PRAGMA create_fts_index requires two arguments: table name and column name",
		}
	}

	tableName, err := extractStringArg(plan.Args[0])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("PRAGMA create_fts_index: invalid table name: %v", err),
		}
	}

	columnName, err := extractStringArg(plan.Args[1])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("PRAGMA create_fts_index: invalid column name: %v", err),
		}
	}

	// Verify the table exists in storage
	table, ok := e.storage.GetTable(tableName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("table %q does not exist", tableName),
		}
	}

	// Find the column index in the table definition
	tableDef, ok := e.catalog.GetTableInSchema("main", tableName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("table %q not found in catalog", tableName),
		}
	}

	colIdx := -1
	for i, col := range tableDef.Columns {
		if col.Name == columnName {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("column %q does not exist in table %q", columnName, tableName),
		}
	}

	// Create the FTS index
	idx := e.ftsRegistry.CreateIndex(tableName, columnName)

	// Scan all existing rows using the table scanner (handles tombstones correctly)
	scanner := table.Scan()
	for {
		chunk := scanner.Next()
		if chunk == nil {
			break
		}

		rowCount := chunk.Count()
		for i := 0; i < rowCount; i++ {
			if colIdx >= chunk.ColumnCount() {
				continue
			}
			val := chunk.GetValue(i, colIdx)
			text, ok := val.(string)
			if !ok || text == "" {
				continue
			}
			// Use the RowID from the scanner if available
			var docID int64
			rowIDPtr := scanner.GetRowID(i)
			if rowIDPtr != nil {
				docID = int64(*rowIDPtr)
			} else {
				docID = int64(i)
			}
			idx.AddDocument(docID, text)
		}
	}

	return &ExecutionResult{
		Columns: []string{"Success"},
		Rows: []map[string]any{
			{"Success": true},
		},
	}, nil
}

// pragmaDropFTSIndex handles PRAGMA drop_fts_index('table').
// It removes the full-text search index for the specified table.
func (e *Executor) pragmaDropFTSIndex(
	_ *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if e.ftsRegistry == nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "full-text search is not available",
		}
	}

	if len(plan.Args) < 1 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "PRAGMA drop_fts_index requires one argument: table name",
		}
	}

	tableName, err := extractStringArg(plan.Args[0])
	if err != nil {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("PRAGMA drop_fts_index: invalid table name: %v", err),
		}
	}

	existed := e.ftsRegistry.DropIndex(tableName)
	if !existed {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeCatalog,
			Msg:  fmt.Sprintf("no FTS index exists for table %q", tableName),
		}
	}

	return &ExecutionResult{
		Columns: []string{"Success"},
		Rows: []map[string]any{
			{"Success": true},
		},
	}, nil
}

// extractStringArg extracts a string value from a bound expression (BoundLiteral).
func extractStringArg(arg binder.BoundExpr) (string, error) {
	lit, ok := arg.(*binder.BoundLiteral)
	if !ok {
		return "", fmt.Errorf("expected string literal argument")
	}
	s, ok := lit.Value.(string)
	if !ok {
		return "", fmt.Errorf("expected string value, got %T", lit.Value)
	}
	return s, nil
}

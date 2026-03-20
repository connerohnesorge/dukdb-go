package executor

import (
	"fmt"
	"math"

	"github.com/dukdb/dukdb-go/internal/planner"
)

// executeFTSSearch executes the fts_search or match_bm25 table function.
// It returns rows with (rowid, score) columns for documents matching the query.
func (e *Executor) executeFTSSearch(
	_ *ExecutionContext,
	plan *planner.PhysicalTableFunctionScan,
) (*ExecutionResult, error) {
	if e.ftsRegistry == nil {
		return nil, fmt.Errorf("full-text search is not available")
	}

	tableName, ok := plan.Options["table_name"].(string)
	if !ok {
		return nil, fmt.Errorf("fts_search: missing table_name option")
	}

	query, ok := plan.Options["query"].(string)
	if !ok {
		return nil, fmt.Errorf("fts_search: missing query option")
	}

	idx, ok := e.ftsRegistry.GetIndex(tableName)
	if !ok {
		return nil, fmt.Errorf("no FTS index exists for table %q; create one with PRAGMA create_fts_index('%s', 'column')", tableName, tableName)
	}

	results := idx.Search(query)

	columns := []string{"rowid", "score"}
	rows := make([]map[string]any, len(results))
	for i, r := range results {
		rows[i] = map[string]any{
			"rowid": r.DocID,
			"score": math.Round(r.Score*10000) / 10000, // Round to 4 decimal places
		}
	}

	return &ExecutionResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

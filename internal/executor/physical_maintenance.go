package executor

import (
	"fmt"
	"strings"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/planner"
)

// executePragma executes a PRAGMA statement.
func (e *Executor) executePragma(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	pragmaName := strings.ToLower(plan.Name)

	switch pragmaName {
	// Information pragmas
	case "database_size":
		return e.pragmaDatabaseSize(ctx)
	case "table_info":
		return e.pragmaTableInfo(ctx, plan)
	case "storage_info":
		return e.pragmaStorageInfo(ctx, plan)
	case "show_tables", "show_tables_all", "tables":
		return e.pragmaShowTables(ctx)
	case "functions":
		return e.pragmaFunctions(ctx)
	case "version":
		return e.pragmaVersion(ctx)
	case "database_list":
		return e.pragmaDatabaseList(ctx)
	case "collations":
		return e.pragmaCollations(ctx)
	case "show":
		return e.pragmaShow(ctx, plan)

	// Configuration pragmas
	case "memory_limit", "max_memory":
		return e.pragmaMemoryLimit(ctx, plan)
	case "threads", "worker_threads":
		return e.pragmaThreads(ctx, plan)
	case "temp_directory":
		return e.pragmaTempDirectory(ctx, plan)

	// Profiling pragmas
	case "enable_profiling":
		return e.pragmaEnableProfiling(ctx)
	case "disable_profiling":
		return e.pragmaDisableProfiling(ctx)
	case "profiling_mode":
		return e.pragmaProfilingMode(ctx, plan)
	case "enable_progress_bar":
		return e.pragmaEnableProgressBar(ctx)
	case "disable_progress_bar":
		return e.pragmaDisableProgressBar(ctx)

	default:
		// Unknown pragma - return empty result
		return &ExecutionResult{
			Rows:    []map[string]any{},
			Columns: []string{},
		}, nil
	}
}

// pragmaDatabaseSize returns database size information.
func (e *Executor) pragmaDatabaseSize(ctx *ExecutionContext) (*ExecutionResult, error) {
	// Calculate total size across all tables
	totalSize := int64(0)
	tableCount := 0

	tables := e.storage.Tables()
	for _, table := range tables {
		totalSize += table.RowCount() * 100 // Rough estimate: 100 bytes per row
		tableCount++
	}

	return &ExecutionResult{
		Columns: []string{"database_size", "block_count", "table_count"},
		Rows: []map[string]any{
			{
				"database_size": totalSize,
				"block_count":   totalSize / 262144, // 256KB blocks
				"table_count":   int64(tableCount),
			},
		},
	}, nil
}

// pragmaTableInfo returns table column information.
func (e *Executor) pragmaTableInfo(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if len(plan.Args) == 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "PRAGMA table_info requires a table name argument",
		}
	}

	// Get table name from first argument
	tableName, err := evalExprToString(ctx, plan.Args[0])
	if err != nil {
		return nil, err
	}

	// Get table definition
	tableDef, ok := e.catalog.GetTableInSchema("main", tableName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("table '%s' not found", tableName),
		}
	}

	result := &ExecutionResult{
		Columns: []string{"cid", "name", "type", "notnull", "dflt_value", "pk"},
		Rows:    make([]map[string]any, len(tableDef.Columns)),
	}

	for i, col := range tableDef.Columns {
		isPK := false
		for _, pkIdx := range tableDef.PrimaryKey {
			if pkIdx == i {
				isPK = true
				break
			}
		}

		result.Rows[i] = map[string]any{
			"cid":        int64(i),
			"name":       col.Name,
			"type":       col.Type.String(),
			"notnull":    boolToInt(!col.Nullable),
			"dflt_value": nil,
			"pk":         boolToInt(isPK),
		}
	}

	return result, nil
}

// pragmaStorageInfo returns storage information for a table.
func (e *Executor) pragmaStorageInfo(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if len(plan.Args) == 0 {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "PRAGMA storage_info requires a table name argument",
		}
	}

	tableName, err := evalExprToString(ctx, plan.Args[0])
	if err != nil {
		return nil, err
	}

	table, ok := e.storage.GetTable(tableName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("table '%s' not found", tableName),
		}
	}

	tableDef, ok := e.catalog.GetTableInSchema("main", tableName)
	if !ok {
		return nil, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("table '%s' not found in catalog", tableName),
		}
	}

	result := &ExecutionResult{
		Columns: []string{"column_name", "column_type", "row_count", "estimated_size"},
		Rows:    make([]map[string]any, len(tableDef.Columns)),
	}

	for i, col := range tableDef.Columns {
		result.Rows[i] = map[string]any{
			"column_name":    col.Name,
			"column_type":    col.Type.String(),
			"row_count":      table.RowCount(),
			"estimated_size": table.RowCount() * 8, // Rough estimate
		}
	}

	return result, nil
}

// pragmaShowTables returns a list of all tables.
func (e *Executor) pragmaShowTables(ctx *ExecutionContext) (*ExecutionResult, error) {
	tables := e.storage.Tables()

	result := &ExecutionResult{
		Columns: []string{"name"},
		Rows:    make([]map[string]any, 0, len(tables)),
	}

	for name := range tables {
		result.Rows = append(result.Rows, map[string]any{
			"name": name,
		})
	}

	return result, nil
}

// pragmaFunctions returns a list of available functions.
func (e *Executor) pragmaFunctions(ctx *ExecutionContext) (*ExecutionResult, error) {
	// Return built-in functions
	functions := []string{
		"ABS", "AVG", "CEIL", "CEILING", "COUNT", "FLOOR", "MAX", "MIN", "ROUND", "SUM",
		"UPPER", "LOWER", "LENGTH", "SUBSTRING", "CONCAT", "TRIM", "LTRIM", "RTRIM",
		"NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"COALESCE", "NULLIF", "IFNULL", "CASE", "CAST",
	}

	result := &ExecutionResult{
		Columns: []string{"name", "type"},
		Rows:    make([]map[string]any, len(functions)),
	}

	for i, name := range functions {
		result.Rows[i] = map[string]any{
			"name": name,
			"type": "scalar",
		}
	}

	return result, nil
}

// pragmaVersion returns the database version.
func (e *Executor) pragmaVersion(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"library_version"},
		Rows: []map[string]any{
			{"library_version": "dukdb-go v0.1.0"},
		},
	}, nil
}

// pragmaDatabaseList returns a list of attached databases.
func (e *Executor) pragmaDatabaseList(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"database_name", "path"},
		Rows: []map[string]any{
			{
				"database_name": "main",
				"path":          ":memory:",
			},
		},
	}, nil
}

// pragmaCollations returns available collations.
func (e *Executor) pragmaCollations(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"collation_name"},
		Rows: []map[string]any{
			{"collation_name": "NOCASE"},
			{"collation_name": "NOACCENT"},
			{"collation_name": "NFC"},
		},
	}, nil
}

// pragmaShow returns configuration values.
func (e *Executor) pragmaShow(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if len(plan.Args) == 0 {
		// Show all settings
		return &ExecutionResult{
			Columns: []string{"name", "value"},
			Rows: []map[string]any{
				{"name": "memory_limit", "value": "4GB"},
				{"name": "threads", "value": "4"},
				{"name": "temp_directory", "value": "/tmp"},
			},
		}, nil
	}

	// Show specific setting
	settingName, err := evalExprToString(ctx, plan.Args[0])
	if err != nil {
		return nil, err
	}

	var value string
	switch strings.ToLower(settingName) {
	case "memory_limit":
		value = "4GB"
	case "threads":
		value = "4"
	case "temp_directory":
		value = "/tmp"
	default:
		value = ""
	}

	return &ExecutionResult{
		Columns: []string{"value"},
		Rows: []map[string]any{
			{"value": value},
		},
	}, nil
}

// Configuration pragma stubs - these would store/retrieve actual config in a real implementation

func (e *Executor) pragmaMemoryLimit(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if plan.Value != nil {
		// SET mode - acknowledge but no-op for now
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}
	// GET mode
	return &ExecutionResult{
		Columns: []string{"memory_limit"},
		Rows:    []map[string]any{{"memory_limit": "4GB"}},
	}, nil
}

func (e *Executor) pragmaThreads(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if plan.Value != nil {
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}
	return &ExecutionResult{
		Columns: []string{"threads"},
		Rows:    []map[string]any{{"threads": int64(4)}},
	}, nil
}

func (e *Executor) pragmaTempDirectory(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if plan.Value != nil {
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}
	return &ExecutionResult{
		Columns: []string{"temp_directory"},
		Rows:    []map[string]any{{"temp_directory": "/tmp"}},
	}, nil
}

// Profiling pragma stubs

func (e *Executor) pragmaEnableProfiling(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"success"},
		Rows:    []map[string]any{{"success": true}},
	}, nil
}

func (e *Executor) pragmaDisableProfiling(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"success"},
		Rows:    []map[string]any{{"success": true}},
	}, nil
}

func (e *Executor) pragmaProfilingMode(ctx *ExecutionContext, plan *planner.PhysicalPragma) (*ExecutionResult, error) {
	if plan.Value != nil {
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}
	return &ExecutionResult{
		Columns: []string{"profiling_mode"},
		Rows:    []map[string]any{{"profiling_mode": "standard"}},
	}, nil
}

func (e *Executor) pragmaEnableProgressBar(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"success"},
		Rows:    []map[string]any{{"success": true}},
	}, nil
}

func (e *Executor) pragmaDisableProgressBar(ctx *ExecutionContext) (*ExecutionResult, error) {
	return &ExecutionResult{
		Columns: []string{"success"},
		Rows:    []map[string]any{{"success": true}},
	}, nil
}

// executeExplain executes an EXPLAIN statement.
func (e *Executor) executeExplain(
	ctx *ExecutionContext,
	plan *planner.PhysicalExplain,
) (*ExecutionResult, error) {
	// Generate plan text
	planText := formatPhysicalPlan(plan.Child, 0)

	if plan.Analyze {
		// For EXPLAIN ANALYZE, we would execute the plan and collect metrics
		// For now, just add a note
		planText = planText + "\n\n(ANALYZE mode: execution metrics would be shown here)"
	}

	return &ExecutionResult{
		Columns: []string{"explain_plan"},
		Rows: []map[string]any{
			{"explain_plan": planText},
		},
	}, nil
}

// formatPhysicalPlan formats a physical plan as a text tree.
func formatPhysicalPlan(plan planner.PhysicalPlan, indent int) string {
	prefix := strings.Repeat("  ", indent)
	var sb strings.Builder

	switch p := plan.(type) {
	case *planner.PhysicalScan:
		sb.WriteString(fmt.Sprintf("%sScan: %s", prefix, p.TableName))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
	case *planner.PhysicalFilter:
		sb.WriteString(fmt.Sprintf("%sFilter", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Child, indent+1))
	case *planner.PhysicalProject:
		sb.WriteString(fmt.Sprintf("%sProject: %d columns", prefix, len(p.Expressions)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Child, indent+1))
	case *planner.PhysicalHashJoin:
		sb.WriteString(fmt.Sprintf("%sHashJoin", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Left, indent+1))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Right, indent+1))
	case *planner.PhysicalNestedLoopJoin:
		sb.WriteString(fmt.Sprintf("%sNestedLoopJoin", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Left, indent+1))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Right, indent+1))
	case *planner.PhysicalHashAggregate:
		sb.WriteString(fmt.Sprintf("%sHashAggregate", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Child, indent+1))
	case *planner.PhysicalSort:
		sb.WriteString(fmt.Sprintf("%sSort", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Child, indent+1))
	case *planner.PhysicalLimit:
		sb.WriteString(fmt.Sprintf("%sLimit", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Child, indent+1))
	case *planner.PhysicalDistinct:
		sb.WriteString(fmt.Sprintf("%sDistinct", prefix))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlan(p.Child, indent+1))
	case *planner.PhysicalDummyScan:
		sb.WriteString(fmt.Sprintf("%sDummyScan", prefix))
	case *planner.PhysicalTableFunctionScan:
		sb.WriteString(fmt.Sprintf("%sTableFunction: %s", prefix, p.FunctionName))
	default:
		sb.WriteString(fmt.Sprintf("%s%T", prefix, plan))
	}

	return sb.String()
}

// executeVacuum executes a VACUUM statement.
func (e *Executor) executeVacuum(
	ctx *ExecutionContext,
	plan *planner.PhysicalVacuum,
) (*ExecutionResult, error) {
	// VACUUM reclaims space from deleted rows
	// In our in-memory implementation, this is mostly a no-op,
	// but we'll signal success

	if plan.TableName != "" {
		// Vacuum specific table
		_, ok := e.storage.GetTable(plan.TableName)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("table '%s' not found", plan.TableName),
			}
		}
		// In a real implementation, we would compact the table storage here
	} else {
		// Vacuum all tables
		// In a real implementation, we would iterate and compact all tables
	}

	return &ExecutionResult{
		Columns:      []string{},
		Rows:         []map[string]any{},
		RowsAffected: 0,
	}, nil
}

// executeAnalyze executes an ANALYZE statement.
func (e *Executor) executeAnalyze(
	ctx *ExecutionContext,
	plan *planner.PhysicalAnalyze,
) (*ExecutionResult, error) {
	// ANALYZE collects statistics about table columns
	// In our implementation, we'll collect basic statistics

	if plan.TableName != "" {
		// Analyze specific table
		table, ok := e.storage.GetTable(plan.TableName)
		if !ok {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("table '%s' not found", plan.TableName),
			}
		}
		// Collect statistics for this table
		_ = table // Would analyze table here
	} else {
		// Analyze all tables
		tables := e.storage.Tables()
		for _, table := range tables {
			_ = table // Would analyze table here
		}
	}

	return &ExecutionResult{
		Columns:      []string{},
		Rows:         []map[string]any{},
		RowsAffected: 0,
	}, nil
}

// executeCheckpoint executes a CHECKPOINT statement.
func (e *Executor) executeCheckpoint(
	ctx *ExecutionContext,
	plan *planner.PhysicalCheckpoint,
) (*ExecutionResult, error) {
	// CHECKPOINT forces pending changes to disk
	// In our in-memory implementation, this is a no-op
	// A real implementation would flush the WAL and write all dirty pages

	// If WAL is enabled, we could flush it here
	if e.wal != nil {
		// e.wal.Sync() would be called here
	}

	return &ExecutionResult{
		Columns:      []string{},
		Rows:         []map[string]any{},
		RowsAffected: 0,
	}, nil
}

// Helper functions

// evalExprToString evaluates a bound expression to a string value.
func evalExprToString(ctx *ExecutionContext, expr binder.BoundExpr) (string, error) {
	switch e := expr.(type) {
	case *binder.BoundLiteral:
		switch v := e.Value.(type) {
		case string:
			return v, nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		case float64:
			return fmt.Sprintf("%g", v), nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	default:
		return "", &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  "expected literal value",
		}
	}
}

// boolToInt converts a boolean to 0 or 1.
func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

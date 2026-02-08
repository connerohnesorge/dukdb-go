package executor

import (
	"fmt"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/cache"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/planner/rewrite"
	"github.com/dukdb/dukdb-go/internal/storage"
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
	case "checkpoint_threshold":
		return e.pragmaCheckpointThreshold(ctx, plan)
	case "query_cache_enabled":
		return e.pragmaQueryCacheEnabled(ctx, plan)
	case "enable_query_cache":
		return e.pragmaToggleQueryCache(ctx, true)
	case "disable_query_cache":
		return e.pragmaToggleQueryCache(ctx, false)
	case "query_cache_max_bytes":
		return e.pragmaQueryCacheMaxBytes(ctx, plan)
	case "query_cache_ttl":
		return e.pragmaQueryCacheTTL(ctx, plan)
	case "query_cache_parameter_mode":
		return e.pragmaQueryCacheParameterMode(ctx, plan)
	case "cache_status":
		return e.pragmaCacheStatus(ctx)
	case "clear_cache":
		return e.pragmaClearCache(ctx)

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
func (e *Executor) pragmaTableInfo(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
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
func (e *Executor) pragmaStorageInfo(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
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
func (e *Executor) pragmaShow(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
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

func (e *Executor) pragmaMemoryLimit(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
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

func (e *Executor) pragmaThreads(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if plan.Value != nil {
		// SET mode - update the global parallel configuration
		valueStr, err := evalExprToString(ctx, plan.Value)
		if err != nil {
			return nil, err
		}

		// Parse the thread count
		var threadCount int
		if _, err := fmt.Sscanf(valueStr, "%d", &threadCount); err != nil {
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("invalid thread count: %s", valueStr),
			}
		}

		// Update the global configuration
		if err := HandlePragmaThreads(threadCount); err != nil {
			return nil, err
		}

		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}
	// GET mode - return current thread configuration
	return &ExecutionResult{
		Columns: []string{"threads"},
		Rows:    []map[string]any{{"threads": int64(GetPragmaThreads())}},
	}, nil
}

func (e *Executor) pragmaTempDirectory(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
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

func (e *Executor) pragmaCheckpointThreshold(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if plan.Value != nil {
		// SET mode - parse, validate, and store the threshold value
		valueStr, err := evalExprToString(ctx, plan.Value)
		if err != nil {
			return nil, err
		}

		// Validate the threshold value using the config package function
		if err := dukdb.ValidateThreshold(valueStr); err != nil {
			return nil, err
		}

		// Store in the settings map
		if ctx.conn != nil {
			ctx.conn.SetSetting("checkpoint_threshold", valueStr)
		}

		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}

	// GET mode - retrieve the current threshold value
	var thresholdValue string
	if ctx.conn != nil {
		val := ctx.conn.GetSetting("checkpoint_threshold")
		if val != "" {
			thresholdValue = val
		} else {
			// Default value if not set
			thresholdValue = "256MB"
		}
	} else {
		thresholdValue = "256MB"
	}

	return &ExecutionResult{
		Columns: []string{"checkpoint_threshold"},
		Rows:    []map[string]any{{"checkpoint_threshold": thresholdValue}},
	}, nil
}

func (e *Executor) pragmaQueryCacheEnabled(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if plan.Value != nil {
		valueStr, err := evalExprToString(ctx, plan.Value)
		if err != nil {
			return nil, err
		}
		enabled, err := parseBoolSetting(valueStr)
		if err != nil {
			return nil, err
		}
		if ctx.conn != nil {
			ctx.conn.SetSetting("query_cache_enabled", boolToString(enabled))
		}
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}

	value := "false"
	if ctx.conn != nil {
		if setting := ctx.conn.GetSetting("query_cache_enabled"); setting != "" {
			value = setting
		}
	}

	return &ExecutionResult{
		Columns: []string{"query_cache_enabled"},
		Rows:    []map[string]any{{"query_cache_enabled": value}},
	}, nil
}

func (e *Executor) pragmaToggleQueryCache(
	ctx *ExecutionContext,
	enabled bool,
) (*ExecutionResult, error) {
	if ctx.conn != nil {
		ctx.conn.SetSetting("query_cache_enabled", boolToString(enabled))
	}
	return &ExecutionResult{
		Columns: []string{"success"},
		Rows:    []map[string]any{{"success": true}},
	}, nil
}

func (e *Executor) pragmaQueryCacheMaxBytes(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if plan.Value != nil {
		valueStr, err := evalExprToString(ctx, plan.Value)
		if err != nil {
			return nil, err
		}
		if _, err := parseCacheBytes(valueStr); err != nil {
			return nil, err
		}
		if ctx.conn != nil {
			ctx.conn.SetSetting("query_cache_max_bytes", valueStr)
		}
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}

	value := ""
	if ctx.conn != nil {
		value = ctx.conn.GetSetting("query_cache_max_bytes")
	}
	if value == "" {
		value = fmt.Sprintf("%d", cache.DefaultMaxBytes)
	}

	return &ExecutionResult{
		Columns: []string{"query_cache_max_bytes"},
		Rows:    []map[string]any{{"query_cache_max_bytes": value}},
	}, nil
}

func (e *Executor) pragmaQueryCacheTTL(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if plan.Value != nil {
		valueStr, err := evalExprToString(ctx, plan.Value)
		if err != nil {
			return nil, err
		}
		if _, err := parseCacheTTL(valueStr); err != nil {
			return nil, err
		}
		if ctx.conn != nil {
			ctx.conn.SetSetting("query_cache_ttl", valueStr)
		}
		return &ExecutionResult{
			Columns: []string{"success"},
			Rows:    []map[string]any{{"success": true}},
		}, nil
	}

	value := ""
	if ctx.conn != nil {
		value = ctx.conn.GetSetting("query_cache_ttl")
	}
	if value == "" {
		value = cache.DefaultTTL.String()
	}

	return &ExecutionResult{
		Columns: []string{"query_cache_ttl"},
		Rows:    []map[string]any{{"query_cache_ttl": value}},
	}, nil
}

func (e *Executor) pragmaQueryCacheParameterMode(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
	if plan.Value != nil {
		valueStr, err := evalExprToString(ctx, plan.Value)
		if err != nil {
			return nil, err
		}
		mode := strings.ToLower(strings.TrimSpace(valueStr))
		switch mode {
		case "exact", "structure":
			if ctx.conn != nil {
				ctx.conn.SetSetting("query_cache_parameter_mode", mode)
			}
			return &ExecutionResult{
				Columns: []string{"success"},
				Rows:    []map[string]any{{"success": true}},
			}, nil
		default:
			return nil, &dukdb.Error{
				Type: dukdb.ErrorTypeExecutor,
				Msg:  fmt.Sprintf("invalid query_cache_parameter_mode: %s", valueStr),
			}
		}
	}

	value := ""
	if ctx.conn != nil {
		value = ctx.conn.GetSetting("query_cache_parameter_mode")
	}
	if value == "" {
		value = "exact"
	}

	return &ExecutionResult{
		Columns: []string{"query_cache_parameter_mode"},
		Rows:    []map[string]any{{"query_cache_parameter_mode": value}},
	}, nil
}

func (e *Executor) pragmaCacheStatus(ctx *ExecutionContext) (*ExecutionResult, error) {
	stats := cache.CacheStats{}
	if e.queryCache != nil {
		stats = e.queryCache.Stats()
	}

	enabled := false
	if ctx.conn != nil {
		value := strings.ToLower(strings.TrimSpace(ctx.conn.GetSetting("query_cache_enabled")))
		enabled = value == "1" || value == "true" || value == "on" || value == "yes"
	}

	maxBytes := int64(cache.DefaultMaxBytes)
	if ctx.conn != nil {
		if value := ctx.conn.GetSetting("query_cache_max_bytes"); value != "" {
			if parsed, err := parseCacheBytes(value); err == nil {
				maxBytes = parsed
			}
		}
	}

	ttlValue := cache.DefaultTTL.String()
	if ctx.conn != nil {
		if value := ctx.conn.GetSetting("query_cache_ttl"); value != "" {
			ttlValue = value
		}
	}

	parameterMode := "exact"
	if ctx.conn != nil {
		if value := ctx.conn.GetSetting("query_cache_parameter_mode"); value != "" {
			parameterMode = value
		}
	}

	return &ExecutionResult{
		Columns: []string{"enabled", "entries", "bytes", "hits", "misses", "evictions", "max_bytes", "ttl", "parameter_mode"},
		Rows: []map[string]any{{
			"enabled":        enabled,
			"entries":        int64(stats.Entries),
			"bytes":          stats.Bytes,
			"hits":           int64(stats.Hits),
			"misses":         int64(stats.Misses),
			"evictions":      int64(stats.Evictions),
			"max_bytes":      maxBytes,
			"ttl":            ttlValue,
			"parameter_mode": parameterMode,
		}},
	}, nil
}

func (e *Executor) pragmaClearCache(ctx *ExecutionContext) (*ExecutionResult, error) {
	if e.queryCache != nil {
		e.queryCache.Clear()
	}
	return &ExecutionResult{
		Columns: []string{"success"},
		Rows:    []map[string]any{{"success": true}},
	}, nil
}

func parseBoolSetting(value string) (bool, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "1", "true", "on", "yes":
		return true, nil
	case "0", "false", "off", "no":
		return false, nil
	default:
		return false, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("invalid boolean value: %s", value),
		}
	}
}

func boolToString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func parseCacheBytes(value string) (int64, error) {
	bytes, err := dukdb.ParseThreshold(value)
	if err == nil {
		return bytes, nil
	}
	parsed, parseErr := parseInt64(value)
	if parseErr != nil {
		return 0, parseErr
	}
	return parsed, nil
}

func parseCacheTTL(value string) (time.Duration, error) {
	if parsed, err := time.ParseDuration(value); err == nil {
		if parsed <= 0 {
			return 0, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "query_cache_ttl must be positive"}
		}
		return parsed, nil
	}
	seconds, err := parseInt64(value)
	if err != nil {
		return 0, &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("invalid query_cache_ttl: %s", value),
		}
	}
	if seconds <= 0 {
		return 0, &dukdb.Error{Type: dukdb.ErrorTypeExecutor, Msg: "query_cache_ttl must be positive"}
	}
	return time.Duration(seconds) * time.Second, nil
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

func (e *Executor) pragmaProfilingMode(
	ctx *ExecutionContext,
	plan *planner.PhysicalPragma,
) (*ExecutionResult, error) {
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
	// Create a cost model for estimating costs
	costModel := optimizer.NewCostModel(
		optimizer.DefaultCostConstants(),
		nil, // CardinalityEstimator - not needed for physical plans
	)

	// Generate plan text with cost annotations
	planText := formatPhysicalPlanWithCost(plan.Child, 0, costModel)
	if stats := plan.RewriteStats; stats != nil {
		if len(stats.Applied) > 0 || len(stats.Skipped) > 0 {
			planText = planText + "\n\n" + formatRewriteStats(stats)
		}
	}

	if plan.Analyze {
		// For EXPLAIN ANALYZE, execute the plan and collect actual metrics
		planText = e.formatExplainAnalyze(ctx, plan.Child, costModel)
	}

	return &ExecutionResult{
		Columns: []string{"explain_plan"},
		Rows: []map[string]any{
			{"explain_plan": planText},
		},
	}, nil
}

func formatRewriteStats(stats *rewrite.Stats) string {
	if stats == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("Rewrite Rules:\n")
	sb.WriteString(fmt.Sprintf("Iterations: %d", stats.Iterations))
	if stats.LimitReached {
		sb.WriteString(" (limit reached)")
	}
	sb.WriteString("\n")

	for _, event := range stats.Events {
		if event.Applied {
			sb.WriteString(fmt.Sprintf("- applied: %s", event.Name))
			if event.BeforeCost > 0 || event.AfterCost > 0 {
				sb.WriteString(fmt.Sprintf(" (cost %.2f -> %.2f)", event.BeforeCost, event.AfterCost))
			}
		} else {
			sb.WriteString(fmt.Sprintf("- skipped: %s", event.Name))
			if event.Reason != "" {
				sb.WriteString(" (" + event.Reason + ")")
			}
		}
		sb.WriteString("\n")
	}

	return strings.TrimRight(sb.String(), "\n")
}

// formatCostAnnotation formats a PlanCost as a cost annotation string.
// Format: (cost=startup..total rows=N width=N)
func formatCostAnnotation(cost optimizer.PlanCost) string {
	return fmt.Sprintf("(cost=%.2f..%.2f rows=%.0f width=%d)",
		cost.StartupCost, cost.TotalCost, cost.OutputRows, cost.OutputWidth)
}

// formatIndexCondition formats the index lookup conditions for EXPLAIN output.
// It formats the lookup keys associated with a PhysicalIndexScan.
// Format examples:
//   - Single key: "(id = 42)"
//   - Multiple keys: "((a = 1) AND (b = 2))"
//   - String values: "(email = 'test@example.com')"
func formatIndexCondition(scan *planner.PhysicalIndexScan) string {
	if scan == nil || len(scan.LookupKeys) == 0 {
		return ""
	}

	// Get column names from the index definition
	var indexCols []string
	if scan.IndexDef != nil {
		indexCols = scan.IndexDef.Columns
	}

	var parts []string
	for i, key := range scan.LookupKeys {
		// Determine column name
		var colName string
		if i < len(indexCols) {
			colName = indexCols[i]
		} else {
			colName = fmt.Sprintf("col%d", i)
		}

		// Format the value
		valueStr := formatExpressionValue(key)

		// Build the condition (equality for point lookups)
		parts = append(parts, fmt.Sprintf("(%s = %s)", colName, valueStr))
	}

	// If single condition, return it directly
	if len(parts) == 1 {
		return parts[0]
	}

	// Multiple conditions - join with AND
	return "(" + strings.Join(parts, " AND ") + ")"
}

// formatExpressionValue formats a bound expression's value for EXPLAIN display.
// Handles literals, column references, and parameter placeholders.
func formatExpressionValue(expr binder.BoundExpr) string {
	if expr == nil {
		return "<nil>"
	}

	switch e := expr.(type) {
	case *binder.BoundLiteral:
		return formatLiteralValue(e.Value)
	case *binder.BoundColumnRef:
		if e.Table != "" {
			return fmt.Sprintf("%s.%s", e.Table, e.Column)
		}
		return e.Column
	default:
		// For other expression types, provide a generic representation
		return "<expr>"
	}
}

// formatLiteralValue formats a literal value for EXPLAIN display.
// Handles various types (strings, integers, floats, booleans, nil).
func formatLiteralValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		// Escape single quotes in the string
		escaped := strings.ReplaceAll(val, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case int:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case []byte:
		return fmt.Sprintf("'\\x%x'", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// formatResidualFilter formats a residual filter condition for EXPLAIN display.
// Returns empty string if filter is nil.
// Handles single predicates, AND/OR combinations, and complex expressions.
func formatResidualFilter(filter binder.BoundExpr) string {
	if filter == nil {
		return ""
	}

	return formatFilterExpression(filter)
}

// formatFilterExpression recursively formats a filter expression for EXPLAIN display.
// It handles AND/OR combinations, comparisons, and other expression types.
func formatFilterExpression(expr binder.BoundExpr) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *binder.BoundBinaryExpr:
		// Handle AND/OR combinations specially for better formatting
		if e.Op == parser.OpAnd {
			left := formatFilterExpression(e.Left)
			right := formatFilterExpression(e.Right)
			if left == "" {
				return right
			}
			if right == "" {
				return left
			}
			return fmt.Sprintf("(%s AND %s)", left, right)
		}
		if e.Op == parser.OpOr {
			left := formatFilterExpression(e.Left)
			right := formatFilterExpression(e.Right)
			if left == "" {
				return right
			}
			if right == "" {
				return left
			}
			return fmt.Sprintf("(%s OR %s)", left, right)
		}
		// Handle comparison operators
		left := formatFilterExpression(e.Left)
		right := formatFilterExpression(e.Right)
		op := formatBinaryOp(e.Op)
		return fmt.Sprintf("(%s %s %s)", left, op, right)

	case *binder.BoundUnaryExpr:
		// Handle NOT, IS NULL, IS NOT NULL, and other unary operators
		operand := formatFilterExpression(e.Expr)
		switch e.Op {
		case parser.OpNot:
			return fmt.Sprintf("(NOT %s)", operand)
		case parser.OpNeg:
			return fmt.Sprintf("(-%s)", operand)
		case parser.OpIsNull:
			return fmt.Sprintf("(%s IS NULL)", operand)
		case parser.OpIsNotNull:
			return fmt.Sprintf("(%s IS NOT NULL)", operand)
		default:
			return operand
		}

	case *binder.BoundLiteral:
		return formatLiteralValue(e.Value)

	case *binder.BoundColumnRef:
		if e.Table != "" {
			return fmt.Sprintf("%s.%s", e.Table, e.Column)
		}
		return e.Column

	case *binder.BoundFunctionCall:
		// Format function calls
		var args []string
		for _, arg := range e.Args {
			args = append(args, formatFilterExpression(arg))
		}
		return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))

	case *binder.BoundBetweenExpr:
		operand := formatFilterExpression(e.Expr)
		low := formatFilterExpression(e.Low)
		high := formatFilterExpression(e.High)
		if e.Not {
			return fmt.Sprintf("(%s NOT BETWEEN %s AND %s)", operand, low, high)
		}
		return fmt.Sprintf("(%s BETWEEN %s AND %s)", operand, low, high)

	case *binder.BoundInListExpr:
		operand := formatFilterExpression(e.Expr)
		var values []string
		for _, val := range e.Values {
			values = append(values, formatFilterExpression(val))
		}
		if e.Not {
			return fmt.Sprintf("(%s NOT IN (%s))", operand, strings.Join(values, ", "))
		}
		return fmt.Sprintf("(%s IN (%s))", operand, strings.Join(values, ", "))

	case *binder.BoundInSubqueryExpr:
		operand := formatFilterExpression(e.Expr)
		if e.Not {
			return fmt.Sprintf("(%s NOT IN <subquery>)", operand)
		}
		return fmt.Sprintf("(%s IN <subquery>)", operand)

	case *binder.BoundExistsExpr:
		if e.Not {
			return "(NOT EXISTS <subquery>)"
		}
		return "(EXISTS <subquery>)"

	case *binder.BoundCastExpr:
		operand := formatFilterExpression(e.Expr)
		return fmt.Sprintf("CAST(%s AS %s)", operand, e.TargetType.String())

	case *binder.BoundCaseExpr:
		// Simplified CASE representation
		return "<CASE>"

	default:
		return "<expr>"
	}
}

// formatBinaryOp converts a binary operator to its string representation.
func formatBinaryOp(op parser.BinaryOp) string {
	switch op {
	case parser.OpEq:
		return "="
	case parser.OpNe:
		return "<>"
	case parser.OpLt:
		return "<"
	case parser.OpGt:
		return ">"
	case parser.OpLe:
		return "<="
	case parser.OpGe:
		return ">="
	case parser.OpAnd:
		return "AND"
	case parser.OpOr:
		return "OR"
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
	default:
		return fmt.Sprintf("op%d", op)
	}
}

// ExplainAnalyzeMetrics holds actual execution metrics for EXPLAIN ANALYZE.
type ExplainAnalyzeMetrics struct {
	ActualRows int64
	ActualTime time.Duration
}

// formatExplainAnalyze executes the plan and formats with both estimated and actual metrics.
func (e *Executor) formatExplainAnalyze(
	ctx *ExecutionContext,
	plan planner.PhysicalPlan,
	costModel *optimizer.CostModel,
) string {
	// Track execution time
	startTime := time.Now()

	// Execute the plan to get actual row counts
	result, err := e.executeWithContext(ctx, plan)
	actualTime := time.Since(startTime)

	if err != nil {
		// If execution fails, show error in the explain output
		return formatPhysicalPlanWithCost(plan, 0, costModel) +
			fmt.Sprintf("\n\n(EXPLAIN ANALYZE failed: %v)", err)
	}

	actualRows := int64(len(result.Rows))

	// Generate plan text with both estimated and actual metrics
	return formatPhysicalPlanWithAnalyze(plan, 0, costModel, actualRows, actualTime)
}

// formatPhysicalPlanWithAnalyze formats a physical plan with both estimated and actual metrics.
func formatPhysicalPlanWithAnalyze(
	plan planner.PhysicalPlan,
	indent int,
	costModel *optimizer.CostModel,
	actualRows int64,
	actualTime time.Duration,
) string {
	// Get estimated cost
	adapter := &physicalPlanAdapter{plan: plan}
	cost := costModel.EstimateCost(adapter)

	prefix := strings.Repeat("  ", indent)
	var sb strings.Builder

	// Format node with both estimated and actual metrics
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		sb.WriteString(fmt.Sprintf("%sScan: %s %s (actual rows=%d time=%.2fms)",
			prefix, p.TableName, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
	case *planner.PhysicalIndexScan:
		// Determine scan type label based on scan characteristics
		scanType := "IndexScan"
		if p.IsIndexOnly {
			scanType = "IndexOnlyScan"
		}
		sb.WriteString(fmt.Sprintf("%s%s: %s USING %s %s (actual rows=%d time=%.2fms)",
			prefix, scanType, p.TableName, p.IndexName, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
		// Add Index Cond line showing lookup keys
		if indexCond := formatIndexCondition(p); indexCond != "" {
			sb.WriteString("\n")
			sb.WriteString(fmt.Sprintf("%s  Index Cond: %s", prefix, indexCond))
		}
		// Add Filter line showing residual filter (predicates that can't be pushed into index)
		if residualFilter := formatResidualFilter(p.ResidualFilter); residualFilter != "" {
			sb.WriteString("\n")
			sb.WriteString(fmt.Sprintf("%s  Filter: %s", prefix, residualFilter))
		}
	case *planner.PhysicalFilter:
		sb.WriteString(fmt.Sprintf("%sFilter %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalProject:
		sb.WriteString(fmt.Sprintf("%sProject: %d columns %s (actual rows=%d time=%.2fms)",
			prefix, len(p.Expressions), formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalHashJoin:
		sb.WriteString(fmt.Sprintf("%sHashJoin %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Left, indent+1, costModel))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Right, indent+1, costModel))
	case *planner.PhysicalNestedLoopJoin:
		sb.WriteString(fmt.Sprintf("%sNestedLoopJoin %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Left, indent+1, costModel))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Right, indent+1, costModel))
	case *planner.PhysicalHashAggregate:
		sb.WriteString(fmt.Sprintf("%sHashAggregate %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalSort:
		sb.WriteString(fmt.Sprintf("%sSort %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalLimit:
		sb.WriteString(fmt.Sprintf("%sLimit %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalDistinct:
		sb.WriteString(fmt.Sprintf("%sDistinct %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalDummyScan:
		sb.WriteString(fmt.Sprintf("%sDummyScan %s (actual rows=%d time=%.2fms)",
			prefix, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
	case *planner.PhysicalTableFunctionScan:
		sb.WriteString(fmt.Sprintf("%sTableFunction: %s %s (actual rows=%d time=%.2fms)",
			prefix, p.FunctionName, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
	default:
		sb.WriteString(fmt.Sprintf("%s%T %s (actual rows=%d time=%.2fms)",
			prefix, plan, formatCostAnnotation(cost), actualRows, float64(actualTime.Microseconds())/1000))
	}

	return sb.String()
}

// physicalPlanAdapter adapts a planner.PhysicalPlan to the optimizer.PhysicalPlanNode interface.
// This allows us to use the CostModel to estimate costs for physical plans.
type physicalPlanAdapter struct {
	plan planner.PhysicalPlan
}

func (a *physicalPlanAdapter) PhysicalPlanType() string {
	switch p := a.plan.(type) {
	case *planner.PhysicalScan:
		return "PhysicalScan"
	case *planner.PhysicalIndexScan:
		// Distinguish between different index scan types for cost estimation
		if p.IsIndexOnly {
			return "PhysicalIndexOnlyScan"
		}
		return "PhysicalIndexScan"
	case *planner.PhysicalFilter:
		return "PhysicalFilter"
	case *planner.PhysicalProject:
		return "PhysicalProject"
	case *planner.PhysicalHashJoin:
		return "PhysicalHashJoin"
	case *planner.PhysicalNestedLoopJoin:
		return "PhysicalNestedLoopJoin"
	case *planner.PhysicalHashAggregate:
		return "PhysicalHashAggregate"
	case *planner.PhysicalSort:
		return "PhysicalSort"
	case *planner.PhysicalLimit:
		return "PhysicalLimit"
	case *planner.PhysicalDistinct:
		return "PhysicalDistinct"
	case *planner.PhysicalWindow:
		return "PhysicalWindow"
	case *planner.PhysicalDummyScan:
		return "PhysicalDummyScan"
	default:
		return "Unknown"
	}
}

func (a *physicalPlanAdapter) PhysicalChildren() []optimizer.PhysicalPlanNode {
	children := a.plan.Children()
	result := make([]optimizer.PhysicalPlanNode, len(children))
	for i, child := range children {
		result[i] = &physicalPlanAdapter{plan: child}
	}
	return result
}

func (a *physicalPlanAdapter) PhysicalOutputColumns() []optimizer.PhysicalOutputColumn {
	cols := a.plan.OutputColumns()
	result := make([]optimizer.PhysicalOutputColumn, len(cols))
	for i, col := range cols {
		result[i] = optimizer.PhysicalOutputColumn{
			Table:     col.Table,
			Column:    col.Column,
			Type:      col.Type,
			TableIdx:  col.TableIdx,
			ColumnIdx: col.ColumnIdx,
		}
	}
	return result
}

// Implement PhysicalScanNode interface for scan nodes
func (a *physicalPlanAdapter) ScanSchema() string {
	if scan, ok := a.plan.(*planner.PhysicalScan); ok {
		return scan.Schema
	}
	return ""
}

func (a *physicalPlanAdapter) ScanTableName() string {
	if scan, ok := a.plan.(*planner.PhysicalScan); ok {
		return scan.TableName
	}
	return ""
}

func (a *physicalPlanAdapter) ScanAlias() string {
	if scan, ok := a.plan.(*planner.PhysicalScan); ok {
		return scan.Alias
	}
	return ""
}

func (a *physicalPlanAdapter) ScanRowCount() float64 {
	if scan, ok := a.plan.(*planner.PhysicalScan); ok {
		if scan.TableDef != nil && scan.TableDef.Statistics != nil {
			return float64(scan.TableDef.Statistics.RowCount)
		}
	}
	return optimizer.DefaultRowCount
}

func (a *physicalPlanAdapter) ScanPageCount() float64 {
	if scan, ok := a.plan.(*planner.PhysicalScan); ok {
		if scan.TableDef != nil && scan.TableDef.Statistics != nil {
			return float64(scan.TableDef.Statistics.PageCount)
		}
	}
	return optimizer.DefaultPageCount
}

// formatPhysicalPlanWithCost formats a physical plan with cost annotations.
func formatPhysicalPlanWithCost(
	plan planner.PhysicalPlan,
	indent int,
	costModel *optimizer.CostModel,
) string {
	prefix := strings.Repeat("  ", indent)
	var sb strings.Builder

	// Get cost estimate for this plan node
	adapter := &physicalPlanAdapter{plan: plan}
	cost := costModel.EstimateCost(adapter)

	switch p := plan.(type) {
	case *planner.PhysicalScan:
		sb.WriteString(fmt.Sprintf("%sScan: %s %s", prefix, p.TableName, formatCostAnnotation(cost)))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
	case *planner.PhysicalIndexScan:
		// Determine scan type label based on scan characteristics
		// Note: IsRangeScan field may be added in future; for now use IndexScan
		scanType := "IndexScan"
		if p.IsIndexOnly {
			scanType = "IndexOnlyScan"
		}
		sb.WriteString(fmt.Sprintf("%s%s: %s USING %s %s",
			prefix, scanType, p.TableName, p.IndexName, formatCostAnnotation(cost)))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
		// Add Index Cond line showing lookup keys
		if indexCond := formatIndexCondition(p); indexCond != "" {
			sb.WriteString("\n")
			sb.WriteString(fmt.Sprintf("%s  Index Cond: %s", prefix, indexCond))
		}
		// Add Filter line showing residual filter (predicates that can't be pushed into index)
		if residualFilter := formatResidualFilter(p.ResidualFilter); residualFilter != "" {
			sb.WriteString("\n")
			sb.WriteString(fmt.Sprintf("%s  Filter: %s", prefix, residualFilter))
		}
	case *planner.PhysicalFilter:
		sb.WriteString(fmt.Sprintf("%sFilter %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalProject:
		sb.WriteString(fmt.Sprintf("%sProject: %d columns %s", prefix, len(p.Expressions), formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalHashJoin:
		sb.WriteString(fmt.Sprintf("%sHashJoin %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Left, indent+1, costModel))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Right, indent+1, costModel))
	case *planner.PhysicalNestedLoopJoin:
		sb.WriteString(fmt.Sprintf("%sNestedLoopJoin %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Left, indent+1, costModel))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Right, indent+1, costModel))
	case *planner.PhysicalHashAggregate:
		sb.WriteString(fmt.Sprintf("%sHashAggregate %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalSort:
		sb.WriteString(fmt.Sprintf("%sSort %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalLimit:
		sb.WriteString(fmt.Sprintf("%sLimit %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalDistinct:
		sb.WriteString(fmt.Sprintf("%sDistinct %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	case *planner.PhysicalDummyScan:
		sb.WriteString(fmt.Sprintf("%sDummyScan %s", prefix, formatCostAnnotation(cost)))
	case *planner.PhysicalTableFunctionScan:
		sb.WriteString(fmt.Sprintf("%sTableFunction: %s %s", prefix, p.FunctionName, formatCostAnnotation(cost)))
	case *planner.PhysicalWindow:
		sb.WriteString(fmt.Sprintf("%sWindow %s", prefix, formatCostAnnotation(cost)))
		sb.WriteString("\n")
		sb.WriteString(formatPhysicalPlanWithCost(p.Child, indent+1, costModel))
	default:
		sb.WriteString(fmt.Sprintf("%s%T %s", prefix, plan, formatCostAnnotation(cost)))
	}

	return sb.String()
}

// formatPhysicalPlan formats a physical plan as a text tree (without costs, for backwards compatibility).
func formatPhysicalPlan(plan planner.PhysicalPlan, indent int) string {
	prefix := strings.Repeat("  ", indent)
	var sb strings.Builder

	switch p := plan.(type) {
	case *planner.PhysicalScan:
		sb.WriteString(fmt.Sprintf("%sScan: %s", prefix, p.TableName))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
	case *planner.PhysicalIndexScan:
		// Determine scan type label based on scan characteristics
		scanType := "IndexScan"
		if p.IsIndexOnly {
			scanType = "IndexOnlyScan"
		}
		sb.WriteString(fmt.Sprintf("%s%s: %s USING %s", prefix, scanType, p.TableName, p.IndexName))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
		// Add Index Cond line showing lookup keys
		if indexCond := formatIndexCondition(p); indexCond != "" {
			sb.WriteString("\n")
			sb.WriteString(fmt.Sprintf("%s  Index Cond: %s", prefix, indexCond))
		}
		// Add Filter line showing residual filter (predicates that can't be pushed into index)
		if residualFilter := formatResidualFilter(p.ResidualFilter); residualFilter != "" {
			sb.WriteString("\n")
			sb.WriteString(fmt.Sprintf("%s  Filter: %s", prefix, residualFilter))
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

	if plan.TableName != "" {
		// Analyze specific table
		err := e.analyzeTable(plan.TableName)
		if err != nil {
			return nil, err
		}
	} else {
		// Analyze all tables
		tables := e.storage.Tables()
		for tableName := range tables {
			err := e.analyzeTable(tableName)
			if err != nil {
				return nil, err
			}
		}
	}

	return &ExecutionResult{
		Columns:      []string{},
		Rows:         []map[string]any{},
		RowsAffected: 0,
	}, nil
}

// analyzeTable collects statistics for a single table.
func (e *Executor) analyzeTable(tableName string) error {
	// Get table from storage
	table, ok := e.storage.GetTable(tableName)
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("table '%s' not found", tableName),
		}
	}

	// Get table definition from catalog
	tableDef, ok := e.catalog.GetTableInSchema("main", tableName)
	if !ok {
		return &dukdb.Error{
			Type: dukdb.ErrorTypeExecutor,
			Msg:  fmt.Sprintf("table '%s' not found in catalog", tableName),
		}
	}

	// Create statistics collector
	collector := optimizer.NewStatisticsCollector()

	// Build column names and types
	columnNames := tableDef.ColumnNames()
	columnTypes := tableDef.ColumnTypes()

	// Create a data reader that reads column data from the table
	dataReader := func(columnIndex int) ([]any, error) {
		return e.readColumnData(table, columnIndex)
	}

	// Collect statistics
	stats, err := collector.CollectTableStats(
		columnNames,
		columnTypes,
		table.RowCount(),
		dataReader,
	)
	if err != nil {
		return err
	}

	// Store statistics in the catalog table definition
	tableDef.Statistics = stats

	return nil
}

// readColumnData reads all values for a column from a table.
func (e *Executor) readColumnData(table *storage.Table, columnIndex int) ([]any, error) {
	var values []any

	// Scan all row groups
	for i := 0; i < table.RowGroupCount(); i++ {
		rg := table.GetRowGroup(i)
		if rg == nil {
			continue
		}

		col := rg.GetColumn(columnIndex)
		if col == nil {
			continue
		}

		// Read all values from this column
		for j := 0; j < col.Count(); j++ {
			values = append(values, col.GetValue(j))
		}
	}

	return values, nil
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

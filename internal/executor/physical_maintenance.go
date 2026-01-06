package executor

import (
	"fmt"
	"strings"
	"time"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/binder"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/planner"
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
	// Create a cost model for estimating costs
	costModel := optimizer.NewCostModel(
		optimizer.DefaultCostConstants(),
		nil, // CardinalityEstimator - not needed for physical plans
	)

	// Generate plan text with cost annotations
	planText := formatPhysicalPlanWithCost(plan.Child, 0, costModel)

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

// formatCostAnnotation formats a PlanCost as a cost annotation string.
// Format: (cost=startup..total rows=N width=N)
func formatCostAnnotation(cost optimizer.PlanCost) string {
	return fmt.Sprintf("(cost=%.2f..%.2f rows=%.0f width=%d)",
		cost.StartupCost, cost.TotalCost, cost.OutputRows, cost.OutputWidth)
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
	switch a.plan.(type) {
	case *planner.PhysicalScan:
		return "PhysicalScan"
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
func formatPhysicalPlanWithCost(plan planner.PhysicalPlan, indent int, costModel *optimizer.CostModel) string {
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

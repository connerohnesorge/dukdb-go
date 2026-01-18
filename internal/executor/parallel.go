// Package executor provides query execution for the native Go DuckDB implementation.
// This file implements parallel execution integration with the main executor.
package executor

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/parallel"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// Parallel execution thresholds and constants.
const (
	// MinRowsForParallel is the minimum number of rows to consider parallelization.
	// Below this threshold, sequential execution is always used.
	MinRowsForParallel = 10000

	// MinCostForParallel is the minimum cost to consider parallelization.
	// Based on the cost model, operations below this cost don't benefit from parallel execution.
	MinCostForParallel = 100.0

	// ParallelOverheadFactor accounts for parallelization overhead.
	// Parallel execution must be at least this much faster to be worth the overhead.
	ParallelOverheadFactor = 1.2

	// DefaultMaxParallelWorkers is the default maximum number of parallel workers.
	DefaultMaxParallelWorkers = 0 // 0 means use GOMAXPROCS
)

// ParallelConfig holds parallel execution settings.
// These settings can be configured via PRAGMA statements or programmatically.
type ParallelConfig struct {
	// NumThreads is the number of worker threads.
	// 0 means use GOMAXPROCS.
	NumThreads int

	// EnableParallelScan enables parallel table scans.
	EnableParallelScan bool

	// EnableParallelJoin enables parallel hash joins.
	EnableParallelJoin bool

	// EnableParallelAggregate enables parallel aggregations.
	EnableParallelAggregate bool

	// EnableParallelSort enables parallel sorting.
	EnableParallelSort bool

	// MinRowsForParallel is the minimum rows to consider parallelization.
	MinRowsForParallel int

	// MemoryLimit is the maximum memory for parallel operations (in bytes).
	// 0 means no limit.
	MemoryLimit int64
}

// DefaultParallelConfig returns the default parallel configuration.
func DefaultParallelConfig() ParallelConfig {
	return ParallelConfig{
		NumThreads:              DefaultMaxParallelWorkers,
		EnableParallelScan:      true,
		EnableParallelJoin:      true,
		EnableParallelAggregate: true,
		EnableParallelSort:      true,
		MinRowsForParallel:      MinRowsForParallel,
		MemoryLimit:             0, // No limit by default
	}
}

// EffectiveThreadCount returns the actual number of threads that will be used.
func (c ParallelConfig) EffectiveThreadCount() int {
	if c.NumThreads <= 0 {
		return runtime.GOMAXPROCS(0)
	}
	return c.NumThreads
}

// Global parallel configuration - can be updated via PRAGMA statements.
var (
	globalParallelConfig     = DefaultParallelConfig()
	globalParallelConfigLock sync.RWMutex
)

// GetGlobalParallelConfig returns the current global parallel configuration.
func GetGlobalParallelConfig() ParallelConfig {
	globalParallelConfigLock.RLock()
	defer globalParallelConfigLock.RUnlock()
	return globalParallelConfig
}

// SetGlobalParallelConfig updates the global parallel configuration.
func SetGlobalParallelConfig(config ParallelConfig) {
	globalParallelConfigLock.Lock()
	defer globalParallelConfigLock.Unlock()
	globalParallelConfig = config
}

// HandlePragmaThreads handles the PRAGMA threads command.
// This updates the global parallel configuration.
func HandlePragmaThreads(value int) error {
	globalParallelConfigLock.Lock()
	defer globalParallelConfigLock.Unlock()

	if value < 0 {
		value = 0 // 0 means use GOMAXPROCS
	}

	// Cap at a reasonable maximum
	maxThreads := runtime.GOMAXPROCS(0) * 4
	if value > maxThreads {
		value = maxThreads
	}

	globalParallelConfig.NumThreads = value
	return nil
}

// GetPragmaThreads returns the current thread configuration.
func GetPragmaThreads() int {
	globalParallelConfigLock.RLock()
	defer globalParallelConfigLock.RUnlock()
	return globalParallelConfig.EffectiveThreadCount()
}

// ParallelExecutor wraps the parallel execution infrastructure.
// It provides parallel execution capabilities for physical plans.
type ParallelExecutor struct {
	// Pool is the thread pool for parallel execution.
	Pool *parallel.ThreadPool

	// Config holds the parallel execution configuration.
	Config ParallelConfig

	// compiler transforms physical plans into pipelines.
	compiler *parallel.PipelineCompiler

	// statistics tracks execution statistics.
	statistics *ParallelExecutionStats

	// mu protects mutable state.
	mu sync.RWMutex
}

// ParallelExecutionStats tracks parallel execution statistics.
type ParallelExecutionStats struct {
	// QueriesExecuted is the total number of queries executed.
	QueriesExecuted atomic.Int64

	// ParallelQueriesExecuted is the number of queries executed in parallel.
	ParallelQueriesExecuted atomic.Int64

	// SequentialQueriesExecuted is the number of queries executed sequentially.
	SequentialQueriesExecuted atomic.Int64

	// TotalRowsProcessed is the total number of rows processed.
	TotalRowsProcessed atomic.Int64

	// TotalTimeParallel is the total time spent in parallel execution (nanoseconds).
	TotalTimeParallel atomic.Int64

	// TotalTimeSequential is the total time spent in sequential execution (nanoseconds).
	TotalTimeSequential atomic.Int64
}

// NewParallelExecutor creates a new ParallelExecutor with the given configuration.
func NewParallelExecutor(config ParallelConfig) *ParallelExecutor {
	numWorkers := config.EffectiveThreadCount()

	var pool *parallel.ThreadPool
	if config.MemoryLimit > 0 {
		pool = parallel.NewThreadPoolWithLimit(numWorkers, config.MemoryLimit)
	} else {
		pool = parallel.NewThreadPool(numWorkers)
	}

	return &ParallelExecutor{
		Pool:       pool,
		Config:     config,
		compiler:   parallel.NewPipelineCompiler(numWorkers),
		statistics: &ParallelExecutionStats{},
	}
}

// NewParallelExecutorWithDefaults creates a new ParallelExecutor with default configuration.
func NewParallelExecutorWithDefaults() *ParallelExecutor {
	return NewParallelExecutor(GetGlobalParallelConfig())
}

// Execute runs a physical plan with parallel execution if beneficial.
// It automatically decides whether to use parallel or sequential execution
// based on cost analysis and configuration.
func (e *ParallelExecutor) Execute(
	ctx context.Context,
	plan planner.PhysicalPlan,
	storage *storage.Storage,
) (*storage.DataChunk, error) {
	return e.ExecuteWithPool(ctx, plan, storage, e.Pool)
}

// ExecuteWithPool uses a provided thread pool (for connection-level pooling).
// This allows different connections to share or have separate thread pools.
func (e *ParallelExecutor) ExecuteWithPool(
	ctx context.Context,
	plan planner.PhysicalPlan,
	stor *storage.Storage,
	pool *parallel.ThreadPool,
) (*storage.DataChunk, error) {
	e.statistics.QueriesExecuted.Add(1)

	// Check if we should parallelize this plan
	if !e.ShouldParallelize(plan, stor) {
		e.statistics.SequentialQueriesExecuted.Add(1)
		// Return nil to indicate sequential execution should be used
		return nil, nil
	}

	e.statistics.ParallelQueriesExecuted.Add(1)

	// Compile the plan into pipelines
	pipelines, err := e.compilePlan(plan, stor)
	if err != nil {
		return nil, err
	}

	if len(pipelines) == 0 {
		return nil, nil
	}

	// Execute pipelines
	executor := parallel.NewPipelineExecutor(pool, pipelines)
	if err := executor.Execute(ctx); err != nil {
		return nil, err
	}

	// Get final result
	return executor.GetFinalResult()
}

// ShouldParallelize determines if a plan should use parallel execution.
// It considers estimated cardinality, cost model, and configuration.
func (e *ParallelExecutor) ShouldParallelize(
	plan planner.PhysicalPlan,
	stor *storage.Storage,
) bool {
	e.mu.RLock()
	config := e.Config
	e.mu.RUnlock()

	// Check if parallelism is globally disabled
	if config.EffectiveThreadCount() <= 1 {
		return false
	}

	// Check based on plan type
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		if !config.EnableParallelScan {
			return false
		}
		// Check estimated row count
		if p.TableDef != nil && p.TableDef.Statistics != nil {
			if p.TableDef.Statistics.RowCount < int64(config.MinRowsForParallel) {
				return false
			}
		}
		// Check actual row count in storage
		if stor != nil {
			table, ok := stor.GetTable(p.TableName)
			if ok && table.RowCount() < int64(config.MinRowsForParallel) {
				return false
			}
		}
		return true

	case *planner.PhysicalHashJoin:
		if !config.EnableParallelJoin {
			return false
		}
		// Check if either side is large enough
		return e.ShouldParallelize(p.Left, stor) || e.ShouldParallelize(p.Right, stor)

	case *planner.PhysicalHashAggregate:
		if !config.EnableParallelAggregate {
			return false
		}
		return e.ShouldParallelize(p.Child, stor)

	case *planner.PhysicalSort:
		if !config.EnableParallelSort {
			return false
		}
		return e.ShouldParallelize(p.Child, stor)

	case *planner.PhysicalFilter:
		return e.ShouldParallelize(p.Child, stor)

	case *planner.PhysicalProject:
		return e.ShouldParallelize(p.Child, stor)

	case *planner.PhysicalLimit:
		return e.ShouldParallelize(p.Child, stor)

	case *planner.PhysicalDistinct:
		return e.ShouldParallelize(p.Child, stor)

	default:
		return false
	}
}

// EstimateParallelCost estimates the cost of parallel execution.
// This takes into account the number of workers and parallelization overhead.
func EstimateParallelCost(plan planner.PhysicalPlan, numWorkers int) float64 {
	seqCost := EstimateSequentialCost(plan)
	if numWorkers <= 1 {
		return seqCost
	}

	// Base parallel cost is sequential cost divided by workers
	baseCost := seqCost / float64(numWorkers)

	// Add parallelization overhead
	overhead := calculateParallelOverhead(plan, numWorkers)

	return baseCost + overhead
}

// EstimateSequentialCost estimates the cost of sequential execution.
func EstimateSequentialCost(plan planner.PhysicalPlan) float64 {
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		// Cost is proportional to row count
		rowCount := float64(1000) // Default estimate
		if p.TableDef != nil && p.TableDef.Statistics != nil {
			rowCount = float64(p.TableDef.Statistics.RowCount)
		}
		// Scanning cost: 0.1 per row
		return rowCount * 0.1

	case *planner.PhysicalHashJoin:
		leftCost := EstimateSequentialCost(p.Left)
		rightCost := EstimateSequentialCost(p.Right)
		// Join cost: sum of children plus hash table overhead
		return leftCost + rightCost*1.5 + leftCost*rightCost*0.001

	case *planner.PhysicalHashAggregate:
		childCost := EstimateSequentialCost(p.Child)
		// Aggregation cost: child cost plus aggregation overhead
		return childCost * 1.2

	case *planner.PhysicalSort:
		childCost := EstimateSequentialCost(p.Child)
		// Sort cost: n log n overhead
		return childCost * 1.5

	case *planner.PhysicalFilter:
		return EstimateSequentialCost(p.Child)

	case *planner.PhysicalProject:
		return EstimateSequentialCost(p.Child)

	case *planner.PhysicalLimit:
		return EstimateSequentialCost(p.Child)

	case *planner.PhysicalDistinct:
		return EstimateSequentialCost(p.Child) * 1.2

	default:
		return 10.0 // Default cost
	}
}

// calculateParallelOverhead calculates the overhead of parallelization.
func calculateParallelOverhead(plan planner.PhysicalPlan, numWorkers int) float64 {
	// Base overhead for thread coordination
	baseOverhead := float64(numWorkers) * 10.0

	switch plan.(type) {
	case *planner.PhysicalHashJoin:
		// Hash join has higher overhead due to partitioning
		return baseOverhead * 2.0

	case *planner.PhysicalHashAggregate:
		// Aggregation has merge overhead
		return baseOverhead * 1.5

	case *planner.PhysicalSort:
		// Sort has merge overhead
		return baseOverhead * 1.5

	default:
		return baseOverhead
	}
}

// compilePlan transforms a physical plan into executable pipelines.
func (e *ParallelExecutor) compilePlan(
	plan planner.PhysicalPlan,
	stor *storage.Storage,
) ([]*parallel.Pipeline, error) {
	// Create pipeline based on plan type
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		return e.compileScan(p, stor)

	case *planner.PhysicalHashJoin:
		return e.compileHashJoin(p, stor)

	case *planner.PhysicalHashAggregate:
		return e.compileAggregate(p, stor)

	case *planner.PhysicalSort:
		return e.compileSort(p, stor)

	case *planner.PhysicalFilter:
		// Filters are pushed down into scan or other operators
		return e.compilePlan(p.Child, stor)

	case *planner.PhysicalProject:
		// Projections are pushed down or applied as pipeline operators
		return e.compilePlan(p.Child, stor)

	default:
		// Unsupported plan type, return empty pipelines
		return nil, nil
	}
}

// compileScan compiles a table scan into a pipeline.
func (e *ParallelExecutor) compileScan(
	plan *planner.PhysicalScan,
	stor *storage.Storage,
) ([]*parallel.Pipeline, error) {
	if stor == nil {
		return nil, nil
	}

	table, ok := stor.GetTable(plan.TableName)
	if !ok {
		return nil, nil
	}

	// Create table data reader adapter
	reader := newStorageTableReader(table, plan.TableDef)

	// Create parallel scan source
	columns := make([]string, 0)
	columnTypes := make([]dukdb.Type, 0)
	if plan.TableDef != nil {
		for _, col := range plan.TableDef.Columns {
			columns = append(columns, col.Name)
			columnTypes = append(columnTypes, col.Type)
		}
	}

	scan := parallel.NewParallelTableScan(
		uint64(0), // Table OID
		plan.TableName,
		columns,
		columnTypes,
		reader,
	)

	// Set projections if specified
	if len(plan.Projections) > 0 {
		scan.SetProjections(plan.Projections)
	}

	// Create pipeline
	pipe := parallel.NewPipeline(0, "scan_"+plan.TableName)
	pipe.SetSource(&pipelineSourceAdapter{scan: scan})
	pipe.SetSink(parallel.NewChunkSink())

	return []*parallel.Pipeline{pipe}, nil
}

// compileHashJoin compiles a hash join into pipelines.
func (e *ParallelExecutor) compileHashJoin(
	plan *planner.PhysicalHashJoin,
	stor *storage.Storage,
) ([]*parallel.Pipeline, error) {
	// Build pipelines for both sides
	leftPipes, err := e.compilePlan(plan.Left, stor)
	if err != nil {
		return nil, err
	}

	rightPipes, err := e.compilePlan(plan.Right, stor)
	if err != nil {
		return nil, err
	}

	// Combine pipelines
	var pipelines []*parallel.Pipeline
	pipelines = append(pipelines, leftPipes...)
	pipelines = append(pipelines, rightPipes...)

	return pipelines, nil
}

// compileAggregate compiles an aggregate into pipelines.
func (e *ParallelExecutor) compileAggregate(
	plan *planner.PhysicalHashAggregate,
	stor *storage.Storage,
) ([]*parallel.Pipeline, error) {
	// Build child pipelines first
	childPipes, err := e.compilePlan(plan.Child, stor)
	if err != nil {
		return nil, err
	}

	return childPipes, nil
}

// compileSort compiles a sort into pipelines.
func (e *ParallelExecutor) compileSort(
	plan *planner.PhysicalSort,
	stor *storage.Storage,
) ([]*parallel.Pipeline, error) {
	// Build child pipelines first
	childPipes, err := e.compilePlan(plan.Child, stor)
	if err != nil {
		return nil, err
	}

	return childPipes, nil
}

// GetStatistics returns the parallel execution statistics.
func (e *ParallelExecutor) GetStatistics() ParallelExecutionStats {
	return ParallelExecutionStats{
		QueriesExecuted:           atomic.Int64{},
		ParallelQueriesExecuted:   atomic.Int64{},
		SequentialQueriesExecuted: atomic.Int64{},
		TotalRowsProcessed:        atomic.Int64{},
		TotalTimeParallel:         atomic.Int64{},
		TotalTimeSequential:       atomic.Int64{},
	}
}

// Shutdown shuts down the parallel executor and releases resources.
func (e *ParallelExecutor) Shutdown() {
	if e.Pool != nil {
		e.Pool.Shutdown()
	}
}

// UpdateConfig updates the parallel executor configuration.
func (e *ParallelExecutor) UpdateConfig(config ParallelConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()

	oldThreads := e.Config.EffectiveThreadCount()
	newThreads := config.EffectiveThreadCount()

	e.Config = config

	// Recreate pool if thread count changed
	if oldThreads != newThreads {
		if e.Pool != nil {
			e.Pool.Shutdown()
		}

		if config.MemoryLimit > 0 {
			e.Pool = parallel.NewThreadPoolWithLimit(newThreads, config.MemoryLimit)
		} else {
			e.Pool = parallel.NewThreadPool(newThreads)
		}

		e.compiler = parallel.NewPipelineCompiler(newThreads)
	}
}

// storageTableReader adapts storage.Table to parallel.TableDataReader.
type storageTableReader struct {
	table    *storage.Table
	tableDef interface {
		ColumnNames() []string
		ColumnTypes() []dukdb.Type
	}
}

func newStorageTableReader(table *storage.Table, tableDef interface {
	ColumnNames() []string
	ColumnTypes() []dukdb.Type
}) *storageTableReader {
	return &storageTableReader{
		table:    table,
		tableDef: tableDef,
	}
}

func (r *storageTableReader) ReadRowGroup(
	tableOID uint64,
	rowGroupID int,
	projections []int,
) (*storage.DataChunk, error) {
	if r.table == nil {
		return nil, nil
	}

	if rowGroupID < 0 || rowGroupID >= r.table.RowGroupCount() {
		return nil, nil
	}

	rg := r.table.GetRowGroup(rowGroupID)
	if rg == nil {
		return nil, nil
	}

	// Read all columns or projected columns
	types := r.tableDef.ColumnTypes()
	if len(projections) > 0 {
		projTypes := make([]dukdb.Type, len(projections))
		for i, idx := range projections {
			if idx >= 0 && idx < len(types) {
				projTypes[i] = types[idx]
			}
		}
		types = projTypes
	}

	rowCount := rg.Count()
	chunk := storage.NewDataChunkWithCapacity(types, rowCount)

	// Copy data from row group
	for row := 0; row < rowCount; row++ {
		var values []any
		if len(projections) > 0 {
			values = make([]any, len(projections))
			for i, idx := range projections {
				col := rg.GetColumn(idx)
				if col != nil {
					values[i] = col.GetValue(row)
				}
			}
		} else {
			numCols := len(types)
			values = make([]any, numCols)
			for col := 0; col < numCols; col++ {
				colVec := rg.GetColumn(col)
				if colVec != nil {
					values[col] = colVec.GetValue(row)
				}
			}
		}
		chunk.AppendRow(values)
	}

	return chunk, nil
}

func (r *storageTableReader) GetRowGroupMeta(tableOID uint64) ([]parallel.RowGroupMeta, error) {
	if r.table == nil {
		return nil, nil
	}

	numRowGroups := r.table.RowGroupCount()
	rowGroups := make([]parallel.RowGroupMeta, numRowGroups)

	var startRow uint64
	for i := 0; i < numRowGroups; i++ {
		rg := r.table.GetRowGroup(i)
		if rg == nil {
			continue
		}
		rowCount := uint64(rg.Count())
		rowGroups[i] = parallel.RowGroupMeta{
			ID:       i,
			StartRow: startRow,
			RowCount: rowCount,
		}
		startRow += rowCount
	}

	return rowGroups, nil
}

func (r *storageTableReader) GetColumnTypes(tableOID uint64) ([]dukdb.Type, error) {
	if r.tableDef == nil {
		return nil, nil
	}
	return r.tableDef.ColumnTypes(), nil
}

func (r *storageTableReader) GetColumnNames(tableOID uint64) ([]string, error) {
	if r.tableDef == nil {
		return nil, nil
	}
	return r.tableDef.ColumnNames(), nil
}

// pipelineSourceAdapter adapts ParallelTableScan to parallel.PipelineSource.
type pipelineSourceAdapter struct {
	scan *parallel.ParallelTableScan
}

func (a *pipelineSourceAdapter) GenerateMorsels() []parallel.Morsel {
	return a.scan.GenerateMorsels()
}

func (a *pipelineSourceAdapter) Scan(morsel parallel.Morsel) (*storage.DataChunk, error) {
	return a.scan.Scan(morsel)
}

func (a *pipelineSourceAdapter) Schema() []parallel.ColumnDef {
	names := a.scan.ProjectedColumnNames()
	types := a.scan.ProjectedColumnTypes()

	schema := make([]parallel.ColumnDef, len(names))
	for i := range names {
		schema[i] = parallel.ColumnDef{
			Name: names[i],
			Type: types[i],
		}
	}
	return schema
}

// ParallelExplainAnnotator adds parallel execution annotations to EXPLAIN output.
// It analyzes a physical plan and adds [PARALLEL] tags to operators that would
// be executed in parallel, along with worker counts and cost estimates.
type ParallelExplainAnnotator struct {
	Config  ParallelConfig
	Storage *storage.Storage
}

// NewParallelExplainAnnotator creates a new ParallelExplainAnnotator.
func NewParallelExplainAnnotator(
	config ParallelConfig,
	stor *storage.Storage,
) *ParallelExplainAnnotator {
	return &ParallelExplainAnnotator{
		Config:  config,
		Storage: stor,
	}
}

// Annotate adds parallel annotations to a physical plan's EXPLAIN output.
// It returns a formatted string with parallel execution information.
func (a *ParallelExplainAnnotator) Annotate(plan planner.PhysicalPlan) string {
	var sb strings.Builder
	a.annotatePlan(&sb, plan, 0)
	return sb.String()
}

// annotatePlan recursively annotates a plan tree.
func (a *ParallelExplainAnnotator) annotatePlan(
	sb *strings.Builder,
	plan planner.PhysicalPlan,
	indent int,
) {
	prefix := strings.Repeat("  ", indent)

	// Check if this plan would be parallelized
	isParallel, numWorkers := a.wouldParallelize(plan)

	// Get cost estimates
	seqCost := EstimateSequentialCost(plan)
	parCost := EstimateParallelCost(plan, numWorkers)

	// Build annotation
	switch p := plan.(type) {
	case *planner.PhysicalScan:
		sb.WriteString(fmt.Sprintf("%s", prefix))
		if isParallel {
			sb.WriteString(fmt.Sprintf("[PARALLEL workers=%d] ", numWorkers))
		}
		sb.WriteString(fmt.Sprintf("Scan: %s", p.TableName))
		if p.Alias != "" && p.Alias != p.TableName {
			sb.WriteString(fmt.Sprintf(" AS %s", p.Alias))
		}
		sb.WriteString(fmt.Sprintf(" (seq_cost=%.2f", seqCost))
		if isParallel {
			sb.WriteString(fmt.Sprintf(" par_cost=%.2f", parCost))
		}
		sb.WriteString(")")

	case *planner.PhysicalHashJoin:
		sb.WriteString(fmt.Sprintf("%s", prefix))
		if isParallel {
			sb.WriteString(fmt.Sprintf("[PARALLEL workers=%d] ", numWorkers))
		}
		sb.WriteString(fmt.Sprintf("HashJoin (seq_cost=%.2f", seqCost))
		if isParallel {
			sb.WriteString(fmt.Sprintf(" par_cost=%.2f", parCost))
		}
		sb.WriteString(")\n")
		a.annotatePlan(sb, p.Left, indent+1)
		sb.WriteString("\n")
		a.annotatePlan(sb, p.Right, indent+1)

	case *planner.PhysicalNestedLoopJoin:
		sb.WriteString(fmt.Sprintf("%sNestedLoopJoin (seq_cost=%.2f)\n", prefix, seqCost))
		a.annotatePlan(sb, p.Left, indent+1)
		sb.WriteString("\n")
		a.annotatePlan(sb, p.Right, indent+1)

	case *planner.PhysicalHashAggregate:
		sb.WriteString(fmt.Sprintf("%s", prefix))
		if isParallel {
			sb.WriteString(fmt.Sprintf("[PARALLEL workers=%d] ", numWorkers))
		}
		sb.WriteString(fmt.Sprintf("HashAggregate (seq_cost=%.2f", seqCost))
		if isParallel {
			sb.WriteString(fmt.Sprintf(" par_cost=%.2f", parCost))
		}
		sb.WriteString(")\n")
		a.annotatePlan(sb, p.Child, indent+1)

	case *planner.PhysicalSort:
		sb.WriteString(fmt.Sprintf("%s", prefix))
		if isParallel {
			sb.WriteString(fmt.Sprintf("[PARALLEL workers=%d] ", numWorkers))
		}
		sb.WriteString(fmt.Sprintf("Sort (seq_cost=%.2f", seqCost))
		if isParallel {
			sb.WriteString(fmt.Sprintf(" par_cost=%.2f", parCost))
		}
		sb.WriteString(")\n")
		a.annotatePlan(sb, p.Child, indent+1)

	case *planner.PhysicalFilter:
		sb.WriteString(fmt.Sprintf("%sFilter (seq_cost=%.2f)\n", prefix, seqCost))
		a.annotatePlan(sb, p.Child, indent+1)

	case *planner.PhysicalProject:
		sb.WriteString(fmt.Sprintf("%sProject: %d columns (seq_cost=%.2f)\n", prefix, len(p.Expressions), seqCost))
		a.annotatePlan(sb, p.Child, indent+1)

	case *planner.PhysicalLimit:
		sb.WriteString(fmt.Sprintf("%sLimit (seq_cost=%.2f)\n", prefix, seqCost))
		a.annotatePlan(sb, p.Child, indent+1)

	case *planner.PhysicalDistinct:
		sb.WriteString(fmt.Sprintf("%sDistinct (seq_cost=%.2f)\n", prefix, seqCost))
		a.annotatePlan(sb, p.Child, indent+1)

	case *planner.PhysicalDummyScan:
		sb.WriteString(fmt.Sprintf("%sDummyScan (seq_cost=%.2f)", prefix, seqCost))

	case *planner.PhysicalTableFunctionScan:
		sb.WriteString(fmt.Sprintf("%sTableFunction: %s (seq_cost=%.2f)", prefix, p.FunctionName, seqCost))

	case *planner.PhysicalWindow:
		sb.WriteString(fmt.Sprintf("%sWindow (seq_cost=%.2f)\n", prefix, seqCost))
		a.annotatePlan(sb, p.Child, indent+1)

	default:
		sb.WriteString(fmt.Sprintf("%s%T (seq_cost=%.2f)", prefix, plan, seqCost))
	}
}

// wouldParallelize determines if a plan node would be parallelized and how many workers would be used.
func (a *ParallelExplainAnnotator) wouldParallelize(plan planner.PhysicalPlan) (bool, int) {
	// Check if parallelism is globally disabled
	numWorkers := a.Config.EffectiveThreadCount()
	if numWorkers <= 1 {
		return false, 1
	}

	switch p := plan.(type) {
	case *planner.PhysicalScan:
		if !a.Config.EnableParallelScan {
			return false, 1
		}
		// Check estimated row count
		if p.TableDef != nil && p.TableDef.Statistics != nil {
			if p.TableDef.Statistics.RowCount < int64(a.Config.MinRowsForParallel) {
				return false, 1
			}
		}
		// Check actual row count in storage
		if a.Storage != nil {
			table, ok := a.Storage.GetTable(p.TableName)
			if ok && table.RowCount() < int64(a.Config.MinRowsForParallel) {
				return false, 1
			}
		}
		return true, numWorkers

	case *planner.PhysicalHashJoin:
		if !a.Config.EnableParallelJoin {
			return false, 1
		}
		// Check if either side is large enough
		leftParallel, _ := a.wouldParallelize(p.Left)
		rightParallel, _ := a.wouldParallelize(p.Right)
		if leftParallel || rightParallel {
			return true, numWorkers
		}
		return false, 1

	case *planner.PhysicalHashAggregate:
		if !a.Config.EnableParallelAggregate {
			return false, 1
		}
		childParallel, _ := a.wouldParallelize(p.Child)
		return childParallel, numWorkers

	case *planner.PhysicalSort:
		if !a.Config.EnableParallelSort {
			return false, 1
		}
		childParallel, _ := a.wouldParallelize(p.Child)
		return childParallel, numWorkers

	default:
		return false, 1
	}
}

// GetParallelExplainOutput returns the EXPLAIN output with parallel annotations.
// This is a helper function that can be called from the executor.
func GetParallelExplainOutput(plan planner.PhysicalPlan, stor *storage.Storage) string {
	config := GetGlobalParallelConfig()
	annotator := NewParallelExplainAnnotator(config, stor)
	return annotator.Annotate(plan)
}

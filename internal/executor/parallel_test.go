// Package executor provides query execution for the native Go DuckDB implementation.
// This file contains tests for parallel execution integration.
package executor

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dukdb "github.com/dukdb/dukdb-go"
	"github.com/dukdb/dukdb-go/internal/catalog"
	"github.com/dukdb/dukdb-go/internal/optimizer"
	"github.com/dukdb/dukdb-go/internal/planner"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// TestDefaultParallelConfig tests the default parallel configuration.
func TestDefaultParallelConfig(t *testing.T) {
	config := DefaultParallelConfig()

	assert.Equal(t, DefaultMaxParallelWorkers, config.NumThreads)
	assert.True(t, config.EnableParallelScan)
	assert.True(t, config.EnableParallelJoin)
	assert.True(t, config.EnableParallelAggregate)
	assert.True(t, config.EnableParallelSort)
	assert.Equal(t, MinRowsForParallel, config.MinRowsForParallel)
	assert.Equal(t, int64(0), config.MemoryLimit)
}

// TestEffectiveThreadCount tests the effective thread count calculation.
func TestEffectiveThreadCount(t *testing.T) {
	// Test default (0) returns GOMAXPROCS
	config := ParallelConfig{NumThreads: 0}
	assert.Equal(t, runtime.GOMAXPROCS(0), config.EffectiveThreadCount())

	// Test negative returns GOMAXPROCS
	config = ParallelConfig{NumThreads: -1}
	assert.Equal(t, runtime.GOMAXPROCS(0), config.EffectiveThreadCount())

	// Test specific value
	config = ParallelConfig{NumThreads: 4}
	assert.Equal(t, 4, config.EffectiveThreadCount())
}

// TestGlobalParallelConfig tests global parallel config get/set.
func TestGlobalParallelConfig(t *testing.T) {
	// Save original config
	originalConfig := GetGlobalParallelConfig()
	defer SetGlobalParallelConfig(originalConfig)

	// Test setting new config
	newConfig := ParallelConfig{
		NumThreads:              8,
		EnableParallelScan:      false,
		EnableParallelJoin:      true,
		EnableParallelAggregate: false,
		EnableParallelSort:      true,
		MinRowsForParallel:      5000,
		MemoryLimit:             1024 * 1024 * 100, // 100MB
	}

	SetGlobalParallelConfig(newConfig)
	retrieved := GetGlobalParallelConfig()

	assert.Equal(t, 8, retrieved.NumThreads)
	assert.False(t, retrieved.EnableParallelScan)
	assert.True(t, retrieved.EnableParallelJoin)
	assert.False(t, retrieved.EnableParallelAggregate)
	assert.True(t, retrieved.EnableParallelSort)
	assert.Equal(t, 5000, retrieved.MinRowsForParallel)
	assert.Equal(t, int64(1024*1024*100), retrieved.MemoryLimit)
}

// TestHandlePragmaThreads tests PRAGMA threads handling.
func TestHandlePragmaThreads(t *testing.T) {
	// Save original config
	originalConfig := GetGlobalParallelConfig()
	defer SetGlobalParallelConfig(originalConfig)

	// Test setting valid thread count
	err := HandlePragmaThreads(4)
	require.NoError(t, err)
	assert.Equal(t, 4, GetPragmaThreads())

	// Test setting 0 (GOMAXPROCS)
	err = HandlePragmaThreads(0)
	require.NoError(t, err)
	assert.Equal(t, runtime.GOMAXPROCS(0), GetPragmaThreads())

	// Test setting negative (should become 0)
	err = HandlePragmaThreads(-5)
	require.NoError(t, err)
	assert.Equal(t, runtime.GOMAXPROCS(0), GetPragmaThreads())
}

// TestNewParallelExecutor tests parallel executor creation.
func TestNewParallelExecutor(t *testing.T) {
	config := DefaultParallelConfig()
	config.NumThreads = 4

	executor := NewParallelExecutor(config)
	require.NotNil(t, executor)
	defer executor.Shutdown()

	assert.NotNil(t, executor.Pool)
	assert.Equal(t, 4, executor.Pool.NumWorkers)
	assert.Equal(t, config, executor.Config)
}

// TestNewParallelExecutorWithDefaults tests default executor creation.
func TestNewParallelExecutorWithDefaults(t *testing.T) {
	executor := NewParallelExecutorWithDefaults()
	require.NotNil(t, executor)
	defer executor.Shutdown()

	assert.NotNil(t, executor.Pool)
}

// TestParallelExecutorUpdateConfig tests configuration updates.
func TestParallelExecutorUpdateConfig(t *testing.T) {
	config := DefaultParallelConfig()
	config.NumThreads = 2

	executor := NewParallelExecutor(config)
	require.NotNil(t, executor)
	defer executor.Shutdown()

	assert.Equal(t, 2, executor.Pool.NumWorkers)

	// Update to 4 threads
	newConfig := config
	newConfig.NumThreads = 4
	executor.UpdateConfig(newConfig)

	assert.Equal(t, 4, executor.Pool.NumWorkers)
}

// createTestTableDef creates a TableDef for testing with the specified row count.
func createTestTableDef(name string, rowCount int64, columns ...*catalog.ColumnDef) *catalog.TableDef {
	if len(columns) == 0 {
		columns = []*catalog.ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER},
		}
	}
	return &catalog.TableDef{
		Name:    name,
		Columns: columns,
		Statistics: &optimizer.TableStatistics{
			RowCount: rowCount,
		},
	}
}

// TestShouldParallelize tests parallelization decision logic.
func TestShouldParallelize(t *testing.T) {
	// Create a test table definition with enough rows
	tableDef := createTestTableDef("test_table", 100000,
		&catalog.ColumnDef{Name: "id", Type: dukdb.TYPE_INTEGER},
		&catalog.ColumnDef{Name: "value", Type: dukdb.TYPE_VARCHAR},
	)

	// Create small table definition
	smallTableDef := createTestTableDef("small_table", 100)

	config := DefaultParallelConfig()
	config.NumThreads = 4
	executor := NewParallelExecutor(config)
	defer executor.Shutdown()

	// Test: Large table scan should parallelize
	largeScan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	assert.True(t, executor.ShouldParallelize(largeScan, nil))

	// Test: Small table scan should not parallelize
	smallScan := &planner.PhysicalScan{
		TableName: "small_table",
		TableDef:  smallTableDef,
	}
	assert.False(t, executor.ShouldParallelize(smallScan, nil))

	// Test: With parallel scan disabled
	noScanConfig := config
	noScanConfig.EnableParallelScan = false
	executor.UpdateConfig(noScanConfig)
	assert.False(t, executor.ShouldParallelize(largeScan, nil))

	// Restore config
	executor.UpdateConfig(config)

	// Test: Single thread should not parallelize
	singleThreadConfig := config
	singleThreadConfig.NumThreads = 1
	executor.UpdateConfig(singleThreadConfig)
	assert.False(t, executor.ShouldParallelize(largeScan, nil))
}

// TestEstimateSequentialCost tests cost estimation.
func TestEstimateSequentialCost(t *testing.T) {
	tableDef := createTestTableDef("test_table", 10000)

	// Test scan cost
	scan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	scanCost := EstimateSequentialCost(scan)
	assert.Greater(t, scanCost, 0.0)
	// 10000 rows * 0.1 = 1000
	assert.Equal(t, 1000.0, scanCost)

	// Test filter cost (should be same as child)
	filter := &planner.PhysicalFilter{
		Child: scan,
	}
	filterCost := EstimateSequentialCost(filter)
	assert.Equal(t, scanCost, filterCost)

	// Test hash aggregate cost (should be higher than child)
	aggregate := &planner.PhysicalHashAggregate{
		Child: scan,
	}
	aggCost := EstimateSequentialCost(aggregate)
	assert.Greater(t, aggCost, scanCost)

	// Test sort cost (should be higher than child)
	sort := &planner.PhysicalSort{
		Child: scan,
	}
	sortCost := EstimateSequentialCost(sort)
	assert.Greater(t, sortCost, scanCost)
}

// TestEstimateParallelCost tests parallel cost estimation.
func TestEstimateParallelCost(t *testing.T) {
	tableDef := createTestTableDef("test_table", 10000)

	scan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}

	seqCost := EstimateSequentialCost(scan)

	// Test: Single worker should have same cost as sequential
	singleCost := EstimateParallelCost(scan, 1)
	assert.Equal(t, seqCost, singleCost)

	// Test: Multiple workers should have lower cost than sequential
	parCost := EstimateParallelCost(scan, 4)
	assert.Less(t, parCost, seqCost)

	// Test: More workers should generally mean lower cost
	moreCost := EstimateParallelCost(scan, 8)
	assert.Less(t, moreCost, parCost)
}

// TestParallelExplainAnnotator tests EXPLAIN with parallel annotations.
func TestParallelExplainAnnotator(t *testing.T) {
	tableDef := createTestTableDef("test_table", 100000)

	config := DefaultParallelConfig()
	config.NumThreads = 4

	annotator := NewParallelExplainAnnotator(config, nil)

	// Test scan annotation
	scan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	output := annotator.Annotate(scan)
	assert.Contains(t, output, "[PARALLEL workers=4]")
	assert.Contains(t, output, "Scan: test_table")
	assert.Contains(t, output, "seq_cost=")
	assert.Contains(t, output, "par_cost=")

	// Test small table (should not be parallel)
	smallTableDef := createTestTableDef("small_table", 100)
	smallScan := &planner.PhysicalScan{
		TableName: "small_table",
		TableDef:  smallTableDef,
	}
	smallOutput := annotator.Annotate(smallScan)
	assert.NotContains(t, smallOutput, "[PARALLEL")
}

// TestParallelExplainAnnotatorWithDisabledParallel tests EXPLAIN without parallelism.
func TestParallelExplainAnnotatorWithDisabledParallel(t *testing.T) {
	tableDef := createTestTableDef("test_table", 100000)

	config := DefaultParallelConfig()
	config.NumThreads = 1 // Single thread disables parallelism

	annotator := NewParallelExplainAnnotator(config, nil)

	scan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}
	output := annotator.Annotate(scan)
	assert.NotContains(t, output, "[PARALLEL")
	assert.Contains(t, output, "seq_cost=")
	assert.NotContains(t, output, "par_cost=")
}

// TestParallelExplainAnnotatorHashJoin tests EXPLAIN for hash joins.
func TestParallelExplainAnnotatorHashJoin(t *testing.T) {
	leftTableDef := createTestTableDef("left_table", 100000)
	rightTableDef := createTestTableDef("right_table", 50000)

	config := DefaultParallelConfig()
	config.NumThreads = 4

	annotator := NewParallelExplainAnnotator(config, nil)

	join := &planner.PhysicalHashJoin{
		Left: &planner.PhysicalScan{
			TableName: "left_table",
			TableDef:  leftTableDef,
		},
		Right: &planner.PhysicalScan{
			TableName: "right_table",
			TableDef:  rightTableDef,
		},
	}

	output := annotator.Annotate(join)
	assert.Contains(t, output, "HashJoin")
	assert.Contains(t, output, "[PARALLEL workers=4]")
}

// TestParallelExecutorExecute tests the Execute method.
func TestParallelExecutorExecute(t *testing.T) {
	config := DefaultParallelConfig()
	config.NumThreads = 2

	executor := NewParallelExecutor(config)
	require.NotNil(t, executor)
	defer executor.Shutdown()

	ctx := context.Background()

	// Create a simple scan plan for a table that doesn't exist in storage
	// This should return nil (indicating sequential execution should be used)
	tableDef := createTestTableDef("nonexistent", 100)
	plan := &planner.PhysicalScan{
		TableName: "nonexistent",
		TableDef:  tableDef,
	}

	// Should not parallelize due to small row count
	result, err := executor.Execute(ctx, plan, nil)
	require.NoError(t, err)
	assert.Nil(t, result) // Indicates sequential execution
}

// TestParallelExecutorShutdown tests proper shutdown.
func TestParallelExecutorShutdown(t *testing.T) {
	config := DefaultParallelConfig()
	config.NumThreads = 2

	executor := NewParallelExecutor(config)
	require.NotNil(t, executor)

	// Shutdown should not panic
	executor.Shutdown()

	// Pool should be shut down
	assert.True(t, executor.Pool.IsShutdown())
}

// TestStorageTableReader tests the storage adapter.
func TestStorageTableReader(t *testing.T) {
	// Create a mock table definition
	tableDef := &catalog.TableDef{
		Name: "test_table",
		Columns: []*catalog.ColumnDef{
			{Name: "id", Type: dukdb.TYPE_INTEGER},
			{Name: "name", Type: dukdb.TYPE_VARCHAR},
		},
	}

	// Create storage and table
	stor := storage.NewStorage()
	colTypes := tableDef.ColumnTypes()
	_, err := stor.CreateTable("test_table", colTypes)
	require.NoError(t, err)

	table, ok := stor.GetTable("test_table")
	require.True(t, ok)

	// Add some data
	chunk := storage.NewDataChunkWithCapacity(colTypes, 100)
	for i := 0; i < 100; i++ {
		chunk.AppendRow([]any{int32(i), "value"})
	}
	err = table.AppendChunk(chunk)
	require.NoError(t, err)

	// Create reader
	reader := newStorageTableReader(table, tableDef)
	require.NotNil(t, reader)

	// Test GetRowGroupMeta
	rowGroups, err := reader.GetRowGroupMeta(0)
	require.NoError(t, err)
	assert.Greater(t, len(rowGroups), 0)

	// Test GetColumnTypes
	types, err := reader.GetColumnTypes(0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(types))
	assert.Equal(t, dukdb.TYPE_INTEGER, types[0])
	assert.Equal(t, dukdb.TYPE_VARCHAR, types[1])

	// Test GetColumnNames
	names, err := reader.GetColumnNames(0)
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name"}, names)

	// Test ReadRowGroup
	if len(rowGroups) > 0 {
		readChunk, err := reader.ReadRowGroup(0, 0, nil)
		require.NoError(t, err)
		assert.NotNil(t, readChunk)
		assert.Greater(t, readChunk.Count(), 0)
	}
}

// TestGetParallelExplainOutput tests the helper function.
func TestGetParallelExplainOutput(t *testing.T) {
	tableDef := createTestTableDef("test_table", 100000)

	plan := &planner.PhysicalScan{
		TableName: "test_table",
		TableDef:  tableDef,
	}

	output := GetParallelExplainOutput(plan, nil)
	assert.NotEmpty(t, output)
	assert.Contains(t, output, "Scan: test_table")
}

// TestParallelConfigConcurrency tests thread-safe config access.
func TestParallelConfigConcurrency(t *testing.T) {
	// Save original config
	originalConfig := GetGlobalParallelConfig()
	defer SetGlobalParallelConfig(originalConfig)

	// Run concurrent reads and writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				if j%2 == 0 {
					_ = GetGlobalParallelConfig()
				} else {
					config := DefaultParallelConfig()
					config.NumThreads = id
					SetGlobalParallelConfig(config)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

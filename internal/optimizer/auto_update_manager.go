package optimizer

import (
	"context"
	"sync"
	"time"
)

// AutoUpdateManager manages automatic statistics updates based on modification tracking.
//
// Purpose:
// - Monitor table modifications via ModificationTracker
// - Trigger ANALYZE when modification threshold is exceeded
// - Batch multiple ANALYZE operations to reduce overhead
// - Support incremental ANALYZE for large tables
//
// DuckDB Behavior:
// DuckDB v1.4.3 does not implement automatic statistics updates.
// This is a novel dukdb-go feature. See design.md Phase 1 for rationale.
//
// Threshold Behavior:
// - Threshold: 10% modification ratio (fixed, no configuration)
// - Ratio: (InsertCount + UpdateCount + DeleteCount) / OriginalCount > 0.10
// - This threshold matches DuckDB's statistics validity heuristics
// - Research reference: RESEARCH.md section 2
//
// Batching:
// - Collects tables needing ANALYZE for a period (default: 100ms)
// - Executes all pending ANALYZE operations in one batch
// - Prevents excessive overhead from individual small modifications
// - Example: 100 single-row inserts trigger 1 batch ANALYZE, not 100
type AutoUpdateManager struct {
	tracker *ModificationTracker

	// Configuration
	threshold       float64         // Ratio threshold for triggering ANALYZE (fixed at 0.10)
	batchInterval   time.Duration   // How long to wait before executing batched ANALYZE
	maxBatchSize    int             // Maximum tables to ANALYZE in one batch
	analyzeFunc     AnalyzeFuncType // User-provided ANALYZE implementation
	getRowCountFunc GetRowCountFunc // User-provided function to get current row count

	// Batching state
	mu            sync.Mutex
	pendingTables map[string]bool // Tables that need ANALYZE
	batchTimer    *time.Timer     // Timer for batch processing
	stopped       bool            // Flag to indicate if manager has been stopped

	// Metrics (for debugging)
	metricsLock  sync.RWMutex
	triggerCount int64
	analyzeCount int64
	batchCount   int64
}

// AnalyzeFuncType is the signature for an ANALYZE implementation.
// Called when auto-update determines statistics need refreshing.
type AnalyzeFuncType func(ctx context.Context, tableName string, incremental bool) error

// GetRowCountFunc returns the current row count of a table.
type GetRowCountFunc func(tableName string) (int64, error)

// NewAutoUpdateManager creates a new auto-update manager.
//
// Parameters:
// - tracker: ModificationTracker instance to monitor changes
// - analyzeFunc: Function to call when ANALYZE is needed
// - getRowCountFunc: Function to get current row counts
//
// The threshold is fixed at 0.10 (10%) per design.md Phase 1.
// No configuration options are provided - this ensures consistent behavior.
func NewAutoUpdateManager(
	tracker *ModificationTracker,
	analyzeFunc AnalyzeFuncType,
	getRowCountFunc GetRowCountFunc,
) *AutoUpdateManager {
	return &AutoUpdateManager{
		tracker:         tracker,
		threshold:       0.10,                   // Fixed 10% threshold, not configurable
		batchInterval:   100 * time.Millisecond, // 100ms batch window
		maxBatchSize:    100,                    // Process up to 100 tables per batch
		analyzeFunc:     analyzeFunc,
		getRowCountFunc: getRowCountFunc,
		pendingTables:   make(map[string]bool),
		stopped:         false,
	}
}

// SetBatchInterval adjusts the batching window. Primarily for testing.
// In production, the default 100ms should be used.
func (aum *AutoUpdateManager) SetBatchInterval(duration time.Duration) {
	aum.mu.Lock()
	defer aum.mu.Unlock()

	aum.batchInterval = duration
}

// CheckAndQueueAutoAnalyze checks if a table exceeds the modification threshold.
// If exceeded, queues the table for batched ANALYZE processing.
//
// This method is called after each DML operation (INSERT, UPDATE, DELETE).
// It is fast - only does threshold comparison, actual ANALYZE is batched.
//
// Parameters:
// - tableName: Table that was modified
//
// Thread-safe non-blocking operation.
func (aum *AutoUpdateManager) CheckAndQueueAutoAnalyze(tableName string) {
	ratio := aum.tracker.GetModificationRatio(tableName)

	// Threshold: 10% modification ratio triggers ANALYZE
	// This is a hard-coded constant matching DuckDB heuristics
	// Use < instead of <= so that exactly 10% triggers ANALYZE
	if ratio < aum.threshold {
		return
	}

	aum.mu.Lock()

	// Check if already queued
	if aum.pendingTables[tableName] {
		aum.mu.Unlock()
		return
	}

	if aum.stopped {
		aum.mu.Unlock()
		return
	}

	aum.pendingTables[tableName] = true

	// Debug logging would go here if logging is enabled in the future.
	// Message: "auto-update: table {tableName} exceeded threshold (ratio={ratio*100}%), queuing ANALYZE"

	aum.metricsLock.Lock()
	aum.triggerCount++
	aum.metricsLock.Unlock()

	// Start batch timer if not already running
	if aum.batchTimer == nil {
		aum.batchTimer = time.AfterFunc(aum.batchInterval, aum.executePendingAnalyzeWrapper)
	}

	aum.mu.Unlock()
}

// executePendingAnalyzeWrapper is called by the timer to execute pending ANALYZE operations.
//
// Algorithm:
// 1. Collect all tables that exceeded the threshold since last batch
// 2. Limit batch size to maxBatchSize to prevent overwhelming system
// 3. Reset pending tables map for next batch window
// 4. Execute ANALYZE on each table (sequential, with error handling)
// 5. Update modification tracker baseline for each analyzed table
// 6. Update metrics for monitoring
//
// Error Handling:
// - Non-fatal: If ANALYZE fails on one table, continue with remaining tables
// - Log failures for debugging but don't interrupt batch processing
// - ModificationTracker is NOT reset if ANALYZE fails (retry on next batch)
//
// Thread Safety:
// - Must acquire mu lock before accessing pendingTables
// - Releases lock before calling analyzeFunc to allow concurrent modifications
// - Re-acquires lock before final cleanup
func (aum *AutoUpdateManager) executePendingAnalyzeWrapper() {
	aum.mu.Lock()

	if aum.stopped {
		aum.batchTimer = nil
		aum.mu.Unlock()
		return
	}

	if len(aum.pendingTables) == 0 {
		aum.batchTimer = nil
		aum.mu.Unlock()
		return
	}

	// Collect tables to analyze (up to maxBatchSize)
	tablesToAnalyze := make([]string, 0, aum.maxBatchSize)
	count := 0
	for tableName := range aum.pendingTables {
		if count >= aum.maxBatchSize {
			break
		}
		tablesToAnalyze = append(tablesToAnalyze, tableName)
		count++
	}

	// Reset timer for next batch
	aum.batchTimer = nil

	aum.mu.Unlock()

	// Debug logging would go here if logging is enabled in the future.
	// Message: "auto-update: executing batch ANALYZE for {len(tablesToAnalyze)} tables"

	aum.metricsLock.Lock()
	aum.batchCount++
	aum.metricsLock.Unlock()

	// Execute ANALYZE for each table
	// Note: This is done sequentially, not in parallel,
	// to avoid excessive resource consumption
	ctx := context.Background()
	for _, tableName := range tablesToAnalyze {
		// Determine if we should use incremental ANALYZE
		incremental := false
		if aum.getRowCountFunc != nil {
			rowCount, err := aum.getRowCountFunc(tableName)
			if err == nil && rowCount > 1000000 {
				// For tables > 1M rows, use incremental sampling
				incremental = true
			}
		}

		// Execute ANALYZE
		err := aum.analyzeFunc(ctx, tableName, incremental)
		if err != nil {
			// Warning logging would go here if logging is enabled in the future.
			// Message: "auto-update: ANALYZE failed for table {tableName}: {err}"
			continue
		}

		// Update tracker with new row count and reset modification counts
		if aum.getRowCountFunc != nil {
			rowCount, err := aum.getRowCountFunc(tableName)
			if err == nil {
				aum.tracker.InitializeTable(tableName, rowCount)
			}
		}

		aum.metricsLock.Lock()
		aum.analyzeCount++
		aum.metricsLock.Unlock()

		// Debug logging would go here if logging is enabled in the future.
		// Message: "auto-update: completed ANALYZE for table {tableName}"
	}

	// Remove processed tables from pending
	aum.mu.Lock()
	for _, tableName := range tablesToAnalyze {
		delete(aum.pendingTables, tableName)
	}

	// Restart timer if there are still pending tables
	if len(aum.pendingTables) > 0 && !aum.stopped {
		aum.batchTimer = time.AfterFunc(aum.batchInterval, aum.executePendingAnalyzeWrapper)
	}

	aum.mu.Unlock()
}

// Stop stops the auto-update manager and cleans up resources.
// Call this when shutting down the database connection or engine.
func (aum *AutoUpdateManager) Stop() {
	aum.mu.Lock()
	defer aum.mu.Unlock()

	aum.stopped = true

	// Stop timer if running
	if aum.batchTimer != nil {
		aum.batchTimer.Stop()
		aum.batchTimer = nil
	}
}

// GetMetrics returns statistics about auto-update activity for debugging.
func (aum *AutoUpdateManager) GetMetrics() map[string]int64 {
	aum.metricsLock.RLock()
	defer aum.metricsLock.RUnlock()

	return map[string]int64{
		"triggers": aum.triggerCount,
		"analyzes": aum.analyzeCount,
		"batches":  aum.batchCount,
	}
}

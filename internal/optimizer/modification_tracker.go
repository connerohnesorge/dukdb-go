package optimizer

import (
	"sync"
	"time"
)

// ModificationTracker tracks DML operations (INSERT, UPDATE, DELETE) on tables
// to determine when statistics need to be automatically updated.
//
// Purpose:
// - Monitor how much a table has changed since last ANALYZE
// - Enable automatic ANALYZE invocation when changes exceed threshold
// - Support batching of ANALYZE operations to reduce overhead
//
// Design Pattern:
// - Thread-safe: Protected by sync.RWMutex for concurrent access
// - Per-table tracking: Each table maintains independent modification counts
// - Threshold-based: Auto-update triggered when (inserted + updated + deleted) / original_count > 0.10
//
// DuckDB Note:
// DuckDB v1.4.3 does not implement automatic statistics update.
// This is a novel dukdb-go feature providing automatic adaptation.
// See: RESEARCH.md section 2 for DuckDB auto-update research findings.
type ModificationTracker struct {
	mu     sync.RWMutex
	tables map[string]*TableModification
}

// TableModification tracks the number of modifications for a single table
// since the last time statistics were collected.
type TableModification struct {
	// InsertCount tracks the number of rows inserted since last ANALYZE
	InsertCount int64

	// UpdateCount tracks the number of rows updated since last ANALYZE
	UpdateCount int64

	// DeleteCount tracks the number of rows deleted since last ANALYZE
	DeleteCount int64

	// LastUpdated records when statistics were last collected via ANALYZE
	LastUpdated time.Time

	// OriginalCount is the row count when statistics were last collected
	// Used to calculate modification ratio
	OriginalCount int64
}

// NewModificationTracker creates a new tracker for monitoring table modifications.
func NewModificationTracker() *ModificationTracker {
	return &ModificationTracker{
		tables: make(map[string]*TableModification),
	}
}

// RecordInsert records one or more INSERT operations on a table.
//
// Parameters:
// - tableName: Name of the table receiving the insert
// - rowCount: Number of rows inserted
//
// Thread-safe and non-blocking.
func (mt *ModificationTracker) RecordInsert(tableName string, rowCount int64) {
	if rowCount <= 0 {
		return
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	mod, exists := mt.tables[tableName]
	if !exists {
		// First modification on this table - initialize with empty stats
		mod = &TableModification{
			OriginalCount: 0,
			LastUpdated:   time.Now(),
		}
		mt.tables[tableName] = mod
	}

	mod.InsertCount += rowCount
}

// RecordUpdate records one or more UPDATE operations on a table.
//
// Parameters:
// - tableName: Name of the table being updated
// - rowCount: Number of rows updated
//
// Thread-safe and non-blocking.
func (mt *ModificationTracker) RecordUpdate(tableName string, rowCount int64) {
	if rowCount <= 0 {
		return
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	mod, exists := mt.tables[tableName]
	if !exists {
		mod = &TableModification{
			OriginalCount: 0,
			LastUpdated:   time.Now(),
		}
		mt.tables[tableName] = mod
	}

	mod.UpdateCount += rowCount
}

// RecordDelete records one or more DELETE operations on a table.
//
// Parameters:
// - tableName: Name of the table being deleted from
// - rowCount: Number of rows deleted
//
// Thread-safe and non-blocking.
func (mt *ModificationTracker) RecordDelete(tableName string, rowCount int64) {
	if rowCount <= 0 {
		return
	}

	mt.mu.Lock()
	defer mt.mu.Unlock()

	mod, exists := mt.tables[tableName]
	if !exists {
		mod = &TableModification{
			OriginalCount: 0,
			LastUpdated:   time.Now(),
		}
		mt.tables[tableName] = mod
	}

	mod.DeleteCount += rowCount
}

// GetModificationRatio calculates the percentage of table data that has changed
// since the last ANALYZE operation.
//
// Returns:
// - Ratio in range [0.0, infinity): 0.1 means 10% of rows changed
// - Returns 0.0 if table has never been analyzed or has 0 rows
// - Returns 0.0 if table doesn't exist in tracker (no modifications yet)
//
// Formula:
// ratio = (InsertCount + UpdateCount + DeleteCount) / OriginalCount
//
// Thread-safe read access.
func (mt *ModificationTracker) GetModificationRatio(tableName string) float64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	mod, exists := mt.tables[tableName]
	if !exists {
		return 0.0
	}

	// If table has never been analyzed or has zero rows, no ratio applies
	if mod.OriginalCount <= 0 {
		return 0.0
	}

	totalChanges := mod.InsertCount + mod.UpdateCount + mod.DeleteCount
	return float64(totalChanges) / float64(mod.OriginalCount)
}

// InitializeTable sets up tracking for a newly analyzed table.
//
// Parameters:
// - tableName: Name of the table
// - rowCount: Current number of rows in the table (used as baseline)
//
// Resets modification counts to zero and records the baseline row count.
// Called after each ANALYZE operation completes successfully.
//
// Thread-safe.
func (mt *ModificationTracker) InitializeTable(tableName string, rowCount int64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.tables[tableName] = &TableModification{
		InsertCount:   0,
		UpdateCount:   0,
		DeleteCount:   0,
		OriginalCount: rowCount,
		LastUpdated:   time.Now(),
	}
}

// Reset clears modification counts for a specific table.
// This is called after ANALYZE has been run successfully.
//
// Parameters:
// - tableName: Name of the table to reset
//
// Does NOT reset OriginalCount - that is maintained across resets
// so we can continue tracking ratios relative to the baseline.
//
// Thread-safe.
func (mt *ModificationTracker) Reset(tableName string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mod, exists := mt.tables[tableName]
	if !exists {
		return
	}

	mod.InsertCount = 0
	mod.UpdateCount = 0
	mod.DeleteCount = 0
	mod.LastUpdated = time.Now()
}

// GetTableModification returns a snapshot of modification data for a table.
// This is useful for monitoring and debugging.
//
// Thread-safe read access.
func (mt *ModificationTracker) GetTableModification(tableName string) *TableModification {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	mod, exists := mt.tables[tableName]
	if !exists {
		return nil
	}

	// Return a copy to prevent external modification
	copy := *mod
	return &copy
}

// GetAllModifications returns a snapshot of modifications for all tracked tables.
// Useful for debugging and status reporting.
//
// Thread-safe read access.
func (mt *ModificationTracker) GetAllModifications() map[string]*TableModification {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	result := make(map[string]*TableModification)
	for tableName, mod := range mt.tables {
		copy := *mod
		result[tableName] = &copy
	}
	return result
}

// ResetAll clears all tracked modifications across all tables.
// Used for testing or when reinitializing the statistics system.
//
// Thread-safe.
func (mt *ModificationTracker) ResetAll() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.tables = make(map[string]*TableModification)
}

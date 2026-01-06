// Package storage provides columnar storage for the native Go DuckDB implementation.
package storage

import (
	"sync"
	"time"

	"github.com/coder/quartz"
)

// VacuumStats contains statistics about vacuum operations.
// These statistics are useful for monitoring the effectiveness of garbage collection
// and diagnosing memory usage patterns.
type VacuumStats struct {
	// VersionsRemoved is the total number of old versions that have been removed
	// across all vacuum operations since the Vacuum was created or statistics were reset.
	VersionsRemoved uint64

	// ChainsProcessed is the total number of version chains that have been examined
	// by the vacuum process. This includes chains where no versions were removed.
	ChainsProcessed uint64

	// LastRunTime is the timestamp of the last vacuum operation.
	// A zero value indicates that no vacuum has been run yet.
	LastRunTime time.Time
}

// Vacuum implements garbage collection for old version chains in the MVCC system.
// It removes versions that are no longer visible to any active transaction, freeing
// memory and reducing version chain traversal overhead.
//
// Version Removal Rules:
// A version can be safely removed if ALL of the following conditions are met:
//  1. The version is committed (CommitTS != 0)
//  2. The version's CommitTS is less than the low watermark
//  3. There is a newer version in the chain (we're not removing the head)
//  4. If it's a delete marker (DeleteTS != 0), the DeleteTS must also be < low watermark
//
// The low watermark is the minimum StartTS of all active transactions. Versions
// committed before the low watermark are no longer visible to any active transaction
// and can be safely garbage collected.
//
// Thread Safety:
// All public methods on Vacuum are thread-safe. The internal mutex protects
// the statistics counters.
//
// Clock Injection:
// The Vacuum accepts a quartz.Clock for deterministic testing. In production,
// use quartz.NewReal() for actual wall-clock time.
type Vacuum struct {
	// lowWatermarkFunc is a function that returns the current low watermark timestamp.
	// This is typically provided by MVCCManager.GetLowWatermark.
	// The low watermark is the minimum StartTS of all active transactions.
	lowWatermarkFunc func() uint64

	// clock is the injected clock for time operations.
	// Use quartz.NewReal() for production, or quartz.NewMock() for testing.
	clock quartz.Clock

	// Statistics
	versionsRemoved uint64    // Total versions removed
	chainsProcessed uint64    // Total chains processed
	lastRunTime     time.Time // Last vacuum run time

	// mu protects the statistics fields.
	mu sync.Mutex
}

// NewVacuum creates a new Vacuum with the given low watermark function and clock.
//
// Parameters:
//   - lowWatermarkFunc: A function that returns the current low watermark timestamp.
//     This is typically MVCCManager.GetLowWatermark. The low watermark is the minimum
//     StartTS of all active transactions. If there are no active transactions, it
//     should return 0 (meaning all committed versions can potentially be cleaned up,
//     except for the head of each chain).
//   - clock: The clock to use for time operations. Use quartz.NewReal() for production,
//     or quartz.NewMock() for deterministic testing.
//
// Example:
//
//	// Production usage
//	mvcc := NewMVCCManager(quartz.NewReal())
//	vacuum := NewVacuum(mvcc.GetLowWatermark, quartz.NewReal())
//
//	// Testing usage
//	mockClock := quartz.NewMock()
//	vacuum := NewVacuum(func() uint64 { return 1000 }, mockClock)
func NewVacuum(lowWatermarkFunc func() uint64, clock quartz.Clock) *Vacuum {
	return &Vacuum{
		lowWatermarkFunc: lowWatermarkFunc,
		clock:            clock,
		versionsRemoved:  0,
		chainsProcessed:  0,
		lastRunTime:      time.Time{}, // Zero value indicates never run
	}
}

// CanRemoveVersion determines if a specific version can be safely removed from
// its version chain.
//
// A version can be removed if ALL of the following conditions are met:
//  1. The version is committed (CommitTS != 0) - uncommitted versions may still
//     be needed by their creating transaction.
//  2. The version's CommitTS is less than the low watermark - versions at or after
//     the low watermark may still be visible to some active transaction.
//  3. If the version has a delete marker (DeleteTS != 0), the DeleteTS must also
//     be less than the low watermark - this ensures the delete operation is also
//     visible to all transactions.
//
// Note: This method does NOT check if the version is the head of its chain.
// The caller (CleanVersionChain) is responsible for ensuring the head version
// is preserved. This separation of concerns allows for flexible chain cleaning
// algorithms.
//
// Parameters:
//   - version: The VersionedRow to check for removal eligibility. Must not be nil.
//   - lowWatermark: The minimum StartTS of all active transactions. Versions
//     committed before this timestamp are not visible to any active transaction.
//
// Returns:
//   - true if the version can be safely removed
//   - false if the version must be preserved
//
// Thread Safety: This method is thread-safe (stateless).
func (v *Vacuum) CanRemoveVersion(version *VersionedRow, lowWatermark uint64) bool {
	if version == nil {
		return false
	}

	// Rule 1: Version must be committed
	// Uncommitted versions (CommitTS == 0) may still be needed by their
	// creating transaction or may be rolled back.
	if version.CommitTS == 0 {
		return false
	}

	// Rule 2: Version's CommitTS must be less than the low watermark
	// If CommitTS >= lowWatermark, some active transaction may still need
	// to see this version based on snapshot visibility rules.
	if version.CommitTS >= lowWatermark {
		return false
	}

	// Rule 3: If the version has been deleted, the delete must also be committed
	// and visible to all transactions (DeleteTS < lowWatermark)
	if version.DeleteTS != 0 && version.DeleteTS >= lowWatermark {
		// The deletion is not yet visible to all transactions
		// We need to keep this version so transactions can see it was deleted
		return false
	}

	// All conditions met - this version can be removed
	return true
}

// CleanVersionChain removes old versions from a single version chain.
// It preserves the newest version(s) that may still be visible to active transactions
// and truncates the chain at the cutoff point.
//
// The algorithm:
//  1. Gets the current low watermark from the MVCCManager
//  2. Acquires an exclusive lock on the chain
//  3. Traverses from the head (newest) toward the tail (oldest)
//  4. Finds the first version that CAN be removed
//  5. Truncates the chain by setting that version's predecessor's PrevPtr to nil
//  6. Counts all removed versions (the truncated tail)
//
// Important invariants:
//   - The head of the chain is NEVER removed (even if it meets removal criteria)
//   - At least one committed version is preserved per chain if one exists
//   - Only committed versions are removed
//   - The chain structure remains valid after cleaning
//
// Parameters:
//   - chain: The VersionChain to clean. Must not be nil.
//
// Returns:
//   - The number of versions removed from the chain (0 if nothing was removed)
//
// Thread Safety: This method acquires an exclusive lock on the chain for the
// duration of the operation.
func (v *Vacuum) CleanVersionChain(chain *VersionChain) int {
	if chain == nil {
		return 0
	}

	// Get low watermark before locking the chain
	lowWatermark := v.lowWatermarkFunc()

	// If lowWatermark is 0, there are no active transactions.
	// We could potentially remove everything except the head,
	// but we need to be careful - we use a high watermark in this case.
	// When lowWatermark is 0, we'll treat it as "max possible" meaning
	// nothing can be removed (no transaction is waiting on any version).
	// Actually, when there are no active transactions, all committed versions
	// except the head are eligible for removal.
	if lowWatermark == 0 {
		// With no active transactions, use max uint64 as watermark
		// This means all committed versions can be removed (except head)
		lowWatermark = ^uint64(0)
	}

	// Lock the chain for modification
	chain.Lock()
	defer chain.Unlock()

	// Empty chain - nothing to clean
	if chain.Head == nil {
		return 0
	}

	removed := 0

	// We need to find the cutoff point where we can truncate.
	// We traverse the chain and find the first removable version that has
	// a predecessor. We'll cut the chain just before that version.
	//
	// Chain structure: Head -> v1 -> v2 -> v3 -> v4 -> nil (newest to oldest)
	//
	// If v2 and everything after can be removed, we set v1.PrevPtr = nil
	// This removes v2, v3, v4 from the chain.

	// Track the previous version as we traverse
	var prev *VersionedRow
	current := chain.Head

	// Find the cutoff point - the first version that we want to keep
	// the previous version of, where that previous version can be removed
	for current != nil {
		// Check if current version can be removed
		// We never remove the head (first iteration, prev == nil)
		if prev != nil && v.CanRemoveVersion(current, lowWatermark) {
			// Current and everything after it can be removed
			// Cut the chain by setting prev's PrevPtr to nil
			prev.PrevPtr = nil

			// Count this version
			removed++

			// Count remaining versions in the truncated tail
			tail := current.PrevPtr
			for tail != nil {
				removed++
				tail = tail.PrevPtr
			}

			break
		}

		prev = current
		current = current.PrevPtr
	}

	// Update statistics
	v.mu.Lock()
	v.versionsRemoved += uint64(removed)
	v.chainsProcessed++
	v.lastRunTime = v.clock.Now()
	v.mu.Unlock()

	return removed
}

// VacuumChains processes multiple version chains and removes old versions from each.
// This is the main entry point for batch garbage collection operations.
//
// Parameters:
//   - chains: A slice of VersionChain pointers to process. Nil chains are skipped.
//
// Returns:
//   - The total number of versions removed across all chains
//
// Thread Safety: This method is thread-safe. Each chain is locked individually
// during processing, allowing concurrent access to other chains.
//
// Example:
//
//	// Vacuum all chains in a table
//	chains := table.GetAllVersionChains()
//	removed := vacuum.VacuumChains(chains)
//	fmt.Printf("Removed %d old versions\n", removed)
func (v *Vacuum) VacuumChains(chains []*VersionChain) int {
	totalRemoved := 0

	for _, chain := range chains {
		if chain != nil {
			totalRemoved += v.CleanVersionChain(chain)
		}
	}

	return totalRemoved
}

// GetStatistics returns the current vacuum statistics.
// The returned struct is a snapshot of the statistics at the time of the call.
//
// Returns:
//   - VacuumStats containing:
//   - VersionsRemoved: Total versions removed since creation or last reset
//   - ChainsProcessed: Total chains examined since creation or last reset
//   - LastRunTime: Time of the last vacuum operation (zero if never run)
//
// Thread Safety: This method is thread-safe.
//
// Example:
//
//	stats := vacuum.GetStatistics()
//	fmt.Printf("Removed %d versions from %d chains\n",
//	    stats.VersionsRemoved, stats.ChainsProcessed)
//	if !stats.LastRunTime.IsZero() {
//	    fmt.Printf("Last run: %v\n", stats.LastRunTime)
//	}
func (v *Vacuum) GetStatistics() VacuumStats {
	v.mu.Lock()
	defer v.mu.Unlock()

	return VacuumStats{
		VersionsRemoved: v.versionsRemoved,
		ChainsProcessed: v.chainsProcessed,
		LastRunTime:     v.lastRunTime,
	}
}

// ResetStatistics resets all vacuum statistics counters to zero.
// This is useful for measuring the effect of vacuum operations over a specific
// time period or for testing purposes.
//
// The LastRunTime is also reset to the zero value, indicating that no vacuum
// has been run since the reset.
//
// Thread Safety: This method is thread-safe.
//
// Example:
//
//	vacuum.ResetStatistics()
//	// ... perform some operations ...
//	stats := vacuum.GetStatistics()
//	fmt.Printf("Since reset: removed %d versions\n", stats.VersionsRemoved)
func (v *Vacuum) ResetStatistics() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.versionsRemoved = 0
	v.chainsProcessed = 0
	v.lastRunTime = time.Time{}
}

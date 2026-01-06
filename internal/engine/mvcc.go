// Package engine provides the core execution engine for the native Go DuckDB implementation.
package engine

import (
	"sort"
	"sync"
	"time"

	"github.com/coder/quartz"
	"github.com/dukdb/dukdb-go/internal/parser"
	"github.com/dukdb/dukdb-go/internal/storage"
)

// MVCCManager manages multi-version concurrency control for the database.
// It provides timestamp allocation, transaction lifecycle management, and
// coordinates visibility checks across concurrent transactions.
//
// The MVCCManager is responsible for:
//   - Assigning monotonically increasing timestamps to transactions
//   - Tracking active transactions and their read/write sets
//   - Providing the low watermark for garbage collection of old versions
//
// Thread Safety:
// All methods are thread-safe. The internal mutex protects concurrent access
// to the manager's state.
//
// Clock Injection:
// The manager accepts a quartz.Clock for deterministic testing. In production,
// use quartz.NewReal() for actual wall-clock time.
type MVCCManager struct {
	// lastTS is the last assigned timestamp. Used to ensure monotonicity.
	// If the clock returns a time that would produce a timestamp <= lastTS,
	// we increment lastTS instead to maintain ordering.
	lastTS uint64

	// clock is the injected clock for timestamp generation.
	// Use quartz.NewReal() for production, or quartz.NewMock() for testing.
	clock quartz.Clock

	// activeTxns maps transaction IDs to their MVCCTransaction instances.
	// Only active (uncommitted) transactions are kept in this map.
	activeTxns map[uint64]*MVCCTransaction

	// mu protects all fields in the manager.
	mu sync.RWMutex
}

// NewMVCCManager creates a new MVCCManager with the given clock.
//
// Parameters:
//   - clock: The clock to use for timestamp generation. Use quartz.NewReal()
//     for production, or quartz.NewMock() for deterministic testing.
//
// Example:
//
//	// Production usage
//	manager := NewMVCCManager(quartz.NewReal())
//
//	// Testing usage
//	mockClock := quartz.NewMock()
//	manager := NewMVCCManager(mockClock)
//	mockClock.Advance(time.Second) // Control time progression
func NewMVCCManager(clock quartz.Clock) *MVCCManager {
	return &MVCCManager{
		lastTS:     0,
		clock:      clock,
		activeTxns: make(map[uint64]*MVCCTransaction),
	}
}

// NextTimestamp returns a monotonically increasing timestamp.
// The timestamp is derived from the clock's current time in nanoseconds,
// but is guaranteed to be greater than any previously returned timestamp.
//
// This ensures timestamp ordering even if:
//   - The clock has low resolution (returns same time for multiple calls)
//   - The clock is adjusted backwards (e.g., NTP corrections)
//   - Multiple transactions start in rapid succession
//
// Thread Safety: This method is thread-safe.
func (m *MVCCManager) NextTimestamp() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get current time as nanoseconds
	ts := uint64(m.clock.Now().UnixNano())

	// Ensure monotonicity: if clock returned same or earlier time,
	// just increment the last timestamp
	if ts <= m.lastTS {
		m.lastTS++
	} else {
		m.lastTS = ts
	}

	return m.lastTS
}

// BeginTransaction starts a new MVCC transaction with the specified isolation level.
// The transaction is assigned a unique ID (based on its start timestamp) and
// is registered as active in the manager.
//
// Parameters:
//   - isolationLevel: The isolation level for this transaction. Determines
//     visibility rules for concurrent data access.
//
// Returns:
//   - A new MVCCTransaction that is active and ready for use.
//
// The transaction captures a snapshot of currently active transactions at start time.
// This snapshot is used for visibility checks in REPEATABLE READ and SERIALIZABLE
// isolation levels.
//
// Thread Safety: This method is thread-safe.
func (m *MVCCManager) BeginTransaction(isolationLevel parser.IsolationLevel) *MVCCTransaction {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get start timestamp (ensures monotonicity)
	startTS := m.nextTimestampLocked()
	startTime := m.clock.Now()

	// Capture snapshot of active transactions at start time
	activeAtStart := make([]uint64, 0, len(m.activeTxns))
	for id := range m.activeTxns {
		activeAtStart = append(activeAtStart, id)
	}

	// Create the transaction
	txn := &MVCCTransaction{
		id:             startTS, // Use timestamp as transaction ID for uniqueness
		active:         true,
		isolationLevel: isolationLevel,
		startTime:      startTime,
		StartTS:        startTS,
		CommitTS:       0, // Not committed yet
		ActiveAtStart:  activeAtStart,
		ReadSet:        make(map[string][]uint64),
		WriteSet:       make(map[string][]uint64),
		manager:        m,
	}

	// Register as active
	m.activeTxns[txn.id] = txn

	return txn
}

// Commit commits the given transaction and assigns it a commit timestamp.
// The transaction is removed from the active set after commit.
//
// Parameters:
//   - txn: The transaction to commit. Must be active.
//
// Returns:
//   - nil on success
//   - An error if the transaction is not active or not managed by this manager
//
// After commit, the transaction's CommitTS is set to a new timestamp that is
// greater than its StartTS and all previously committed transactions.
//
// Thread Safety: This method is thread-safe.
func (m *MVCCManager) Commit(txn *MVCCTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !txn.active {
		return storage.ErrSerializationFailure // Transaction already ended
	}

	// Verify this transaction belongs to this manager
	if m.activeTxns[txn.id] != txn {
		return storage.ErrSerializationFailure // Transaction not found
	}

	// Assign commit timestamp
	txn.CommitTS = m.nextTimestampLocked()

	// Remove from active set
	delete(m.activeTxns, txn.id)
	txn.active = false

	return nil
}

// Rollback rolls back the given transaction without committing.
// The transaction is removed from the active set.
//
// Parameters:
//   - txn: The transaction to roll back. Must be active.
//
// Returns:
//   - nil on success
//   - An error if the transaction is not active or not managed by this manager
//
// The transaction's CommitTS remains 0 after rollback, indicating it was never committed.
//
// Thread Safety: This method is thread-safe.
func (m *MVCCManager) Rollback(txn *MVCCTransaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !txn.active {
		return storage.ErrSerializationFailure // Transaction already ended
	}

	// Verify this transaction belongs to this manager
	if m.activeTxns[txn.id] != txn {
		return storage.ErrSerializationFailure // Transaction not found
	}

	// Remove from active set (don't set CommitTS - it was never committed)
	delete(m.activeTxns, txn.id)
	txn.active = false

	return nil
}

// GetActiveTransactions returns a slice of all currently active transactions.
// The returned slice is a snapshot; modifications to it do not affect the manager.
//
// Returns:
//   - A slice of pointers to active MVCCTransaction instances
//
// Thread Safety: This method is thread-safe. The returned slice is a copy.
func (m *MVCCManager) GetActiveTransactions() []*MVCCTransaction {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*MVCCTransaction, 0, len(m.activeTxns))
	for _, txn := range m.activeTxns {
		result = append(result, txn)
	}

	return result
}

// GetLowWatermark returns the minimum start timestamp among all active transactions.
// This timestamp represents the oldest point in time that any active transaction
// might still need to see.
//
// Returns:
//   - The minimum StartTS of all active transactions
//   - 0 if there are no active transactions
//
// Use case: The low watermark is used for garbage collection of old row versions.
// Any row version with a CommitTS less than the low watermark and that has been
// superseded by a newer version can be safely removed, as no active transaction
// will need to see it.
//
// Thread Safety: This method is thread-safe.
func (m *MVCCManager) GetLowWatermark() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.activeTxns) == 0 {
		return 0
	}

	minTS := ^uint64(0) // Max uint64
	for _, txn := range m.activeTxns {
		if txn.StartTS < minTS {
			minTS = txn.StartTS
		}
	}

	return minTS
}

// nextTimestampLocked returns a monotonically increasing timestamp.
// Caller must hold m.mu lock.
func (m *MVCCManager) nextTimestampLocked() uint64 {
	ts := uint64(m.clock.Now().UnixNano())
	if ts <= m.lastTS {
		m.lastTS++
	} else {
		m.lastTS = ts
	}

	return m.lastTS
}

// MVCCTransaction represents a transaction in the MVCC system.
// It tracks the transaction's lifecycle, visibility information, and read/write sets
// for conflict detection in SERIALIZABLE isolation.
//
// The transaction maintains:
//   - Core identity: ID, active status, isolation level, start time
//   - MVCC timestamps: StartTS (when began), CommitTS (when committed, 0 until then)
//   - Snapshot info: Which transactions were active when this one started
//   - Read/Write sets: Which rows were read/written (for conflict detection)
//
// Lifecycle:
//  1. Created via MVCCManager.BeginTransaction() - active=true, CommitTS=0
//  2. Operations recorded via RecordRead/RecordWrite
//  3. Ends via Commit (sets CommitTS) or Rollback (CommitTS stays 0)
//
// Thread Safety:
// The MVCCTransaction is designed to be used by a single goroutine (the connection
// that owns it). The read/write set methods have internal synchronization for
// safety, but concurrent use from multiple goroutines is not recommended.
type MVCCTransaction struct {
	// id is the unique identifier for this transaction.
	// Assigned from the start timestamp for guaranteed uniqueness.
	id uint64

	// active indicates whether this transaction is still running.
	// Set to false after Commit or Rollback.
	active bool

	// isolationLevel determines visibility rules for this transaction.
	isolationLevel parser.IsolationLevel

	// startTime is when this transaction began (wall clock).
	startTime time.Time

	// StartTS is the transaction's start timestamp from the MVCC manager.
	// Used for visibility calculations and as a unique identifier.
	StartTS uint64

	// CommitTS is the commit timestamp assigned when the transaction commits.
	// A value of 0 indicates the transaction has not yet committed.
	CommitTS uint64

	// ActiveAtStart contains the IDs of transactions that were active
	// when this transaction started. Used for snapshot-based visibility
	// in REPEATABLE READ and SERIALIZABLE isolation levels.
	ActiveAtStart []uint64

	// ReadSet tracks rows read by this transaction.
	// Maps table name to a list of row IDs read from that table.
	// Used for conflict detection in SERIALIZABLE isolation.
	ReadSet map[string][]uint64

	// WriteSet tracks rows written by this transaction.
	// Maps table name to a list of row IDs written to that table.
	// Used for conflict detection in SERIALIZABLE isolation.
	WriteSet map[string][]uint64

	// manager is a reference back to the MVCCManager for coordination.
	manager *MVCCManager

	// mu protects ReadSet and WriteSet for concurrent access.
	mu sync.RWMutex

	// activeAtStartSet is a lazily-built set for O(1) lookups in WasActiveAtStart.
	activeAtStartSet map[uint64]struct{}
}

// ID returns the unique identifier for this transaction.
// The ID is assigned from the start timestamp, ensuring uniqueness.
func (t *MVCCTransaction) ID() uint64 {
	return t.id
}

// IsActive returns whether this transaction is still active (not committed or rolled back).
func (t *MVCCTransaction) IsActive() bool {
	return t.active
}

// GetIsolationLevel returns the isolation level of this transaction.
func (t *MVCCTransaction) GetIsolationLevel() parser.IsolationLevel {
	return t.isolationLevel
}

// GetStartTime returns the wall-clock time when this transaction started.
func (t *MVCCTransaction) GetStartTime() time.Time {
	return t.startTime
}

// GetStartTS returns the MVCC start timestamp for this transaction.
// This timestamp is used for visibility calculations.
func (t *MVCCTransaction) GetStartTS() uint64 {
	return t.StartTS
}

// GetCommitTS returns the commit timestamp for this transaction.
// Returns 0 if the transaction has not yet committed.
func (t *MVCCTransaction) GetCommitTS() uint64 {
	return t.CommitTS
}

// RecordRead tracks that this transaction has read a row from a table.
// This information is used for conflict detection in SERIALIZABLE isolation.
//
// Parameters:
//   - table: The name of the table containing the row
//   - rowID: The unique identifier of the row that was read
//
// Thread Safety: This method is thread-safe.
func (t *MVCCTransaction) RecordRead(table string, rowID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.ReadSet[table] = append(t.ReadSet[table], rowID)
}

// RecordWrite tracks that this transaction has written a row to a table.
// This information is used for conflict detection in SERIALIZABLE isolation.
//
// Parameters:
//   - table: The name of the table containing the row
//   - rowID: The unique identifier of the row that was written
//
// Thread Safety: This method is thread-safe.
func (t *MVCCTransaction) RecordWrite(table string, rowID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.WriteSet[table] = append(t.WriteSet[table], rowID)
}

// GetReadSet returns a copy of this transaction's read set.
// The read set maps table names to lists of row IDs read from each table.
//
// Returns:
//   - A copy of the read set map. Modifications do not affect the transaction.
//
// Thread Safety: This method is thread-safe.
func (t *MVCCTransaction) GetReadSet() map[string][]uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string][]uint64, len(t.ReadSet))
	for table, rowIDs := range t.ReadSet {
		copyIDs := make([]uint64, len(rowIDs))
		copy(copyIDs, rowIDs)
		result[table] = copyIDs
	}

	return result
}

// GetWriteSet returns a copy of this transaction's write set.
// The write set maps table names to lists of row IDs written to each table.
//
// Returns:
//   - A copy of the write set map. Modifications do not affect the transaction.
//
// Thread Safety: This method is thread-safe.
func (t *MVCCTransaction) GetWriteSet() map[string][]uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make(map[string][]uint64, len(t.WriteSet))
	for table, rowIDs := range t.WriteSet {
		copyIDs := make([]uint64, len(rowIDs))
		copy(copyIDs, rowIDs)
		result[table] = copyIDs
	}

	return result
}

// VisibilityChecker returns the appropriate visibility checker for this
// transaction's isolation level.
//
// Returns:
//   - A VisibilityChecker implementation suitable for the transaction's isolation level
//
// The returned checker implements the visibility rules for:
//   - READ UNCOMMITTED: Can see uncommitted changes
//   - READ COMMITTED: Statement-level snapshots
//   - REPEATABLE READ: Transaction-level snapshots
//   - SERIALIZABLE: Same as REPEATABLE READ (conflict detection is separate)
func (t *MVCCTransaction) VisibilityChecker() storage.VisibilityChecker {
	return storage.GetVisibilityChecker(t.isolationLevel)
}

// WasActiveAtStart checks if a transaction was active when this transaction started.
// This is used for visibility checks in REPEATABLE READ and SERIALIZABLE isolation.
//
// Parameters:
//   - txnID: The ID of the transaction to check
//
// Returns:
//   - true if txnID was in the active set when this transaction began
//   - false otherwise
//
// A transaction that was active at start time has not yet committed from this
// transaction's perspective. Therefore, rows created by that transaction should
// not be visible (for REPEATABLE READ and SERIALIZABLE).
//
// Thread Safety: This method is thread-safe.
func (t *MVCCTransaction) WasActiveAtStart(txnID uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Lazily build the set for O(1) lookups
	if t.activeAtStartSet == nil {
		t.activeAtStartSet = make(map[uint64]struct{}, len(t.ActiveAtStart))
		for _, id := range t.ActiveAtStart {
			t.activeAtStartSet[id] = struct{}{}
		}
	}

	_, exists := t.activeAtStartSet[txnID]

	return exists
}

// GetActiveAtStart returns the IDs of transactions that were active when this
// transaction started. This is a copy of the original slice.
//
// Returns:
//   - A copy of the ActiveAtStart slice
//
// Thread Safety: This method is thread-safe.
func (t *MVCCTransaction) GetActiveAtStart() []uint64 {
	result := make([]uint64, len(t.ActiveAtStart))
	copy(result, t.ActiveAtStart)
	// Sort for consistent ordering
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

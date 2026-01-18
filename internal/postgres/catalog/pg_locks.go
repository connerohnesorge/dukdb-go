// Package catalog provides PostgreSQL-compatible pg_catalog views for dukdb-go.
// This file implements the pg_locks view for lock monitoring.
package catalog

// pg_locks columns - PostgreSQL lock monitoring view.
// Reference: https://www.postgresql.org/docs/current/view-pg-locks.html
var pgLocksColumns = []string{
	"locktype",           // Type of the lockable object
	"database",           // OID of the database (null if not database-specific)
	"relation",           // OID of the relation (null if not relation-specific)
	"page",               // Page number within the relation (null if not page-specific)
	"tuple",              // Tuple number within the page (null if not tuple-specific)
	"virtualxid",         // Virtual transaction ID
	"transactionid",      // Transaction ID (null if not transaction-specific)
	"classid",            // OID of the system catalog containing the object
	"objid",              // OID of the object within the system catalog
	"objsubid",           // Column number for table-column-level locks
	"virtualtransaction", // Virtual transaction ID holding/waiting for lock
	"pid",                // Process ID of the backend holding/waiting for lock
	"mode",               // Lock mode (AccessShareLock, RowShareLock, etc.)
	"granted",            // True if lock is held, false if waiting
	"fastpath",           // True if lock was taken via fast path
	"waitstart",          // Time when lock wait started (null if not waiting)
}

// LockInfo represents information about a single lock.
type LockInfo struct {
	// LockType is the type of the lockable object (relation, transactionid, etc.).
	LockType string

	// DatabaseOID is the database OID (null if not database-specific).
	DatabaseOID *int64

	// RelationOID is the relation OID (null if not relation-specific).
	RelationOID *int64

	// Page is the page number (null if not page-specific).
	Page *int32

	// Tuple is the tuple number (null if not tuple-specific).
	Tuple *int32

	// VirtualXID is the virtual transaction ID.
	VirtualXID string

	// TransactionID is the transaction ID (null if not transaction-specific).
	TransactionID *int64

	// VirtualTransaction is the virtual transaction holding/waiting.
	VirtualTransaction string

	// PID is the process ID holding/waiting for the lock.
	PID int64

	// Mode is the lock mode.
	Mode string

	// Granted is true if the lock is held, false if waiting.
	Granted bool

	// FastPath is true if the lock was taken via fast path.
	FastPath bool

	// WaitStart is when the lock wait started (nil if not waiting).
	WaitStart *string
}

// LockProvider is an interface for getting lock information.
// The server package implements this interface to provide lock data.
type LockProvider interface {
	// GetLocks returns all current locks.
	GetLocks() []*LockInfo
}

// PgLocksView handles pg_locks queries.
type PgLocksView struct {
	provider LockProvider
}

// NewPgLocksView creates a new pg_locks view handler.
func NewPgLocksView(provider LockProvider) *PgLocksView {
	return &PgLocksView{
		provider: provider,
	}
}

// Query returns pg_locks data.
func (v *PgLocksView) Query(filters []Filter) *QueryResult {
	result := &QueryResult{
		Columns: pgLocksColumns,
		Rows:    make([]map[string]any, 0),
	}

	if v.provider == nil {
		// Return empty result when no provider is available.
		// This is common since DukDB doesn't have explicit locking like PostgreSQL.
		return result
	}

	locks := v.provider.GetLocks()
	for _, lock := range locks {
		row := map[string]any{
			"locktype":           lock.LockType,
			"database":           int64PtrToAny(lock.DatabaseOID),
			"relation":           int64PtrToAny(lock.RelationOID),
			"page":               int32PtrToAny(lock.Page),
			"tuple":              int32PtrToAny(lock.Tuple),
			"virtualxid":         lock.VirtualXID,
			"transactionid":      int64PtrToAny(lock.TransactionID),
			"classid":            nil,
			"objid":              nil,
			"objsubid":           nil,
			"virtualtransaction": lock.VirtualTransaction,
			"pid":                lock.PID,
			"mode":               lock.Mode,
			"granted":            lock.Granted,
			"fastpath":           lock.FastPath,
			"waitstart":          stringPtrToAny(lock.WaitStart),
		}

		if matchesFilters(row, filters) {
			result.Rows = append(result.Rows, row)
		}
	}

	return result
}

// int64PtrToAny converts *int64 to any (nil if nil).
func int64PtrToAny(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// int32PtrToAny converts *int32 to any (nil if nil).
func int32PtrToAny(p *int32) any {
	if p == nil {
		return nil
	}
	return *p
}

// stringPtrToAny converts *string to any (nil if nil).
func stringPtrToAny(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

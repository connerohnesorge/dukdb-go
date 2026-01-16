// Package server provides a PostgreSQL wire protocol server for dukdb-go.
//
// This file provides foundation types and documentation for PostgreSQL logical
// replication protocol support. This is marked as FUTURE work and is not yet
// fully implemented.

package server

import (
	"context"
	"errors"
	"sync"
	"time"
)

// =============================================================================
// REPLICATION PROTOCOL DOCUMENTATION
// =============================================================================
//
// PostgreSQL Logical Replication Protocol Overview
// ------------------------------------------------
//
// PostgreSQL's logical replication protocol enables streaming changes from a
// database to subscribers. This allows for:
//
//   - Real-time Change Data Capture (CDC)
//   - Database replication and synchronization
//   - Event sourcing and audit logging
//   - Integration with systems like Debezium, Kafka Connect, etc.
//
// Protocol Components
// -------------------
//
// 1. Replication Connection:
//    - Special connection mode enabled by "replication" startup parameter
//    - Uses the same wire protocol with additional replication commands
//    - Commands: IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, START_REPLICATION, etc.
//
// 2. Replication Slots:
//    - Named markers that track replication progress
//    - Prevent WAL segments from being recycled before consumed
//    - Types: physical (byte-level) and logical (decoded changes)
//
// 3. Output Plugins:
//    - Transform WAL records into a consumable format
//    - Standard plugins: pgoutput (native), wal2json, test_decoding
//    - Custom plugins can be created for specific formats
//
// 4. Publications/Subscriptions:
//    - Publications define which tables/changes to replicate
//    - Subscriptions connect to publications and apply changes
//
// Wire Protocol Messages
// ----------------------
//
// Replication-specific message types:
//
//   - CopyBothResponse (W): Enters bidirectional COPY mode for streaming
//   - XLogData (w): Contains WAL data being streamed
//   - PrimaryKeepalive (k): Server heartbeat with LSN position
//   - StandbyStatusUpdate (r): Client feedback with flush/apply positions
//
// LSN (Log Sequence Number):
//   - 64-bit position in the WAL stream
//   - Format: XXXXXXXX/XXXXXXXX (high 32 bits / low 32 bits)
//   - Used for tracking replication progress
//
// Implementation Considerations for DukDB
// ---------------------------------------
//
// Since DukDB is an embedded database without traditional WAL, implementing
// logical replication requires:
//
// 1. Change Tracking Layer:
//    - Hook into the execution engine to capture row-level changes
//    - Track INSERT, UPDATE, DELETE operations with before/after images
//    - Maintain change ordering (LSN equivalent)
//
// 2. Output Format:
//    - Implement pgoutput protocol for PostgreSQL compatibility
//    - Consider wal2json for simpler CDC integrations
//
// 3. Slot Management:
//    - Persistent storage for slot positions
//    - Garbage collection of consumed changes
//
// 4. Streaming Infrastructure:
//    - CopyBoth mode implementation
//    - Heartbeat/keepalive handling
//    - Client feedback processing
//
// References
// ----------
//
//   - PostgreSQL Protocol: https://www.postgresql.org/docs/current/protocol-replication.html
//   - Logical Decoding: https://www.postgresql.org/docs/current/logicaldecoding.html
//   - pgoutput: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
//   - Debezium PostgreSQL: https://debezium.io/documentation/reference/stable/connectors/postgresql.html
//
// =============================================================================

// Common errors for replication operations.
var (
	// ErrReplicationNotSupported indicates that replication is not yet implemented.
	ErrReplicationNotSupported = errors.New("logical replication is not yet supported")

	// ErrSlotNotFound indicates that the requested replication slot does not exist.
	ErrSlotNotFound = errors.New("replication slot not found")

	// ErrSlotAlreadyExists indicates that a slot with the given name already exists.
	ErrSlotAlreadyExists = errors.New("replication slot already exists")

	// ErrSlotInUse indicates that the slot is currently active on another connection.
	ErrSlotInUse = errors.New("replication slot is in use")

	// ErrInvalidLSN indicates an invalid Log Sequence Number format.
	ErrInvalidLSN = errors.New("invalid LSN format")

	// ErrReplicationNotConnected indicates no active replication connection.
	ErrReplicationNotConnected = errors.New("not connected in replication mode")
)

// LSN represents a Log Sequence Number in PostgreSQL's WAL.
// It is a 64-bit value representing a position in the write-ahead log.
type LSN uint64

// Note: String() and ParseLSN are implemented in replication_impl.go

// ReplicationSlotType represents the type of replication slot.
type ReplicationSlotType int

const (
	// SlotTypeLogical represents a logical replication slot.
	SlotTypeLogical ReplicationSlotType = iota

	// SlotTypePhysical represents a physical replication slot.
	SlotTypePhysical
)

// ReplicationSlot represents a named marker for tracking replication progress.
// Slots persist across server restarts and track which changes have been
// consumed by subscribers.
type ReplicationSlot struct {
	// Name is the unique identifier for this slot.
	Name string

	// Plugin is the output plugin used for logical decoding (e.g., "pgoutput").
	Plugin string

	// SlotType indicates whether this is a logical or physical slot.
	SlotType ReplicationSlotType

	// Database is the database this slot is associated with.
	Database string

	// Active indicates whether the slot is currently being used.
	Active bool

	// RestartLSN is the oldest LSN that may still be needed by consumers.
	RestartLSN LSN

	// ConfirmedFlushLSN is the LSN up to which the consumer has confirmed receipt.
	ConfirmedFlushLSN LSN

	// CreatedAt is when the slot was created.
	CreatedAt time.Time
}

// ReplicationSlotManager manages replication slots.
// This is a stub interface - actual implementation would provide slot persistence.
type ReplicationSlotManager interface {
	// CreateSlot creates a new replication slot.
	// Returns the initial consistent point LSN.
	CreateSlot(ctx context.Context, name, plugin string, temporary bool) (*ReplicationSlot, error)

	// DropSlot removes a replication slot.
	DropSlot(ctx context.Context, name string) error

	// GetSlot retrieves information about a slot.
	GetSlot(ctx context.Context, name string) (*ReplicationSlot, error)

	// ListSlots returns all replication slots.
	ListSlots(ctx context.Context) ([]*ReplicationSlot, error)

	// AdvanceSlot moves the slot's restart_lsn forward.
	AdvanceSlot(ctx context.Context, name string, lsn LSN) error
}

// OutputPlugin defines the interface for logical replication output plugins.
// Output plugins transform internal change records into wire protocol format.
type OutputPlugin interface {
	// Name returns the plugin name (e.g., "pgoutput", "wal2json").
	Name() string

	// Startup is called when replication begins.
	Startup(ctx context.Context, options map[string]string) error

	// Shutdown is called when replication ends.
	Shutdown(ctx context.Context) error

	// BeginTransaction is called at the start of a transaction.
	BeginTransaction(xid uint32, commitTime time.Time, finalLSN LSN) ([]byte, error)

	// CommitTransaction is called at the end of a transaction.
	CommitTransaction(xid uint32, commitTime time.Time, commitLSN LSN) ([]byte, error)

	// Insert encodes an INSERT change.
	Insert(relation string, newRow map[string]interface{}) ([]byte, error)

	// Update encodes an UPDATE change.
	Update(relation string, oldRow, newRow map[string]interface{}) ([]byte, error)

	// Delete encodes a DELETE change.
	Delete(relation string, oldRow map[string]interface{}) ([]byte, error)
}

// ChangeRecord represents a single row-level change in the database.
// This would be captured by hooks in the execution engine.
type ChangeRecord struct {
	// LSN is the sequence number for this change.
	LSN LSN

	// XID is the transaction ID.
	XID uint32

	// Type is the operation type: INSERT, UPDATE, DELETE.
	Type ChangeType

	// Schema is the schema name.
	Schema string

	// Table is the table name.
	Table string

	// OldRow contains the row data before the change (for UPDATE/DELETE).
	OldRow map[string]interface{}

	// NewRow contains the row data after the change (for INSERT/UPDATE).
	NewRow map[string]interface{}

	// Timestamp is when the change occurred.
	Timestamp time.Time
}

// ChangeType represents the type of row-level change.
type ChangeType int

const (
	// ChangeInsert represents an INSERT operation.
	ChangeInsert ChangeType = iota

	// ChangeUpdate represents an UPDATE operation.
	ChangeUpdate

	// ChangeDelete represents a DELETE operation.
	ChangeDelete
)

// String returns the string representation of the change type.
func (c ChangeType) String() string {
	switch c {
	case ChangeInsert:
		return "INSERT"
	case ChangeUpdate:
		return "UPDATE"
	case ChangeDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Publication defines which tables and operations to replicate.
// This corresponds to PostgreSQL's CREATE PUBLICATION command.
type Publication struct {
	// Name is the publication name.
	Name string

	// AllTables indicates whether all tables are published.
	AllTables bool

	// Tables lists specific tables to publish (if AllTables is false).
	Tables []string

	// PublishInserts indicates whether INSERT operations are published.
	PublishInserts bool

	// PublishUpdates indicates whether UPDATE operations are published.
	PublishUpdates bool

	// PublishDeletes indicates whether DELETE operations are published.
	PublishDeletes bool

	// PublishTruncates indicates whether TRUNCATE operations are published.
	PublishTruncates bool
}

// ReplicationConnection represents a connection in replication mode.
// This handles the special replication protocol commands and streaming.
type ReplicationConnection struct {
	mu sync.Mutex

	// session is the underlying database session.
	session *Session

	// slot is the active replication slot.
	slot *ReplicationSlot

	// plugin is the active output plugin.
	plugin OutputPlugin

	// streaming indicates if we're actively streaming changes.
	streaming bool

	// lastSentLSN tracks the last LSN sent to the client.
	lastSentLSN LSN

	// lastFeedbackTime tracks when we last received client feedback.
	lastFeedbackTime time.Time
}

// ReplicationManager coordinates replication functionality.
// This is the main entry point for replication operations.
type ReplicationManager struct {
	mu sync.RWMutex

	// slots maps slot names to slot objects.
	slots map[string]*ReplicationSlot

	// activeConnections tracks active replication connections per slot.
	activeConnections map[string]*ReplicationConnection

	// publications maps publication names to publication objects.
	publications map[string]*Publication
}

// NewReplicationManager creates a new replication manager.
// Note: This returns a stub implementation that returns ErrReplicationNotSupported
// for all operations until full implementation is completed.
func NewReplicationManager() *ReplicationManager {
	return &ReplicationManager{
		slots:             make(map[string]*ReplicationSlot),
		activeConnections: make(map[string]*ReplicationConnection),
		publications:      make(map[string]*Publication),
	}
}

// Note: IsReplicationSupported is implemented in replication_impl.go

// =============================================================================
// IMPLEMENTATION NOTES
// =============================================================================
//
// Full replication protocol implementation is provided in replication_impl.go.
// This includes:
//
// - InMemorySlotManager: In-memory replication slot management
// - WALSender: Change streaming to replication subscribers
// - PublicationManager: Publication/subscription management
// - PgOutputPlugin: Native PostgreSQL output plugin format
// - ReplicationHandler: Main entry point for replication operations
//
// Wire protocol messages:
// - XLogData ('w'): WAL data streaming
// - PrimaryKeepalive ('k'): Server heartbeat
// - StandbyStatusUpdate ('r'): Client feedback
//
// The implementation is compatible with Debezium and other CDC tools.
//
// Task Status (Tasks 22.1-22.5):
// - Task 22.1: Research PostgreSQL logical replication protocol - COMPLETED
// - Task 22.2: Design replication slot management - COMPLETED
// - Task 22.3: Implement WAL sender for change streaming - COMPLETED
// - Task 22.4: Add publication/subscription support - COMPLETED
// - Task 22.5: Test infrastructure for Debezium CDC - COMPLETED
//
// =============================================================================

// CreateSlot creates a new replication slot.
// This delegates to the internal slot storage.
func (rm *ReplicationManager) CreateSlot(ctx context.Context, name, plugin string, temporary bool) (*ReplicationSlot, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if slot already exists
	if _, exists := rm.slots[name]; exists {
		return nil, ErrSlotAlreadyExists
	}

	slot := &ReplicationSlot{
		Name:              name,
		Plugin:            plugin,
		SlotType:          SlotTypeLogical,
		Database:          "dukdb",
		Active:            false,
		RestartLSN:        LSN(1),
		ConfirmedFlushLSN: LSN(1),
		CreatedAt:         time.Now(),
	}

	rm.slots[name] = slot

	// Return a copy
	slotCopy := *slot
	return &slotCopy, nil
}

// DropSlot removes a replication slot.
func (rm *ReplicationManager) DropSlot(ctx context.Context, name string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	slot, exists := rm.slots[name]
	if !exists {
		return ErrSlotNotFound
	}

	// Cannot drop active slot
	if slot.Active {
		return ErrSlotInUse
	}

	delete(rm.slots, name)
	delete(rm.activeConnections, name)

	return nil
}

// StartReplication begins streaming changes.
// This prepares the replication connection for streaming.
func (rm *ReplicationManager) StartReplication(ctx context.Context, slotName string, startLSN LSN) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	slot, exists := rm.slots[slotName]
	if !exists {
		return ErrSlotNotFound
	}

	if slot.Active {
		return ErrSlotInUse
	}

	// Mark slot as active
	slot.Active = true

	return nil
}

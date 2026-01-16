// Package server provides PostgreSQL replication protocol implementation.
//
// This file implements the PostgreSQL logical replication protocol,
// enabling CDC (Change Data Capture) capabilities compatible with
// tools like Debezium.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// LSN IMPLEMENTATION
// =============================================================================

// String returns the LSN in PostgreSQL's standard format: XXXXXXXX/XXXXXXXX.
func (l LSN) String() string {
	high := uint32(l >> 32)
	low := uint32(l & 0xFFFFFFFF)
	return fmt.Sprintf("%X/%X", high, low)
}

// ParseLSN parses an LSN string in the format XXXXXXXX/XXXXXXXX.
func ParseLSN(s string) (LSN, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return 0, ErrInvalidLSN
	}

	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, ErrInvalidLSN
	}

	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, ErrInvalidLSN
	}

	return LSN((high << 32) | low), nil
}

// InvalidLSN represents an invalid or unset LSN value.
const InvalidLSN LSN = 0

// =============================================================================
// REPLICATION SLOT MANAGER IMPLEMENTATION
// =============================================================================

// InMemorySlotManager implements ReplicationSlotManager with in-memory storage.
// This is suitable for development and testing. Production deployments
// should use a persistent storage backend.
type InMemorySlotManager struct {
	mu    sync.RWMutex
	slots map[string]*ReplicationSlot

	// lsnCounter is used to generate monotonically increasing LSNs.
	lsnCounter uint64

	// temporarySlots tracks temporary slots that should be cleaned up.
	temporarySlots map[string]bool

	// activeConsumers tracks which slots are currently being consumed.
	activeConsumers map[string]uint64 // slot name -> session ID
}

// NewInMemorySlotManager creates a new in-memory slot manager.
func NewInMemorySlotManager() *InMemorySlotManager {
	return &InMemorySlotManager{
		slots:           make(map[string]*ReplicationSlot),
		temporarySlots:  make(map[string]bool),
		activeConsumers: make(map[string]uint64),
		lsnCounter:      1, // Start at 1 so 0/0 is invalid
	}
}

// CreateSlot creates a new replication slot.
func (m *InMemorySlotManager) CreateSlot(ctx context.Context, name, plugin string, temporary bool) (*ReplicationSlot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if slot already exists
	if _, exists := m.slots[name]; exists {
		return nil, ErrSlotAlreadyExists
	}

	// Generate initial LSN
	lsn := m.nextLSN()

	slot := &ReplicationSlot{
		Name:              name,
		Plugin:            plugin,
		SlotType:          SlotTypeLogical,
		Database:          "dukdb",
		Active:            false,
		RestartLSN:        lsn,
		ConfirmedFlushLSN: lsn,
		CreatedAt:         time.Now(),
	}

	m.slots[name] = slot
	if temporary {
		m.temporarySlots[name] = true
	}

	return slot, nil
}

// DropSlot removes a replication slot.
func (m *InMemorySlotManager) DropSlot(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot, exists := m.slots[name]
	if !exists {
		return ErrSlotNotFound
	}

	// Cannot drop active slot
	if slot.Active {
		return ErrSlotInUse
	}

	delete(m.slots, name)
	delete(m.temporarySlots, name)
	delete(m.activeConsumers, name)

	return nil
}

// GetSlot retrieves information about a slot.
func (m *InMemorySlotManager) GetSlot(ctx context.Context, name string) (*ReplicationSlot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slot, exists := m.slots[name]
	if !exists {
		return nil, ErrSlotNotFound
	}

	// Return a copy to prevent external modification
	slotCopy := *slot
	return &slotCopy, nil
}

// ListSlots returns all replication slots.
func (m *InMemorySlotManager) ListSlots(ctx context.Context) ([]*ReplicationSlot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*ReplicationSlot, 0, len(m.slots))
	for _, slot := range m.slots {
		slotCopy := *slot
		result = append(result, &slotCopy)
	}

	return result, nil
}

// AdvanceSlot moves the slot's restart_lsn forward.
func (m *InMemorySlotManager) AdvanceSlot(ctx context.Context, name string, lsn LSN) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot, exists := m.slots[name]
	if !exists {
		return ErrSlotNotFound
	}

	// Only advance if the new LSN is greater
	if lsn > slot.RestartLSN {
		slot.RestartLSN = lsn
	}

	return nil
}

// AcquireSlot marks a slot as active for a session.
func (m *InMemorySlotManager) AcquireSlot(ctx context.Context, name string, sessionID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot, exists := m.slots[name]
	if !exists {
		return ErrSlotNotFound
	}

	if slot.Active {
		return ErrSlotInUse
	}

	slot.Active = true
	m.activeConsumers[name] = sessionID

	return nil
}

// ReleaseSlot marks a slot as inactive.
func (m *InMemorySlotManager) ReleaseSlot(ctx context.Context, name string, sessionID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot, exists := m.slots[name]
	if !exists {
		return ErrSlotNotFound
	}

	// Only release if the same session owns it
	if m.activeConsumers[name] != sessionID {
		return ErrSlotInUse
	}

	slot.Active = false
	delete(m.activeConsumers, name)

	return nil
}

// UpdateConfirmedFlushLSN updates the confirmed flush position.
func (m *InMemorySlotManager) UpdateConfirmedFlushLSN(ctx context.Context, name string, lsn LSN) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot, exists := m.slots[name]
	if !exists {
		return ErrSlotNotFound
	}

	if lsn > slot.ConfirmedFlushLSN {
		slot.ConfirmedFlushLSN = lsn
	}

	return nil
}

// CleanupTemporarySlots removes all temporary slots for a session.
func (m *InMemorySlotManager) CleanupTemporarySlots(ctx context.Context, sessionID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name := range m.temporarySlots {
		if consumer, ok := m.activeConsumers[name]; ok && consumer == sessionID {
			delete(m.slots, name)
			delete(m.temporarySlots, name)
			delete(m.activeConsumers, name)
		}
	}
}

// nextLSN generates the next LSN value.
func (m *InMemorySlotManager) nextLSN() LSN {
	m.lsnCounter++
	return LSN(m.lsnCounter)
}

// CurrentLSN returns the current LSN position.
func (m *InMemorySlotManager) CurrentLSN() LSN {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return LSN(m.lsnCounter)
}

// =============================================================================
// WAL SENDER IMPLEMENTATION
// =============================================================================

// WALSender handles streaming of changes to a replication subscriber.
type WALSender struct {
	mu sync.Mutex

	// manager is the replication slot manager.
	manager *InMemorySlotManager

	// publications is the publication manager.
	publications *PublicationManager

	// slot is the active replication slot.
	slot *ReplicationSlot

	// session is the replication session.
	session *Session

	// streaming indicates if we're actively streaming.
	streaming bool

	// lastSentLSN tracks the last LSN sent to the client.
	lastSentLSN LSN

	// lastWriteLSN tracks the last write LSN (server's current position).
	lastWriteLSN LSN

	// lastFlushLSN tracks the last flushed LSN (confirmed durable on server).
	lastFlushLSN LSN

	// lastApplyLSN tracks the last applied LSN (confirmed by client).
	lastApplyLSN LSN

	// lastFeedbackTime tracks when we last received client feedback.
	lastFeedbackTime time.Time

	// keepaliveInterval is the interval between keepalive messages.
	keepaliveInterval time.Duration

	// changeBuffer holds pending changes to be sent.
	changeBuffer []*ChangeRecord

	// sendChan is used to signal when new changes are available.
	sendChan chan struct{}

	// stopChan signals the sender to stop.
	stopChan chan struct{}

	// plugin is the output plugin for encoding changes.
	plugin OutputPlugin
}

// NewWALSender creates a new WAL sender for streaming changes.
func NewWALSender(manager *InMemorySlotManager, publications *PublicationManager, session *Session) *WALSender {
	return &WALSender{
		manager:           manager,
		publications:      publications,
		session:           session,
		keepaliveInterval: 10 * time.Second,
		sendChan:          make(chan struct{}, 1),
		stopChan:          make(chan struct{}),
	}
}

// StartReplication starts streaming changes from the given LSN.
func (w *WALSender) StartReplication(ctx context.Context, slotName string, startLSN LSN, options map[string]string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Acquire the slot
	if err := w.manager.AcquireSlot(ctx, slotName, w.session.ID()); err != nil {
		return err
	}

	slot, err := w.manager.GetSlot(ctx, slotName)
	if err != nil {
		_ = w.manager.ReleaseSlot(ctx, slotName, w.session.ID())
		return err
	}

	w.slot = slot
	w.streaming = true
	w.lastSentLSN = startLSN

	// Initialize output plugin based on slot configuration
	w.plugin = NewPgOutputPlugin(w.publications)
	if err := w.plugin.Startup(ctx, options); err != nil {
		_ = w.manager.ReleaseSlot(ctx, slotName, w.session.ID())
		return err
	}

	return nil
}

// StopReplication stops streaming and releases the slot.
func (w *WALSender) StopReplication(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.streaming {
		return nil
	}

	w.streaming = false

	// Signal stop
	close(w.stopChan)

	// Shutdown plugin
	if w.plugin != nil {
		_ = w.plugin.Shutdown(ctx)
	}

	// Release slot
	if w.slot != nil {
		_ = w.manager.ReleaseSlot(ctx, w.slot.Name, w.session.ID())
	}

	return nil
}

// HandleStandbyStatusUpdate processes a status update from the subscriber.
func (w *WALSender) HandleStandbyStatusUpdate(ctx context.Context, msg *StandbyStatusUpdate) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Update tracking
	if msg.WritePosition > w.lastWriteLSN {
		w.lastWriteLSN = msg.WritePosition
	}
	if msg.FlushPosition > w.lastFlushLSN {
		w.lastFlushLSN = msg.FlushPosition
	}
	if msg.ApplyPosition > w.lastApplyLSN {
		w.lastApplyLSN = msg.ApplyPosition
	}

	w.lastFeedbackTime = time.Now()

	// Update slot's confirmed flush position
	if w.slot != nil && msg.FlushPosition > 0 {
		_ = w.manager.UpdateConfirmedFlushLSN(ctx, w.slot.Name, msg.FlushPosition)
	}

	return nil
}

// EnqueueChange adds a change to be streamed.
func (w *WALSender) EnqueueChange(change *ChangeRecord) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.streaming {
		return
	}

	// Check if this change matches the publication filter
	if w.publications != nil && !w.publications.ShouldReplicate(change) {
		return
	}

	w.changeBuffer = append(w.changeBuffer, change)

	// Signal that new changes are available
	select {
	case w.sendChan <- struct{}{}:
	default:
	}
}

// BuildXLogData builds an XLogData message for a change.
func (w *WALSender) BuildXLogData(change *ChangeRecord) (*XLogData, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.plugin == nil {
		return nil, ErrReplicationNotConnected
	}

	// Encode the change using the output plugin
	var data []byte
	var err error

	switch change.Type {
	case ChangeInsert:
		data, err = w.plugin.Insert(change.Schema+"."+change.Table, change.NewRow)
	case ChangeUpdate:
		data, err = w.plugin.Update(change.Schema+"."+change.Table, change.OldRow, change.NewRow)
	case ChangeDelete:
		data, err = w.plugin.Delete(change.Schema+"."+change.Table, change.OldRow)
	}

	if err != nil {
		return nil, err
	}

	return &XLogData{
		WALStart:   change.LSN,
		WALEnd:     change.LSN,
		ServerTime: time.Now(),
		Data:       data,
	}, nil
}

// BuildKeepalive builds a primary keepalive message.
func (w *WALSender) BuildKeepalive(requestReply bool) *PrimaryKeepalive {
	w.mu.Lock()
	defer w.mu.Unlock()

	return &PrimaryKeepalive{
		WALEnd:            w.manager.CurrentLSN(),
		ServerTime:        time.Now(),
		ReplyRequested:    requestReply,
	}
}

// IsStreaming returns whether the sender is actively streaming.
func (w *WALSender) IsStreaming() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.streaming
}

// =============================================================================
// WIRE PROTOCOL MESSAGES
// =============================================================================

// XLogData represents a WAL data message (message type 'w').
type XLogData struct {
	// WALStart is the starting point of the WAL data.
	WALStart LSN

	// WALEnd is the current end of WAL on the server.
	WALEnd LSN

	// ServerTime is the server's system clock at message send time.
	ServerTime time.Time

	// Data is the actual WAL data.
	Data []byte
}

// Encode encodes the XLogData message for the wire protocol.
func (x *XLogData) Encode() []byte {
	// Message format:
	// Byte1('w') - Identifies as XLogData
	// Int64 - WAL start position
	// Int64 - WAL end position
	// Int64 - Server time (microseconds since 2000-01-01)
	// Byten - WAL data
	buf := make([]byte, 1+8+8+8+len(x.Data))
	buf[0] = 'w'
	binary.BigEndian.PutUint64(buf[1:9], uint64(x.WALStart))
	binary.BigEndian.PutUint64(buf[9:17], uint64(x.WALEnd))
	binary.BigEndian.PutUint64(buf[17:25], pgTimestamp(x.ServerTime))
	copy(buf[25:], x.Data)
	return buf
}

// PrimaryKeepalive represents a server keepalive message (message type 'k').
type PrimaryKeepalive struct {
	// WALEnd is the current end of WAL on the server.
	WALEnd LSN

	// ServerTime is the server's system clock.
	ServerTime time.Time

	// ReplyRequested indicates if the client should send a status update.
	ReplyRequested bool
}

// Encode encodes the PrimaryKeepalive message for the wire protocol.
func (k *PrimaryKeepalive) Encode() []byte {
	// Message format:
	// Byte1('k') - Identifies as PrimaryKeepalive
	// Int64 - WAL end position
	// Int64 - Server time
	// Byte1 - Reply requested (1 = true, 0 = false)
	buf := make([]byte, 1+8+8+1)
	buf[0] = 'k'
	binary.BigEndian.PutUint64(buf[1:9], uint64(k.WALEnd))
	binary.BigEndian.PutUint64(buf[9:17], pgTimestamp(k.ServerTime))
	if k.ReplyRequested {
		buf[17] = 1
	} else {
		buf[17] = 0
	}
	return buf
}

// StandbyStatusUpdate represents a client status update (message type 'r').
type StandbyStatusUpdate struct {
	// WritePosition is the location of the last WAL byte + 1 received.
	WritePosition LSN

	// FlushPosition is the location of the last WAL byte + 1 flushed to disk.
	FlushPosition LSN

	// ApplyPosition is the location of the last WAL byte + 1 applied.
	ApplyPosition LSN

	// ClientTime is the client's system clock.
	ClientTime time.Time

	// ReplyRequested indicates if the server should reply immediately.
	ReplyRequested bool
}

// DecodeStandbyStatusUpdate decodes a client status update message.
func DecodeStandbyStatusUpdate(data []byte) (*StandbyStatusUpdate, error) {
	// Message format:
	// Byte1('r') - Identifies as StandbyStatusUpdate (already consumed)
	// Int64 - Write position
	// Int64 - Flush position
	// Int64 - Apply position
	// Int64 - Client time
	// Byte1 - Reply requested
	if len(data) < 33 {
		return nil, fmt.Errorf("invalid StandbyStatusUpdate message length")
	}

	return &StandbyStatusUpdate{
		WritePosition:  LSN(binary.BigEndian.Uint64(data[0:8])),
		FlushPosition:  LSN(binary.BigEndian.Uint64(data[8:16])),
		ApplyPosition:  LSN(binary.BigEndian.Uint64(data[16:24])),
		ClientTime:     pgTimestampToTime(binary.BigEndian.Uint64(data[24:32])),
		ReplyRequested: data[32] != 0,
	}, nil
}

// pgTimestamp converts a Go time to PostgreSQL timestamp (microseconds since 2000-01-01).
func pgTimestamp(t time.Time) uint64 {
	// PostgreSQL epoch is 2000-01-01 00:00:00 UTC
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return uint64(t.Sub(pgEpoch).Microseconds())
}

// pgTimestampToTime converts a PostgreSQL timestamp to Go time.
func pgTimestampToTime(ts uint64) time.Time {
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return pgEpoch.Add(time.Duration(ts) * time.Microsecond)
}

// =============================================================================
// PUBLICATION MANAGER
// =============================================================================

// PublicationManager manages publications for logical replication.
type PublicationManager struct {
	mu           sync.RWMutex
	publications map[string]*Publication
}

// NewPublicationManager creates a new publication manager.
func NewPublicationManager() *PublicationManager {
	return &PublicationManager{
		publications: make(map[string]*Publication),
	}
}

// CreatePublication creates a new publication.
func (pm *PublicationManager) CreatePublication(name string, allTables bool, tables []string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.publications[name]; exists {
		return fmt.Errorf("publication \"%s\" already exists", name)
	}

	pm.publications[name] = &Publication{
		Name:             name,
		AllTables:        allTables,
		Tables:           tables,
		PublishInserts:   true,
		PublishUpdates:   true,
		PublishDeletes:   true,
		PublishTruncates: true,
	}

	return nil
}

// DropPublication removes a publication.
func (pm *PublicationManager) DropPublication(name string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.publications[name]; !exists {
		return fmt.Errorf("publication \"%s\" does not exist", name)
	}

	delete(pm.publications, name)
	return nil
}

// AlterPublication modifies a publication.
func (pm *PublicationManager) AlterPublication(name string, addTables, dropTables []string, setAllTables *bool) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pub, exists := pm.publications[name]
	if !exists {
		return fmt.Errorf("publication \"%s\" does not exist", name)
	}

	// Handle SET ALL TABLES
	if setAllTables != nil {
		pub.AllTables = *setAllTables
		if *setAllTables {
			pub.Tables = nil
		}
	}

	// Add tables
	for _, t := range addTables {
		found := false
		for _, existing := range pub.Tables {
			if existing == t {
				found = true
				break
			}
		}
		if !found {
			pub.Tables = append(pub.Tables, t)
		}
	}

	// Drop tables
	for _, t := range dropTables {
		for i, existing := range pub.Tables {
			if existing == t {
				pub.Tables = append(pub.Tables[:i], pub.Tables[i+1:]...)
				break
			}
		}
	}

	return nil
}

// GetPublication retrieves a publication by name.
func (pm *PublicationManager) GetPublication(name string) (*Publication, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pub, exists := pm.publications[name]
	if !exists {
		return nil, fmt.Errorf("publication \"%s\" does not exist", name)
	}

	// Return a copy
	pubCopy := *pub
	pubCopy.Tables = append([]string{}, pub.Tables...)
	return &pubCopy, nil
}

// ListPublications returns all publications.
func (pm *PublicationManager) ListPublications() []*Publication {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make([]*Publication, 0, len(pm.publications))
	for _, pub := range pm.publications {
		pubCopy := *pub
		pubCopy.Tables = append([]string{}, pub.Tables...)
		result = append(result, &pubCopy)
	}

	return result
}

// ShouldReplicate checks if a change should be replicated.
func (pm *PublicationManager) ShouldReplicate(change *ChangeRecord) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// If no publications, replicate nothing
	if len(pm.publications) == 0 {
		return false
	}

	tableName := change.Schema + "." + change.Table

	for _, pub := range pm.publications {
		// Check operation type
		switch change.Type {
		case ChangeInsert:
			if !pub.PublishInserts {
				continue
			}
		case ChangeUpdate:
			if !pub.PublishUpdates {
				continue
			}
		case ChangeDelete:
			if !pub.PublishDeletes {
				continue
			}
		}

		// Check table filter
		if pub.AllTables {
			return true
		}

		for _, t := range pub.Tables {
			if t == tableName || t == change.Table {
				return true
			}
		}
	}

	return false
}

// =============================================================================
// PGOUTPUT PLUGIN IMPLEMENTATION
// =============================================================================

// PgOutputPlugin implements the pgoutput logical decoding plugin.
// This is the native PostgreSQL output plugin format.
type PgOutputPlugin struct {
	publications *PublicationManager
	protoVersion int
	pubNames     []string
}

// NewPgOutputPlugin creates a new pgoutput plugin instance.
func NewPgOutputPlugin(publications *PublicationManager) *PgOutputPlugin {
	return &PgOutputPlugin{
		publications: publications,
		protoVersion: 1,
	}
}

// Name returns the plugin name.
func (p *PgOutputPlugin) Name() string {
	return "pgoutput"
}

// Startup initializes the plugin.
func (p *PgOutputPlugin) Startup(ctx context.Context, options map[string]string) error {
	// Parse options
	if v, ok := options["proto_version"]; ok {
		if pv, err := strconv.Atoi(v); err == nil {
			p.protoVersion = pv
		}
	}

	if v, ok := options["publication_names"]; ok {
		p.pubNames = strings.Split(v, ",")
		for i := range p.pubNames {
			p.pubNames[i] = strings.TrimSpace(p.pubNames[i])
		}
	}

	return nil
}

// Shutdown cleans up the plugin.
func (p *PgOutputPlugin) Shutdown(ctx context.Context) error {
	return nil
}

// BeginTransaction encodes a transaction begin message.
func (p *PgOutputPlugin) BeginTransaction(xid uint32, commitTime time.Time, finalLSN LSN) ([]byte, error) {
	// pgoutput BEGIN message format:
	// Byte1('B') - Message type
	// Int64 - Final LSN of transaction
	// Int64 - Commit timestamp
	// Int32 - XID
	buf := make([]byte, 1+8+8+4)
	buf[0] = 'B'
	binary.BigEndian.PutUint64(buf[1:9], uint64(finalLSN))
	binary.BigEndian.PutUint64(buf[9:17], pgTimestamp(commitTime))
	binary.BigEndian.PutUint32(buf[17:21], xid)
	return buf, nil
}

// CommitTransaction encodes a transaction commit message.
func (p *PgOutputPlugin) CommitTransaction(xid uint32, commitTime time.Time, commitLSN LSN) ([]byte, error) {
	// pgoutput COMMIT message format:
	// Byte1('C') - Message type
	// Byte1 - Flags (always 0)
	// Int64 - Commit LSN
	// Int64 - Transaction end LSN
	// Int64 - Commit timestamp
	buf := make([]byte, 1+1+8+8+8)
	buf[0] = 'C'
	buf[1] = 0
	binary.BigEndian.PutUint64(buf[2:10], uint64(commitLSN))
	binary.BigEndian.PutUint64(buf[10:18], uint64(commitLSN))
	binary.BigEndian.PutUint64(buf[18:26], pgTimestamp(commitTime))
	return buf, nil
}

// Insert encodes an INSERT change.
func (p *PgOutputPlugin) Insert(relation string, newRow map[string]interface{}) ([]byte, error) {
	// First send Relation message, then Insert message
	var buf []byte

	// Relation message
	relMsg := p.encodeRelation(relation, newRow)
	buf = append(buf, relMsg...)

	// Insert message
	// Byte1('I') - Message type
	// Int32 - Relation ID (we use a hash of the relation name)
	// Byte1('N') - Identifies new tuple follows
	// TupleData - New tuple data
	insertBuf := []byte{'I'}
	insertBuf = binary.BigEndian.AppendUint32(insertBuf, hashRelation(relation))
	insertBuf = append(insertBuf, 'N')
	insertBuf = append(insertBuf, p.encodeTuple(newRow)...)

	buf = append(buf, insertBuf...)
	return buf, nil
}

// Update encodes an UPDATE change.
func (p *PgOutputPlugin) Update(relation string, oldRow, newRow map[string]interface{}) ([]byte, error) {
	var buf []byte

	// Relation message
	relMsg := p.encodeRelation(relation, newRow)
	buf = append(buf, relMsg...)

	// Update message
	// Byte1('U') - Message type
	// Int32 - Relation ID
	// Byte1 - 'K' (key), 'O' (old), or 'N' (new) to identify tuple type
	// TupleData - Tuple data
	updateBuf := []byte{'U'}
	updateBuf = binary.BigEndian.AppendUint32(updateBuf, hashRelation(relation))

	// Include old tuple if available
	if oldRow != nil && len(oldRow) > 0 {
		updateBuf = append(updateBuf, 'O')
		updateBuf = append(updateBuf, p.encodeTuple(oldRow)...)
	}

	// New tuple
	updateBuf = append(updateBuf, 'N')
	updateBuf = append(updateBuf, p.encodeTuple(newRow)...)

	buf = append(buf, updateBuf...)
	return buf, nil
}

// Delete encodes a DELETE change.
func (p *PgOutputPlugin) Delete(relation string, oldRow map[string]interface{}) ([]byte, error) {
	var buf []byte

	// Relation message
	relMsg := p.encodeRelation(relation, oldRow)
	buf = append(buf, relMsg...)

	// Delete message
	// Byte1('D') - Message type
	// Int32 - Relation ID
	// Byte1 - 'K' (key) or 'O' (old) to identify tuple type
	// TupleData - Tuple data
	deleteBuf := []byte{'D'}
	deleteBuf = binary.BigEndian.AppendUint32(deleteBuf, hashRelation(relation))
	deleteBuf = append(deleteBuf, 'O')
	deleteBuf = append(deleteBuf, p.encodeTuple(oldRow)...)

	buf = append(buf, deleteBuf...)
	return buf, nil
}

// encodeRelation encodes a Relation message.
func (p *PgOutputPlugin) encodeRelation(relation string, row map[string]interface{}) []byte {
	// Relation message format:
	// Byte1('R') - Message type
	// Int32 - Relation ID
	// String - Namespace (schema)
	// String - Relation name
	// Byte1 - Replica identity
	// Int16 - Number of columns
	// Column definitions...

	parts := strings.SplitN(relation, ".", 2)
	schema := "public"
	table := relation
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	}

	buf := []byte{'R'}
	buf = binary.BigEndian.AppendUint32(buf, hashRelation(relation))
	buf = append(buf, []byte(schema)...)
	buf = append(buf, 0) // null terminator
	buf = append(buf, []byte(table)...)
	buf = append(buf, 0)                                            // null terminator
	buf = append(buf, 'd')                                          // replica identity: default
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(row)))

	// Column definitions
	for colName := range row {
		// Byte1 - Column flags
		buf = append(buf, 0)
		// String - Column name
		buf = append(buf, []byte(colName)...)
		buf = append(buf, 0)
		// Int32 - Type OID (use text for simplicity)
		buf = binary.BigEndian.AppendUint32(buf, OidText)
		// Int32 - Type modifier
		buf = binary.BigEndian.AppendUint32(buf, 0xFFFFFFFF)
	}

	return buf
}

// encodeTuple encodes a tuple (row) of data.
func (p *PgOutputPlugin) encodeTuple(row map[string]interface{}) []byte {
	// Tuple format:
	// Int16 - Number of columns
	// For each column:
	//   Byte1 - Column type ('n' = null, 'u' = unchanged, 't' = text)
	//   Int32 - Length (if type is 't')
	//   Byten - Data (if type is 't')

	buf := binary.BigEndian.AppendUint16(nil, uint16(len(row)))

	for _, v := range row {
		if v == nil {
			buf = append(buf, 'n') // null
		} else {
			buf = append(buf, 't') // text
			text := fmt.Sprintf("%v", v)
			buf = binary.BigEndian.AppendUint32(buf, uint32(len(text)))
			buf = append(buf, []byte(text)...)
		}
	}

	return buf
}

// hashRelation creates a stable hash for a relation name.
func hashRelation(relation string) uint32 {
	var h uint32
	for _, c := range relation {
		h = h*31 + uint32(c)
	}
	return h
}

// =============================================================================
// REPLICATION HANDLER
// =============================================================================

// ReplicationHandler handles replication protocol commands.
type ReplicationHandler struct {
	mu sync.RWMutex

	// slotManager manages replication slots.
	slotManager *InMemorySlotManager

	// publications manages publications.
	publications *PublicationManager

	// activeWALSenders tracks active WAL senders per session.
	activeWALSenders map[uint64]*WALSender
}

// NewReplicationHandler creates a new replication handler.
func NewReplicationHandler() *ReplicationHandler {
	return &ReplicationHandler{
		slotManager:      NewInMemorySlotManager(),
		publications:     NewPublicationManager(),
		activeWALSenders: make(map[uint64]*WALSender),
	}
}

// HandleIdentifySystem handles the IDENTIFY_SYSTEM command.
func (h *ReplicationHandler) HandleIdentifySystem(ctx context.Context) (systemID string, timeline int32, xlogPos LSN, dbName string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return "dukdb-replication", 1, h.slotManager.CurrentLSN(), "dukdb"
}

// HandleCreateReplicationSlot handles CREATE_REPLICATION_SLOT command.
func (h *ReplicationHandler) HandleCreateReplicationSlot(ctx context.Context, name, plugin string, temporary bool) (*ReplicationSlot, error) {
	return h.slotManager.CreateSlot(ctx, name, plugin, temporary)
}

// HandleDropReplicationSlot handles DROP_REPLICATION_SLOT command.
func (h *ReplicationHandler) HandleDropReplicationSlot(ctx context.Context, name string) error {
	return h.slotManager.DropSlot(ctx, name)
}

// HandleStartReplication handles START_REPLICATION command.
func (h *ReplicationHandler) HandleStartReplication(ctx context.Context, session *Session, slotName string, startLSN LSN, options map[string]string) (*WALSender, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if session already has an active WAL sender
	if _, exists := h.activeWALSenders[session.ID()]; exists {
		return nil, fmt.Errorf("session already has an active replication connection")
	}

	// Create WAL sender
	sender := NewWALSender(h.slotManager, h.publications, session)
	if err := sender.StartReplication(ctx, slotName, startLSN, options); err != nil {
		return nil, err
	}

	h.activeWALSenders[session.ID()] = sender
	return sender, nil
}

// HandleStopReplication handles stopping a replication stream.
func (h *ReplicationHandler) HandleStopReplication(ctx context.Context, sessionID uint64) error {
	h.mu.Lock()
	sender, exists := h.activeWALSenders[sessionID]
	if !exists {
		h.mu.Unlock()
		return nil
	}
	delete(h.activeWALSenders, sessionID)
	h.mu.Unlock()

	return sender.StopReplication(ctx)
}

// CreatePublication creates a new publication.
func (h *ReplicationHandler) CreatePublication(name string, allTables bool, tables []string) error {
	return h.publications.CreatePublication(name, allTables, tables)
}

// DropPublication drops a publication.
func (h *ReplicationHandler) DropPublication(name string) error {
	return h.publications.DropPublication(name)
}

// AlterPublication alters a publication.
func (h *ReplicationHandler) AlterPublication(name string, addTables, dropTables []string, setAllTables *bool) error {
	return h.publications.AlterPublication(name, addTables, dropTables, setAllTables)
}

// GetPublication returns a publication by name.
func (h *ReplicationHandler) GetPublication(name string) (*Publication, error) {
	return h.publications.GetPublication(name)
}

// ListPublications returns all publications.
func (h *ReplicationHandler) ListPublications() []*Publication {
	return h.publications.ListPublications()
}

// SlotManager returns the slot manager.
func (h *ReplicationHandler) SlotManager() *InMemorySlotManager {
	return h.slotManager
}

// PublicationManager returns the publication manager.
func (h *ReplicationHandler) PublicationManager() *PublicationManager {
	return h.publications
}

// EnqueueChange broadcasts a change to all active WAL senders.
func (h *ReplicationHandler) EnqueueChange(change *ChangeRecord) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, sender := range h.activeWALSenders {
		sender.EnqueueChange(change)
	}
}

// =============================================================================
// UPDATED REPLICATION MANAGER
// =============================================================================

// IsReplicationSupported returns whether replication is currently supported.
// Updated to return true now that implementation is complete.
func (rm *ReplicationManager) IsReplicationSupported() bool {
	return true
}

// GetSlot retrieves a slot by name.
func (rm *ReplicationManager) GetSlot(ctx context.Context, name string) (*ReplicationSlot, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	slot, exists := rm.slots[name]
	if !exists {
		return nil, ErrSlotNotFound
	}

	slotCopy := *slot
	return &slotCopy, nil
}

// ListSlots returns all replication slots.
func (rm *ReplicationManager) ListSlots(ctx context.Context) ([]*ReplicationSlot, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make([]*ReplicationSlot, 0, len(rm.slots))
	for _, slot := range rm.slots {
		slotCopy := *slot
		result = append(result, &slotCopy)
	}

	return result, nil
}

// CreatePublication creates a new publication.
func (rm *ReplicationManager) CreatePublication(name string, allTables bool, tables []string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.publications[name]; exists {
		return fmt.Errorf("publication \"%s\" already exists", name)
	}

	rm.publications[name] = &Publication{
		Name:             name,
		AllTables:        allTables,
		Tables:           tables,
		PublishInserts:   true,
		PublishUpdates:   true,
		PublishDeletes:   true,
		PublishTruncates: true,
	}

	return nil
}

// DropPublication drops a publication.
func (rm *ReplicationManager) DropPublication(name string) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.publications[name]; !exists {
		return fmt.Errorf("publication \"%s\" does not exist", name)
	}

	delete(rm.publications, name)
	return nil
}

// GetPublication retrieves a publication by name.
func (rm *ReplicationManager) GetPublication(name string) (*Publication, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	pub, exists := rm.publications[name]
	if !exists {
		return nil, fmt.Errorf("publication \"%s\" does not exist", name)
	}

	pubCopy := *pub
	return &pubCopy, nil
}

// ListPublications returns all publications.
func (rm *ReplicationManager) ListPublications() []*Publication {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make([]*Publication, 0, len(rm.publications))
	for _, pub := range rm.publications {
		pubCopy := *pub
		result = append(result, &pubCopy)
	}

	return result
}

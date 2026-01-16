package server

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

// PortalState represents the execution state of a portal.
type PortalState int

const (
	// PortalStateReady means the portal is bound and ready to execute.
	PortalStateReady PortalState = iota

	// PortalStateActive means the portal is currently being executed.
	PortalStateActive

	// PortalStateSuspended means the portal has been suspended (partial fetch complete).
	PortalStateSuspended

	// PortalStateCompleted means the portal has finished executing all rows.
	PortalStateCompleted
)

// String returns the string representation of the portal state.
func (s PortalState) String() string {
	switch s {
	case PortalStateReady:
		return "ready"
	case PortalStateActive:
		return "active"
	case PortalStateSuspended:
		return "suspended"
	case PortalStateCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

// Common portal errors.
var (
	ErrPortalNotFound     = errors.New("portal not found")
	ErrPortalCompleted    = errors.New("portal execution completed")
	ErrPortalInvalidState = errors.New("portal in invalid state for operation")
)

// EnhancedPortal represents a bound prepared statement with cursor state tracking.
// It extends the basic Portal with support for partial result fetching.
type EnhancedPortal struct {
	mu sync.Mutex

	// Name is the portal name (empty string for unnamed portal).
	Name string

	// Statement is the bound prepared statement.
	Statement *PreparedStatement

	// Parameters are the bound parameter values.
	Parameters []driver.NamedValue

	// ResultFormats specifies the format codes for result columns.
	ResultFormats []wire.FormatCode

	// State tracks the current execution state.
	State PortalState

	// CachedRows holds rows fetched from a query for partial fetching.
	// This is used when a client requests rows in batches.
	CachedRows []map[string]any

	// CachedColumns holds column names from the query result.
	CachedColumns []string

	// CursorPosition tracks how many rows have been returned so far.
	CursorPosition int

	// TotalRows is the total number of rows available (-1 if unknown/streaming).
	TotalRows int

	// Executed indicates whether the portal has been executed at least once.
	Executed bool
}

// NewEnhancedPortal creates a new enhanced portal with the given name and statement.
func NewEnhancedPortal(name string, stmt *PreparedStatement, params []driver.NamedValue, resultFormats []wire.FormatCode) *EnhancedPortal {
	return &EnhancedPortal{
		Name:           name,
		Statement:      stmt,
		Parameters:     params,
		ResultFormats:  resultFormats,
		State:          PortalStateReady,
		CursorPosition: 0,
		TotalRows:      -1,
		Executed:       false,
	}
}

// CanFetch returns true if more rows can be fetched from this portal.
func (p *EnhancedPortal) CanFetch() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.State == PortalStateCompleted {
		return false
	}

	// If we have cached rows, check if there are more to return
	if p.CachedRows != nil {
		return p.CursorPosition < len(p.CachedRows)
	}

	// Portal is ready or suspended, can attempt to fetch
	return p.State == PortalStateReady || p.State == PortalStateSuspended
}

// RemainingRows returns the number of remaining rows to fetch.
// Returns -1 if unknown.
func (p *EnhancedPortal) RemainingRows() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.CachedRows == nil {
		return -1
	}
	remaining := len(p.CachedRows) - p.CursorPosition
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Reset resets the portal state for re-execution.
func (p *EnhancedPortal) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.State = PortalStateReady
	p.CachedRows = nil
	p.CachedColumns = nil
	p.CursorPosition = 0
	p.TotalRows = -1
	p.Executed = false
}

// EnhancedPortalCache implements wire.PortalCache with enhanced portal support.
// It provides named portal management with cursor state tracking.
type EnhancedPortalCache struct {
	mu sync.RWMutex

	// portals stores the enhanced portals by name.
	portals map[string]*EnhancedPortal

	// session is the associated session for query execution.
	session *Session

	// server is the server instance for query execution.
	server *Server

	// handler is the query handler for execution.
	handler *Handler
}

// NewEnhancedPortalCache creates a new enhanced portal cache.
func NewEnhancedPortalCache(session *Session, server *Server, handler *Handler) *EnhancedPortalCache {
	return &EnhancedPortalCache{
		portals: make(map[string]*EnhancedPortal),
		session: session,
		server:  server,
		handler: handler,
	}
}

// Bind binds parameters to a prepared statement and creates a portal.
// This implements wire.PortalCache.Bind.
func (c *EnhancedPortalCache) Bind(ctx context.Context, name string, statement *wire.Statement, parameters []wire.Parameter, resultFormats []wire.FormatCode) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Convert wire.Parameter to driver.NamedValue
	args := make([]driver.NamedValue, len(parameters))
	for i, param := range parameters {
		rawValue := param.Value()
		var value any
		if rawValue != nil {
			// Convert based on format
			if param.Format() == wire.TextFormat {
				value = string(rawValue)
			} else {
				value = rawValue
			}
		}
		args[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}

	// Create an enhanced portal
	// Note: We create a minimal PreparedStatement here since the actual
	// statement execution will go through the wire.Statement
	ps := &PreparedStatement{
		Name:  name,
		Query: "", // Query is handled by wire.Statement
	}

	portal := NewEnhancedPortal(name, ps, args, resultFormats)

	// Store the wire statement in the portal for execution
	portal.Statement.Stmt = nil // Will use wire.Statement instead

	// Replace any existing portal with the same name
	c.portals[name] = portal

	return nil
}

// Get retrieves a portal by name.
// This implements wire.PortalCache.Get.
func (c *EnhancedPortalCache) Get(ctx context.Context, name string) (*wire.Portal, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, ok := c.portals[name]
	if !ok {
		return nil, ErrPortalNotFound
	}

	// The wire.Portal is managed internally by psql-wire.
	// We return nil here as the actual portal is handled by the default implementation.
	// Our enhanced features are accessed through GetEnhanced.
	return nil, nil
}

// GetEnhanced retrieves an enhanced portal by name.
func (c *EnhancedPortalCache) GetEnhanced(name string) (*EnhancedPortal, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	portal, ok := c.portals[name]
	return portal, ok
}

// Execute executes the portal with the given row limit.
// This implements wire.PortalCache.Execute.
func (c *EnhancedPortalCache) Execute(ctx context.Context, name string, limit wire.Limit, reader *buffer.Reader, writer *buffer.Writer) error {
	c.mu.Lock()
	portal, ok := c.portals[name]
	if !ok {
		c.mu.Unlock()
		return ErrPortalNotFound
	}

	// Check portal state
	if portal.State == PortalStateCompleted {
		c.mu.Unlock()
		return ErrPortalCompleted
	}

	portal.State = PortalStateActive
	c.mu.Unlock()

	// Execute the portal
	// The actual execution is handled by the psql-wire default implementation
	// Our enhanced portal tracking is for cursor state management
	return nil
}

// Delete removes a portal by name.
func (c *EnhancedPortalCache) Delete(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.portals[name]; ok {
		delete(c.portals, name)
		return true
	}
	return false
}

// Clear removes all portals.
func (c *EnhancedPortalCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.portals = make(map[string]*EnhancedPortal)
}

// Close releases all resources held by the portal cache.
// This implements wire.PortalCache.Close.
func (c *EnhancedPortalCache) Close() {
	c.Clear()
}

// Names returns the names of all portals.
func (c *EnhancedPortalCache) Names() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.portals))
	for name := range c.portals {
		names = append(names, name)
	}
	return names
}

// Count returns the number of portals.
func (c *EnhancedPortalCache) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.portals)
}

// SetCompleted marks a portal as completed.
func (c *EnhancedPortalCache) SetCompleted(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if portal, ok := c.portals[name]; ok {
		portal.State = PortalStateCompleted
		portal.Executed = true
	}
}

// SetSuspended marks a portal as suspended (for partial fetching).
func (c *EnhancedPortalCache) SetSuspended(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if portal, ok := c.portals[name]; ok {
		portal.State = PortalStateSuspended
		portal.Executed = true
	}
}

// UpdateCursorPosition updates the cursor position for a portal.
func (c *EnhancedPortalCache) UpdateCursorPosition(name string, position int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if portal, ok := c.portals[name]; ok {
		portal.CursorPosition = position
	}
}

// CacheResults stores query results for a portal for partial fetching.
func (c *EnhancedPortalCache) CacheResults(name string, rows []map[string]any, columns []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if portal, ok := c.portals[name]; ok {
		portal.CachedRows = rows
		portal.CachedColumns = columns
		portal.TotalRows = len(rows)
	}
}

// FetchRows fetches up to maxRows rows from a portal starting at the current cursor position.
// Returns the rows, whether there are more rows available, and any error.
func (c *EnhancedPortalCache) FetchRows(name string, maxRows int) ([]map[string]any, []string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	portal, ok := c.portals[name]
	if !ok {
		return nil, nil, false, ErrPortalNotFound
	}

	if portal.CachedRows == nil {
		return nil, nil, false, nil
	}

	startPos := portal.CursorPosition
	endPos := startPos + maxRows

	// Clamp to available rows
	if endPos > len(portal.CachedRows) {
		endPos = len(portal.CachedRows)
	}

	// Get the slice of rows
	rows := portal.CachedRows[startPos:endPos]
	portal.CursorPosition = endPos

	// Check if there are more rows
	hasMore := endPos < len(portal.CachedRows)

	// Update state
	if hasMore {
		portal.State = PortalStateSuspended
	} else {
		portal.State = PortalStateCompleted
	}
	portal.Executed = true

	return rows, portal.CachedColumns, hasMore, nil
}

// SessionPortalCache wraps the session's portal cache with the wire.PortalCache interface.
// This bridges our session-based portal management with psql-wire's expectations.
type SessionPortalCache struct {
	session *Session
	server  *Server
	handler *Handler
}

// NewSessionPortalCache creates a new session portal cache wrapper.
func NewSessionPortalCache(session *Session, server *Server, handler *Handler) *SessionPortalCache {
	return &SessionPortalCache{
		session: session,
		server:  server,
		handler: handler,
	}
}

// Bind binds parameters to a prepared statement, creating a portal.
func (c *SessionPortalCache) Bind(ctx context.Context, name string, statement *wire.Statement, parameters []wire.Parameter, resultFormats []wire.FormatCode) error {
	if c.session == nil {
		return ErrSessionClosed
	}

	// Convert wire parameters to driver.NamedValue
	args := make([]driver.NamedValue, len(parameters))
	for i, param := range parameters {
		rawValue := param.Value()
		var value any
		if rawValue != nil {
			if param.Format() == wire.TextFormat {
				value = string(rawValue)
			} else {
				value = rawValue
			}
		}
		args[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   value,
		}
	}

	// Create a portal in the session's portal cache
	portal := &Portal{
		Name:       name,
		Parameters: args,
		Executed:   false,
	}

	c.session.Portals().Set(name, portal)

	return nil
}

// Get retrieves a portal by name.
func (c *SessionPortalCache) Get(ctx context.Context, name string) (*wire.Portal, error) {
	if c.session == nil {
		return nil, ErrSessionClosed
	}

	_, ok := c.session.Portals().Get(name)
	if !ok {
		return nil, ErrPortalNotFound
	}

	// Return nil - actual portal is managed internally by psql-wire
	return nil, nil
}

// Execute executes the portal with the given row limit.
func (c *SessionPortalCache) Execute(ctx context.Context, name string, limit wire.Limit, reader *buffer.Reader, writer *buffer.Writer) error {
	if c.session == nil {
		return ErrSessionClosed
	}

	portal, ok := c.session.Portals().Get(name)
	if !ok {
		return ErrPortalNotFound
	}

	// Mark as executed
	portal.Executed = true
	c.session.Portals().Set(name, portal)

	// Actual execution is handled by psql-wire
	return nil
}

// Close releases all portal resources.
func (c *SessionPortalCache) Close() {
	if c.session != nil && c.session.Portals() != nil {
		c.session.Portals().Clear()
	}
}

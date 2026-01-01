# Design: ConnId() Public API

## Architecture Overview

Simple two-layer design:

```
Public API (root package)
    ↓
Backend Connection ID (internal/engine/connection.go)
```

**Version Compatibility**: Targets duckdb-go v1.4.3 API compatibility.

## Layer 1: Backend Connection ID Tracking

Location: `internal/engine/connection.go` (or wherever backend Connection struct is defined)

### Global ID Counter

```go
// Package-level atomic counter for connection IDs
// Starts at 1 (0 reserved for invalid/uninitialized connections)
var nextConnID atomic.Uint64

func init() {
	nextConnID.Store(1)  // Initialize to 1
}

// generateConnID returns next unique connection ID
// Thread-safe via atomic increment
func generateConnID() uint64 {
	return nextConnID.Add(1) - 1  // Add returns new value, so subtract 1 to get previous
}
```

**Design Choice**: Use `sync/atomic.Uint64` for lock-free ID generation. This is faster than mutexes and sufficient for monotonic counter.

### Connection Struct Enhancement

```go
type Connection struct {
	// Existing fields...
	catalog  *catalog.Catalog
	executor *executor.Executor

	// NEW: Connection ID field
	id uint64  // Unique connection ID, assigned at creation time

	// Existing fields...
	closed bool
	mu     sync.Mutex
}

// NewConnection creates new connection with unique ID
func NewConnection(cat *catalog.Catalog, exec *executor.Executor) *Connection {
	return &Connection{
		catalog:  cat,
		executor: exec,
		id:       generateConnID(),  // Assign unique ID at creation
		closed:   false,
	}
}

// ID returns this connection's unique identifier
// Thread-safe - ID is immutable after construction
func (c *Connection) ID() uint64 {
	return c.id
}

// IsClosed returns whether connection is closed
// Thread-safe with mutex
func (c *Connection) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
```

**Design Choices**:
- **ID assigned at construction**: Once assigned, never changes (immutability)
- **No lock needed for ID()**: ID is immutable, safe to read without lock
- **Closed check uses existing mutex**: Reuse connection's mutex for consistency

## Layer 2: Public API

Location: Root package (`/connid.go` - new file)

```go
// ConnId returns the unique identifier for the given database connection.
//
// Each connection has a unique uint64 ID assigned at creation time.
// The ID is stable for the connection's lifetime and never changes.
// Different connections always have different IDs.
//
// Returns error if:
//   - Connection is nil
//   - Connection is closed
//   - Underlying connection cannot be accessed
//
// Example:
//   conn, _ := sql.Open("dukdb", "")
//   defer conn.Close()
//
//   id, err := dukdb.ConnId(conn.Conn(context.Background()))
//   if err != nil {
//       log.Fatal(err)
//   }
//   fmt.Printf("Connection ID: %d\n", id)
//
// Thread-safe: Can be called concurrently on same or different connections.
func ConnId(c *sql.Conn) (uint64, error) {
	if c == nil {
		return 0, fmt.Errorf("connection is nil")
	}

	// Extract underlying dukdb connection using database/sql Raw() method
	var connID uint64
	err := c.Raw(func(driverConn any) error {
		// Type assert to *Conn (our driver.Conn implementation)
		dukdbConn, ok := driverConn.(*Conn)
		if !ok {
			return fmt.Errorf("not a dukdb connection (got type %T)", driverConn)
		}

		// Check if connection is closed
		if dukdbConn.backend.IsClosed() {
			return fmt.Errorf("connection is closed")
		}

		// Get ID from backend connection
		connID = dukdbConn.backend.ID()
		return nil
	})

	if err != nil {
		return 0, err
	}

	return connID, nil
}
```

**Design Choices**:
- **Use `database/sql.Conn.Raw()`**: Standard Go way to access underlying driver connection
- **Type assertion to `*Conn`**: Ensures we have a dukdb connection, not some other driver
- **Check closed state**: Return error if connection is closed (matches reference behavior)
- **Return 0 on error**: 0 is invalid ID (counter starts at 1), clear error signal

## Error Handling

### Error Cases

1. **Nil connection**: `connection is nil` - user passed nil `*sql.Conn`
2. **Closed connection**: `connection is closed` - connection was already closed
3. **Wrong driver**: `not a dukdb connection` - connection is from different database driver
4. **Unknown error**: Propagate any unexpected error from Raw()

### Error Type

All errors returned as simple `error` type with descriptive messages. No custom error types needed for this simple API.

## Thread Safety

### ID Generation

- **`atomic.Uint64.Add()`**: Lock-free, thread-safe increment
- **No race conditions**: Multiple goroutines can create connections concurrently
- **Guaranteed uniqueness**: Atomic operation ensures each connection gets distinct ID

### ID Access

- **`Connection.ID()`**: No lock needed, ID is immutable after construction
- **`Connection.IsClosed()`**: Uses existing connection mutex for safety
- **Public `ConnId()`**: Thread-safe via immutable ID and closed check mutex

## ID Space Management

### ID Range

- **Type**: `uint64` (0 to 18,446,744,073,709,551,615)
- **Start value**: 1 (0 reserved for invalid)
- **Increment**: +1 per connection
- **Wraparound**: Extremely unlikely - 18 quintillion connections required

**Back-of-envelope calculation**:
- If creating 1,000,000 connections/second
- Would take 584,542 years to wrap around
- Wraparound not a practical concern

### ID Reuse

- **Never reused**: IDs increment monotonically, old IDs never recycled
- **No ID pool**: Simpler implementation, vast ID space makes pooling unnecessary
- **Per-process**: IDs reset when process restarts (ephemeral)

## Performance Considerations

### ID Generation Overhead

- **Atomic increment**: ~1-5ns on modern CPU (lock-free)
- **Once per connection**: Amortized over connection lifetime
- **Negligible impact**: Connection creation already has ms-level overhead

### ID Lookup Overhead

- **Direct field access**: ~1ns (one memory read)
- **No lock**: ID is immutable, no synchronization needed
- **Target**: <100ns for full ConnId() call including Raw() overhead

### Memory Overhead

- **8 bytes per connection**: One uint64 field
- **8 bytes global**: One atomic counter
- **Total**: Minimal, <1KB even with 100 concurrent connections

## Testing Strategy

### Unit Tests

```go
func TestConnId_Uniqueness(t *testing.T) {
	// Create 100 connections, verify all IDs are unique
	db, _ := sql.Open("dukdb", "")
	defer db.Close()

	ids := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		conn, _ := db.Conn(context.Background())
		id, err := dukdb.ConnId(conn)
		require.NoError(t, err)
		require.False(t, ids[id], "ID %d duplicated", id)
		ids[id] = true
		conn.Close()
	}
}

func TestConnId_Stability(t *testing.T) {
	// Call ConnId() 100 times on same connection, verify same ID returned
	db, _ := sql.Open("dukdb", "")
	defer db.Close()

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	firstID, _ := dukdb.ConnId(conn)
	for i := 0; i < 100; i++ {
		id, err := dukdb.ConnId(conn)
		require.NoError(t, err)
		assert.Equal(t, firstID, id)
	}
}

func TestConnId_ClosedConnection(t *testing.T) {
	// Close connection, verify ConnId() returns error
	db, _ := sql.Open("dukdb", "")
	defer db.Close()

	conn, _ := db.Conn(context.Background())
	conn.Close()

	id, err := dukdb.ConnId(conn)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
	assert.Equal(t, uint64(0), id)
}

func TestConnId_NilConnection(t *testing.T) {
	// Pass nil connection, verify error
	id, err := dukdb.ConnId(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
	assert.Equal(t, uint64(0), id)
}

func TestConnId_Concurrent(t *testing.T) {
	// Create 100 connections concurrently, verify all IDs unique
	db, _ := sql.Open("dukdb", "")
	defer db.Close()

	var wg sync.WaitGroup
	idChan := make(chan uint64, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, _ := db.Conn(context.Background())
			defer conn.Close()
			id, err := dukdb.ConnId(conn)
			require.NoError(t, err)
			idChan <- id
		}()
	}

	wg.Wait()
	close(idChan)

	ids := make(map[uint64]bool)
	for id := range idChan {
		require.False(t, ids[id], "ID %d duplicated in concurrent test", id)
		ids[id] = true
	}
}
```

### Compatibility Testing

```go
func TestConnId_CompatibilityWithReference(t *testing.T) {
	// Compare behavior against reference duckdb-go v1.4.3

	// Create connection with dukdb-go
	dukdbDB, _ := sql.Open("dukdb", "")
	defer dukdbDB.Close()
	dukdbConn, _ := dukdbDB.Conn(context.Background())
	defer dukdbConn.Close()

	// Create connection with reference duckdb-go
	refDB, _ := sql.Open("duckdb", "")
	defer refDB.Close()
	refConn, _ := refDB.Conn(context.Background())
	defer refConn.Close()

	// Get IDs from both
	dukdbID, err1 := dukdb.ConnId(dukdbConn)
	refID, err2 := duckdb.ConnId(refConn)

	// Both should succeed
	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// IDs should be non-zero
	assert.NotEqual(t, uint64(0), dukdbID)
	assert.NotEqual(t, uint64(0), refID)

	// Behavior should match (both return valid IDs)
	// Note: Actual ID values will differ (different processes)
}
```

### Performance Benchmarks

```go
func BenchmarkConnId(b *testing.B) {
	db, _ := sql.Open("dukdb", "")
	defer db.Close()

	conn, _ := db.Conn(context.Background())
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = dukdb.ConnId(conn)
	}
}

// Expected: <100ns per operation
```

## Dependencies

### Existing Infrastructure
- `database/sql` package - for `*sql.Conn` and `Raw()` method
- Backend Connection struct - needs ID field added
- Atomic operations - `sync/atomic.Uint64`

### New Components
- **ConnId() public function** (root package `connid.go`): NEW, ~40 lines
- **ID field in Connection** (internal/engine/connection.go): MODIFY, +1 field
- **generateConnID() helper** (internal/engine/connection.go): NEW, ~10 lines
- **nextConnID global counter** (internal/engine/connection.go): NEW, +1 global variable

### External Dependencies
- None - pure Go standard library

## Risks & Mitigation

### Risk 1: Backend Connection Structure Unknown
**Impact**: Low - need to locate Connection struct
**Mitigation**: Grep for "type Connection struct" in internal/ - likely in engine or similar
**Estimated Effort**: 15 minutes

### Risk 2: ID Counter Overflow
**Impact**: Extremely Low - would take hundreds of thousands of years
**Mitigation**: Use uint64 for maximum range, document as non-issue
**Estimated Effort**: 0 (no action needed)

### Risk 3: Closed Connection Detection
**Impact**: Medium - need reliable way to check if connection is closed
**Mitigation**: Add IsClosed() method to backend Connection if not present
**Estimated Effort**: 30 minutes

## Alternative Designs Considered

### Alternative: Store IDs in Driver instead of Connection
```go
type Driver struct {
	connections map[*Connection]uint64
	mu sync.Mutex
}
```

**Rejected**: Requires map lookup and mutex lock on every ConnId() call. Slower and more complex than storing ID in Connection directly.

### Alternative: Use Timestamp as ID
```go
id := time.Now().UnixNano()
```

**Rejected**: Not guaranteed unique if two connections created in same nanosecond. Atomic counter is simpler and guaranteed unique.

## Future Enhancements (Out of Scope)

1. **Connection Metadata API**: `ConnMetadata(c *sql.Conn) (*Metadata, error)` returning ID, creation time, query count, etc.
2. **Connection Name API**: Allow users to assign human-readable names to connections
3. **Connection Pool Statistics**: Track connection pool metrics (active, idle, max lifetime)
4. **Connection Debugging API**: Dump connection state for debugging

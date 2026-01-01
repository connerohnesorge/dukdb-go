# Change: Add ConnId() Public API for Connection Identification

## Why

dukdb-go currently lacks the `ConnId()` public API that exists in duckdb-go v1.4.3 (connection.go:337). This function returns a unique identifier for database connections, enabling per-connection state tracking, connection pooling strategies, and debugging of multi-connection scenarios. Without this API, users cannot distinguish connections or correlate connection-specific operations (queries, transactions) with specific connection instances.

## What

Implement `ConnId(c *sql.Conn) (uint64, error)` public API that:

1. **Extracts underlying connection** from `*sql.Conn` wrapper using `database/sql` Raw() method
2. **Returns unique connection ID** as uint64 (monotonically increasing)
3. **Maintains ID stability** - same connection always returns same ID throughout its lifetime
4. **Thread-safe ID generation** - concurrent connection creations get distinct IDs

## Impact

### Users Affected
- **Connection Pool Managers**: Can track which connections are active, idle, or stale
- **Monitoring/Observability Tools**: Can correlate queries with specific connections for tracing
- **Multi-Tenant Applications**: Can enforce connection isolation per tenant
- **Debug Tooling**: Can identify which connection executed specific operations

### Breaking Changes
None - this is additive functionality only.

### Performance Impact
- **ID generation**: O(1) atomic increment, negligible overhead
- **ID lookup**: O(1) map lookup or struct field access, <10ns typical
- **Memory**: 8 bytes per connection for uint64 ID

### Dependencies
- Backend connection wrapper (`internal/engine/connection.go` or similar) must track connection IDs
- No external dependencies required

## Alternatives Considered

### Alternative 1: Use Go's pointer address as ID
- **Pro**: No additional state needed, instantly available
- **Con**: Pointer addresses can be reused after GC, breaks ID stability guarantee
- **Rejected**: IDs must be stable and never reused

### Alternative 2: Generate random UUIDs instead of sequential IDs
- **Pro**: Globally unique, no collision risk across processes
- **Con**: 16 bytes instead of 8, more complex generation, harder to read/debug
- **Rejected**: Sequential IDs are simpler and sufficient for single-process use

### Alternative 3: Expose connection object directly instead of just ID
- **Pro**: Users get full connection access
- **Con**: Breaks encapsulation, allows unsafe operations
- **Rejected**: ID is sufficient for most use cases, maintains safety

## Success Criteria

- [ ] ConnId() function added to public API matching signature `func ConnId(c *sql.Conn) (uint64, error)`
- [ ] Returns unique uint64 ID for each connection
- [ ] Same connection always returns same ID (stable across calls)
- [ ] Different connections always return different IDs (uniqueness)
- [ ] Thread-safe ID generation (concurrent connections get distinct IDs)
- [ ] Error returned if connection is closed or invalid
- [ ] All unit tests pass (10+ test cases)
- [ ] Compatibility tests pass against reference duckdb-go v1.4.3
- [ ] Performance <100ns for ID lookup (negligible overhead)
- [ ] No regression in existing test suite

## Rollout Plan

### Phase 1: Backend Connection ID Tracking (Week 1)
- Add ID field to backend Connection struct
- Implement atomic ID generation (sync/atomic increment)
- Add ConnId() method to backend Connection
- Target: Backend exposes connection IDs

### Phase 2: Public API Implementation (Week 1)
- Add ConnId() public function using database/sql Raw()
- Extract backend connection and return ID
- Error handling for closed/invalid connections
- Target: Public API functional

### Phase 3: Testing & Validation (Week 1)
- Unit tests for ID uniqueness, stability, thread-safety
- Compatibility tests against reference implementation
- Performance benchmarks (<100ns target)
- Target: All tests passing

## Out of Scope

- Cross-process connection ID coordination (IDs only unique within process)
- Connection ID persistence across database restarts (IDs are ephemeral, reset on restart)
- Connection metadata beyond ID (name, creation time, etc. - future enhancement)
- Connection ID reservation or pre-allocation (sequential generation only)

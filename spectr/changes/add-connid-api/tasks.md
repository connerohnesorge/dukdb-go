# Implementation Tasks: Add ConnId() Public API

## Phase 1: Backend Connection ID Tracking

### Locate Backend Connection Struct
- [ ] Grep for "type Connection struct" in internal/ directories
- [ ] Verify Connection struct has access to closed state
- [ ] Document current Connection struct fields
- [ ] Identify where NewConnection() or equivalent is called

### Add Global ID Counter
- [ ] Add package-level `var nextConnID atomic.Uint64` to connection file
- [ ] Add init() function to initialize counter to 1
- [ ] Add generateConnID() helper function using atomic.Add()
- [ ] Test: Verify atomic increment works correctly

### Enhance Connection Struct
- [ ] Add `id uint64` field to Connection struct
- [ ] Modify NewConnection() to call generateConnID() and assign to id field
- [ ] Add `ID() uint64` method to Connection returning id field
- [ ] Add or verify `IsClosed() bool` method exists
- [ ] Test: Create connection, verify ID is assigned and non-zero

## Phase 2: Public API Implementation

### Create ConnId() Public Function
- [ ] Create connid.go in root package (new file)
- [ ] Implement ConnId(c *sql.Conn) (uint64, error) signature
- [ ] Handle nil connection check → return error "connection is nil"
- [ ] Use c.Raw() to access underlying driver connection
- [ ] Type assert to *Conn (driver.Conn implementation)
- [ ] Check if connection is closed → return error "connection is closed"
- [ ] Call backend.ID() and return
- [ ] Test: Basic integration test calling public API

### Error Handling
- [ ] Test nil connection returns error with "nil" in message
- [ ] Test closed connection returns error with "closed" in message
- [ ] Test wrong driver type returns error with "not a dukdb connection"
- [ ] Verify error cases return 0 for ID (invalid ID value)

## Phase 3: Unit Testing

### Basic Functionality Tests
- [ ] Add TestConnId_ReturnsNonZeroID: Verify ID is non-zero
- [ ] Add TestConnId_Uniqueness: Create 100 connections, verify all IDs unique
- [ ] Add TestConnId_Stability: Call ConnId() 100 times on same connection, verify same ID
- [ ] Add TestConnId_DifferentConnections: Verify different connections have different IDs

### Error Case Tests
- [ ] Add TestConnId_NilConnection: Pass nil, verify error
- [ ] Add TestConnId_ClosedConnection: Close connection, verify error
- [ ] Add TestConnId_ErrorReturnsZero: Verify error cases return 0 for ID

### Thread Safety Tests
- [ ] Add TestConnId_Concurrent: Create 100 connections concurrently, verify all IDs unique
- [ ] Add TestConnId_ConcurrentCalls: Call ConnId() on same connection from 10 goroutines, verify same ID
- [ ] Add TestConnId_NoRaceConditions: Run with -race flag, verify no races

### Edge Case Tests
- [ ] Add TestConnId_Sequential: Verify IDs increment sequentially (1, 2, 3, ...)
- [ ] Add TestConnId_AfterReopen: Close and reopen connection, verify new ID assigned
- [ ] Target: 10+ unit tests passing

## Phase 4: Compatibility Testing

### Reference Implementation Comparison
- [ ] Create compatibility/connid_test.go
- [ ] Implement comparison test against reference duckdb-go v1.4.3
- [ ] Verify both implementations return non-zero IDs
- [ ] Verify both implementations return errors for nil connections
- [ ] Verify both implementations return errors for closed connections
- [ ] Verify behavior matches reference for all error cases

### Cross-Driver Testing
- [ ] Test that ConnId() returns error when passed non-dukdb connection
- [ ] Verify error message includes "not a dukdb connection"

## Phase 5: Performance Benchmarking

### Benchmark Creation
- [ ] Create benchmark: BenchmarkConnId
- [ ] Benchmark: BenchmarkConnId_NewConnection (includes ID generation overhead)
- [ ] Benchmark: BenchmarkConnId_Concurrent (concurrent access)
- [ ] Target: <100ns per ConnId() call
- [ ] Target: <5ns for ID generation (atomic increment)

### Performance Validation
- [ ] Run benchmarks on real hardware
- [ ] Verify <100ns target met for ConnId()
- [ ] Verify <5ns target met for generateConnID()
- [ ] Profile any slow paths if targets not met

## Phase 6: Documentation & Polish

### Code Documentation
- [ ] Add godoc comments to ConnId() function with examples
- [ ] Add godoc comments to Connection.ID() method
- [ ] Add godoc comments to generateConnID() helper
- [ ] Add usage example showing connection ID tracking
- [ ] Verify godoc examples are runnable (Example_ConnId)

### Documentation Updates
- [ ] Add ConnId() to API reference (if exists)
- [ ] Add usage example: connection pooling with ID tracking
- [ ] Add usage example: query tracing with connection ID
- [ ] Document thread-safety guarantees
- [ ] Document ID space and wraparound behavior

### Error Message Polish
- [ ] Verify all error messages are clear and actionable
- [ ] Ensure error messages match reference implementation format
- [ ] Verify no panic paths (all errors returned gracefully)

## Phase 7: Final Validation

### Regression Testing
- [ ] Run full test suite: `nix develop -c gotestsum --format short-verbose ./...`
- [ ] Verify no regressions in existing tests
- [ ] Verify all new tests pass
- [ ] Run linter: `nix develop -c golangci-lint run`
- [ ] Fix any linting issues
- [ ] Run with race detector: `go test -race ./...`

### Compatibility Verification
- [ ] Run compatibility tests against reference duckdb-go v1.4.3
- [ ] Verify 100% API signature match
- [ ] Verify behavior matches for all test cases
- [ ] Document any intentional deviations (none expected)

### Performance Validation
- [ ] Confirm <100ns target for ConnId() call
- [ ] Confirm <5ns target for ID generation
- [ ] Verify no performance regression in connection creation
- [ ] Verify memory overhead is 8 bytes per connection (uint64)

### Final Checklist
- [ ] All unit tests pass (10+)
- [ ] All compatibility tests pass
- [ ] All benchmarks meet targets
- [ ] Zero regressions in existing test suite
- [ ] Code coverage >90% on new code (simple feature, high coverage expected)
- [ ] Documentation complete
- [ ] golangci-lint passes
- [ ] Race detector passes

## Parallel Work Opportunities

**Can be done in parallel**:
- Phase 1 (backend) and Phase 2 (public API) can overlap once Connection struct is located
- Phase 6 (documentation) can start anytime

**Must be sequential**:
- Phase 1 → Phase 2 (need backend ID tracking before public API)
- Phase 2 → Phase 3/4/5 (need public API before testing)
- Phase 7 depends on all previous phases (final validation)

## Definition of Done

- [ ] ConnId() public API implemented and tested
- [ ] Returns unique uint64 ID for each connection
- [ ] Same connection always returns same ID (stability)
- [ ] Different connections always return different IDs (uniqueness)
- [ ] Thread-safe ID generation and access
- [ ] Error handling for nil/closed connections
- [ ] 10+ unit tests pass
- [ ] Compatibility tests pass against reference duckdb-go v1.4.3
- [ ] Performance <100ns for ConnId() call
- [ ] Zero regressions in existing test suite
- [ ] Documentation complete with examples
- [ ] Code review approved
- [ ] CI pipeline green

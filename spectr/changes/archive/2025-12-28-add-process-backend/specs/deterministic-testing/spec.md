## ADDED Requirements

### Requirement: Clock Interface Injection

All time-dependent code SHALL use an injected quartz.Clock interface.

#### Scenario: Engine accepts clock parameter
- GIVEN a new Engine instance
- WHEN created with NewEngineWithClock(clock)
- THEN all time operations use the provided clock

#### Scenario: Default engine uses real clock
- GIVEN NewEngine() called without clock parameter
- WHEN engine performs time operations
- THEN quartz.NewReal() is used (passes through to time stdlib)

#### Scenario: Mock clock in tests
- GIVEN NewEngineWithClock(quartz.NewMock(t))
- WHEN tests advance the mock clock
- THEN engine time operations reflect the mocked time

### Requirement: Transaction Timestamps via Clock

Transaction timestamps SHALL be obtained from the injected clock.

#### Scenario: Transaction start time
- GIVEN a mock clock set to "2024-01-15 10:30:00 UTC"
- WHEN Begin() is called
- THEN transaction.StartTime equals "2024-01-15 10:30:00 UTC"

#### Scenario: Commit timestamp
- GIVEN a transaction started at T1
- AND mock clock advanced to T2
- WHEN Commit() is called
- THEN transaction.CommitTS uses time T2

### Requirement: CURRENT_TIMESTAMP via Clock

The CURRENT_TIMESTAMP SQL function SHALL use the injected clock.

#### Scenario: Deterministic current_timestamp
- GIVEN mock clock set to "2024-06-15 14:30:45.123456 UTC"
- WHEN executing "SELECT current_timestamp"
- THEN result equals "2024-06-15 14:30:45.123456 UTC"

#### Scenario: Multiple timestamps in transaction
- GIVEN a transaction
- WHEN executing multiple "SELECT current_timestamp" queries
- THEN all return the same value (per DuckDB semantics)

### Requirement: Query Timeout via Clock

Query timeout checking SHALL use clock.Until() not time.Until().

#### Scenario: Timeout detection
- GIVEN a query with 5-second context timeout
- AND mock clock
- WHEN clock is advanced 6 seconds during query
- THEN query returns context.DeadlineExceeded

#### Scenario: No timeout
- GIVEN a query with 5-second context timeout
- AND mock clock
- WHEN query completes before clock advances 5 seconds
- THEN query returns successfully

### Requirement: TickerFunc for Periodic Operations

Periodic operations SHALL use clock.TickerFunc for deterministic testing.

#### Scenario: WAL checkpoint interval
- GIVEN WALManager with 5-minute checkpoint interval
- AND mock clock
- WHEN clock is advanced 5 minutes
- THEN checkpoint is triggered
- AND Advance().MustWait() returns after checkpoint completes

#### Scenario: Connection pool cleanup
- GIVEN ConnPool with 10-minute idle timeout
- AND connection idle since T1
- AND mock clock at T1 + 11 minutes
- WHEN cleanup tick fires
- THEN idle connection is closed

### Requirement: Zero Flaky Tests Policy

Tests SHALL NOT use non-deterministic time constructs.

#### Scenario: No time.Sleep in tests
- GIVEN test codebase
- WHEN scanning for time.Sleep calls
- THEN zero occurrences in *_test.go files

#### Scenario: No runtime.Gosched for sync
- GIVEN test codebase
- WHEN scanning for runtime.Gosched calls
- THEN zero occurrences used for timing synchronization

#### Scenario: No polling loops
- GIVEN test codebase
- WHEN scanning for Eventually/Consistently patterns
- THEN zero occurrences without mock clock involvement

### Requirement: Trap-Based Synchronization

Asynchronous clock operations SHALL be synchronized using traps.

#### Scenario: Trap async clock call
- GIVEN async goroutine that calls clock.Now()
- WHEN trap is set with mClock.Trap().Now()
- THEN trap.Wait() blocks until call occurs
- AND call.Release() allows call to return

#### Scenario: Tagged traps
- GIVEN multiple clock calls with different tags
- WHEN trap is set with specific tag
- THEN only matching calls are trapped

### Requirement: Clock Propagation to Operators

ExecutionContext SHALL propagate clock to all operators.

#### Scenario: Operator receives clock
- GIVEN an ExecutionContext with mock clock
- WHEN operator checks timeout
- THEN it uses ctx.clock not time package

#### Scenario: Nested operator clock access
- GIVEN a pipeline with multiple operators
- WHEN any operator needs time
- THEN all use the same clock instance from context

### Requirement: Interval Arithmetic via Clock

INTERVAL type operations SHALL use clock for current time reference.

#### Scenario: INTERVAL addition
- GIVEN mock clock at "2024-01-15"
- WHEN executing "SELECT current_date + INTERVAL '1 month'"
- THEN result equals "2024-02-15"

#### Scenario: Age calculation
- GIVEN mock clock at "2024-06-15"
- WHEN executing "SELECT age(timestamp '2024-01-15')"
- THEN result equals "5 months"

### Requirement: File Timestamp Operations via Clock

File metadata timestamps SHALL use injected clock.

#### Scenario: File creation timestamp
- GIVEN mock clock at T1
- WHEN writing DuckDB file
- THEN file header contains timestamp T1

#### Scenario: Last modified timestamp
- GIVEN existing file
- AND mock clock advanced to T2
- WHEN checkpoint writes to file
- THEN modification timestamp reflects T2

### Requirement: Test Helper Functions

Test utilities SHALL provide clock-aware helpers.

#### Scenario: withMockClock helper
- GIVEN test function
- WHEN using withMockClock(t, func(mClock, engine))
- THEN engine is configured with mock clock
- AND test has deterministic time control

#### Scenario: advanceAndWait helper
- GIVEN mock clock and duration
- WHEN calling advanceAndWait(mClock, 5*time.Second, ctx)
- THEN clock advances
- AND all triggered timers/tickers complete

### Requirement: Tagging Convention

All clock calls SHALL use component/method tagging.

#### Scenario: Tag format
- GIVEN clock call in TransactionManager.Begin
- WHEN calling clock.Now()
- THEN call includes tags ("TransactionManager", "Begin")

#### Scenario: Multi-phase tagging
- GIVEN method with multiple clock calls
- WHEN measuring duration
- THEN start call tagged ("Component", "Method", "start")
- AND end call tagged ("Component", "Method", "end")

#### Scenario: Trap matching
- GIVEN multiple components calling clock.Now()
- WHEN trap set with mClock.Trap().Now("TransactionManager")
- THEN only TransactionManager calls are trapped

### Requirement: Advance Restrictions

Mock clock advance SHALL be limited to next event.

#### Scenario: Advance exactly to event
- GIVEN timer set for 1 second
- WHEN calling mClock.Advance(1*time.Second)
- THEN timer fires
- AND MustWait returns successfully

#### Scenario: Advance past event fails
- GIVEN timer set for 1 second
- WHEN calling mClock.Advance(2*time.Second)
- THEN test fails with clear error
- AND message indicates overshot event

#### Scenario: Multiple events via loop
- GIVEN ticker with 1-second interval
- WHEN advancing 10 times with loop
- THEN each Advance().MustWait() triggers one tick
- AND all 10 ticks processed deterministically

### Requirement: AdvanceNext for Unknown Timing

AdvanceNext SHALL advance to next event regardless of duration.

#### Scenario: Unknown timer duration
- GIVEN timer set by code under test
- AND test doesn't know exact duration
- WHEN calling d, w := mClock.AdvanceNext()
- THEN clock advances to timer
- AND d contains actual duration
- AND w.MustWait() waits for timer completion

### Requirement: Peek for Conditional Advance

Peek SHALL return duration to next event without advancing.

#### Scenario: Check next event timing
- GIVEN timer at T+5s
- WHEN calling d, ok := mClock.Peek()
- THEN ok is true
- AND d equals 5 seconds
- AND clock has NOT advanced

#### Scenario: No pending events
- GIVEN no timers or tickers
- WHEN calling d, ok := mClock.Peek()
- THEN ok is false

### Requirement: DuckDB Compatibility Tests

Tests SHALL verify DuckDB-compatible timestamp behavior.

#### Scenario: CURRENT_TIMESTAMP in transaction
- GIVEN transaction with multiple SELECT current_timestamp
- WHEN executed in same transaction
- THEN all return same value (DuckDB semantics)

#### Scenario: Timestamp precision
- GIVEN mock clock at known microsecond
- WHEN selecting timestamp
- THEN microsecond precision preserved

#### Scenario: Timestamp zone handling
- GIVEN TIMESTAMP and TIMESTAMPTZ columns
- WHEN querying with mock clock
- THEN timezone handling matches DuckDB

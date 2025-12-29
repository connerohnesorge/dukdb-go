## Context

Query profiling provides visibility into execution plans and performance metrics. This is essential for optimizing queries and understanding execution behavior.

**Stakeholders**: Database administrators, application developers

**Constraints**:
- Must integrate with execution engine
- Profiling should have minimal overhead when disabled
- Must capture metrics during execution

## Goals / Non-Goals

### Goals
- Capture query plan tree with metrics
- Per-operator timing and row counts
- Memory usage tracking
- API-compatible with duckdb-go GetProfilingInfo

### Non-Goals
- Real-time streaming profiling
- Historical query log
- Automatic performance recommendations

## Decisions

### Decision 1: Profile Storage

**What**: Store profiling data in connection-scoped storage

**Why**: Profile data is per-query, per-connection

**Implementation**:
```go
type Conn struct {
    // ... existing fields
    lastProfile *ProfilingInfo
}
```

### Decision 2: Metric Collection with Clock Injection

**What**: Collect metrics during operator execution using injected clock

**Why**:
- Must capture timing and row counts as they happen
- Per deterministic-testing spec, all time-dependent code must use injected clock
- Enables deterministic testing of profiling metrics

**Implementation**:
```go
type operatorMetrics struct {
    clock      quartz.Clock  // Injected clock for timing
    startTime  time.Time
    endTime    time.Time
    rowsIn     int64
    rowsOut    int64
}

func (op *operator) execute(ctx context.Context) error {
    if profiling {
        op.metrics.startTime = op.metrics.clock.Now()
        defer func() { op.metrics.endTime = op.metrics.clock.Now() }()
    }
    // ... execution
}

// ProfilingContext captures timing with injected clock
type ProfilingContext struct {
    clock     quartz.Clock
    startTime time.Time
}

func NewProfilingContext(clock quartz.Clock) *ProfilingContext {
    return &ProfilingContext{clock: clock}
}

func (p *ProfilingContext) Start() {
    p.startTime = p.clock.Now()
}

func (p *ProfilingContext) Elapsed() time.Duration {
    return p.clock.Since(p.startTime)
}

// Tests use mock clock for deterministic timing
func TestProfilingTiming(t *testing.T) {
    mClock := quartz.NewMock(t)
    mClock.Set(time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC))

    ctx := NewProfilingContext(mClock)
    ctx.Start()

    mClock.Advance(500*time.Millisecond).MustWait()

    elapsed := ctx.Elapsed()
    assert.Equal(t, 500*time.Millisecond, elapsed)
}
```

## Risks / Trade-offs

### Risk 1: Profiling Overhead
**Risk**: Profiling slows down queries
**Mitigation**: Off by default; fast early-out check

## Migration Plan

New capability with no migration required.
